/**
 * Durable recovery contracts and profile-scoped filesystem store
 * Zones: recovery, queue lifecycle, delivery, multi-instance bus, filesystem
 * Owns the strict v1 schema, inbound/outbound recovery state machines, byte
 * reservations, identity claims, retention, and downgrade quarantine primitives.
 * Runtime wiring to polling, queue dispatch, and the multi-instance bus remains
 * outside this domain.
 */

import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  existsSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, join } from "node:path";

import { withTelegramFileTransaction } from "./locks.ts";
import {
  getTelegramProfilePathSuffix,
  resolveTelegramTempDir,
} from "./paths.ts";
import type { TelegramTarget } from "./target.ts";

export const RECOVERY_SCHEMA_VERSION = 1 as const;
export const RECOVERY_PROFILE_QUOTA_BYTES = 512 * 1024 * 1024;
export const RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS = 24 * 60 * 60 * 1000;
export const RECOVERY_TERMINAL_METADATA_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
export const RECOVERY_TERMINAL_METADATA_MAX_RECORDS = 100_000;
export const RECOVERY_TERMINAL_METADATA_MAX_BYTES = 64 * 1024 * 1024;
export const RECOVERY_STORE_DIRECTORY_MODE = 0o700;
export const RECOVERY_STORE_FILE_MODE = 0o600;
export const RECOVERY_STORE_DIRECTORY_NAME = "recovery-v1";
export const RECOVERY_STORE_QUARANTINE_PREFIX = "recovery-v1-quarantine-";
export const RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS = 3;
export const RECOVERY_OUTBOUND_RETRY_DELAYS_MS = [250, 1_000] as const;

export const RECOVERY_INBOUND_STATES = [
  "observed",
  "admitted",
  "pre-dispatch",
  "dispatching",
  "completed",
  "execution-uncertain",
  "explicitly-discarded",
] as const;
export type RecoveryInboundState = (typeof RECOVERY_INBOUND_STATES)[number];
export const RECOVERY_INBOUND_TERMINAL_REASONS = [
  "ignored",
  "unauthorized",
  "unsupported",
  "poison-skipped",
] as const;
export type RecoveryInboundTerminalReason =
  (typeof RECOVERY_INBOUND_TERMINAL_REASONS)[number];
export const RECOVERY_UNRESOLVED_INBOUND_STATES = [
  "observed",
  "admitted",
  "pre-dispatch",
  "dispatching",
  "execution-uncertain",
] as const satisfies readonly RecoveryInboundState[];

export const RECOVERY_OUTBOUND_STATES = [
  "planned",
  "pending",
  "sending",
  "delivered",
  "delivery-uncertain",
  "retryable-pending",
  "explicitly-discarded",
] as const;
export type RecoveryOutboundState = (typeof RECOVERY_OUTBOUND_STATES)[number];
export const RECOVERY_UNRESOLVED_OUTBOUND_STATES = [
  "planned",
  "pending",
  "sending",
  "delivery-uncertain",
  "retryable-pending",
] as const satisfies readonly RecoveryOutboundState[];

export const RECOVERY_OUTBOUND_UNCERTAINTY_REASONS = [
  "commit-unknown",
  "timeout-after-write",
  "connection-lost-after-write",
  "malformed-success",
  "response-lost",
  "authority-lost-after-start",
  "process-reopened-sending",
  "confirmed-before-receipt",
] as const;
export type RecoveryOutboundUncertaintyReason =
  (typeof RECOVERY_OUTBOUND_UNCERTAINTY_REASONS)[number];

export const RECOVERY_BUS_STATES = [
  "pending",
  "completed",
  "bus-uncertain",
  "explicitly-discarded",
] as const;
export type RecoveryBusState = (typeof RECOVERY_BUS_STATES)[number];
export const RECOVERY_UNRESOLVED_BUS_STATES = [
  "pending",
  "bus-uncertain",
] as const satisfies readonly RecoveryBusState[];

export const RECOVERY_REASSIGNMENT_STATES = [
  "requested",
  "binding-transfer-pending",
  "binding-transferred",
  "recovery-grant-committed",
  "cancelled-before-transfer",
  "rollback-pending",
] as const;
export type RecoveryReassignmentState =
  (typeof RECOVERY_REASSIGNMENT_STATES)[number];

export const RECOVERY_STORE_MODES = ["active", "downgrade-exclusive"] as const;
export type RecoveryStoreMode = (typeof RECOVERY_STORE_MODES)[number];

export const RECOVERY_OPERATION_GATE_PHASES = [
  "active",
  "fencing",
  "downgrade-exclusive",
  "quarantined",
] as const;
export type RecoveryOperationGatePhase =
  (typeof RECOVERY_OPERATION_GATE_PHASES)[number];

export interface RecoveryOperationGateState {
  profile: string;
  phase: RecoveryOperationGatePhase;
  fenceGeneration?: string;
  inFlight: number;
}

export interface RecoveryOperationLease {
  profile: string;
  operationGeneration: number;
  release(): void;
}

interface MutableRecoveryOperationGateState extends RecoveryOperationGateState {
  operationGeneration: number;
  drainWaiters: Set<() => void>;
}

/**
 * Process-local half of the downgrade fence. The on-disk store mode remains the
 * final cross-process authority, while this gate prevents already-running runtime
 * work from racing the exclusive transition in this process.
 */
export class RecoveryProfileOperationGate {
  readonly #states = new Map<string, MutableRecoveryOperationGateState>();

  #state(profile: string): MutableRecoveryOperationGateState {
    if (!profile) throw new Error("Recovery gate profile is required");
    let state = this.#states.get(profile);
    if (!state) {
      state = {
        profile,
        phase: "active",
        inFlight: 0,
        operationGeneration: 1,
        drainWaiters: new Set(),
      };
      this.#states.set(profile, state);
    }
    return state;
  }

  getState(profile: string): RecoveryOperationGateState {
    const state = this.#state(profile);
    return {
      profile: state.profile,
      phase: state.phase,
      ...(state.fenceGeneration
        ? { fenceGeneration: state.fenceGeneration }
        : {}),
      inFlight: state.inFlight,
    };
  }

  enter(profile: string): RecoveryOperationLease | undefined {
    const state = this.#state(profile);
    if (state.phase !== "active") return undefined;
    state.inFlight += 1;
    const operationGeneration = state.operationGeneration;
    let released = false;
    return {
      profile,
      operationGeneration,
      release: () => {
        if (released) return;
        released = true;
        const current = this.#state(profile);
        if (
          current.operationGeneration !== operationGeneration ||
          current.inFlight <= 0
        ) {
          throw new Error("Recovery operation lease generation mismatch");
        }
        current.inFlight -= 1;
        if (current.inFlight === 0 && current.phase !== "active") {
          const waiters = [...current.drainWaiters];
          current.drainWaiters.clear();
          for (const resolve of waiters) resolve();
        }
      },
    };
  }

  beginFencing(profile: string, fenceGeneration: string = randomUUID()): string {
    if (!fenceGeneration) {
      throw new Error("Recovery fence generation is required");
    }
    const state = this.#state(profile);
    if (state.phase === "active") {
      state.phase = "fencing";
      state.fenceGeneration = fenceGeneration;
      return fenceGeneration;
    }
    if (
      state.phase === "fencing" &&
      state.fenceGeneration === fenceGeneration
    ) {
      return fenceGeneration;
    }
    throw new Error("Recovery gate is already fenced by another generation");
  }

  async awaitDrained(profile: string, fenceGeneration: string): Promise<void> {
    const state = this.#requireFence(profile, fenceGeneration);
    if (state.inFlight === 0) return;
    await new Promise<void>((resolve) => state.drainWaiters.add(resolve));
    this.#requireFence(profile, fenceGeneration);
  }

  beginDowngradeExclusive(profile: string, fenceGeneration: string): void {
    const state = this.#requireFence(profile, fenceGeneration);
    if (state.phase !== "fencing" || state.inFlight !== 0) {
      throw new Error("Recovery gate cannot become exclusive before drain");
    }
    state.phase = "downgrade-exclusive";
  }

  markQuarantined(profile: string, fenceGeneration: string): void {
    const state = this.#requireFence(profile, fenceGeneration);
    if (state.phase !== "downgrade-exclusive" || state.inFlight !== 0) {
      throw new Error("Recovery gate is not downgrade-exclusive");
    }
    state.phase = "quarantined";
  }

  resume(profile: string, fenceGeneration: string): void {
    const state = this.#requireResumableFence(profile, fenceGeneration);
    this.#activate(state);
  }

  async resumeAfter(
    profile: string,
    fenceGeneration: string,
    resumeRuntime: () => Promise<void> | void,
  ): Promise<void> {
    this.#requireResumableFence(profile, fenceGeneration);
    await resumeRuntime();
    const state = this.#requireResumableFence(profile, fenceGeneration);
    this.#activate(state);
  }

  #requireResumableFence(
    profile: string,
    fenceGeneration: string,
  ): MutableRecoveryOperationGateState {
    const state = this.#requireFence(profile, fenceGeneration);
    if (state.phase === "quarantined") {
      throw new Error("Quarantined recovery gate cannot resume");
    }
    if (state.inFlight !== 0) {
      throw new Error("Recovery gate cannot resume with work in flight");
    }
    return state;
  }

  #activate(state: MutableRecoveryOperationGateState): void {
    state.phase = "active";
    delete state.fenceGeneration;
    state.operationGeneration += 1;
    state.drainWaiters.clear();
  }

  #requireFence(
    profile: string,
    fenceGeneration: string,
  ): MutableRecoveryOperationGateState {
    const state = this.#state(profile);
    if (
      state.phase === "active" ||
      !fenceGeneration ||
      state.fenceGeneration !== fenceGeneration
    ) {
      throw new Error("Recovery fence generation mismatch");
    }
    return state;
  }
}

export type RecoveryOwnerIdentity =
  | {
      kind: "leader";
      ownerId: string;
      leaderEpoch: string | number;
    }
  | {
      kind: "manual-follower";
      ownerId: string;
      registrationGeneration: string;
    };

export interface RecoveryIdentity {
  profile: string;
  target: TelegramTarget;
  owner: RecoveryOwnerIdentity;
  /** Pi session fence; distinct from transport leader/registration generations. */
  sessionGeneration: number;
}

export interface RecoveryPayloadReference {
  payloadId: string;
  byteLength: number;
  sha256: string;
}

export interface RecoverySpoolReference {
  spoolId: string;
  byteLength: number;
  sha256: string;
}

interface RecoveryRecordBase {
  recordId: string;
  identity: RecoveryIdentity;
  createdRevision: number;
  /** Revision that established the current state; terminal states never transition. */
  stateRevision: number;
  createdAtMs: number;
  updatedAtMs: number;
  payloadRef?: RecoveryPayloadReference;
  spoolRefs: RecoverySpoolReference[];
}

export interface RecoveryInboundRecord extends RecoveryRecordBase {
  family: "inbound";
  updateId: number;
  turnId: string;
  state: RecoveryInboundState;
  /** Immutable revision at which this update first became durably acknowledged. */
  admissionRevision?: number;
  linkedAttemptOf?: string;
  terminalReason?: RecoveryInboundTerminalReason;
}

export interface RecoveryOutboundActiveUnit {
  unitIndex: number;
  attemptId: string;
  startedAtMs: number;
}

export interface RecoveryOutboundReceipt {
  unitIndex: number;
  operationId: string;
  method: string;
  messageId?: number;
  committedAtMs: number;
}

export interface RecoveryOutboundUncertainty {
  unitIndex: number;
  reason: RecoveryOutboundUncertaintyReason;
  observedAtMs: number;
}

export interface RecoveryOutboundRecord extends RecoveryRecordBase {
  family: "outbound";
  intentId: string;
  turnId: string;
  sourceInboundRecordIds: string[];
  state: RecoveryOutboundState;
  nextUnitIndex: number;
  activeUnit?: RecoveryOutboundActiveUnit;
  automaticAttemptCount: number;
  retryNotBeforeMs?: number;
  receipts: RecoveryOutboundReceipt[];
  uncertainty?: RecoveryOutboundUncertainty;
  linkedAttemptOf?: string;
}

export type RecoveryOutboundRenderingMode = "rich" | "html";
export type RecoveryOutboundMediaKind =
  | "photo"
  | "video"
  | "audio"
  | "document"
  | "voice";

export interface RecoveryOutboundButton {
  label: string;
  prompt: string;
}

export interface RecoveryOutboundVoiceMetadata {
  text: string;
  automatic: boolean;
}

export type RecoveryOutboundUnit =
  | {
      kind: "final-text";
      operationId: string;
      method: string;
      content: string;
      contentMode: "rich-markdown" | "html" | "plain";
    }
  | {
      kind: "rich-media" | "attachment" | "voice";
      operationId: string;
      method: string;
      spoolRefIndex: number;
      fileName: string;
      mediaKind: RecoveryOutboundMediaKind;
      caption?: string;
    }
  | {
      kind: "guest";
      operationId: string;
      method: string;
      markdown?: string;
      spoolRefIndex?: number;
      fileName?: string;
      mediaKind?: RecoveryOutboundMediaKind;
      caption?: string;
    };

export interface RecoveryOutboundPlanInput {
  intentId: string;
  turnId: string;
  sourceInboundRecordIds: readonly string[];
  claim: RecoveryIdentityClaim;
  replyToMessageId: number;
  renderingMode: RecoveryOutboundRenderingMode;
  finalMarkdown: string;
  renderedChunks: readonly string[];
  units: readonly RecoveryOutboundUnit[];
  buttons?: readonly RecoveryOutboundButton[];
  voice?: RecoveryOutboundVoiceMetadata;
  guestQueryId?: string;
  spool?: readonly Uint8Array[];
}

interface RecoveryOutboundPayloadV1 {
  version: 1;
  intentId: string;
  turnId: string;
  replyToMessageId: number;
  renderingMode: RecoveryOutboundRenderingMode;
  finalMarkdown: string;
  renderedChunks: string[];
  units: RecoveryOutboundUnit[];
  buttons: RecoveryOutboundButton[];
  voice?: RecoveryOutboundVoiceMetadata;
  guestQueryId?: string;
}

export interface RecoveryBusRecord extends RecoveryRecordBase {
  family: "bus";
  requestId: string;
  payloadFingerprint: string;
  state: RecoveryBusState;
}

export interface RecoveryReassignmentRecord {
  family: "reassignment";
  reassignmentId: string;
  profile: string;
  target: TelegramTarget;
  oldOwner: RecoveryOwnerIdentity;
  newOwner: RecoveryOwnerIdentity;
  newSessionGeneration: number;
  captureThroughRevision: number;
  unresolvedRecordIds: string[];
  state: RecoveryReassignmentState;
  createdAtMs: number;
  updatedAtMs: number;
}

/**
 * Process-local single-use handoff shape. It is validated independently and is
 * deliberately absent from RecoverySnapshotV1 so no handoff capability is persisted.
 */
export interface RecoverySameProcessHandoff {
  handoffId: string;
  profile: string;
  target: TelegramTarget;
  owner: RecoveryOwnerIdentity;
  fromSessionGeneration: number;
  toSessionGeneration: number;
  createdAtMs: number;
  expiresAtMs: number;
  consumedAtMs?: number;
}

export interface RecoveryQuotaAccounting {
  recordBytes: number;
  payloadBytes: number;
  spoolBytes: number;
  reservedBytes: number;
  totalBytes: number;
}

export interface RecoverySnapshotV1 {
  version: typeof RECOVERY_SCHEMA_VERSION;
  profile: string;
  mode: RecoveryStoreMode;
  revision: number;
  writtenAtMs: number;
  quota: RecoveryQuotaAccounting;
  /** Sole durable inbound offset authority; polling wiring is added in P0-C C2. */
  committedUpdateId: number | null;
  inbound: RecoveryInboundRecord[];
  outbound: RecoveryOutboundRecord[];
  bus: RecoveryBusRecord[];
  reassignments: RecoveryReassignmentRecord[];
}

const PROFILE_PATTERN = /^[a-z0-9]{1,32}$/;
const RESERVED_PROFILES = new Set(["active", "main"]);
const CONTROL_CHARACTER_PATTERN = /[\u0000-\u001f\u007f]/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;

type JsonObject = Record<string, unknown>;

function fail(path: string, message: string): never {
  throw new Error(`Invalid recovery snapshot at ${path}: ${message}`);
}

function readObject(value: unknown, path: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    fail(path, "expected an object");
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    fail(path, "expected a plain object");
  }
  return value as JsonObject;
}

function assertKeys(
  value: JsonObject,
  path: string,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  const allowed = new Set([...required, ...optional]);
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) fail(`${path}.${key}`, "unknown field");
  }
  for (const key of required) {
    if (!Object.hasOwn(value, key)) fail(`${path}.${key}`, "missing field");
  }
}

function readStableString(value: unknown, path: string): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 256 ||
    CONTROL_CHARACTER_PATTERN.test(value)
  ) {
    fail(path, "expected a non-empty stable string without control characters");
  }
  return value;
}

function readText(value: unknown, path: string): string {
  if (typeof value !== "string" || value.includes("\u0000")) {
    fail(path, "expected text without NUL characters");
  }
  return value;
}

function readBoolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") fail(path, "expected a boolean");
  return value;
}

function readSafeFileName(value: unknown, path: string): string {
  const fileName = readStableString(value, path);
  if (
    fileName === "." ||
    fileName === ".." ||
    fileName.includes("/") ||
    fileName.includes("\\") ||
    basename(fileName) !== fileName
  ) {
    fail(path, "expected a safe filename without a path");
  }
  return fileName;
}

function readProfile(value: unknown, path: string): string {
  if (
    typeof value !== "string" ||
    !PROFILE_PATTERN.test(value) ||
    RESERVED_PROFILES.has(value)
  ) {
    fail(path, "expected a valid Telegram profile name");
  }
  return value;
}

function readSafeInteger(
  value: unknown,
  path: string,
  options: { minimum?: number; nonzero?: boolean } = {},
): number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    !Number.isSafeInteger(value) ||
    (options.minimum !== undefined && value < options.minimum) ||
    (options.nonzero && value === 0)
  ) {
    fail(path, "expected a finite safe integer");
  }
  return value;
}

function readTimestamp(value: unknown, path: string): number {
  return readSafeInteger(value, path, { minimum: 0 });
}

function readStringOrIntegerGeneration(
  value: unknown,
  path: string,
): string | number {
  return typeof value === "number"
    ? readSafeInteger(value, path, { minimum: 0 })
    : readStableString(value, path);
}

function readEnum<T extends string>(
  value: unknown,
  path: string,
  allowed: readonly T[],
): T {
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    fail(path, `expected one of ${allowed.join(", ")}`);
  }
  return value as T;
}

function readArray<T>(
  value: unknown,
  path: string,
  readEntry: (entry: unknown, entryPath: string) => T,
): T[] {
  if (!Array.isArray(value)) fail(path, "expected an array");
  return value.map((entry, index) => readEntry(entry, `${path}[${index}]`));
}

function readTarget(value: unknown, path: string): TelegramTarget {
  const object = readObject(value, path);
  assertKeys(object, path, ["chatId"], ["threadId"]);
  const target: TelegramTarget = {
    chatId: readSafeInteger(object.chatId, `${path}.chatId`, { nonzero: true }),
  };
  if (Object.hasOwn(object, "threadId")) {
    target.threadId = readSafeInteger(object.threadId, `${path}.threadId`, {
      minimum: 1,
    });
  }
  return target;
}

function readOwner(value: unknown, path: string): RecoveryOwnerIdentity {
  const object = readObject(value, path);
  if (object.kind === "leader") {
    assertKeys(object, path, ["kind", "ownerId", "leaderEpoch"]);
    return {
      kind: "leader",
      ownerId: readStableString(object.ownerId, `${path}.ownerId`),
      leaderEpoch: readStringOrIntegerGeneration(
        object.leaderEpoch,
        `${path}.leaderEpoch`,
      ),
    };
  }
  if (object.kind === "manual-follower") {
    assertKeys(object, path, ["kind", "ownerId", "registrationGeneration"]);
    return {
      kind: "manual-follower",
      ownerId: readStableString(object.ownerId, `${path}.ownerId`),
      registrationGeneration: readStableString(
        object.registrationGeneration,
        `${path}.registrationGeneration`,
      ),
    };
  }
  fail(`${path}.kind`, "expected leader or manual-follower");
}

function readIdentity(value: unknown, path: string): RecoveryIdentity {
  const object = readObject(value, path);
  assertKeys(object, path, ["profile", "target", "owner", "sessionGeneration"]);
  return {
    profile: readProfile(object.profile, `${path}.profile`),
    target: readTarget(object.target, `${path}.target`),
    owner: readOwner(object.owner, `${path}.owner`),
    sessionGeneration: readSafeInteger(
      object.sessionGeneration,
      `${path}.sessionGeneration`,
      { minimum: 0 },
    ),
  };
}

function readPayloadRef(
  value: unknown,
  path: string,
): RecoveryPayloadReference {
  const object = readObject(value, path);
  assertKeys(object, path, ["payloadId", "byteLength", "sha256"]);
  const sha256 = readStableString(object.sha256, `${path}.sha256`);
  if (!SHA256_PATTERN.test(sha256))
    fail(`${path}.sha256`, "expected SHA-256 hex");
  return {
    payloadId: readStableString(object.payloadId, `${path}.payloadId`),
    byteLength: readSafeInteger(object.byteLength, `${path}.byteLength`, {
      minimum: 0,
    }),
    sha256,
  };
}

function readSpoolRef(value: unknown, path: string): RecoverySpoolReference {
  const object = readObject(value, path);
  assertKeys(object, path, ["spoolId", "byteLength", "sha256"]);
  const sha256 = readStableString(object.sha256, `${path}.sha256`);
  if (!SHA256_PATTERN.test(sha256))
    fail(`${path}.sha256`, "expected SHA-256 hex");
  return {
    spoolId: readStableString(object.spoolId, `${path}.spoolId`),
    byteLength: readSafeInteger(object.byteLength, `${path}.byteLength`, {
      minimum: 0,
    }),
    sha256,
  };
}

interface ParsedRecordBase {
  recordId: string;
  identity: RecoveryIdentity;
  createdRevision: number;
  stateRevision: number;
  createdAtMs: number;
  updatedAtMs: number;
  payloadRef?: RecoveryPayloadReference;
  spoolRefs: RecoverySpoolReference[];
}

const RECORD_BASE_REQUIRED_KEYS = [
  "family",
  "recordId",
  "identity",
  "createdRevision",
  "stateRevision",
  "createdAtMs",
  "updatedAtMs",
  "spoolRefs",
] as const;
const RECORD_BASE_OPTIONAL_KEYS = ["payloadRef"] as const;

function readRecordBase(object: JsonObject, path: string): ParsedRecordBase {
  const createdAtMs = readTimestamp(object.createdAtMs, `${path}.createdAtMs`);
  const updatedAtMs = readTimestamp(object.updatedAtMs, `${path}.updatedAtMs`);
  if (updatedAtMs < createdAtMs) {
    fail(`${path}.updatedAtMs`, "must not precede createdAtMs");
  }
  const parsed: ParsedRecordBase = {
    recordId: readStableString(object.recordId, `${path}.recordId`),
    identity: readIdentity(object.identity, `${path}.identity`),
    createdRevision: readSafeInteger(
      object.createdRevision,
      `${path}.createdRevision`,
      { minimum: 0 },
    ),
    stateRevision: readSafeInteger(
      object.stateRevision,
      `${path}.stateRevision`,
      { minimum: 0 },
    ),
    createdAtMs,
    updatedAtMs,
    spoolRefs: readArray(object.spoolRefs, `${path}.spoolRefs`, readSpoolRef),
  };
  if (Object.hasOwn(object, "payloadRef")) {
    parsed.payloadRef = readPayloadRef(object.payloadRef, `${path}.payloadRef`);
  }
  assertUniqueStrings(
    parsed.spoolRefs.map((entry) => entry.spoolId),
    `${path}.spoolRefs`,
  );
  return parsed;
}

function readInboundRecord(
  value: unknown,
  path: string,
): RecoveryInboundRecord {
  const object = readObject(value, path);
  assertKeys(
    object,
    path,
    [...RECORD_BASE_REQUIRED_KEYS, "updateId", "turnId", "state"],
    [
      ...RECORD_BASE_OPTIONAL_KEYS,
      "admissionRevision",
      "linkedAttemptOf",
      "terminalReason",
    ],
  );
  if (object.family !== "inbound") fail(`${path}.family`, "expected inbound");
  const base = readRecordBase(object, path);
  const admissionRevision = Object.hasOwn(object, "admissionRevision")
    ? readSafeInteger(object.admissionRevision, `${path}.admissionRevision`, {
        minimum: 0,
      })
    : undefined;
  const linkedAttemptOf = Object.hasOwn(object, "linkedAttemptOf")
    ? readStableString(object.linkedAttemptOf, `${path}.linkedAttemptOf`)
    : undefined;
  const terminalReason = Object.hasOwn(object, "terminalReason")
    ? readEnum(
        object.terminalReason,
        `${path}.terminalReason`,
        RECOVERY_INBOUND_TERMINAL_REASONS,
      )
    : undefined;
  return {
    ...base,
    family: "inbound",
    updateId: readSafeInteger(object.updateId, `${path}.updateId`, {
      minimum: 0,
    }),
    turnId: readStableString(object.turnId, `${path}.turnId`),
    state: readEnum(object.state, `${path}.state`, RECOVERY_INBOUND_STATES),
    ...(admissionRevision !== undefined ? { admissionRevision } : {}),
    ...(linkedAttemptOf ? { linkedAttemptOf } : {}),
    ...(terminalReason ? { terminalReason } : {}),
  };
}

function readOutboundRecord(
  value: unknown,
  path: string,
): RecoveryOutboundRecord {
  const object = readObject(value, path);
  assertKeys(
    object,
    path,
    [
      ...RECORD_BASE_REQUIRED_KEYS,
      "intentId",
      "turnId",
      "sourceInboundRecordIds",
      "state",
      "nextUnitIndex",
      "automaticAttemptCount",
      "receipts",
    ],
    [
      ...RECORD_BASE_OPTIONAL_KEYS,
      "activeUnit",
      "retryNotBeforeMs",
      "uncertainty",
      "linkedAttemptOf",
    ],
  );
  if (object.family !== "outbound") fail(`${path}.family`, "expected outbound");
  const base = readRecordBase(object, path);
  const sourceInboundRecordIds = readArray(
    object.sourceInboundRecordIds,
    `${path}.sourceInboundRecordIds`,
    readStableString,
  );
  if (sourceInboundRecordIds.length === 0) {
    fail(`${path}.sourceInboundRecordIds`, "must not be empty");
  }
  assertUniqueStrings(sourceInboundRecordIds, `${path}.sourceInboundRecordIds`);
  const state = readEnum(object.state, `${path}.state`, RECOVERY_OUTBOUND_STATES);
  const receipts = readArray(
    object.receipts,
    `${path}.receipts`,
    readOutboundReceipt,
  );
  assertUniqueStrings(
    receipts.map((receipt) => receipt.operationId),
    `${path}.receipts.operationId`,
  );
  for (let index = 0; index < receipts.length; index += 1) {
    const receipt = receipts[index]!;
    if (receipt.unitIndex !== index) {
      fail(`${path}.receipts[${index}].unitIndex`, "must be contiguous and ordered");
    }
    if (
      receipt.committedAtMs < base.createdAtMs ||
      receipt.committedAtMs > base.updatedAtMs ||
      (index > 0 && receipt.committedAtMs < receipts[index - 1]!.committedAtMs)
    ) {
      fail(
        `${path}.receipts[${index}].committedAtMs`,
        "must be ordered within the record lifetime",
      );
    }
  }
  const nextUnitIndex = readSafeInteger(
    object.nextUnitIndex,
    `${path}.nextUnitIndex`,
    { minimum: 0 },
  );
  if (nextUnitIndex !== receipts.length) {
    fail(`${path}.nextUnitIndex`, "must equal the confirmed receipt count");
  }
  const automaticAttemptCount = readSafeInteger(
    object.automaticAttemptCount,
    `${path}.automaticAttemptCount`,
    { minimum: 0 },
  );
  if (automaticAttemptCount > RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS) {
    fail(`${path}.automaticAttemptCount`, "exceeds the automatic start limit");
  }
  const activeUnit = Object.hasOwn(object, "activeUnit")
    ? readOutboundActiveUnit(object.activeUnit, `${path}.activeUnit`)
    : undefined;
  const retryNotBeforeMs = Object.hasOwn(object, "retryNotBeforeMs")
    ? readTimestamp(object.retryNotBeforeMs, `${path}.retryNotBeforeMs`)
    : undefined;
  const uncertainty = Object.hasOwn(object, "uncertainty")
    ? readOutboundUncertainty(object.uncertainty, `${path}.uncertainty`)
    : undefined;
  const linkedAttemptOf = Object.hasOwn(object, "linkedAttemptOf")
    ? readStableString(object.linkedAttemptOf, `${path}.linkedAttemptOf`)
    : undefined;
  if (state === "sending") {
    if (!activeUnit || activeUnit.unitIndex !== nextUnitIndex) {
      fail(`${path}.activeUnit`, "is required for the exact next sending unit");
    }
    if (
      activeUnit.startedAtMs < base.createdAtMs ||
      activeUnit.startedAtMs > base.updatedAtMs
    ) {
      fail(`${path}.activeUnit.startedAtMs`, "must fall within the record lifetime");
    }
    if (automaticAttemptCount < 1 || retryNotBeforeMs !== undefined || uncertainty) {
      fail(path, "sending state has invalid retry or uncertainty metadata");
    }
  } else if (activeUnit) {
    fail(`${path}.activeUnit`, "is valid only while sending");
  }
  if (state === "retryable-pending") {
    if (automaticAttemptCount < 1 || uncertainty) {
      fail(path, "retryable-pending requires prior safe attempts only");
    }
    if (
      automaticAttemptCount < RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS &&
      retryNotBeforeMs === undefined
    ) {
      fail(`${path}.retryNotBeforeMs`, "is required before automatic retry");
    }
    if (
      automaticAttemptCount === RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS &&
      retryNotBeforeMs !== undefined
    ) {
      fail(`${path}.retryNotBeforeMs`, "must be absent after retry exhaustion");
    }
  } else if (retryNotBeforeMs !== undefined) {
    fail(`${path}.retryNotBeforeMs`, "is valid only while retryable-pending");
  }
  if (state === "delivery-uncertain") {
    if (
      !uncertainty ||
      uncertainty.unitIndex !== nextUnitIndex ||
      automaticAttemptCount < 1
    ) {
      fail(`${path}.uncertainty`, "must identify the exact ambiguous unit");
    }
    if (
      uncertainty.observedAtMs < base.createdAtMs ||
      uncertainty.observedAtMs > base.updatedAtMs
    ) {
      fail(`${path}.uncertainty.observedAtMs`, "must fall within the record lifetime");
    }
  } else if (uncertainty) {
    fail(`${path}.uncertainty`, "is valid only for delivery-uncertain");
  }
  if (state === "planned" && nextUnitIndex !== 0) {
    fail(`${path}.nextUnitIndex`, "planned work cannot have receipts");
  }
  if (
    (state === "planned" || state === "pending") &&
    automaticAttemptCount !== 0
  ) {
    fail(`${path}.automaticAttemptCount`, "must be zero for a fresh pending unit");
  }
  if (state === "delivered" && automaticAttemptCount !== 0) {
    fail(`${path}.automaticAttemptCount`, "must reset after final receipt");
  }
  if (
    (state === "delivered" || state === "explicitly-discarded") &&
    base.spoolRefs.length > 0
  ) {
    fail(`${path}.spoolRefs`, "terminal resolution must release outbound spools");
  }
  if (state === "explicitly-discarded" && base.payloadRef) {
    fail(`${path}.payloadRef`, "discarded outbound work must release its payload");
  }
  return {
    ...base,
    family: "outbound",
    intentId: readStableString(object.intentId, `${path}.intentId`),
    turnId: readStableString(object.turnId, `${path}.turnId`),
    sourceInboundRecordIds,
    state,
    nextUnitIndex,
    ...(activeUnit ? { activeUnit } : {}),
    automaticAttemptCount,
    ...(retryNotBeforeMs !== undefined ? { retryNotBeforeMs } : {}),
    receipts,
    ...(uncertainty ? { uncertainty } : {}),
    ...(linkedAttemptOf ? { linkedAttemptOf } : {}),
  };
}

function readOutboundActiveUnit(
  value: unknown,
  path: string,
): RecoveryOutboundActiveUnit {
  const object = readObject(value, path);
  assertKeys(object, path, ["unitIndex", "attemptId", "startedAtMs"]);
  return {
    unitIndex: readSafeInteger(object.unitIndex, `${path}.unitIndex`, {
      minimum: 0,
    }),
    attemptId: readStableString(object.attemptId, `${path}.attemptId`),
    startedAtMs: readTimestamp(object.startedAtMs, `${path}.startedAtMs`),
  };
}

function readOutboundReceipt(
  value: unknown,
  path: string,
): RecoveryOutboundReceipt {
  const object = readObject(value, path);
  assertKeys(
    object,
    path,
    ["unitIndex", "operationId", "method", "committedAtMs"],
    ["messageId"],
  );
  const receipt: RecoveryOutboundReceipt = {
    unitIndex: readSafeInteger(object.unitIndex, `${path}.unitIndex`, {
      minimum: 0,
    }),
    operationId: readStableString(
      object.operationId,
      `${path}.operationId`,
    ),
    method: readStableString(object.method, `${path}.method`),
    committedAtMs: readTimestamp(
      object.committedAtMs,
      `${path}.committedAtMs`,
    ),
  };
  if (Object.hasOwn(object, "messageId")) {
    receipt.messageId = readSafeInteger(
      object.messageId,
      `${path}.messageId`,
      { minimum: 1 },
    );
  }
  return receipt;
}

function readOutboundUncertainty(
  value: unknown,
  path: string,
): RecoveryOutboundUncertainty {
  const object = readObject(value, path);
  assertKeys(object, path, ["unitIndex", "reason", "observedAtMs"]);
  return {
    unitIndex: readSafeInteger(object.unitIndex, `${path}.unitIndex`, {
      minimum: 0,
    }),
    reason: readEnum(
      object.reason,
      `${path}.reason`,
      RECOVERY_OUTBOUND_UNCERTAINTY_REASONS,
    ),
    observedAtMs: readTimestamp(object.observedAtMs, `${path}.observedAtMs`),
  };
}

function readOutboundButton(
  value: unknown,
  path: string,
): RecoveryOutboundButton {
  const object = readObject(value, path);
  assertKeys(object, path, ["label", "prompt"]);
  return {
    label: readStableString(object.label, `${path}.label`),
    prompt: readText(object.prompt, `${path}.prompt`),
  };
}

function readOutboundVoiceMetadata(
  value: unknown,
  path: string,
): RecoveryOutboundVoiceMetadata {
  const object = readObject(value, path);
  assertKeys(object, path, ["text", "automatic"]);
  return {
    text: readText(object.text, `${path}.text`),
    automatic: readBoolean(object.automatic, `${path}.automatic`),
  };
}

function readOutboundUnit(value: unknown, path: string): RecoveryOutboundUnit {
  const object = readObject(value, path);
  const kind = readEnum(object.kind, `${path}.kind`, [
    "final-text",
    "rich-media",
    "attachment",
    "voice",
    "guest",
  ] as const);
  if (kind === "final-text") {
    assertKeys(object, path, [
      "kind",
      "operationId",
      "method",
      "content",
      "contentMode",
    ]);
    return {
      kind,
      operationId: readStableString(
        object.operationId,
        `${path}.operationId`,
      ),
      method: readStableString(object.method, `${path}.method`),
      content: readText(object.content, `${path}.content`),
      contentMode: readEnum(object.contentMode, `${path}.contentMode`, [
        "rich-markdown",
        "html",
        "plain",
      ] as const),
    };
  }
  if (kind === "guest") {
    assertKeys(
      object,
      path,
      ["kind", "operationId", "method"],
      ["markdown", "spoolRefIndex", "fileName", "mediaKind", "caption"],
    );
    const unit: Extract<RecoveryOutboundUnit, { kind: "guest" }> = {
      kind,
      operationId: readStableString(
        object.operationId,
        `${path}.operationId`,
      ),
      method: readStableString(object.method, `${path}.method`),
    };
    if (Object.hasOwn(object, "markdown")) {
      unit.markdown = readText(object.markdown, `${path}.markdown`);
    }
    const attachmentKeys = ["spoolRefIndex", "fileName", "mediaKind"];
    const attachmentKeyCount = attachmentKeys.filter((key) =>
      Object.hasOwn(object, key)
    ).length;
    if (attachmentKeyCount !== 0 && attachmentKeyCount !== attachmentKeys.length) {
      fail(path, "guest attachment metadata must be complete");
    }
    if (attachmentKeyCount > 0) {
      unit.spoolRefIndex = readSafeInteger(
        object.spoolRefIndex,
        `${path}.spoolRefIndex`,
        { minimum: 0 },
      );
      unit.fileName = readSafeFileName(object.fileName, `${path}.fileName`);
      unit.mediaKind = readEnum(object.mediaKind, `${path}.mediaKind`, [
        "photo",
        "video",
        "audio",
        "document",
        "voice",
      ] as const);
    }
    if (Object.hasOwn(object, "caption")) {
      unit.caption = readText(object.caption, `${path}.caption`);
    }
    if (unit.markdown === undefined && unit.spoolRefIndex === undefined) {
      fail(path, "guest unit requires markdown or an attachment");
    }
    return unit;
  }
  assertKeys(
    object,
    path,
    [
      "kind",
      "operationId",
      "method",
      "spoolRefIndex",
      "fileName",
      "mediaKind",
    ],
    ["caption"],
  );
  const unit: Extract<
    RecoveryOutboundUnit,
    { kind: "rich-media" | "attachment" | "voice" }
  > = {
    kind,
    operationId: readStableString(object.operationId, `${path}.operationId`),
    method: readStableString(object.method, `${path}.method`),
    spoolRefIndex: readSafeInteger(
      object.spoolRefIndex,
      `${path}.spoolRefIndex`,
      { minimum: 0 },
    ),
    fileName: readSafeFileName(object.fileName, `${path}.fileName`),
    mediaKind: readEnum(object.mediaKind, `${path}.mediaKind`, [
      "photo",
      "video",
      "audio",
      "document",
      "voice",
    ] as const),
  };
  if (Object.hasOwn(object, "caption")) {
    unit.caption = readText(object.caption, `${path}.caption`);
  }
  if (kind === "voice" && unit.mediaKind !== "voice") {
    fail(`${path}.mediaKind`, "voice units require voice media");
  }
  return unit;
}

function readOutboundPayload(
  value: unknown,
  path: string,
  spoolCount: number,
): RecoveryOutboundPayloadV1 {
  const object = readObject(value, path);
  assertKeys(
    object,
    path,
    [
      "version",
      "intentId",
      "turnId",
      "replyToMessageId",
      "renderingMode",
      "finalMarkdown",
      "renderedChunks",
      "units",
      "buttons",
    ],
    ["voice", "guestQueryId"],
  );
  if (object.version !== 1) fail(`${path}.version`, "expected 1");
  const payload: RecoveryOutboundPayloadV1 = {
    version: 1,
    intentId: readStableString(object.intentId, `${path}.intentId`),
    turnId: readStableString(object.turnId, `${path}.turnId`),
    replyToMessageId: readSafeInteger(
      object.replyToMessageId,
      `${path}.replyToMessageId`,
      { minimum: 1 },
    ),
    renderingMode: readEnum(object.renderingMode, `${path}.renderingMode`, [
      "rich",
      "html",
    ] as const),
    finalMarkdown: readText(object.finalMarkdown, `${path}.finalMarkdown`),
    renderedChunks: readArray(
      object.renderedChunks,
      `${path}.renderedChunks`,
      readText,
    ),
    units: readArray(object.units, `${path}.units`, readOutboundUnit),
    buttons: readArray(object.buttons, `${path}.buttons`, readOutboundButton),
  };
  if (Object.hasOwn(object, "voice")) {
    payload.voice = readOutboundVoiceMetadata(object.voice, `${path}.voice`);
  }
  if (Object.hasOwn(object, "guestQueryId")) {
    payload.guestQueryId = readStableString(
      object.guestQueryId,
      `${path}.guestQueryId`,
    );
  }
  if (payload.units.length === 0) fail(`${path}.units`, "must not be empty");
  assertUniqueStrings(
    payload.units.map((unit) => unit.operationId),
    `${path}.units.operationId`,
  );
  for (let index = 0; index < payload.units.length; index += 1) {
    const unit = payload.units[index]!;
    if ("spoolRefIndex" in unit && unit.spoolRefIndex !== undefined) {
      if (unit.spoolRefIndex >= spoolCount) {
        fail(
          `${path}.units[${index}].spoolRefIndex`,
          "does not reference an outbound spool",
        );
      }
    }
    if (unit.kind === "guest" && payload.guestQueryId === undefined) {
      fail(`${path}.guestQueryId`, "is required by guest units");
    }
  }
  return payload;
}

function parseOutboundPayload(
  bytes: Uint8Array,
  path: string,
  spoolCount: number,
): RecoveryOutboundPayloadV1 {
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(bytes).toString("utf8"));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    fail(path, `invalid JSON: ${message}`);
  }
  return readOutboundPayload(value, path, spoolCount);
}

function readBusRecord(value: unknown, path: string): RecoveryBusRecord {
  const object = readObject(value, path);
  assertKeys(
    object,
    path,
    [...RECORD_BASE_REQUIRED_KEYS, "requestId", "payloadFingerprint", "state"],
    RECORD_BASE_OPTIONAL_KEYS,
  );
  if (object.family !== "bus") fail(`${path}.family`, "expected bus");
  return {
    ...readRecordBase(object, path),
    family: "bus",
    requestId: readStableString(object.requestId, `${path}.requestId`),
    payloadFingerprint: readStableString(
      object.payloadFingerprint,
      `${path}.payloadFingerprint`,
    ),
    state: readEnum(object.state, `${path}.state`, RECOVERY_BUS_STATES),
  };
}

function readReassignment(
  value: unknown,
  path: string,
): RecoveryReassignmentRecord {
  const object = readObject(value, path);
  assertKeys(object, path, [
    "family",
    "reassignmentId",
    "profile",
    "target",
    "oldOwner",
    "newOwner",
    "newSessionGeneration",
    "captureThroughRevision",
    "unresolvedRecordIds",
    "state",
    "createdAtMs",
    "updatedAtMs",
  ]);
  if (object.family !== "reassignment") {
    fail(`${path}.family`, "expected reassignment");
  }
  const createdAtMs = readTimestamp(object.createdAtMs, `${path}.createdAtMs`);
  const updatedAtMs = readTimestamp(object.updatedAtMs, `${path}.updatedAtMs`);
  if (updatedAtMs < createdAtMs) {
    fail(`${path}.updatedAtMs`, "must not precede createdAtMs");
  }
  const unresolvedRecordIds = readArray(
    object.unresolvedRecordIds,
    `${path}.unresolvedRecordIds`,
    readStableString,
  );
  assertUniqueStrings(unresolvedRecordIds, `${path}.unresolvedRecordIds`);
  return {
    family: "reassignment",
    reassignmentId: readStableString(
      object.reassignmentId,
      `${path}.reassignmentId`,
    ),
    profile: readProfile(object.profile, `${path}.profile`),
    target: readTarget(object.target, `${path}.target`),
    oldOwner: readOwner(object.oldOwner, `${path}.oldOwner`),
    newOwner: readOwner(object.newOwner, `${path}.newOwner`),
    newSessionGeneration: readSafeInteger(
      object.newSessionGeneration,
      `${path}.newSessionGeneration`,
      { minimum: 0 },
    ),
    captureThroughRevision: readSafeInteger(
      object.captureThroughRevision,
      `${path}.captureThroughRevision`,
      { minimum: 0 },
    ),
    unresolvedRecordIds,
    state: readEnum(
      object.state,
      `${path}.state`,
      RECOVERY_REASSIGNMENT_STATES,
    ),
    createdAtMs,
    updatedAtMs,
  };
}

function readSameProcessHandoff(
  value: unknown,
  path: string,
): RecoverySameProcessHandoff {
  const object = readObject(value, path);
  assertKeys(
    object,
    path,
    [
      "handoffId",
      "profile",
      "target",
      "owner",
      "fromSessionGeneration",
      "toSessionGeneration",
      "createdAtMs",
      "expiresAtMs",
    ],
    ["consumedAtMs"],
  );
  const createdAtMs = readTimestamp(object.createdAtMs, `${path}.createdAtMs`);
  const expiresAtMs = readTimestamp(object.expiresAtMs, `${path}.expiresAtMs`);
  if (expiresAtMs < createdAtMs) {
    fail(`${path}.expiresAtMs`, "must not precede createdAtMs");
  }
  const fromSessionGeneration = readSafeInteger(
    object.fromSessionGeneration,
    `${path}.fromSessionGeneration`,
    { minimum: 0 },
  );
  const toSessionGeneration = readSafeInteger(
    object.toSessionGeneration,
    `${path}.toSessionGeneration`,
    { minimum: 0 },
  );
  if (toSessionGeneration <= fromSessionGeneration) {
    fail(
      `${path}.toSessionGeneration`,
      "must be newer than fromSessionGeneration",
    );
  }
  const result: RecoverySameProcessHandoff = {
    handoffId: readStableString(object.handoffId, `${path}.handoffId`),
    profile: readProfile(object.profile, `${path}.profile`),
    target: readTarget(object.target, `${path}.target`),
    owner: readOwner(object.owner, `${path}.owner`),
    fromSessionGeneration,
    toSessionGeneration,
    createdAtMs,
    expiresAtMs,
  };
  if (Object.hasOwn(object, "consumedAtMs")) {
    const consumedAtMs = readTimestamp(
      object.consumedAtMs,
      `${path}.consumedAtMs`,
    );
    if (consumedAtMs < createdAtMs || consumedAtMs > expiresAtMs) {
      fail(`${path}.consumedAtMs`, "must fall within the handoff lifetime");
    }
    result.consumedAtMs = consumedAtMs;
  }
  return result;
}

export function validateRecoverySameProcessHandoff(
  value: unknown,
): RecoverySameProcessHandoff {
  return readSameProcessHandoff(value, "$handoff");
}

function readQuota(value: unknown, path: string): RecoveryQuotaAccounting {
  const object = readObject(value, path);
  assertKeys(object, path, [
    "recordBytes",
    "payloadBytes",
    "spoolBytes",
    "reservedBytes",
    "totalBytes",
  ]);
  const result: RecoveryQuotaAccounting = {
    recordBytes: readSafeInteger(object.recordBytes, `${path}.recordBytes`, {
      minimum: 0,
    }),
    payloadBytes: readSafeInteger(object.payloadBytes, `${path}.payloadBytes`, {
      minimum: 0,
    }),
    spoolBytes: readSafeInteger(object.spoolBytes, `${path}.spoolBytes`, {
      minimum: 0,
    }),
    reservedBytes: readSafeInteger(
      object.reservedBytes,
      `${path}.reservedBytes`,
      {
        minimum: 0,
      },
    ),
    totalBytes: readSafeInteger(object.totalBytes, `${path}.totalBytes`, {
      minimum: 0,
    }),
  };
  const computed =
    result.recordBytes +
    result.payloadBytes +
    result.spoolBytes +
    result.reservedBytes;
  if (!Number.isSafeInteger(computed) || result.totalBytes !== computed) {
    fail(`${path}.totalBytes`, "must equal the exact component sum");
  }
  if (result.totalBytes > RECOVERY_PROFILE_QUOTA_BYTES) {
    fail(`${path}.totalBytes`, "exceeds the per-profile quota");
  }
  return result;
}

function assertUniqueStrings(values: readonly string[], path: string): void {
  const seen = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) fail(path, `duplicate id ${value}`);
    seen.add(value);
  }
}

export function areRecoveryTargetsEqual(
  left: TelegramTarget,
  right: TelegramTarget,
): boolean {
  return left.chatId === right.chatId && left.threadId === right.threadId;
}

export function areRecoveryOwnersEqual(
  left: RecoveryOwnerIdentity,
  right: RecoveryOwnerIdentity,
): boolean {
  if (left.kind !== right.kind || left.ownerId !== right.ownerId) return false;
  return left.kind === "leader" && right.kind === "leader"
    ? left.leaderEpoch === right.leaderEpoch
    : left.kind === "manual-follower" && right.kind === "manual-follower"
      ? left.registrationGeneration === right.registrationGeneration
      : false;
}

function isUnresolvedRecord(
  record: RecoveryInboundRecord | RecoveryOutboundRecord | RecoveryBusRecord,
): boolean {
  switch (record.family) {
    case "inbound":
      return (RECOVERY_UNRESOLVED_INBOUND_STATES as readonly string[]).includes(
        record.state,
      );
    case "outbound":
      return (
        RECOVERY_UNRESOLVED_OUTBOUND_STATES as readonly string[]
      ).includes(record.state);
    case "bus":
      return (RECOVERY_UNRESOLVED_BUS_STATES as readonly string[]).includes(
        record.state,
      );
  }
}

function wasUnresolvedAtRevision(
  record: RecoveryInboundRecord | RecoveryOutboundRecord | RecoveryBusRecord,
  revision: number,
): boolean {
  if (record.createdRevision > revision) return false;
  return isUnresolvedRecord(record) || record.stateRevision > revision;
}

function targetTuple(target: TelegramTarget): readonly [number, number | null] {
  return [target.chatId, target.threadId ?? null];
}

function assertStructurallyUniqueRequests(snapshot: RecoverySnapshotV1): void {
  assertUniqueStrings(
    snapshot.outbound.map((record) =>
      JSON.stringify([
        record.identity.profile,
        ...targetTuple(record.identity.target),
        record.intentId,
        record.turnId,
      ]),
    ),
    "$.outbound.intentId",
  );
  assertUniqueStrings(
    snapshot.bus.map((record) =>
      JSON.stringify([
        ...targetTuple(record.identity.target),
        record.requestId,
      ]),
    ),
    "$.bus.requestId",
  );
}

function assertSnapshotConsistency(snapshot: RecoverySnapshotV1): void {
  const records = [...snapshot.inbound, ...snapshot.outbound, ...snapshot.bus];
  assertUniqueStrings(
    records.map((record) => record.recordId),
    "$.records",
  );
  assertUniqueStrings(
    snapshot.reassignments.map((record) => record.reassignmentId),
    "$.reassignments",
  );
  assertUniqueStrings(
    snapshot.inbound
      .filter((record) => record.linkedAttemptOf === undefined)
      .map((record) => String(record.updateId)),
    "$.inbound.updateId",
  );
  const inboundById = new Map(
    snapshot.inbound.map((record) => [record.recordId, record] as const),
  );
  assertUniqueStrings(
    snapshot.inbound.flatMap((record) =>
      record.linkedAttemptOf ? [record.linkedAttemptOf] : [],
    ),
    "$.inbound.linkedAttemptOf",
  );
  for (const record of snapshot.inbound) {
    if (!record.linkedAttemptOf) continue;
    const source = inboundById.get(record.linkedAttemptOf);
    if (!source) {
      fail(
        `$.inbound.${record.recordId}.linkedAttemptOf`,
        "must reference an existing inbound record",
      );
    }
    if (source.updateId !== record.updateId) {
      fail(
        `$.inbound.${record.recordId}.updateId`,
        "must match the linked source update id",
      );
    }
  }
  const outboundById = new Map(
    snapshot.outbound.map((record) => [record.recordId, record] as const),
  );
  assertUniqueStrings(
    snapshot.outbound.flatMap((record) =>
      record.linkedAttemptOf ? [record.linkedAttemptOf] : [],
    ),
    "$.outbound.linkedAttemptOf",
  );
  for (const record of snapshot.outbound) {
    const path = `$.outbound.${record.recordId}`;
    const visitedAttempts = new Set<string>();
    let attemptCursor: RecoveryOutboundRecord | undefined = record;
    while (attemptCursor?.linkedAttemptOf) {
      if (visitedAttempts.has(attemptCursor.recordId)) {
        fail(`${path}.linkedAttemptOf`, "must not form a retry cycle");
      }
      visitedAttempts.add(attemptCursor.recordId);
      attemptCursor = outboundById.get(attemptCursor.linkedAttemptOf);
    }
    for (const sourceId of record.sourceInboundRecordIds) {
      const source = inboundById.get(sourceId);
      if (!source) {
        fail(`${path}.sourceInboundRecordIds`, "references an unknown inbound record");
      }
      const sourceIdentityAuthorized =
        identitiesEqual(source.identity, record.identity) ||
        hasSnapshotCommittedGrant(snapshot, source.recordId, record.identity) ||
        (record.linkedAttemptOf !== undefined &&
          hasSnapshotCommittedGrant(
            snapshot,
            record.linkedAttemptOf,
            record.identity,
          ));
      if (source.turnId !== record.turnId || !sourceIdentityAuthorized) {
        fail(`${path}.sourceInboundRecordIds`, "has a mismatched source turn or identity");
      }
      const outboundTerminalDisposition =
        record.state === "delivered" ||
        record.state === "delivery-uncertain" ||
        record.state === "explicitly-discarded";
      if (outboundTerminalDisposition && source.state !== "completed") {
        fail(`${path}.sourceInboundRecordIds`, "terminal outbound work requires completed inbound sources");
      }
      if (
        !outboundTerminalDisposition &&
        source.state !== "dispatching" &&
        !(record.linkedAttemptOf && source.state === "completed")
      ) {
        fail(`${path}.sourceInboundRecordIds`, "pending outbound work requires dispatching inbound sources");
      }
    }
    if (record.linkedAttemptOf) {
      const source = outboundById.get(record.linkedAttemptOf);
      if (!source || source.state !== "delivery-uncertain") {
        fail(`${path}.linkedAttemptOf`, "must reference delivery-uncertain outbound work");
      }
      if (
        source.turnId !== record.turnId ||
        (!identitiesEqual(source.identity, record.identity) &&
          !hasSnapshotCommittedGrant(snapshot, source.recordId, record.identity)) ||
        JSON.stringify(source.sourceInboundRecordIds) !==
          JSON.stringify(record.sourceInboundRecordIds)
      ) {
        fail(`${path}.linkedAttemptOf`, "must preserve the exact source identity and turn");
      }
      if (source.payloadRef || source.spoolRefs.length > 0) {
        fail(`${path}.linkedAttemptOf`, "must own transferred payload and spool references exclusively");
      }
    }
    const linkedRetryExists = snapshot.outbound.some(
      (candidate) => candidate.linkedAttemptOf === record.recordId,
    );
    if (
      record.state !== "delivered" &&
      record.state !== "explicitly-discarded" &&
      !record.payloadRef &&
      !(record.state === "delivery-uncertain" && linkedRetryExists)
    ) {
      fail(`${path}.payloadRef`, "is required for unresolved outbound work");
    }
  }
  assertStructurallyUniqueRequests(snapshot);

  const payloadRefs = records.flatMap((record) =>
    record.payloadRef ? [record.payloadRef] : [],
  );
  const spoolRefs = records.flatMap((record) => record.spoolRefs);
  assertUniqueStrings(
    payloadRefs.map((reference) => reference.payloadId),
    "$.payloadRefs",
  );
  assertUniqueStrings(
    spoolRefs.map((reference) => reference.spoolId),
    "$.spoolRefs",
  );
  const payloadBytes = payloadRefs.reduce(
    (sum, reference) => sum + reference.byteLength,
    0,
  );
  const spoolBytes = spoolRefs.reduce(
    (sum, reference) => sum + reference.byteLength,
    0,
  );
  if (
    !Number.isSafeInteger(payloadBytes) ||
    payloadBytes !== snapshot.quota.payloadBytes
  ) {
    fail("$.quota.payloadBytes", "must equal referenced payload bytes");
  }
  if (
    !Number.isSafeInteger(spoolBytes) ||
    spoolBytes !== snapshot.quota.spoolBytes
  ) {
    fail("$.quota.spoolBytes", "must equal referenced spool bytes");
  }

  for (const record of records) {
    if (record.family === "inbound") {
      if (record.state === "observed") {
        if (record.admissionRevision !== undefined) {
          fail(
            `$.records.${record.recordId}.admissionRevision`,
            "must be absent while the record is only observed",
          );
        }
      } else if (
        record.admissionRevision === undefined ||
        record.admissionRevision < record.createdRevision ||
        record.admissionRevision > record.stateRevision
      ) {
        fail(
          `$.records.${record.recordId}.admissionRevision`,
          "must identify the immutable durable admission revision",
        );
      }
    }
    if (
      record.family === "inbound" &&
      record.terminalReason !== undefined &&
      record.state !== "explicitly-discarded"
    ) {
      fail(
        `$.records.${record.recordId}.terminalReason`,
        "is valid only for an explicitly-discarded terminal disposition",
      );
    }
    if (record.identity.profile !== snapshot.profile) {
      fail(`$.records.${record.recordId}.identity.profile`, "profile mismatch");
    }
    if (record.createdRevision > snapshot.revision) {
      fail(
        `$.records.${record.recordId}.createdRevision`,
        "exceeds snapshot revision",
      );
    }
    if (
      record.stateRevision < record.createdRevision ||
      record.stateRevision > snapshot.revision
    ) {
      fail(
        `$.records.${record.recordId}.stateRevision`,
        "must be between createdRevision and snapshot revision",
      );
    }
    if (record.updatedAtMs > snapshot.writtenAtMs) {
      fail(
        `$.records.${record.recordId}.updatedAtMs`,
        "exceeds snapshot writtenAtMs",
      );
    }
  }
  const claimedRecordIds = new Map<string, string>();
  for (const reassignment of snapshot.reassignments) {
    const path = `$.reassignments.${reassignment.reassignmentId}`;
    if (reassignment.profile !== snapshot.profile) {
      fail(`${path}.profile`, "profile mismatch");
    }
    if (reassignment.captureThroughRevision > snapshot.revision) {
      fail(`${path}.captureThroughRevision`, "exceeds snapshot revision");
    }
    if (reassignment.updatedAtMs > snapshot.writtenAtMs) {
      fail(`${path}.updatedAtMs`, "exceeds snapshot writtenAtMs");
    }
    if (reassignment.state !== "cancelled-before-transfer") {
      for (const recordId of reassignment.unresolvedRecordIds) {
        const previousClaim = claimedRecordIds.get(recordId);
        if (previousClaim) {
          fail(
            `${path}.unresolvedRecordIds`,
            `record ${recordId} is already claimed by ${previousClaim}`,
          );
        }
        claimedRecordIds.set(recordId, reassignment.reassignmentId);
      }
    }
    const expectedIds = records
      .filter(
        (record) =>
          wasUnresolvedAtRevision(
            record,
            reassignment.captureThroughRevision,
          ) &&
          record.identity.profile === reassignment.profile &&
          areRecoveryTargetsEqual(
            record.identity.target,
            reassignment.target,
          ) &&
          areRecoveryOwnersEqual(record.identity.owner, reassignment.oldOwner),
      )
      .map((record) => record.recordId)
      .sort();
    const claimedIds = [...reassignment.unresolvedRecordIds].sort();
    if (
      expectedIds.length !== claimedIds.length ||
      expectedIds.some((recordId, index) => recordId !== claimedIds[index])
    ) {
      fail(
        `${path}.unresolvedRecordIds`,
        "must exactly match unresolved records captured through the declared revision",
      );
    }
  }
}

export function validateRecoverySnapshot(value: unknown): RecoverySnapshotV1 {
  const object = readObject(value, "$");
  assertKeys(object, "$", [
    "version",
    "profile",
    "mode",
    "revision",
    "writtenAtMs",
    "quota",
    "committedUpdateId",
    "inbound",
    "outbound",
    "bus",
    "reassignments",
  ]);
  if (object.version !== RECOVERY_SCHEMA_VERSION) {
    fail("$.version", `expected ${RECOVERY_SCHEMA_VERSION}`);
  }
  const snapshot: RecoverySnapshotV1 = {
    version: RECOVERY_SCHEMA_VERSION,
    profile: readProfile(object.profile, "$.profile"),
    mode: readEnum(object.mode, "$.mode", RECOVERY_STORE_MODES),
    revision: readSafeInteger(object.revision, "$.revision", { minimum: 0 }),
    writtenAtMs: readTimestamp(object.writtenAtMs, "$.writtenAtMs"),
    quota: readQuota(object.quota, "$.quota"),
    committedUpdateId:
      object.committedUpdateId === null
        ? null
        : readSafeInteger(object.committedUpdateId, "$.committedUpdateId", {
            minimum: 0,
          }),
    inbound: readArray(object.inbound, "$.inbound", readInboundRecord),
    outbound: readArray(object.outbound, "$.outbound", readOutboundRecord),
    bus: readArray(object.bus, "$.bus", readBusRecord),
    reassignments: readArray(
      object.reassignments,
      "$.reassignments",
      readReassignment,
    ),
  };
  assertSnapshotConsistency(snapshot);
  return snapshot;
}

export function parseRecoverySnapshot(serialized: string): RecoverySnapshotV1 {
  let value: unknown;
  try {
    value = JSON.parse(serialized);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Invalid recovery snapshot JSON: ${message}`);
  }
  return validateRecoverySnapshot(value);
}

const RECOVERY_SNAPSHOT_FILE_NAME = "snapshot.json";
const RECOVERY_PAYLOAD_DIRECTORY_NAME = "payloads";
const RECOVERY_SPOOL_DIRECTORY_NAME = "spool";
const RECOVERY_MATERIALIZED_DIRECTORY_NAME = "materialized";
const RECOVERY_BINARY_FILE_SUFFIX = ".bin";
const RECOVERY_TEMP_FILE_MARKER = ".tmp-";
const RECOVERY_SAFE_FILE_ID_PATTERN = /^[a-zA-Z0-9_-]+$/;
const RECOVERY_HANDOFF_REGISTRY_KEY = Symbol.for(
  "@ststgc/pi-telegram/recovery-consumed-handoffs-v1",
);

export type RecoveryInboundFaultId =
  | "IN-01"
  | "IN-02"
  | "IN-03"
  | "IN-04"
  | "IN-05"
  | "IN-06"
  | "IN-07"
  | "IN-08"
  | "IN-GROUP-01"
  | "IN-GROUP-02"
  | "IN-GROUP-03"
  | "DOWN-01"
  | "DOWN-02";

export type RecoveryOutboundFaultId =
  | "OUT-02"
  | "OUT-03"
  | "OUT-04"
  | "OUT-05"
  | "OUT-06";

export type RecoveryStorageFaultId =
  | RecoveryInboundFaultId
  | RecoveryOutboundFaultId;

export interface RecoveryFileSystem {
  chmod: typeof chmodSync;
  close: typeof closeSync;
  exists: typeof existsSync;
  fsync: typeof fsyncSync;
  lstat: typeof lstatSync;
  mkdir: typeof mkdirSync;
  open: typeof openSync;
  readFile: typeof readFileSync;
  readDir: typeof readdirSync;
  rename: typeof renameSync;
  remove: typeof rmSync;
  stat: typeof statSync;
  unlink: typeof unlinkSync;
  writeFile: typeof writeFileSync;
}

const NODE_RECOVERY_FILE_SYSTEM: RecoveryFileSystem = {
  chmod: chmodSync,
  close: closeSync,
  exists: existsSync,
  fsync: fsyncSync,
  lstat: lstatSync,
  mkdir: mkdirSync,
  open: openSync,
  readFile: readFileSync,
  readDir: readdirSync,
  rename: renameSync,
  remove: rmSync,
  stat: statSync,
  unlink: unlinkSync,
  writeFile: writeFileSync,
};

export interface RecoveryReassignmentBindingValidation {
  reassignment: RecoveryReassignmentRecord;
  currentIdentity: RecoveryIdentity;
  expectedBinding: "new-owner" | "old-owner-restored";
}

export interface RecoveryStoreOpenOptions {
  profile: string;
  agentDir?: string;
  rootPath?: string;
  now?: () => number;
  randomId?: () => string;
  fs?: Partial<RecoveryFileSystem>;
  fault?: (faultId: RecoveryStorageFaultId) => void;
  /** Deterministic test seam; production remains capped by the frozen 512 MiB limit. */
  quotaBytes?: number;
  isIdentityAuthenticated?: (identity: RecoveryIdentity) => boolean;
  validateReassignmentBinding?: (
    input: RecoveryReassignmentBindingValidation,
  ) => boolean;
  /** Opaque-handle derivation seam used only for deterministic collision tests. */
  actionId?: (stableInput: string) => string;
  /** Deterministic test seams; production remains capped by the frozen limits. */
  terminalMetadataMaxRecords?: number;
  terminalMetadataMaxBytes?: number;
}

export interface RecoveryInboundAdmissionInput {
  recordId: string;
  payload: Uint8Array;
  spool?: readonly Uint8Array[];
}

export interface RecoveryInboundAdmissionProof {
  version: 1;
  updateId: number;
  recordId: string;
  turnId: string;
  profile: string;
  target: TelegramTarget;
  ownerId: string;
  registrationGeneration: string;
  sessionGeneration: number;
  admissionRevision: number;
  disposition: "admitted" | "terminal";
}

export interface RecoveryInboundMaterializationInput extends RecoveryInboundAdmissionInput {
  claim: RecoveryIdentityClaim;
  /** Canonical queue turn shared by every member of one materialized group. */
  turnId?: string;
  /** Exact prior raw-update payload allowed to be replaced after replay drain. */
  previousPayload?: Uint8Array;
}

export interface RecoveryDispatchClaim {
  recordId: string;
  claim: RecoveryIdentityClaim;
}

export interface RecoveryIdentityClaim {
  identity: RecoveryIdentity;
}

export interface RecoveryOutboundClaimInput {
  recordId: string;
  claim: RecoveryIdentityClaim;
}

export interface RecoveryOutboundReceiptInput extends RecoveryOutboundClaimInput {
  attemptId: string;
  operationId: string;
  method: string;
  messageId?: number;
}

export interface RecoveryOutboundFailureInput extends RecoveryOutboundClaimInput {
  attemptId: string;
}

export interface RecoveryOutboundUncertainInput extends RecoveryOutboundFailureInput {
  reason: RecoveryOutboundUncertaintyReason;
}

export interface RecoveryOutboundDrainItem {
  record: RecoveryOutboundRecord;
  payload: Uint8Array;
  spool: Uint8Array[];
}

export interface RecoveryOutboundRetryResult extends RecoveryOutboundDrainItem {
  duplicationWarning: true;
}

export interface RecoveryInboundDrainItem {
  record: RecoveryInboundRecord;
  payload: Uint8Array;
  spool: Uint8Array[];
}

export interface RecoveryInboundRetryResult extends RecoveryInboundDrainItem {
  duplicationWarning: true;
}

export interface RecoveryInboundActionResult {
  actionId: string;
  turnId: string;
  state: RecoveryInboundState;
  payload?: Uint8Array;
  spool: Uint8Array[];
  duplicationWarning?: true;
}

export interface RecoveryStatusItem {
  id: string;
  actionId: string;
  family: "inbound" | "outbound" | "bus";
  state: RecoveryInboundState | RecoveryOutboundState | RecoveryBusState;
  ageMs: number;
  requiredAction: "drain" | "retry-or-discard" | "none";
}

export interface RecoveryMetadataStatus {
  profile: string;
  mode: RecoveryStoreMode;
  admissionEnabled: boolean;
  committedUpdateId: number | null;
  counts: Record<RecoveryInboundState, number>;
  quota: RecoveryQuotaAccounting & { limitBytes: number };
  oldestUnresolvedAgeMs: number | null;
  items: RecoveryStatusItem[];
  incidents: readonly string[];
}

export interface RecoveryDowngradePreflight {
  safe: boolean;
  blockerCount: number;
  blockers: RecoveryStatusItem[];
}

export interface RecoveryReassignmentRequest {
  target: TelegramTarget;
  oldOwner: RecoveryOwnerIdentity;
  newIdentity: RecoveryIdentity;
}

export interface RecoveryOrphanReassignmentCandidate {
  actionId: string;
  unresolvedCount: number;
  oldestAgeMs: number;
  states: string[];
}

export interface RecoveryReassignmentActionResult {
  actionId: string;
  state: RecoveryReassignmentState;
  unresolvedCount: number;
}

export class RecoverySnapshotCommitUnknownError extends Error {
  readonly cause: unknown;

  constructor(cause: unknown) {
    super("Recovery snapshot publication may have committed");
    this.name = "RecoverySnapshotCommitUnknownError";
    this.cause = cause;
  }
}

export class RecoveryQuotaExceededError extends Error {
  readonly requiredBytes: number;
  readonly limitBytes: number;

  constructor(requiredBytes: number, limitBytes: number) {
    super(
      `Recovery admission requires ${requiredBytes} bytes but the profile limit is ${limitBytes}`,
    );
    this.name = "RecoveryQuotaExceededError";
    this.requiredBytes = requiredBytes;
    this.limitBytes = limitBytes;
  }
}

export class RecoveryQuarantineError extends Error {
  readonly quarantinePaths: readonly string[];

  constructor(quarantinePaths: readonly string[]) {
    super(
      "Recovery store is quarantined; explicit compatible restore is required",
    );
    this.name = "RecoveryQuarantineError";
    this.quarantinePaths = quarantinePaths;
  }
}

export function resolveRecoveryStorePath(
  profile: string,
  agentDir?: string,
): string {
  readProfile(profile, "$profile");
  return join(
    resolveTelegramTempDir(agentDir),
    `${RECOVERY_STORE_DIRECTORY_NAME}${getTelegramProfilePathSuffix(profile)}`,
  );
}

function cloneSnapshot(snapshot: RecoverySnapshotV1): RecoverySnapshotV1 {
  return structuredClone(snapshot);
}

function assertSafeFileId(id: string, path: string): void {
  if (!RECOVERY_SAFE_FILE_ID_PATTERN.test(id)) {
    throw new Error(`Invalid recovery file id at ${path}`);
  }
}

function assertSnapshotFileIds(snapshot: RecoverySnapshotV1): void {
  for (const record of [
    ...snapshot.inbound,
    ...snapshot.outbound,
    ...snapshot.bus,
  ]) {
    if (record.payloadRef) {
      assertSafeFileId(
        record.payloadRef.payloadId,
        `${record.recordId}.payloadId`,
      );
    }
    for (const spool of record.spoolRefs) {
      assertSafeFileId(spool.spoolId, `${record.recordId}.spoolId`);
    }
  }
}

function isInboundUnresolved(state: RecoveryInboundState): boolean {
  return (RECOVERY_UNRESOLVED_INBOUND_STATES as readonly string[]).includes(
    state,
  );
}

function isInboundTerminal(state: RecoveryInboundState): boolean {
  return state === "completed" || state === "explicitly-discarded";
}

function identitiesEqual(
  left: RecoveryIdentity,
  right: RecoveryIdentity,
): boolean {
  return (
    left.profile === right.profile &&
    left.sessionGeneration === right.sessionGeneration &&
    areRecoveryTargetsEqual(left.target, right.target) &&
    areRecoveryOwnersEqual(left.owner, right.owner)
  );
}

function hasSnapshotCommittedGrant(
  snapshot: RecoverySnapshotV1,
  recordId: string,
  identity: RecoveryIdentity,
): boolean {
  return snapshot.reassignments.some(
    (entry) =>
      entry.state === "recovery-grant-committed" &&
      entry.unresolvedRecordIds.includes(recordId) &&
      entry.profile === identity.profile &&
      areRecoveryTargetsEqual(entry.target, identity.target) &&
      areRecoveryOwnersEqual(entry.newOwner, identity.owner) &&
      entry.newSessionGeneration === identity.sessionGeneration,
  );
}

function ownerTargetEqual(
  identity: RecoveryIdentity,
  profile: string,
  target: TelegramTarget,
  owner: RecoveryOwnerIdentity,
): boolean {
  return (
    identity.profile === profile &&
    areRecoveryTargetsEqual(identity.target, target) &&
    areRecoveryOwnersEqual(identity.owner, owner)
  );
}

function getConsumedHandoffRegistry(): Set<string> {
  const globals = globalThis as Record<PropertyKey, unknown>;
  const existing = globals[RECOVERY_HANDOFF_REGISTRY_KEY];
  if (existing instanceof Set) return existing as Set<string>;
  const created = new Set<string>();
  globals[RECOVERY_HANDOFF_REGISTRY_KEY] = created;
  return created;
}

function emptyCounts(): Record<RecoveryInboundState, number> {
  return {
    observed: 0,
    admitted: 0,
    "pre-dispatch": 0,
    dispatching: 0,
    completed: 0,
    "execution-uncertain": 0,
    "explicitly-discarded": 0,
  };
}

function sha256Bytes(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function stableRedactedId(stableInput: string): string {
  return createHash("sha256").update(stableInput).digest("hex").slice(0, 12);
}

function createInitialSnapshot(
  profile: string,
  nowMs: number,
): RecoverySnapshotV1 {
  return {
    version: RECOVERY_SCHEMA_VERSION,
    profile,
    mode: "active",
    revision: 0,
    writtenAtMs: nowMs,
    quota: {
      recordBytes: 0,
      payloadBytes: 0,
      spoolBytes: 0,
      reservedBytes: 0,
      totalBytes: 0,
    },
    committedUpdateId: null,
    inbound: [],
    outbound: [],
    bus: [],
    reassignments: [],
  };
}

function getQuarantineProfilePrefix(profile: string): string {
  return `${RECOVERY_STORE_QUARANTINE_PREFIX}${profile.length}-${profile}-`;
}

function listQuarantines(
  fs: RecoveryFileSystem,
  rootPath: string,
  profile: string,
): string[] {
  const parent = dirname(rootPath);
  if (!fs.exists(parent)) return [];
  const profilePrefix = getQuarantineProfilePrefix(profile);
  return fs
    .readDir(parent)
    .filter((name) => {
      if (!name.startsWith(profilePrefix)) return false;
      const suffix = name.slice(profilePrefix.length);
      if (!/^\d+-[a-zA-Z0-9_-]+$/u.test(suffix)) return false;
      return fs.lstat(join(parent, name)).isDirectory();
    })
    .map((name) => join(parent, name))
    .sort();
}

function fsyncPath(fs: RecoveryFileSystem, path: string): void {
  const descriptor = fs.open(path, "r");
  try {
    fs.fsync(descriptor);
  } finally {
    fs.close(descriptor);
  }
}

function physicalFileBytes(
  fs: RecoveryFileSystem,
  directory: string,
): number {
  let total = 0;
  for (const name of fs.readDir(directory)) {
    const path = join(directory, name);
    const stat = fs.lstat(path);
    if (stat.isDirectory()) {
      total += physicalFileBytes(fs, path);
    } else if (stat.isFile()) {
      total += fs.stat(path).size;
    } else {
      throw new Error(`Unsupported recovery filesystem entry: ${path}`);
    }
    if (!Number.isSafeInteger(total)) {
      throw new Error("Recovery physical byte accounting overflow");
    }
  }
  return total;
}

function writePrivateFile(
  fs: RecoveryFileSystem,
  path: string,
  content: Uint8Array | string,
): void {
  const descriptor = fs.open(path, "wx", RECOVERY_STORE_FILE_MODE);
  try {
    fs.writeFile(descriptor, content);
    fs.fsync(descriptor);
  } finally {
    fs.close(descriptor);
  }
  fs.chmod(path, RECOVERY_STORE_FILE_MODE);
}

function referencedBytes(snapshot: RecoverySnapshotV1): {
  payloadBytes: number;
  spoolBytes: number;
} {
  const records = [...snapshot.inbound, ...snapshot.outbound, ...snapshot.bus];
  return {
    payloadBytes: records.reduce(
      (total, record) => total + (record.payloadRef?.byteLength ?? 0),
      0,
    ),
    spoolBytes: records.reduce(
      (total, record) =>
        total + record.spoolRefs.reduce((sum, ref) => sum + ref.byteLength, 0),
      0,
    ),
  };
}

function serializeAccountedSnapshot(snapshot: RecoverySnapshotV1): string {
  const bytes = referencedBytes(snapshot);
  snapshot.quota.payloadBytes = bytes.payloadBytes;
  snapshot.quota.spoolBytes = bytes.spoolBytes;
  if (
    !Number.isSafeInteger(snapshot.quota.reservedBytes) ||
    snapshot.quota.reservedBytes < 0
  ) {
    throw new Error("Recovery reserved byte accounting is invalid");
  }
  let serialized = "";
  let recordBytes = -1;
  for (let attempt = 0; attempt < 12; attempt += 1) {
    snapshot.quota.recordBytes = Math.max(0, recordBytes);
    snapshot.quota.totalBytes =
      snapshot.quota.recordBytes +
      bytes.payloadBytes +
      bytes.spoolBytes +
      snapshot.quota.reservedBytes;
    serialized = `${JSON.stringify(snapshot, null, 2)}\n`;
    const next = Buffer.byteLength(serialized);
    if (next === snapshot.quota.recordBytes) {
      validateRecoverySnapshot(snapshot);
      return serialized;
    }
    recordBytes = next;
  }
  throw new Error("Recovery snapshot byte accounting did not converge");
}

type RecoveryRecord =
  RecoveryInboundRecord | RecoveryOutboundRecord | RecoveryBusRecord;

function isTerminalRecord(record: RecoveryRecord): boolean {
  switch (record.family) {
    case "inbound":
      return isInboundTerminal(record.state);
    case "outbound":
      return (
        record.state === "delivered" || record.state === "explicitly-discarded"
      );
    case "bus":
      return (
        record.state === "completed" || record.state === "explicitly-discarded"
      );
  }
}

function isSuccessfulTerminalRecord(record: RecoveryRecord): boolean {
  return (
    (record.family === "inbound" && record.state === "completed") ||
    (record.family === "outbound" && record.state === "delivered") ||
    (record.family === "bus" && record.state === "completed")
  );
}

function terminalMetadataBytes(record: RecoveryRecord): number {
  const serialized = JSON.stringify(record, null, 2);
  const lineCount = serialized.split("\n").length;
  // Records are nested inside a pretty-printed snapshot array. Account for the
  // additional four-space base indent on every line plus comma/newline framing.
  return Buffer.byteLength(serialized) + lineCount * 4 + 2;
}

function makeBinaryPath(directory: string, id: string): string {
  assertSafeFileId(id, "binary reference");
  return join(directory, `${id}${RECOVERY_BINARY_FILE_SUFFIX}`);
}

function validateRecoveryStoreBackup(
  fs: RecoveryFileSystem,
  rootPath: string,
  profile: string,
): RecoverySnapshotV1 {
  const payloadDirectory = join(rootPath, RECOVERY_PAYLOAD_DIRECTORY_NAME);
  const spoolDirectory = join(rootPath, RECOVERY_SPOOL_DIRECTORY_NAME);
  const materializedDirectory = join(
    rootPath,
    RECOVERY_MATERIALIZED_DIRECTORY_NAME,
  );
  const snapshotPath = join(rootPath, RECOVERY_SNAPSHOT_FILE_NAME);
  for (const directory of [
    rootPath,
    payloadDirectory,
    spoolDirectory,
    materializedDirectory,
  ]) {
    if (!fs.exists(directory) || !fs.lstat(directory).isDirectory()) {
      throw new Error(`Invalid recovery quarantine directory: ${directory}`);
    }
  }
  if (!fs.exists(snapshotPath) || !fs.lstat(snapshotPath).isFile()) {
    throw new Error("Recovery quarantine snapshot is missing");
  }
  const serialized = fs.readFile(snapshotPath, "utf8") as string;
  const snapshot = parseRecoverySnapshot(serialized);
  if (snapshot.profile !== profile || snapshot.mode !== "downgrade-exclusive") {
    throw new Error("Recovery quarantine is incompatible with this profile");
  }
  if (
    snapshot.quota.recordBytes !== Buffer.byteLength(serialized) ||
    snapshot.quota.totalBytes > RECOVERY_PROFILE_QUOTA_BYTES
  ) {
    throw new Error("Recovery quarantine byte accounting mismatch");
  }
  assertSnapshotFileIds(snapshot);
  const expectedRootEntries = new Set([
    RECOVERY_SNAPSHOT_FILE_NAME,
    RECOVERY_PAYLOAD_DIRECTORY_NAME,
    RECOVERY_SPOOL_DIRECTORY_NAME,
    RECOVERY_MATERIALIZED_DIRECTORY_NAME,
  ]);
  const rootEntries = fs.readDir(rootPath);
  if (
    rootEntries.length !== expectedRootEntries.size ||
    rootEntries.some((name) => !expectedRootEntries.has(name))
  ) {
    throw new Error("Recovery quarantine contains unexpected root entries");
  }
  const expectedPayloadNames = new Set<string>();
  const expectedSpoolNames = new Set<string>();
  let physicalBinaryBytes = 0;
  for (const record of [
    ...snapshot.inbound,
    ...snapshot.outbound,
    ...snapshot.bus,
  ]) {
    if (record.payloadRef) {
      expectedPayloadNames.add(
        `${record.payloadRef.payloadId}${RECOVERY_BINARY_FILE_SUFFIX}`,
      );
      physicalBinaryBytes += record.payloadRef.byteLength;
    }
    for (const spool of record.spoolRefs) {
      expectedSpoolNames.add(`${spool.spoolId}${RECOVERY_BINARY_FILE_SUFFIX}`);
      physicalBinaryBytes += spool.byteLength;
    }
    const references: Array<{
      path: string;
      byteLength: number;
      sha256: string;
    }> = [
      ...(record.payloadRef
        ? [
            {
              path: makeBinaryPath(
                payloadDirectory,
                record.payloadRef.payloadId,
              ),
              byteLength: record.payloadRef.byteLength,
              sha256: record.payloadRef.sha256,
            },
          ]
        : []),
      ...record.spoolRefs.map((reference) => ({
        path: makeBinaryPath(spoolDirectory, reference.spoolId),
        byteLength: reference.byteLength,
        sha256: reference.sha256,
      })),
    ];
    for (const reference of references) {
      if (
        !fs.exists(reference.path) ||
        !fs.lstat(reference.path).isFile() ||
        fs.stat(reference.path).size !== reference.byteLength
      ) {
        throw new Error("Recovery quarantine binary is missing or truncated");
      }
      const bytes = fs.readFile(reference.path) as Buffer;
      if (sha256Bytes(bytes) !== reference.sha256) {
        throw new Error("Recovery quarantine binary digest mismatch");
      }
    }
  }
  for (const [directory, expectedNames] of [
    [payloadDirectory, expectedPayloadNames],
    [spoolDirectory, expectedSpoolNames],
  ] as const) {
    const names = fs.readDir(directory);
    if (
      names.length !== expectedNames.size ||
      names.some((name) => !expectedNames.has(name))
    ) {
      throw new Error("Recovery quarantine contains unexpected binary files");
    }
  }
  if (fs.readDir(materializedDirectory).length !== 0) {
    throw new Error("Recovery quarantine contains materialized cache files");
  }
  const actualPhysicalBytes = Buffer.byteLength(serialized) + physicalBinaryBytes;
  if (
    snapshot.quota.reservedBytes !== 0 ||
    snapshot.quota.totalBytes !== actualPhysicalBytes
  ) {
    throw new Error("Recovery quarantine physical byte accounting mismatch");
  }
  for (const directory of [
    rootPath,
    payloadDirectory,
    spoolDirectory,
    materializedDirectory,
  ]) {
    fs.chmod(directory, RECOVERY_STORE_DIRECTORY_MODE);
  }
  fs.chmod(snapshotPath, RECOVERY_STORE_FILE_MODE);
  for (const directory of [payloadDirectory, spoolDirectory]) {
    for (const name of fs.readDir(directory)) {
      const path = join(directory, name);
      if (fs.lstat(path).isFile()) fs.chmod(path, RECOVERY_STORE_FILE_MODE);
    }
  }
  return snapshot;
}

export class RecoveryStore {
  readonly profile: string;
  readonly rootPath: string;
  readonly snapshotPath: string;
  readonly payloadDirectory: string;
  readonly spoolDirectory: string;
  readonly materializedDirectory: string;
  readonly quotaBytes: number;

  readonly #fs: RecoveryFileSystem;
  readonly #now: () => number;
  readonly #randomId: () => string;
  readonly #fault?: (faultId: RecoveryStorageFaultId) => void;
  readonly #isIdentityAuthenticated?: (identity: RecoveryIdentity) => boolean;
  readonly #validateReassignmentBinding?: (
    input: RecoveryReassignmentBindingValidation,
  ) => boolean;
  readonly #actionId: (stableInput: string) => string;
  readonly #terminalMetadataMaxRecords: number;
  readonly #terminalMetadataMaxBytes: number;
  readonly #incidents: string[] = [];
  #admissionDisabled = false;

  constructor(options: RecoveryStoreOpenOptions) {
    this.profile = readProfile(options.profile, "$profile");
    this.rootPath =
      options.rootPath ??
      resolveRecoveryStorePath(this.profile, options.agentDir);
    this.snapshotPath = join(this.rootPath, RECOVERY_SNAPSHOT_FILE_NAME);
    this.payloadDirectory = join(
      this.rootPath,
      RECOVERY_PAYLOAD_DIRECTORY_NAME,
    );
    this.spoolDirectory = join(this.rootPath, RECOVERY_SPOOL_DIRECTORY_NAME);
    this.materializedDirectory = join(
      this.rootPath,
      RECOVERY_MATERIALIZED_DIRECTORY_NAME,
    );
    this.quotaBytes = options.quotaBytes ?? RECOVERY_PROFILE_QUOTA_BYTES;
    if (
      !Number.isSafeInteger(this.quotaBytes) ||
      this.quotaBytes <= 0 ||
      this.quotaBytes > RECOVERY_PROFILE_QUOTA_BYTES
    ) {
      throw new Error("Invalid recovery quota limit");
    }
    this.#fs = { ...NODE_RECOVERY_FILE_SYSTEM, ...options.fs };
    this.#now = options.now ?? Date.now;
    this.#randomId = options.randomId ?? randomUUID;
    this.#fault = options.fault;
    this.#isIdentityAuthenticated = options.isIdentityAuthenticated;
    this.#validateReassignmentBinding = options.validateReassignmentBinding;
    this.#actionId = options.actionId ?? stableRedactedId;
    this.#terminalMetadataMaxRecords =
      options.terminalMetadataMaxRecords ??
      RECOVERY_TERMINAL_METADATA_MAX_RECORDS;
    this.#terminalMetadataMaxBytes =
      options.terminalMetadataMaxBytes ?? RECOVERY_TERMINAL_METADATA_MAX_BYTES;
    if (
      !Number.isSafeInteger(this.#terminalMetadataMaxRecords) ||
      this.#terminalMetadataMaxRecords <= 0 ||
      this.#terminalMetadataMaxRecords >
        RECOVERY_TERMINAL_METADATA_MAX_RECORDS ||
      !Number.isSafeInteger(this.#terminalMetadataMaxBytes) ||
      this.#terminalMetadataMaxBytes <= 0 ||
      this.#terminalMetadataMaxBytes > RECOVERY_TERMINAL_METADATA_MAX_BYTES
    ) {
      throw new Error("Invalid recovery terminal metadata limit");
    }
  }

  initialize(): this {
    const parent = dirname(this.rootPath);
    this.#fs.mkdir(parent, {
      recursive: true,
      mode: RECOVERY_STORE_DIRECTORY_MODE,
    });
    this.#fs.chmod(parent, RECOVERY_STORE_DIRECTORY_MODE);
    withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      if (!this.#fs.exists(this.rootPath)) {
        const quarantines = listQuarantines(
          this.#fs,
          this.rootPath,
          this.profile,
        );
        if (quarantines.length > 0)
          throw new RecoveryQuarantineError(quarantines);
        this.#fs.mkdir(this.rootPath, {
          mode: RECOVERY_STORE_DIRECTORY_MODE,
        });
        this.#fs.mkdir(this.payloadDirectory, {
          mode: RECOVERY_STORE_DIRECTORY_MODE,
        });
        this.#fs.mkdir(this.spoolDirectory, {
          mode: RECOVERY_STORE_DIRECTORY_MODE,
        });
        this.#fs.mkdir(this.materializedDirectory, {
          mode: RECOVERY_STORE_DIRECTORY_MODE,
        });
        this.#fs.chmod(this.rootPath, RECOVERY_STORE_DIRECTORY_MODE);
        this.#fs.chmod(this.payloadDirectory, RECOVERY_STORE_DIRECTORY_MODE);
        this.#fs.chmod(this.spoolDirectory, RECOVERY_STORE_DIRECTORY_MODE);
        this.#fs.chmod(
          this.materializedDirectory,
          RECOVERY_STORE_DIRECTORY_MODE,
        );
        const initial = createInitialSnapshot(this.profile, this.#now());
        this.#writeSnapshotLocked(initial);
        fsyncPath(this.#fs, parent);
      }
      if (!this.#fs.exists(this.materializedDirectory)) {
        this.#fs.mkdir(this.materializedDirectory, {
          mode: RECOVERY_STORE_DIRECTORY_MODE,
        });
        fsyncPath(this.#fs, this.rootPath);
      }
      this.#assertPrivateStoreShape();
      let snapshot = this.#readSnapshotLocked();
      const outboundSourceIds = new Set(
        snapshot.outbound.flatMap((record) => record.sourceInboundRecordIds),
      );
      const dispatching = snapshot.inbound.filter(
        (record) =>
          record.state === "dispatching" &&
          !outboundSourceIds.has(record.recordId),
      );
      const sendingOutbound = snapshot.outbound.filter(
        (record) => record.state === "sending",
      );
      if (dispatching.length > 0 || sendingOutbound.length > 0) {
        snapshot = cloneSnapshot(snapshot);
        const revision = snapshot.revision + 1;
        const nowMs = this.#now();
        const deleteAfterCommit: string[] = [];
        for (const record of snapshot.inbound) {
          if (
            record.state !== "dispatching" ||
            outboundSourceIds.has(record.recordId)
          ) continue;
          record.state = "execution-uncertain";
          record.stateRevision = revision;
          record.updatedAtMs = nowMs;
        }
        for (const outbound of snapshot.outbound) {
          if (outbound.state !== "sending" || !outbound.activeUnit) continue;
          const unitIndex = outbound.activeUnit.unitIndex;
          outbound.state = "delivery-uncertain";
          outbound.uncertainty = {
            unitIndex,
            reason: "process-reopened-sending",
            observedAtMs: nowMs,
          };
          delete outbound.activeUnit;
          delete outbound.retryNotBeforeMs;
          outbound.stateRevision = revision;
          outbound.updatedAtMs = nowMs;
          for (const recordId of outbound.sourceInboundRecordIds) {
            const inbound = this.#getInbound(snapshot, recordId);
            if (inbound.state === "completed") continue;
            if (
              inbound.state !== "dispatching" ||
              !identitiesEqual(inbound.identity, outbound.identity)
            ) {
              throw new Error(
                "Reopened outbound source identity/state mismatch",
              );
            }
            for (const spool of inbound.spoolRefs) {
              deleteAfterCommit.push(
                makeBinaryPath(this.spoolDirectory, spool.spoolId),
              );
            }
            deleteAfterCommit.push(
              join(this.materializedDirectory, inbound.recordId),
            );
            inbound.spoolRefs = [];
            inbound.state = "completed";
            inbound.stateRevision = revision;
            inbound.updatedAtMs = nowMs;
          }
        }
        snapshot.revision = revision;
        snapshot.writtenAtMs = nowMs;
        this.#writeSnapshotLocked(snapshot);
        for (const path of deleteAfterCommit) {
          try {
            this.#fs.remove(path, { force: true, recursive: true });
            fsyncPath(this.#fs, dirname(path));
          } catch {
            this.#incidents.push("post-reopen-cleanup-failed");
          }
        }
        if (dispatching.length > 0) {
          this.#incidents.push("orphaned-dispatch-marked-uncertain");
        }
        if (sendingOutbound.length > 0) {
          this.#incidents.push("outbound-sending-marked-uncertain");
        }
      }
      this.#admissionDisabled = snapshot.mode !== "active";
      this.#reconcileFilesLocked(snapshot);
    });
    return this;
  }

  #assertPrivateStoreShape(): void {
    for (const directory of [
      this.rootPath,
      this.payloadDirectory,
      this.spoolDirectory,
      this.materializedDirectory,
    ]) {
      if (
        !this.#fs.exists(directory) ||
        !this.#fs.lstat(directory).isDirectory()
      ) {
        throw new Error(`Invalid recovery directory: ${directory}`);
      }
      this.#fs.chmod(directory, RECOVERY_STORE_DIRECTORY_MODE);
    }
    if (!this.#fs.exists(this.snapshotPath)) {
      throw new Error(`Missing recovery snapshot: ${this.snapshotPath}`);
    }
    if (!this.#fs.lstat(this.snapshotPath).isFile()) {
      throw new Error(`Invalid recovery snapshot file: ${this.snapshotPath}`);
    }
    this.#fs.chmod(this.snapshotPath, RECOVERY_STORE_FILE_MODE);
  }

  #readSnapshotLocked(): RecoverySnapshotV1 {
    if (
      !this.#fs.exists(this.rootPath) ||
      !this.#fs.exists(this.snapshotPath)
    ) {
      this.#admissionDisabled = true;
      throw new Error("Recovery store is closed or quarantined");
    }
    const serialized = this.#fs.readFile(this.snapshotPath, "utf8") as string;
    const snapshot = parseRecoverySnapshot(serialized);
    if (snapshot.profile !== this.profile) {
      throw new Error("Recovery snapshot profile mismatch");
    }
    assertSnapshotFileIds(snapshot);
    const actualBytes = Buffer.byteLength(serialized);
    if (snapshot.quota.recordBytes !== actualBytes) {
      throw new Error("Recovery snapshot byte accounting mismatch");
    }
    if (snapshot.quota.totalBytes > this.quotaBytes) {
      throw new RecoveryQuotaExceededError(
        snapshot.quota.totalBytes,
        this.quotaBytes,
      );
    }
    return snapshot;
  }

  #readSnapshotForMutationLocked(): RecoverySnapshotV1 {
    const snapshot = this.#readSnapshotLocked();
    this.#reconcileFilesLocked(snapshot);
    return this.#readSnapshotLocked();
  }

  #writeSnapshotLocked(snapshot: RecoverySnapshotV1): void {
    const serialized = serializeAccountedSnapshot(snapshot);
    const publicationPeakBytes =
      physicalFileBytes(this.#fs, this.rootPath) +
      Buffer.byteLength(serialized);
    if (
      snapshot.quota.totalBytes > this.quotaBytes ||
      publicationPeakBytes > this.quotaBytes
    ) {
      throw new RecoveryQuotaExceededError(
        Math.max(snapshot.quota.totalBytes, publicationPeakBytes),
        this.quotaBytes,
      );
    }
    const tempPath = `${this.snapshotPath}${RECOVERY_TEMP_FILE_MARKER}${process.pid}-${this.#randomId()}`;
    let published = false;
    try {
      writePrivateFile(this.#fs, tempPath, serialized);
      this.#fs.rename(tempPath, this.snapshotPath);
      published = true;
      this.#fs.chmod(this.snapshotPath, RECOVERY_STORE_FILE_MODE);
      fsyncPath(this.#fs, this.rootPath);
    } catch (error) {
      if (published) throw new RecoverySnapshotCommitUnknownError(error);
      throw error;
    } finally {
      if (this.#fs.exists(tempPath)) this.#fs.unlink(tempPath);
    }
  }

  #reconcileFilesLocked(snapshot: RecoverySnapshotV1): void {
    let rootChanged = false;
    for (const name of this.#fs.readDir(this.rootPath)) {
      if (
        name === RECOVERY_SNAPSHOT_FILE_NAME ||
        name === RECOVERY_PAYLOAD_DIRECTORY_NAME ||
        name === RECOVERY_SPOOL_DIRECTORY_NAME ||
        name === RECOVERY_MATERIALIZED_DIRECTORY_NAME
      ) {
        continue;
      }
      const path = join(this.rootPath, name);
      if (
        name.startsWith(
          `${RECOVERY_SNAPSHOT_FILE_NAME}${RECOVERY_TEMP_FILE_MARKER}`,
        )
      ) {
        this.#fs.remove(path, { force: true, recursive: true });
        rootChanged = true;
        this.#incidents.push("orphan-snapshot-tail-removed");
        continue;
      }
      throw new Error(`Unexpected recovery store entry: ${path}`);
    }
    if (rootChanged) fsyncPath(this.#fs, this.rootPath);
    const expectedPayloads = new Set<string>();
    const expectedSpools = new Set<string>();
    for (const record of [
      ...snapshot.inbound,
      ...snapshot.outbound,
      ...snapshot.bus,
    ]) {
      if (record.payloadRef) {
        expectedPayloads.add(
          `${record.payloadRef.payloadId}${RECOVERY_BINARY_FILE_SUFFIX}`,
        );
      }
      for (const spool of record.spoolRefs) {
        expectedSpools.add(`${spool.spoolId}${RECOVERY_BINARY_FILE_SUFFIX}`);
      }
    }
    for (const [directory, expected] of [
      [this.payloadDirectory, expectedPayloads],
      [this.spoolDirectory, expectedSpools],
    ] as const) {
      let directoryChanged = false;
      for (const name of this.#fs.readDir(directory)) {
        const path = join(directory, name);
        if (name.includes(RECOVERY_TEMP_FILE_MARKER) || !expected.has(name)) {
          this.#fs.remove(path, { force: true, recursive: true });
          directoryChanged = true;
          this.#incidents.push("orphan-recovery-file-removed");
          continue;
        }
        if (!this.#fs.lstat(path).isFile()) {
          throw new Error(`Invalid recovery binary file: ${path}`);
        }
        this.#fs.chmod(path, RECOVERY_STORE_FILE_MODE);
      }
      if (directoryChanged) fsyncPath(this.#fs, directory);
    }
    for (const record of [
      ...snapshot.inbound,
      ...snapshot.outbound,
      ...snapshot.bus,
    ]) {
      if (record.payloadRef) {
        this.#readVerifiedBinary(
          this.payloadDirectory,
          record.payloadRef,
          record.recordId,
        );
      }
      for (const spool of record.spoolRefs) {
        this.#readVerifiedBinary(this.spoolDirectory, spool, record.recordId);
      }
      if (record.family === "outbound" && record.payloadRef) {
        this.#readOutboundPayload(snapshot, record);
      }
    }
    const reservedBytes = this.#reconcileMaterializedFilesLocked(snapshot);
    if (snapshot.quota.reservedBytes !== reservedBytes) {
      snapshot.quota.reservedBytes = reservedBytes;
      this.#writeSnapshotLocked(snapshot);
    }
  }

  #reconcileMaterializedFilesLocked(snapshot: RecoverySnapshotV1): number {
    const liveRecords = new Map(
      snapshot.inbound
        .filter(
          (record) =>
            (record.state === "pre-dispatch" ||
              record.state === "dispatching") &&
            record.spoolRefs.length > 0,
        )
        .map((record) => [record.recordId, record] as const),
    );
    let changed = false;
    let reservedBytes = 0;
    for (const name of this.#fs.readDir(this.materializedDirectory)) {
      const path = join(this.materializedDirectory, name);
      const record = liveRecords.get(name);
      if (
        name.includes(RECOVERY_TEMP_FILE_MARKER) ||
        !RECOVERY_SAFE_FILE_ID_PATTERN.test(name) ||
        !record
      ) {
        this.#fs.remove(path, { force: true, recursive: true });
        changed = true;
        this.#incidents.push("orphan-materialized-cache-removed");
        continue;
      }
      try {
        reservedBytes += this.#validateMaterializedDirectoryLocked(
          path,
          record,
        ).byteLength;
      } catch {
        this.#fs.remove(path, { force: true, recursive: true });
        changed = true;
        this.#incidents.push("corrupt-materialized-cache-removed");
      }
    }
    if (changed) fsyncPath(this.#fs, this.materializedDirectory);
    return reservedBytes;
  }

  #countMaterializedBytesLocked(path = this.materializedDirectory): number {
    let total = 0;
    for (const name of this.#fs.readDir(path)) {
      const child = join(path, name);
      const stat = this.#fs.lstat(child);
      if (stat.isDirectory()) {
        total += this.#countMaterializedBytesLocked(child);
      } else if (stat.isFile()) {
        total += this.#fs.stat(child).size;
      }
      if (!Number.isSafeInteger(total)) {
        throw new Error("Recovery materialized byte accounting overflow");
      }
    }
    return total;
  }

  #refreshMaterializedQuotaFailSoftLocked(
    snapshot: RecoverySnapshotV1,
    incident: string,
  ): void {
    try {
      this.#reconcileFilesLocked(snapshot);
      return;
    } catch {
      this.#incidents.push(incident);
    }
    try {
      const current = this.#readSnapshotLocked();
      const actualBytes = this.#countMaterializedBytesLocked();
      if (current.quota.reservedBytes !== actualBytes) {
        current.quota.reservedBytes = actualBytes;
        this.#writeSnapshotLocked(current);
      }
    } catch {
      this.#incidents.push("materialized-quota-accounting-failed");
    }
  }

  #validateMaterializedDirectoryLocked(
    path: string,
    record: RecoveryInboundRecord,
  ): { paths: string[]; byteLength: number } {
    if (!this.#fs.lstat(path).isDirectory()) {
      throw new Error("Recovery materialized cache is not a directory");
    }
    this.#fs.chmod(path, RECOVERY_STORE_DIRECTORY_MODE);
    const names = this.#fs.readDir(path).sort();
    if (names.length !== record.spoolRefs.length) {
      throw new Error("Recovery materialized cache file count mismatch");
    }
    let byteLength = 0;
    const paths: string[] = [];
    for (let index = 0; index < record.spoolRefs.length; index += 1) {
      const name = names[index];
      const reference = record.spoolRefs[index];
      if (
        !name ||
        !reference ||
        !name.startsWith(`${String(index).padStart(3, "0")}-`)
      ) {
        throw new Error("Recovery materialized cache index mismatch");
      }
      const filePath = join(path, name);
      if (
        !this.#fs.lstat(filePath).isFile() ||
        this.#fs.stat(filePath).size !== reference.byteLength
      ) {
        throw new Error("Recovery materialized cache byte mismatch");
      }
      const bytes = this.#fs.readFile(filePath) as Buffer;
      if (sha256Bytes(bytes) !== reference.sha256) {
        throw new Error("Recovery materialized cache digest mismatch");
      }
      this.#fs.chmod(filePath, RECOVERY_STORE_FILE_MODE);
      byteLength += reference.byteLength;
      paths.push(filePath);
    }
    return { paths, byteLength };
  }

  #verifyRecordBinaries(record: RecoveryRecord): void {
    if (record.payloadRef) {
      this.#readVerifiedBinary(
        this.payloadDirectory,
        record.payloadRef,
        record.recordId,
      );
    }
    for (const spool of record.spoolRefs) {
      this.#readVerifiedBinary(this.spoolDirectory, spool, record.recordId);
    }
  }

  #readOutboundPayload(
    snapshot: RecoverySnapshotV1,
    record: RecoveryOutboundRecord,
  ): { payload: RecoveryOutboundPayloadV1; bytes: Buffer } {
    if (!record.payloadRef) {
      throw new Error(`Recovery outbound record ${record.recordId} has no payload`);
    }
    const bytes = this.#readVerifiedBinary(
      this.payloadDirectory,
      record.payloadRef,
      record.recordId,
    );
    const payload = parseOutboundPayload(
      bytes,
      `$outbound.${record.recordId}.payload`,
      record.state === "delivered"
        ? Number.MAX_SAFE_INTEGER
        : record.spoolRefs.length,
    );
    let payloadOwner = record;
    const visited = new Set<string>();
    while (payloadOwner.linkedAttemptOf) {
      if (visited.has(payloadOwner.recordId)) {
        throw new Error("Recovery linked outbound retry cycle detected");
      }
      visited.add(payloadOwner.recordId);
      const source = snapshot.outbound.find(
        (entry) => entry.recordId === payloadOwner.linkedAttemptOf,
      );
      if (!source) throw new Error("Recovery linked outbound source is missing");
      payloadOwner = source;
    }
    const expectedIntentId = payloadOwner.intentId;
    if (payload.intentId !== expectedIntentId || payload.turnId !== record.turnId) {
      throw new Error("Recovery outbound payload identity mismatch");
    }
    if (record.nextUnitIndex > payload.units.length) {
      throw new Error("Recovery outbound next unit exceeds the delivery plan");
    }
    if (record.state === "delivered" && record.nextUnitIndex !== payload.units.length) {
      throw new Error("Recovery delivered outbound record has incomplete units");
    }
    if (
      record.state !== "delivered" &&
      record.state !== "explicitly-discarded" &&
      record.nextUnitIndex >= payload.units.length
    ) {
      throw new Error("Recovery unresolved outbound record has no next unit");
    }
    for (const receipt of record.receipts) {
      const unit = payload.units[receipt.unitIndex];
      if (
        !unit ||
        unit.operationId !== receipt.operationId ||
        unit.method !== receipt.method
      ) {
        throw new Error("Recovery outbound receipt does not match its delivery unit");
      }
    }
    const usedSpoolIndices = payload.units.flatMap((unit) =>
      "spoolRefIndex" in unit && unit.spoolRefIndex !== undefined
        ? [unit.spoolRefIndex]
        : [],
    );
    const sortedSpoolIndices = [...usedSpoolIndices].sort(
      (left, right) => left - right,
    );
    if (
      new Set(usedSpoolIndices).size !== usedSpoolIndices.length ||
      sortedSpoolIndices.some((value, index) => value !== index) ||
      (record.state !== "delivered" &&
        usedSpoolIndices.length !== record.spoolRefs.length)
    ) {
      throw new Error("Recovery outbound spool references must be used exactly once");
    }
    return { payload, bytes };
  }

  #readVerifiedBinary(
    directory: string,
    reference: RecoveryPayloadReference | RecoverySpoolReference,
    recordId: string,
  ): Buffer {
    const id =
      "payloadId" in reference ? reference.payloadId : reference.spoolId;
    const path = makeBinaryPath(directory, id);
    if (
      !this.#fs.exists(path) ||
      this.#fs.stat(path).size !== reference.byteLength
    ) {
      throw new Error(`Missing or truncated recovery binary for ${recordId}`);
    }
    const bytes = this.#fs.readFile(path) as Buffer;
    if (sha256Bytes(bytes) !== reference.sha256) {
      throw new Error(`Recovery binary digest mismatch for ${recordId}`);
    }
    return bytes;
  }

  #assertActive(snapshot: RecoverySnapshotV1): void {
    if (this.#admissionDisabled || snapshot.mode !== "active") {
      throw new Error("Recovery admission is disabled");
    }
  }

  #assertNotFencedByReassignment(
    snapshot: RecoverySnapshotV1,
    identity: RecoveryIdentity,
  ): void {
    if (
      snapshot.reassignments.some(
        (entry) =>
          entry.state !== "recovery-grant-committed" &&
          entry.state !== "cancelled-before-transfer" &&
          identity.profile === entry.profile &&
          areRecoveryTargetsEqual(identity.target, entry.target),
      )
    ) {
      throw new Error("Recovery inbound admission is fenced by reassignment");
    }
  }

  #authenticate(identity: RecoveryIdentity): void {
    const parsed = readIdentity(identity, "$claim.identity");
    if (parsed.profile !== this.profile) {
      throw new Error("Recovery claim profile mismatch");
    }
    if (!this.#isIdentityAuthenticated?.(parsed)) {
      throw new Error("Recovery identity is not currently authenticated");
    }
  }

  #hasCommittedGrant(
    snapshot: RecoverySnapshotV1,
    record: RecoveryRecord,
    identity: RecoveryIdentity,
  ): boolean {
    return hasSnapshotCommittedGrant(snapshot, record.recordId, identity);
  }

  #assertClaim(
    snapshot: RecoverySnapshotV1,
    record: RecoveryInboundRecord,
    claim: RecoveryIdentityClaim,
  ): void {
    this.#authenticate(claim.identity);
    const blockingReassignment = snapshot.reassignments.some(
      (entry) =>
        entry.unresolvedRecordIds.includes(record.recordId) &&
        entry.state !== "recovery-grant-committed" &&
        entry.state !== "cancelled-before-transfer",
    );
    if (blockingReassignment) {
      throw new Error("Recovery record is fenced by a pending reassignment");
    }
    if (
      !identitiesEqual(record.identity, claim.identity) &&
      !this.#hasCommittedGrant(snapshot, record, claim.identity)
    ) {
      throw new Error("Recovery record identity claim denied");
    }
  }

  #assertOutboundClaim(
    snapshot: RecoverySnapshotV1,
    record: RecoveryOutboundRecord,
    claim: RecoveryIdentityClaim,
  ): void {
    this.#authenticate(claim.identity);
    const blockingReassignment = snapshot.reassignments.some(
      (entry) =>
        entry.unresolvedRecordIds.includes(record.recordId) &&
        entry.state !== "recovery-grant-committed" &&
        entry.state !== "cancelled-before-transfer",
    );
    if (blockingReassignment) {
      throw new Error("Recovery outbound record is fenced by a pending reassignment");
    }
    if (
      !identitiesEqual(record.identity, claim.identity) &&
      !this.#hasCommittedGrant(snapshot, record, claim.identity)
    ) {
      throw new Error("Recovery outbound identity claim denied");
    }
  }

  #commit<T>(
    mutate: (
      snapshot: RecoverySnapshotV1,
      revision: number,
      nowMs: number,
      deleteAfterCommit: string[],
    ) => T,
  ): T {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const current = this.#readSnapshotForMutationLocked();
      const snapshot = cloneSnapshot(current);
      const revision = current.revision + 1;
      const nowMs = this.#now();
      const deleteAfterCommit: string[] = [];
      const result = mutate(snapshot, revision, nowMs, deleteAfterCommit);
      snapshot.revision = revision;
      snapshot.writtenAtMs = nowMs;
      this.#writeSnapshotLocked(snapshot);
      const cleanedDirectories = new Set<string>();
      for (const path of deleteAfterCommit) {
        try {
          this.#fs.remove(path, { force: true, recursive: true });
          cleanedDirectories.add(dirname(path));
        } catch {
          this.#incidents.push("post-commit-cleanup-failed");
        }
      }
      for (const directory of cleanedDirectories) {
        try {
          fsyncPath(this.#fs, directory);
        } catch {
          this.#incidents.push("post-commit-cleanup-fsync-failed");
        }
      }
      if (
        deleteAfterCommit.some(
          (path) => dirname(path) === this.materializedDirectory,
        )
      ) {
        this.#refreshMaterializedQuotaFailSoftLocked(
          snapshot,
          "post-commit-quota-refresh-failed",
        );
      }
      return result;
    });
  }

  observeInbound(
    updateId: number,
    identity: RecoveryIdentity,
  ): RecoveryInboundRecord {
    readSafeInteger(updateId, "$updateId", { minimum: 0 });
    const parsedIdentity = readIdentity(identity, "$identity");
    if (parsedIdentity.profile !== this.profile) {
      throw new Error("Recovery identity profile mismatch");
    }
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const current = this.#readSnapshotForMutationLocked();
      this.#assertActive(current);
      const existing = current.inbound.find(
        (record) => record.updateId === updateId && !record.linkedAttemptOf,
      );
      if (existing) {
        if (!identitiesEqual(existing.identity, parsedIdentity)) {
          throw new Error("Recovery update id collision with another identity");
        }
        this.#verifyRecordBinaries(existing);
        return structuredClone(existing);
      }
      this.#assertNotFencedByReassignment(current, parsedIdentity);
      this.#fault?.("IN-01");
      const nowMs = this.#now();
      const revision = current.revision + 1;
      const snapshot = cloneSnapshot(current);
      const record: RecoveryInboundRecord = {
        family: "inbound",
        recordId: this.#randomId(),
        updateId,
        turnId: this.#randomId(),
        state: "observed",
        identity: parsedIdentity,
        createdRevision: revision,
        stateRevision: revision,
        createdAtMs: nowMs,
        updatedAtMs: nowMs,
        spoolRefs: [],
      };
      snapshot.inbound.push(record);
      snapshot.revision = revision;
      snapshot.writtenAtMs = nowMs;
      this.#writeSnapshotLocked(snapshot);
      return structuredClone(record);
    });
  }

  recordTerminalInboundDisposition(
    updateId: number,
    identity: RecoveryIdentity,
    reason: RecoveryInboundTerminalReason,
  ): RecoveryInboundRecord {
    readSafeInteger(updateId, "$updateId", { minimum: 0 });
    const parsedIdentity = readIdentity(identity, "$identity");
    readEnum(reason, "$reason", RECOVERY_INBOUND_TERMINAL_REASONS);
    if (parsedIdentity.profile !== this.profile) {
      throw new Error("Recovery identity profile mismatch");
    }
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const current = this.#readSnapshotForMutationLocked();
      this.#assertActive(current);
      this.#assertNotFencedByReassignment(current, parsedIdentity);
      const existing = current.inbound.find(
        (record) => record.updateId === updateId && !record.linkedAttemptOf,
      );
      if (existing) {
        if (!identitiesEqual(existing.identity, parsedIdentity)) {
          throw new Error("Recovery update id collision with another identity");
        }
        if (isInboundTerminal(existing.state)) {
          if (existing.terminalReason !== reason) {
            throw new Error("Recovery terminal disposition reason collision");
          }
          this.#verifyRecordBinaries(existing);
          return structuredClone(existing);
        }
        if (existing.state !== "observed") {
          throw new Error(
            "Admitted recovery work cannot become an ignored disposition",
          );
        }
      }
      const nowMs = this.#now();
      const revision = current.revision + 1;
      const snapshot = cloneSnapshot(current);
      const record = existing
        ? this.#getInbound(snapshot, existing.recordId)
        : {
            family: "inbound" as const,
            recordId: this.#randomId(),
            updateId,
            turnId: this.#randomId(),
            state: "explicitly-discarded" as const,
            terminalReason: reason,
            identity: parsedIdentity,
            createdRevision: revision,
            stateRevision: revision,
            createdAtMs: nowMs,
            updatedAtMs: nowMs,
            spoolRefs: [],
          };
      if (existing) {
        record.state = "explicitly-discarded";
        record.admissionRevision = revision;
        record.terminalReason = reason;
        record.stateRevision = revision;
        record.updatedAtMs = nowMs;
      } else {
        record.admissionRevision = revision;
        snapshot.inbound.push(record);
      }
      snapshot.revision = revision;
      snapshot.writtenAtMs = nowMs;
      this.#writeSnapshotLocked(snapshot);
      return structuredClone(record);
    });
  }

  admitInbound(input: RecoveryInboundAdmissionInput): RecoveryInboundRecord {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const current = this.#readSnapshotForMutationLocked();
      this.#assertActive(current);
      const existing = current.inbound.find(
        (record) => record.recordId === input.recordId,
      );
      if (!existing) throw new Error("Unknown recovery inbound record");
      if (existing.state !== "observed") {
        if (isInboundTerminal(existing.state)) {
          this.#verifyRecordBinaries(existing);
          return structuredClone(existing);
        }
        this.#assertAdmissionBytes(existing, input);
        return structuredClone(existing);
      }

      const nowMs = this.#now();
      const revision = current.revision + 1;
      const snapshot = cloneSnapshot(current);
      const record = snapshot.inbound.find(
        (entry) => entry.recordId === input.recordId,
      )!;
      const payloadId = this.#randomId();
      assertSafeFileId(payloadId, "$payloadId");
      const spoolIds = (input.spool ?? []).map(() => {
        const id = this.#randomId();
        assertSafeFileId(id, "$spoolId");
        return id;
      });
      record.payloadRef = {
        payloadId,
        byteLength: input.payload.byteLength,
        sha256: sha256Bytes(input.payload),
      };
      record.spoolRefs = spoolIds.map((spoolId, index) => ({
        spoolId,
        byteLength: input.spool![index]!.byteLength,
        sha256: sha256Bytes(input.spool![index]!),
      }));
      record.state = "admitted";
      record.admissionRevision = revision;
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      snapshot.revision = revision;
      snapshot.writtenAtMs = nowMs;
      const prospective = serializeAccountedSnapshot(snapshot);
      void prospective;
      if (snapshot.quota.totalBytes > this.quotaBytes) {
        throw new RecoveryQuotaExceededError(
          snapshot.quota.totalBytes,
          this.quotaBytes,
        );
      }

      const published: string[] = [];
      const staged: string[] = [];
      let committed = false;
      try {
        const binaries: Array<{
          directory: string;
          id: string;
          bytes: Uint8Array;
        }> = [
          {
            directory: this.payloadDirectory,
            id: payloadId,
            bytes: input.payload,
          },
          ...(input.spool ?? []).map((bytes, index) => ({
            directory: this.spoolDirectory,
            id: spoolIds[index]!,
            bytes,
          })),
        ];
        const publishedDirectories = new Set<string>();
        for (const binary of binaries) {
          const finalPath = makeBinaryPath(binary.directory, binary.id);
          if (this.#fs.exists(finalPath)) {
            throw new Error(`Recovery binary id collision: ${binary.id}`);
          }
          const tempPath = `${finalPath}${RECOVERY_TEMP_FILE_MARKER}${process.pid}-${this.#randomId()}`;
          staged.push(tempPath);
          writePrivateFile(this.#fs, tempPath, binary.bytes);
          this.#fs.rename(tempPath, finalPath);
          staged.pop();
          published.push(finalPath);
          publishedDirectories.add(binary.directory);
        }
        for (const directory of publishedDirectories)
          fsyncPath(this.#fs, directory);
        this.#fault?.("IN-02");
        this.#writeSnapshotLocked(snapshot);
        committed = true;
        this.#fault?.("IN-03");
        return structuredClone(record);
      } catch (error) {
        const injectedCrash =
          (error as { faultId?: unknown })?.faultId === "IN-02";
        if (
          !committed &&
          !injectedCrash &&
          !(error instanceof RecoverySnapshotCommitUnknownError)
        ) {
          const cleanedDirectories = new Set<string>();
          for (const path of [...staged, ...published]) {
            this.#fs.remove(path, { force: true, recursive: true });
            cleanedDirectories.add(dirname(path));
          }
          for (const directory of cleanedDirectories)
            fsyncPath(this.#fs, directory);
        }
        throw error;
      }
    });
  }

  #assertAdmissionBytes(
    record: RecoveryInboundRecord,
    input: RecoveryInboundAdmissionInput,
  ): void {
    if (
      !record.payloadRef ||
      record.spoolRefs.length !== (input.spool ?? []).length
    ) {
      throw new Error("Recovery admission idempotency payload mismatch");
    }
    const payload = this.#readVerifiedBinary(
      this.payloadDirectory,
      record.payloadRef,
      record.recordId,
    );
    if (!payload.equals(Buffer.from(input.payload))) {
      throw new Error("Recovery admission idempotency payload mismatch");
    }
    for (let index = 0; index < record.spoolRefs.length; index += 1) {
      const stored = this.#readVerifiedBinary(
        this.spoolDirectory,
        record.spoolRefs[index]!,
        record.recordId,
      );
      if (!stored.equals(Buffer.from(input.spool![index]!))) {
        throw new Error("Recovery admission idempotency spool mismatch");
      }
    }
  }

  verifyInboundAdmissionProof(
    proof: RecoveryInboundAdmissionProof,
  ): RecoveryInboundRecord {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const snapshot = this.#readSnapshotLocked();
      if (
        proof.version !== 1 ||
        proof.profile !== this.profile ||
        !Number.isSafeInteger(proof.updateId) ||
        proof.updateId < 0 ||
        !Number.isSafeInteger(proof.sessionGeneration) ||
        proof.sessionGeneration < 0 ||
        !Number.isSafeInteger(proof.admissionRevision) ||
        proof.admissionRevision < 0
      ) {
        throw new Error("Invalid recovery inbound admission proof");
      }
      const record = snapshot.inbound.find(
        (entry) =>
          entry.recordId === proof.recordId &&
          entry.updateId === proof.updateId &&
          !entry.linkedAttemptOf,
      );
      if (
        !record ||
        record.turnId !== proof.turnId ||
        record.admissionRevision !== proof.admissionRevision ||
        record.identity.profile !== proof.profile ||
        record.identity.sessionGeneration !== proof.sessionGeneration ||
        !areRecoveryTargetsEqual(record.identity.target, proof.target) ||
        record.identity.owner.kind !== "manual-follower" ||
        record.identity.owner.ownerId !== proof.ownerId ||
        record.identity.owner.registrationGeneration !==
          proof.registrationGeneration
      ) {
        throw new Error("Recovery inbound admission proof identity mismatch");
      }
      const terminal = isInboundTerminal(record.state);
      if (
        (proof.disposition === "terminal" && !terminal) ||
        (proof.disposition === "admitted" && record.state === "observed")
      ) {
        throw new Error("Recovery inbound admission proof disposition mismatch");
      }
      this.#verifyRecordBinaries(record);
      return structuredClone(record);
    });
  }

  commitUpdatePrefix(updateId: number): number {
    readSafeInteger(updateId, "$updateId", { minimum: 0 });
    const result = this.#commit((snapshot) => {
      this.#assertActive(snapshot);
      if (
        snapshot.committedUpdateId !== null &&
        updateId < snapshot.committedUpdateId
      ) {
        throw new Error("Recovery committed prefix cannot move backwards");
      }
      const record = snapshot.inbound.find(
        (entry) => entry.updateId === updateId && !entry.linkedAttemptOf,
      );
      if (!record || record.state === "observed") {
        throw new Error(
          "Recovery prefix requires a durable admitted or terminal record",
        );
      }
      if (
        snapshot.inbound.some(
          (entry) =>
            !entry.linkedAttemptOf &&
            entry.updateId <= updateId &&
            entry.state === "observed",
        )
      ) {
        throw new Error(
          "Recovery prefix cannot cross an observed admission gap",
        );
      }
      snapshot.committedUpdateId = updateId;
      return updateId;
    });
    this.#fault?.("IN-04");
    return result;
  }

  getCommittedUpdateId(): number | null {
    return withTelegramFileTransaction(
      `${this.rootPath}.transaction`,
      () => this.#readSnapshotLocked().committedUpdateId,
    );
  }

  migrateCommittedUpdateId(updateId: number): number {
    readSafeInteger(updateId, "$updateId", { minimum: 0 });
    return this.#commit((snapshot) => {
      this.#assertActive(snapshot);
      if (snapshot.committedUpdateId !== null) {
        return snapshot.committedUpdateId;
      }
      if (snapshot.inbound.length > 0) {
        throw new Error(
          "Recovery offset migration requires an empty inbound store",
        );
      }
      snapshot.committedUpdateId = updateId;
      return updateId;
    });
  }

  materializeInbound(
    input: RecoveryInboundMaterializationInput,
  ): RecoveryInboundRecord {
    return this.materializeInboundGroup([input])[0]!;
  }

  materializeInboundGroup(
    inputs: readonly RecoveryInboundMaterializationInput[],
  ): RecoveryInboundRecord[] {
    if (inputs.length === 0) return [];
    const uniqueIds = new Set(inputs.map((input) => input.recordId));
    if (uniqueIds.size !== inputs.length) {
      throw new Error(
        "Recovery materialization group contains duplicate record ids",
      );
    }
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const current = this.#readSnapshotForMutationLocked();
      this.#assertActive(current);
      const existingRecords = inputs.map((input) => {
        const record = this.#getInbound(current, input.recordId);
        this.#assertClaim(current, record, input.claim);
        return record;
      });
      const exactMaterializedIds = new Set<string>();
      for (let index = 0; index < existingRecords.length; index += 1) {
        const record = existingRecords[index]!;
        const input = inputs[index]!;
        if (record.state === "admitted") continue;
        if (record.state !== "pre-dispatch") {
          throw new Error(
            "Recovery materialization group must be entirely admitted or pre-dispatch",
          );
        }
        try {
          this.#assertAdmissionBytes(record, input);
          exactMaterializedIds.add(record.recordId);
        } catch (mismatch) {
          if (
            !input.previousPayload ||
            !record.payloadRef ||
            record.spoolRefs.length > 0
          ) {
            throw mismatch;
          }
          const prior = this.#readVerifiedBinary(
            this.payloadDirectory,
            record.payloadRef,
            record.recordId,
          );
          if (!prior.equals(Buffer.from(input.previousPayload))) {
            throw mismatch;
          }
        }
      }
      if (
        existingRecords.every((record) => record.state === "pre-dispatch") &&
        exactMaterializedIds.size === existingRecords.length
      ) {
        return existingRecords.map((record) => structuredClone(record));
      }

      const snapshot = cloneSnapshot(current);
      const nowMs = this.#now();
      const revision = snapshot.revision + 1;
      const publications: Array<{
        directory: string;
        id: string;
        bytes: Uint8Array;
      }> = [];
      const oldPaths: string[] = [];
      const records = inputs.map((input) => {
        const record = this.#getInbound(snapshot, input.recordId);
        if (record.payloadRef) {
          if (
            input.previousPayload &&
            !exactMaterializedIds.has(record.recordId)
          ) {
            const prior = this.#readVerifiedBinary(
              this.payloadDirectory,
              record.payloadRef,
              record.recordId,
            );
            if (!prior.equals(Buffer.from(input.previousPayload))) {
              throw new Error(
                "Recovery materialization prior payload mismatch",
              );
            }
          }
          oldPaths.push(
            makeBinaryPath(this.payloadDirectory, record.payloadRef.payloadId),
          );
        }
        for (const spool of record.spoolRefs) {
          oldPaths.push(makeBinaryPath(this.spoolDirectory, spool.spoolId));
        }
        const payloadId = this.#randomId();
        assertSafeFileId(payloadId, "$payloadId");
        const spoolIds = (input.spool ?? []).map(() => {
          const id = this.#randomId();
          assertSafeFileId(id, "$spoolId");
          return id;
        });
        if (input.turnId) record.turnId = input.turnId;
        record.payloadRef = {
          payloadId,
          byteLength: input.payload.byteLength,
          sha256: sha256Bytes(input.payload),
        };
        record.spoolRefs = spoolIds.map((spoolId, index) => ({
          spoolId,
          byteLength: input.spool![index]!.byteLength,
          sha256: sha256Bytes(input.spool![index]!),
        }));
        record.state = "pre-dispatch";
        record.stateRevision = revision;
        record.updatedAtMs = nowMs;
        publications.push({
          directory: this.payloadDirectory,
          id: payloadId,
          bytes: input.payload,
        });
        for (let index = 0; index < spoolIds.length; index += 1) {
          publications.push({
            directory: this.spoolDirectory,
            id: spoolIds[index]!,
            bytes: input.spool![index]!,
          });
        }
        return record;
      });
      snapshot.revision = revision;
      snapshot.writtenAtMs = nowMs;
      const uniqueOldPaths = [...new Set(oldPaths)];
      const oldPathBytes = new Map(
        uniqueOldPaths.map((path) => [
          path,
          this.#fs.exists(path) ? this.#fs.stat(path).size : 0,
        ]),
      );
      const retainedOldBytes = [...oldPathBytes.values()].reduce(
        (total, byteLength) => total + byteLength,
        0,
      );
      snapshot.quota.reservedBytes += retainedOldBytes;
      serializeAccountedSnapshot(snapshot);
      if (snapshot.quota.totalBytes > this.quotaBytes) {
        throw new RecoveryQuotaExceededError(
          snapshot.quota.totalBytes,
          this.quotaBytes,
        );
      }

      const published: string[] = [];
      let committed = false;
      try {
        this.#fault?.("IN-GROUP-01");
        const directories = new Set<string>();
        for (let index = 0; index < publications.length; index += 1) {
          const publication = publications[index]!;
          const path = makeBinaryPath(publication.directory, publication.id);
          if (this.#fs.exists(path)) {
            throw new Error(`Recovery binary id collision: ${publication.id}`);
          }
          writePrivateFile(this.#fs, path, publication.bytes);
          published.push(path);
          directories.add(publication.directory);
          if (index + 1 < publications.length) this.#fault?.("IN-GROUP-02");
        }
        for (const directory of directories) fsyncPath(this.#fs, directory);
        this.#fault?.("IN-GROUP-03");
        this.#writeSnapshotLocked(snapshot);
        committed = true;
        const cleanedDirectories = new Set<string>();
        const retainedCleanupPaths = new Set<string>();
        for (const path of uniqueOldPaths) {
          try {
            this.#fs.remove(path, { force: true, recursive: true });
            cleanedDirectories.add(dirname(path));
          } catch {
            retainedCleanupPaths.add(path);
            this.#incidents.push("post-commit-cleanup-failed");
          }
        }
        for (const directory of cleanedDirectories) {
          try {
            fsyncPath(this.#fs, directory);
          } catch {
            for (const path of uniqueOldPaths) {
              if (dirname(path) === directory) retainedCleanupPaths.add(path);
            }
            this.#incidents.push("post-commit-cleanup-fsync-failed");
          }
        }
        const retainedCleanupBytes = [...retainedCleanupPaths].reduce(
          (total, path) => total + (oldPathBytes.get(path) ?? 0),
          0,
        );
        try {
          snapshot.quota.reservedBytes =
            this.#reconcileMaterializedFilesLocked(snapshot) +
            retainedCleanupBytes;
          this.#writeSnapshotLocked(snapshot);
        } catch {
          this.#incidents.push("post-commit-quota-refresh-failed");
        }
        this.#fault?.("IN-05");
        return records.map((record) => structuredClone(record));
      } catch (error) {
        if (
          !committed &&
          !(error instanceof RecoverySnapshotCommitUnknownError)
        ) {
          const cleanedDirectories = new Set<string>();
          for (const path of published) {
            this.#fs.remove(path, { force: true, recursive: true });
            cleanedDirectories.add(dirname(path));
          }
          for (const directory of cleanedDirectories)
            fsyncPath(this.#fs, directory);
        }
        throw error;
      }
    });
  }

  markPreDispatch(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundRecord {
    const result = this.#transition(
      recordId,
      claim,
      ["admitted"],
      "pre-dispatch",
    );
    this.#fault?.("IN-05");
    return result;
  }

  markDispatching(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundRecord {
    return this.markDispatchingGroup([{ recordId, claim }])[0]!;
  }

  markDispatchingGroup(
    claims: readonly RecoveryDispatchClaim[],
  ): RecoveryInboundRecord[] {
    if (claims.length === 0) return [];
    const uniqueIds = new Set(claims.map((entry) => entry.recordId));
    if (uniqueIds.size !== claims.length) {
      throw new Error("Recovery dispatch group contains duplicate record ids");
    }
    const result = this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const records = claims.map(({ recordId, claim }) => {
        const record = this.#getInbound(snapshot, recordId);
        this.#assertClaim(snapshot, record, claim);
        if (record.state !== "pre-dispatch" && record.state !== "dispatching") {
          throw new Error(
            `Invalid inbound transition ${record.state} -> dispatching`,
          );
        }
        return record;
      });
      for (const record of records) {
        if (record.state === "dispatching") continue;
        record.state = "dispatching";
        record.stateRevision = revision;
        record.updatedAtMs = nowMs;
      }
      return records.map((record) => structuredClone(record));
    });
    this.#fault?.("IN-06");
    return result;
  }

  restoreInboundSpool(
    recordId: string,
    claim: RecoveryIdentityClaim,
    fileNames: readonly string[],
  ): string[] {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const current = this.#readSnapshotForMutationLocked();
      const record = this.#getInbound(current, recordId);
      this.#assertClaim(current, record, claim);
      if (record.state !== "pre-dispatch") {
        throw new Error("Only pre-dispatch inbound spool may be restored");
      }
      if (record.spoolRefs.length !== fileNames.length) {
        throw new Error("Recovery spool mapping length mismatch");
      }
      if (record.spoolRefs.length === 0) return [];
      const finalDirectory = join(this.materializedDirectory, recordId);
      if (this.#fs.exists(finalDirectory)) {
        return this.#validateMaterializedDirectoryLocked(
          finalDirectory,
          record,
        ).paths;
      }

      const projectedBytes = record.spoolRefs.reduce(
        (total, reference) => total + reference.byteLength,
        0,
      );
      const reservation = cloneSnapshot(current);
      reservation.revision += 1;
      reservation.writtenAtMs = this.#now();
      reservation.quota.reservedBytes += projectedBytes;
      this.#writeSnapshotLocked(reservation);

      const tempDirectory = `${finalDirectory}${RECOVERY_TEMP_FILE_MARKER}${process.pid}-${this.#randomId()}`;
      let published = false;
      try {
        this.#fs.remove(tempDirectory, { force: true, recursive: true });
        this.#fs.mkdir(tempDirectory, { mode: RECOVERY_STORE_DIRECTORY_MODE });
        const names = record.spoolRefs.map((reference, index) => {
          const sourceName = fileNames[index];
          if (sourceName === undefined) {
            throw new Error("Recovery spool mapping is incomplete");
          }
          const safeBase =
            basename(sourceName)
              .replace(/[^a-zA-Z0-9._-]+/gu, "_")
              .replace(/^\.+/u, "") || "attachment.bin";
          const fileName = `${String(index).padStart(3, "0")}-${safeBase}`;
          const bytes = this.#readVerifiedBinary(
            this.spoolDirectory,
            reference,
            record.recordId,
          );
          writePrivateFile(this.#fs, join(tempDirectory, fileName), bytes);
          return fileName;
        });
        fsyncPath(this.#fs, tempDirectory);
        this.#fs.rename(tempDirectory, finalDirectory);
        published = true;
        this.#fs.chmod(finalDirectory, RECOVERY_STORE_DIRECTORY_MODE);
        fsyncPath(this.#fs, this.materializedDirectory);
        this.#reconcileFilesLocked(reservation);
        return names.map((fileName) => join(finalDirectory, fileName));
      } catch (error) {
        try {
          this.#fs.remove(tempDirectory, { force: true, recursive: true });
          if (published) {
            this.#fs.remove(finalDirectory, { force: true, recursive: true });
          }
          fsyncPath(this.#fs, this.materializedDirectory);
          this.#refreshMaterializedQuotaFailSoftLocked(
            reservation,
            "materialized-rollback-cleanup-failed",
          );
        } catch {
          this.#incidents.push("materialized-rollback-cleanup-failed");
          this.#refreshMaterializedQuotaFailSoftLocked(
            reservation,
            "materialized-rollback-quota-refresh-failed",
          );
        }
        throw error;
      } finally {
        if (this.#fs.exists(tempDirectory)) {
          try {
            this.#fs.remove(tempDirectory, { force: true, recursive: true });
          } catch {
            this.#incidents.push("materialized-temp-cleanup-failed");
          }
        }
      }
    });
  }

  markExecutionUncertain(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundRecord {
    return this.markExecutionUncertainGroup([{ recordId, claim }])[0]!;
  }

  markExecutionUncertainGroup(
    claims: readonly RecoveryDispatchClaim[],
  ): RecoveryInboundRecord[] {
    const result = this.#transitionGroup(
      claims,
      ["dispatching"],
      "execution-uncertain",
    );
    this.#fault?.("IN-07");
    return result;
  }

  markCompleted(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundRecord {
    return this.markCompletedGroup([{ recordId, claim }])[0]!;
  }

  markCompletedGroup(
    claims: readonly RecoveryDispatchClaim[],
  ): RecoveryInboundRecord[] {
    const result = this.#completeInboundGroup(claims, ["dispatching"]);
    this.#fault?.("IN-08");
    return result;
  }

  terminalizeInboundGroup(
    claims: readonly RecoveryDispatchClaim[],
  ): RecoveryInboundRecord[] {
    return this.#completeInboundGroup(claims, ["admitted", "pre-dispatch"]);
  }

  #completeInboundGroup(
    claims: readonly RecoveryDispatchClaim[],
    from: readonly RecoveryInboundState[],
  ): RecoveryInboundRecord[] {
    if (claims.length === 0) return [];
    const uniqueIds = new Set(claims.map((entry) => entry.recordId));
    if (uniqueIds.size !== claims.length) {
      throw new Error("Recovery completion group contains duplicate record ids");
    }
    return this.#commit((snapshot, revision, nowMs, deleteAfterCommit) => {
      this.#assertActive(snapshot);
      const records = claims.map(({ recordId, claim }) => {
        const record = this.#getInbound(snapshot, recordId);
        this.#assertClaim(snapshot, record, claim);
        if (record.state !== "completed" && !from.includes(record.state)) {
          throw new Error(
            `Invalid inbound transition ${record.state} -> completed`,
          );
        }
        return record;
      });
      for (const record of records) {
        if (record.state === "completed") continue;
        for (const spool of record.spoolRefs) {
          deleteAfterCommit.push(
            makeBinaryPath(this.spoolDirectory, spool.spoolId),
          );
        }
        deleteAfterCommit.push(
          join(this.materializedDirectory, record.recordId),
        );
        record.spoolRefs = [];
        record.state = "completed";
        record.stateRevision = revision;
        record.updatedAtMs = nowMs;
      }
      return records.map((record) => structuredClone(record));
    });
  }

  #transitionGroup(
    claims: readonly RecoveryDispatchClaim[],
    from: readonly RecoveryInboundState[],
    to: RecoveryInboundState,
  ): RecoveryInboundRecord[] {
    if (claims.length === 0) return [];
    const uniqueIds = new Set(claims.map((entry) => entry.recordId));
    if (uniqueIds.size !== claims.length) {
      throw new Error("Recovery transition group contains duplicate record ids");
    }
    return this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const records = claims.map(({ recordId, claim }) => {
        const record = this.#getInbound(snapshot, recordId);
        this.#assertClaim(snapshot, record, claim);
        if (record.state !== to && !from.includes(record.state)) {
          throw new Error(`Invalid inbound transition ${record.state} -> ${to}`);
        }
        return record;
      });
      for (const record of records) {
        if (record.state === to) continue;
        record.state = to;
        record.stateRevision = revision;
        record.updatedAtMs = nowMs;
      }
      return records.map((record) => structuredClone(record));
    });
  }

  #transition(
    recordId: string,
    claim: RecoveryIdentityClaim,
    from: readonly RecoveryInboundState[],
    to: RecoveryInboundState,
  ): RecoveryInboundRecord {
    return this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const record = this.#getInbound(snapshot, recordId);
      this.#assertClaim(snapshot, record, claim);
      if (record.state === to) return structuredClone(record);
      if (!from.includes(record.state)) {
        throw new Error(`Invalid inbound transition ${record.state} -> ${to}`);
      }
      record.state = to;
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      return structuredClone(record);
    });
  }

  discardInbound(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundRecord {
    return this.#commit((snapshot, revision, nowMs, deleteAfterCommit) => {
      this.#assertActive(snapshot);
      const record = this.#getInbound(snapshot, recordId);
      this.#assertClaim(snapshot, record, claim);
      if (record.state === "explicitly-discarded") {
        return structuredClone(record);
      }
      if (record.state === "dispatching" || record.state === "completed") {
        throw new Error(`Cannot discard inbound record in ${record.state}`);
      }
      if (record.payloadRef) {
        deleteAfterCommit.push(
          makeBinaryPath(this.payloadDirectory, record.payloadRef.payloadId),
        );
        delete record.payloadRef;
      }
      for (const spool of record.spoolRefs) {
        deleteAfterCommit.push(
          makeBinaryPath(this.spoolDirectory, spool.spoolId),
        );
      }
      deleteAfterCommit.push(join(this.materializedDirectory, record.recordId));
      record.spoolRefs = [];
      record.state = "explicitly-discarded";
      record.admissionRevision ??= revision;
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      return structuredClone(record);
    });
  }

  markPoisonSkippedInbound(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundRecord {
    const record = this.discardInbound(recordId, claim);
    return this.#commit((snapshot, revision, nowMs) => {
      const current = this.#getInbound(snapshot, record.recordId);
      this.#assertClaim(snapshot, current, claim);
      current.terminalReason = "poison-skipped";
      current.stateRevision = revision;
      current.updatedAtMs = nowMs;
      return structuredClone(current);
    });
  }

  discardInboundAction(
    actionId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundActionResult {
    const recordId = withTelegramFileTransaction(
      `${this.rootPath}.transaction`,
      () =>
        this.#resolveInboundActionId(this.#readSnapshotLocked(), actionId)
          .recordId,
    );
    const record = this.discardInbound(recordId, claim);
    return {
      actionId: this.#opaqueActionId(`record:${record.recordId}`),
      turnId: record.turnId,
      state: record.state,
      spool: [],
    };
  }

  retryUncertainInbound(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundRetryResult {
    return this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const source = this.#getInbound(snapshot, recordId);
      this.#assertClaim(snapshot, source, claim);
      const existing = snapshot.inbound.find(
        (record) => record.linkedAttemptOf === source.recordId,
      );
      if (existing) {
        this.#assertClaim(snapshot, existing, claim);
        return {
          ...this.#readDrainItem(existing),
          duplicationWarning: true,
        };
      }
      if (source.state !== "execution-uncertain") {
        throw new Error("Only execution-uncertain inbound work may be retried");
      }
      const attempt: RecoveryInboundRecord = {
        ...structuredClone(source),
        identity: structuredClone(claim.identity),
        recordId: this.#randomId(),
        turnId: this.#randomId(),
        state: "pre-dispatch",
        admissionRevision: revision,
        linkedAttemptOf: source.recordId,
        createdRevision: revision,
        stateRevision: revision,
        createdAtMs: nowMs,
        updatedAtMs: nowMs,
      };
      source.state = "explicitly-discarded";
      source.stateRevision = revision;
      source.updatedAtMs = nowMs;
      delete source.payloadRef;
      source.spoolRefs = [];
      snapshot.inbound.push(attempt);
      return {
        ...this.#readDrainItem(attempt),
        duplicationWarning: true,
      };
    });
  }

  retryUncertainInboundAction(
    actionId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryInboundActionResult {
    const recordId = withTelegramFileTransaction(
      `${this.rootPath}.transaction`,
      () =>
        this.#resolveInboundActionId(this.#readSnapshotLocked(), actionId)
          .recordId,
    );
    const result = this.retryUncertainInbound(recordId, claim);
    return {
      actionId: this.#opaqueActionId(`record:${result.record.recordId}`),
      turnId: result.record.turnId,
      state: result.record.state,
      payload: result.payload,
      spool: result.spool,
      duplicationWarning: true,
    };
  }

  listReplayableInboundRecords(): RecoveryInboundRecord[] {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () =>
      this.#readSnapshotLocked()
        .inbound.filter(
          (record) =>
            record.state === "admitted" || record.state === "pre-dispatch",
        )
        .map((record) => structuredClone(record)),
    );
  }

  drainSafeInbound(claim: RecoveryIdentityClaim): RecoveryInboundDrainItem[] {
    return this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      this.#authenticate(claim.identity);
      const drained: RecoveryInboundDrainItem[] = [];
      for (const record of snapshot.inbound) {
        if (record.state !== "admitted" && record.state !== "pre-dispatch") {
          continue;
        }
        try {
          this.#assertClaim(snapshot, record, claim);
        } catch {
          continue;
        }
        if (record.state === "admitted") {
          record.state = "pre-dispatch";
          record.stateRevision = revision;
          record.updatedAtMs = nowMs;
        }
        drained.push(this.#readDrainItem(record));
      }
      return drained;
    });
  }

  #readDrainItem(record: RecoveryInboundRecord): RecoveryInboundDrainItem {
    if (!record.payloadRef) {
      throw new Error(`Recovery record ${record.recordId} has no payload`);
    }
    return {
      record: structuredClone(record),
      payload: this.#readVerifiedBinary(
        this.payloadDirectory,
        record.payloadRef,
        record.recordId,
      ),
      spool: record.spoolRefs.map((ref) =>
        this.#readVerifiedBinary(this.spoolDirectory, ref, record.recordId),
      ),
    };
  }

  planOutbound(input: RecoveryOutboundPlanInput): RecoveryOutboundRecord {
    const identity = readIdentity(input.claim.identity, "$plan.claim.identity");
    const sourceInboundRecordIds = input.sourceInboundRecordIds.map((recordId, index) =>
      readStableString(recordId, `$plan.sourceInboundRecordIds[${index}]`),
    );
    if (sourceInboundRecordIds.length === 0) {
      throw new Error("Recovery outbound plan requires inbound source records");
    }
    assertUniqueStrings(sourceInboundRecordIds, "$plan.sourceInboundRecordIds");
    const spool = [...(input.spool ?? [])];
    const payload = readOutboundPayload(
      {
        version: 1,
        intentId: input.intentId,
        turnId: input.turnId,
        replyToMessageId: input.replyToMessageId,
        renderingMode: input.renderingMode,
        finalMarkdown: input.finalMarkdown,
        renderedChunks: [...input.renderedChunks],
        units: structuredClone(input.units),
        buttons: structuredClone(input.buttons ?? []),
        ...(input.voice ? { voice: structuredClone(input.voice) } : {}),
        ...(input.guestQueryId ? { guestQueryId: input.guestQueryId } : {}),
      },
      "$plan.payload",
      spool.length,
    );
    const payloadBytes = Buffer.from(JSON.stringify(payload), "utf8");
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const current = this.#readSnapshotForMutationLocked();
      this.#assertActive(current);
      this.#authenticate(identity);
      this.#assertNotFencedByReassignment(current, identity);
      const existing = current.outbound.find(
        (record) =>
          record.identity.profile === identity.profile &&
          areRecoveryTargetsEqual(record.identity.target, identity.target) &&
          record.intentId === payload.intentId &&
          record.turnId === payload.turnId,
      );
      if (existing) {
        this.#assertOutboundClaim(current, existing, input.claim);
        if (
          !identitiesEqual(existing.identity, identity) ||
          JSON.stringify(existing.sourceInboundRecordIds) !==
            JSON.stringify(sourceInboundRecordIds)
        ) {
          throw new Error("Recovery outbound plan idempotency mismatch");
        }
        const stored = this.#readOutboundPayload(current, existing);
        if (!stored.bytes.equals(payloadBytes) || existing.spoolRefs.length !== spool.length) {
          throw new Error("Recovery outbound plan idempotency payload mismatch");
        }
        for (let index = 0; index < spool.length; index += 1) {
          const bytes = this.#readVerifiedBinary(
            this.spoolDirectory,
            existing.spoolRefs[index]!,
            existing.recordId,
          );
          if (!bytes.equals(Buffer.from(spool[index]!))) {
            throw new Error("Recovery outbound plan idempotency spool mismatch");
          }
        }
        return structuredClone(existing);
      }
      for (const recordId of sourceInboundRecordIds) {
        const record = this.#getInbound(current, recordId);
        this.#assertClaim(current, record, input.claim);
        if (record.state !== "dispatching") {
          throw new Error("Recovery outbound sources must be dispatching");
        }
        if (record.turnId !== payload.turnId) {
          throw new Error("Recovery outbound source turn mismatch");
        }
      }
      const nowMs = this.#now();
      const revision = current.revision + 1;
      const snapshot = cloneSnapshot(current);
      const payloadId = this.#randomId();
      assertSafeFileId(payloadId, "$plan.payloadId");
      const spoolIds = spool.map(() => {
        const spoolId = this.#randomId();
        assertSafeFileId(spoolId, "$plan.spoolId");
        return spoolId;
      });
      const record: RecoveryOutboundRecord = {
        family: "outbound",
        recordId: this.#randomId(),
        intentId: payload.intentId,
        turnId: payload.turnId,
        sourceInboundRecordIds,
        state: "planned",
        nextUnitIndex: 0,
        automaticAttemptCount: 0,
        receipts: [],
        identity,
        createdRevision: revision,
        stateRevision: revision,
        createdAtMs: nowMs,
        updatedAtMs: nowMs,
        payloadRef: {
          payloadId,
          byteLength: payloadBytes.byteLength,
          sha256: sha256Bytes(payloadBytes),
        },
        spoolRefs: spoolIds.map((spoolId, index) => ({
          spoolId,
          byteLength: spool[index]!.byteLength,
          sha256: sha256Bytes(spool[index]!),
        })),
      };
      snapshot.outbound.push(record);
      snapshot.revision = revision;
      snapshot.writtenAtMs = nowMs;
      serializeAccountedSnapshot(snapshot);
      if (snapshot.quota.totalBytes > this.quotaBytes) {
        throw new RecoveryQuotaExceededError(snapshot.quota.totalBytes, this.quotaBytes);
      }
      const publications = [
        { directory: this.payloadDirectory, id: payloadId, bytes: payloadBytes },
        ...spool.map((bytes, index) => ({
          directory: this.spoolDirectory,
          id: spoolIds[index]!,
          bytes,
        })),
      ];
      const published: string[] = [];
      let committed = false;
      let commitUnknown = false;
      try {
        const directories = new Set<string>();
        for (const publication of publications) {
          const path = makeBinaryPath(publication.directory, publication.id);
          if (this.#fs.exists(path)) {
            throw new Error(`Recovery binary id collision: ${publication.id}`);
          }
          writePrivateFile(this.#fs, path, publication.bytes);
          published.push(path);
          directories.add(publication.directory);
        }
        for (const directory of directories) fsyncPath(this.#fs, directory);
        this.#writeSnapshotLocked(snapshot);
        committed = true;
        return structuredClone(record);
      } catch (error) {
        commitUnknown = error instanceof RecoverySnapshotCommitUnknownError;
        throw error;
      } finally {
        if (!committed && !commitUnknown) {
          const directories = new Set<string>();
          for (const path of published) {
            this.#fs.remove(path, { force: true, recursive: true });
            directories.add(dirname(path));
          }
          for (const directory of directories) fsyncPath(this.#fs, directory);
        }
      }
    });
  }

  activateOutbound(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryOutboundRecord {
    const record = this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const current = this.#getOutbound(snapshot, recordId);
      this.#assertOutboundClaim(snapshot, current, claim);
      if (current.state === "pending") return structuredClone(current);
      if (current.state !== "planned") {
        throw new Error(`Invalid outbound transition ${current.state} -> pending`);
      }
      current.state = "pending";
      current.stateRevision = revision;
      current.updatedAtMs = nowMs;
      return structuredClone(current);
    });
    this.#fault?.("OUT-02");
    return record;
  }

  claimOutboundUnit(input: RecoveryOutboundClaimInput): RecoveryOutboundDrainItem {
    return this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const record = this.#getOutbound(snapshot, input.recordId);
      this.#assertOutboundClaim(snapshot, record, input.claim);
      if (
        record.state !== "planned" &&
        record.state !== "pending" &&
        record.state !== "retryable-pending"
      ) {
        throw new Error(`Invalid outbound transition ${record.state} -> sending`);
      }
      if (record.state === "retryable-pending") {
        if (record.automaticAttemptCount >= RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS) {
          throw new Error("Recovery outbound automatic attempts are exhausted");
        }
        if (record.retryNotBeforeMs === undefined || nowMs < record.retryNotBeforeMs) {
          throw new Error("Recovery outbound retry is not eligible yet");
        }
      }
      const { payload } = this.#readOutboundPayload(snapshot, record);
      if (!payload.units[record.nextUnitIndex]) {
        throw new Error("Recovery outbound record has no claimable unit");
      }
      record.state = "sending";
      record.automaticAttemptCount += 1;
      delete record.retryNotBeforeMs;
      record.activeUnit = {
        unitIndex: record.nextUnitIndex,
        attemptId: this.#randomId(),
        startedAtMs: nowMs,
      };
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      return this.#readOutboundDrainItem(snapshot, record);
    });
  }

  recordOutboundReceipt(input: RecoveryOutboundReceiptInput): RecoveryOutboundRecord {
    let finalReceipt = false;
    const result = this.#commit((snapshot, revision, nowMs, deleteAfterCommit) => {
      this.#assertActive(snapshot);
      const record = this.#getOutbound(snapshot, input.recordId);
      this.#assertOutboundClaim(snapshot, record, input.claim);
      const { payload } = this.#readOutboundPayload(snapshot, record);
      const unit = payload.units[record.nextUnitIndex];
      this.#assertOutboundAttempt(record, input.attemptId);
      const operationId = readStableString(input.operationId, "$receipt.operationId");
      const method = readStableString(input.method, "$receipt.method");
      if (!unit || unit.operationId !== operationId || unit.method !== method) {
        throw new Error("Recovery outbound receipt does not match the active unit");
      }
      const messageId = input.messageId === undefined
        ? undefined
        : readSafeInteger(input.messageId, "$receipt.messageId", { minimum: 1 });
      finalReceipt = record.nextUnitIndex + 1 === payload.units.length;
      if (finalReceipt) this.#fault?.("OUT-05");
      record.receipts.push({
        unitIndex: record.nextUnitIndex,
        operationId,
        method,
        ...(messageId !== undefined ? { messageId } : {}),
        committedAtMs: nowMs,
      });
      record.nextUnitIndex += 1;
      delete record.activeUnit;
      record.automaticAttemptCount = 0;
      if (finalReceipt) {
        record.state = "delivered";
        for (const spool of record.spoolRefs) {
          deleteAfterCommit.push(makeBinaryPath(this.spoolDirectory, spool.spoolId));
        }
        record.spoolRefs = [];
        this.#completeOutboundInboundSources(
          snapshot,
          record,
          input.claim,
          revision,
          nowMs,
          deleteAfterCommit,
        );
      } else {
        record.state = "pending";
      }
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      return structuredClone(record);
    });
    if (finalReceipt) this.#fault?.("OUT-06");
    return result;
  }

  recordOutboundSafeFailure(
    input: RecoveryOutboundFailureInput,
  ): RecoveryOutboundRecord {
    const result = this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const record = this.#getOutbound(snapshot, input.recordId);
      this.#assertOutboundClaim(snapshot, record, input.claim);
      this.#assertOutboundAttempt(record, input.attemptId);
      record.state = "retryable-pending";
      delete record.activeUnit;
      if (record.automaticAttemptCount < RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS) {
        record.retryNotBeforeMs =
          nowMs + RECOVERY_OUTBOUND_RETRY_DELAYS_MS[record.automaticAttemptCount - 1]!;
      } else {
        delete record.retryNotBeforeMs;
      }
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      return structuredClone(record);
    });
    this.#fault?.("OUT-03");
    return result;
  }

  markOutboundUncertain(
    input: RecoveryOutboundUncertainInput,
  ): RecoveryOutboundRecord {
    const reason = readEnum(
      input.reason,
      "$uncertain.reason",
      RECOVERY_OUTBOUND_UNCERTAINTY_REASONS,
    );
    const result = this.#commit((snapshot, revision, nowMs, deleteAfterCommit) => {
      this.#assertActive(snapshot);
      const record = this.#getOutbound(snapshot, input.recordId);
      this.#assertOutboundClaim(snapshot, record, input.claim);
      this.#assertOutboundAttempt(record, input.attemptId);
      record.state = "delivery-uncertain";
      record.uncertainty = {
        unitIndex: record.nextUnitIndex,
        reason,
        observedAtMs: nowMs,
      };
      delete record.activeUnit;
      delete record.retryNotBeforeMs;
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      this.#completeOutboundInboundSources(
        snapshot,
        record,
        input.claim,
        revision,
        nowMs,
        deleteAfterCommit,
      );
      return structuredClone(record);
    });
    this.#fault?.("OUT-04");
    return result;
  }

  discardOutbound(
    recordId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryOutboundRecord {
    return this.#commit((snapshot, revision, nowMs, deleteAfterCommit) => {
      this.#assertActive(snapshot);
      const record = this.#getOutbound(snapshot, recordId);
      this.#assertOutboundClaim(snapshot, record, claim);
      if (record.state === "explicitly-discarded") return structuredClone(record);
      if (snapshot.outbound.some((entry) => entry.linkedAttemptOf === record.recordId)) {
        throw new Error("Cannot discard outbound work after a linked retry exists");
      }
      if (
        record.state !== "planned" &&
        record.state !== "pending" &&
        record.state !== "retryable-pending" &&
        record.state !== "delivery-uncertain"
      ) {
        throw new Error(`Cannot discard outbound record in ${record.state}`);
      }
      if (record.payloadRef) {
        deleteAfterCommit.push(
          makeBinaryPath(this.payloadDirectory, record.payloadRef.payloadId),
        );
        delete record.payloadRef;
      }
      for (const spool of record.spoolRefs) {
        deleteAfterCommit.push(makeBinaryPath(this.spoolDirectory, spool.spoolId));
      }
      record.spoolRefs = [];
      record.state = "explicitly-discarded";
      delete record.activeUnit;
      delete record.retryNotBeforeMs;
      delete record.uncertainty;
      record.stateRevision = revision;
      record.updatedAtMs = nowMs;
      this.#completeOutboundInboundSources(
        snapshot,
        record,
        claim,
        revision,
        nowMs,
        deleteAfterCommit,
      );
      return structuredClone(record);
    });
  }

  retryUncertainOutbound(
    recordId: string,
    newIntentId: string,
    claim: RecoveryIdentityClaim,
  ): RecoveryOutboundRetryResult {
    const parsedIntentId = readStableString(newIntentId, "$retry.intentId");
    return this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      const source = this.#getOutbound(snapshot, recordId);
      this.#assertOutboundClaim(snapshot, source, claim);
      const existing = snapshot.outbound.find(
        (record) => record.linkedAttemptOf === source.recordId,
      );
      if (existing) {
        this.#assertOutboundClaim(snapshot, existing, claim);
        if (existing.intentId !== parsedIntentId) {
          throw new Error("Recovery linked outbound retry intent mismatch");
        }
        return {
          ...this.#readOutboundDrainItem(snapshot, existing),
          duplicationWarning: true,
        };
      }
      if (source.state !== "delivery-uncertain" || !source.payloadRef) {
        throw new Error("Only delivery-uncertain outbound work with payload may be retried");
      }
      if (
        snapshot.outbound.some(
          (record) =>
            record.identity.profile === claim.identity.profile &&
            areRecoveryTargetsEqual(record.identity.target, claim.identity.target) &&
            record.intentId === parsedIntentId &&
            record.turnId === source.turnId,
        )
      ) {
        throw new Error("Recovery linked outbound retry intent collision");
      }
      const attempt: RecoveryOutboundRecord = {
        ...structuredClone(source),
        recordId: this.#randomId(),
        intentId: parsedIntentId,
        identity: structuredClone(claim.identity),
        state: "pending",
        automaticAttemptCount: 0,
        linkedAttemptOf: source.recordId,
        createdRevision: revision,
        stateRevision: revision,
        createdAtMs: nowMs,
        updatedAtMs: nowMs,
      };
      delete attempt.activeUnit;
      delete attempt.retryNotBeforeMs;
      delete attempt.uncertainty;
      delete source.payloadRef;
      source.spoolRefs = [];
      source.stateRevision = revision;
      source.updatedAtMs = nowMs;
      snapshot.outbound.push(attempt);
      return {
        ...this.#readOutboundDrainItem(snapshot, attempt),
        duplicationWarning: true,
      };
    });
  }

  listClaimableOutboundRecords(
    claim: RecoveryIdentityClaim,
  ): RecoveryOutboundDrainItem[] {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const snapshot = this.#readSnapshotForMutationLocked();
      this.#assertActive(snapshot);
      this.#authenticate(claim.identity);
      const nowMs = this.#now();
      const result: RecoveryOutboundDrainItem[] = [];
      for (const record of snapshot.outbound) {
        const claimable =
          record.state === "planned" ||
          record.state === "pending" ||
          (record.state === "retryable-pending" &&
            record.automaticAttemptCount < RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS &&
            record.retryNotBeforeMs !== undefined &&
            nowMs >= record.retryNotBeforeMs);
        if (!claimable) continue;
        try {
          this.#assertOutboundClaim(snapshot, record, claim);
        } catch {
          continue;
        }
        result.push(this.#readOutboundDrainItem(snapshot, record));
      }
      return result;
    });
  }

  drainSafeOutbound(claim: RecoveryIdentityClaim): RecoveryOutboundDrainItem[] {
    return this.#commit((snapshot, revision, nowMs) => {
      this.#assertActive(snapshot);
      this.#authenticate(claim.identity);
      const result: RecoveryOutboundDrainItem[] = [];
      for (const record of snapshot.outbound) {
        if (
          record.state !== "planned" &&
          record.state !== "pending" &&
          record.state !== "retryable-pending"
        ) {
          continue;
        }
        try {
          this.#assertOutboundClaim(snapshot, record, claim);
        } catch {
          continue;
        }
        if (record.state !== "pending" || record.automaticAttemptCount !== 0) {
          record.state = "pending";
          record.automaticAttemptCount = 0;
          delete record.retryNotBeforeMs;
          record.stateRevision = revision;
          record.updatedAtMs = nowMs;
        }
        result.push(this.#readOutboundDrainItem(snapshot, record));
      }
      return result;
    });
  }

  #assertOutboundAttempt(
    record: RecoveryOutboundRecord,
    attemptId: string,
  ): void {
    const parsedAttemptId = readStableString(attemptId, "$attemptId");
    if (
      record.state !== "sending" ||
      !record.activeUnit ||
      record.activeUnit.attemptId !== parsedAttemptId ||
      record.activeUnit.unitIndex !== record.nextUnitIndex
    ) {
      throw new Error("Recovery outbound active attempt claim denied");
    }
  }

  #completeOutboundInboundSources(
    snapshot: RecoverySnapshotV1,
    outbound: RecoveryOutboundRecord,
    claim: RecoveryIdentityClaim,
    revision: number,
    nowMs: number,
    deleteAfterCommit: string[],
  ): void {
    for (const recordId of outbound.sourceInboundRecordIds) {
      const inbound = this.#getInbound(snapshot, recordId);
      if (inbound.state === "completed") continue;
      this.#assertClaim(snapshot, inbound, claim);
      if (inbound.state !== "dispatching") {
        throw new Error("Recovery outbound disposition requires dispatching inbound sources");
      }
      for (const spool of inbound.spoolRefs) {
        deleteAfterCommit.push(makeBinaryPath(this.spoolDirectory, spool.spoolId));
      }
      deleteAfterCommit.push(join(this.materializedDirectory, inbound.recordId));
      inbound.spoolRefs = [];
      inbound.state = "completed";
      inbound.stateRevision = revision;
      inbound.updatedAtMs = nowMs;
    }
  }

  #readOutboundDrainItem(
    snapshot: RecoverySnapshotV1,
    record: RecoveryOutboundRecord,
  ): RecoveryOutboundDrainItem {
    const { bytes } = this.#readOutboundPayload(snapshot, record);
    return {
      record: structuredClone(record),
      payload: bytes,
      spool: record.spoolRefs.map((reference) =>
        this.#readVerifiedBinary(this.spoolDirectory, reference, record.recordId),
      ),
    };
  }

  #getOutbound(
    snapshot: RecoverySnapshotV1,
    recordId: string,
  ): RecoveryOutboundRecord {
    const record = snapshot.outbound.find((entry) => entry.recordId === recordId);
    if (!record) throw new Error("Unknown recovery outbound record");
    return record;
  }

  #opaqueActionId(stableInput: string): string {
    return readStableString(this.#actionId(stableInput), "$actionId");
  }

  #recordActionIds(snapshot: RecoverySnapshotV1): Map<string, string> {
    const result = new Map<string, string>();
    for (const record of [
      ...snapshot.inbound,
      ...snapshot.outbound,
      ...snapshot.bus,
    ]) {
      const actionId = this.#opaqueActionId(`record:${record.recordId}`);
      if (result.has(actionId)) {
        throw new Error(`Recovery opaque action id collision: ${actionId}`);
      }
      result.set(actionId, record.recordId);
    }
    return result;
  }

  #resolveInboundActionId(
    snapshot: RecoverySnapshotV1,
    actionId: string,
  ): RecoveryInboundRecord {
    const recordId = this.#recordActionIds(snapshot).get(actionId);
    if (!recordId) throw new Error("Unknown recovery action id");
    return this.#getInbound(snapshot, recordId);
  }

  #getInbound(
    snapshot: RecoverySnapshotV1,
    recordId: string,
  ): RecoveryInboundRecord {
    const record = snapshot.inbound.find(
      (entry) => entry.recordId === recordId,
    );
    if (!record) throw new Error("Unknown recovery inbound record");
    return record;
  }

  consumeSameProcessHandoff(
    handoffValue: RecoverySameProcessHandoff,
    currentIdentity: RecoveryIdentity,
  ): { handoff: RecoverySameProcessHandoff; claimedRecordIds: string[] } {
    const handoff = validateRecoverySameProcessHandoff(handoffValue);
    const current = readIdentity(currentIdentity, "$currentIdentity");
    this.#authenticate(current);
    if (
      handoff.profile !== current.profile ||
      !areRecoveryTargetsEqual(handoff.target, current.target) ||
      !areRecoveryOwnersEqual(handoff.owner, current.owner) ||
      handoff.toSessionGeneration !== current.sessionGeneration ||
      handoff.consumedAtMs !== undefined ||
      this.#now() > handoff.expiresAtMs
    ) {
      throw new Error("Recovery same-process handoff claim denied");
    }
    const registry = getConsumedHandoffRegistry();
    const registryKey = `${this.rootPath}\u0000${handoff.handoffId}`;
    if (registry.has(registryKey)) {
      throw new Error("Recovery same-process handoff was already consumed");
    }
    const claimedRecordIds = this.#commit((snapshot) => {
      this.#assertActive(snapshot);
      const claimed: string[] = [];
      for (const record of [
        ...snapshot.inbound,
        ...snapshot.outbound,
        ...snapshot.bus,
      ]) {
        if (
          isUnresolvedRecord(record) &&
          ownerTargetEqual(
            record.identity,
            handoff.profile,
            handoff.target,
            handoff.owner,
          ) &&
          record.identity.sessionGeneration === handoff.fromSessionGeneration
        ) {
          record.identity = structuredClone(current);
          claimed.push(record.recordId);
        }
      }
      const movedOutboundSourceIds = new Set(
        snapshot.outbound
          .filter((record) => claimed.includes(record.recordId))
          .flatMap((record) => record.sourceInboundRecordIds),
      );
      for (const source of snapshot.inbound) {
        if (
          movedOutboundSourceIds.has(source.recordId) &&
          ownerTargetEqual(
            source.identity,
            handoff.profile,
            handoff.target,
            handoff.owner,
          ) &&
          source.identity.sessionGeneration === handoff.fromSessionGeneration
        ) {
          source.identity = structuredClone(current);
        }
      }
      return claimed;
    });
    registry.add(registryKey);
    return {
      handoff: { ...handoff, consumedAtMs: this.#now() },
      claimedRecordIds,
    };
  }

  getOrphanReassignmentCandidates(): RecoveryOrphanReassignmentCandidate[] {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const snapshot = this.#readSnapshotLocked();
      const nowMs = this.#now();
      return this.#collectOrphanCandidates(snapshot).map((candidate) => ({
        actionId: candidate.actionId,
        unresolvedCount: candidate.records.length,
        oldestAgeMs: Math.max(
          ...candidate.records.map((record) =>
            Math.max(0, nowMs - record.createdAtMs),
          ),
        ),
        states: [
          ...new Set(candidate.records.map((record) => record.state)),
        ].sort(),
      }));
    });
  }

  requestReassignmentAction(
    actionId: string,
    newIdentity: RecoveryIdentity,
  ): RecoveryReassignmentActionResult {
    const candidate = withTelegramFileTransaction(
      `${this.rootPath}.transaction`,
      () => {
        const candidates = this.#collectOrphanCandidates(
          this.#readSnapshotLocked(),
        );
        const match = candidates.find((entry) => entry.actionId === actionId);
        if (!match) throw new Error("Unknown orphan reassignment action id");
        return {
          target: structuredClone(match.target),
          oldOwner: structuredClone(match.oldOwner),
        };
      },
    );
    const reassignment = this.requestReassignment({
      ...candidate,
      newIdentity,
    });
    return {
      actionId: this.#opaqueActionId(
        `reassignment:${reassignment.reassignmentId}`,
      ),
      state: reassignment.state,
      unresolvedCount: reassignment.unresolvedRecordIds.length,
    };
  }

  #collectOrphanCandidates(snapshot: RecoverySnapshotV1): Array<{
    actionId: string;
    target: TelegramTarget;
    oldOwner: RecoveryOwnerIdentity;
    records: Array<
      RecoveryInboundRecord | RecoveryOutboundRecord | RecoveryBusRecord
    >;
  }> {
    const groups = new Map<
      string,
      {
        target: TelegramTarget;
        oldOwner: RecoveryOwnerIdentity;
        records: Array<
          RecoveryInboundRecord | RecoveryOutboundRecord | RecoveryBusRecord
        >;
      }
    >();
    for (const record of [
      ...snapshot.inbound,
      ...snapshot.outbound,
      ...snapshot.bus,
    ]) {
      if (!isUnresolvedRecord(record)) continue;
      if (this.#isIdentityAuthenticated?.(record.identity)) continue;
      const key = JSON.stringify([
        record.identity.profile,
        record.identity.target.chatId,
        record.identity.target.threadId ?? null,
        record.identity.owner,
      ]);
      const group = groups.get(key) ?? {
        target: structuredClone(record.identity.target),
        oldOwner: structuredClone(record.identity.owner),
        records: [],
      };
      group.records.push(record);
      groups.set(key, group);
    }
    const seen = new Set<string>();
    return [...groups.entries()].map(([key, group]) => {
      const actionId = this.#opaqueActionId(`orphan:${key}`);
      if (seen.has(actionId)) {
        throw new Error(`Recovery orphan action id collision: ${actionId}`);
      }
      seen.add(actionId);
      return { actionId, ...group };
    });
  }

  #resolveReassignmentActionId(
    snapshot: RecoverySnapshotV1,
    actionId: string,
  ): RecoveryReassignmentRecord {
    const matches = snapshot.reassignments.filter(
      (record) =>
        this.#opaqueActionId(`reassignment:${record.reassignmentId}`) ===
        actionId,
    );
    if (matches.length !== 1) {
      throw new Error("Unknown recovery reassignment action id");
    }
    return matches[0]!;
  }

  requestReassignment(
    request: RecoveryReassignmentRequest,
  ): RecoveryReassignmentRecord {
    const newIdentity = readIdentity(request.newIdentity, "$newIdentity");
    this.#authenticate(newIdentity);
    if (
      newIdentity.profile !== this.profile ||
      !areRecoveryTargetsEqual(newIdentity.target, request.target)
    ) {
      throw new Error("Recovery reassignment target/profile mismatch");
    }
    return this.#commit((snapshot, _revision, nowMs) => {
      this.#assertActive(snapshot);
      const unresolvedRecordIds = [
        ...snapshot.inbound,
        ...snapshot.outbound,
        ...snapshot.bus,
      ]
        .filter(
          (record) =>
            isUnresolvedRecord(record) &&
            ownerTargetEqual(
              record.identity,
              this.profile,
              request.target,
              request.oldOwner,
            ),
        )
        .map((record) => record.recordId);
      if (unresolvedRecordIds.length === 0) {
        throw new Error("Recovery reassignment has no unresolved records");
      }
      const collision = snapshot.reassignments.find(
        (entry) =>
          entry.state !== "cancelled-before-transfer" &&
          entry.unresolvedRecordIds.some((id) =>
            unresolvedRecordIds.includes(id),
          ),
      );
      if (collision) {
        if (
          areRecoveryOwnersEqual(collision.newOwner, newIdentity.owner) &&
          collision.newSessionGeneration === newIdentity.sessionGeneration &&
          areRecoveryTargetsEqual(collision.target, request.target)
        ) {
          return structuredClone(collision);
        }
        throw new Error("Recovery reassignment collision");
      }
      const record: RecoveryReassignmentRecord = {
        family: "reassignment",
        reassignmentId: this.#randomId(),
        profile: this.profile,
        target: structuredClone(request.target),
        oldOwner: structuredClone(request.oldOwner),
        newOwner: structuredClone(newIdentity.owner),
        newSessionGeneration: newIdentity.sessionGeneration,
        captureThroughRevision: snapshot.revision,
        unresolvedRecordIds,
        state: "requested",
        createdAtMs: nowMs,
        updatedAtMs: nowMs,
      };
      snapshot.reassignments.push(record);
      return structuredClone(record);
    });
  }

  completeReassignmentAction(
    actionId: string,
    authenticatedIdentity: RecoveryIdentity,
  ): RecoveryReassignmentActionResult {
    for (;;) {
      const current = withTelegramFileTransaction(
        `${this.rootPath}.transaction`,
        () =>
          structuredClone(
            this.#resolveReassignmentActionId(
              this.#readSnapshotLocked(),
              actionId,
            ),
          ),
      );
      switch (current.state) {
        case "requested":
          this.advanceReassignment(
            current.reassignmentId,
            "requested",
            "binding-transfer-pending",
            authenticatedIdentity,
          );
          break;
        case "binding-transfer-pending":
          this.advanceReassignment(
            current.reassignmentId,
            "binding-transfer-pending",
            "binding-transferred",
            authenticatedIdentity,
          );
          break;
        case "binding-transferred":
          this.advanceReassignment(
            current.reassignmentId,
            "binding-transferred",
            "recovery-grant-committed",
            authenticatedIdentity,
          );
          break;
        case "recovery-grant-committed":
          return {
            actionId,
            state: current.state,
            unresolvedCount: current.unresolvedRecordIds.length,
          };
        case "cancelled-before-transfer":
        case "rollback-pending":
          throw new Error(
            `Recovery reassignment cannot complete from ${current.state}`,
          );
      }
    }
  }

  advanceReassignment(
    reassignmentId: string,
    expected: RecoveryReassignmentState,
    next: RecoveryReassignmentState,
    authenticatedIdentity: RecoveryIdentity,
  ): RecoveryReassignmentRecord {
    const identity = readIdentity(authenticatedIdentity, "$identity");
    this.#authenticate(identity);
    const allowed: Record<
      RecoveryReassignmentState,
      readonly RecoveryReassignmentState[]
    > = {
      requested: ["binding-transfer-pending", "cancelled-before-transfer"],
      "binding-transfer-pending": ["binding-transferred", "rollback-pending"],
      "binding-transferred": ["recovery-grant-committed", "rollback-pending"],
      "recovery-grant-committed": [],
      "cancelled-before-transfer": [],
      "rollback-pending": [
        "cancelled-before-transfer",
        "binding-transfer-pending",
      ],
    };
    return this.#commit((snapshot, _revision, nowMs) => {
      this.#assertActive(snapshot);
      const record = snapshot.reassignments.find(
        (entry) => entry.reassignmentId === reassignmentId,
      );
      if (!record) throw new Error("Unknown recovery reassignment");
      if (
        !areRecoveryOwnersEqual(record.newOwner, identity.owner) ||
        !areRecoveryTargetsEqual(record.target, identity.target) ||
        record.profile !== identity.profile ||
        record.newSessionGeneration !== identity.sessionGeneration
      ) {
        throw new Error("Recovery reassignment owner claim denied");
      }
      if (record.state === next) return structuredClone(record);
      if (record.state !== expected || !allowed[expected].includes(next)) {
        throw new Error(
          `Invalid recovery reassignment ${record.state} -> ${next}`,
        );
      }
      const expectedBinding =
        next === "binding-transferred" || next === "recovery-grant-committed"
          ? "new-owner"
          : expected === "rollback-pending" &&
              next === "cancelled-before-transfer"
            ? "old-owner-restored"
            : undefined;
      if (
        expectedBinding &&
        !this.#validateReassignmentBinding?.({
          reassignment: structuredClone(record),
          currentIdentity: structuredClone(identity),
          expectedBinding,
        })
      ) {
        throw new Error(
          "Recovery reassignment binding/authentication revalidation failed",
        );
      }
      record.state = next;
      record.updatedAtMs = nowMs;
      return structuredClone(record);
    });
  }

  compact(): RecoveryMetadataStatus {
    this.#commit((snapshot, _revision, nowMs, deleteAfterCommit) => {
      this.#applyRetention(snapshot, nowMs, deleteAfterCommit);
    });
    return this.getStatus();
  }

  #applyRetention(
    snapshot: RecoverySnapshotV1,
    nowMs: number,
    deleteAfterCommit: string[],
  ): void {
    const recordsById = new Map(
      [...snapshot.inbound, ...snapshot.outbound, ...snapshot.bus].map(
        (record) => [record.recordId, record] as const,
      ),
    );
    snapshot.reassignments = snapshot.reassignments.filter((entry) => {
      const terminal =
        entry.state === "recovery-grant-committed" ||
        entry.state === "cancelled-before-transfer";
      const stillAuthorizesUnresolved = entry.unresolvedRecordIds.some(
        (recordId) => {
          const record = recordsById.get(recordId);
          return record !== undefined && isUnresolvedRecord(record);
        },
      );
      return (
        !terminal ||
        stillAuthorizesUnresolved ||
        nowMs - entry.updatedAtMs <= RECOVERY_TERMINAL_METADATA_RETENTION_MS
      );
    });
    const reassignmentsProtectingUnresolved = snapshot.reassignments.filter(
      (entry) =>
        entry.unresolvedRecordIds.some((recordId) => {
          const record = recordsById.get(recordId);
          return record !== undefined && isUnresolvedRecord(record);
        }),
    );
    const protectedIds = new Set([
      ...reassignmentsProtectingUnresolved.flatMap(
        (entry) => entry.unresolvedRecordIds,
      ),
      // A terminal source linked from unresolved retry work is not
      // terminal-only metadata; it remains part of that unresolved attempt.
      ...snapshot.inbound.flatMap((record) =>
        record.linkedAttemptOf ? [record.linkedAttemptOf] : [],
      ),
      ...snapshot.outbound.flatMap((record) =>
        record.linkedAttemptOf ? [record.linkedAttemptOf] : [],
      ),
      // Exact inbound completion links remain part of unresolved outbound
      // evidence, especially while delivery is uncertain indefinitely.
      ...snapshot.outbound.flatMap((record) => record.sourceInboundRecordIds),
    ]);
    const allRecords: RecoveryRecord[] = [
      ...snapshot.inbound,
      ...snapshot.outbound,
      ...snapshot.bus,
    ];
    for (const record of allRecords) {
      if (!isTerminalRecord(record)) continue;
      if (
        record.payloadRef &&
        (!isSuccessfulTerminalRecord(record) ||
          nowMs - record.updatedAtMs >= RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS)
      ) {
        deleteAfterCommit.push(
          makeBinaryPath(this.payloadDirectory, record.payloadRef.payloadId),
        );
        delete record.payloadRef;
      }
      for (const spool of record.spoolRefs) {
        deleteAfterCommit.push(
          makeBinaryPath(this.spoolDirectory, spool.spoolId),
        );
      }
      record.spoolRefs = [];
    }
    let terminals = allRecords
      .filter(isTerminalRecord)
      .sort((left, right) => left.updatedAtMs - right.updatedAtMs);
    const remove = new Set<string>();
    for (const record of terminals) {
      if (
        !protectedIds.has(record.recordId) &&
        nowMs - record.updatedAtMs > RECOVERY_TERMINAL_METADATA_RETENTION_MS
      ) {
        remove.add(record.recordId);
      }
    }
    terminals = terminals.filter((record) => !remove.has(record.recordId));
    let metadataBytes = terminals.reduce(
      (sum, record) => sum + terminalMetadataBytes(record),
      0,
    );
    let metadataCount = terminals.length;
    for (const record of terminals) {
      if (
        metadataCount <= this.#terminalMetadataMaxRecords &&
        metadataBytes <= this.#terminalMetadataMaxBytes
      ) {
        break;
      }
      if (protectedIds.has(record.recordId)) continue;
      remove.add(record.recordId);
      metadataCount -= 1;
      metadataBytes -= terminalMetadataBytes(record);
    }
    for (const record of allRecords) {
      if (!remove.has(record.recordId)) continue;
      if (record.payloadRef) {
        deleteAfterCommit.push(
          makeBinaryPath(this.payloadDirectory, record.payloadRef.payloadId),
        );
      }
      for (const spool of record.spoolRefs) {
        deleteAfterCommit.push(
          makeBinaryPath(this.spoolDirectory, spool.spoolId),
        );
      }
    }
    snapshot.reassignments = snapshot.reassignments.filter(
      (entry) =>
        !entry.unresolvedRecordIds.some((recordId) => remove.has(recordId)),
    );
    snapshot.inbound = snapshot.inbound.filter(
      (record) => !remove.has(record.recordId),
    );
    snapshot.outbound = snapshot.outbound.filter(
      (record) => !remove.has(record.recordId),
    );
    snapshot.bus = snapshot.bus.filter(
      (record) => !remove.has(record.recordId),
    );
  }

  getStatus(): RecoveryMetadataStatus {
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const snapshot = this.#readSnapshotLocked();
      const counts = emptyCounts();
      const nowMs = this.#now();
      const actionIdsByRecord = new Map(
        [...this.#recordActionIds(snapshot)].map(([actionId, recordId]) => [
          recordId,
          actionId,
        ]),
      );
      const unresolved = snapshot.inbound.filter((record) => {
        counts[record.state] += 1;
        return isInboundUnresolved(record.state);
      });
      const items: RecoveryStatusItem[] = [
        ...unresolved.map((record): RecoveryStatusItem => ({
          id: actionIdsByRecord.get(record.recordId)!,
          actionId: actionIdsByRecord.get(record.recordId)!,
          family: "inbound",
          state: record.state,
          ageMs: Math.max(0, nowMs - record.createdAtMs),
          requiredAction:
            record.state === "execution-uncertain"
              ? "retry-or-discard"
              : record.state === "admitted" || record.state === "pre-dispatch"
                ? "drain"
                : "none",
        })),
        ...snapshot.outbound
          .filter(isUnresolvedRecord)
          .map((record): RecoveryStatusItem => ({
            id: actionIdsByRecord.get(record.recordId)!,
            actionId: actionIdsByRecord.get(record.recordId)!,
            family: "outbound",
            state: record.state,
            ageMs: Math.max(0, nowMs - record.createdAtMs),
            requiredAction:
              record.state === "delivery-uncertain" ||
              record.state === "sending"
                ? "retry-or-discard"
                : "drain",
          })),
        ...snapshot.bus
          .filter(isUnresolvedRecord)
          .map((record): RecoveryStatusItem => ({
            id: actionIdsByRecord.get(record.recordId)!,
            actionId: actionIdsByRecord.get(record.recordId)!,
            family: "bus",
            state: record.state,
            ageMs: Math.max(0, nowMs - record.createdAtMs),
            requiredAction:
              record.state === "bus-uncertain" ? "retry-or-discard" : "drain",
          })),
      ];
      return {
        profile: this.profile,
        mode: snapshot.mode,
        admissionEnabled:
          !this.#admissionDisabled && snapshot.mode === "active",
        committedUpdateId: snapshot.committedUpdateId,
        counts,
        quota: { ...snapshot.quota, limitBytes: this.quotaBytes },
        oldestUnresolvedAgeMs:
          items.length === 0
            ? null
            : Math.max(...items.map((item) => item.ageMs)),
        items,
        incidents: [...new Set(this.#incidents)],
      };
    });
  }

  downgradePreflight(): RecoveryDowngradePreflight {
    const status = this.getStatus();
    const blockers = status.items;
    return {
      safe: blockers.length === 0,
      blockerCount: blockers.length,
      blockers,
    };
  }

  beginDowngradeExclusive(): RecoveryDowngradePreflight {
    this.#admissionDisabled = true;
    try {
      return this.#commit((snapshot) => {
        const blockers = snapshot.inbound.filter((record) =>
          isInboundUnresolved(record.state),
        );
        if (
          blockers.length > 0 ||
          snapshot.outbound.some(isUnresolvedRecord) ||
          snapshot.bus.some(isUnresolvedRecord)
        ) {
          throw new Error("Recovery downgrade blocked by nonterminal records");
        }
        this.#fault?.("DOWN-01");
        snapshot.mode = "downgrade-exclusive";
        return { safe: true, blockerCount: 0, blockers: [] };
      });
    } catch (error) {
      this.#admissionDisabled = false;
      throw error;
    }
  }

  quarantineForDowngrade(): string {
    this.#admissionDisabled = true;
    return withTelegramFileTransaction(`${this.rootPath}.transaction`, () => {
      const snapshot = this.#readSnapshotForMutationLocked();
      if (snapshot.mode !== "downgrade-exclusive") {
        throw new Error("Recovery downgrade-exclusive preflight is required");
      }
      if (
        snapshot.inbound.some((record) => isInboundUnresolved(record.state)) ||
        snapshot.outbound.some(isUnresolvedRecord) ||
        snapshot.bus.some(isUnresolvedRecord)
      ) {
        throw new Error("Recovery downgrade blockers appeared after preflight");
      }
      const quarantinePath = join(
        dirname(this.rootPath),
        `${getQuarantineProfilePrefix(this.profile)}${this.#now()}-${this.#randomId()}`,
      );
      this.#fs.rename(this.rootPath, quarantinePath);
      this.#fault?.("DOWN-02");
      fsyncPath(this.#fs, dirname(this.rootPath));
      return quarantinePath;
    });
  }

  cancelDowngradeExclusive(): void {
    this.#commit((snapshot) => {
      if (snapshot.mode !== "downgrade-exclusive") {
        throw new Error("Recovery store is not in downgrade-exclusive mode");
      }
      snapshot.mode = "active";
    });
    this.#admissionDisabled = false;
  }
}

export function openRecoveryStore(
  options: RecoveryStoreOpenOptions,
): RecoveryStore {
  return new RecoveryStore(options).initialize();
}

export function restoreRecoveryQuarantine(options: {
  profile: string;
  quarantinePath: string;
  agentDir?: string;
  rootPath?: string;
  fs?: Partial<RecoveryFileSystem>;
  now?: () => number;
  randomId?: () => string;
}): string {
  const profile = readProfile(options.profile, "$profile");
  const rootPath =
    options.rootPath ?? resolveRecoveryStorePath(profile, options.agentDir);
  const fs: RecoveryFileSystem = {
    ...NODE_RECOVERY_FILE_SYSTEM,
    ...options.fs,
  };
  const quarantines = listQuarantines(fs, rootPath, profile);
  if (!quarantines.includes(options.quarantinePath)) {
    throw new Error(
      "Recovery quarantine path is not an exact compatible profile backup",
    );
  }
  return withTelegramFileTransaction(`${rootPath}.transaction`, () => {
    if (fs.exists(rootPath)) {
      throw new Error(
        "Cannot restore recovery quarantine over an active store",
      );
    }
    const snapshot = validateRecoveryStoreBackup(
      fs,
      options.quarantinePath,
      profile,
    );
    const active = cloneSnapshot(snapshot);
    active.mode = "active";
    active.revision += 1;
    active.writtenAtMs = (options.now ?? Date.now)();
    const serialized = serializeAccountedSnapshot(active);
    const restorePublicationPeak =
      physicalFileBytes(fs, options.quarantinePath) +
      Buffer.byteLength(serialized);
    if (restorePublicationPeak > RECOVERY_PROFILE_QUOTA_BYTES) {
      throw new RecoveryQuotaExceededError(
        restorePublicationPeak,
        RECOVERY_PROFILE_QUOTA_BYTES,
      );
    }
    fs.rename(options.quarantinePath, rootPath);
    const tempPath = `${join(rootPath, RECOVERY_SNAPSHOT_FILE_NAME)}${RECOVERY_TEMP_FILE_MARKER}${process.pid}-${(options.randomId ?? randomUUID)()}`;
    writePrivateFile(fs, tempPath, serialized);
    fs.rename(tempPath, join(rootPath, RECOVERY_SNAPSHOT_FILE_NAME));
    fsyncPath(fs, rootPath);
    fsyncPath(fs, dirname(rootPath));
    return rootPath;
  });
}
