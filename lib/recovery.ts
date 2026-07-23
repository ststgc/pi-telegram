/**
 * Durable recovery schema contracts and strict snapshot validation
 * Zones: recovery, queue lifecycle, delivery, multi-instance bus
 * Owns only the versioned serializable shapes and frozen policy constants used by
 * later recovery storage work; it performs no filesystem I/O or runtime activation.
 */

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
}

export interface RecoverySpoolReference {
  spoolId: string;
  byteLength: number;
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
  linkedAttemptOf?: string;
}

export interface RecoveryOutboundRecord extends RecoveryRecordBase {
  family: "outbound";
  intentId: string;
  turnId: string;
  state: RecoveryOutboundState;
  linkedAttemptOf?: string;
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
  inbound: RecoveryInboundRecord[];
  outbound: RecoveryOutboundRecord[];
  bus: RecoveryBusRecord[];
  reassignments: RecoveryReassignmentRecord[];
}

const PROFILE_PATTERN = /^[a-z0-9]{1,32}$/;
const RESERVED_PROFILES = new Set(["active", "main"]);
const CONTROL_CHARACTER_PATTERN = /[\u0000-\u001f\u007f]/;

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
    assertKeys(object, path, [
      "kind",
      "ownerId",
      "registrationGeneration",
    ]);
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
  assertKeys(object, path, ["payloadId", "byteLength"]);
  return {
    payloadId: readStableString(object.payloadId, `${path}.payloadId`),
    byteLength: readSafeInteger(object.byteLength, `${path}.byteLength`, {
      minimum: 0,
    }),
  };
}

function readSpoolRef(value: unknown, path: string): RecoverySpoolReference {
  const object = readObject(value, path);
  assertKeys(object, path, ["spoolId", "byteLength"]);
  return {
    spoolId: readStableString(object.spoolId, `${path}.spoolId`),
    byteLength: readSafeInteger(object.byteLength, `${path}.byteLength`, {
      minimum: 0,
    }),
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
    [...RECORD_BASE_OPTIONAL_KEYS, "linkedAttemptOf"],
  );
  if (object.family !== "inbound") fail(`${path}.family`, "expected inbound");
  const base = readRecordBase(object, path);
  const linkedAttemptOf = Object.hasOwn(object, "linkedAttemptOf")
    ? readStableString(object.linkedAttemptOf, `${path}.linkedAttemptOf`)
    : undefined;
  return {
    ...base,
    family: "inbound",
    updateId: readSafeInteger(object.updateId, `${path}.updateId`, {
      minimum: 0,
    }),
    turnId: readStableString(object.turnId, `${path}.turnId`),
    state: readEnum(object.state, `${path}.state`, RECOVERY_INBOUND_STATES),
    ...(linkedAttemptOf ? { linkedAttemptOf } : {}),
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
    [...RECORD_BASE_REQUIRED_KEYS, "intentId", "turnId", "state"],
    [...RECORD_BASE_OPTIONAL_KEYS, "linkedAttemptOf"],
  );
  if (object.family !== "outbound") fail(`${path}.family`, "expected outbound");
  const base = readRecordBase(object, path);
  const linkedAttemptOf = Object.hasOwn(object, "linkedAttemptOf")
    ? readStableString(object.linkedAttemptOf, `${path}.linkedAttemptOf`)
    : undefined;
  return {
    ...base,
    family: "outbound",
    intentId: readStableString(object.intentId, `${path}.intentId`),
    turnId: readStableString(object.turnId, `${path}.turnId`),
    state: readEnum(object.state, `${path}.state`, RECOVERY_OUTBOUND_STATES),
    ...(linkedAttemptOf ? { linkedAttemptOf } : {}),
  };
}

function readBusRecord(value: unknown, path: string): RecoveryBusRecord {
  const object = readObject(value, path);
  assertKeys(
    object,
    path,
    [
      ...RECORD_BASE_REQUIRED_KEYS,
      "requestId",
      "payloadFingerprint",
      "state",
    ],
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
    reservedBytes: readSafeInteger(object.reservedBytes, `${path}.reservedBytes`, {
      minimum: 0,
    }),
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
      return (RECOVERY_UNRESOLVED_OUTBOUND_STATES as readonly string[]).includes(
        record.state,
      );
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
  assertUniqueStrings(records.map((record) => record.recordId), "$.records");
  assertUniqueStrings(
    snapshot.reassignments.map((record) => record.reassignmentId),
    "$.reassignments",
  );
  assertUniqueStrings(
    snapshot.inbound.map((record) => String(record.updateId)),
    "$.inbound.updateId",
  );
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
  if (!Number.isSafeInteger(payloadBytes) || payloadBytes !== snapshot.quota.payloadBytes) {
    fail("$.quota.payloadBytes", "must equal referenced payload bytes");
  }
  if (!Number.isSafeInteger(spoolBytes) || spoolBytes !== snapshot.quota.spoolBytes) {
    fail("$.quota.spoolBytes", "must equal referenced spool bytes");
  }

  for (const record of records) {
    if (record.identity.profile !== snapshot.profile) {
      fail(`$.records.${record.recordId}.identity.profile`, "profile mismatch");
    }
    if (record.createdRevision > snapshot.revision) {
      fail(`$.records.${record.recordId}.createdRevision`, "exceeds snapshot revision");
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
      fail(`$.records.${record.recordId}.updatedAtMs`, "exceeds snapshot writtenAtMs");
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
          areRecoveryTargetsEqual(record.identity.target, reassignment.target) &&
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
