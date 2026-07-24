/**
 * Durable inbound wiring between Telegram polling, follower IPC, and Pi queue lifecycle
 * Zones: telegram, recovery, polling, queue lifecycle, multi-instance bus, filesystem
 * Owns role-neutral admission, offset authority, strict replay envelopes, durable
 * follower proofs, process-local handoffs, and exact owner/session fencing.
 */

import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import {
  isTelegramFollowerDurableAdmissionAckV1,
  type TelegramFollowerDurableAdmissionAckV1,
} from "./bus.ts";
import {
  getTelegramOperationOwnedFiles,
  setTelegramOperationOwnedFiles,
} from "./operation-files.ts";
import type {
  TelegramDurableInboundAdmission,
  TelegramPollingConfig,
} from "./polling.ts";
import type { PendingTelegramTurn } from "./queue.ts";
import {
  areRecoveryOwnersEqual,
  areRecoveryTargetsEqual,
  openRecoveryStore,
  RecoveryProfileOperationGate,
  type RecoveryDowngradePreflight,
  type RecoveryIdentity,
  type RecoveryInboundRecord,
  type RecoveryIdentityClaim,
  type RecoveryMetadataStatus,
  type RecoveryOrphanReassignmentCandidate,
  type RecoveryOutboundDrainItem,
  type RecoveryReassignmentActionResult,
  type RecoveryReassignmentBindingValidation,
  type RecoverySameProcessHandoff,
  type RecoveryStore,
} from "./recovery.ts";
import type { TelegramTarget } from "./target.ts";
import {
  planTelegramDurableInboundResponsibility,
  type TelegramDurableInboundResponsibility,
  type TelegramInboundHandlingOutcome,
  type TelegramMessageOwnershipLookup,
  type TelegramTargetOwnershipLookup,
} from "./updates.ts";

interface TelegramInboundRecoveryMessage {
  message_id?: number;
  message_thread_id?: number;
  business_connection_id?: string;
  date?: number;
  chat?: { id?: number; type?: string };
  from?: { id?: number; is_bot?: boolean };
  text?: string;
  entities?: unknown[];
}

export interface TelegramInboundRecoveryUpdate {
  update_id: number;
  message?: TelegramInboundRecoveryMessage;
  edited_message?: TelegramInboundRecoveryMessage;
  callback_query?: {
    from?: { id?: number; is_bot?: boolean };
    message?: TelegramInboundRecoveryMessage;
  };
  message_reaction?: TelegramInboundRecoveryMessage;
  deleted_business_messages?: object;
  guest_message?: {
    from?: { id?: number; is_bot?: boolean };
    chat?: { id?: number };
    message_thread_id?: number;
  };
}

export const INBOUND_RECOVERY_COMPACTION_INTERVAL_MS = 60 * 60 * 1000;

export type InboundRecoveryTimer = ReturnType<typeof setTimeout>;

export interface InboundRecoveryRuntimeDeps<
  TUpdate extends TelegramInboundRecoveryUpdate,
  TContext,
> {
  getProfile: () => string | undefined;
  getAllowedUserId: () => number | undefined;
  getCurrentInstanceId?: () => string | undefined;
  getMessageOwnership?: TelegramMessageOwnershipLookup;
  getTargetOwnership?: TelegramTargetOwnershipLookup;
  getSessionGeneration: () => number;
  isSessionActive: (ctx: TContext, generation?: number) => boolean;
  resolveCurrentIdentity: (
    target: TelegramTarget,
    ctx?: TContext,
  ) => RecoveryIdentity | undefined;
  isIdentityAuthenticated: (identity: RecoveryIdentity) => boolean;
  resolveOperatorIdentity?: (ctx: TContext) => RecoveryIdentity | undefined;
  refreshReassignmentBinding?: () => Promise<void>;
  validateReassignmentBinding?: (
    input: RecoveryReassignmentBindingValidation,
  ) => boolean;
  planResponsibility?: (
    update: TUpdate,
  ) => TelegramDurableInboundResponsibility;
  forwardUpdate?: (input: {
    update: TUpdate;
    profile: string;
    target: TelegramTarget;
    ownership: { instanceId: string; ownerGeneration: string };
  }) => Promise<TelegramFollowerDurableAdmissionAckV1 | undefined>;
  isFollowerAdmissionCurrent?: (
    proof: TelegramFollowerDurableAdmissionAckV1,
  ) => boolean;
  agentDir?: string;
  openStore?: typeof openRecoveryStore;
  operationGate?: RecoveryProfileOperationGate;
  now?: () => number;
  compactionIntervalMs?: number;
  setCompactionTimer?: (
    callback: () => void,
    delayMs: number,
  ) => InboundRecoveryTimer;
  clearCompactionTimer?: (timer: InboundRecoveryTimer) => void;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
}

export interface RecoveryReassignmentThreadRecordView {
  target: TelegramTarget;
  status?: string;
  owner?: {
    kind?: string;
    instanceId?: string;
    telegramProfile?: string;
  };
}

export interface RecoveryReassignmentLiveFollowerView {
  target?: TelegramTarget;
  manualFollowerOwnerId?: string;
  registrationGeneration?: string;
}

export interface RecoveryReassignmentBindingAuthorityInput {
  validation: RecoveryReassignmentBindingValidation;
  currentSessionGeneration: number;
  identityAuthenticated: boolean;
  threadRecords: readonly RecoveryReassignmentThreadRecordView[];
  liveFollowers: readonly RecoveryReassignmentLiveFollowerView[];
}

/**
 * Proves the current recovery grantee against live generation authority and the
 * refreshed target store. Deliberately excludes cwd: process location is not a
 * recovery ownership credential.
 */
export function validateRecoveryReassignmentBindingAuthority(
  input: RecoveryReassignmentBindingAuthorityInput,
): boolean {
  const { validation, currentSessionGeneration } = input;
  const { reassignment, currentIdentity } = validation;
  if (
    !input.identityAuthenticated ||
    currentIdentity.profile !== reassignment.profile ||
    currentIdentity.sessionGeneration !== currentSessionGeneration ||
    currentIdentity.sessionGeneration !== reassignment.newSessionGeneration ||
    !areRecoveryTargetsEqual(currentIdentity.target, reassignment.target) ||
    !areRecoveryOwnersEqual(currentIdentity.owner, reassignment.newOwner)
  ) {
    return false;
  }

  const bindingOwner =
    validation.expectedBinding === "new-owner"
      ? reassignment.newOwner
      : reassignment.oldOwner;
  const targetRecords = input.threadRecords.filter((record) =>
    areRecoveryTargetsEqual(record.target, reassignment.target),
  );
  const targetFollowers = input.liveFollowers.filter((follower) =>
    follower.target
      ? areRecoveryTargetsEqual(follower.target, reassignment.target)
      : false,
  );

  if (reassignment.target.threadId === undefined) {
    return (
      bindingOwner.kind === "leader" &&
      targetRecords.length === 0 &&
      targetFollowers.length === 0
    );
  }
  if (targetRecords.length !== 1) return false;
  const record = targetRecords[0]!;
  if (record.status !== "active" || !record.owner) return false;
  const recordProfile = record.owner.telegramProfile ?? "default";
  if (recordProfile !== reassignment.profile) return false;

  if (bindingOwner.kind === "leader") {
    return (
      targetFollowers.length === 0 &&
      record.owner.kind === "leader" &&
      record.owner.instanceId === bindingOwner.ownerId
    );
  }
  return (
    targetFollowers.length === 1 &&
    targetFollowers[0]!.manualFollowerOwnerId === bindingOwner.ownerId &&
    targetFollowers[0]!.registrationGeneration ===
      bindingOwner.registrationGeneration &&
    record.owner.kind === "manual-follower" &&
    record.owner.instanceId === bindingOwner.ownerId
  );
}

export interface InboundRecoveryRuntime<
  TUpdate extends TelegramInboundRecoveryUpdate,
  TContext,
> {
  initializeOffset(config: TelegramPollingConfig): Promise<number | undefined>;
  admitUpdate(update: TUpdate, ctx: TContext): Promise<TelegramDurableInboundAdmission>;
  admitForwardedUpdate(
    update: TUpdate,
    ctx: TContext,
    expected: {
      profile: string;
      target: TelegramTarget;
      ownerId: string;
      registrationGeneration: string;
      sessionGeneration: number;
    },
  ): Promise<TelegramFollowerDurableAdmissionAckV1>;
  commitUpdate(updateId: number): Promise<number>;
  verifyFollowerAdmissionProof(
    proof: TelegramFollowerDurableAdmissionAckV1,
  ): boolean;
  handleAdmittedUpdate(
    update: TUpdate,
    ctx: TContext,
    handle: (update: TUpdate, ctx: TContext) => Promise<unknown>,
  ): Promise<void>;
  markPoisonSkipped(updateId: number): Promise<void>;
  decorateTurn<TTurn extends PendingTelegramTurn>(
    messages: readonly TelegramInboundRecoveryMessage[],
    turn: TTurn,
  ): TTurn;
  claimTurnDispatch(turn: PendingTelegramTurn): boolean;
  getTurnOutboundClaim(turn: PendingTelegramTurn): RecoveryIdentityClaim;
  getOutboundStore(): RecoveryStore;
  releaseTurnAfterOutboundHandoff(turn: PendingTelegramTurn): void;
  markTurnDispatchFailed(turn: PendingTelegramTurn): void;
  completeTurn(turn: PendingTelegramTurn): void;
  settleDeferredMessages(
    messages: readonly TelegramInboundRecoveryMessage[],
    outcome: TelegramInboundHandlingOutcome,
  ): void;
  recordDeferredFailure(
    messages: readonly TelegramInboundRecoveryMessage[],
    error: unknown,
  ): void;
  terminalizeDeletedMessageIds(
    messageIds: readonly number[],
    scope?: {
      profile?: string;
      chatId?: number;
      exactThreadId?: number | null;
      businessConnectionId?: string;
    },
  ): void;
  publishSessionHandoffs(toSessionGeneration: number): number;
  rehydrateOutbound(
    ctx: TContext,
    schedule: (
      item: RecoveryOutboundDrainItem,
      claim: RecoveryIdentityClaim,
    ) => void | Promise<void>,
  ): Promise<void>;
  rehydrate(
    ctx: TContext,
    handle: (update: TUpdate, ctx: TContext) => Promise<unknown>,
    appendTurn: (turn: PendingTelegramTurn) => void,
  ): Promise<void>;
  getRecoveryStatus(): RecoveryMetadataStatus;
  getOrphanReassignmentCandidates(): RecoveryOrphanReassignmentCandidate[];
  drainSafeForOperator(
    ctx: TContext,
    handle: (update: TUpdate, ctx: TContext) => Promise<unknown>,
    appendTurn: (turn: PendingTelegramTurn) => boolean,
    scheduleOutbound: (
      item: RecoveryOutboundDrainItem,
      claim: RecoveryIdentityClaim,
    ) => void | Promise<void>,
  ): Promise<number>;
  retryUncertainForOperator(
    actionId: string,
    ctx: TContext,
    handle: (update: TUpdate, ctx: TContext) => Promise<unknown>,
    appendTurn: (turn: PendingTelegramTurn) => boolean,
    scheduleOutbound: (
      item: RecoveryOutboundDrainItem,
      claim: RecoveryIdentityClaim,
    ) => void | Promise<void>,
  ): Promise<{ scheduled: boolean; duplicationWarning: true }>;
  discardForOperator(
    actionId: string,
    ctx: TContext,
    afterDurableDiscard: (turnId: string) => void,
  ): void;
  reassignForOperator(
    actionId: string,
    ctx: TContext,
  ): Promise<RecoveryReassignmentActionResult>;
  downgradePreflight(): RecoveryDowngradePreflight;
  enterOperation(): { release(): void } | undefined;
  runGatedOperation<TResult>(
    operation: () => TResult | Promise<TResult>,
  ): Promise<TResult>;
  operationGate: RecoveryProfileOperationGate;
  beginDowngradeExclusive(): RecoveryDowngradePreflight;
  quarantineForDowngrade(): string;
  cancelDowngradeExclusive(): void;
}

interface RecoverySpoolMapping {
  index: number;
  originalPath: string;
  fileName: string;
}

type RecoveryPayloadEnvelope =
  | { version: 1; kind: "update"; update: TelegramInboundRecoveryUpdate }
  | {
      version: 1;
      kind: "turn";
      turn: PendingTelegramTurn;
      spool: RecoverySpoolMapping[];
    };

interface AdmittedUpdate {
  record: RecoveryInboundRecord;
  identity: RecoveryIdentity;
  sourcePayload?: Uint8Array;
  admission: TelegramDurableInboundAdmission;
}

const HANDOFF_REGISTRY_KEY = Symbol.for(
  "@ststgc/pi-telegram/local-inbound-handoffs-v1",
);
const HANDOFF_TTL_MS = 60_000;

function getHandoffRegistry(): Map<string, RecoverySameProcessHandoff[]> {
  const globals = globalThis as Record<PropertyKey, unknown>;
  const existing = globals[HANDOFF_REGISTRY_KEY];
  if (existing instanceof Map) {
    return existing as Map<string, RecoverySameProcessHandoff[]>;
  }
  const registry = new Map<string, RecoverySameProcessHandoff[]>();
  globals[HANDOFF_REGISTRY_KEY] = registry;
  return registry;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

const DEFERRED_OUTCOME_REASONS = new Set([
  "text-group",
  "media-group",
  "operator-reroute",
  "follower-admission-pending",
  "session-replay",
]);
const COMPLETED_OUTCOME_REASONS = new Set([
  "ignored",
  "deleted",
  "reaction",
  "topic-lifecycle",
  "callback",
  "guest",
  "command",
  "menu",
  "public-handler",
  "unauthorized",
  "unsupported",
]);

function requireInboundHandlingOutcome(
  value: unknown,
): TelegramInboundHandlingOutcome {
  if (!isPlainObject(value) || typeof value.kind !== "string") {
    throw new Error("Telegram inbound handler returned no durable outcome");
  }
  switch (value.kind) {
    case "prompt-materialized":
      assertExactKeys(value, ["kind", "turnId", "recordIds"]);
      if (
        typeof value.turnId === "string" &&
        value.turnId.length > 0 &&
        Array.isArray(value.recordIds) &&
        value.recordIds.every(
          (recordId) => typeof recordId === "string" && recordId.length > 0,
        )
      ) {
        return value as unknown as TelegramInboundHandlingOutcome;
      }
      break;
    case "deferred":
      assertExactKeys(value, ["kind", "reason", "key"]);
      if (
        DEFERRED_OUTCOME_REASONS.has(String(value.reason)) &&
        typeof value.key === "string" &&
        value.key.length > 0
      ) {
        return value as unknown as TelegramInboundHandlingOutcome;
      }
      break;
    case "completed":
      assertExactKeys(value, ["kind", "reason"]);
      if (COMPLETED_OUTCOME_REASONS.has(String(value.reason))) {
        return value as unknown as TelegramInboundHandlingOutcome;
      }
      break;
    case "follower-admitted":
      assertExactKeys(value, ["kind", "admission"]);
      if (isTelegramFollowerDurableAdmissionAckV1(value.admission)) {
        return value as unknown as TelegramInboundHandlingOutcome;
      }
      break;
  }
  throw new Error("Telegram inbound handler returned an invalid durable outcome");
}

function assertExactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  const allowed = new Set([...required, ...optional]);
  for (const key of required) {
    if (!Object.hasOwn(value, key)) throw new Error(`Missing durable envelope key: ${key}`);
  }
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new Error(`Unknown durable envelope key: ${key}`);
  }
}

function validateRecoveryTurn(value: unknown): PendingTelegramTurn {
  if (!isPlainObject(value)) throw new Error("Invalid durable turn");
  assertExactKeys(
    value,
    [
      "kind",
      "chatId",
      "replyToMessageId",
      "sourceMessageIds",
      "queueOrder",
      "queueLane",
      "laneOrder",
      "queuedAttachments",
      "content",
      "historyText",
      "statusSummary",
      "recovery",
    ],
    [
      "target",
      "transportStamp",
      "businessConnectionId",
      "guestQueryId",
      "priorityEmoji",
      "voiceReplyPreferred",
      "voiceReplyRequired",
      "recoveryFiles",
    ],
  );
  if (
    value.kind !== "prompt" ||
    typeof value.chatId !== "number" ||
    (value.businessConnectionId !== undefined &&
      typeof value.businessConnectionId !== "string") ||
    typeof value.replyToMessageId !== "number" ||
    !Array.isArray(value.sourceMessageIds) ||
    !value.sourceMessageIds.every((id) => Number.isSafeInteger(id)) ||
    typeof value.queueOrder !== "number" ||
    value.queueLane !== "default" &&
      value.queueLane !== "priority" &&
      value.queueLane !== "control" ||
    typeof value.laneOrder !== "number" ||
    !Array.isArray(value.queuedAttachments) ||
    !Array.isArray(value.content) ||
    typeof value.historyText !== "string" ||
    typeof value.statusSummary !== "string"
  ) {
    throw new Error("Invalid durable turn fields");
  }
  const recovery = value.recovery;
  if (
    !isPlainObject(recovery) ||
    !Array.isArray(recovery.recordIds) ||
    recovery.recordIds.length === 0 ||
    !recovery.recordIds.every((id) => typeof id === "string" && id.length > 0) ||
    typeof recovery.turnId !== "string" ||
    recovery.turnId.length === 0
  ) {
    throw new Error("Invalid durable turn recovery identity");
  }
  for (const item of value.content) {
    if (!isPlainObject(item) || (item.type !== "text" && item.type !== "image")) {
      throw new Error("Invalid durable turn content");
    }
    if (item.type === "text" && typeof item.text !== "string") {
      throw new Error("Invalid durable text content");
    }
    if (
      item.type === "image" &&
      (typeof item.data !== "string" || typeof item.mimeType !== "string")
    ) {
      throw new Error("Invalid durable image content");
    }
  }
  if (value.recoveryFiles !== undefined) {
    if (!Array.isArray(value.recoveryFiles)) throw new Error("Invalid recovery files");
    for (const file of value.recoveryFiles) {
      if (
        !isPlainObject(file) ||
        typeof file.path !== "string" ||
        typeof file.fileName !== "string"
      ) {
        throw new Error("Invalid recovery file mapping");
      }
    }
  }
  return structuredClone(value) as unknown as PendingTelegramTurn;
}

function encodeEnvelope(envelope: RecoveryPayloadEnvelope): Uint8Array {
  return Buffer.from(JSON.stringify(envelope), "utf8");
}

function decodeEnvelope(payload: Uint8Array): RecoveryPayloadEnvelope {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(payload).toString("utf8"));
  } catch {
    throw new Error("Invalid durable inbound payload JSON");
  }
  if (!isPlainObject(parsed)) throw new Error("Invalid durable inbound envelope");
  if (parsed.kind === "update") {
    assertExactKeys(parsed, ["version", "kind", "update"]);
    if (parsed.version !== 1 || !isPlainObject(parsed.update)) {
      throw new Error("Invalid durable update envelope");
    }
    assertExactKeys(parsed.update, ["update_id"], [
      "message",
      "edited_message",
      "callback_query",
      "message_reaction",
      "deleted_business_messages",
      "guest_message",
      "my_chat_member",
      "chat_member",
      "chat_join_request",
      "business_connection",
      "business_message",
      "edited_business_message",
      "purchased_paid_media",
      "poll",
      "poll_answer",
      "inline_query",
      "chosen_inline_result",
      "shipping_query",
      "pre_checkout_query",
    ]);
    if (!Number.isSafeInteger(parsed.update.update_id)) {
      throw new Error("Invalid durable update id");
    }
    return {
      version: 1,
      kind: "update",
      update: structuredClone(parsed.update) as unknown as TelegramInboundRecoveryUpdate,
    };
  }
  if (parsed.kind === "turn") {
    assertExactKeys(parsed, ["version", "kind", "turn", "spool"]);
    if (parsed.version !== 1 || !Array.isArray(parsed.spool)) {
      throw new Error("Invalid durable turn envelope");
    }
    const spool = parsed.spool.map((entry, index) => {
      if (!isPlainObject(entry)) throw new Error("Invalid durable spool mapping");
      assertExactKeys(entry, ["index", "originalPath", "fileName"]);
      if (
        entry.index !== index ||
        typeof entry.originalPath !== "string" ||
        entry.originalPath.length === 0 ||
        typeof entry.fileName !== "string" ||
        entry.fileName.length === 0
      ) {
        throw new Error("Invalid durable spool mapping fields");
      }
      return {
        index,
        originalPath: entry.originalPath,
        fileName: entry.fileName,
      };
    });
    const turn = validateRecoveryTurn(parsed.turn);
    if ((turn.recoveryFiles?.length ?? 0) !== spool.length) {
      throw new Error("Durable spool mapping does not match turn files");
    }
    return { version: 1, kind: "turn", turn, spool };
  }
  throw new Error("Unsupported durable inbound envelope");
}

function getUpdateMessage(
  update: TelegramInboundRecoveryUpdate,
): TelegramInboundRecoveryMessage | undefined {
  return (
    update.message ??
    update.edited_message ??
    update.callback_query?.message ??
    update.message_reaction
  );
}

function getSourceMessageIds(update: TelegramInboundRecoveryUpdate): number[] {
  const message = getUpdateMessage(update);
  return typeof message?.message_id === "number" ? [message.message_id] : [];
}

function readTurnSpool(turn: PendingTelegramTurn): {
  bytes: Uint8Array[];
  mapping: RecoverySpoolMapping[];
} {
  const files = turn.recoveryFiles ?? [];
  return {
    bytes: files.map((file) => readFileSync(file.path)),
    mapping: files.map((file, index) => ({
      index,
      originalPath: file.path,
      fileName: file.fileName,
    })),
  };
}

function rewriteTurnPaths(
  turn: PendingTelegramTurn,
  mapping: readonly RecoverySpoolMapping[],
  restoredPaths: readonly string[],
): PendingTelegramTurn {
  if (mapping.length !== restoredPaths.length) {
    throw new Error("Restored recovery spool mapping mismatch");
  }
  const replacements = new Map(
    mapping.map((entry, index) => [entry.originalPath, restoredPaths[index]!] as const),
  );
  const replace = (value: string): string => {
    let result = value;
    for (const [from, to] of replacements) result = result.split(from).join(to);
    return result;
  };
  const restored = structuredClone(turn);
  restored.content = restored.content.map((item) =>
    item.type === "text" ? { ...item, text: replace(item.text) } : item,
  );
  restored.historyText = replace(restored.historyText);
  restored.statusSummary = replace(restored.statusSummary);
  if (restored.recoveryFiles) {
    restored.recoveryFiles = restored.recoveryFiles.map((file) => ({
      ...file,
      path: replacements.get(file.path) ?? file.path,
    }));
  }
  return restored;
}

export function createInboundRecoveryRuntime<
  TUpdate extends TelegramInboundRecoveryUpdate,
  TContext,
>(
  deps: InboundRecoveryRuntimeDeps<TUpdate, TContext>,
): InboundRecoveryRuntime<TUpdate, TContext> {
  let store: RecoveryStore | undefined;
  let storeProfile: string | undefined;
  const admittedByUpdate = new Map<number, AdmittedUpdate>();
  const recordsByMessageId = new Map<number, Map<string, AdmittedUpdate>>();
  const identityByRecordId = new Map<string, RecoveryIdentity>();
  const followerProofByUpdate = new Map<
    number,
    TelegramFollowerDurableAdmissionAckV1
  >();
  const operatorReplayedTurnIds = new Set<string>();
  const dispatchLeasesByTurnId = new Map<string, { release(): void }>();
  const now = deps.now ?? Date.now;
  const compactionIntervalMs =
    deps.compactionIntervalMs ?? INBOUND_RECOVERY_COMPACTION_INTERVAL_MS;
  if (
    !Number.isSafeInteger(compactionIntervalMs) ||
    compactionIntervalMs <= 0 ||
    compactionIntervalMs > INBOUND_RECOVERY_COMPACTION_INTERVAL_MS
  ) {
    throw new Error("Invalid inbound recovery compaction interval");
  }
  const setCompactionTimer =
    deps.setCompactionTimer ??
    ((callback: () => void, delayMs: number) => setTimeout(callback, delayMs));
  const clearCompactionTimer =
    deps.clearCompactionTimer ??
    ((timer: InboundRecoveryTimer) => clearTimeout(timer));
  let compactionTimer: InboundRecoveryTimer | undefined;
  const operationGate = deps.operationGate ?? new RecoveryProfileOperationGate();
  const getProfile = (): string => deps.getProfile() ?? "default";
  const enterOperation = (): { release(): void } | undefined =>
    operationGate.enter(getProfile());
  const requireOperation = (): { release(): void } => {
    const lease = enterOperation();
    if (!lease) throw new Error("Telegram recovery operations are fenced");
    return lease;
  };
  const runGatedOperation = async <TResult>(
    operation: () => TResult | Promise<TResult>,
  ): Promise<TResult> => {
    const lease = requireOperation();
    try {
      return await operation();
    } finally {
      lease.release();
    }
  };
  const runGatedOperationSync = <TResult>(operation: () => TResult): TResult => {
    const lease = requireOperation();
    try {
      return operation();
    } finally {
      lease.release();
    }
  };

  const isIdentityAuthenticated = (identity: RecoveryIdentity): boolean =>
    identity.profile === getProfile() &&
    deps.isIdentityAuthenticated(identity);

  const clearScheduledCompaction = (): void => {
    if (!compactionTimer) return;
    clearCompactionTimer(compactionTimer);
    compactionTimer = undefined;
  };
  const scheduleCompaction = (profile: string, recoveryStore: RecoveryStore): void => {
    clearScheduledCompaction();
    compactionTimer = setCompactionTimer(() => {
      compactionTimer = undefined;
      if (store !== recoveryStore || storeProfile !== profile) return;
      const lease = operationGate.enter(profile);
      if (lease) {
        try {
          recoveryStore.compact();
        } catch (error) {
          deps.recordRuntimeEvent?.("recovery", error, {
            action: "periodic-compaction",
          });
        } finally {
          lease.release();
        }
      }
      if (
        store === recoveryStore &&
        storeProfile === profile &&
        operationGate.getState(profile).phase !== "quarantined"
      ) {
        scheduleCompaction(profile, recoveryStore);
      }
    }, compactionIntervalMs);
    compactionTimer.unref?.();
  };

  const getStore = (): RecoveryStore => {
    const profile = getProfile();
    if (!store || storeProfile !== profile) {
      clearScheduledCompaction();
      const opened = (deps.openStore ?? openRecoveryStore)({
        profile,
        agentDir: deps.agentDir,
        isIdentityAuthenticated,
        validateReassignmentBinding: deps.validateReassignmentBinding,
      });
      opened.compact();
      store = opened;
      storeProfile = profile;
      scheduleCompaction(profile, opened);
      admittedByUpdate.clear();
      recordsByMessageId.clear();
      identityByRecordId.clear();
      followerProofByUpdate.clear();
      operatorReplayedTurnIds.clear();
      for (const lease of dispatchLeasesByTurnId.values()) lease.release();
      dispatchLeasesByTurnId.clear();
    }
    return store;
  };

  const createIdentity = (
    target: TelegramTarget,
    ctx?: TContext,
  ): RecoveryIdentity => {
    const generation = deps.getSessionGeneration();
    if (ctx !== undefined && !deps.isSessionActive(ctx, generation)) {
      throw new Error("Stale session cannot admit recovery work");
    }
    const identity = deps.resolveCurrentIdentity(target, ctx);
    if (
      !identity ||
      identity.profile !== (deps.getProfile() ?? "default") ||
      identity.sessionGeneration !== generation ||
      !areRecoveryTargetsEqual(identity.target, target) ||
      !isIdentityAuthenticated(identity)
    ) {
      throw new Error("Telegram recovery identity is not currently authenticated");
    }
    return structuredClone(identity);
  };

  const remember = (
    update: TelegramInboundRecoveryUpdate,
    admitted: AdmittedUpdate,
  ): void => {
    admittedByUpdate.set(update.update_id, admitted);
    identityByRecordId.set(admitted.record.recordId, admitted.identity);
    for (const messageId of getSourceMessageIds(update)) {
      const matches = recordsByMessageId.get(messageId) ?? new Map();
      matches.set(admitted.record.recordId, admitted);
      recordsByMessageId.set(messageId, matches);
    }
  };

  const claimForRecord = (recordId: string): { identity: RecoveryIdentity } => {
    const identity = identityByRecordId.get(recordId);
    if (!identity) throw new Error("Recovery queue turn has no live identity claim");
    return { identity };
  };

  const matchesForMessages = (
    messages: readonly TelegramInboundRecoveryMessage[],
  ): AdmittedUpdate[] => {
    const matches = new Map<string, AdmittedUpdate>();
    for (const message of messages) {
      if (typeof message.message_id !== "number") continue;
      const admitted = recordsByMessageId.get(message.message_id);
      for (const candidate of admitted?.values() ?? []) {
        if (candidate.admission.kind === "admitted") {
          matches.set(candidate.record.recordId, candidate);
        }
      }
    }
    return [...matches.values()];
  };

  const settleMatches = (
    matches: readonly AdmittedUpdate[],
    outcome: TelegramInboundHandlingOutcome,
  ): void => {
    if (matches.length === 0) return;
    switch (outcome.kind) {
      case "completed": {
        const settled = getStore().terminalizeInboundGroup(
          matches.map(({ record }) => ({
            recordId: record.recordId,
            claim: claimForRecord(record.recordId),
          })),
        );
        for (let index = 0; index < matches.length; index += 1) {
          matches[index]!.record = settled[index]!;
        }
        return;
      }
      case "prompt-materialized": {
        const expected = new Set(matches.map(({ record }) => record.recordId));
        if (
          outcome.recordIds.length !== expected.size ||
          outcome.recordIds.some((recordId) => !expected.has(recordId))
        ) {
          throw new Error("Materialized Telegram outcome does not match its source group");
        }
        for (const match of matches) {
          if (
            match.record.state !== "pre-dispatch" ||
            match.record.turnId !== outcome.turnId
          ) {
            throw new Error("Materialized Telegram outcome lacks durable pre-dispatch proof");
          }
        }
        return;
      }
      case "deferred":
        if (!outcome.key) throw new Error("Deferred Telegram outcome requires a stable key");
        return;
      case "follower-admitted":
        throw new Error(
          "Follower durable admission proof validation is not wired for local recovery",
        );
    }
  };

  const planResponsibility = (
    update: TUpdate,
  ): TelegramDurableInboundResponsibility =>
    deps.planResponsibility?.(update) ??
    planTelegramDurableInboundResponsibility(
      update as unknown as Parameters<
        typeof planTelegramDurableInboundResponsibility
      >[0],
      {
        allowedUserId: deps.getAllowedUserId(),
        currentInstanceId: deps.getCurrentInstanceId?.(),
        getMessageOwnership: deps.getMessageOwnership,
        getTargetOwnership: deps.getTargetOwnership,
      },
    );

  const admitLocally = (
    update: TUpdate,
    ctx: TContext,
    responsibility: Exclude<
      TelegramDurableInboundResponsibility,
      { kind: "follower" }
    >,
  ): { admission: TelegramDurableInboundAdmission; record: RecoveryInboundRecord } => {
    const recoveryStore = getStore();
    const identity = createIdentity(responsibility.target, ctx);
    if (responsibility.kind === "terminal") {
      const admission: TelegramDurableInboundAdmission = {
        kind:
          responsibility.pairingProof && deps.getAllowedUserId() === undefined
            ? "pairing-proof"
            : "terminal",
      };
      const record = recoveryStore.recordTerminalInboundDisposition(
        update.update_id,
        identity,
        responsibility.reason,
      );
      remember(update, { record, identity, admission });
      return { admission, record };
    }
    const sourcePayload = encodeEnvelope({ version: 1, kind: "update", update });
    const observed = recoveryStore.observeInbound(update.update_id, identity);
    const record = recoveryStore.admitInbound({
      recordId: observed.recordId,
      payload: sourcePayload,
    });
    const admission = { kind: "admitted" as const };
    remember(update, { record, identity, sourcePayload, admission });
    return { admission, record };
  };

  const durableProofFor = (
    record: RecoveryInboundRecord,
  ): TelegramFollowerDurableAdmissionAckV1 => {
    if (
      record.identity.owner.kind !== "manual-follower" ||
      record.admissionRevision === undefined ||
      record.state === "observed"
    ) {
      throw new Error("Follower recovery record is not durably acknowledged");
    }
    return {
      version: 1,
      updateId: record.updateId,
      recordId: record.recordId,
      turnId: record.turnId,
      profile: record.identity.profile,
      target: structuredClone(record.identity.target),
      ownerId: record.identity.owner.ownerId,
      registrationGeneration:
        record.identity.owner.registrationGeneration,
      sessionGeneration: record.identity.sessionGeneration,
      admissionRevision: record.admissionRevision,
      disposition:
        record.state === "completed" || record.state === "explicitly-discarded"
          ? "terminal"
          : "admitted",
    };
  };

  const resolveOperatorIdentity = (
    ctx: TContext,
  ): RecoveryIdentity | undefined => {
    const generation = deps.getSessionGeneration();
    if (!deps.isSessionActive(ctx, generation)) return undefined;
    const identity = deps.resolveOperatorIdentity?.(ctx);
    if (
      !identity ||
      identity.profile !== (deps.getProfile() ?? "default") ||
      identity.sessionGeneration !== generation ||
      !isIdentityAuthenticated(identity)
    ) {
      return undefined;
    }
    return structuredClone(identity);
  };

  const getOperatorIdentity = (ctx: TContext): RecoveryIdentity => {
    const identity = resolveOperatorIdentity(ctx);
    if (!identity) {
      throw new Error("Recovery operator identity is not currently authenticated");
    }
    return identity;
  };

  const replayRetriedEnvelope = async (
    actionId: string,
    ctx: TContext,
    handle: (update: TUpdate, ctx: TContext) => Promise<unknown>,
    appendTurn: (turn: PendingTelegramTurn) => boolean,
  ): Promise<{ appended: boolean; duplicationWarning: true }> => {
    const identity = getOperatorIdentity(ctx);
    const recoveryStore = getStore();
    const result = recoveryStore.retryUncertainInboundAction(actionId, {
      identity,
    });
    const record = recoveryStore
      .listReplayableInboundRecords()
      .find((candidate) => candidate.turnId === result.turnId);
    if (!record || !result.payload) {
      throw new Error("Linked recovery retry envelope is unavailable");
    }
    if (operatorReplayedTurnIds.has(record.turnId)) {
      return { appended: false, duplicationWarning: true };
    }
    identityByRecordId.set(record.recordId, identity);
    const envelope = decodeEnvelope(result.payload);
    if (envelope.kind === "turn") {
      const restoredPaths = recoveryStore.restoreInboundSpool(
        record.recordId,
        { identity },
        envelope.spool.map((entry) => entry.fileName),
      );
      const restored = rewriteTurnPaths(
        envelope.turn,
        envelope.spool,
        restoredPaths,
      );
      restored.recovery = {
        recordIds: [record.recordId],
        turnId: record.turnId,
      };
      const appended = appendTurn(restored);
      operatorReplayedTurnIds.add(record.turnId);
      return { appended, duplicationWarning: true };
    }
    remember(envelope.update, {
      record,
      identity,
      sourcePayload: result.payload,
      admission: { kind: "admitted" },
    });
    const outcome = requireInboundHandlingOutcome(
      await handle(envelope.update as TUpdate, ctx),
    );
    settleMatches([admittedByUpdate.get(envelope.update.update_id)!], outcome);
    operatorReplayedTurnIds.add(record.turnId);
    return { appended: true, duplicationWarning: true };
  };

  const consumePublishedHandoffs = (ctx: TContext): void => {
    const recoveryStore = getStore();
    const currentGeneration = deps.getSessionGeneration();
    const registry = getHandoffRegistry();
    const published = registry.get(recoveryStore.rootPath) ?? [];
    const remaining: RecoverySameProcessHandoff[] = [];
    for (const handoff of published) {
      if (
        handoff.toSessionGeneration !== currentGeneration ||
        handoff.expiresAtMs < now()
      ) {
        continue;
      }
      try {
        const currentIdentity = createIdentity(handoff.target, ctx);
        if (
          handoff.profile !== currentIdentity.profile ||
          !areRecoveryOwnersEqual(handoff.owner, currentIdentity.owner)
        ) {
          remaining.push(handoff);
          continue;
        }
        const consumed = recoveryStore.consumeSameProcessHandoff(
          handoff,
          currentIdentity,
        );
        for (const recordId of consumed.claimedRecordIds) {
          identityByRecordId.set(recordId, currentIdentity);
        }
      } catch {
        remaining.push(handoff);
      }
    }
    registry.set(recoveryStore.rootPath, remaining);
  };

  const runtime: InboundRecoveryRuntime<TUpdate, TContext> = {
    operationGate,
    enterOperation,
    runGatedOperation,

    getRecoveryStatus() {
      const lease = requireOperation();
      try {
        return getStore().getStatus();
      } finally {
        lease.release();
      }
    },

    getOrphanReassignmentCandidates() {
      const lease = requireOperation();
      try {
        return getStore().getOrphanReassignmentCandidates();
      } finally {
        lease.release();
      }
    },

    async drainSafeForOperator(ctx, handle, appendTurn, scheduleOutbound) {
      return runGatedOperation(async () => {
        const identity = getOperatorIdentity(ctx);
        const claim = { identity };
        const outbound = getStore().drainSafeOutbound(claim);
        for (const item of outbound) {
          await scheduleOutbound(item, claim);
        }
        let appended = 0;
        await runtime.rehydrate(ctx, handle, (turn) => {
          if (appendTurn(turn)) appended += 1;
        });
        return outbound.length + appended;
      });
    },

    retryUncertainForOperator(
      actionId,
      ctx,
      handle,
      appendTurn,
      scheduleOutbound,
    ) {
      return runGatedOperation(async () => {
        const recoveryStore = getStore();
        const item = recoveryStore
          .getStatus()
          .items.find((candidate) => candidate.actionId === actionId);
        if (!item) throw new Error("Unknown recovery action id");
        if (item.family === "inbound") {
          const result = await replayRetriedEnvelope(
            actionId,
            ctx,
            handle,
            appendTurn,
          );
          return {
            scheduled: result.appended,
            duplicationWarning: true,
          };
        }
        if (item.family !== "outbound") {
          throw new Error("Recovery bus actions are not supported");
        }
        const identity = getOperatorIdentity(ctx);
        const claim = { identity };
        const result = recoveryStore.retryUncertainOutboundAction(
          actionId,
          `operator-retry-v1:${actionId}`,
          claim,
        );
        await scheduleOutbound(result, claim);
        return { scheduled: true, duplicationWarning: true };
      });
    },

    discardForOperator(actionId, ctx, afterDurableDiscard) {
      runGatedOperationSync(() => {
        const identity = getOperatorIdentity(ctx);
        const recoveryStore = getStore();
        const item = recoveryStore
          .getStatus()
          .items.find((candidate) => candidate.actionId === actionId);
        if (!item) throw new Error("Unknown recovery action id");
        if (item.family === "inbound") {
          const result = recoveryStore.discardInboundAction(actionId, {
            identity,
          });
          afterDurableDiscard(result.turnId);
          return;
        }
        if (item.family !== "outbound") {
          throw new Error("Recovery bus actions are not supported");
        }
        const result = recoveryStore.discardOutboundAction(actionId, {
          identity,
        });
        afterDurableDiscard(result.turnId);
      });
    },

    async reassignForOperator(actionId, ctx) {
      return runGatedOperation(async () => {
        const identity = getOperatorIdentity(ctx);
        const recoveryStore = getStore();
        const requested = recoveryStore.requestReassignmentAction(
          actionId,
          identity,
        );
        await deps.refreshReassignmentBinding?.();
        return recoveryStore.completeReassignmentAction(
          requested.actionId,
          identity,
        );
      });
    },

    downgradePreflight() {
      return runGatedOperationSync(() => getStore().downgradePreflight());
    },

    beginDowngradeExclusive() {
      return getStore().beginDowngradeExclusive();
    },

    quarantineForDowngrade() {
      clearScheduledCompaction();
      return getStore().quarantineForDowngrade();
    },

    cancelDowngradeExclusive() {
      const recoveryStore = getStore();
      recoveryStore.cancelDowngradeExclusive();
      scheduleCompaction(getProfile(), recoveryStore);
    },

    async initializeOffset(config) {
      return runGatedOperation(() => {
        const recoveryStore = getStore();
        let committed = recoveryStore.getCommittedUpdateId();
        if (committed === null && config.lastUpdateId !== undefined) {
          committed = recoveryStore.migrateCommittedUpdateId(config.lastUpdateId);
        }
        return committed ?? undefined;
      });
    },

    async admitUpdate(update, ctx) {
      return runGatedOperation(async () => {
        const responsibility = planResponsibility(update);
        if (responsibility.kind !== "follower") {
          return admitLocally(update, ctx, responsibility).admission;
        }
        const proof = await deps.forwardUpdate?.({
          update,
          profile: getProfile(),
          target: responsibility.target,
          ownership: {
            instanceId: responsibility.instanceId,
            ownerGeneration: responsibility.registrationGeneration,
          },
        });
        if (
          !isTelegramFollowerDurableAdmissionAckV1(proof) ||
          proof.updateId !== update.update_id ||
          proof.profile !== getProfile() ||
          proof.target.chatId !== responsibility.target.chatId ||
          proof.target.threadId !== responsibility.target.threadId ||
          proof.registrationGeneration !==
            responsibility.registrationGeneration ||
          deps.isFollowerAdmissionCurrent?.(proof) === false
        ) {
          throw new Error("Follower durable admission proof is absent or stale");
        }
        getStore().verifyInboundAdmissionProof(proof);
        followerProofByUpdate.set(update.update_id, proof);
        return { kind: "follower-admitted" };
      });
    },

    async admitForwardedUpdate(update, ctx, expected) {
      return runGatedOperation(async () => {
        const responsibility = planResponsibility(update);
        if (responsibility.kind === "follower") {
          throw new Error("Forwarded update resolved to another follower");
        }
        const identity = createIdentity(expected.target, ctx);
        if (
          expected.profile !== identity.profile ||
          expected.sessionGeneration !== identity.sessionGeneration ||
          identity.owner.kind !== "manual-follower" ||
          identity.owner.ownerId !== expected.ownerId ||
          identity.owner.registrationGeneration !==
            expected.registrationGeneration ||
          !areRecoveryTargetsEqual(identity.target, responsibility.target)
        ) {
          throw new Error("Forwarded update recovery identity mismatch");
        }
        const { record } = admitLocally(update, ctx, responsibility);
        return durableProofFor(record);
      });
    },

    async commitUpdate(updateId) {
      return runGatedOperation(() => {
        const proof = followerProofByUpdate.get(updateId);
        if (proof) {
          if (deps.isFollowerAdmissionCurrent?.(proof) === false) {
            throw new Error("Follower durable admission proof became stale");
          }
          getStore().verifyInboundAdmissionProof(proof);
        }
        const committed = getStore().commitUpdatePrefix(updateId);
        followerProofByUpdate.delete(updateId);
        return committed;
      });
    },

    verifyFollowerAdmissionProof(proof) {
      try {
        if (deps.isFollowerAdmissionCurrent?.(proof) === false) return false;
        getStore().verifyInboundAdmissionProof(proof);
        return true;
      } catch {
        return false;
      }
    },

    async handleAdmittedUpdate(update, ctx, handle) {
      const outcome = requireInboundHandlingOutcome(await handle(update, ctx));
      const admitted = admittedByUpdate.get(update.update_id);
      if (
        admitted &&
        (admitted.record.state === "admitted" ||
          admitted.record.state === "pre-dispatch")
      ) {
        settleMatches([admitted], outcome);
      }
    },

    async markPoisonSkipped(updateId) {
      await runGatedOperation(() => {
        const admitted = admittedByUpdate.get(updateId);
        if (!admitted || admitted.record.state === "explicitly-discarded") return;
        getStore().markPoisonSkippedInbound(admitted.record.recordId, {
          identity: admitted.identity,
        });
      });
    },

    decorateTurn(messages, turn) {
      const operationFiles = getTelegramOperationOwnedFiles(turn);
      try {
        return runGatedOperationSync(() => {
          const records = matchesForMessages(messages);
          if (records.length === 0) return turn;
          const recovery = {
            recordIds: records.map(({ record }) => record.recordId),
            turnId: records[0]!.record.turnId,
          };
          const decorated = { ...turn, recovery } as typeof turn;
          setTelegramOperationOwnedFiles(decorated, operationFiles);
          const spool = readTurnSpool(decorated);
          const payload = encodeEnvelope({
            version: 1,
            kind: "turn",
            turn: decorated,
            spool: spool.mapping,
          });
          const recoveryStore = getStore();
          const materialized = recoveryStore.materializeInboundGroup(
            records.map((admitted) => ({
              recordId: admitted.record.recordId,
              claim: { identity: admitted.identity },
              turnId: recovery.turnId,
              payload,
              spool: spool.bytes,
              ...(admitted.sourcePayload
                ? { previousPayload: admitted.sourcePayload }
                : {}),
            })),
          );
          for (let index = 0; index < records.length; index += 1) {
            const admitted = records[index]!;
            admitted.record = materialized[index]!;
            admitted.sourcePayload = undefined;
            identityByRecordId.set(admitted.record.recordId, admitted.identity);
          }
          const primary = records[0]!;
          const restoredPaths = recoveryStore.restoreInboundSpool(
            primary.record.recordId,
            { identity: primary.identity },
            spool.mapping.map((entry) => entry.fileName),
          );
          const restored = rewriteTurnPaths(
            decorated,
            spool.mapping,
            restoredPaths,
          );
          for (const file of operationFiles) {
            try {
              file.cleanupSync();
            } catch {
              // Durable paths are already authoritative; cleanup is fail-soft.
            }
          }
          return restored as typeof turn;
        });
      } catch (error) {
        for (const file of operationFiles) {
          try {
            file.cleanupSync();
          } catch {
            // Preserve the materialization error; cleanup is fail-soft.
          }
        }
        throw error;
      }
    },

    claimTurnDispatch(turn) {
      const recovery = turn.recovery;
      const lease = enterOperation();
      if (!lease) return false;
      if (!recovery) {
        lease.release();
        return true;
      }
      try {
        getStore().markDispatchingGroup(
          recovery.recordIds.map((recordId) => ({
            recordId,
            claim: claimForRecord(recordId),
          })),
        );
        dispatchLeasesByTurnId.set(recovery.turnId, lease);
        return true;
      } catch (error) {
        lease.release();
        deps.recordRuntimeEvent?.("recovery", error, {
          phase: "dispatch-claim",
          recordCount: recovery.recordIds.length,
        });
        return false;
      }
    },

    getTurnOutboundClaim(turn) {
      const recovery = turn.recovery;
      if (!recovery || recovery.recordIds.length === 0) {
        throw new Error("Telegram turn has no durable inbound identity");
      }
      if (!dispatchLeasesByTurnId.has(recovery.turnId)) {
        throw new Error("Telegram turn has no active durable dispatch lease");
      }
      const claim = claimForRecord(recovery.recordIds[0]!);
      for (const recordId of recovery.recordIds.slice(1)) {
        const candidate = claimForRecord(recordId);
        if (
          candidate.identity.profile !== claim.identity.profile ||
          !areRecoveryTargetsEqual(
            candidate.identity.target,
            claim.identity.target,
          ) ||
          !areRecoveryOwnersEqual(
            candidate.identity.owner,
            claim.identity.owner,
          ) ||
          candidate.identity.sessionGeneration !==
            claim.identity.sessionGeneration
        ) {
          throw new Error("Telegram turn recovery group identity mismatch");
        }
      }
      return { identity: structuredClone(claim.identity) };
    },

    getOutboundStore() {
      return getStore();
    },

    releaseTurnAfterOutboundHandoff(turn) {
      const recovery = turn.recovery;
      if (!recovery) {
        throw new Error("Telegram turn has no durable inbound relationship");
      }
      const lease = dispatchLeasesByTurnId.get(recovery.turnId);
      if (!lease) {
        throw new Error("Telegram turn durable dispatch lease is absent");
      }
      dispatchLeasesByTurnId.delete(recovery.turnId);
      lease.release();
    },

    markTurnDispatchFailed(turn) {
      const recovery = turn.recovery;
      if (!recovery) return;
      try {
        getStore().markExecutionUncertainGroup(
          recovery.recordIds.map((recordId) => ({
            recordId,
            claim: claimForRecord(recordId),
          })),
        );
      } catch (error) {
        deps.recordRuntimeEvent?.("recovery", error, {
          phase: "dispatch-failure",
          recordCount: recovery.recordIds.length,
        });
        throw error;
      }
      const lease = dispatchLeasesByTurnId.get(recovery.turnId);
      dispatchLeasesByTurnId.delete(recovery.turnId);
      lease?.release();
    },

    completeTurn(turn) {
      const recovery = turn.recovery;
      if (!recovery) return;
      try {
        getStore().markCompletedGroup(
          recovery.recordIds.map((recordId) => ({
            recordId,
            claim: claimForRecord(recordId),
          })),
        );
      } catch (error) {
        deps.recordRuntimeEvent?.("recovery", error, {
          phase: "complete",
          recordCount: recovery.recordIds.length,
        });
        throw error;
      }
      const lease = dispatchLeasesByTurnId.get(recovery.turnId);
      dispatchLeasesByTurnId.delete(recovery.turnId);
      lease?.release();
    },

    settleDeferredMessages(messages, outcome) {
      runGatedOperationSync(() =>
        settleMatches(matchesForMessages(messages), outcome),
      );
    },

    recordDeferredFailure(messages, error) {
      deps.recordRuntimeEvent?.("recovery", error, {
        phase: "deferred-group",
        recordCount: matchesForMessages(messages).length,
      });
    },

    terminalizeDeletedMessageIds(messageIds, scope) {
      runGatedOperationSync(() => {
        const matches = new Map<string, AdmittedUpdate>();
        for (const messageId of messageIds) {
          for (const admitted of recordsByMessageId.get(messageId)?.values() ?? []) {
            if (
              admitted.record.state !== "admitted" &&
              admitted.record.state !== "pre-dispatch"
            ) {
              continue;
            }
            if (
              scope?.profile !== undefined &&
              admitted.identity.profile !== scope.profile
            ) {
              continue;
            }
            if (
              scope?.chatId !== undefined &&
              admitted.identity.target.chatId !== scope.chatId
            ) {
              continue;
            }
            if (
              scope?.exactThreadId !== undefined &&
              (admitted.identity.target.threadId ?? null) !==
                scope.exactThreadId
            ) {
              continue;
            }
            if (scope?.businessConnectionId !== undefined) {
              const envelope = admitted.sourcePayload
                ? decodeEnvelope(admitted.sourcePayload)
                : undefined;
              const sourceMessage =
                envelope?.kind === "update"
                  ? getUpdateMessage(envelope.update)
                  : undefined;
              if (
                sourceMessage?.business_connection_id !==
                scope.businessConnectionId
              ) {
                continue;
              }
            }
            matches.set(admitted.record.recordId, admitted);
          }
        }
        settleMatches([...matches.values()], {
          kind: "completed",
          reason: "deleted",
        });
      });
    },

    publishSessionHandoffs(toSessionGeneration) {
      const fromSessionGeneration = deps.getSessionGeneration();
      if (
        !Number.isSafeInteger(toSessionGeneration) ||
        toSessionGeneration <= fromSessionGeneration
      ) {
        return 0;
      }
      const recoveryStore = getStore();
      const issuedAtMs = now();
      const handoffs: RecoverySameProcessHandoff[] = [];
      const seen = new Set<string>();
      for (const identity of recoveryStore.listUnresolvedIdentities()) {
        if (!isIdentityAuthenticated(identity)) continue;
        const key = JSON.stringify([
          identity.target.chatId,
          identity.target.threadId ?? null,
          identity.owner,
          identity.sessionGeneration,
        ]);
        if (seen.has(key)) continue;
        seen.add(key);
        handoffs.push({
          handoffId: randomUUID(),
          profile: identity.profile,
          target: structuredClone(identity.target),
          owner: structuredClone(identity.owner),
          fromSessionGeneration,
          toSessionGeneration,
          createdAtMs: issuedAtMs,
          expiresAtMs: issuedAtMs + HANDOFF_TTL_MS,
        });
      }
      if (handoffs.length > 0) {
        getHandoffRegistry().set(recoveryStore.rootPath, handoffs);
      }
      return handoffs.length;
    },

    async rehydrateOutbound(ctx, schedule) {
      return runGatedOperation(async () => {
        consumePublishedHandoffs(ctx);
        const identity = resolveOperatorIdentity(ctx);
        if (!identity) return;
        const recoveryStore = getStore();
        const claim = { identity };
        recoveryStore.reconcileOutboundTerminalSources(claim);
        const claimable = recoveryStore.listRehydratableOutboundRecords(claim);
        for (const item of claimable) {
          const record = item.record.state === "planned"
            ? recoveryStore.activateOutbound(item.record.recordId, claim)
            : item.record;
          await schedule({ ...item, record }, claim);
        }
      });
    },

    async rehydrate(ctx, handle, appendTurn) {
      return runGatedOperation(async () => {
      const recoveryStore = getStore();
      consumePublishedHandoffs(ctx);
      const replayableRecords = recoveryStore.listReplayableInboundRecords();
      const restoredTurnRecordIds = new Set<string>();
      for (const record of replayableRecords) {
        if (restoredTurnRecordIds.has(record.recordId)) continue;
        let currentIdentity: RecoveryIdentity;
        try {
          currentIdentity = createIdentity(record.identity.target, ctx);
        } catch {
          continue;
        }
        if (
          !areRecoveryTargetsEqual(record.identity.target, currentIdentity.target) ||
          !isIdentityAuthenticated(currentIdentity)
        ) {
          continue;
        }
        const claimable = recoveryStore.drainSafeInbound({
          identity: currentIdentity,
        });
        const drained = claimable.find(
          (item) => item.record.recordId === record.recordId,
        );
        if (!drained) continue;
        identityByRecordId.set(record.recordId, currentIdentity);
        let envelope: RecoveryPayloadEnvelope;
        try {
          envelope = decodeEnvelope(drained.payload);
        } catch {
          recoveryStore.markPoisonSkippedInbound(record.recordId, {
            identity: currentIdentity,
          });
          deps.recordRuntimeEvent?.(
            "recovery",
            "corrupt durable inbound payload",
            { phase: "payload-corrupt" },
          );
          continue;
        }
        if (envelope.kind === "turn") {
          const recovery = envelope.turn.recovery!;
          const exactRecordIds = recovery.recordIds;
          const uniqueRecordIds = new Set(exactRecordIds);
          const groupRecords = replayableRecords.filter((candidate) =>
            uniqueRecordIds.has(candidate.recordId),
          );
          const claimableRecordIds = new Set(
            claimable.map((item) => item.record.recordId),
          );
          const exactGroup =
            uniqueRecordIds.size === exactRecordIds.length &&
            uniqueRecordIds.has(record.recordId) &&
            record.turnId === recovery.turnId &&
            groupRecords.length === exactRecordIds.length &&
            groupRecords.every(
              (candidate) =>
                candidate.turnId === recovery.turnId &&
                candidate.identity.profile === currentIdentity.profile &&
                areRecoveryTargetsEqual(
                  candidate.identity.target,
                  currentIdentity.target,
                ) &&
                claimableRecordIds.has(candidate.recordId),
            );
          if (!exactGroup) {
            recoveryStore.markPoisonSkippedInbound(record.recordId, {
              identity: currentIdentity,
            });
            deps.recordRuntimeEvent?.(
              "recovery",
              "durable turn group identity mismatch",
              { phase: "turn-group-corrupt" },
            );
            continue;
          }
          const restoredPaths = recoveryStore.restoreInboundSpool(
            record.recordId,
            { identity: currentIdentity },
            envelope.spool.map((entry) => entry.fileName),
          );
          const restoredTurn = rewriteTurnPaths(
            envelope.turn,
            envelope.spool,
            restoredPaths,
          );
          for (const recordId of exactRecordIds) {
            identityByRecordId.set(recordId, currentIdentity);
            restoredTurnRecordIds.add(recordId);
          }
          appendTurn(restoredTurn);
          continue;
        }
        remember(envelope.update, {
          record: drained.record,
          identity: currentIdentity,
          sourcePayload: drained.payload,
          admission: { kind: "admitted" },
        });
        const outcome = requireInboundHandlingOutcome(
          await handle(envelope.update as TUpdate, ctx),
        );
        settleMatches(
          [admittedByUpdate.get(envelope.update.update_id)!],
          outcome,
        );
      }
      });
    },
  };

  return runtime;
}
