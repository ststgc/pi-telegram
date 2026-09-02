/**
 * Regression tests for recovery schema contracts and deterministic fault injection
 * Zones: recovery, queue lifecycle, delivery, multi-instance bus, test infrastructure
 * Guards strict versioned parsing, frozen policy constants, and the bounded S0 fault inventory.
 */

import assert from "node:assert/strict";
import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdtempSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, sep } from "node:path";
import test from "node:test";

import {
  areRecoveryOwnersEqual,
  openRecoveryStore,
  parseRecoverySnapshot,
  RECOVERY_BUS_STATES,
  RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS,
  RECOVERY_INBOUND_STATES,
  RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS,
  RECOVERY_OUTBOUND_RETRY_DELAYS_MS,
  RECOVERY_OUTBOUND_STATES,
  RECOVERY_OUTBOUND_UNCERTAINTY_REASONS,
  RECOVERY_PROFILE_QUOTA_BYTES,
  RECOVERY_REASSIGNMENT_STATES,
  RECOVERY_SCHEMA_VERSION,
  RECOVERY_STORE_DIRECTORY_MODE,
  RECOVERY_STORE_DIRECTORY_NAME,
  RECOVERY_STORE_FILE_MODE,
  RECOVERY_STORE_MODES,
  RECOVERY_STORE_QUARANTINE_PREFIX,
  RECOVERY_TERMINAL_METADATA_MAX_BYTES,
  RECOVERY_TERMINAL_METADATA_MAX_RECORDS,
  RECOVERY_TERMINAL_METADATA_RETENTION_MS,
  RECOVERY_UNRESOLVED_BUS_STATES,
  RECOVERY_UNRESOLVED_INBOUND_STATES,
  RECOVERY_UNRESOLVED_OUTBOUND_STATES,
  RecoveryProfileOperationGate,
  RecoveryQuarantineError,
  RecoveryQuotaExceededError,
  restoreRecoveryQuarantine,
  type RecoveryIdentity,
  type RecoveryOwnerIdentity,
  type RecoverySameProcessHandoff,
  type RecoverySnapshotV1,
  type RecoveryStore,
  validateRecoverySameProcessHandoff,
  validateRecoverySnapshot,
} from "../lib/recovery.ts";
import {
  InjectedReliabilityFaultError,
  RELIABILITY_FAULT_IDS,
  ReliabilityFaultController,
  type ReliabilityFaultId,
} from "./fixtures/reliability-faults.ts";

test("profile downgrade gate fences races, drains in-flight work, and never reopens quarantine", async () => {
  const gate = new RecoveryProfileOperationGate();
  const first = gate.enter("default");
  assert.ok(first);
  assert.equal(gate.getState("default").inFlight, 1);

  assert.equal(gate.beginFencing("default", "fence-a"), "fence-a");
  assert.equal(gate.enter("default"), undefined);
  let drained = false;
  const drain = gate.awaitDrained("default", "fence-a").then(() => {
    drained = true;
  });
  await Promise.resolve();
  assert.equal(drained, false);
  first.release();
  await drain;
  assert.equal(drained, true);

  assert.throws(
    () => gate.beginDowngradeExclusive("default", "fence-stale"),
    /generation mismatch/,
  );
  gate.beginDowngradeExclusive("default", "fence-a");
  assert.equal(gate.getState("default").phase, "downgrade-exclusive");
  gate.resume("default", "fence-a");
  assert.equal(gate.getState("default").phase, "active");

  gate.beginFencing("default", "fence-b");
  await gate.awaitDrained("default", "fence-b");
  gate.beginDowngradeExclusive("default", "fence-b");
  gate.markQuarantined("default", "fence-b");
  assert.equal(gate.getState("default").phase, "quarantined");
  assert.equal(gate.enter("default"), undefined);
  assert.throws(() => gate.resume("default", "fence-b"), /cannot resume/);
});

const OLD_IDENTITY: RecoveryIdentity = {
  profile: "default",
  target: { chatId: 1001, threadId: 42 },
  owner: {
    kind: "manual-follower",
    ownerId: "manual-owner-a",
    registrationGeneration: "registration-a",
  },
  sessionGeneration: 3,
};
const TEST_SHA256 = "0".repeat(64);

const NEW_OWNER: RecoveryOwnerIdentity = {
  kind: "leader",
  ownerId: "leader-b",
  leaderEpoch: "epoch-b",
};

function makeHandoff(): RecoverySameProcessHandoff {
  return {
    handoffId: "handoff-1",
    profile: "default",
    target: OLD_IDENTITY.target,
    owner: OLD_IDENTITY.owner,
    fromSessionGeneration: 3,
    toSessionGeneration: 4,
    createdAtMs: 700,
    expiresAtMs: 1200,
    consumedAtMs: 850,
  };
}

function makeSnapshot(
  mode: RecoverySnapshotV1["mode"],
  reassignmentState: RecoverySnapshotV1["reassignments"][number]["state"] =
    "requested",
): RecoverySnapshotV1 {
  const inbound: RecoverySnapshotV1["inbound"] = RECOVERY_INBOUND_STATES.map((state, index) => ({
    family: "inbound" as const,
    recordId: `inbound-${index}`,
    updateId: 2000 + index,
    turnId: `turn-in-${index}`,
    state,
    identity: OLD_IDENTITY,
    createdRevision: 1 + index,
    stateRevision: 1 + index,
    ...(state !== "observed" ? { admissionRevision: 1 + index } : {}),
    createdAtMs: 100 + index,
    updatedAtMs: 200 + index,
    payloadRef: {
      payloadId: `prompt-${index}`,
      byteLength: 10 + index,
      sha256: TEST_SHA256,
    },
    spoolRefs: [{
      spoolId: `in-spool-${index}`,
      byteLength: 20 + index,
      sha256: TEST_SHA256,
    }],
  }));
  const outboundSources = RECOVERY_OUTBOUND_STATES.map((state, index) => {
    const terminal =
      state === "delivered" ||
      state === "delivery-uncertain" ||
      state === "explicitly-discarded";
    return {
      family: "inbound" as const,
      recordId: `outbound-source-${index}`,
      updateId: 3000 + index,
      turnId: `turn-out-${index}`,
      state: terminal ? "completed" as const : "dispatching" as const,
      identity: OLD_IDENTITY,
      createdRevision: 30 + index,
      stateRevision: 30 + index,
      admissionRevision: 30 + index,
      createdAtMs: 250 + index,
      updatedAtMs: 275 + index,
      spoolRefs: [],
    };
  });
  inbound.push(...outboundSources);
  const outbound = RECOVERY_OUTBOUND_STATES.map((state, index) => {
    const delivered = state === "delivered";
    const discarded = state === "explicitly-discarded";
    const sending = state === "sending";
    const uncertain = state === "delivery-uncertain";
    const retryable = state === "retryable-pending";
    return {
      family: "outbound" as const,
      recordId: `outbound-${index}`,
      intentId: `intent-${index}`,
      turnId: `turn-out-${index}`,
      sourceInboundRecordIds: [`outbound-source-${index}`],
      state,
      nextUnitIndex: delivered ? 1 : 0,
      ...(sending
        ? { activeUnit: { unitIndex: 0, attemptId: `attempt-${index}`, startedAtMs: 350 + index } }
        : {}),
      automaticAttemptCount: sending || uncertain || retryable ? 1 : 0,
      ...(retryable ? { retryNotBeforeMs: 900 } : {}),
      receipts: delivered
        ? [{
            unitIndex: 0,
            operationId: `operation-${index}`,
            method: "sendMessage",
            messageId: 500 + index,
            committedAtMs: 390 + index,
          }]
        : [],
      unitProgress: delivered
        ? [{
            unitIndex: 0,
            operationId: `operation-${index}`,
            outcome: "committed" as const,
          }]
        : [],
      ...(uncertain
        ? {
            uncertainty: {
              unitIndex: 0,
              reason: "commit-unknown" as const,
              observedAtMs: 390 + index,
            },
          }
        : {}),
      identity: OLD_IDENTITY,
      createdRevision: 10 + index,
      stateRevision: 10 + index,
      createdAtMs: 300 + index,
      updatedAtMs: 400 + index,
      ...(!discarded
        ? {
            payloadRef: {
              payloadId: `answer-${index}`,
              byteLength: 30 + index,
              sha256: TEST_SHA256,
            },
          }
        : {}),
      spoolRefs: delivered || discarded
        ? []
        : [{
            spoolId: `out-spool-${index}`,
            byteLength: 40 + index,
            sha256: TEST_SHA256,
          }],
    };
  });
  const bus = RECOVERY_BUS_STATES.map((state, index) => ({
    family: "bus" as const,
    recordId: `bus-${index}`,
    requestId: `request-${index}`,
    envelopeKind: "follower.callApi" as const,
    method: "call",
    apiMethod: "sendMessage",
    payloadFingerprint: `sha256-${index}`,
    followerInstanceId: "follower-a",
    manualFollowerOwnerId: "manual-owner-a",
    registrationGeneration: "registration-a",
    leaderEpoch: "leader-epoch-a",
    leaderSessionGeneration: 7,
    state,
    identity: OLD_IDENTITY,
    createdRevision: 20 + index,
    stateRevision: 20 + index,
    createdAtMs: 500 + index,
    updatedAtMs: 600 + index,
    ...(state !== "explicitly-discarded"
      ? {
          payloadRef: {
            payloadId: `bus-payload-${index}`,
            byteLength: 50 + index,
            sha256: TEST_SHA256,
          },
        }
      : {}),
    spoolRefs: [],
  }));
  const unresolvedRecordIds = [...inbound, ...outbound, ...bus]
    .filter((record) => {
      if (record.family === "inbound") {
        return (RECOVERY_UNRESOLVED_INBOUND_STATES as readonly string[]).includes(record.state);
      }
      if (record.family === "outbound") {
        return (RECOVERY_UNRESOLVED_OUTBOUND_STATES as readonly string[]).includes(record.state);
      }
      return (RECOVERY_UNRESOLVED_BUS_STATES as readonly string[]).includes(record.state);
    })
    .map((record) => record.recordId);
  const allRecords = [...inbound, ...outbound, ...bus];
  const payloadBytes = allRecords.reduce(
    (sum, record) => sum + (record.payloadRef?.byteLength ?? 0),
    0,
  );
  const spoolBytes = allRecords.reduce(
    (sum, record) =>
      sum + record.spoolRefs.reduce((nested, ref) => nested + ref.byteLength, 0),
    0,
  );
  return {
    version: RECOVERY_SCHEMA_VERSION,
    profile: "default",
    mode,
    revision: 100,
    writtenAtMs: 1000,
    quota: {
      recordBytes: 1000,
      payloadBytes,
      spoolBytes,
      reservedBytes: 10,
      totalBytes: 1000 + payloadBytes + spoolBytes + 10,
    },
    committedUpdateId: 1999,
    inbound,
    outbound,
    bus,
    reassignments: [
      {
        family: "reassignment" as const,
        reassignmentId: "reassignment-1",
        profile: "default",
        target: OLD_IDENTITY.target,
        oldOwner: OLD_IDENTITY.owner,
        newOwner: NEW_OWNER,
        newSessionGeneration: 4,
        captureThroughRevision: 50,
        unresolvedRecordIds,
        state: reassignmentState,
        createdAtMs: 700,
        updatedAtMs: 800,
      },
    ],
  };
}

function jsonValue(snapshot = makeSnapshot("active")): Record<string, unknown> {
  return JSON.parse(JSON.stringify(snapshot)) as Record<string, unknown>;
}

function expectInvalid(
  mutate: (value: Record<string, unknown>) => void,
  pattern: RegExp,
): void {
  const value = jsonValue();
  mutate(value);
  assert.throws(() => validateRecoverySnapshot(value), pattern);
}

function records(value: Record<string, unknown>, family: "inbound" | "outbound" | "bus") {
  return value[family] as Array<Record<string, unknown>>;
}

function createStoreHarness(options: {
  fault?: (faultId: ReliabilityFaultId) => void;
  quotaBytes?: number;
  authenticated?: (identity: RecoveryIdentity) => boolean;
  validateReassignmentBinding?: Parameters<typeof openRecoveryStore>[0]["validateReassignmentBinding"];
  actionId?: (stableInput: string) => string;
  terminalMetadataMaxRecords?: number;
  terminalMetadataMaxBytes?: number;
  fs?: Parameters<typeof openRecoveryStore>[0]["fs"];
} = {}): {
  directory: string;
  rootPath: string;
  store: RecoveryStore;
  now: { value: number };
} {
  const directory = mkdtempSync(join(tmpdir(), "pi-telegram-recovery-"));
  const rootPath = join(directory, "recovery-v1");
  const now = { value: 10_000 };
  let id = 0;
  const store = openRecoveryStore({
    profile: "default",
    rootPath,
    now: () => now.value,
    randomId: () => `id-${++id}`,
    quotaBytes: options.quotaBytes,
    fault: options.fault,
    isIdentityAuthenticated:
      options.authenticated ?? ((identity) => identitiesMatch(identity, OLD_IDENTITY)),
    validateReassignmentBinding: options.validateReassignmentBinding,
    actionId: options.actionId,
    terminalMetadataMaxRecords: options.terminalMetadataMaxRecords,
    terminalMetadataMaxBytes: options.terminalMetadataMaxBytes,
    fs: options.fs,
  });
  return { directory, rootPath, store, now };
}

function identitiesMatch(left: RecoveryIdentity, right: RecoveryIdentity): boolean {
  return (
    left.profile === right.profile &&
    left.target.chatId === right.target.chatId &&
    left.target.threadId === right.target.threadId &&
    left.sessionGeneration === right.sessionGeneration &&
    areRecoveryOwnersEqual(left.owner, right.owner)
  );
}

function reopenStore(
  harness: ReturnType<typeof createStoreHarness>,
  options: {
    fault?: (faultId: ReliabilityFaultId) => void;
    authenticated?: (identity: RecoveryIdentity) => boolean;
    shouldRecoverInFlight?: (
      family: "inbound" | "outbound" | "bus",
      identity: RecoveryIdentity,
    ) => boolean;
    validateReassignmentBinding?: Parameters<typeof openRecoveryStore>[0]["validateReassignmentBinding"];
    actionId?: (stableInput: string) => string;
    terminalMetadataMaxRecords?: number;
    terminalMetadataMaxBytes?: number;
    fs?: Parameters<typeof openRecoveryStore>[0]["fs"];
  } = {},
): RecoveryStore {
  let id = 1000;
  return openRecoveryStore({
    profile: "default",
    rootPath: harness.rootPath,
    now: () => harness.now.value,
    randomId: () => `reopen-${++id}`,
    fault: options.fault,
    isIdentityAuthenticated:
      options.authenticated ?? ((identity) => identitiesMatch(identity, OLD_IDENTITY)),
    shouldRecoverInFlight: options.shouldRecoverInFlight,
    validateReassignmentBinding: options.validateReassignmentBinding,
    actionId: options.actionId,
    terminalMetadataMaxRecords: options.terminalMetadataMaxRecords,
    terminalMetadataMaxBytes: options.terminalMetadataMaxBytes,
    fs: options.fs,
  });
}

function removeHarness(harness: ReturnType<typeof createStoreHarness>): void {
  rmSync(harness.directory, { recursive: true, force: true });
}

function readStoreSnapshot(rootPath: string): RecoverySnapshotV1 {
  return parseRecoverySnapshot(readFileSync(join(rootPath, "snapshot.json"), "utf8"));
}

function prepareOutbound(
  harness: ReturnType<typeof createStoreHarness>,
  options: {
    updateId?: number;
    intentId?: string;
    replyToMessageId?: number;
    guestQueryId?: string;
    units?: Parameters<RecoveryStore["planOutbound"]>[0]["units"];
    spool?: readonly Uint8Array[];
  } = {},
) {
  const inbound = harness.store.observeInbound(options.updateId ?? 800, OLD_IDENTITY);
  harness.store.admitInbound({
    recordId: inbound.recordId,
    payload: Buffer.from("durable inbound source"),
  });
  harness.store.markPreDispatch(inbound.recordId, { identity: OLD_IDENTITY });
  harness.store.markDispatching(inbound.recordId, { identity: OLD_IDENTITY });
  const units = options.units ?? [
    {
      kind: "final-text" as const,
      operationId: "operation-0",
      method: "sendRichMessage",
      content: "first",
      contentMode: "rich-markdown" as const,
    },
    {
      kind: "final-text" as const,
      operationId: "operation-1",
      method: "sendRichMessage",
      content: "second",
      contentMode: "rich-markdown" as const,
    },
  ];
  const outbound = harness.store.planOutbound({
    intentId: options.intentId ?? "intent-800",
    turnId: inbound.turnId,
    sourceInboundRecordIds: [inbound.recordId],
    claim: { identity: OLD_IDENTITY },
    replyToMessageId: options.replyToMessageId ?? 77,
    renderingMode: "rich",
    finalMarkdown: "first\n\nsecond",
    renderedChunks: [],
    units,
    ...(options.guestQueryId
      ? {
          guestQueryId: options.guestQueryId,
          guestStagingTarget: { chatId: 840585 },
        }
      : {}),
    spool: options.spool,
  });
  return { inbound, outbound, units };
}

test("recovery snapshot v1 roundtrips all states, quota, and store modes", () => {
  for (const mode of RECOVERY_STORE_MODES) {
    for (const reassignmentState of RECOVERY_REASSIGNMENT_STATES) {
      const snapshot = makeSnapshot(mode, reassignmentState);
      assert.deepEqual(parseRecoverySnapshot(JSON.stringify(snapshot)), snapshot);
    }
  }
  assert.deepEqual(validateRecoverySameProcessHandoff(makeHandoff()), makeHandoff());
});

test("recovery constants preserve frozen quota, retention, paths, and unresolved states", () => {
  assert.equal(RECOVERY_SCHEMA_VERSION, 1);
  assert.equal(RECOVERY_PROFILE_QUOTA_BYTES, 512 * 1024 * 1024);
  assert.equal(RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS, 86_400_000);
  assert.equal(RECOVERY_TERMINAL_METADATA_RETENTION_MS, 604_800_000);
  assert.equal(RECOVERY_TERMINAL_METADATA_MAX_RECORDS, 100_000);
  assert.equal(RECOVERY_TERMINAL_METADATA_MAX_BYTES, 64 * 1024 * 1024);
  assert.equal(RECOVERY_STORE_DIRECTORY_MODE, 0o700);
  assert.equal(RECOVERY_STORE_FILE_MODE, 0o600);
  assert.equal(RECOVERY_STORE_DIRECTORY_NAME, "recovery-v1");
  assert.equal(RECOVERY_STORE_QUARANTINE_PREFIX, "recovery-v1-quarantine-");
  assert.deepEqual(RECOVERY_UNRESOLVED_INBOUND_STATES, [
    "observed", "admitted", "pre-dispatch", "dispatching", "execution-uncertain",
  ]);
  assert.deepEqual(RECOVERY_UNRESOLVED_OUTBOUND_STATES, [
    "planned", "pending", "sending", "delivery-uncertain", "retryable-pending",
  ]);
  assert.deepEqual(RECOVERY_UNRESOLVED_BUS_STATES, ["pending", "bus-uncertain"]);
});

test("outbound policy constants freeze automatic starts, delays, and uncertainty reasons", () => {
  assert.equal(RECOVERY_OUTBOUND_MAX_AUTOMATIC_STARTS, 3);
  assert.deepEqual(RECOVERY_OUTBOUND_RETRY_DELAYS_MS, [250, 1_000]);
  assert.deepEqual(RECOVERY_OUTBOUND_UNCERTAINTY_REASONS, [
    "commit-unknown",
    "timeout-after-write",
    "connection-lost-after-write",
    "malformed-success",
    "response-lost",
    "authority-lost-after-start",
    "process-reopened-sending",
    "confirmed-before-receipt",
  ]);
});

test("strict outbound record parsing rejects state-specific metadata, unordered receipts, and mismatched sources", () => {
  expectInvalid((value) => {
    records(value, "outbound")[0]!.unknown = true;
  }, /unknown field/);
  expectInvalid((value) => {
    records(value, "outbound")[0]!.activeUnit = {
      unitIndex: 0,
      attemptId: "invalid-planned-attempt",
      startedAtMs: 1,
    };
  }, /only while sending/);
  expectInvalid((value) => {
    delete records(value, "outbound")[2]!.activeUnit;
  }, /required for the exact next sending unit/);
  expectInvalid((value) => {
    delete records(value, "outbound")[4]!.uncertainty;
  }, /must identify the exact ambiguous unit/);
  expectInvalid((value) => {
    delete records(value, "outbound")[5]!.retryNotBeforeMs;
  }, /required before automatic retry/);
  expectInvalid((value) => {
    const delivered = records(value, "outbound")[3]!;
    (delivered.receipts as Array<Record<string, unknown>>)[0]!.unitIndex = 1;
  }, /must follow every confirmed receipt/);
  expectInvalid((value) => {
    const delivered = records(value, "outbound")[3]!;
    const receipts = delivered.receipts as Array<Record<string, unknown>>;
    receipts.push({ ...receipts[0], unitIndex: 1 });
    delivered.nextUnitIndex = 2;
  }, /duplicate id/);
  expectInvalid((value) => {
    delete records(value, "outbound")[3]!.unitProgress;
  }, /missing field/);
  expectInvalid((value) => {
    const delivered = records(value, "outbound")[3]!;
    (delivered.unitProgress as Array<Record<string, unknown>>)[0]!.outcome =
      "skipped";
  }, /reason/);
  expectInvalid((value) => {
    const delivered = records(value, "outbound")[3]!;
    (delivered.unitProgress as Array<Record<string, unknown>>)[0]!.operationId =
      "other-operation";
  }, /committed progress must exactly match receipts/);
  expectInvalid((value) => {
    records(value, "outbound")[0]!.turnId = "wrong-turn";
  }, /mismatched source turn/);
  expectInvalid((value) => {
    records(value, "outbound")[0]!.sourceInboundRecordIds = ["missing"];
  }, /unknown inbound record/);
});

test("existing version-1 snapshots with an empty outbound array remain readable", () => {
  const snapshot = makeSnapshot("active");
  snapshot.outbound = [];
  snapshot.inbound = snapshot.inbound.filter(
    (record) => !record.recordId.startsWith("outbound-source-"),
  );
  snapshot.reassignments[0]!.unresolvedRecordIds =
    snapshot.reassignments[0]!.unresolvedRecordIds.filter(
      (recordId) => !recordId.startsWith("outbound-") && !recordId.startsWith("outbound-source-"),
    );
  const allRecords = [...snapshot.inbound, ...snapshot.bus];
  snapshot.quota.payloadBytes = allRecords.reduce(
    (sum, record) => sum + (record.payloadRef?.byteLength ?? 0),
    0,
  );
  snapshot.quota.spoolBytes = allRecords.reduce(
    (sum, record) =>
      sum + record.spoolRefs.reduce((nested, reference) => nested + reference.byteLength, 0),
    0,
  );
  snapshot.quota.totalBytes =
    snapshot.quota.recordBytes +
    snapshot.quota.payloadBytes +
    snapshot.quota.spoolBytes +
    snapshot.quota.reservedBytes;
  assert.deepEqual(parseRecoverySnapshot(JSON.stringify(snapshot)), snapshot);
});

test("parser rejects invalid version, state, identity, revision, numbers, arrays, and unknown fields", () => {
  expectInvalid((value) => { value.version = 2; }, /version/);
  expectInvalid((value) => { value.mode = "idle"; }, /mode/);
  expectInvalid((value) => { records(value, "inbound")[0]!.state = "queued"; }, /state/);
  expectInvalid((value) => { value.unexpected = true; }, /unknown field/);
  expectInvalid((value) => {
    const identity = records(value, "inbound")[0]!.identity as Record<string, unknown>;
    identity.secret = true;
  }, /unknown field/);
  expectInvalid((value) => { value.bus = {}; }, /expected an array/);
  expectInvalid((value) => {
    const identity = records(value, "inbound")[0]!.identity as Record<string, unknown>;
    identity.sessionGeneration = -1;
  }, /sessionGeneration/);
  expectInvalid((value) => { records(value, "inbound")[0]!.createdRevision = 101; }, /createdRevision/);
  expectInvalid((value) => { records(value, "inbound")[0]!.stateRevision = 0; }, /stateRevision/);
  expectInvalid((value) => { records(value, "inbound")[0]!.stateRevision = 101; }, /stateRevision/);
  expectInvalid((value) => {
    const reassignments = value.reassignments as Array<Record<string, unknown>>;
    reassignments[0]!.captureThroughRevision = 101;
  }, /captureThroughRevision/);
  expectInvalid((value) => { value.writtenAtMs = 199; }, /updatedAtMs/);
  expectInvalid((value) => {
    const reassignments = value.reassignments as Array<Record<string, unknown>>;
    reassignments[0]!.updatedAtMs = 1001;
  }, /updatedAtMs/);
  expectInvalid((value) => { value.revision = 1.5; }, /revision/);
  expectInvalid((value) => { records(value, "inbound")[0]!.updateId = Number.NaN; }, /updateId/);
  assert.throws(() => parseRecoverySnapshot("{"), /Invalid recovery snapshot JSON/);
});

test("handoff validator is strict, generation-linked, time-bounded, and process-local", () => {
  assert.throws(
    () =>
      validateRecoverySameProcessHandoff({
        ...makeHandoff(),
        toSessionGeneration: 3,
      }),
    /newer than fromSessionGeneration/,
  );
  assert.throws(
    () =>
      validateRecoverySameProcessHandoff({
        ...makeHandoff(),
        toSessionGeneration: 2,
      }),
    /newer than fromSessionGeneration/,
  );
  assert.throws(
    () =>
      validateRecoverySameProcessHandoff({
        ...makeHandoff(),
        consumedAtMs: 1300,
      }),
    /handoff lifetime/,
  );
  assert.throws(
    () => validateRecoverySameProcessHandoff({ ...makeHandoff(), unknown: true }),
    /unknown field/,
  );
  const snapshot = jsonValue();
  snapshot.handoffs = [makeHandoff()];
  assert.throws(() => validateRecoverySnapshot(snapshot), /handoffs: unknown field/);
});

test("reassignment claims exactly unresolved records at capture revision", () => {
  expectInvalid((value) => {
    const reassignments = value.reassignments as Array<Record<string, unknown>>;
    (reassignments[0]!.unresolvedRecordIds as string[]).pop();
  }, /must exactly match unresolved records/);
  expectInvalid((value) => {
    const reassignments = value.reassignments as Array<Record<string, unknown>>;
    (reassignments[0]!.unresolvedRecordIds as string[]).push("inbound-4");
  }, /must exactly match unresolved records/);
  expectInvalid((value) => {
    const reassignments = value.reassignments as Array<Record<string, unknown>>;
    const ids = reassignments[0]!.unresolvedRecordIds as string[];
    ids.push(ids[0]!);
  }, /duplicate id/);

  const snapshot = makeSnapshot("active");
  snapshot.inbound[0]!.createdRevision = 60;
  snapshot.inbound[0]!.stateRevision = 60;
  for (const reassignment of snapshot.reassignments) {
    reassignment.unresolvedRecordIds = reassignment.unresolvedRecordIds.filter(
      (recordId) => recordId !== "inbound-0",
    );
  }
  assert.deepEqual(parseRecoverySnapshot(JSON.stringify(snapshot)), snapshot);
});

test("captured reassignment survives grant or cancel followed by terminal resolution", () => {
  for (const reassignmentState of [
    "recovery-grant-committed",
    "cancelled-before-transfer",
  ] as const) {
    const snapshot = makeSnapshot("active", reassignmentState);
    const captured = snapshot.inbound[0]!;
    captured.state = "completed";
    captured.admissionRevision = 40;
    captured.stateRevision = 60;
    assert.deepEqual(parseRecoverySnapshot(JSON.stringify(snapshot)), snapshot);

    captured.stateRevision = 40;
    assert.throws(
      () => parseRecoverySnapshot(JSON.stringify(snapshot)),
      /must exactly match unresolved records/,
    );
  }
});

test("reassignment records cannot claim the same unresolved record twice", () => {
  expectInvalid((value) => {
    const reassignments = value.reassignments as Array<Record<string, unknown>>;
    reassignments.push({
      ...reassignments[0]!,
      reassignmentId: "reassignment-2",
      state: "binding-transfer-pending",
    });
  }, /already claimed/);

  const snapshot = makeSnapshot("active");
  snapshot.reassignments.push({
    ...snapshot.reassignments[0]!,
    reassignmentId: "cancelled-reassignment",
    state: "cancelled-before-transfer",
  });
  assert.deepEqual(parseRecoverySnapshot(JSON.stringify(snapshot)), snapshot);
});

test("request idempotency keys use collision-free structural tuples", () => {
  const allowed = makeSnapshot("active");
  allowed.outbound[1]!.intentId = allowed.outbound[0]!.intentId;
  assert.deepEqual(parseRecoverySnapshot(JSON.stringify(allowed)), allowed);

  expectInvalid((value) => {
    const outbound = records(value, "outbound");
    outbound[1]!.intentId = outbound[0]!.intentId;
    outbound[1]!.turnId = outbound[0]!.turnId;
    outbound[1]!.sourceInboundRecordIds = outbound[0]!.sourceInboundRecordIds;
  }, /duplicate id/);
  expectInvalid((value) => {
    const bus = records(value, "bus");
    bus[1]!.requestId = bus[0]!.requestId;
  }, /duplicate id/);
});

test("owner matching is structural and cannot collide through colon-delimited ids", () => {
  const ownerA: RecoveryOwnerIdentity = { kind: "leader", ownerId: "a:b", leaderEpoch: "c" };
  const ownerB: RecoveryOwnerIdentity = { kind: "leader", ownerId: "a", leaderEpoch: "b:c" };
  assert.equal(areRecoveryOwnersEqual(ownerA, ownerB), false);

  const snapshot = makeSnapshot("active");
  snapshot.bus[0]!.identity = { ...snapshot.bus[0]!.identity, owner: ownerA };
  snapshot.bus[1]!.identity = { ...snapshot.bus[1]!.identity, owner: ownerB };
  for (const reassignment of snapshot.reassignments) {
    reassignment.unresolvedRecordIds = reassignment.unresolvedRecordIds.filter(
      (recordId) => recordId !== "bus-0" && recordId !== "bus-1",
    );
  }
  snapshot.reassignments.push({
    family: "reassignment",
    reassignmentId: "colon-safe",
    profile: "default",
    target: OLD_IDENTITY.target,
    oldOwner: ownerA,
    newOwner: NEW_OWNER,
    newSessionGeneration: 4,
    captureThroughRevision: 50,
    unresolvedRecordIds: ["bus-0"],
    state: "requested",
    createdAtMs: 700,
    updatedAtMs: 800,
  });
  assert.deepEqual(parseRecoverySnapshot(JSON.stringify(snapshot)), snapshot);
});

test("quota accounting and global payload/spool references fail closed", () => {
  expectInvalid((value) => {
    const quota = value.quota as Record<string, unknown>;
    quota.totalBytes = 1999;
  }, /exact component sum/);
  expectInvalid((value) => {
    const quota = value.quota as Record<string, unknown>;
    quota.recordBytes = RECOVERY_PROFILE_QUOTA_BYTES;
    quota.totalBytes =
      RECOVERY_PROFILE_QUOTA_BYTES +
      Number(quota.payloadBytes) +
      Number(quota.spoolBytes) +
      Number(quota.reservedBytes);
  }, /per-profile quota/);
  expectInvalid((value) => {
    const inbound = records(value, "inbound");
    const first = inbound[0]!.payloadRef as Record<string, unknown>;
    const second = inbound[1]!.payloadRef as Record<string, unknown>;
    second.payloadId = first.payloadId;
  }, /duplicate id/);
  expectInvalid((value) => {
    const inbound = records(value, "inbound");
    const first = (inbound[0]!.spoolRefs as Array<Record<string, unknown>>)[0]!;
    const second = (inbound[1]!.spoolRefs as Array<Record<string, unknown>>)[0]!;
    second.spoolId = first.spoolId;
  }, /duplicate id/);
  expectInvalid((value) => {
    const quota = value.quota as Record<string, unknown>;
    quota.payloadBytes = Number(quota.payloadBytes) - 1;
    quota.totalBytes = Number(quota.totalBytes) - 1;
  }, /referenced payload bytes/);
  expectInvalid((value) => {
    const payloadRef = records(value, "inbound")[0]!.payloadRef as Record<string, unknown>;
    delete payloadRef.sha256;
  }, /sha256.*missing field/);
  expectInvalid((value) => {
    const payloadRef = records(value, "inbound")[0]!.payloadRef as Record<string, unknown>;
    payloadRef.sha256 = "not-a-digest";
  }, /SHA-256/);
});

test("profile-scoped recovery store is private and admits idempotently with one prefix authority", () => {
  const harness = createStoreHarness();
  try {
    const { store, rootPath } = harness;
    const observed = store.observeInbound(101, OLD_IDENTITY);
    assert.deepEqual(store.observeInbound(101, OLD_IDENTITY), observed);
    const admitted = store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("prompt-body"),
      spool: [Buffer.from("attachment")],
    });
    assert.equal(admitted.turnId, observed.turnId);
    assert.equal(admitted.state, "admitted");
    assert.deepEqual(
      store.admitInbound({
        recordId: observed.recordId,
        payload: Buffer.from("prompt-body"),
        spool: [Buffer.from("attachment")],
      }),
      admitted,
    );
    assert.throws(
      () =>
        store.admitInbound({
          recordId: observed.recordId,
          payload: Buffer.from("different"),
          spool: [Buffer.from("attachment")],
        }),
      /payload mismatch/,
    );
    assert.equal(store.commitUpdatePrefix(101), 101);
    assert.equal(store.getCommittedUpdateId(), 101);
    assert.throws(() => store.commitUpdatePrefix(100), /backwards/);

    const claim = { identity: OLD_IDENTITY };
    assert.equal(store.markPreDispatch(observed.recordId, claim).state, "pre-dispatch");
    assert.equal(store.markDispatching(observed.recordId, claim).state, "dispatching");
    assert.equal(store.markCompleted(observed.recordId, claim).state, "completed");
    assert.equal(readdirSync(join(rootPath, "spool")).length, 0);
    if (process.platform !== "win32") {
      assert.equal(statSync(rootPath).mode & 0o777, 0o700);
      assert.equal(statSync(join(rootPath, "payloads")).mode & 0o777, 0o700);
      assert.equal(statSync(join(rootPath, "snapshot.json")).mode & 0o777, 0o600);
      const payloadName = readdirSync(join(rootPath, "payloads"))[0]!;
      assert.equal(statSync(join(rootPath, "payloads", payloadName)).mode & 0o777, 0o600);
    }
    assert.equal(readStoreSnapshot(rootPath).committedUpdateId, 101);
  } finally {
    removeHarness(harness);
  }
});

test("immutable admission revision verifies exact manual-follower durable proofs", () => {
  const harness = createStoreHarness();
  try {
    const observed = harness.store.observeInbound(101, OLD_IDENTITY);
    assert.equal(observed.admissionRevision, undefined);
    const admitted = harness.store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("forwarded-update"),
    });
    assert.ok(admitted.admissionRevision);
    const proof = {
      version: 1 as const,
      updateId: admitted.updateId,
      recordId: admitted.recordId,
      turnId: admitted.turnId,
      profile: admitted.identity.profile,
      target: admitted.identity.target,
      ownerId: "manual-owner-a",
      registrationGeneration: "registration-a",
      sessionGeneration: 3,
      admissionRevision: admitted.admissionRevision!,
      disposition: "admitted" as const,
    };
    assert.equal(
      harness.store.verifyInboundAdmissionProof(proof).recordId,
      admitted.recordId,
    );
    const materialized = harness.store.materializeInbound({
      recordId: admitted.recordId,
      claim: { identity: OLD_IDENTITY },
      payload: Buffer.from("materialized-turn"),
      previousPayload: Buffer.from("forwarded-update"),
    });
    assert.equal(materialized.admissionRevision, admitted.admissionRevision);
    assert.throws(
      () =>
        harness.store.verifyInboundAdmissionProof({
          ...proof,
          target: { chatId: 1001, threadId: 43 },
        }),
      /identity mismatch/,
    );
    assert.throws(
      () =>
        harness.store.verifyInboundAdmissionProof({
          ...proof,
          admissionRevision: proof.admissionRevision + 1,
        }),
      /identity mismatch/,
    );
  } finally {
    removeHarness(harness);
  }
});

test("quota reservation fails before partial admission and preserves observed retry state", () => {
  const harness = createStoreHarness({ quotaBytes: 2_000 });
  try {
    const observed = harness.store.observeInbound(102, OLD_IDENTITY);
    const before = readStoreSnapshot(harness.rootPath);
    assert.throws(
      () =>
        harness.store.admitInbound({
          recordId: observed.recordId,
          payload: Buffer.alloc(4_000, 7),
          spool: [Buffer.alloc(100, 8)],
        }),
      RecoveryQuotaExceededError,
    );
    const after = readStoreSnapshot(harness.rootPath);
    assert.equal(after.revision, before.revision);
    assert.equal(after.inbound[0]!.state, "observed");
    assert.equal(after.quota.payloadBytes, 0);
    assert.deepEqual(readdirSync(join(harness.rootPath, "payloads")), []);
    assert.deepEqual(readdirSync(join(harness.rootPath, "spool")), []);
    assert.throws(() => harness.store.commitUpdatePrefix(102), /durable admitted/);
  } finally {
    removeHarness(harness);
  }
});

test("lightweight terminal dispositions are idempotent, payload-free, and prefix eligible", () => {
  const harness = createStoreHarness();
  try {
    const terminal = harness.store.recordTerminalInboundDisposition(
      102,
      OLD_IDENTITY,
      "unauthorized",
    );
    assert.equal(terminal.state, "explicitly-discarded");
    assert.equal(terminal.terminalReason, "unauthorized");
    assert.equal(terminal.payloadRef, undefined);
    assert.deepEqual(terminal.spoolRefs, []);
    assert.deepEqual(
      harness.store.recordTerminalInboundDisposition(102, OLD_IDENTITY, "unauthorized"),
      terminal,
    );
    assert.equal(harness.store.commitUpdatePrefix(102), 102);
    assert.throws(
      () => harness.store.recordTerminalInboundDisposition(102, OLD_IDENTITY, "unsupported"),
      /reason collision/,
    );
    const observed = harness.store.observeInbound(103, OLD_IDENTITY);
    assert.equal(
      harness.store.recordTerminalInboundDisposition(
        103,
        OLD_IDENTITY,
        "poison-skipped",
      ).recordId,
      observed.recordId,
    );
    assert.equal(
      harness.store.admitInbound({
        recordId: observed.recordId,
        payload: Buffer.from("late"),
      }).state,
      "explicitly-discarded",
    );
  } finally {
    removeHarness(harness);
  }
});

test("outbound transitions preserve ordered receipts and atomically complete inbound on delivery", () => {
  const harness = createStoreHarness();
  try {
    const { inbound, outbound } = prepareOutbound(harness);
    assert.equal(outbound.state, "planned");
    assert.deepEqual(
      harness.store.listClaimableOutboundRecords({ identity: OLD_IDENTITY })
        .map((item) => item.record.recordId),
      [outbound.recordId],
    );
    assert.equal(
      harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY }).state,
      "pending",
    );
    const first = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    assert.equal(first.record.state, "sending");
    assert.equal(first.record.activeUnit?.unitIndex, 0);
    assert.equal(first.record.automaticAttemptCount, 1);
    assert.throws(
      () => harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY }),
      /sending -> pending/,
    );
    assert.throws(
      () => harness.store.recordOutboundReceipt({
        recordId: outbound.recordId,
        claim: { identity: OLD_IDENTITY },
        attemptId: first.record.activeUnit!.attemptId,
        operationId: "wrong-operation",
        method: "sendRichMessage",
      }),
      /does not match/,
    );
    const afterFirst = harness.store.recordOutboundReceipt({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: first.record.activeUnit!.attemptId,
      operationId: "operation-0",
      method: "sendRichMessage",
      messageId: 900,
    });
    assert.equal(afterFirst.state, "pending");
    assert.equal(afterFirst.nextUnitIndex, 1);
    assert.equal(afterFirst.automaticAttemptCount, 0);
    assert.deepEqual(afterFirst.receipts.map((receipt) => receipt.operationId), ["operation-0"]);
    assert.throws(
      () => harness.store.recordOutboundSafeFailure({
        recordId: outbound.recordId,
        claim: { identity: OLD_IDENTITY },
        attemptId: first.record.activeUnit!.attemptId,
      }),
      /active attempt claim denied/,
    );
    const second = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    const delivered = harness.store.recordOutboundReceipt({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: second.record.activeUnit!.attemptId,
      operationId: "operation-1",
      method: "sendRichMessage",
      messageId: 901,
    });
    assert.equal(delivered.state, "delivered");
    assert.equal(delivered.nextUnitIndex, 2);
    assert.equal(delivered.receipts.length, 2);
    const snapshot = readStoreSnapshot(harness.rootPath);
    const completed = snapshot.inbound.find((record) => record.recordId === inbound.recordId)!;
    const durable = snapshot.outbound.find((record) => record.recordId === outbound.recordId)!;
    assert.equal(completed.state, "completed");
    assert.equal(completed.stateRevision, durable.stateRevision);
    assert.throws(
      () => harness.store.discardOutbound(outbound.recordId, { identity: OLD_IDENTITY }),
      /Cannot discard.*delivered/,
    );
  } finally {
    removeHarness(harness);
  }
});

test("outbound known-safe failures use exactly three starts and exhausted work is discard-only", () => {
  const harness = createStoreHarness();
  try {
    const { inbound, outbound } = prepareOutbound(harness);
    harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
    const first = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    let retryable = harness.store.recordOutboundSafeFailure({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: first.record.activeUnit!.attemptId,
    });
    assert.equal(retryable.automaticAttemptCount, 1);
    assert.equal(retryable.retryNotBeforeMs, harness.now.value + 250);
    assert.throws(
      () => harness.store.claimOutboundUnit({
        recordId: outbound.recordId,
        claim: { identity: OLD_IDENTITY },
      }),
      /not eligible yet/,
    );
    harness.now.value += 250;
    const second = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    retryable = harness.store.recordOutboundSafeFailure({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: second.record.activeUnit!.attemptId,
    });
    assert.equal(retryable.automaticAttemptCount, 2);
    assert.equal(retryable.retryNotBeforeMs, harness.now.value + 1_000);
    harness.now.value += 1_000;
    const third = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    retryable = harness.store.recordOutboundSafeFailure({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: third.record.activeUnit!.attemptId,
    });
    assert.equal(retryable.automaticAttemptCount, 3);
    assert.equal(retryable.retryNotBeforeMs, undefined);
    assert.deepEqual(
      harness.store.listClaimableOutboundRecords({ identity: OLD_IDENTITY }),
      [],
    );
    assert.throws(
      () => harness.store.claimOutboundUnit({
        recordId: outbound.recordId,
        claim: { identity: OLD_IDENTITY },
      }),
      /attempts are exhausted/,
    );
    assert.deepEqual(
      harness.store.drainSafeOutbound({ identity: OLD_IDENTITY }),
      [],
    );
    const statusItem = harness.store.getStatus().items.find(
      (item) => item.family === "outbound" && item.state === "retryable-pending",
    );
    assert.equal(statusItem?.requiredAction, "discard");
    assert.equal(
      harness.store.discardOutboundAction(statusItem!.actionId, {
        identity: OLD_IDENTITY,
      }).state,
      "explicitly-discarded",
    );
    assert.equal(
      readStoreSnapshot(harness.rootPath).inbound.find(
        (record) => record.recordId === inbound.recordId,
      )!.state,
      "completed",
    );
  } finally {
    removeHarness(harness);
  }
});

test("outbound uncertainty never auto-retries and explicit linked retry transfers exact binary references", () => {
  const harness = createStoreHarness();
  try {
    const spool = Buffer.from("uncertain attachment");
    const { inbound, outbound } = prepareOutbound(harness, {
      units: [{
        kind: "attachment",
        operationId: "attachment-operation",
        method: "sendDocument",
        spoolRefIndex: 0,
        fileName: "artifact.txt",
        mediaKind: "document",
      }],
      spool: [spool],
    });
    const beforeRefs = {
      payloadId: outbound.payloadRef!.payloadId,
      spoolIds: outbound.spoolRefs.map((reference) => reference.spoolId),
    };
    harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
    const claimed = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    const uncertain = harness.store.markOutboundUncertain({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: claimed.record.activeUnit!.attemptId,
      reason: "commit-unknown",
    });
    assert.equal(uncertain.state, "delivery-uncertain");
    assert.deepEqual(
      harness.store.listClaimableOutboundRecords({ identity: OLD_IDENTITY }),
      [],
    );
    assert.deepEqual(harness.store.drainSafeOutbound({ identity: OLD_IDENTITY }), []);
    assert.throws(
      () => harness.store.claimOutboundUnit({
        recordId: outbound.recordId,
        claim: { identity: OLD_IDENTITY },
      }),
      /delivery-uncertain -> sending/,
    );
    assert.equal(
      readStoreSnapshot(harness.rootPath).inbound.find(
        (record) => record.recordId === inbound.recordId,
      )!.state,
      "completed",
    );

    const retry = harness.store.retryUncertainOutbound(
      outbound.recordId,
      "intent-linked-retry",
      { identity: OLD_IDENTITY },
    );
    assert.equal(retry.duplicationWarning, true);
    assert.equal(retry.record.state, "pending");
    assert.equal(retry.record.linkedAttemptOf, outbound.recordId);
    assert.equal(retry.record.payloadRef!.payloadId, beforeRefs.payloadId);
    assert.deepEqual(
      retry.record.spoolRefs.map((reference) => reference.spoolId),
      beforeRefs.spoolIds,
    );
    assert.equal(Buffer.from(retry.spool[0]!).toString(), spool.toString());
    const snapshot = readStoreSnapshot(harness.rootPath);
    const source = snapshot.outbound.find((record) => record.recordId === outbound.recordId)!;
    const attempt = snapshot.outbound.find((record) => record.recordId === retry.record.recordId)!;
    assert.equal(source.payloadRef, undefined);
    assert.deepEqual(source.spoolRefs, []);
    assert.equal(attempt.payloadRef!.payloadId, beforeRefs.payloadId);
    assert.equal(
      new Set(snapshot.outbound.flatMap((record) => record.spoolRefs.map((ref) => ref.spoolId))).size,
      1,
    );
    assert.equal(
      harness.store.retryUncertainOutbound(
        outbound.recordId,
        "intent-linked-retry",
        { identity: OLD_IDENTITY },
      ).record.recordId,
      retry.record.recordId,
    );
    assert.throws(
      () => harness.store.retryUncertainOutbound(
        outbound.recordId,
        "different-linked-intent",
        { identity: OLD_IDENTITY },
      ),
      /intent mismatch/,
    );
    const retryClaim = harness.store.claimOutboundUnit({
      recordId: retry.record.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    harness.store.markOutboundUncertain({
      recordId: retry.record.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: retryClaim.record.activeUnit!.attemptId,
      reason: "response-lost",
    });
    const secondRetry = harness.store.retryUncertainOutbound(
      retry.record.recordId,
      "intent-linked-retry-2",
      { identity: OLD_IDENTITY },
    );
    assert.equal(secondRetry.record.linkedAttemptOf, retry.record.recordId);
    assert.equal(secondRetry.record.payloadRef!.payloadId, beforeRefs.payloadId);
    assert.equal(
      harness.store.discardOutbound(outbound.recordId, {
        identity: OLD_IDENTITY,
      }).state,
      "explicitly-discarded",
    );
  } finally {
    removeHarness(harness);
  }
});

test("outbound planning strictly validates semantic payloads, filenames, operation ids, and idempotency", () => {
  const duplicateHarness = createStoreHarness();
  try {
    assert.throws(
      () => prepareOutbound(duplicateHarness, {
        units: [
          {
            kind: "final-text",
            operationId: "duplicate-operation",
            method: "sendRichMessage",
            content: "one",
            contentMode: "rich-markdown",
          },
          {
            kind: "final-text",
            operationId: "duplicate-operation",
            method: "sendRichMessage",
            content: "two",
            contentMode: "rich-markdown",
          },
        ],
      }),
      /duplicate id/,
    );
    assert.equal(readStoreSnapshot(duplicateHarness.rootPath).outbound.length, 0);
  } finally {
    removeHarness(duplicateHarness);
  }

  const filenameHarness = createStoreHarness();
  try {
    assert.throws(
      () => prepareOutbound(filenameHarness, {
        units: [{
          kind: "attachment",
          operationId: "unsafe-file",
          method: "sendDocument",
          spoolRefIndex: 0,
          fileName: "../secret.txt",
          mediaKind: "document",
        }],
        spool: [Buffer.from("secret")],
      }),
      /safe filename/,
    );
    assert.equal(readStoreSnapshot(filenameHarness.rootPath).outbound.length, 0);
  } finally {
    removeHarness(filenameHarness);
  }

  const guestHarness = createStoreHarness();
  try {
    const guest = prepareOutbound(guestHarness, {
      replyToMessageId: 0,
      guestQueryId: "guest-query-1",
      units: [{
        kind: "guest-text",
        operationId: "guest-operation",
        method: "answerGuestQuery",
        markdown: "Guest answer",
      }],
    });
    assert.equal(guest.outbound.state, "planned");
  } finally {
    removeHarness(guestHarness);
  }

  const zeroReplyHarness = createStoreHarness();
  try {
    assert.throws(
      () => prepareOutbound(zeroReplyHarness, { replyToMessageId: 0 }),
      /positive for non-Guest plans/,
    );
    assert.equal(readStoreSnapshot(zeroReplyHarness.rootPath).outbound.length, 0);
  } finally {
    removeHarness(zeroReplyHarness);
  }

  const harness = createStoreHarness();
  try {
    const prepared = prepareOutbound(harness);
    const repeated = harness.store.planOutbound({
      intentId: prepared.outbound.intentId,
      turnId: prepared.inbound.turnId,
      sourceInboundRecordIds: [prepared.inbound.recordId],
      claim: { identity: OLD_IDENTITY },
      replyToMessageId: 77,
      renderingMode: "rich",
      finalMarkdown: "first\n\nsecond",
      renderedChunks: [],
      units: prepared.units,
    });
    assert.equal(repeated.recordId, prepared.outbound.recordId);
    assert.throws(
      () => harness.store.planOutbound({
        intentId: prepared.outbound.intentId,
        turnId: prepared.inbound.turnId,
        sourceInboundRecordIds: [prepared.inbound.recordId],
        claim: { identity: OLD_IDENTITY },
        replyToMessageId: 77,
        renderingMode: "rich",
        finalMarkdown: "changed semantic answer",
        renderedChunks: [],
        units: prepared.units,
      }),
      /idempotency payload mismatch/,
    );
    const wrongIdentity: RecoveryIdentity = {
      ...OLD_IDENTITY,
      owner: { kind: "leader", ownerId: "wrong", leaderEpoch: "wrong" },
    };
    assert.throws(
      () => harness.store.listClaimableOutboundRecords({ identity: wrongIdentity }),
      /not currently authenticated/,
    );
  } finally {
    removeHarness(harness);
  }
});

test("outbound fallback branches preserve stable operations and strict state on reopen", () => {
  const harness = createStoreHarness();
  try {
    const units: Parameters<RecoveryStore["planOutbound"]>[0]["units"] = [
      {
        kind: "rich-media",
        operationId: "rich-primary",
        method: "sendRichMessage",
        spoolRefIndex: 0,
        fileName: "result.png",
        mediaKind: "photo",
        caption: "Result",
        branch: {
          success: { kind: "terminal" },
          knownFailure: {
            kind: "operation",
            operationId: "fallback-text",
          },
        },
      },
      {
        kind: "final-text",
        operationId: "fallback-text",
        method: "sendRichMessage",
        content: "Result",
        contentMode: "rich-markdown",
      },
      {
        kind: "attachment",
        operationId: "fallback-file",
        method: "sendPhoto",
        spoolRefIndex: 0,
        fileName: "result.png",
        mediaKind: "photo",
      },
    ];
    const { outbound } = prepareOutbound(harness, {
      intentId: "branch-intent",
      units,
      spool: [Buffer.from("image")],
    });
    assert.equal(outbound.state, "planned");
    assert.equal(outbound.nextUnitIndex, 0);
    harness.store.activateOutbound(outbound.recordId, {
      identity: OLD_IDENTITY,
    });
    const primary = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    const fallbackPending = harness.store.recordOutboundSafeFailure({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: primary.record.activeUnit!.attemptId,
    });
    assert.equal(fallbackPending.state, "pending");
    assert.equal(fallbackPending.nextUnitIndex, 1);
    assert.equal(fallbackPending.receipts.length, 0);
    assert.deepEqual(fallbackPending.unitProgress, [{
      unitIndex: 0,
      operationId: "rich-primary",
      outcome: "skipped",
      reason: "known-failure",
    }]);
    const reopened = reopenStore(harness);
    const claimed = reopened.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    const payload = JSON.parse(Buffer.from(claimed.payload).toString("utf8")) as {
      units: Array<{ operationId: string }>;
    };
    assert.deepEqual(
      payload.units.map((unit) => unit.operationId),
      ["rich-primary", "fallback-text", "fallback-file"],
    );
    assert.equal(claimed.record.activeUnit?.unitIndex, 1);
  } finally {
    removeHarness(harness);
  }
});

test("outbound state parser accepts a Rich success that skips declared fallback units", () => {
  const harness = createStoreHarness();
  try {
    const { inbound, outbound } = prepareOutbound(harness, {
      intentId: "branch-success-intent",
      units: [
        {
          kind: "rich-media",
          operationId: "rich-success",
          method: "sendRichMessage",
          spoolRefIndex: 0,
          fileName: "result.png",
          mediaKind: "photo",
          caption: "Result",
          branch: {
            success: { kind: "terminal" },
            knownFailure: {
              kind: "operation",
              operationId: "unused-text",
            },
          },
        },
        {
          kind: "final-text",
          operationId: "unused-text",
          method: "sendRichMessage",
          content: "Result",
          contentMode: "rich-markdown",
        },
        {
          kind: "attachment",
          operationId: "unused-file",
          method: "sendPhoto",
          spoolRefIndex: 0,
          fileName: "result.png",
          mediaKind: "photo",
        },
      ],
      spool: [Buffer.from("image")],
    });
    harness.store.activateOutbound(outbound.recordId, {
      identity: OLD_IDENTITY,
    });
    const claimed = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    const delivered = harness.store.recordOutboundReceipt({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: claimed.record.activeUnit!.attemptId,
      operationId: "rich-success",
      method: "sendRichMessage",
      messageId: 99,
    });
    assert.equal(delivered.state, "delivered");
    assert.equal(delivered.nextUnitIndex, 3);
    assert.deepEqual(delivered.receipts.map((receipt) => receipt.unitIndex), [0]);
    assert.deepEqual(delivered.unitProgress, [
      { unitIndex: 0, operationId: "rich-success", outcome: "committed" },
      {
        unitIndex: 1,
        operationId: "unused-text",
        outcome: "skipped",
        reason: "branch",
      },
      {
        unitIndex: 2,
        operationId: "unused-file",
        outcome: "skipped",
        reason: "branch",
      },
    ]);
    assert.equal(
      readStoreSnapshot(harness.rootPath).inbound.find(
        (entry) => entry.recordId === inbound.recordId,
      )!.state,
      "completed",
    );
    assert.doesNotThrow(() => reopenStore(harness));
  } finally {
    removeHarness(harness);
  }
});

test("outbound parser rejects invalid methods, modes, fields, and fallback graphs", () => {
  const expectRejected = (
    units: Parameters<RecoveryStore["planOutbound"]>[0]["units"],
    pattern: RegExp,
    options: { spool?: readonly Uint8Array[]; guest?: boolean } = {},
  ): void => {
    const harness = createStoreHarness();
    try {
      assert.throws(
        () => prepareOutbound(harness, {
          replyToMessageId: options.guest ? 0 : 77,
          ...(options.guest ? { guestQueryId: "strict-guest" } : {}),
          units,
          spool: options.spool,
        }),
        pattern,
      );
      assert.equal(readStoreSnapshot(harness.rootPath).outbound.length, 0);
    } finally {
      removeHarness(harness);
    }
  };

  expectRejected([{
    kind: "final-text",
    operationId: "wrong-text-method",
    method: "sendMessage",
    content: "Rich text",
    contentMode: "rich-markdown",
  }], /does not match final-text content mode/);
  expectRejected([{
    kind: "attachment",
    operationId: "wrong-attachment-media",
    method: "sendPhoto",
    spoolRefIndex: 0,
    fileName: "file.pdf",
    mediaKind: "document",
  }], /attachment method and media kind do not match/, {
    spool: [Buffer.from("file")],
  });
  expectRejected([{
    kind: "guest-text",
    operationId: "guest-wrong-method",
    method: "sendMessage",
    markdown: "text",
  }], /guest-text units require answerGuestQuery/, { guest: true });
  expectRejected([
    {
      kind: "rich-media",
      operationId: "unknown-root",
      method: "sendRichMessage",
      spoolRefIndex: 0,
      fileName: "result.png",
      mediaKind: "photo",
      caption: "Result",
      branch: {
        knownFailure: { kind: "operation", operationId: "missing" },
      },
    },
  ], /references an unknown operation/, { spool: [Buffer.from("image")] });
  expectRejected([
    {
      kind: "rich-media",
      operationId: "ordered-root",
      method: "sendRichMessage",
      spoolRefIndex: 0,
      fileName: "result.png",
      mediaKind: "photo",
      caption: "Result",
      branch: {
        knownFailure: { kind: "operation", operationId: "ordered-root" },
      },
    },
    {
      kind: "final-text",
      operationId: "ordered-text",
      method: "sendRichMessage",
      content: "Result",
      contentMode: "rich-markdown",
    },
    {
      kind: "attachment",
      operationId: "ordered-file",
      method: "sendPhoto",
      spoolRefIndex: 0,
      fileName: "result.png",
      mediaKind: "photo",
    },
  ], /must reference a later operation/, { spool: [Buffer.from("image")] });
  expectRejected([
    {
      kind: "final-text",
      operationId: "first",
      method: "sendRichMessage",
      content: "First",
      contentMode: "rich-markdown",
    },
    {
      kind: "final-text",
      operationId: "second",
      method: "sendRichMessage",
      content: "Second",
      contentMode: "rich-markdown",
      branch: {
        success: { kind: "operation", operationId: "first" },
      },
    },
  ], /must reference a later operation/);
});

test("outbound publication is quota-fenced and digest verification fails closed", () => {
  const quotaHarness = createStoreHarness({ quotaBytes: 32_000 });
  try {
    const inbound = quotaHarness.store.observeInbound(810, OLD_IDENTITY);
    quotaHarness.store.admitInbound({
      recordId: inbound.recordId,
      payload: Buffer.from("quota source"),
    });
    quotaHarness.store.markPreDispatch(inbound.recordId, { identity: OLD_IDENTITY });
    quotaHarness.store.markDispatching(inbound.recordId, { identity: OLD_IDENTITY });
    const payloadNames = readdirSync(quotaHarness.store.payloadDirectory).sort();
    const spoolNames = readdirSync(quotaHarness.store.spoolDirectory).sort();
    assert.throws(
      () => quotaHarness.store.planOutbound({
        intentId: "quota-intent",
        turnId: inbound.turnId,
        sourceInboundRecordIds: [inbound.recordId],
        claim: { identity: OLD_IDENTITY },
        replyToMessageId: 1,
        renderingMode: "rich",
        finalMarkdown: "x".repeat(40_000),
        renderedChunks: [],
        units: [{
          kind: "final-text",
          operationId: "quota-operation",
          method: "sendRichMessage",
          content: "x".repeat(40_000),
          contentMode: "rich-markdown",
        }],
      }),
      RecoveryQuotaExceededError,
    );
    assert.equal(readStoreSnapshot(quotaHarness.rootPath).outbound.length, 0);
    assert.deepEqual(readdirSync(quotaHarness.store.payloadDirectory).sort(), payloadNames);
    assert.deepEqual(readdirSync(quotaHarness.store.spoolDirectory).sort(), spoolNames);
  } finally {
    removeHarness(quotaHarness);
  }

  const digestHarness = createStoreHarness();
  try {
    const { outbound } = prepareOutbound(digestHarness);
    const payloadPath = join(
      digestHarness.store.payloadDirectory,
      `${outbound.payloadRef!.payloadId}.bin`,
    );
    const bytes = readFileSync(payloadPath);
    bytes[0] = bytes[0] === 0x7b ? 0x5b : 0x7b;
    writeFileSync(payloadPath, bytes, { mode: 0o600 });
    assert.throws(
      () => digestHarness.store.listClaimableOutboundRecords({ identity: OLD_IDENTITY }),
      /digest mismatch/,
    );
    assert.throws(() => reopenStore(digestHarness), /digest mismatch/);
  } finally {
    removeHarness(digestHarness);
  }
});

test("outbound discard releases payload and spool only after durable disposition while uncertainty retains both indefinitely", () => {
  const discardHarness = createStoreHarness();
  try {
    const { inbound, outbound } = prepareOutbound(discardHarness, {
      units: [{
        kind: "attachment",
        operationId: "discard-operation",
        method: "sendDocument",
        spoolRefIndex: 0,
        fileName: "discard.txt",
        mediaKind: "document",
      }],
      spool: [Buffer.from("discard spool")],
    });
    const payloadPath = join(
      discardHarness.store.payloadDirectory,
      `${outbound.payloadRef!.payloadId}.bin`,
    );
    const spoolPath = join(
      discardHarness.store.spoolDirectory,
      `${outbound.spoolRefs[0]!.spoolId}.bin`,
    );
    assert.equal(existsSync(payloadPath), true);
    assert.equal(existsSync(spoolPath), true);
    const discarded = discardHarness.store.discardOutbound(
      outbound.recordId,
      { identity: OLD_IDENTITY },
    );
    assert.equal(discarded.state, "explicitly-discarded");
    assert.equal(existsSync(payloadPath), false);
    assert.equal(existsSync(spoolPath), false);
    assert.equal(
      readStoreSnapshot(discardHarness.rootPath).inbound.find(
        (record) => record.recordId === inbound.recordId,
      )!.state,
      "completed",
    );
  } finally {
    removeHarness(discardHarness);
  }

  const uncertainHarness = createStoreHarness();
  try {
    const { outbound } = prepareOutbound(uncertainHarness, {
      units: [{
        kind: "attachment",
        operationId: "retain-operation",
        method: "sendDocument",
        spoolRefIndex: 0,
        fileName: "retain.txt",
        mediaKind: "document",
      }],
      spool: [Buffer.from("retain spool")],
    });
    uncertainHarness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
    const claimed = uncertainHarness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    uncertainHarness.store.markOutboundUncertain({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: claimed.record.activeUnit!.attemptId,
      reason: "response-lost",
    });
    uncertainHarness.now.value +=
      RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS +
      RECOVERY_TERMINAL_METADATA_RETENTION_MS +
      1;
    uncertainHarness.store.compact();
    const retained = readStoreSnapshot(uncertainHarness.rootPath).outbound[0]!;
    assert.equal(retained.state, "delivery-uncertain");
    assert.ok(retained.payloadRef);
    assert.equal(retained.spoolRefs.length, 1);
    assert.equal(readdirSync(uncertainHarness.store.payloadDirectory).length, 1);
    assert.equal(readdirSync(uncertainHarness.store.spoolDirectory).length, 1);
  } finally {
    removeHarness(uncertainHarness);
  }
});

test("outbound discard accepts pending, retryable, and uncertain states but rejects sending", () => {
  for (const state of ["pending", "retryable-pending", "delivery-uncertain"] as const) {
    const harness = createStoreHarness();
    try {
      const { inbound, outbound } = prepareOutbound(harness);
      harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
      if (state !== "pending") {
        const claimed = harness.store.claimOutboundUnit({
          recordId: outbound.recordId,
          claim: { identity: OLD_IDENTITY },
        });
        if (state === "retryable-pending") {
          harness.store.recordOutboundSafeFailure({
            recordId: outbound.recordId,
            claim: { identity: OLD_IDENTITY },
            attemptId: claimed.record.activeUnit!.attemptId,
          });
        } else {
          harness.store.markOutboundUncertain({
            recordId: outbound.recordId,
            claim: { identity: OLD_IDENTITY },
            attemptId: claimed.record.activeUnit!.attemptId,
            reason: "commit-unknown",
          });
        }
      }
      const discarded = harness.store.discardOutbound(
        outbound.recordId,
        { identity: OLD_IDENTITY },
      );
      assert.equal(discarded.state, "explicitly-discarded");
      assert.equal(
        readStoreSnapshot(harness.rootPath).inbound.find(
          (record) => record.recordId === inbound.recordId,
        )!.state,
        "completed",
      );
    } finally {
      removeHarness(harness);
    }
  }

  const sendingHarness = createStoreHarness();
  try {
    const { outbound } = prepareOutbound(sendingHarness);
    const direct = sendingHarness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    assert.equal(direct.record.state, "sending");
    assert.throws(
      () => sendingHarness.store.claimOutboundUnit({
        recordId: outbound.recordId,
        claim: { identity: OLD_IDENTITY },
      }),
      /sending -> sending/,
    );
    assert.throws(
      () => sendingHarness.store.discardOutbound(
        outbound.recordId,
        { identity: OLD_IDENTITY },
      ),
      /Cannot discard.*sending/,
    );
  } finally {
    removeHarness(sendingHarness);
  }
});

test("reopen marks dispatching uncertain, never drains it, and explicit retry is linked and idempotent", () => {
  const harness = createStoreHarness();
  try {
    const observed = harness.store.observeInbound(103, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("uncertain prompt"),
    });
    const claim = { identity: OLD_IDENTITY };
    harness.store.markPreDispatch(observed.recordId, claim);
    harness.store.markDispatching(observed.recordId, claim);

    const reopened = reopenStore(harness);
    const status = reopened.getStatus();
    assert.equal(status.counts["execution-uncertain"], 1);
    assert.equal(status.counts.dispatching, 0);
    assert.deepEqual(reopened.drainSafeInbound(claim), []);
    const uncertainActionId = status.items[0]!.actionId;
    const retry = reopened.retryUncertainInboundAction(uncertainActionId, claim);
    assert.equal(retry.duplicationWarning, true);
    assert.equal(retry.state, "pre-dispatch");
    assert.equal(Buffer.from(retry.payload!).toString(), "uncertain prompt");
    assert.doesNotMatch(JSON.stringify(retry), /recordId|linkedAttemptOf/);
    assert.equal(
      reopened.retryUncertainInboundAction(uncertainActionId, claim).actionId,
      retry.actionId,
    );
    assert.equal(
      reopened.discardInboundAction(retry.actionId, claim).state,
      "explicitly-discarded",
    );
    assert.deepEqual(readdirSync(join(harness.rootPath, "payloads")), []);
  } finally {
    removeHarness(harness);
  }
});

test("foreign store open preserves in-flight work owned by another live runtime", () => {
  const harness = createStoreHarness();
  try {
    const observed = harness.store.observeInbound(104, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("live foreign prompt"),
    });
    const claim = { identity: OLD_IDENTITY };
    harness.store.markPreDispatch(observed.recordId, claim);
    harness.store.markDispatching(observed.recordId, claim);

    let ownsRecoveryAuthority = false;
    const foreign = reopenStore(harness, {
      authenticated: () => false,
      shouldRecoverInFlight: () => ownsRecoveryAuthority,
    });
    const passiveStatus = foreign.getStatus();
    assert.equal(passiveStatus.counts.dispatching, 1);
    assert.equal(passiveStatus.counts["execution-uncertain"], 0);

    ownsRecoveryAuthority = true;
    foreign.initialize();
    const authoritativeStatus = foreign.getStatus();
    assert.equal(authoritativeStatus.counts.dispatching, 0);
    assert.equal(authoritativeStatus.counts["execution-uncertain"], 1);
  } finally {
    removeHarness(harness);
  }
});

test("safe drain advances only admitted work and status remains metadata-only", () => {
  const harness = createStoreHarness();
  try {
    const first = harness.store.observeInbound(104, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: first.recordId, payload: Buffer.from("secret prompt") });
    const second = harness.store.observeInbound(105, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: second.recordId, payload: Buffer.from("another secret") });
    harness.store.markPreDispatch(second.recordId, { identity: OLD_IDENTITY });
    const drained = harness.store.drainSafeInbound({ identity: OLD_IDENTITY });
    assert.deepEqual(drained.map((item) => item.record.recordId), [first.recordId, second.recordId]);
    assert.ok(drained.every((item) => item.record.state === "pre-dispatch"));
    const serializedStatus = JSON.stringify(harness.store.getStatus());
    assert.doesNotMatch(serializedStatus, /secret prompt|another secret|1001|manual-owner-a|registration-a/);
    assert.match(serializedStatus, /"id":"[0-9a-f]{12}"/);
    assert.match(serializedStatus, /"requiredAction":"drain"/);
  } finally {
    removeHarness(harness);
  }
});

test("same-process handoff is exact, expiring, single-use, and reassigns only matching unresolved identity", () => {
  const nextIdentity: RecoveryIdentity = { ...OLD_IDENTITY, sessionGeneration: 4 };
  const harness = createStoreHarness({
    authenticated: (identity) =>
      identitiesMatch(identity, OLD_IDENTITY) || identitiesMatch(identity, nextIdentity),
  });
  try {
    const observed = harness.store.observeInbound(106, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: observed.recordId, payload: Buffer.from("handoff") });
    const { consumedAtMs: _consumedAtMs, ...unconsumedHandoff } = makeHandoff();
    const handoff: RecoverySameProcessHandoff = {
      ...unconsumedHandoff,
      createdAtMs: harness.now.value,
      expiresAtMs: harness.now.value + 500,
    };
    assert.throws(
      () => harness.store.markPreDispatch(observed.recordId, { identity: nextIdentity }),
      /claim denied/,
    );
    const claimed = harness.store.consumeSameProcessHandoff(handoff, nextIdentity);
    assert.deepEqual(claimed.claimedRecordIds, [observed.recordId]);
    assert.equal(claimed.handoff.consumedAtMs, harness.now.value);
    assert.equal(
      harness.store.markPreDispatch(observed.recordId, { identity: nextIdentity }).state,
      "pre-dispatch",
    );
    assert.throws(
      () => harness.store.consumeSameProcessHandoff(handoff, nextIdentity),
      /already consumed/,
    );
    const wrongTarget = { ...nextIdentity, target: { chatId: 1001, threadId: 99 } };
    assert.throws(
      () =>
        harness.store.consumeSameProcessHandoff(
          { ...handoff, handoffId: "wrong-target" },
          wrongTarget,
        ),
      /not currently authenticated|claim denied/,
    );
    harness.now.value = handoff.expiresAtMs + 1;
    assert.throws(
      () =>
        harness.store.consumeSameProcessHandoff(
          { ...handoff, handoffId: "expired-handoff" },
          nextIdentity,
        ),
      /claim denied/,
    );
  } finally {
    removeHarness(harness);
  }
});

test("same-process handoff updates matching unresolved inbound, outbound, and bus records", () => {
  const nextIdentity: RecoveryIdentity = { ...OLD_IDENTITY, sessionGeneration: 4 };
  const harness = createStoreHarness({
    authenticated: (identity) =>
      identitiesMatch(identity, OLD_IDENTITY) || identitiesMatch(identity, nextIdentity),
  });
  try {
    const inbound = harness.store.observeInbound(120, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: inbound.recordId, payload: Buffer.from("handoff") });
    harness.store.markPreDispatch(inbound.recordId, { identity: OLD_IDENTITY });
    harness.store.markDispatching(inbound.recordId, { identity: OLD_IDENTITY });
    const outbound = harness.store.planOutbound({
      intentId: "intent-handoff",
      turnId: inbound.turnId,
      sourceInboundRecordIds: [inbound.recordId],
      claim: { identity: OLD_IDENTITY },
      replyToMessageId: 1,
      renderingMode: "rich",
      finalMarkdown: "handoff reply",
      renderedChunks: [],
      units: [{
        kind: "final-text",
        operationId: "handoff-operation",
        method: "sendRichMessage",
        content: "handoff reply",
        contentMode: "rich-markdown",
      }],
    });
    harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
    const bus = harness.store.admitBus({
      identity: OLD_IDENTITY,
      envelope: {
        kind: "follower.callApi",
        profile: "default",
        target: OLD_IDENTITY.target,
        requestId: "request-handoff",
        instanceId: "follower-a",
        manualFollowerOwnerId: "manual-owner-a",
        registrationGeneration: "registration-a",
        followerSessionGeneration: 3,
        method: "call",
        args: ["sendMessage", { chat_id: 1001, message_thread_id: 42 }],
      },
      apiMethod: "sendMessage",
      leaderEpoch: "leader-epoch-a",
      leaderSessionGeneration: 7,
    });
    const handoff: RecoverySameProcessHandoff = {
      handoffId: "all-family-handoff",
      profile: "default",
      target: OLD_IDENTITY.target,
      owner: OLD_IDENTITY.owner,
      fromSessionGeneration: 3,
      toSessionGeneration: 4,
      createdAtMs: harness.now.value,
      expiresAtMs: harness.now.value + 1000,
    };
    const result = harness.store.consumeSameProcessHandoff(handoff, nextIdentity);
    assert.deepEqual(
      [...result.claimedRecordIds].sort(),
      [inbound.recordId, outbound.recordId, bus.record.recordId].sort(),
    );
    const moved = readStoreSnapshot(harness.rootPath);
    assert.ok(
      [...moved.inbound, ...moved.outbound, ...moved.bus].every(
        (record) => record.identity.sessionGeneration === 4,
      ),
    );
  } finally {
    removeHarness(harness);
  }
});

test("same-process handoff moves uncertain outbound authority with its completed inbound source metadata", () => {
  const nextIdentity: RecoveryIdentity = { ...OLD_IDENTITY, sessionGeneration: 4 };
  const harness = createStoreHarness({
    authenticated: (identity) =>
      identitiesMatch(identity, OLD_IDENTITY) || identitiesMatch(identity, nextIdentity),
  });
  try {
    const { inbound, outbound } = prepareOutbound(harness, { updateId: 121 });
    harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
    const claimed = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    harness.store.markOutboundUncertain({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: claimed.record.activeUnit!.attemptId,
      reason: "commit-unknown",
    });
    const handoff: RecoverySameProcessHandoff = {
      handoffId: "uncertain-outbound-handoff",
      profile: "default",
      target: OLD_IDENTITY.target,
      owner: OLD_IDENTITY.owner,
      fromSessionGeneration: 3,
      toSessionGeneration: 4,
      createdAtMs: harness.now.value,
      expiresAtMs: harness.now.value + 500,
    };
    const result = harness.store.consumeSameProcessHandoff(handoff, nextIdentity);
    assert.deepEqual(result.claimedRecordIds, [outbound.recordId]);
    const snapshot = readStoreSnapshot(harness.rootPath);
    assert.equal(
      snapshot.outbound.find((record) => record.recordId === outbound.recordId)!
        .identity.sessionGeneration,
      4,
    );
    assert.equal(
      snapshot.inbound.find((record) => record.recordId === inbound.recordId)!
        .identity.sessionGeneration,
      4,
    );
  } finally {
    removeHarness(harness);
  }
});

test("explicit reassignment captures exact unresolved ids, fences actions, and grants only the authenticated target", () => {
  const newIdentity: RecoveryIdentity = {
    profile: "default",
    target: OLD_IDENTITY.target,
    owner: NEW_OWNER,
    sessionGeneration: 4,
  };
  let authoritativeBinding: "old" | "new" = "old";
  const validateReassignmentBinding: NonNullable<
    Parameters<typeof openRecoveryStore>[0]["validateReassignmentBinding"]
  > = ({ currentIdentity, expectedBinding }) =>
    identitiesMatch(currentIdentity, newIdentity) &&
    ((expectedBinding === "new-owner" && authoritativeBinding === "new") ||
      (expectedBinding === "old-owner-restored" && authoritativeBinding === "old"));
  const harness = createStoreHarness({
    authenticated: (identity) =>
      identitiesMatch(identity, OLD_IDENTITY) || identitiesMatch(identity, newIdentity),
    validateReassignmentBinding,
  });
  try {
    const first = harness.store.observeInbound(107, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: first.recordId, payload: Buffer.from("move") });
    const request = harness.store.requestReassignment({
      target: OLD_IDENTITY.target,
      oldOwner: OLD_IDENTITY.owner,
      newIdentity,
    });
    assert.deepEqual(request.unresolvedRecordIds, [first.recordId]);
    assert.throws(
      () => harness.store.markPreDispatch(first.recordId, { identity: OLD_IDENTITY }),
      /pending reassignment/,
    );
    assert.throws(
      () => harness.store.observeInbound(108, OLD_IDENTITY),
      /fenced by reassignment/,
    );
    assert.equal(
      harness.store.requestReassignment({
        target: OLD_IDENTITY.target,
        oldOwner: OLD_IDENTITY.owner,
        newIdentity,
      }).reassignmentId,
      request.reassignmentId,
    );
    let runtime = reopenStore(harness, {
      authenticated: (identity) =>
        identitiesMatch(identity, OLD_IDENTITY) || identitiesMatch(identity, newIdentity),
      validateReassignmentBinding,
    });
    assert.equal(
      runtime.requestReassignment({
        target: OLD_IDENTITY.target,
        oldOwner: OLD_IDENTITY.owner,
        newIdentity,
      }).reassignmentId,
      request.reassignmentId,
    );
    runtime.advanceReassignment(request.reassignmentId, "requested", "binding-transfer-pending", newIdentity);
    const missingBindingValidation = reopenStore(harness, {
      authenticated: (identity) => identitiesMatch(identity, newIdentity),
    });
    assert.throws(
      () => missingBindingValidation.advanceReassignment(
        request.reassignmentId,
        "binding-transfer-pending",
        "binding-transferred",
        newIdentity,
      ),
      /revalidation failed/,
    );
    const rejectedBindingValidation = reopenStore(harness, {
      authenticated: (identity) => identitiesMatch(identity, newIdentity),
      validateReassignmentBinding: () => false,
    });
    assert.throws(
      () => rejectedBindingValidation.advanceReassignment(
        request.reassignmentId,
        "binding-transfer-pending",
        "binding-transferred",
        newIdentity,
      ),
      /revalidation failed/,
    );
    authoritativeBinding = "new";
    runtime = reopenStore(harness, {
      authenticated: (identity) => identitiesMatch(identity, newIdentity),
      validateReassignmentBinding,
    });
    runtime.advanceReassignment(request.reassignmentId, "binding-transfer-pending", "binding-transferred", newIdentity);
    runtime = reopenStore(harness, {
      authenticated: (identity) => identitiesMatch(identity, newIdentity),
      validateReassignmentBinding,
    });
    runtime.advanceReassignment(request.reassignmentId, "binding-transferred", "recovery-grant-committed", newIdentity);
    runtime = reopenStore(harness, {
      authenticated: (identity) => identitiesMatch(identity, newIdentity),
      validateReassignmentBinding,
    });
    assert.equal(
      runtime.markPreDispatch(first.recordId, { identity: newIdentity }).state,
      "pre-dispatch",
    );
    const later = harness.store.observeInbound(108, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: later.recordId, payload: Buffer.from("later") });
    assert.throws(
      () => harness.store.markPreDispatch(later.recordId, { identity: newIdentity }),
      /claim denied/,
    );
    const nonOwner = { ...newIdentity, owner: { kind: "leader" as const, ownerId: "other", leaderEpoch: "other" } };
    assert.throws(
      () =>
        runtime.advanceReassignment(
          request.reassignmentId,
          "recovery-grant-committed",
          "recovery-grant-committed",
          nonOwner,
        ),
      /not currently authenticated/,
    );
  } finally {
    removeHarness(harness);
  }
});

test("reassignment rollback requires authoritative old-binding restoration", () => {
  const newIdentity: RecoveryIdentity = {
    profile: "default",
    target: OLD_IDENTITY.target,
    owner: NEW_OWNER,
    sessionGeneration: 4,
  };
  let authoritativeBinding: "old" | "new" = "old";
  const harness = createStoreHarness({
    authenticated: (identity) =>
      identitiesMatch(identity, OLD_IDENTITY) ||
      identitiesMatch(identity, newIdentity),
    validateReassignmentBinding: ({ currentIdentity, expectedBinding }) =>
      identitiesMatch(currentIdentity, newIdentity) &&
      ((expectedBinding === "new-owner" && authoritativeBinding === "new") ||
        (expectedBinding === "old-owner-restored" &&
          authoritativeBinding === "old")),
  });
  try {
    const observed = harness.store.observeInbound(129, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("rollback"),
    });
    const request = harness.store.requestReassignment({
      target: OLD_IDENTITY.target,
      oldOwner: OLD_IDENTITY.owner,
      newIdentity,
    });
    harness.store.advanceReassignment(
      request.reassignmentId,
      "requested",
      "binding-transfer-pending",
      newIdentity,
    );
    authoritativeBinding = "new";
    harness.store.advanceReassignment(
      request.reassignmentId,
      "binding-transfer-pending",
      "binding-transferred",
      newIdentity,
    );
    harness.store.advanceReassignment(
      request.reassignmentId,
      "binding-transferred",
      "rollback-pending",
      newIdentity,
    );
    assert.throws(
      () =>
        harness.store.advanceReassignment(
          request.reassignmentId,
          "rollback-pending",
          "cancelled-before-transfer",
          newIdentity,
        ),
      /revalidation failed/,
    );
    authoritativeBinding = "old";
    assert.equal(
      harness.store.advanceReassignment(
        request.reassignmentId,
        "rollback-pending",
        "cancelled-before-transfer",
        newIdentity,
      ).state,
      "cancelled-before-transfer",
    );
  } finally {
    removeHarness(harness);
  }
});

test("terminal inbound dispositions cannot bypass a pending reassignment fence", () => {
  const newIdentity: RecoveryIdentity = {
    profile: "default",
    target: OLD_IDENTITY.target,
    owner: NEW_OWNER,
    sessionGeneration: 4,
  };
  const harness = createStoreHarness({
    authenticated: (identity) =>
      identitiesMatch(identity, OLD_IDENTITY) ||
      identitiesMatch(identity, newIdentity),
  });
  try {
    harness.store.observeInbound(129, OLD_IDENTITY);
    harness.store.requestReassignment({
      target: OLD_IDENTITY.target,
      oldOwner: OLD_IDENTITY.owner,
      newIdentity,
    });
    assert.throws(
      () =>
        harness.store.recordTerminalInboundDisposition(
          129,
          OLD_IDENTITY,
          "unauthorized",
        ),
      /fenced by reassignment/,
    );
    assert.throws(
      () =>
        harness.store.recordTerminalInboundDisposition(
          130,
          OLD_IDENTITY,
          "unsupported",
        ),
      /fenced by reassignment/,
    );
  } finally {
    removeHarness(harness);
  }
});

test("orphan candidates and recovery actions expose opaque handles and fail closed on collisions", () => {
  const newIdentity: RecoveryIdentity = {
    profile: "default",
    target: OLD_IDENTITY.target,
    owner: NEW_OWNER,
    sessionGeneration: 4,
  };
  const harness = createStoreHarness({
    authenticated: (identity) => identitiesMatch(identity, newIdentity),
  });
  try {
    const observed = harness.store.observeInbound(130, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: observed.recordId, payload: Buffer.from("orphan") });
    const candidates = harness.store.getOrphanReassignmentCandidates();
    assert.equal(candidates.length, 1);
    assert.equal(candidates[0]!.unresolvedCount, 1);
    assert.doesNotMatch(
      JSON.stringify(candidates),
      /1001|manual-owner-a|registration-a|recordId|id-[0-9]/,
    );
    const requested = harness.store.requestReassignmentAction(
      candidates[0]!.actionId,
      newIdentity,
    );
    assert.equal(requested.state, "requested");
    assert.equal(requested.unresolvedCount, 1);
    assert.doesNotMatch(JSON.stringify(requested), /recordId|manual-owner-a|1001/);
  } finally {
    removeHarness(harness);
  }

  const collisionHarness = createStoreHarness({
    authenticated: () => false,
    actionId: () => "collision",
  });
  try {
    collisionHarness.store.observeInbound(131, OLD_IDENTITY);
    collisionHarness.store.observeInbound(132, {
      ...OLD_IDENTITY,
      target: { chatId: 1001, threadId: 99 },
    });
    assert.throws(
      () => collisionHarness.store.getOrphanReassignmentCandidates(),
      /orphan action id collision/,
    );
    assert.throws(() => collisionHarness.store.getStatus(), /action id collision/);
  } finally {
    removeHarness(collisionHarness);
  }
});

test("retention removes terminal payload after 24 hours and metadata after seven days without touching unresolved payload", () => {
  const harness = createStoreHarness();
  try {
    const completed = harness.store.observeInbound(109, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: completed.recordId, payload: Buffer.from("terminal") });
    harness.store.markPreDispatch(completed.recordId, { identity: OLD_IDENTITY });
    harness.store.markDispatching(completed.recordId, { identity: OLD_IDENTITY });
    harness.store.markCompleted(completed.recordId, { identity: OLD_IDENTITY });
    const pending = harness.store.observeInbound(110, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: pending.recordId, payload: Buffer.from("pending") });
    harness.now.value += RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS;
    harness.store.compact();
    let snapshot = readStoreSnapshot(harness.rootPath);
    assert.equal(snapshot.inbound.find((record) => record.recordId === completed.recordId)!.payloadRef, undefined);
    assert.ok(snapshot.inbound.find((record) => record.recordId === pending.recordId)!.payloadRef);
    harness.now.value += RECOVERY_TERMINAL_METADATA_RETENTION_MS + 1;
    harness.store.compact();
    snapshot = readStoreSnapshot(harness.rootPath);
    assert.equal(snapshot.inbound.some((record) => record.recordId === completed.recordId), false);
    assert.equal(snapshot.inbound.some((record) => record.recordId === pending.recordId), true);
  } finally {
    removeHarness(harness);
  }
});

test("retention covers terminal outbound and bus payloads, spool cleanup, and dedup metadata horizon", () => {
  const harness = createStoreHarness();
  try {
    const inbound = harness.store.observeInbound(1090, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: inbound.recordId,
      payload: Buffer.from("outbound source"),
    });
    harness.store.markPreDispatch(inbound.recordId, { identity: OLD_IDENTITY });
    harness.store.markDispatching(inbound.recordId, { identity: OLD_IDENTITY });
    const planned = harness.store.planOutbound({
      intentId: "outbound-terminal-intent",
      turnId: inbound.turnId,
      sourceInboundRecordIds: [inbound.recordId],
      claim: { identity: OLD_IDENTITY },
      replyToMessageId: 1,
      renderingMode: "rich",
      finalMarkdown: "outbound body",
      renderedChunks: [],
      units: [{
        kind: "attachment",
        operationId: "outbound-terminal-operation",
        method: "sendDocument",
        spoolRefIndex: 0,
        fileName: "artifact.txt",
        mediaKind: "document",
      }],
      spool: [Buffer.from("outbound spool")],
    });
    harness.store.activateOutbound(planned.recordId, { identity: OLD_IDENTITY });
    const claimed = harness.store.claimOutboundUnit({
      recordId: planned.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    harness.store.recordOutboundReceipt({
      recordId: planned.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: claimed.record.activeUnit!.attemptId,
      operationId: "outbound-terminal-operation",
      method: "sendDocument",
      messageId: 55,
    });

    const bus = harness.store.admitBus({
      identity: OLD_IDENTITY,
      envelope: {
        kind: "follower.callApi",
        profile: "default",
        target: OLD_IDENTITY.target,
        requestId: "bus-terminal-request",
        instanceId: "follower-a",
        manualFollowerOwnerId: "manual-owner-a",
        registrationGeneration: "registration-a",
        followerSessionGeneration: 3,
        method: "call",
        args: ["sendMessage", { chat_id: 1001, message_thread_id: 42 }],
      },
      apiMethod: "sendMessage",
      leaderEpoch: "leader-epoch-a",
      leaderSessionGeneration: 7,
    });
    harness.store.claimBus({
      recordId: bus.record.recordId,
      identity: OLD_IDENTITY,
      leaderEpoch: "leader-epoch-a",
      leaderSessionGeneration: 7,
    });
    harness.store.completeBus(
      bus.record.recordId,
      { identity: OLD_IDENTITY },
      { ok: true, result: { message_id: 56 } },
    );
    harness.store.compact();
    assert.deepEqual(readdirSync(join(harness.rootPath, "spool")), []);
    let retained = readStoreSnapshot(harness.rootPath);
    assert.ok(retained.outbound[0]!.payloadRef);
    assert.ok(retained.bus[0]!.payloadRef);
    harness.now.value += RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS;
    harness.store.compact();
    retained = readStoreSnapshot(harness.rootPath);
    assert.equal(retained.outbound[0]!.payloadRef, undefined);
    assert.ok(retained.bus[0]!.payloadRef);
    assert.equal(retained.outbound.length, 1);
    assert.equal(retained.bus.length, 1);
    harness.now.value += RECOVERY_TERMINAL_METADATA_RETENTION_MS + 1;
    harness.store.compact();
    retained = readStoreSnapshot(harness.rootPath);
    assert.equal(retained.outbound.length, 0);
    assert.equal(retained.bus.length, 0);
  } finally {
    removeHarness(harness);
  }
});

test("terminal metadata byte cap evicts oldest full serialized records conservatively", () => {
  const harness = createStoreHarness({ terminalMetadataMaxBytes: 1 });
  try {
    const completed = harness.store.observeInbound(140, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: completed.recordId,
      payload: Buffer.from("terminal-cap"),
    });
    harness.store.markPreDispatch(completed.recordId, {
      identity: OLD_IDENTITY,
    });
    harness.store.markDispatching(completed.recordId, {
      identity: OLD_IDENTITY,
    });
    harness.store.markCompleted(completed.recordId, {
      identity: OLD_IDENTITY,
    });
    const unresolved = harness.store.observeInbound(141, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: unresolved.recordId,
      payload: Buffer.from("must-survive"),
    });

    harness.store.compact();
    const snapshot = readStoreSnapshot(harness.rootPath);
    assert.equal(
      snapshot.inbound.some((record) => record.recordId === completed.recordId),
      false,
    );
    assert.equal(
      snapshot.inbound.some((record) => record.recordId === unresolved.recordId),
      true,
    );
  } finally {
    removeHarness(harness);
  }
});

test("terminal metadata cap prunes completed reassignment metadata without unresolved grants", () => {
  const newIdentity: RecoveryIdentity = {
    profile: "default",
    target: OLD_IDENTITY.target,
    owner: NEW_OWNER,
    sessionGeneration: 4,
  };
  const harness = createStoreHarness({
    terminalMetadataMaxBytes: 1,
    authenticated: (identity) =>
      identitiesMatch(identity, OLD_IDENTITY) ||
      identitiesMatch(identity, newIdentity),
    validateReassignmentBinding: ({ expectedBinding }) =>
      expectedBinding === "new-owner",
  });
  try {
    const admitted = harness.store.observeInbound(142, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: admitted.recordId,
      payload: Buffer.from("reassigned-terminal"),
    });
    const reassignment = harness.store.requestReassignment({
      target: OLD_IDENTITY.target,
      oldOwner: OLD_IDENTITY.owner,
      newIdentity,
    });
    harness.store.advanceReassignment(
      reassignment.reassignmentId,
      "requested",
      "binding-transfer-pending",
      newIdentity,
    );
    harness.store.advanceReassignment(
      reassignment.reassignmentId,
      "binding-transfer-pending",
      "binding-transferred",
      newIdentity,
    );
    harness.store.advanceReassignment(
      reassignment.reassignmentId,
      "binding-transferred",
      "recovery-grant-committed",
      newIdentity,
    );
    harness.store.markPreDispatch(admitted.recordId, {
      identity: newIdentity,
    });
    harness.store.markDispatching(admitted.recordId, {
      identity: newIdentity,
    });
    harness.store.markCompleted(admitted.recordId, {
      identity: newIdentity,
    });

    harness.store.compact();
    const snapshot = readStoreSnapshot(harness.rootPath);
    assert.equal(snapshot.inbound.length, 0);
    assert.equal(snapshot.reassignments.length, 0);
  } finally {
    removeHarness(harness);
  }
});

test("corrupt active snapshots and truncated referenced files fail closed while orphan tails reconcile", () => {
  const harness = createStoreHarness();
  try {
    const observed = harness.store.observeInbound(111, OLD_IDENTITY);
    harness.store.admitInbound({ recordId: observed.recordId, payload: Buffer.from("complete") });
    writeFileSync(join(harness.rootPath, "payloads", "orphan.bin"), "tail", { mode: 0o600 });
    writeFileSync(join(harness.rootPath, "snapshot.json.tmp-crash"), "{", { mode: 0o600 });
    const reconciled = reopenStore(harness);
    assert.equal(readdirSync(join(harness.rootPath, "payloads")).includes("orphan.bin"), false);
    assert.equal(readdirSync(harness.rootPath).includes("snapshot.json.tmp-crash"), false);
    assert.ok(reconciled.getStatus().incidents.includes("orphan-snapshot-tail-removed"));
    const payloadName = readdirSync(join(harness.rootPath, "payloads"))[0]!;
    const payloadPath = join(harness.rootPath, "payloads", payloadName);
    writeFileSync(payloadPath, "corrupt!", { mode: 0o600 });
    assert.throws(
      () => harness.store.admitInbound({
        recordId: observed.recordId,
        payload: Buffer.from("complete"),
      }),
      /digest mismatch/,
    );
    assert.throws(
      () => harness.store.drainSafeInbound({ identity: OLD_IDENTITY }),
      /digest mismatch/,
    );
    assert.throws(() => reopenStore(harness), /digest mismatch/);
    writeFileSync(payloadPath, "complete", { mode: 0o600 });
    writeFileSync(payloadPath, "x", { mode: 0o600 });
    assert.throws(() => reopenStore(harness), /truncated recovery binary/);
    writeFileSync(join(harness.rootPath, "snapshot.json"), "{", { mode: 0o600 });
    assert.throws(() => reopenStore(harness), /Invalid recovery snapshot JSON/);
  } finally {
    removeHarness(harness);
  }
});

test("filesystem durability ordering fsyncs parent, binary directories before snapshot, and cleanup directories", () => {
  const events: string[] = [];
  const descriptorPaths = new Map<number, string>();
  const trackedOpen = ((...args: Parameters<typeof openSync>) => {
    const descriptor = openSync(...args);
    descriptorPaths.set(descriptor, String(args[0]));
    return descriptor;
  }) as typeof openSync;
  const trackedFsync = ((descriptor: number) => {
    events.push(`fsync:${descriptorPaths.get(descriptor) ?? "unknown"}`);
    return fsyncSync(descriptor);
  }) as typeof fsyncSync;
  const trackedClose = ((descriptor: number) => {
    descriptorPaths.delete(descriptor);
    return closeSync(descriptor);
  }) as typeof closeSync;
  const trackedRename = ((...args: Parameters<typeof renameSync>) => {
    events.push(`rename:${String(args[0])}->${String(args[1])}`);
    return renameSync(...args);
  }) as typeof renameSync;
  const harness = createStoreHarness({
    fs: {
      open: trackedOpen,
      fsync: trackedFsync,
      close: trackedClose,
      rename: trackedRename,
    },
  });
  try {
    assert.ok(events.includes(`fsync:${harness.directory}`));
    events.length = 0;
    const observed = harness.store.observeInbound(140, OLD_IDENTITY);
    events.length = 0;
    harness.store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("payload"),
      spool: [Buffer.from("spool")],
    });
    const payloadRename = events.findIndex(
      (entry) =>
        entry.includes(`${sep}payloads${sep}`) && entry.startsWith("rename:"),
    );
    const payloadDirectoryFsync = events.findIndex((entry) => entry === `fsync:${join(harness.rootPath, "payloads")}`);
    const spoolDirectoryFsync = events.findIndex((entry) => entry === `fsync:${join(harness.rootPath, "spool")}`);
    const snapshotRename = events.findIndex(
      (entry) => entry.startsWith("rename:") && entry.endsWith("->" + join(harness.rootPath, "snapshot.json")),
    );
    assert.ok(payloadRename >= 0);
    assert.ok(payloadDirectoryFsync > payloadRename);
    assert.ok(spoolDirectoryFsync > payloadRename);
    assert.ok(snapshotRename > payloadDirectoryFsync);
    assert.ok(snapshotRename > spoolDirectoryFsync);

    harness.store.markPreDispatch(observed.recordId, { identity: OLD_IDENTITY });
    harness.store.markDispatching(observed.recordId, { identity: OLD_IDENTITY });
    events.length = 0;
    harness.store.markCompleted(observed.recordId, { identity: OLD_IDENTITY });
    assert.ok(events.includes(`fsync:${join(harness.rootPath, "spool")}`));
  } finally {
    removeHarness(harness);
  }
});

test("downgrade is exclusive, refuses blockers, quarantines durably, and requires explicit restore", () => {
  const harness = createStoreHarness();
  try {
    const observed = harness.store.observeInbound(112, OLD_IDENTITY);
    assert.throws(() => harness.store.beginDowngradeExclusive(), /blocked by nonterminal/);
    assert.equal(harness.store.getStatus().admissionEnabled, true);
    harness.store.discardInbound(observed.recordId, { identity: OLD_IDENTITY });
    assert.deepEqual(harness.store.beginDowngradeExclusive(), {
      safe: true,
      blockerCount: 0,
      blockers: [],
    });
    assert.equal(harness.store.getStatus().admissionEnabled, false);
    assert.throws(() => harness.store.observeInbound(113, OLD_IDENTITY), /admission is disabled/);
    const quarantinePath = harness.store.quarantineForDowngrade();
    assert.equal(statSync(quarantinePath).isDirectory(), true);
    assert.throws(
      () => reopenStore(harness),
      (error: unknown) => error instanceof RecoveryQuarantineError,
    );
    restoreRecoveryQuarantine({
      profile: "default",
      rootPath: harness.rootPath,
      quarantinePath,
    });
    assert.equal(reopenStore(harness).getStatus().admissionEnabled, true);
  } finally {
    removeHarness(harness);
  }
});

test("quarantine restore validates every referenced binary before activation", () => {
  const harness = createStoreHarness();
  try {
    const completed = harness.store.observeInbound(150, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: completed.recordId,
      payload: Buffer.from("restore-ok"),
    });
    harness.store.markPreDispatch(completed.recordId, {
      identity: OLD_IDENTITY,
    });
    harness.store.markDispatching(completed.recordId, {
      identity: OLD_IDENTITY,
    });
    harness.store.markCompleted(completed.recordId, {
      identity: OLD_IDENTITY,
    });
    harness.store.beginDowngradeExclusive();
    const quarantinePath = harness.store.quarantineForDowngrade();
    const payloadPath = join(
      quarantinePath,
      "payloads",
      readdirSync(join(quarantinePath, "payloads"))[0]!,
    );
    writeFileSync(payloadPath, "restore-no", { mode: 0o600 });

    assert.throws(
      () =>
        restoreRecoveryQuarantine({
          profile: "default",
          rootPath: harness.rootPath,
          quarantinePath,
        }),
      /digest mismatch/,
    );
    assert.equal(existsSync(harness.rootPath), false);
    assert.equal(statSync(quarantinePath).isDirectory(), true);
  } finally {
    removeHarness(harness);
  }
});

test("final outbound uncertainty atomically completes every exact grouped inbound source", () => {
  const harness = createStoreHarness();
  try {
    const observed = [820, 821].map((updateId) => {
      const record = harness.store.observeInbound(updateId, OLD_IDENTITY);
      harness.store.admitInbound({
        recordId: record.recordId,
        payload: Buffer.from(`raw-${updateId}`),
      });
      return record;
    });
    const materialized = harness.store.materializeInboundGroup(
      observed.map((record, index) => ({
        recordId: record.recordId,
        claim: { identity: OLD_IDENTITY },
        turnId: "grouped-outbound-turn",
        payload: Buffer.from(`grouped-${index}`),
      })),
    );
    harness.store.markDispatchingGroup(
      materialized.map((record) => ({
        recordId: record.recordId,
        claim: { identity: OLD_IDENTITY },
      })),
    );
    const outbound = harness.store.planOutbound({
      intentId: "grouped-outbound-intent",
      turnId: "grouped-outbound-turn",
      sourceInboundRecordIds: materialized.map((record) => record.recordId),
      claim: { identity: OLD_IDENTITY },
      replyToMessageId: 99,
      renderingMode: "rich",
      finalMarkdown: "grouped result",
      renderedChunks: [],
      units: [{
        kind: "final-text",
        operationId: "grouped-operation",
        method: "sendRichMessage",
        content: "grouped result",
        contentMode: "rich-markdown",
      }],
    });
    harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
    const claimed = harness.store.claimOutboundUnit({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
    });
    harness.store.markOutboundUncertain({
      recordId: outbound.recordId,
      claim: { identity: OLD_IDENTITY },
      attemptId: claimed.record.activeUnit!.attemptId,
      reason: "confirmed-before-receipt",
    });
    const snapshot = readStoreSnapshot(harness.rootPath);
    const durableOutbound = snapshot.outbound.find(
      (record) => record.recordId === outbound.recordId,
    )!;
    const sources = snapshot.inbound.filter((record) =>
      materialized.some((source) => source.recordId === record.recordId),
    );
    assert.equal(durableOutbound.state, "delivery-uncertain");
    assert.equal(sources.length, 2);
    assert.ok(sources.every((record) => record.state === "completed"));
    assert.ok(
      sources.every((record) => record.stateRevision === durableOutbound.stateRevision),
    );
  } finally {
    removeHarness(harness);
  }
});

test("OUT-02 through OUT-06 fault seams preserve their exact durable storage boundary", () => {
  for (const faultId of ["OUT-02", "OUT-03", "OUT-04", "OUT-05", "OUT-06"] as const) {
    const controller = new ReliabilityFaultController(faultId, {
      seed: `outbound-${faultId}`,
    });
    const harness = createStoreHarness({ fault: (id) => controller.hit(id) });
    try {
      const { inbound, outbound } = prepareOutbound(harness, {
        units: [{
          kind: "final-text",
          operationId: "fault-operation",
          method: "sendRichMessage",
          content: "fault result",
          contentMode: "rich-markdown",
        }],
      });
      if (faultId === "OUT-02") {
        assert.throws(
          () => harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY }),
          InjectedReliabilityFaultError,
        );
      } else {
        harness.store.activateOutbound(outbound.recordId, { identity: OLD_IDENTITY });
        const claimed = harness.store.claimOutboundUnit({
          recordId: outbound.recordId,
          claim: { identity: OLD_IDENTITY },
        });
        if (faultId === "OUT-03") {
          assert.throws(
            () => harness.store.recordOutboundSafeFailure({
              recordId: outbound.recordId,
              claim: { identity: OLD_IDENTITY },
              attemptId: claimed.record.activeUnit!.attemptId,
            }),
            InjectedReliabilityFaultError,
          );
        } else if (faultId === "OUT-04") {
          assert.throws(
            () => harness.store.markOutboundUncertain({
              recordId: outbound.recordId,
              claim: { identity: OLD_IDENTITY },
              attemptId: claimed.record.activeUnit!.attemptId,
              reason: "commit-unknown",
            }),
            InjectedReliabilityFaultError,
          );
        } else {
          assert.throws(
            () => harness.store.recordOutboundReceipt({
              recordId: outbound.recordId,
              claim: { identity: OLD_IDENTITY },
              attemptId: claimed.record.activeUnit!.attemptId,
              operationId: "fault-operation",
              method: "sendRichMessage",
              messageId: 123,
            }),
            InjectedReliabilityFaultError,
          );
        }
      }
      controller.assertInjected();
      const snapshot = readStoreSnapshot(harness.rootPath);
      const stored = snapshot.outbound.find((record) => record.recordId === outbound.recordId)!;
      const storedInbound = snapshot.inbound.find((record) => record.recordId === inbound.recordId)!;
      const expectedState = {
        "OUT-02": "pending",
        "OUT-03": "retryable-pending",
        "OUT-04": "delivery-uncertain",
        "OUT-05": "sending",
        "OUT-06": "delivered",
      } as const;
      assert.equal(stored.state, expectedState[faultId]);
      assert.equal(
        storedInbound.state,
        faultId === "OUT-04" || faultId === "OUT-06" ? "completed" : "dispatching",
      );
      const reopened = reopenStore(harness);
      const reopenedSnapshot = readStoreSnapshot(harness.rootPath);
      const reopenedOutbound = reopenedSnapshot.outbound.find(
        (record) => record.recordId === outbound.recordId,
      )!;
      assert.equal(
        reopenedOutbound.state,
        faultId === "OUT-05" ? "delivery-uncertain" : expectedState[faultId],
      );
      if (faultId === "OUT-05") {
        assert.deepEqual(reopenedOutbound.uncertainty, {
          unitIndex: 0,
          reason: "process-reopened-sending",
          observedAtMs: reopenedOutbound.updatedAtMs,
        });
        assert.equal(
          reopenedSnapshot.inbound.find(
            (record) => record.recordId === inbound.recordId,
          )!.state,
          "completed",
        );
      }
      assert.equal(
        reopened.getStatus().items.some((item) => item.family === "outbound"),
        faultId !== "OUT-06",
      );
    } finally {
      removeHarness(harness);
    }
  }
});

test("all injected inbound and downgrade store seams leave a fail-closed reopen state", () => {
  const cases = [
    "IN-01", "IN-02", "IN-03", "IN-04", "IN-05",
    "IN-06", "IN-07", "IN-08", "DOWN-01", "DOWN-02",
  ] as const;
  for (const faultId of cases) {
    const controller = new ReliabilityFaultController(faultId, { seed: `seed-${faultId}` });
    const harness = createStoreHarness({
      fault: (id) => controller.hit(id),
    });
    try {
      if (faultId === "IN-01") {
        assert.throws(() => harness.store.observeInbound(200, OLD_IDENTITY), InjectedReliabilityFaultError);
        assert.equal(reopenStore(harness).getStatus().counts.observed, 0);
      } else {
        const observed = harness.store.observeInbound(200, OLD_IDENTITY);
        if (faultId === "IN-02" || faultId === "IN-03") {
          assert.throws(
            () => harness.store.admitInbound({ recordId: observed.recordId, payload: Buffer.from("fault") }),
            InjectedReliabilityFaultError,
          );
          const reopened = reopenStore(harness);
          const reopenedStatus = reopened.getStatus();
          assert.equal(
            reopenedStatus.counts[faultId === "IN-02" ? "observed" : "admitted"],
            1,
          );
          if (faultId === "IN-02") {
            assert.deepEqual(readdirSync(join(harness.rootPath, "payloads")), []);
            assert.ok(reopenedStatus.incidents.includes("orphan-recovery-file-removed"));
          }
        } else {
          harness.store.admitInbound({ recordId: observed.recordId, payload: Buffer.from("fault") });
          const claim = { identity: OLD_IDENTITY };
          if (faultId === "IN-04") {
            assert.throws(() => harness.store.commitUpdatePrefix(200), InjectedReliabilityFaultError);
            const reopened = reopenStore(harness);
            assert.equal(reopened.getCommittedUpdateId(), 200);
            const rehydrated = reopened.drainSafeInbound(claim);
            assert.equal(rehydrated.length, 1);
            assert.equal(rehydrated[0]!.record.state, "pre-dispatch");
            assert.equal(Buffer.from(rehydrated[0]!.payload).toString(), "fault");
          } else if (faultId === "IN-05") {
            assert.throws(() => harness.store.markPreDispatch(observed.recordId, claim), InjectedReliabilityFaultError);
            assert.equal(reopenStore(harness).getStatus().counts["pre-dispatch"], 1);
          } else if (faultId === "IN-06") {
            harness.store.markPreDispatch(observed.recordId, claim);
            assert.throws(() => harness.store.markDispatching(observed.recordId, claim), InjectedReliabilityFaultError);
            assert.equal(reopenStore(harness).getStatus().counts["execution-uncertain"], 1);
          } else if (faultId === "IN-07") {
            harness.store.markPreDispatch(observed.recordId, claim);
            harness.store.markDispatching(observed.recordId, claim);
            assert.throws(() => harness.store.markExecutionUncertain(observed.recordId, claim), InjectedReliabilityFaultError);
            assert.equal(reopenStore(harness).getStatus().counts["execution-uncertain"], 1);
          } else if (faultId === "IN-08") {
            harness.store.markPreDispatch(observed.recordId, claim);
            harness.store.markDispatching(observed.recordId, claim);
            assert.throws(() => harness.store.markCompleted(observed.recordId, claim), InjectedReliabilityFaultError);
            assert.equal(reopenStore(harness).getStatus().counts.completed, 1);
          } else {
            harness.store.discardInbound(observed.recordId, claim);
            if (faultId === "DOWN-01") {
              assert.throws(() => harness.store.beginDowngradeExclusive(), InjectedReliabilityFaultError);
              assert.equal(reopenStore(harness).getStatus().mode, "active");
            } else {
              harness.store.beginDowngradeExclusive();
              assert.throws(() => harness.store.quarantineForDowngrade(), InjectedReliabilityFaultError);
              assert.throws(() => reopenStore(harness), RecoveryQuarantineError);
            }
          }
        }
      }
      controller.assertInjected();
    } finally {
      removeHarness(harness);
    }
  }
});

const EXPECTED_FAULT_IDS = [
  "IN-01", "IN-02", "IN-03", "IN-04", "IN-05", "IN-06", "IN-07", "IN-08",
  "IN-GROUP-01", "IN-GROUP-02", "IN-GROUP-03",
  "OUT-01", "OUT-02", "OUT-03", "OUT-04", "OUT-05", "OUT-06",
  "BUS-01", "BUS-02", "BUS-03", "BUS-04", "PAIR-01", "PAIR-02", "DOWN-01", "DOWN-02",
] satisfies ReliabilityFaultId[];

test("post-commit cleanup is fail-soft and the next mutation reconciles physical files", () => {
  const agentDir = mkdtempSync(join(tmpdir(), "pi-telegram-cleanup-"));
  const rootPath = join(agentDir, "recovery-v1");
  let failNextPayloadCleanup = false;
  let payloadCleanupAttempts = 0;
  const trackedRemove = ((...args: Parameters<typeof rmSync>) => {
    const path = String(args[0]);
    if (
      path.includes(`${join(rootPath, "payloads")}${sep}`) &&
      path.endsWith(".bin")
    ) {
      payloadCleanupAttempts += 1;
      if (failNextPayloadCleanup) {
        failNextPayloadCleanup = false;
        throw new Error("injected cleanup failure");
      }
    }
    return rmSync(...args);
  }) as typeof rmSync;
  try {
    const identity = {
      profile: "default",
      target: { chatId: 7 },
      owner: {
        kind: "leader" as const,
        ownerId: "owner",
        leaderEpoch: "epoch",
      },
      sessionGeneration: 1,
    };
    const store = openRecoveryStore({
      profile: "default",
      rootPath,
      fs: { remove: trackedRemove },
      isIdentityAuthenticated: () => true,
    });
    const observed = store.observeInbound(500, identity);
    store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("raw-update"),
    });
    failNextPayloadCleanup = true;
    assert.doesNotThrow(() =>
      store.materializeInbound({
        recordId: observed.recordId,
        claim: { identity },
        payload: Buffer.from("materialized-turn"),
        previousPayload: Buffer.from("raw-update"),
      }),
    );
    assert.ok(store.getStatus().incidents.includes("post-commit-cleanup-failed"));
    assert.equal(payloadCleanupAttempts, 1);
    store.observeInbound(501, identity);
    assert.equal(payloadCleanupAttempts, 2);
  } finally {
    rmSync(agentDir, { force: true, recursive: true });
  }
});

test("materialized spool bytes are reserved, reconciled, verified, and quota-fenced before publication", () => {
  const agentDir = mkdtempSync(join(tmpdir(), "pi-telegram-materialized-quota-"));
  try {
    const rootPath = join(agentDir, "recovery-v1");
    const store = openRecoveryStore({
      profile: "default",
      rootPath,
      isIdentityAuthenticated: () => true,
    });
    const spool = Buffer.from("materialized-attachment");
    const observed = store.observeInbound(601, OLD_IDENTITY);
    store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("turn"),
      spool: [spool],
    });
    store.markPreDispatch(observed.recordId, { identity: OLD_IDENTITY });
    const beforeRestore = store.getStatus();
    const limited = openRecoveryStore({
      profile: "default",
      rootPath,
      quotaBytes: beforeRestore.quota.totalBytes + spool.byteLength - 1,
      isIdentityAuthenticated: () => true,
    });
    assert.throws(
      () =>
        limited.restoreInboundSpool(
          observed.recordId,
          { identity: OLD_IDENTITY },
          ["attachment.txt"],
        ),
      RecoveryQuotaExceededError,
    );
    assert.deepEqual(readdirSync(store.materializedDirectory), []);
    assert.equal(store.getStatus().quota.reservedBytes, 0);

    const [materializedPath] = store.restoreInboundSpool(
      observed.recordId,
      { identity: OLD_IDENTITY },
      ["attachment.txt"],
    );
    assert.ok(materializedPath);
    assert.equal(readFileSync(materializedPath).toString(), spool.toString());
    const restoredStatus = store.getStatus();
    assert.equal(restoredStatus.quota.reservedBytes, spool.byteLength);
    assert.equal(
      restoredStatus.quota.totalBytes,
      restoredStatus.quota.recordBytes +
        restoredStatus.quota.payloadBytes +
        restoredStatus.quota.spoolBytes +
        restoredStatus.quota.reservedBytes,
    );

    writeFileSync(materializedPath, Buffer.alloc(spool.byteLength, 0x78));
    const orphan = join(store.materializedDirectory, "orphan.tmp-tail");
    writeFileSync(orphan, "orphan");
    store.observeInbound(602, OLD_IDENTITY);
    assert.equal(existsSync(materializedPath), false);
    assert.equal(existsSync(orphan), false);
    assert.equal(store.getStatus().quota.reservedBytes, 0);
    assert.ok(
      store
        .getStatus()
        .incidents.includes("corrupt-materialized-cache-removed"),
    );
  } finally {
    rmSync(agentDir, { force: true, recursive: true });
  }
});

test("materialized cleanup failure is fail-soft, remains reserved, and blocks later quota mutation", () => {
  const agentDir = mkdtempSync(join(tmpdir(), "pi-telegram-materialized-cleanup-"));
  const rootPath = join(agentDir, "recovery-v1");
  let failMaterializedCleanup = false;
  const trackedRemove = ((...args: Parameters<typeof rmSync>) => {
    const path = String(args[0]);
    if (
      failMaterializedCleanup &&
      path.startsWith(join(rootPath, "materialized"))
    ) {
      throw new Error("persistent materialized cleanup failure");
    }
    return rmSync(...args);
  }) as typeof rmSync;
  try {
    const store = openRecoveryStore({
      profile: "default",
      rootPath,
      fs: { remove: trackedRemove },
      isIdentityAuthenticated: () => true,
    });
    const spool = Buffer.from("retained-cache");
    const observed = store.observeInbound(611, OLD_IDENTITY);
    store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("turn"),
      spool: [spool],
    });
    store.markPreDispatch(observed.recordId, { identity: OLD_IDENTITY });
    const [materializedPath] = store.restoreInboundSpool(
      observed.recordId,
      { identity: OLD_IDENTITY },
      ["attachment.txt"],
    );
    assert.ok(materializedPath);
    failMaterializedCleanup = true;
    store.markDispatching(observed.recordId, { identity: OLD_IDENTITY });
    assert.doesNotThrow(() =>
      store.markCompleted(observed.recordId, { identity: OLD_IDENTITY }),
    );
    assert.equal(existsSync(materializedPath), true);
    const status = store.getStatus();
    assert.equal(status.quota.reservedBytes, spool.byteLength);
    assert.ok(status.incidents.includes("post-commit-cleanup-failed"));
    assert.ok(status.incidents.includes("post-commit-quota-refresh-failed"));
    assert.throws(
      () => store.observeInbound(612, OLD_IDENTITY),
      /persistent materialized cleanup failure/,
    );
  } finally {
    failMaterializedCleanup = false;
    rmSync(agentDir, { force: true, recursive: true });
  }
});

test("fault inventory is exact and controller injects once with seed", () => {
  assert.deepEqual(RELIABILITY_FAULT_IDS, EXPECTED_FAULT_IDS);
  assert.equal(new Set(RELIABILITY_FAULT_IDS).size, 25);
  const controller = new ReliabilityFaultController("OUT-04", { seed: 2026072201 });
  controller.hit("IN-01");
  assert.throws(
    () => controller.hit("OUT-04"),
    (error: unknown) =>
      error instanceof InjectedReliabilityFaultError &&
      error.faultId === "OUT-04" &&
      error.message.includes("PI_RELIABILITY_SEED=2026072201"),
  );
  controller.hit("OUT-04");
  controller.assertInjected();
  assert.deepEqual(controller.hits, ["IN-01", "OUT-04", "OUT-04"]);
});

test("fault controller requires completion and reports missed boundary seed", () => {
  const missed = new ReliabilityFaultController("DOWN-02", {
    env: { PI_RELIABILITY_SEED: "seed-from-env" },
  });
  assert.throws(
    () => missed.assertInjected(),
    /DOWN-02.*PI_RELIABILITY_SEED=seed-from-env/,
  );
  assert.throws(
    () => new ReliabilityFaultController("IN-01", { env: {} }),
    /requires PI_RELIABILITY_SEED/,
  );
});

test("fault helper remains excluded from package files", () => {
  const manifest = JSON.parse(readFileSync("package.json", "utf8")) as { files?: string[] };
  assert.ok(manifest.files);
  assert.equal(manifest.files.some((entry) => entry.startsWith("tests")), false);
});

test("inbound group materialization faults preserve all records, quota, and files", () => {
  for (const faultId of [
    "IN-GROUP-01",
    "IN-GROUP-02",
    "IN-GROUP-03",
  ] as const) {
    const agentDir = mkdtempSync(join(tmpdir(), `pi-telegram-${faultId}-`));
    try {
      const controller = new ReliabilityFaultController(faultId, {
        seed: `group-${faultId}`,
      });
      const store = openRecoveryStore({
        profile: "default",
        agentDir,
        isIdentityAuthenticated: () => true,
        fault: (id) => controller.hit(id),
      });
      const records = [701, 702].map((updateId) => {
        const observed = store.observeInbound(updateId, OLD_IDENTITY);
        return store.admitInbound({
          recordId: observed.recordId,
          payload: Buffer.from(`raw-${updateId}`),
        });
      });
      const before = parseRecoverySnapshot(
        readFileSync(store.snapshotPath, "utf8"),
      );
      const payloadNames = readdirSync(store.payloadDirectory).sort();
      const spoolNames = readdirSync(store.spoolDirectory).sort();
      assert.throws(
        () =>
          store.materializeInboundGroup(
            records.map((record) => ({
              recordId: record.recordId,
              claim: { identity: OLD_IDENTITY },
              turnId: "group-turn",
              payload: Buffer.from("materialized-group"),
              spool: [Buffer.from("spool-a"), Buffer.from("spool-b")],
              previousPayload: Buffer.from(`raw-${record.updateId}`),
            }))),
        InjectedReliabilityFaultError,
      );
      controller.assertInjected();
      const after = parseRecoverySnapshot(
        readFileSync(store.snapshotPath, "utf8"),
      );
      assert.deepEqual(
        after.inbound.map((record) => record.state),
        ["admitted", "admitted"],
      );
      assert.deepEqual(after.quota, before.quota);
      assert.deepEqual(readdirSync(store.payloadDirectory).sort(), payloadNames);
      assert.deepEqual(readdirSync(store.spoolDirectory).sort(), spoolNames);
    } finally {
      rmSync(agentDir, { recursive: true, force: true });
    }
  }
});

test("inbound group materialization and lifecycle transitions commit one group revision", () => {
  const agentDir = mkdtempSync(join(tmpdir(), "pi-telegram-group-commit-"));
  try {
    const store = openRecoveryStore({
      profile: "default",
      agentDir,
      isIdentityAuthenticated: () => true,
    });
    const records = [711, 712].map((updateId) => {
      const observed = store.observeInbound(updateId, OLD_IDENTITY);
      return store.admitInbound({
        recordId: observed.recordId,
        payload: Buffer.from(`raw-${updateId}`),
      });
    });
    const materialized = store.materializeInboundGroup(
      records.map((record) => ({
        recordId: record.recordId,
        claim: { identity: OLD_IDENTITY },
        turnId: "canonical-group-turn",
        payload: Buffer.from("materialized-group"),
        previousPayload: Buffer.from(`raw-${record.updateId}`),
      })),
    );
    assert.deepEqual(
      materialized.map(({ state, turnId, stateRevision }) => ({
        state,
        turnId,
        stateRevision,
      })),
      [
        {
          state: "pre-dispatch",
          turnId: "canonical-group-turn",
          stateRevision: materialized[0]!.stateRevision,
        },
        {
          state: "pre-dispatch",
          turnId: "canonical-group-turn",
          stateRevision: materialized[0]!.stateRevision,
        },
      ],
    );
    const claims = materialized.map((record) => ({
      recordId: record.recordId,
      claim: { identity: OLD_IDENTITY },
    }));
    const dispatching = store.markDispatchingGroup(claims);
    const completed = store.markCompletedGroup(claims);
    assert.equal(new Set(dispatching.map((record) => record.stateRevision)).size, 1);
    assert.equal(new Set(completed.map((record) => record.stateRevision)).size, 1);
    assert.deepEqual(completed.map((record) => record.state), ["completed", "completed"]);
  } finally {
    rmSync(agentDir, { recursive: true, force: true });
  }
});

test("opaque reassignment completion resumes crash stages idempotently and fences the target tuple", () => {
  const harness = createStoreHarness();
  try {
    const observed = harness.store.observeInbound(9090, OLD_IDENTITY);
    harness.store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("reassign-once"),
    });
    const newIdentity: RecoveryIdentity = {
      ...OLD_IDENTITY,
      owner: NEW_OWNER,
      sessionGeneration: OLD_IDENTITY.sessionGeneration + 1,
    };
    const runtime = reopenStore(harness, {
      authenticated: (identity) => identitiesMatch(identity, newIdentity),
      validateReassignmentBinding: () => true,
    });
    const candidate = runtime.getOrphanReassignmentCandidates()[0]!;
    const requested = runtime.requestReassignmentAction(
      candidate.actionId,
      newIdentity,
    );
    const snapshot = readStoreSnapshot(harness.rootPath);
    const reassignment = snapshot.reassignments[0]!;

    runtime.advanceReassignment(
      reassignment.reassignmentId,
      "requested",
      "binding-transfer-pending",
      newIdentity,
    );
    assert.throws(
      () => runtime.observeInbound(9091, newIdentity),
      /fenced by reassignment/,
    );

    const completed = runtime.completeReassignmentAction(
      requested.actionId,
      newIdentity,
    );
    assert.equal(completed.state, "recovery-grant-committed");
    assert.deepEqual(
      runtime.completeReassignmentAction(requested.actionId, newIdentity),
      completed,
    );
    assert.throws(
      () => runtime.completeReassignmentAction("wrong-action", newIdentity),
      /Unknown recovery reassignment action id/,
    );
  } finally {
    removeHarness(harness);
  }
});
