/**
 * Regression tests for recovery schema contracts and deterministic fault injection
 * Zones: recovery, queue lifecycle, delivery, multi-instance bus, test infrastructure
 * Guards strict versioned parsing, frozen policy constants, and the bounded S0 fault inventory.
 */

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  areRecoveryOwnersEqual,
  parseRecoverySnapshot,
  RECOVERY_BUS_STATES,
  RECOVERY_DELIVERED_PAYLOAD_RETENTION_MS,
  RECOVERY_INBOUND_STATES,
  RECOVERY_OUTBOUND_STATES,
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
  type RecoveryIdentity,
  type RecoveryOwnerIdentity,
  type RecoverySameProcessHandoff,
  type RecoverySnapshotV1,
  validateRecoverySameProcessHandoff,
  validateRecoverySnapshot,
} from "../lib/recovery.ts";
import {
  InjectedReliabilityFaultError,
  RELIABILITY_FAULT_IDS,
  ReliabilityFaultController,
  type ReliabilityFaultId,
} from "./fixtures/reliability-faults.ts";

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
  const inbound = RECOVERY_INBOUND_STATES.map((state, index) => ({
    family: "inbound" as const,
    recordId: `inbound-${index}`,
    updateId: 2000 + index,
    turnId: `turn-in-${index}`,
    state,
    identity: OLD_IDENTITY,
    createdRevision: 1 + index,
    stateRevision: 1 + index,
    createdAtMs: 100 + index,
    updatedAtMs: 200 + index,
    payloadRef: { payloadId: `prompt-${index}`, byteLength: 10 + index },
    spoolRefs: [{ spoolId: `in-spool-${index}`, byteLength: 20 + index }],
  }));
  const outbound = RECOVERY_OUTBOUND_STATES.map((state, index) => ({
    family: "outbound" as const,
    recordId: `outbound-${index}`,
    intentId: `intent-${index}`,
    turnId: `turn-out-${index}`,
    state,
    identity: OLD_IDENTITY,
    createdRevision: 10 + index,
    stateRevision: 10 + index,
    createdAtMs: 300 + index,
    updatedAtMs: 400 + index,
    payloadRef: { payloadId: `answer-${index}`, byteLength: 30 + index },
    spoolRefs: [{ spoolId: `out-spool-${index}`, byteLength: 40 + index }],
  }));
  const bus = RECOVERY_BUS_STATES.map((state, index) => ({
    family: "bus" as const,
    recordId: `bus-${index}`,
    requestId: `request-${index}`,
    payloadFingerprint: `sha256-${index}`,
    state,
    identity: OLD_IDENTITY,
    createdRevision: 20 + index,
    stateRevision: 20 + index,
    createdAtMs: 500 + index,
    updatedAtMs: 600 + index,
    payloadRef: { payloadId: `bus-payload-${index}`, byteLength: 50 + index },
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
  return {
    version: RECOVERY_SCHEMA_VERSION,
    profile: "default",
    mode,
    revision: 100,
    writtenAtMs: 1000,
    quota: {
      recordBytes: 1000,
      payloadBytes: 528,
      spoolBytes: 462,
      reservedBytes: 10,
      totalBytes: 2000,
    },
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
    quota.totalBytes = RECOVERY_PROFILE_QUOTA_BYTES + 1000;
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
    quota.payloadBytes = 527;
    quota.totalBytes = 1999;
  }, /referenced payload bytes/);
});

const EXPECTED_FAULT_IDS = [
  "IN-01", "IN-02", "IN-03", "IN-04", "IN-05", "IN-06", "IN-07", "IN-08",
  "OUT-01", "OUT-02", "OUT-03", "OUT-04", "OUT-05", "OUT-06",
  "BUS-01", "BUS-02", "BUS-03", "BUS-04", "PAIR-01", "PAIR-02", "DOWN-01", "DOWN-02",
] satisfies ReliabilityFaultId[];

test("fault inventory is exact and controller injects once with seed", () => {
  assert.deepEqual(RELIABILITY_FAULT_IDS, EXPECTED_FAULT_IDS);
  assert.equal(new Set(RELIABILITY_FAULT_IDS).size, 22);
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
