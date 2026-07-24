/**
 * Local durable inbound wiring regressions
 * Zones: recovery, polling, queue lifecycle, fault injection
 * Covers C2a classic/leader admission, offset, replay, and Pi dispatch fencing.
 */

import assert from "node:assert/strict";
import { fork, type ChildProcess } from "node:child_process";
import {
  chmodSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  createTelegramBusAuthSecret,
  createTelegramBusFollowerRegistry,
  createTelegramBusForeignOwnedUpdateForwarder,
  createTelegramBusLocalServer,
  getTelegramBusFollowerSocketPath,
  getTelegramBusSocketPath,
  sendTelegramBusLocalEnvelope,
} from "../lib/bus.ts";
import { createTelegramBusLeaderEnvelopeHandler } from "../lib/bus-leader.ts";
import {
  createInboundRecoveryRuntime,
  validateRecoveryReassignmentBindingAuthority,
  type TelegramInboundRecoveryUpdate,
} from "../lib/inbound-recovery.ts";
import { createTelegramMediaGroupController } from "../lib/media.ts";
import { commitTelegramDurableOutbound } from "../lib/outbound-recovery.ts";
import { runTelegramPollLoop } from "../lib/polling.ts";
import {
  openRecoveryStore,
  parseRecoverySnapshot,
  RECOVERY_STORE_QUARANTINE_PREFIX,
  RecoveryProfileOperationGate,
  resolveRecoveryStorePath,
  type RecoveryIdentity,
  type RecoveryInboundFaultId,
  type RecoveryReassignmentBindingValidation,
  type RecoveryReassignmentRecord,
} from "../lib/recovery.ts";
import type { PendingTelegramTurn } from "../lib/queue.ts";
import type { TelegramInboundHandlingOutcome } from "../lib/updates.ts";

interface TestContext {
  generation: number;
}

function withTempAgentDir(run: (agentDir: string) => Promise<void> | void) {
  return async () => {
    const agentDir = mkdtempSync(join(tmpdir(), "pi-telegram-c2a-"));
    try {
      await run(agentDir);
    } finally {
      rmSync(agentDir, { force: true, recursive: true });
    }
  };
}

test("production reassignment authority requires exact live owner and generations", () => {
  const leaderIdentity: RecoveryIdentity = {
    profile: "default",
    target: { chatId: 7, threadId: 70 },
    owner: {
      kind: "leader",
      ownerId: "leader-a",
      leaderEpoch: "epoch-a",
    },
    sessionGeneration: 4,
  };
  const reassignment: RecoveryReassignmentRecord = {
    family: "reassignment",
    reassignmentId: "reassignment-a",
    profile: "default",
    target: leaderIdentity.target,
    oldOwner: {
      kind: "manual-follower",
      ownerId: "old-owner",
      registrationGeneration: "old-generation",
    },
    newOwner: leaderIdentity.owner,
    newSessionGeneration: 4,
    captureThroughRevision: 1,
    unresolvedRecordIds: ["private-record-id"],
    state: "binding-transfer-pending",
    createdAtMs: 1,
    updatedAtMs: 1,
  };
  const validation: RecoveryReassignmentBindingValidation = {
    reassignment,
    currentIdentity: leaderIdentity,
    expectedBinding: "new-owner",
  };
  const threadRecords = [
    {
      target: leaderIdentity.target,
      status: "active",
      owner: {
        kind: "leader",
        instanceId: "leader-a",
        telegramProfile: "default",
      },
    },
  ];
  const base = {
    validation,
    currentSessionGeneration: 4,
    identityAuthenticated: true,
    threadRecords,
    liveFollowers: [],
  };

  assert.equal(validateRecoveryReassignmentBindingAuthority(base), true);
  assert.equal(
    validateRecoveryReassignmentBindingAuthority({
      ...base,
      currentSessionGeneration: 5,
    }),
    false,
  );
  assert.equal(
    validateRecoveryReassignmentBindingAuthority({
      ...base,
      validation: {
        ...validation,
        currentIdentity: {
          ...leaderIdentity,
          owner: {
            kind: "leader",
            ownerId: "leader-a",
            leaderEpoch: "wrong-epoch",
          },
        },
      },
    }),
    false,
  );
  assert.equal(
    validateRecoveryReassignmentBindingAuthority({
      ...base,
      threadRecords: [...threadRecords, ...threadRecords],
    }),
    false,
  );
  assert.equal(
    validateRecoveryReassignmentBindingAuthority({
      ...base,
      liveFollowers: [
        {
          target: leaderIdentity.target,
          manualFollowerOwnerId: "conflicting-owner",
          registrationGeneration: "conflicting-generation",
        },
      ],
    }),
    false,
  );

  const followerIdentity: RecoveryIdentity = {
    ...leaderIdentity,
    owner: {
      kind: "manual-follower",
      ownerId: "follower-owner",
      registrationGeneration: "generation-a",
    },
  };
  const followerValidation: RecoveryReassignmentBindingValidation = {
    ...validation,
    reassignment: {
      ...reassignment,
      newOwner: followerIdentity.owner,
    },
    currentIdentity: followerIdentity,
  };
  const followerBase = {
    validation: followerValidation,
    currentSessionGeneration: 4,
    identityAuthenticated: true,
    threadRecords: [
      {
        target: followerIdentity.target,
        status: "active",
        owner: {
          kind: "manual-follower",
          instanceId: "follower-owner",
          telegramProfile: "default",
        },
      },
    ],
    liveFollowers: [
      {
        target: followerIdentity.target,
        manualFollowerOwnerId: "follower-owner",
        registrationGeneration: "generation-a",
      },
    ],
  };
  assert.equal(
    validateRecoveryReassignmentBindingAuthority(followerBase),
    true,
  );
  assert.equal(
    validateRecoveryReassignmentBindingAuthority({
      ...followerBase,
      liveFollowers: [
        {
          ...followerBase.liveFollowers[0],
          registrationGeneration: "wrong-generation",
        },
      ],
    }),
    false,
  );
  assert.equal(
    validateRecoveryReassignmentBindingAuthority({
      ...followerBase,
      liveFollowers: [],
    }),
    false,
  );
});

function createHarness(
  agentDir: string,
  options: {
    allowedUserId?: number;
    events?: Array<{ category: string; phase?: unknown }>;
  } = { allowedUserId: 7 },
) {
  const ctx: TestContext = { generation: 1 };
  let generation = 1;
  let owns = true;
  const runtime = createInboundRecoveryRuntime<
    TelegramInboundRecoveryUpdate,
    TestContext
  >({
    agentDir,
    getProfile: () => "default",
    getAllowedUserId: () => options.allowedUserId,
    getCurrentInstanceId: () => "instance-a",
    getSessionGeneration: () => generation,
    isSessionActive: (candidate, expected) =>
      candidate === ctx && expected === generation,
    resolveCurrentIdentity(target) {
      if (!owns) return undefined;
      return {
        profile: "default",
        target,
        owner: {
          kind: "leader",
          ownerId: "instance-a",
          leaderEpoch: "epoch-a",
        },
        sessionGeneration: generation,
      };
    },
    resolveOperatorIdentity() {
      if (!owns) return undefined;
      return {
        profile: "default",
        target: { chatId: 7 },
        owner: {
          kind: "leader",
          ownerId: "instance-a",
          leaderEpoch: "epoch-a",
        },
        sessionGeneration: generation,
      };
    },
    isIdentityAuthenticated(candidate) {
      return (
        owns &&
        candidate.sessionGeneration === generation &&
        candidate.owner.kind === "leader" &&
        candidate.owner.ownerId === "instance-a" &&
        candidate.owner.leaderEpoch === "epoch-a"
      );
    },
    recordRuntimeEvent(category, _error, details) {
      options.events?.push({ category, phase: details?.phase });
    },
  });
  const identity: RecoveryIdentity = {
    profile: "default",
    target: { chatId: 7 },
    owner: {
      kind: "leader",
      ownerId: "instance-a",
      leaderEpoch: "epoch-a",
    },
    sessionGeneration: 1,
  };
  const inspect = () =>
    openRecoveryStore({
      profile: "default",
      rootPath: resolveRecoveryStorePath("default", agentDir),
      isIdentityAuthenticated: (candidate) =>
        owns &&
        candidate.sessionGeneration === generation &&
        candidate.owner.kind === "leader" &&
        candidate.owner.ownerId === "instance-a" &&
        candidate.owner.leaderEpoch === "epoch-a",
    });
  return {
    ctx,
    runtime,
    identity,
    inspect,
    setGeneration(value: number) {
      generation = value;
      ctx.generation = value;
    },
    loseOwnership() {
      owns = false;
    },
  };
}

test(
  "runtime compacts on gated open, repeats at bounded cadence, and never reschedules after quarantine",
  withTempAgentDir(async (agentDir) => {
    const rootPath = resolveRecoveryStorePath("default", agentDir);
    const identity: RecoveryIdentity = {
      profile: "default",
      target: { chatId: 7 },
      owner: {
        kind: "leader",
        ownerId: "instance-a",
        leaderEpoch: "epoch-a",
      },
      sessionGeneration: 1,
    };
    const seed = openRecoveryStore({
      profile: "default",
      rootPath,
      now: () => 0,
      isIdentityAuthenticated: () => true,
    });
    const observed = seed.observeInbound(1, identity);
    seed.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("expired-completed-payload"),
    });
    seed.markPreDispatch(observed.recordId, { identity });
    seed.markDispatching(observed.recordId, { identity });
    seed.markCompleted(observed.recordId, { identity });
    assert.ok(
      parseRecoverySnapshot(readFileSync(seed.snapshotPath, "utf8")).inbound[0]
        ?.payloadRef,
    );

    const gate = new RecoveryProfileOperationGate();
    let scheduledCallback: (() => void) | undefined;
    let scheduleCalls = 0;
    let clearCalls = 0;
    const runtime = createInboundRecoveryRuntime<
      TelegramInboundRecoveryUpdate,
      TestContext
    >({
      agentDir,
      operationGate: gate,
      getProfile: () => "default",
      getAllowedUserId: () => 7,
      getSessionGeneration: () => 1,
      isSessionActive: () => true,
      resolveCurrentIdentity: () => identity,
      isIdentityAuthenticated: () => true,
      compactionIntervalMs: 10,
      setCompactionTimer(callback, delayMs) {
        assert.equal(delayMs, 10);
        scheduledCallback = callback;
        scheduleCalls += 1;
        const timer = setTimeout(() => {}, 2_147_483_647);
        timer.unref();
        return timer;
      },
      clearCompactionTimer(timer) {
        clearCalls += 1;
        clearTimeout(timer);
      },
    });

    await runtime.initializeOffset({});
    const opened = parseRecoverySnapshot(readFileSync(seed.snapshotPath, "utf8"));
    assert.equal(opened.inbound[0]?.payloadRef, undefined);
    assert.equal(scheduleCalls, 1);
    const revisionBeforePeriodic = opened.revision;
    const periodic = scheduledCallback;
    assert.ok(periodic);
    periodic();
    assert.ok(
      parseRecoverySnapshot(readFileSync(seed.snapshotPath, "utf8")).revision >
        revisionBeforePeriodic,
    );
    assert.equal(scheduleCalls, 2);

    assert.deepEqual(runtime.downgradePreflight(), {
      safe: true,
      blockerCount: 0,
      blockers: [],
    });
    const fenceGeneration = gate.beginFencing("default", "fence-timer");
    await gate.awaitDrained("default", fenceGeneration);
    gate.beginDowngradeExclusive("default", fenceGeneration);
    runtime.beginDowngradeExclusive();
    const staleCallback = scheduledCallback;
    runtime.quarantineForDowngrade();
    gate.markQuarantined("default", fenceGeneration);
    const schedulesAtQuarantine = scheduleCalls;
    assert.ok(clearCalls >= 1);
    staleCallback?.();
    assert.equal(scheduleCalls, schedulesAtQuarantine);
    assert.equal(gate.getState("default").phase, "quarantined");
  }),
);

function update(updateId: number): TelegramInboundRecoveryUpdate {
  return {
    update_id: updateId,
    message: {
      message_id: updateId * 10,
      chat: { id: 7 },
      from: { id: 7, is_bot: false },
    },
  };
}

function turnFor(
  updateValue: TelegramInboundRecoveryUpdate,
): PendingTelegramTurn {
  const message = updateValue.message!;
  return {
    kind: "prompt",
    chatId: 7,
    target: { chatId: 7 },
    replyToMessageId: message.message_id!,
    sourceMessageIds: [message.message_id!],
    queueOrder: 1,
    queueLane: "default",
    laneOrder: 1,
    queuedAttachments: [],
    content: [{ type: "text", text: "[telegram] hello" }],
    historyText: "hello",
    statusSummary: "hello",
  };
}

test(
  "IN-01/02/03 local admission is durable before offset authority advances",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const value = update(10);
    await harness.runtime.admitUpdate(value, harness.ctx);

    const store = harness.inspect();
    assert.equal(store.getCommittedUpdateId(), null);
    const records = store.listReplayableInboundRecords();
    assert.equal(records.length, 1);
    assert.equal(records[0]?.state, "admitted");
    assert.equal(records[0]?.updateId, 10);
  }),
);

test(
  "IN-04/05 offset-before-materialization rehydrates one stable queue turn",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const value = update(11);
    await harness.runtime.admitUpdate(value, harness.ctx);
    await harness.runtime.commitUpdate(value.update_id);

    const queue: PendingTelegramTurn[] = [];
    let handlerCalls = 0;
    await harness.runtime.rehydrate(
      harness.ctx,
      async (replayed) => {
        handlerCalls += 1;
        const materialized = harness.runtime.decorateTurn(
          [replayed.message!],
          turnFor(replayed),
        );
        queue.push(materialized);
        const outcome: TelegramInboundHandlingOutcome = {
          kind: "prompt-materialized",
          turnId: materialized.recovery!.turnId,
          recordIds: materialized.recovery!.recordIds,
        };
        return outcome;
      },
      (turn) => queue.push(turn),
    );

    assert.equal(handlerCalls, 1);
    assert.equal(queue.length, 1);
    assert.equal(queue[0]?.recovery?.recordIds.length, 1);
    assert.equal(harness.inspect().getCommittedUpdateId(), 11);
    assert.equal(
      harness.inspect().listReplayableInboundRecords()[0]?.state,
      "pre-dispatch",
    );
  }),
);

test(
  "IN-06/07 dispatching-before-Pi becomes uncertain on reopen and never auto invokes Pi",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const value = update(12);
    await harness.runtime.admitUpdate(value, harness.ctx);
    await harness.runtime.commitUpdate(value.update_id);
    const durableTurn = harness.runtime.decorateTurn(
      [value.message!],
      turnFor(value),
    );
    assert.equal(harness.runtime.claimTurnDispatch(durableTurn), true);
    let piCalls = 0;

    const reopened = harness.inspect();
    const status = reopened.getStatus();
    assert.equal(status.counts["execution-uncertain"], 1);
    await harness.runtime.rehydrate(
      harness.ctx,
      async () => {
        piCalls += 1;
      },
      () => {
        piCalls += 1;
      },
    );
    assert.equal(piCalls, 0);
  }),
);

test(
  "IN-08 stale owner cannot complete a dispatching turn",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const value = update(13);
    await harness.runtime.admitUpdate(value, harness.ctx);
    const durableTurn = harness.runtime.decorateTurn(
      [value.message!],
      turnFor(value),
    );
    assert.equal(harness.runtime.claimTurnDispatch(durableTurn), true);
    harness.loseOwnership();
    assert.throws(
      () => harness.runtime.completeTurn(durableTurn),
      /not currently authenticated/,
    );
    const record = harness.inspect().getStatus().items[0];
    assert.ok(record);
    assert.notEqual(record.state, "completed");
  }),
);

test(
  "follower durable admission survives lost ACK and isolates exact same-CWD targets",
  withTempAgentDir(async (agentDir) => {
    const followerCtx: TestContext = { generation: 1 };
    const target = { chatId: 7, threadId: 8 };
    const followerIdentity: RecoveryIdentity = {
      profile: "default",
      target,
      owner: {
        kind: "manual-follower",
        ownerId: "manual-a",
        registrationGeneration: "registration-a",
      },
      sessionGeneration: 1,
    };
    const follower = createInboundRecoveryRuntime<
      TelegramInboundRecoveryUpdate,
      TestContext
    >({
      agentDir,
      getProfile: () => "default",
      getAllowedUserId: () => 7,
      getCurrentInstanceId: () => "follower-a",
      getSessionGeneration: () => 1,
      isSessionActive: (candidate) => candidate === followerCtx,
      resolveCurrentIdentity(candidateTarget) {
        return candidateTarget.chatId === target.chatId &&
          candidateTarget.threadId === target.threadId
          ? followerIdentity
          : undefined;
      },
      isIdentityAuthenticated: (identity) =>
        identity.owner.kind === "manual-follower" &&
        identity.owner.ownerId === "manual-a" &&
        identity.owner.registrationGeneration === "registration-a" &&
        identity.target.threadId === 8,
    });
    const followerBIdentity: RecoveryIdentity = {
      ...followerIdentity,
      target: { chatId: 7, threadId: 9 },
      owner: {
        kind: "manual-follower",
        ownerId: "manual-b",
        registrationGeneration: "registration-b",
      },
    };
    const followerB = createInboundRecoveryRuntime<
      TelegramInboundRecoveryUpdate,
      TestContext
    >({
      agentDir,
      getProfile: () => "default",
      getAllowedUserId: () => 7,
      getCurrentInstanceId: () => "follower-b",
      getSessionGeneration: () => 1,
      isSessionActive: () => true,
      resolveCurrentIdentity(candidateTarget) {
        return candidateTarget.threadId === 9
          ? followerBIdentity
          : undefined;
      },
      isIdentityAuthenticated: (identity) =>
        identity.owner.kind === "manual-follower" &&
        identity.owner.ownerId === "manual-b" &&
        identity.target.threadId === 9,
    });
    const forwarded = update(88);
    forwarded.message!.message_thread_id = 8;
    await assert.rejects(() =>
      followerB.admitForwardedUpdate(forwarded, followerCtx, {
        profile: "default",
        target,
        ownerId: "manual-b",
        registrationGeneration: "registration-b",
        sessionGeneration: 1,
      }),
    );

    let dropFirstResponse = true;
    let lastProof:
      | Awaited<ReturnType<typeof follower.admitForwardedUpdate>>
      | undefined;
    let leaderIdentityResolutions = 0;
    const leader = createInboundRecoveryRuntime<
      TelegramInboundRecoveryUpdate,
      TestContext
    >({
      agentDir,
      getProfile: () => "default",
      getAllowedUserId: () => 7,
      getCurrentInstanceId: () => "leader",
      getTargetOwnership: () => ({
        instanceId: "follower-a",
        ownerGeneration: "registration-a",
      }),
      getSessionGeneration: () => 1,
      isSessionActive: () => true,
      resolveCurrentIdentity() {
        leaderIdentityResolutions += 1;
        return undefined;
      },
      isIdentityAuthenticated: () => false,
      async forwardUpdate(input) {
        lastProof = await follower.admitForwardedUpdate(
          input.update,
          followerCtx,
          {
            profile: input.profile,
            target: input.target,
            ownerId: "manual-a",
            registrationGeneration: input.ownership.ownerGeneration,
            sessionGeneration: 1,
          },
        );
        if (dropFirstResponse) {
          dropFirstResponse = false;
          return undefined;
        }
        return lastProof;
      },
      isFollowerAdmissionCurrent: (proof) =>
        proof.ownerId === "manual-a" &&
        proof.registrationGeneration === "registration-a" &&
        proof.target.threadId === 8,
    });

    await assert.rejects(
      () => leader.admitUpdate(forwarded, followerCtx),
      /absent or stale/,
    );
    const isFollowerAAuthenticated = (identity: RecoveryIdentity) =>
      identity.profile === "default" &&
      identity.target.chatId === 7 &&
      identity.target.threadId === 8 &&
      identity.sessionGeneration === 1 &&
      identity.owner.kind === "manual-follower" &&
      identity.owner.ownerId === "manual-a" &&
      identity.owner.registrationGeneration === "registration-a";
    const afterLoss = openRecoveryStore({
      profile: "default",
      rootPath: resolveRecoveryStorePath("default", agentDir),
      isIdentityAuthenticated: isFollowerAAuthenticated,
    }).listReplayableInboundRecords();
    assert.equal(afterLoss.length, 1);
    assert.equal(afterLoss[0]!.identity.owner.kind, "manual-follower");
    assert.equal(leaderIdentityResolutions, 0);

    assert.deepEqual(await leader.admitUpdate(forwarded, followerCtx), {
      kind: "follower-admitted",
    });
    assert.equal(lastProof?.recordId, afterLoss[0]!.recordId);
    assert.equal(
      lastProof?.admissionRevision,
      afterLoss[0]!.admissionRevision,
    );
    assert.equal(await leader.commitUpdate(88), 88);
    assert.equal(
      openRecoveryStore({
        profile: "default",
        rootPath: resolveRecoveryStorePath("default", agentDir),
        isIdentityAuthenticated: isFollowerAAuthenticated,
      }).getCommittedUpdateId(),
      88,
    );
  }),
);

test("mixed batch N / quota gap N+1 / N+2 stops at the contiguous durable prefix", async () => {
  const controller = new AbortController();
  const admitted: number[] = [];
  const committed: number[] = [];
  const handled: number[] = [];
  let calls = 0;
  await runTelegramPollLoop({
    ctx: "ctx",
    signal: controller.signal,
    config: { botToken: "123:abc", lastUpdateId: 9 },
    deleteWebhook: async () => {},
    getUpdates: async () => {
      calls += 1;
      if (calls === 1) {
        return [{ update_id: 10 }, { update_id: 11 }, { update_id: 12 }];
      }
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async () => {},
    handleUpdate: async (item) => {
      handled.push(item.update_id);
      return { kind: "completed", reason: "ignored" };
    },
    durableInbound: {
      initializeOffset: async () => 9,
      admitUpdate: async (item) => {
        admitted.push(item.update_id);
        if (item.update_id === 11) throw new Error("quota");
        return { kind: "admitted" };
      },
      commitUpdate: async (id) => {
        committed.push(id);
        return id;
      },
      handleAdmittedUpdate: async (item, ctx, handle) => {
        await handle(item, ctx);
      },
      markPoisonSkipped: async () => {},
    },
    onErrorStatus: () => {},
    onStatusReset: () => {},
    sleep: async () => {
      controller.abort();
    },
  });
  assert.deepEqual(admitted, [10, 11]);
  assert.deepEqual(committed, [10]);
  assert.deepEqual(handled, [10]);
});

test(
  "unauthorized updates retain metadata only and diagnostics stay redacted",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const hostile = update(14);
    hostile.message!.from = { id: 999, is_bot: false };
    await harness.runtime.admitUpdate(hostile, harness.ctx);
    const store = harness.inspect();
    const records = store.listReplayableInboundRecords();
    assert.deepEqual(records, []);
    const status = store.getStatus();
    assert.equal(status.counts["explicitly-discarded"], 1);
    const serialized = JSON.stringify(status);
    assert.equal(serialized.includes("999"), false);
    assert.equal(serialized.includes("message_id"), false);
  }),
);

test("post-prefix materialization retries in-session until it succeeds", async () => {
  const controller = new AbortController();
  let handlerCalls = 0;
  let commits = 0;
  let sleeps = 0;
  let polls = 0;
  await runTelegramPollLoop({
    ctx: "ctx",
    signal: controller.signal,
    config: { botToken: "123:abc", lastUpdateId: 0 },
    deleteWebhook: async () => {},
    getUpdates: async () => {
      polls += 1;
      if (polls === 1) return [{ update_id: 1 }];
      controller.abort();
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async () => {},
    handleUpdate: async () => {
      handlerCalls += 1;
      if (handlerCalls < 3) throw new Error("materialization pending");
      return { kind: "completed", reason: "ignored" };
    },
    durableInbound: {
      initializeOffset: async () => 0,
      admitUpdate: async () => ({ kind: "admitted" }),
      commitUpdate: async (id) => {
        commits += 1;
        return id;
      },
      handleAdmittedUpdate: async (item, ctx, handle) => {
        await handle(item, ctx);
      },
      markPoisonSkipped: async () => {},
    },
    onErrorStatus: () => {},
    onStatusReset: () => {},
    sleep: async () => {
      sleeps += 1;
    },
  });
  assert.equal(commits, 1);
  assert.equal(handlerCalls, 3);
  assert.equal(sleeps, 2);
});

test("offset mirror failures are diagnostic-only and never break a durable batch", async () => {
  const controller = new AbortController();
  const commits: number[] = [];
  const handled: number[] = [];
  const phases: unknown[] = [];
  let polls = 0;
  await runTelegramPollLoop({
    ctx: "ctx",
    signal: controller.signal,
    config: { botToken: "123:abc", lastUpdateId: 0 },
    deleteWebhook: async () => {},
    getUpdates: async () => {
      polls += 1;
      if (polls === 1) return [{ update_id: 1 }, { update_id: 2 }];
      controller.abort();
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async () => {
      throw new Error("mirror unavailable");
    },
    handleUpdate: async (item) => {
      handled.push(item.update_id);
      return { kind: "completed", reason: "ignored" };
    },
    durableInbound: {
      initializeOffset: async () => 0,
      admitUpdate: async () => ({ kind: "admitted" }),
      commitUpdate: async (id) => {
        commits.push(id);
        return id;
      },
      handleAdmittedUpdate: async (item, ctx, handle) => {
        await handle(item, ctx);
      },
      markPoisonSkipped: async () => {},
    },
    onErrorStatus: () => {},
    onStatusReset: () => {},
    sleep: async () => {},
    recordRuntimeEvent: (_category, _error, details) =>
      phases.push(details?.phase),
  });
  assert.deepEqual(commits, [1, 2]);
  assert.deepEqual(handled, [1, 2]);
  assert.deepEqual(phases, ["offset-mirror", "offset-mirror"]);
});

test("terminal unauthorized updates suppress handlers while exact pairing proof runs pre-commit", async () => {
  const controller = new AbortController();
  const handled: number[] = [];
  const committed: number[] = [];
  let polls = 0;
  await runTelegramPollLoop({
    ctx: "ctx",
    signal: controller.signal,
    config: { botToken: "123:abc", lastUpdateId: 0 },
    deleteWebhook: async () => {},
    getUpdates: async () => {
      polls += 1;
      if (polls === 1) return [{ update_id: 1 }, { update_id: 2 }];
      controller.abort();
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async () => {},
    handleUpdate: async (item) => {
      handled.push(item.update_id);
      return { kind: "completed", reason: "ignored" };
    },
    durableInbound: {
      initializeOffset: async () => 0,
      admitUpdate: async (item) => ({
        kind: item.update_id === 2 ? "pairing-proof" : "terminal",
      }),
      commitUpdate: async (id) => {
        committed.push(id);
        return id;
      },
      handleAdmittedUpdate: async (item, ctx, handle) => {
        await handle(item, ctx);
      },
      markPoisonSkipped: async () => {},
    },
    onErrorStatus: () => {},
    onStatusReset: () => {},
    sleep: async () => {},
  });
  assert.deepEqual(handled, [2]);
  assert.deepEqual(committed, [1, 2]);
});

test("abort after admission and during retry sleep prevents later attempts or prefix commit", async () => {
  const boundaries: readonly ("admission" | "retry-sleep")[] = [
    "admission",
    "retry-sleep",
  ];
  for (const boundary of boundaries) {
    const controller = new AbortController();
    let commits = 0;
    let handles = 0;
    await runTelegramPollLoop({
      ctx: "ctx",
      signal: controller.signal,
      config: { botToken: "123:abc", lastUpdateId: 0 },
      deleteWebhook: async () => {},
      getUpdates: async () => [{ update_id: 1 }],
      persistConfig: async () => {},
      handleUpdate: async () => {
        handles += 1;
        throw new Error("retry");
      },
      durableInbound: {
        initializeOffset: async () => 0,
        admitUpdate: async () => {
          if (boundary === "admission") controller.abort();
          return {
            kind: boundary === "admission" ? "admitted" : "pairing-proof",
          };
        },
        commitUpdate: async (id) => {
          commits += 1;
          return id;
        },
        handleAdmittedUpdate: async (item, ctx, handle) => {
          await handle(item, ctx);
        },
        markPoisonSkipped: async () => {},
      },
      onErrorStatus: () => {},
      onStatusReset: () => {},
      sleep: async () => {
        controller.abort();
      },
    });
    assert.equal(commits, 0, boundary);
    assert.equal(handles, boundary === "admission" ? 0 : 1, boundary);
  }
});

test(
  "exact pairing parser yields pairing-proof admission while other unpaired input is terminal",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir, {});
    const code = "01".repeat(16);
    const proof: TelegramInboundRecoveryUpdate = {
      update_id: 30,
      message: {
        message_id: 300,
        date: 1,
        chat: { id: 7, type: "private" },
        from: { id: 7, is_bot: false },
        text: `/start ${code}`,
        entities: [],
      },
    };
    const ordinary = update(31);
    assert.deepEqual(await harness.runtime.admitUpdate(proof, harness.ctx), {
      kind: "pairing-proof",
    });
    assert.deepEqual(await harness.runtime.admitUpdate(ordinary, harness.ctx), {
      kind: "terminal",
    });
    assert.equal(harness.inspect().listReplayableInboundRecords().length, 0);
  }),
);

test(
  "dispatch group claim is atomic when any record claim fails",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const store = harness.inspect();
    const records = [40, 41].map((id) => {
      const observed = store.observeInbound(id, harness.identity);
      store.admitInbound({
        recordId: observed.recordId,
        payload: Buffer.from(`raw-${id}`),
      });
      return store.materializeInbound({
        recordId: observed.recordId,
        claim: { identity: harness.identity },
        payload: Buffer.from(`turn-${id}`),
      });
    });
    assert.throws(() =>
      store.markDispatchingGroup([
        {
          recordId: records[0]!.recordId,
          claim: { identity: harness.identity },
        },
        {
          recordId: records[1]!.recordId,
          claim: {
            identity: { ...harness.identity, sessionGeneration: 2 },
          },
        },
      ]),
    );
    assert.deepEqual(
      store.listReplayableInboundRecords().map((record) => record.state),
      ["pre-dispatch", "pre-dispatch"],
    );
  }),
);

test(
  "genuine published handoff reclaims safe work; no handoff leaves restart work unclaimed",
  withTempAgentDir(async (agentDir) => {
    const claimed = createHarness(agentDir);
    const value = update(50);
    await claimed.runtime.admitUpdate(value, claimed.ctx);
    assert.equal(claimed.runtime.publishSessionHandoffs(3), 1);
    claimed.setGeneration(3);
    let replayed = 0;
    await claimed.runtime.rehydrate(
      claimed.ctx,
      async (item) => {
        replayed += 1;
        const materialized = claimed.runtime.decorateTurn(
          [item.message!],
          turnFor(item),
        );
        const outcome: TelegramInboundHandlingOutcome = {
          kind: "prompt-materialized",
          turnId: materialized.recovery!.turnId,
          recordIds: materialized.recovery!.recordIds,
        };
        return outcome;
      },
      () => {},
    );
    assert.equal(replayed, 1);

    const unclaimedDir = mkdtempSync(join(tmpdir(), "pi-telegram-no-handoff-"));
    try {
      const unclaimed = createHarness(unclaimedDir);
      await unclaimed.runtime.admitUpdate(update(51), unclaimed.ctx);
      unclaimed.setGeneration(3);
      let calls = 0;
      await unclaimed.runtime.rehydrate(
        unclaimed.ctx,
        async () => {
          calls += 1;
          const outcome: TelegramInboundHandlingOutcome = {
            kind: "completed",
            reason: "ignored",
          };
          return outcome;
        },
        () => {
          calls += 1;
        },
      );
      assert.equal(calls, 0);
      assert.equal(
        unclaimed.inspect().listReplayableInboundRecords()[0]?.state,
        "admitted",
      );
    } finally {
      rmSync(unclaimedDir, { force: true, recursive: true });
    }
  }),
);

async function seedPendingOutbound(
  runtime: ReturnType<typeof createHarness>["runtime"],
  identity: RecoveryIdentity,
  updateId: number,
) {
  const store = runtime.getOutboundStore();
  const inbound = store.observeInbound(updateId, identity);
  store.admitInbound({
    recordId: inbound.recordId,
    payload: Buffer.from(`outbound source ${updateId}`),
  });
  store.markPreDispatch(inbound.recordId, { identity });
  store.markDispatching(inbound.recordId, { identity });
  const item = await commitTelegramDurableOutbound({
    intentId: `intent-${updateId}`,
    turnId: inbound.turnId,
    sourceInboundRecordIds: [inbound.recordId],
    claim: { identity },
    replyToMessageId: 10,
    renderingMode: "rich",
    finalMarkdown: `final ${updateId}`,
    queuedAttachments: [],
  }, {
    store,
    transformReply: async (text) => text,
  });
  return {
    store,
    inbound,
    record: store.activateOutbound(item.record.recordId, { identity }),
  };
}

test(
  "outbound rehydrate claims exact leader and follower handoffs while full follower restart stays unclaimed",
  withTempAgentDir(async (agentDir) => {
    for (const owner of [
      {
        kind: "leader" as const,
        ownerId: "instance-a",
        leaderEpoch: "epoch-a",
      },
      {
        kind: "manual-follower" as const,
        ownerId: "manual-a",
        registrationGeneration: "registration-a",
      },
    ]) {
      const isolatedDir = mkdtempSync(join(agentDir, `${owner.kind}-`));
      let generation = 1;
      const ctx: TestContext = { generation };
      const currentIdentity = (): RecoveryIdentity => ({
        profile: "default",
        target: { chatId: 7, threadId: owner.kind === "leader" ? 70 : 71 },
        owner,
        sessionGeneration: generation,
      });
      const runtime = createInboundRecoveryRuntime<
        TelegramInboundRecoveryUpdate,
        TestContext
      >({
        agentDir: isolatedDir,
        getProfile: () => "default",
        getAllowedUserId: () => 7,
        getCurrentInstanceId: () => owner.ownerId,
        getSessionGeneration: () => generation,
        isSessionActive: (candidate, expected) =>
          candidate === ctx && expected === generation,
        resolveCurrentIdentity: (target) =>
          target.chatId === 7 &&
              target.threadId === currentIdentity().target.threadId
            ? currentIdentity()
            : undefined,
        resolveOperatorIdentity: () => currentIdentity(),
        isIdentityAuthenticated: (candidate) =>
          JSON.stringify(candidate) === JSON.stringify(currentIdentity()),
      });
      const seeded = await seedPendingOutbound(
        runtime,
        currentIdentity(),
        owner.kind === "leader" ? 201 : 202,
      );
      assert.equal(runtime.publishSessionHandoffs(2), 1);
      generation = 2;
      ctx.generation = 2;
      const scheduled: Array<{ state: string; identity: RecoveryIdentity }> = [];
      await runtime.rehydrateOutbound(ctx, (item, claim) => {
        scheduled.push({ state: item.record.state, identity: claim.identity });
      });
      assert.deepEqual(scheduled, [{
        state: "pending",
        identity: currentIdentity(),
      }]);
      assert.equal(
        seeded.store.listClaimableOutboundRecords({ identity: currentIdentity() })
          .length,
        1,
      );
    }

    const restartDir = mkdtempSync(join(agentDir, "follower-restart-"));
    const oldIdentity: RecoveryIdentity = {
      profile: "default",
      target: { chatId: 7, threadId: 72 },
      owner: {
        kind: "manual-follower",
        ownerId: "manual-restart",
        registrationGeneration: "registration-old",
      },
      sessionGeneration: 1,
    };
    const seedRuntime = createInboundRecoveryRuntime<
      TelegramInboundRecoveryUpdate,
      TestContext
    >({
      agentDir: restartDir,
      getProfile: () => "default",
      getAllowedUserId: () => 7,
      getSessionGeneration: () => 1,
      isSessionActive: () => true,
      resolveCurrentIdentity: () => oldIdentity,
      resolveOperatorIdentity: () => oldIdentity,
      isIdentityAuthenticated: (candidate) =>
        JSON.stringify(candidate) === JSON.stringify(oldIdentity),
    });
    const seeded = await seedPendingOutbound(seedRuntime, oldIdentity, 203);
    const restartedIdentity: RecoveryIdentity = {
      ...oldIdentity,
      owner: {
        kind: "manual-follower",
        ownerId: "manual-restart",
        registrationGeneration: "registration-new",
      },
    };
    const restartedCtx: TestContext = { generation: 1 };
    const restarted = createInboundRecoveryRuntime<
      TelegramInboundRecoveryUpdate,
      TestContext
    >({
      agentDir: restartDir,
      getProfile: () => "default",
      getAllowedUserId: () => 7,
      getSessionGeneration: () => 1,
      isSessionActive: (candidate) => candidate === restartedCtx,
      resolveCurrentIdentity: () => restartedIdentity,
      resolveOperatorIdentity: () => restartedIdentity,
      isIdentityAuthenticated: (candidate) =>
        JSON.stringify(candidate) === JSON.stringify(restartedIdentity),
    });
    let restartSchedules = 0;
    await restarted.rehydrateOutbound(restartedCtx, () => {
      restartSchedules += 1;
    });
    assert.equal(restartSchedules, 0);
    assert.equal(
      seeded.store.listClaimableOutboundRecords({ identity: oldIdentity }).length,
      1,
    );
  }),
);

test(
  "startup outbound rehydrate resumes pending, skips confirmed resend, and leaves reopened sending uncertain",
  withTempAgentDir(async (agentDir) => {
    const pendingHarness = createHarness(mkdtempSync(join(agentDir, "pending-")));
    const pending = await seedPendingOutbound(
      pendingHarness.runtime,
      pendingHarness.identity,
      211,
    );
    const startupEvents: string[] = [];
    await pendingHarness.runtime.rehydrateOutbound(
      pendingHarness.ctx,
      (item) => {
        startupEvents.push(`outbound:${item.record.state}`);
      },
    );
    await pendingHarness.runtime.rehydrate(
      pendingHarness.ctx,
      async () => {
        startupEvents.push("inbound");
        return { kind: "completed", reason: "ignored" };
      },
      () => startupEvents.push("queue"),
    );
    assert.deepEqual(startupEvents, ["outbound:pending"]);

    const future = await seedPendingOutbound(
      pendingHarness.runtime,
      pendingHarness.identity,
      213,
    );
    const futureClaim = future.store.claimOutboundUnit({
      recordId: future.record.recordId,
      claim: { identity: pendingHarness.identity },
    });
    future.store.recordOutboundSafeFailure({
      recordId: future.record.recordId,
      claim: { identity: pendingHarness.identity },
      attemptId: futureClaim.record.activeUnit!.attemptId,
    });
    const futureSchedules: string[] = [];
    await pendingHarness.runtime.rehydrateOutbound(pendingHarness.ctx, (item) => {
      if (item.record.recordId === future.record.recordId) {
        futureSchedules.push(item.record.state);
      }
    });
    assert.deepEqual(futureSchedules, ["retryable-pending"]);

    const confirmed = pending.store.claimOutboundUnit({
      recordId: pending.record.recordId,
      claim: { identity: pendingHarness.identity },
    });
    pending.store.recordOutboundReceipt({
      recordId: confirmed.record.recordId,
      claim: { identity: pendingHarness.identity },
      attemptId: confirmed.record.activeUnit!.attemptId,
      operationId: "outbound-unit-0000",
      method: "sendRichMessage",
      messageId: 700,
    });
    let confirmedSchedules = 0;
    await pendingHarness.runtime.rehydrateOutbound(pendingHarness.ctx, (item) => {
      if (item.record.recordId === confirmed.record.recordId) {
        confirmedSchedules += 1;
      }
    });
    assert.equal(confirmedSchedules, 0);

    const sendingAgentDir = mkdtempSync(join(agentDir, "sending-"));
    const sendingHarness = createHarness(sendingAgentDir);
    const sending = await seedPendingOutbound(
      sendingHarness.runtime,
      sendingHarness.identity,
      212,
    );
    sending.store.claimOutboundUnit({
      recordId: sending.record.recordId,
      claim: { identity: sendingHarness.identity },
    });
    const reopenedRuntime = createInboundRecoveryRuntime<
      TelegramInboundRecoveryUpdate,
      TestContext
    >({
      agentDir: sendingAgentDir,
      getProfile: () => "default",
      getAllowedUserId: () => 7,
      getSessionGeneration: () => 1,
      isSessionActive: (candidate) => candidate === sendingHarness.ctx,
      resolveCurrentIdentity: () => sendingHarness.identity,
      resolveOperatorIdentity: () => sendingHarness.identity,
      isIdentityAuthenticated: (candidate) =>
        JSON.stringify(candidate) === JSON.stringify(sendingHarness.identity),
    });
    let sendingSchedules = 0;
    await reopenedRuntime.rehydrateOutbound(sendingHarness.ctx, () => {
      sendingSchedules += 1;
    });
    assert.equal(sendingSchedules, 0);
    const status = reopenedRuntime.getOutboundStore().getStatus();
    assert.equal(
      status.items.filter(
        (item) =>
          item.family === "outbound" && item.state === "delivery-uncertain",
      ).length,
      1,
    );
    assert.equal(status.counts.completed, 1);
  }),
);

test(
  "operator outbound drain, uncertain retry, discard, and stale handles stay exact",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const pending = await seedPendingOutbound(
      harness.runtime,
      harness.identity,
      213,
    );
    const retryable = await seedPendingOutbound(
      harness.runtime,
      harness.identity,
      214,
    );
    const retryableClaim = retryable.store.claimOutboundUnit({
      recordId: retryable.record.recordId,
      claim: { identity: harness.identity },
    });
    const retryableRecord = retryable.store.recordOutboundSafeFailure({
      recordId: retryable.record.recordId,
      claim: { identity: harness.identity },
      attemptId: retryableClaim.record.activeUnit!.attemptId,
    });
    const sending = await seedPendingOutbound(
      harness.runtime,
      harness.identity,
      215,
    );
    sending.store.claimOutboundUnit({
      recordId: sending.record.recordId,
      claim: { identity: harness.identity },
    });
    const uncertain = await seedPendingOutbound(
      harness.runtime,
      harness.identity,
      216,
    );
    const uncertainClaim = uncertain.store.claimOutboundUnit({
      recordId: uncertain.record.recordId,
      claim: { identity: harness.identity },
    });
    uncertain.store.markOutboundUncertain({
      recordId: uncertain.record.recordId,
      claim: { identity: harness.identity },
      attemptId: uncertainClaim.record.activeUnit!.attemptId,
      reason: "response-lost",
    });

    const scheduled: Array<{
      item: Parameters<
        Parameters<typeof harness.runtime.drainSafeForOperator>[3]
      >[0];
      claim: Parameters<
        Parameters<typeof harness.runtime.drainSafeForOperator>[3]
      >[1];
    }> = [];
    const count = await harness.runtime.drainSafeForOperator(
      harness.ctx,
      async () => ({ kind: "completed", reason: "ignored" }),
      () => false,
      (item, claim) => {
        scheduled.push({ item, claim });
      },
    );
    assert.equal(count, 2);
    assert.deepEqual(
      scheduled.map(({ item }) => item.record.recordId).sort(),
      [pending.record.recordId, retryable.record.recordId].sort(),
    );
    assert.equal(
      scheduled.find(({ item }) =>
        item.record.recordId === retryable.record.recordId
      )!.item.record.automaticAttemptCount,
      retryableRecord.automaticAttemptCount,
    );
    assert.equal(
      scheduled.some(({ item }) => item.record.recordId === sending.record.recordId),
      false,
    );
    assert.equal(
      scheduled.some(({ item }) => item.record.recordId === uncertain.record.recordId),
      false,
    );
    assert.deepEqual(scheduled[0]!.claim.identity, harness.identity);

    const uncertainHandle = harness.runtime.getRecoveryStatus().items.find(
      (item) =>
        item.family === "outbound" &&
        item.state === "delivery-uncertain" &&
        item.actionId,
    )!.actionId;
    let linkedRecordId: string | undefined;
    const retry = await harness.runtime.retryUncertainForOperator(
      uncertainHandle,
      harness.ctx,
      async () => ({ kind: "completed", reason: "ignored" }),
      () => false,
      (item, claim) => {
        linkedRecordId = item.record.recordId;
        assert.equal(item.record.state, "pending");
        assert.equal(item.record.linkedAttemptOf, uncertain.record.recordId);
        assert.deepEqual(claim.identity, harness.identity);
      },
    );
    assert.deepEqual(retry, {
      scheduled: true,
      duplicationWarning: true,
    });
    assert.ok(linkedRecordId);
    await assert.rejects(
      harness.runtime.retryUncertainForOperator(
        uncertainHandle,
        harness.ctx,
        async () => ({ kind: "completed", reason: "ignored" }),
        () => false,
        () => {},
      ),
      /Unknown recovery action id/,
    );

    const discard = await seedPendingOutbound(
      harness.runtime,
      harness.identity,
      217,
    );
    const discardClaim = discard.store.claimOutboundUnit({
      recordId: discard.record.recordId,
      claim: { identity: harness.identity },
    });
    discard.store.markOutboundUncertain({
      recordId: discard.record.recordId,
      claim: { identity: harness.identity },
      attemptId: discardClaim.record.activeUnit!.attemptId,
      reason: "commit-unknown",
    });
    const discardHandle = harness.runtime.getRecoveryStatus().items.find(
      (item) =>
        item.family === "outbound" &&
        item.state === "delivery-uncertain" &&
        item.actionId !== uncertainHandle,
    )!.actionId;
    const discardedTurns: string[] = [];
    harness.runtime.discardForOperator(
      discardHandle,
      harness.ctx,
      (turnId) => discardedTurns.push(turnId),
    );
    assert.deepEqual(discardedTurns, [discard.record.turnId]);
    assert.equal(
      harness.runtime.getRecoveryStatus().items.some(
        (item) => item.actionId === discardHandle,
      ),
      false,
    );
    assert.throws(
      () => harness.runtime.discardForOperator(
        "wrong_handle",
        harness.ctx,
        () => {},
      ),
      /Unknown recovery action id/,
    );
  }),
);

test(
  "strict corrupt envelope is terminalized without blocking a later valid record",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const store = harness.inspect();
    const bad = store.observeInbound(60, harness.identity);
    store.admitInbound({
      recordId: bad.recordId,
      payload: Buffer.from(
        '{"version":1,"kind":"update","update":{"update_id":60},"extra":true}',
      ),
    });
    const good = store.observeInbound(61, harness.identity);
    store.admitInbound({
      recordId: good.recordId,
      payload: Buffer.from(
        JSON.stringify({
          version: 1,
          kind: "update",
          update: update(61),
        }),
      ),
    });
    let handled = 0;
    await harness.runtime.rehydrate(
      harness.ctx,
      async () => {
        handled += 1;
        const outcome: TelegramInboundHandlingOutcome = {
          kind: "completed",
          reason: "ignored",
        };
        return outcome;
      },
      () => {},
    );
    assert.equal(handled, 1);
    assert.equal(store.getStatus().counts["explicitly-discarded"], 1);
  }),
);

test(
  "document/audio spool restores private files and rewrites prompt paths after handoff",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const originalDirectory = mkdtempSync(join(agentDir, "downloads-"));
    const documentPath = join(originalDirectory, "notes.txt");
    const audioPath = join(originalDirectory, "voice.ogg");
    writeFileSync(documentPath, "document-body");
    writeFileSync(audioPath, Buffer.from([1, 2, 3, 4]));
    chmodSync(documentPath, 0o600);
    chmodSync(audioPath, 0o600);
    const value = update(70);
    await harness.runtime.admitUpdate(value, harness.ctx);
    const turn = turnFor(value);
    turn.content[0] = {
      type: "text",
      text: `[telegram] files\n${documentPath}\n${audioPath}`,
    };
    turn.historyText = `${documentPath}\n${audioPath}`;
    turn.recoveryFiles = [
      { path: documentPath, fileName: "notes.txt", kind: "document" },
      { path: audioPath, fileName: "voice.ogg", kind: "audio" },
    ];
    const durable = harness.runtime.decorateTurn([value.message!], turn);
    assert.equal(harness.runtime.publishSessionHandoffs(3), 1);
    rmSync(originalDirectory, { force: true, recursive: true });
    harness.setGeneration(3);
    let restored: PendingTelegramTurn | undefined;
    await harness.runtime.rehydrate(
      harness.ctx,
      async () => {},
      (item) => {
        restored = item;
      },
    );
    assert.ok(restored);
    const restoredFiles = restored.recoveryFiles!;
    assert.equal(restoredFiles.length, 2);
    assert.equal(readFileSync(restoredFiles[0]!.path, "utf8"), "document-body");
    assert.deepEqual([...readFileSync(restoredFiles[1]!.path)], [1, 2, 3, 4]);
    if (process.platform !== "win32") {
      assert.equal(statSync(restoredFiles[0]!.path).mode & 0o777, 0o600);
    }
    const restoredContent = restored.content[0];
    assert.equal(
      restoredContent?.type === "text" &&
        restoredContent.text.includes(documentPath),
      false,
    );
    assert.equal(harness.runtime.claimTurnDispatch(restored), true);
    harness.runtime.completeTurn(restored);
    assert.equal(existsSync(restoredFiles[0]!.path), false);
    assert.equal(durable.recovery?.turnId, restored.recovery?.turnId);
  }),
);

test(
  "pre-dispatch materialization mismatch never overwrites existing durable bytes",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const store = harness.inspect();
    const observed = store.observeInbound(80, harness.identity);
    store.admitInbound({
      recordId: observed.recordId,
      payload: Buffer.from("raw"),
    });
    store.materializeInbound({
      recordId: observed.recordId,
      claim: { identity: harness.identity },
      payload: Buffer.from("turn-a"),
    });
    assert.throws(() =>
      store.materializeInbound({
        recordId: observed.recordId,
        claim: { identity: harness.identity },
        payload: Buffer.from("turn-b"),
        previousPayload: Buffer.from("raw"),
      }),
    );
    const drained = store.drainSafeInbound({ identity: harness.identity });
    assert.equal(Buffer.from(drained[0]!.payload).toString(), "turn-a");
  }),
);

test(
  "strict malformed outcomes remain admitted and cannot terminalize recovery work",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const value = update(90);
    await harness.runtime.admitUpdate(value, harness.ctx);
    for (const malformed of [
      { kind: "completed", reason: "invented" },
      { kind: "completed", reason: "ignored", extra: true },
      { kind: "deferred", reason: "session-replay", key: "" },
      { kind: "follower-admitted", admission: { version: 1 } },
    ]) {
      await assert.rejects(() =>
        harness.runtime.handleAdmittedUpdate(
          value,
          harness.ctx,
          async () => malformed,
        ),
      );
      assert.equal(
        harness.inspect().listReplayableInboundRecords()[0]?.state,
        "admitted",
      );
    }
  }),
);

test(
  "grouped turn rehydrates and restores once for its exact record set",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const values = [update(91), update(92)];
    for (const value of values) {
      await harness.runtime.admitUpdate(value, harness.ctx);
    }
    const turn = turnFor(values[0]!);
    turn.sourceMessageIds = values.map((value) => value.message!.message_id!);
    const durable = harness.runtime.decorateTurn(
      values.map((value) => value.message!),
      turn,
    );
    assert.equal(durable.recovery?.recordIds.length, 2);
    assert.equal(harness.runtime.publishSessionHandoffs(3), 1);
    harness.setGeneration(3);
    const restored: PendingTelegramTurn[] = [];
    await harness.runtime.rehydrate(
      harness.ctx,
      async () => {
        throw new Error("pre-dispatch group must not replay raw updates");
      },
      (item) => restored.push(item),
    );
    assert.equal(restored.length, 1);
    assert.deepEqual(
      restored[0]?.recovery?.recordIds,
      durable.recovery?.recordIds,
    );
  }),
);

test(
  "album deletion and stop-style group removal terminalize every removed member",
  withTempAgentDir(async (agentDir) => {
    const harness = createHarness(agentDir);
    const album = [update(93), update(94)];
    for (const value of album) await harness.runtime.admitUpdate(value, harness.ctx);
    const media = createTelegramMediaGroupController<{
      message_id: number;
      chat: { id: number };
      media_group_id?: string;
    }>({
      setTimer: () => {
        const timer = setTimeout(() => {}, 60_000);
        timer.unref();
        return timer;
      },
      clearTimer: clearTimeout,
    });
    for (const value of album) {
      media.queueMessage({
        message: {
          message_id: value.message!.message_id!,
          chat: { id: 7 },
          media_group_id: "album",
        },
        dispatchMessages: async () => ({ kind: "completed", reason: "ignored" }),
      });
    }
    const removedAlbumIds = media.removeMessages([
      album[0]!.message!.message_id!,
    ]);
    assert.deepEqual(removedAlbumIds.sort((a, b) => a - b), [930, 940]);
    harness.runtime.terminalizeDeletedMessageIds(removedAlbumIds);

    const textGroup = [update(95), update(96)];
    for (const value of textGroup) {
      await harness.runtime.admitUpdate(value, harness.ctx);
    }
    harness.runtime.decorateTurn(
      textGroup.map((value) => value.message!),
      {
        ...turnFor(textGroup[0]!),
        sourceMessageIds: textGroup.map((value) => value.message!.message_id!),
      },
    );
    harness.runtime.terminalizeDeletedMessageIds(
      textGroup.map((value) => value.message!.message_id!),
    );
    assert.equal(harness.inspect().getStatus().counts.completed, 4);
  }),
);

const PROCESS_FIXTURE_PATH = join(
  process.cwd(),
  "tests/fixtures/inbound-recovery-process.ts",
);

function isFixtureMessage(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function startProcessFixture(args: readonly string[]): ChildProcess {
  const child = fork(PROCESS_FIXTURE_PATH, [...args], {
    execArgv: ["--experimental-strip-types"],
    env: { ...process.env },
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let stderr = "";
  child.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  child.on("message", (message: unknown) => {
    if (
      isFixtureMessage(message) &&
      message.type === "fixture-error" &&
      typeof message.error === "string"
    ) {
      stderr += message.error;
    }
  });
  child.once("close", (code, signal) => {
    if (code && stderr) {
      process.stderr.write(
        `inbound recovery fixture exited ${code}/${signal ?? "none"}: ${stderr}\n`,
      );
    }
  });
  return child;
}

function waitForFixtureMessage(
  child: ChildProcess,
  predicate: (message: Record<string, unknown>) => boolean,
  timeoutMs = 5_000,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for inbound recovery fixture IPC"));
    }, timeoutMs);
    const onMessage = (message: unknown) => {
      if (!isFixtureMessage(message) || !predicate(message)) return;
      cleanup();
      resolve(message);
    };
    const onClose = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(
        new Error(
          `Inbound recovery fixture closed before IPC (${code}/${signal ?? "none"})`,
        ),
      );
    };
    const cleanup = () => {
      clearTimeout(timeout);
      child.off("message", onMessage);
      child.off("close", onClose);
    };
    child.on("message", onMessage);
    child.on("close", onClose);
  });
}

async function stopProcessFixture(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise<void>((resolve) => {
    child.once("close", () => resolve());
  });
  child.kill("SIGKILL");
  await closed;
}

async function requestFixture(
  child: ChildProcess,
  command: "status" | "rehydrate" | "reassign" | "gate-status",
): Promise<Record<string, unknown>> {
  const requestId = `${command}:${Date.now()}:${Math.random()}`;
  const response = waitForFixtureMessage(
    child,
    (message) =>
      message.type === "response" && message.requestId === requestId,
  );
  child.send({ requestId, command });
  const message = await response;
  if (typeof message.error === "string") throw new Error(message.error);
  return message;
}

function getFixtureCount(
  message: Record<string, unknown>,
  state: string,
  field = "counts",
): number {
  const counts = message[field];
  if (!isFixtureMessage(counts) || typeof counts[state] !== "number") return 0;
  return counts[state];
}

function createProcessLeaderRecoveryRuntime(
  rootPath: string,
  registry: ReturnType<typeof createTelegramBusFollowerRegistry>,
  forwardUpdate: ReturnType<
    typeof createTelegramBusForeignOwnedUpdateForwarder
  >["forwardUpdate"],
) {
  const ctx: TestContext = { generation: 1 };
  const identity: RecoveryIdentity = {
    profile: "default",
    target: { chatId: 7 },
    owner: {
      kind: "leader",
      ownerId: "process-leader",
      leaderEpoch: "process-leader-epoch",
    },
    sessionGeneration: 1,
  };
  let lastFollowerProof:
    | Awaited<ReturnType<typeof forwardUpdate>>
    | undefined;
  const runtime = createInboundRecoveryRuntime<
    TelegramInboundRecoveryUpdate,
    TestContext
  >({
    getProfile: () => "default",
    getAllowedUserId: () => 7,
    getCurrentInstanceId: () => "process-leader",
    getTargetOwnership: (target) => {
      const follower = registry.getByTarget(target);
      return follower?.registrationGeneration
        ? {
            instanceId: follower.instanceId,
            ownerGeneration: follower.registrationGeneration,
          }
        : undefined;
    },
    getSessionGeneration: () => 1,
    isSessionActive: (candidate, generation) =>
      candidate === ctx && generation === 1,
    resolveCurrentIdentity: (target) => ({ ...identity, target }),
    isIdentityAuthenticated: (candidate) =>
      candidate.owner.kind === "leader" &&
      candidate.owner.ownerId === "process-leader" &&
      candidate.owner.leaderEpoch === "process-leader-epoch" &&
      candidate.sessionGeneration === 1,
    async forwardUpdate(input) {
      lastFollowerProof = await forwardUpdate(input);
      return lastFollowerProof;
    },
    isFollowerAdmissionCurrent: (proof) => {
      const follower = registry.getByTarget(proof.target);
      return (
        follower?.registrationGeneration === proof.registrationGeneration &&
        follower.manualFollowerOwnerId === proof.ownerId
      );
    },
    openStore: (options) => openRecoveryStore({ ...options, rootPath }),
  });
  return {
    ctx,
    runtime,
    getLastFollowerProof: () => lastFollowerProof,
  };
}

function registerProcessFollower(
  registry: ReturnType<typeof createTelegramBusFollowerRegistry>,
  input: {
    instanceId: string;
    ownerId: string;
    socketPath: string;
    threadId: number;
    registrationGeneration: string;
  },
): void {
  registry.register({
    instanceId: input.instanceId,
    manualFollowerOwnerId: input.ownerId,
    busSocketPath: input.socketPath,
    target: { chatId: 7, threadId: input.threadId },
    registrationGeneration: input.registrationGeneration,
    connectedAtMs: Date.now(),
  });
}

function createThreadUpdate(
  updateId: number,
  threadId: number,
): TelegramInboundRecoveryUpdate {
  const value = update(updateId);
  value.message!.message_thread_id = threadId;
  return value;
}

test(
  "composed child polling crashes at IN boundaries and runtime rehydrate preserves prefix, queue, and Pi-call safety",
  withTempAgentDir(async (agentDir) => {
    const faults: readonly RecoveryInboundFaultId[] = [
      "IN-03",
      "IN-04",
      "IN-05",
      "IN-06",
    ];
    for (const faultId of faults) {
      const rootPath = join(agentDir, `composed-${faultId}`);
      const child = startProcessFixture(["classic-crash", rootPath, faultId]);
      await waitForFixtureMessage(
        child,
        (message) =>
          message.type === "ready" && message.faultId === faultId,
      );
      await waitForFixtureMessage(
        child,
        (message) =>
          message.type === "fault-boundary" && message.faultId === faultId,
      );
      await stopProcessFixture(child);

      const reopened = startProcessFixture(["rehydrate", rootPath]);
      const evidence = await waitForFixtureMessage(
        reopened,
        (message) => message.type === "rehydrated",
      );
      assert.equal(evidence.prefix, faultId === "IN-03" ? 0 : 1, faultId);
      assert.equal(evidence.queueCount, faultId === "IN-06" ? 0 : 1, faultId);
      assert.equal(evidence.piCalls, faultId === "IN-06" ? 0 : 1, faultId);
      assert.equal(
        getFixtureCount(evidence, "execution-uncertain", "beforeCounts"),
        faultId === "IN-06" ? 1 : 0,
        faultId,
      );
      await stopProcessFixture(reopened);
    }
  }),
);

test(
  "composed local IPC follower admission proves pre-commit silence, redelivery identity, target isolation, restart reassignment, and fence resume",
  withTempAgentDir(async (agentDir) => {
    const authSecret = createTelegramBusAuthSecret();
    const leaderSocketPath = getTelegramBusSocketPath(
      agentDir,
      process.platform,
      "p0c",
    );
    const registry = createTelegramBusFollowerRegistry();
    const leader = createTelegramBusLocalServer({
      socketPath: leaderSocketPath,
      handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
        followerRegistry: registry,
        authSecret,
        verifyFollowerDurableAdmission: ({ proof, follower }) =>
          follower.manualFollowerOwnerId === proof.ownerId &&
          follower.registrationGeneration === proof.registrationGeneration &&
          follower.target?.chatId === proof.target.chatId &&
          follower.target.threadId === proof.target.threadId,
      }),
    });
    const forwarder = createTelegramBusForeignOwnedUpdateForwarder({
      socketPath: leaderSocketPath,
      createRequestId: (() => {
        let sequence = 0;
        return () => `process-leader:${++sequence}`;
      })(),
      getAuthSecret: () => authSecret,
      timeoutMs: 750,
    });
    const children: ChildProcess[] = [];
    try {
      await leader.start();

      const precommitRoot = join(agentDir, "precommit-recovery");
      const precommitSocket = getTelegramBusFollowerSocketPath(
        "follower-precommit",
        agentDir,
        process.platform,
        "p0c",
      );
      const precommit = startProcessFixture([
        "follower",
        precommitRoot,
        precommitSocket,
        "follower-precommit",
        "6",
        "1",
        "generation-precommit",
        "manual-owner-precommit",
        authSecret,
        "IN-02",
      ]);
      children.push(precommit);
      await waitForFixtureMessage(precommit, (message) => message.type === "ready");
      registerProcessFollower(registry, {
        instanceId: "follower-precommit",
        ownerId: "manual-owner-precommit",
        socketPath: precommitSocket,
        threadId: 6,
        registrationGeneration: "generation-precommit",
      });
      const precommitLeader = createProcessLeaderRecoveryRuntime(
        precommitRoot,
        registry,
        forwarder.forwardUpdate,
      );
      const precommitAdmission = assert.rejects(
        precommitLeader.runtime.admitUpdate(
          createThreadUpdate(100, 6),
          precommitLeader.ctx,
        ),
        /absent or stale/u,
      );
      await waitForFixtureMessage(
        precommit,
        (message) =>
          message.type === "fault-boundary" && message.faultId === "IN-02",
      );
      await stopProcessFixture(precommit);
      await precommitAdmission;
      const precommitInspect = startProcessFixture([
        "rehydrate",
        precommitRoot,
      ]);
      children.push(precommitInspect);
      const precommitEvidence = await waitForFixtureMessage(
        precommitInspect,
        (message) => message.type === "rehydrated",
      );
      assert.equal(precommitEvidence.prefix, null);
      assert.equal(precommitEvidence.piCalls, 0);
      await stopProcessFixture(precommitInspect);
      registry.remove("follower-precommit");

      const sharedRoot = join(agentDir, "shared-follower-recovery");
      const followerASocket = getTelegramBusFollowerSocketPath(
        "follower-a",
        agentDir,
        process.platform,
        "p0c",
      );
      const followerBSocket = getTelegramBusFollowerSocketPath(
        "follower-b",
        agentDir,
        process.platform,
        "p0c",
      );
      const followerAFault = startProcessFixture([
        "follower",
        sharedRoot,
        followerASocket,
        "follower-a",
        "8",
        "1",
        "generation-a",
        "manual-owner-a",
        authSecret,
        "IN-03",
      ]);
      children.push(followerAFault);
      await waitForFixtureMessage(
        followerAFault,
        (message) => message.type === "ready",
      );
      registerProcessFollower(registry, {
        instanceId: "follower-a",
        ownerId: "manual-owner-a",
        socketPath: followerASocket,
        threadId: 8,
        registrationGeneration: "generation-a",
      });
      const processLeader = createProcessLeaderRecoveryRuntime(
        sharedRoot,
        registry,
        forwarder.forwardUpdate,
      );
      const lostAdmission = assert.rejects(
        processLeader.runtime.admitUpdate(
          createThreadUpdate(101, 8),
          processLeader.ctx,
        ),
        /absent or stale/u,
      );
      await waitForFixtureMessage(
        followerAFault,
        (message) =>
          message.type === "fault-boundary" && message.faultId === "IN-03",
      );
      await stopProcessFixture(followerAFault);
      await lostAdmission;

      const followerA = startProcessFixture([
        "follower",
        sharedRoot,
        followerASocket,
        "follower-a",
        "8",
        "1",
        "generation-a",
        "manual-owner-a",
        authSecret,
      ]);
      children.push(followerA);
      await waitForFixtureMessage(followerA, (message) => message.type === "ready");
      const beforeRedelivery = await requestFixture(followerA, "status");
      assert.equal(getFixtureCount(beforeRedelivery, "admitted"), 1);
      const beforeActionIds = beforeRedelivery.itemActionIds;
      assert.ok(Array.isArray(beforeActionIds));
      await processLeader.runtime.admitUpdate(
        createThreadUpdate(101, 8),
        processLeader.ctx,
      );
      const redeliveredProof = processLeader.getLastFollowerProof();
      assert.equal(redeliveredProof?.ownerId, "manual-owner-a");
      assert.equal(redeliveredProof?.registrationGeneration, "generation-a");
      await processLeader.runtime.commitUpdate(101);
      const afterRedelivery = await requestFixture(followerA, "status");
      assert.deepEqual(afterRedelivery.itemActionIds, beforeActionIds);
      assert.equal(getFixtureCount(afterRedelivery, "admitted"), 1);

      const followerB = startProcessFixture([
        "follower",
        sharedRoot,
        followerBSocket,
        "follower-b",
        "9",
        "1",
        "generation-b",
        "manual-owner-b",
        authSecret,
      ]);
      children.push(followerB);
      await waitForFixtureMessage(followerB, (message) => message.type === "ready");
      registerProcessFollower(registry, {
        instanceId: "follower-b",
        ownerId: "manual-owner-b",
        socketPath: followerBSocket,
        threadId: 9,
        registrationGeneration: "generation-b",
      });
      await processLeader.runtime.admitUpdate(
        createThreadUpdate(102, 8),
        processLeader.ctx,
      );
      const proofA = processLeader.getLastFollowerProof();
      await processLeader.runtime.commitUpdate(102);
      await processLeader.runtime.admitUpdate(
        createThreadUpdate(103, 9),
        processLeader.ctx,
      );
      const proofB = processLeader.getLastFollowerProof();
      await processLeader.runtime.commitUpdate(103);
      assert.deepEqual(
        [proofA?.ownerId, proofA?.target.threadId],
        ["manual-owner-a", 8],
      );
      assert.deepEqual(
        [proofB?.ownerId, proofB?.target.threadId],
        ["manual-owner-b", 9],
      );

      const beforeStale = await requestFixture(followerA, "status");
      const staleProof = await forwarder.forwardUpdate({
        update: createThreadUpdate(104, 8),
        profile: "default",
        target: { chatId: 7, threadId: 8 },
        ownership: {
          instanceId: "follower-a",
          ownerGeneration: "generation-stale",
        },
      });
      assert.equal(staleProof, undefined);
      const afterStale = await requestFixture(followerA, "status");
      assert.deepEqual(afterStale.itemActionIds, beforeStale.itemActionIds);
      assert.equal(getFixtureCount(afterStale, "admitted"), 3);

      await stopProcessFixture(followerA);
      const followerANew = startProcessFixture([
        "follower",
        sharedRoot,
        followerASocket,
        "follower-a",
        "8",
        "2",
        "generation-a-new",
        "manual-owner-a",
        authSecret,
      ]);
      children.push(followerANew);
      await waitForFixtureMessage(
        followerANew,
        (message) => message.type === "ready",
      );
      registerProcessFollower(registry, {
        instanceId: "follower-a",
        ownerId: "manual-owner-a",
        socketPath: followerASocket,
        threadId: 8,
        registrationGeneration: "generation-a-new",
      });
      const beforeReassignment = await requestFixture(followerANew, "rehydrate");
      assert.equal(beforeReassignment.queueCount, 0);
      assert.ok(
        typeof beforeReassignment.orphanCount === "number" &&
          beforeReassignment.orphanCount >= 1,
      );
      const afterReassignment = await requestFixture(followerANew, "reassign");
      assert.equal(afterReassignment.reassignmentState, "recovery-grant-committed");
      assert.equal(afterReassignment.queueCount, 2);

      const fenceGeneration = "process-fence-generation";
      const fenced = await sendTelegramBusLocalEnvelope({
        socketPath: followerASocket,
        timeoutMs: 1_000,
        envelope: {
          kind: "leader.fenceRecovery",
          requestId: "process-fence",
          auth: authSecret,
          profile: "default",
          recipientInstanceId: "follower-a",
          recipientRegistrationGeneration: "generation-a-new",
          fenceGeneration,
          sentAtMs: Date.now(),
        },
      });
      assert.equal(fenced?.kind === "bus.ack" && fenced.ok, true);
      const fencedState = await requestFixture(followerANew, "gate-status");
      assert.ok(isFixtureMessage(fencedState.gate));
      assert.equal(fencedState.gate.profile, "default");
      assert.equal(fencedState.gate.phase, "fencing");
      assert.equal(fencedState.gate.fenceGeneration, fenceGeneration);
      assert.equal(fencedState.gate.inFlight, 0);
      const resumed = await sendTelegramBusLocalEnvelope({
        socketPath: followerASocket,
        timeoutMs: 1_000,
        envelope: {
          kind: "leader.resumeRecovery",
          requestId: "process-resume",
          auth: authSecret,
          profile: "default",
          recipientInstanceId: "follower-a",
          recipientRegistrationGeneration: "generation-a-new",
          fenceGeneration,
          sentAtMs: Date.now(),
        },
      });
      assert.equal(resumed?.kind === "bus.ack" && resumed.ok, true);
      const resumedState = await requestFixture(followerANew, "gate-status");
      assert.ok(isFixtureMessage(resumedState.gate));
      assert.equal(resumedState.gate.profile, "default");
      assert.equal(resumedState.gate.phase, "active");
      assert.equal(resumedState.gate.inFlight, 0);
    } finally {
      await Promise.all(children.map((child) => stopProcessFixture(child)));
      await leader.stop();
    }
  }),
);

test(
  "downgrade child crashes before rename or after rename before report without reopening quarantined authority",
  withTempAgentDir(async (agentDir) => {
    for (const faultId of ["DOWN-01", "DOWN-02"] satisfies readonly RecoveryInboundFaultId[]) {
      const rootPath = join(agentDir, `downgrade-${faultId}`);
      const child = startProcessFixture([
        "downgrade-crash",
        rootPath,
        faultId,
      ]);
      await waitForFixtureMessage(child, (message) => message.type === "ready");
      await waitForFixtureMessage(
        child,
        (message) =>
          message.type === "fault-boundary" && message.faultId === faultId,
      );
      await stopProcessFixture(child);
      const quarantines = readdirSync(agentDir).filter((entry) =>
        entry.startsWith(RECOVERY_STORE_QUARANTINE_PREFIX),
      );
      assert.equal(quarantines.length, faultId === "DOWN-02" ? 1 : 0);
      assert.equal(existsSync(rootPath), faultId === "DOWN-01");
      const reopened = startProcessFixture(["rehydrate", rootPath]);
      if (faultId === "DOWN-02") {
        const failure = await waitForFixtureMessage(
          reopened,
          (message) => message.type === "fixture-error",
        );
        assert.match(String(failure.error), /Recovery store is quarantined/u);
      } else {
        const evidence = await waitForFixtureMessage(
          reopened,
          (message) => message.type === "rehydrated",
        );
        assert.equal(evidence.prefix, null);
        assert.equal(evidence.queueCount, 0);
        assert.equal(evidence.piCalls, 0);
      }
      await stopProcessFixture(reopened);
    }
  }),
);
