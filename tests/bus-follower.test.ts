/**
 * Regression tests for Telegram multi-instance bus follower helpers
 * Covers follower registration, forwarded update receiving, and follower-routed API calls
 */

import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  createTelegramBusFollowerApiCaller,
  createTelegramBusFollowerHeartbeatRecoveryHandler,
  createTelegramBusFollowerRegistrationRuntime,
  createTelegramBusFollowerPromotionHandler,
  createTelegramBusFollowerRegistrationState,
  createTelegramBusFollowerRuntimeAssembly,
  createTelegramBusFollowerSessionRefreshHook,
  createTelegramBusFollowerSessionReplacementSuspender,
  createTelegramBusFollowerTargetReplacementHandler,
  createTelegramBusForwardedRouteHandlers,
  createTelegramBusForwardedUpdateReceiverRuntime,
  createTelegramManualFollowerProfileKeyResolver,
  getTelegramFollowerSessionHandoff,
  setTelegramFollowerSessionHandoff,
} from "../lib/bus-follower.ts";
import {
  createTelegramBusFollowerRegistry,
  createTelegramBusFollowerTargetController,
  createTelegramBusForeignOwnedUpdateForwarder,
  createTelegramBusLocalServer,
  resolveTelegramBusSocketPath,
  sendTelegramBusLocalEnvelope,
} from "../lib/bus.ts";
import { getTelegramBusTransportKind } from "../lib/bus-transport.ts";
import { createTelegramBusLeaderEnvelopeHandler } from "../lib/bus-leader.ts";
import {
  createTelegramTopicTargetStore,
  getTelegramLeaderSessionHandoff,
  setTelegramLeaderSessionHandoff,
} from "../lib/threads.ts";
import { RecoveryProfileOperationGate } from "../lib/recovery.ts";
import { isTelegramApiCommitUnknownError } from "../lib/telegram-api.ts";
import {
  createTelegramInteractionRuntime,
  type TelegramInteractionActiveTurnSnapshot,
} from "../lib/interactions.ts";
import {
  createTelegramInteractionPriorityHandle,
  createTelegramUpdateHandle,
} from "../lib/updates.ts";

async function waitForCondition(
  predicate: () => boolean,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail("Timed out waiting for condition");
}

test("Bus follower route handlers adapt forwarded envelopes", async () => {
  const events: string[] = [];
  const handlers = createTelegramBusForwardedRouteHandlers<
    string,
    { emoji: string },
    { id: string },
    { text: string }
  >({
    handleUpdate(update, ctx) {
      events.push(
        `${ctx}:${update.callback_query?.id ?? update.message?.text ?? update.edited_message?.text}`,
      );
    },
    handleAuthorizedReactionUpdate(reaction, ctx) {
      events.push(`${ctx}:${reaction.emoji}`);
    },
  });

  await handlers.handleForwardedCallback({ id: "callback" }, "ctx");
  await handlers.handleForwardedReaction({ emoji: "👍" }, "ctx");
  await handlers.handleForwardedMessage?.({ text: "message" }, "ctx");
  await handlers.handleForwardedEditedMessage?.({ text: "edited" }, "ctx");

  assert.deepEqual(events, [
    "ctx:callback",
    "ctx:👍",
    "ctx:message",
    "ctx:edited",
  ]);
});

test("Bus follower profile key resolver follows the active profile", () => {
  let profileName: string | undefined;
  const resolveProfileKey = createTelegramManualFollowerProfileKeyResolver({
    getActiveProfileName: () => profileName,
    manualFollowerOwnerId: "7",
  });
  assert.equal(resolveProfileKey(), "manual:7");
  profileName = "work";
  assert.equal(resolveProfileKey(), "profile:work:manual:7");
});

test("Bus follower promotion handler transfers binding only after leadership acquisition", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-promotion-"));
  const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
  const events: unknown[] = [];
  const promote = createTelegramBusFollowerPromotionHandler({
    topicTargetStore: store,
    instanceId: "inst-a",
    getActiveProfileName: () => "work",
    startLeader: async (ctx: { cwd: string }, _election, onAcquired) => {
      events.push(`acquired:${ctx.cwd}`);
      await onAcquired();
      return true;
    },
    recordRuntimeEvent: (category, message, details) => {
      events.push({ category, message, details });
    },
    getPid: () => 10,
    getNowMs: () => 500,
  });
  try {
    await promote(
      { cwd: "/repo" },
      {
        target: { chatId: 42, threadId: 11 },
        slot: "E",
        threadName: "Ember",
      },
      {},
    );
    assert.equal(store.list()[0]?.profileKey, "profile:work:cwd:/repo");
    assert.equal(store.list()[0]?.owner?.kind, "leader");
    assert.equal(events[0], "acquired:/repo");
    assert.deepEqual(events[1], {
      category: "bus",
      message: "Follower thread binding promoted to leader",
      details: {
        phase: "follower-promoted-binding",
        chatId: 42,
        threadId: 11,
        slot: "E",
        threadName: "Ember",
      },
    });
    assert.deepEqual(events[2], {
      category: "bus",
      message: "Promoted leader binding retained for session replacement",
      details: {
        phase: "follower-promoted-session-handoff",
        chatId: 42,
        threadId: 11,
        slot: "E",
        threadName: "Ember",
      },
    });
    assert.deepEqual(getTelegramLeaderSessionHandoff(), {
      pid: 10,
      instanceId: "inst-a",
      createdAtMs: 500,
      profileKey: "profile:work:cwd:/repo",
      target: { chatId: 42, threadId: 11 },
      slot: "E",
      threadName: "Ember",
    });
  } finally {
    setTelegramLeaderSessionHandoff(undefined);
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower promotion leaves binding unchanged when election is lost", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-election-lost-"));
  const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
  const promote = createTelegramBusFollowerPromotionHandler({
    topicTargetStore: store,
    instanceId: "inst-a",
    getActiveProfileName: () => "work",
    startLeader: async () => false,
    recordRuntimeEvent: () => undefined,
  });
  try {
    assert.equal(
      await promote(
        { cwd: "/repo" },
        {
          target: { chatId: 42, threadId: 11 },
          slot: "E",
          threadName: "Ember",
        },
        { expectedOwner: { pid: 99 } },
      ),
      false,
    );
    assert.deepEqual(store.list(), []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower receiver handles leader-forwarded updates and target replacement", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-forward-"));
  const leaderSocketPath = join(dir, "leader.sock");
  const followerSocketPath = join(dir, "follower.sock");
  const registry = createTelegramBusFollowerRegistry();
  const received: unknown[] = [];
  let nowMs = 2000;
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath: followerSocketPath,
    instanceId: "inst-b",
    getContext() {
      return "ctx";
    },
    handleForwardedCallback(query, ctx) {
      received.push({ kind: "callback", query, ctx });
    },
    handleForwardedReaction(reactionUpdate, ctx) {
      received.push({ kind: "reaction", reactionUpdate, ctx });
    },
    handleForwardedMessage(message, ctx) {
      received.push({ kind: "message", message, ctx });
    },
    handleForwardedEditedMessage(message, ctx) {
      received.push({ kind: "edited-message", message, ctx });
    },
    handleReplaceTarget(input, ctx) {
      received.push({ kind: "replace-target", input, ctx });
    },
  });
  const leader = createTelegramBusLocalServer({
    socketPath: leaderSocketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      getNowMs: () => nowMs,
    }),
  });
  try {
    await receiver.start();
    await leader.start();
    registry.register({
      instanceId: "inst-b",
      busSocketPath: followerSocketPath,
      connectedAtMs: 1000,
    });
    const callbackResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardCallback",
        requestId: "leader:1",
        recipientInstanceId: "inst-b",
        query: { id: "cb-1", data: "queue:pause" },
        sentAtMs: 2000,
      },
    });
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 2000);
    nowMs = 3000;
    const reactionResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardReaction",
        requestId: "leader:2",
        recipientInstanceId: "inst-b",
        reactionUpdate: { message_id: 9, new_reaction: [] },
        sentAtMs: 3000,
      },
    });
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 3000);
    nowMs = 4000;
    const messageResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardMessage",
        requestId: "leader:3",
        recipientInstanceId: "inst-b",
        message: { message_id: 10, text: "hi" },
        sentAtMs: 4000,
      },
    });
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 4000);
    nowMs = 5000;
    const editedMessageResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardEditedMessage",
        requestId: "leader:4",
        recipientInstanceId: "inst-b",
        message: { message_id: 10, text: "edited" },
        sentAtMs: 5000,
      },
    });
    const targetController = createTelegramBusFollowerTargetController({
      socketPath: followerSocketPath,
      createRequestId: () => "leader:5",
      getNowMs: () => 6000,
    });
    const replaceTargetResponse = await targetController.replaceTarget({
      follower: registry.get("inst-b")!,
      target: { chatId: 7, threadId: 42 },
      oldTarget: { chatId: 7, threadId: 10 },
      reason: "thread-restore",
    });
    assert.deepEqual(callbackResponse, {
      kind: "bus.ack",
      requestId: "leader:1",
      ok: true,
      message: undefined,
    });
    assert.deepEqual(reactionResponse, {
      kind: "bus.ack",
      requestId: "leader:2",
      ok: true,
      message: undefined,
    });
    assert.deepEqual(messageResponse, {
      kind: "bus.ack",
      requestId: "leader:3",
      ok: true,
      message: undefined,
    });
    assert.deepEqual(editedMessageResponse, {
      kind: "bus.ack",
      requestId: "leader:4",
      ok: true,
      message: undefined,
    });
    assert.equal(replaceTargetResponse, true);
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 5000);
    assert.deepEqual(received, [
      {
        kind: "callback",
        query: { id: "cb-1", data: "queue:pause" },
        ctx: "ctx",
      },
      {
        kind: "reaction",
        reactionUpdate: { message_id: 9, new_reaction: [] },
        ctx: "ctx",
      },
      { kind: "message", message: { message_id: 10, text: "hi" }, ctx: "ctx" },
      {
        kind: "edited-message",
        message: { message_id: 10, text: "edited" },
        ctx: "ctx",
      },
      {
        kind: "replace-target",
        input: {
          target: { chatId: 7, threadId: 42 },
          oldTarget: { chatId: 7, threadId: 10 },
          reason: "thread-restore",
        },
        ctx: "ctx",
      },
    ]);
  } finally {
    await leader.stop();
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Interaction full-update routing settles only the exact follower Promise before public handlers", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-interaction-roundtrip-"));
  const leaderSocketPath = join(dir, "leader.sock");
  const followerASocketPath = join(dir, "follower-a.sock");
  const followerBSocketPath = join(dir, "follower-b.sock");
  const ownerId = "paired-owner";
  const targets = {
    a: { chatId: 7, threadId: 11 },
    b: { chatId: 7, threadId: 22 },
  };
  const generations = { a: "generation-a", b: "generation-b" };
  const identity = (target: { chatId: number; threadId: number }) => ({
    target,
    profile: "default",
    transportGeneration: "transport-1",
    sessionGeneration: "1",
    authorityGeneration: "follower-current",
  });
  const snapshot = (
    target: { chatId: number; threadId: number },
  ): TelegramInteractionActiveTurnSnapshot => ({
    turnId: `turn-${target.threadId}`,
    ...identity(target),
    sourceMessageIds: [],
  });
  const runtimeA = createTelegramInteractionRuntime({
    generation: "runtime-a",
    captureActiveTurn: () => snapshot(targets.a),
    isActive: () => true,
    createToken: () => "AAAAAAAAAAAAAAAA",
    delivery: {
      async sendView() {
        return {
          ok: true,
          value: { target: targets.a, messageIds: [101], generation: "delivery-a" },
        };
      },
      async editView(handle) { return { ok: true, value: handle }; },
      async deleteView() { return { ok: true, value: undefined }; },
    },
  });
  const runtimeB = createTelegramInteractionRuntime({
    generation: "runtime-b",
    captureActiveTurn: () => snapshot(targets.b),
    isActive: () => true,
    delivery: {
      async sendView() {
        return {
          ok: true,
          value: { target: targets.b, messageIds: [201], generation: "delivery-b" },
        };
      },
      async editView(handle) { return { ok: true, value: handle }; },
      async deleteView() { return { ok: true, value: undefined }; },
    },
  });
  const pendingA = runtimeA.request({
    question: "A",
    mode: { kind: "single-select", options: [{ label: "A", value: "a" }] },
  });
  const pendingB = runtimeB.request({ question: "B", mode: { kind: "text" } });
  await new Promise<void>((resolve) => setImmediate(resolve));

  let followerPublic = 0;
  let followerDefault = 0;
  let recursiveForwards = 0;
  const pairedGate = {
    getAllowedUserId: () => 7,
    async claim() { return { kind: "already-paired" as const }; },
    async sendGenericResponse() {},
    async onPaired() {},
    recordSideEffectFailure() {},
  };
  const makeFollowerHandler = (
    instanceId: string,
    target: { chatId: number; threadId: number },
    anchor: number,
    runtime: typeof runtimeA,
  ) => {
    const priority = createTelegramInteractionPriorityHandle({
      getCurrentInstanceId: () => instanceId,
      getCurrentProfile: () => "default",
      getMessageOwnership: (_chatId, messageId) =>
        messageId === anchor
          ? {
              instanceId,
              ownerGeneration:
                instanceId === "follower-a" ? generations.a : generations.b,
              target,
              purpose: "interaction" as const,
            }
          : undefined,
      classifyMessageOwnership: (_chatId, messageId) =>
        messageId === anchor ? { purpose: "interaction" as const } : undefined,
      isInteractionPending: runtime.isPending,
      foreignOwnedUpdateForwarder: {
        async forwardUpdate() {
          recursiveForwards += 1;
          return undefined;
        },
      },
      prepareCallback(input) {
        return runtime.prepareCallback({ ...input, ...identity(target) });
      },
      handleInput(input) {
        return runtime.handleInput({ ...input, ...identity(target) });
      },
      async answerCallbackQuery() {},
    });
    return createTelegramUpdateHandle({
      pairingGate: pairedGate,
      priorityHandle: priority,
      registry: {
        version: 1 as const,
        add: () => () => {},
        async dispatch() { followerPublic += 1; return "consume" as const; },
      },
      async defaultHandle() {
        followerDefault += 1;
        return { kind: "completed" as const, reason: "ignored" as const };
      },
    });
  };
  const followerHandlers = {
    a: makeFollowerHandler("follower-a", targets.a, 101, runtimeA),
    b: makeFollowerHandler("follower-b", targets.b, 201, runtimeB),
  };
  const proof = (
    updateId: number,
    target: { chatId: number; threadId: number },
    generation: string,
  ) => ({
    version: 1 as const,
    updateId,
    recordId: `record-${updateId}`,
    turnId: `turn-${updateId}`,
    profile: "default",
    target,
    ownerId,
    registrationGeneration: generation,
    sessionGeneration: 1,
    admissionRevision: 1,
    disposition: "terminal" as const,
  });
  const makeReceiver = (
    socketPath: string,
    instanceId: string,
    target: { chatId: number; threadId: number },
    generation: string,
    handleUpdate: ReturnType<typeof makeFollowerHandler>,
  ) => createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId,
    getRegistrationGeneration: () => generation,
    getProfile: () => "default",
    getTarget: () => target,
    getSessionGeneration: () => 1,
    manualFollowerOwnerId: ownerId,
    getContext: () => "ctx",
    async handleForwardedUpdate(input, ctx) {
      await handleUpdate(input.update, ctx);
      return proof(input.update.update_id, target, generation);
    },
    handleForwardedCallback() {},
    handleForwardedReaction() {},
  });
  const receiverA = makeReceiver(
    followerASocketPath,
    "follower-a",
    targets.a,
    generations.a,
    followerHandlers.a,
  );
  const receiverB = makeReceiver(
    followerBSocketPath,
    "follower-b",
    targets.b,
    generations.b,
    followerHandlers.b,
  );
  const registry = createTelegramBusFollowerRegistry();
  registry.register({
    instanceId: "follower-a",
    manualFollowerOwnerId: ownerId,
    registrationGeneration: generations.a,
    sessionGeneration: 1,
    connectedAtMs: 1,
    busSocketPath: followerASocketPath,
    target: targets.a,
  });
  registry.register({
    instanceId: "follower-b",
    manualFollowerOwnerId: ownerId,
    registrationGeneration: generations.b,
    sessionGeneration: 1,
    connectedAtMs: 1,
    busSocketPath: followerBSocketPath,
    target: targets.b,
  });
  const leader = createTelegramBusLocalServer({
    socketPath: leaderSocketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
    }),
  });
  const forwarder = createTelegramBusForeignOwnedUpdateForwarder({
    socketPath: leaderSocketPath,
    createRequestId: (() => {
      let sequence = 0;
      return () => `interaction:${++sequence}`;
    })(),
  });
  const leaderOwnership = new Map<number, {
    instanceId: string;
    ownerGeneration?: string;
    target: { chatId: number; threadId: number };
    purpose: "interaction";
  }>([
    [101, { instanceId: "follower-a", ownerGeneration: generations.a, target: targets.a, purpose: "interaction" }],
    [201, { instanceId: "follower-b", ownerGeneration: generations.b, target: targets.b, purpose: "interaction" }],
  ]);
  let leaderPublic = 0;
  let leaderDefault = 0;
  const callbackA = "interact:AAAAAAAAAAAAAAAA:pick:0";
  const callbackUpdate = (
    updateId: number,
    messageId: number,
    threadId: number | undefined,
    data = callbackA,
    userId = 7,
  ) => ({
    update_id: updateId,
    callback_query: {
      id: `callback-${updateId}`,
      data,
      from: { id: userId, is_bot: false },
      message: {
        message_id: messageId,
        ...(threadId === undefined ? {} : { message_thread_id: threadId }),
        chat: { id: 7, type: "private" },
      },
    },
  });
  const leaderPriority = createTelegramInteractionPriorityHandle({
    getCurrentInstanceId: () => "leader",
    getCurrentProfile: () => "default",
    getMessageOwnership: (_chatId, messageId) => leaderOwnership.get(messageId),
    classifyMessageOwnership: (_chatId, messageId) => {
      const purpose = leaderOwnership.get(messageId)?.purpose;
      return purpose ? { purpose } : undefined;
    },
    isInteractionPending: () => false,
    foreignOwnedUpdateForwarder: forwarder,
    prepareCallback() {
      return {
        acknowledgement: "This interaction has expired.",
        async commit() { return { handled: true }; },
      };
    },
    async handleInput() { return { handled: true }; },
    async answerCallbackQuery() {},
  });
  const leaderHandler = createTelegramUpdateHandle({
    pairingGate: pairedGate,
    priorityHandle: leaderPriority,
    registry: {
      version: 1,
      add: () => () => {},
      async dispatch() { leaderPublic += 1; return "consume"; },
    },
    async defaultHandle() {
      leaderDefault += 1;
      return { kind: "completed", reason: "ignored" };
    },
  });

  try {
    await receiverA.start();
    await receiverB.start();
    await leader.start();

    await leaderHandler(callbackUpdate(1, 201, 22), "ctx");
    const missingGeneration = leaderOwnership.get(101)!;
    missingGeneration.ownerGeneration = undefined;
    await leaderHandler(callbackUpdate(2, 101, 11), "ctx");
    missingGeneration.ownerGeneration = "stale-generation";
    await leaderHandler(callbackUpdate(3, 101, 11), "ctx");
    missingGeneration.ownerGeneration = generations.a;
    await leaderHandler(callbackUpdate(4, 101, 99), "ctx");
    await leaderHandler(callbackUpdate(5, 999, 11), "ctx");
    await leaderHandler(callbackUpdate(6, 101, 11, callbackA, 8), "ctx");
    assert.equal(
      await forwarder.forwardUpdate({
        update: callbackUpdate(7, 101, 11),
        profile: "other",
        target: targets.a,
        ownership: leaderOwnership.get(101)!,
      }),
      undefined,
    );
    assert.equal(
      await forwarder.forwardUpdate({
        update: callbackUpdate(8, 101, 11),
        profile: "default",
        target: { chatId: 7, threadId: 99 },
        ownership: leaderOwnership.get(101)!,
      }),
      undefined,
    );

    await leaderHandler(
      callbackUpdate(9, 101, undefined),
      "ctx",
    );
    assert.deepEqual(await pendingA, {
      handled: true,
      result: {
        status: "answered",
        answers: [{ type: "option", index: 1, label: "A", value: "a" }],
      },
    });
    await leaderHandler(callbackUpdate(10, 101, undefined), "ctx");
    await leaderHandler({
      update_id: 11,
      message: {
        message_id: 202,
        message_thread_id: 22,
        chat: { id: 7, type: "private" },
        from: { id: 7, is_bot: false },
        text: "Follower B answer",
        reply_to_message: {
          message_id: 201,
          chat: { id: 7, type: "private" },
          from: { id: 99, is_bot: true },
        },
      },
    }, "ctx");
    assert.deepEqual(await pendingB, {
      handled: true,
      result: {
        status: "answered",
        answers: [{ type: "text", label: "Follower B answer", value: "Follower B answer" }],
      },
    });

    await leaderHandler({ update_id: 12, message: {
      message_id: 300, chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false }, photo: [{ file_id: "photo" }],
    } } as unknown as Parameters<typeof leaderHandler>[0], "ctx");
    await leaderHandler({ update_id: 13, edited_message: {
      message_id: 301, chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false }, text: "edited",
    } }, "ctx");
    await leaderHandler({ update_id: 14, message: {
      message_id: 302, chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false }, text: "unanchored",
    } }, "ctx");
    await leaderHandler({ update_id: 15, message: {
      message_id: 303, chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false }, text: "ordinary",
      reply_to_message: {
        message_id: 999, chat: { id: 7, type: "private" },
        from: { id: 99, is_bot: true },
      },
    } }, "ctx");

    assert.equal(recursiveForwards, 0, "forwarded updates re-enter locally without recursion");
    assert.equal(followerPublic, 0, "interaction candidates stay private on followers");
    assert.equal(followerDefault, 0, "interaction candidates never reach follower fallback");
    assert.equal(leaderPublic, 4, "only media/edit/unanchored/ordinary updates remain public");
    assert.equal(leaderDefault, 0);
  } finally {
    runtimeA.shutdown();
    runtimeB.shutdown();
    await leader.stop();
    await receiverA.stop();
    await receiverB.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower recovery fence authenticates exact authority, drains, and resumes exact generation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-recovery-fence-"));
  const socketPath = join(dir, "follower.sock");
  const gate = new RecoveryProfileOperationGate();
  const activeLease = gate.enter("work");
  assert.ok(activeLease);
  let suspended = 0;
  let resumed = 0;
  let resumeFails = true;
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId: "inst-b",
    getAuthSecret: () => "secret",
    getRegistrationGeneration: () => "registration-b",
    getProfile: () => "work",
    getContext: () => "ctx",
    recoveryFence: {
      enter: (profile) => gate.enter(profile),
      beginFencing: (profile, generation) =>
        gate.beginFencing(profile, generation),
      awaitDrained: (profile, generation) =>
        gate.awaitDrained(profile, generation),
      resumeAfter: (profile, generation, resumeRuntime) =>
        gate.resumeAfter(profile, generation, resumeRuntime),
      suspendRuntime() {
        suspended += 1;
      },
      resumeRuntime() {
        resumed += 1;
        if (resumeFails) throw new Error("follower resume failed");
      },
    },
    handleForwardedCallback() {},
    handleForwardedReaction() {},
  });
  const send = (
    overrides: Partial<{
      auth: string;
      profile: string;
      recipientInstanceId: string;
      recipientRegistrationGeneration: string;
      fenceGeneration: string;
    }> = {},
    kind: "leader.fenceRecovery" | "leader.resumeRecovery" =
      "leader.fenceRecovery",
  ) =>
    sendTelegramBusLocalEnvelope({
      socketPath,
      timeoutMs: 500,
      envelope: {
        kind,
        requestId: `${kind}:${Math.random()}`,
        auth: overrides.auth ?? "secret",
        profile: overrides.profile ?? "work",
        recipientInstanceId: overrides.recipientInstanceId ?? "inst-b",
        recipientRegistrationGeneration:
          overrides.recipientRegistrationGeneration ?? "registration-b",
        fenceGeneration: overrides.fenceGeneration ?? "fence-b",
        sentAtMs: 1000,
      },
    });
  try {
    await receiver.start();
    for (const response of [
      await send({ auth: "wrong" }),
      await send({ profile: "other" }),
      await send({ recipientRegistrationGeneration: "stale" }),
      await send({ recipientInstanceId: "other" }),
    ]) {
      assert.equal(response?.kind === "bus.ack" && response.ok, false);
    }

    let fenceSettled = false;
    const fenceResponse = send().then((response) => {
      fenceSettled = true;
      return response;
    });
    await waitForCondition(() => suspended === 1);
    assert.equal(fenceSettled, false);
    assert.equal(gate.getState("work").phase, "fencing");
    activeLease.release();
    const fenced = await fenceResponse;
    assert.equal(fenced?.kind, "bus.ack");
    assert.equal(fenced?.ok, true);
    assert.deepEqual(
      fenced?.kind === "bus.ack" ? fenced.recoveryFence : undefined,
      {
        version: 1,
        state: "fenced",
        profile: "work",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "registration-b",
        fenceGeneration: "fence-b",
      },
    );
    assert.equal(gate.enter("work"), undefined);

    const failedResume = await send({}, "leader.resumeRecovery");
    assert.equal(failedResume?.kind, "bus.ack");
    assert.equal(failedResume?.ok, false);
    assert.equal(resumed, 1);
    assert.equal(gate.getState("work").phase, "fencing");
    assert.equal(gate.enter("work"), undefined);

    resumeFails = false;
    const resumeResponse = await send({}, "leader.resumeRecovery");
    assert.equal(resumeResponse?.kind, "bus.ack");
    assert.equal(resumeResponse?.ok, true);
    assert.equal(resumed, 2);
    assert.equal(gate.getState("work").phase, "active");
    const staleResume = await send({}, "leader.resumeRecovery");
    assert.equal(
      staleResume?.kind === "bus.ack" && staleResume.ok,
      false,
    );
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower receiver rejects delayed work from a replaced registration generation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-forward-generation-"));
  const socketPath = join(dir, "follower.sock");
  let handled = 0;
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId: "inst-b",
    getRegistrationGeneration: () => "generation-new",
    getContext: () => "ctx",
    handleForwardedCallback() {
      handled += 1;
    },
    handleForwardedReaction() {},
  });
  try {
    await receiver.start();
    const response = await sendTelegramBusLocalEnvelope({
      socketPath,
      envelope: {
        kind: "leader.forwardCallback",
        requestId: "leader:old:1",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-old",
        query: { id: "old" },
        sentAtMs: 2000,
      },
    });
    assert.equal(handled, 0);
    assert.deepEqual(response, {
      kind: "bus.ack",
      requestId: "leader:old:1",
      ok: false,
      message: "Stale Telegram bus follower registration generation.",
    });
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower heartbeat recovery invalidates follower authority before promotion", async () => {
  const promoted: unknown[] = [];
  const authorityEvents: string[] = [];
  let leaderStateCalls = 0;
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    {
      slot: "F",
      threadName: "Fjord",
    },
  );
  registrationState.subscribeAuthorityChange(() => {
    authorityEvents.push(
      `invalidate:${String(registrationState.isRegistered())}:${registrationState.getGeneration() ?? "none"}`,
    );
  });
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => registrationState.setRegistered(false),
    }),
    getLeaderState: () => {
      leaderStateCalls += 1;
      return leaderStateCalls === 1
        ? { kind: "active-elsewhere", lock: { pid: 99 } }
        : { kind: "inactive" };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async (_ctx, binding) => {
      authorityEvents.push(`promote:${String(registrationState.isRegistered())}`);
      promoted.push(binding);
      return true;
    },
    sleep: async () => undefined,
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "ctx");

  assert.deepEqual(promoted, [
    { target: { chatId: 42, threadId: 10 }, slot: "F", threadName: "Fjord" },
  ]);
  assert.deepEqual(authorityEvents, ["invalidate:true:none", "promote:false"]);
});

test("Bus follower registration invalidates old authority before replacement publication", () => {
  const state = createTelegramBusFollowerRegistrationState();
  state.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    { generation: "generation-1" },
  );
  const events: string[] = [];
  const unsubscribe = state.subscribeAuthorityChange(() => {
    events.push(
      `${String(state.isRegistered())}:${state.getTarget()?.threadId ?? "none"}:${state.getGeneration() ?? "none"}`,
    );
  });

  state.setRegistered(
    true,
    { chatId: 42, threadId: 11 },
    { generation: "generation-2" },
  );
  state.setRegistered(false);
  unsubscribe();
  state.setRegistered(true, { chatId: 42, threadId: 12 }, {
    generation: "generation-3",
  });

  assert.deepEqual(events, [
    "true:10:generation-1",
    "true:11:generation-2",
  ]);
  assert.equal(state.getTarget()?.threadId, 12);
  assert.equal(state.getGeneration(), "generation-3");
});

test("Bus follower election defers a higher slot to the lowest live candidate", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    { slot: "D", threadName: "Dawn" },
  );
  registrationState.setEligibleElectionSlots(["D", "C"]);
  let state: "inactive" | "winner" = "inactive";
  let promoted = 0;
  let registered = 0;
  const events: Array<Record<string, unknown> | undefined> = [];
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => {
        registered += 1;
        return true;
      },
      setContext: () => undefined,
      stop: () => registrationState.setRegistered(false),
    }),
    getLeaderState: () =>
      state === "inactive"
        ? { kind: "inactive" }
        : {
            kind: "active-elsewhere",
            lock: { pid: 99, instanceId: "slot-c", leaderEpoch: "epoch-c" },
          },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async () => {
      promoted += 1;
      return true;
    },
    sleep: async () => {
      state = "winner";
    },
    promotionGraceMs: 2500,
    recordRuntimeEvent: (_category, _message, details) => {
      events.push(details);
    },
  });

  await handler(new Error("leader disconnected"), "ctx");

  assert.equal(promoted, 0);
  assert.equal(registered, 1);
  assert.equal(
    events.some(
      (details) =>
        details?.phase === "follower-promotion-slot-priority" &&
        details.lowerEligibleSlot === "C",
    ),
    true,
  );
});

test("Bus follower heartbeat recovery never promotes over a live leader lease", async () => {
  const promoted: unknown[] = [];
  const phases: Array<string | undefined> = [];
  const events: Array<{ message: unknown; phase?: unknown }> = [];
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const liveLeader = {
    kind: "active-elsewhere" as const,
    lock: {
      pid: 99,
      instanceId: "leader-a",
      leaderEpoch: "epoch-a",
    },
  };
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => liveLeader,
    setLifecyclePhase: (phase) => {
      phases.push(phase);
    },
    updateStatus: () => undefined,
    promoteToLeader: async (_ctx, binding) => {
      promoted.push(binding);
      return true;
    },
    sleep: async () => undefined,
    promotionGraceMs: 0,
    recordRuntimeEvent: (_category, message, details) => {
      events.push({ message, phase: details?.phase });
    },
  });

  await handler(new Error("heartbeat failed"), "ctx");

  assert.deepEqual(promoted, []);
  assert.equal(phases.at(-1), undefined);
  assert.equal(
    events.some(
      (event) => event.phase === "follower-promotion-live-owner",
    ),
    true,
  );
});

test("Bus follower heartbeat recovery retries until a live lease becomes stale", async () => {
  let stateReadCount = 0;
  let scheduledRetry: (() => void) | undefined;
  let resolvePromoted: (() => void) | undefined;
  const promoted = new Promise<void>((resolve) => {
    resolvePromoted = resolve;
  });
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    { slot: "F", threadName: "Fjord" },
  );
  const liveLock = {
    pid: 99,
    instanceId: "leader-a",
    leaderEpoch: "epoch-a",
  };
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => {
      stateReadCount += 1;
      return stateReadCount <= 2
        ? { kind: "active-elsewhere", lock: liveLock }
        : { kind: "stale", lock: liveLock };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async (_ctx, binding, election) => {
      assert.deepEqual(binding, {
        target: { chatId: 42, threadId: 10 },
        slot: "F",
        threadName: "Fjord",
      });
      assert.deepEqual(election, { expectedOwner: liveLock });
      resolvePromoted?.();
      return true;
    },
    sleep: async () => undefined,
    scheduleRetry: (retry) => {
      scheduledRetry = retry;
    },
    getActiveContext: () => "ctx",
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "ctx");
  assert.ok(scheduledRetry);
  scheduledRetry();
  await promoted;
});

test("Bus follower election loser schedules re-registration with the winner", async () => {
  const scheduled: Array<() => void> = [];
  let registrationCalls = 0;
  let promotionCalls = 0;
  let registrationTarget: unknown;
  let resolveRegistered: (() => void) | undefined;
  const registered = new Promise<void>((resolve) => {
    resolveRegistered = resolve;
  });
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const staleLock = { pid: 99, leaderEpoch: "old-epoch" };
  const winnerLock = { pid: 100, leaderEpoch: "winner-epoch" };
  let state: "stale" | "winner" = "stale";
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async (_ctx, _leader, options) => {
        registrationCalls += 1;
        registrationTarget = options?.target;
        resolveRegistered?.();
        return true;
      },
      setContext: () => undefined,
      stop: () => {
        registrationState.setRegistered(false);
      },
    }),
    getLeaderState: () =>
      state === "stale"
        ? { kind: "stale", lock: staleLock }
        : { kind: "active-elsewhere", lock: winnerLock },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async () => {
      promotionCalls += 1;
      state = "winner";
      return false;
    },
    sleep: async () => undefined,
    scheduleRetry: (retry) => {
      scheduled.push(retry);
    },
    getActiveContext: () => "ctx",
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "ctx");
  assert.equal(promotionCalls, 1);
  assert.equal(scheduled.length, 1);
  scheduled.shift()?.();
  await registered;
  assert.equal(registrationCalls, 1);
  assert.deepEqual(registrationTarget, { chatId: 42, threadId: 10 });
});

test("Bus follower scheduled recovery transfers across session context replacement", async () => {
  const scheduled: Array<() => void> = [];
  let activeContext: string | undefined = "old-ctx";
  let stateReads = 0;
  let promotedContext: string | undefined;
  let resolvePromoted: (() => void) | undefined;
  const promoted = new Promise<void>((resolve) => {
    resolvePromoted = resolve;
  });
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const lock = { pid: 99, leaderEpoch: "epoch-a" };
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => {
      stateReads += 1;
      return stateReads <= 2
        ? { kind: "active-elsewhere", lock }
        : { kind: "stale", lock };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async (ctx) => {
      promotedContext = ctx;
      resolvePromoted?.();
      return true;
    },
    sleep: async () => undefined,
    scheduleRetry: (retry) => {
      scheduled.push(retry);
    },
    getActiveContext: () => activeContext,
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "old-ctx");
  activeContext = undefined;
  scheduled.shift()?.();
  assert.equal(scheduled.length, 1);
  activeContext = "new-ctx";
  scheduled.shift()?.();
  await promoted;
  assert.equal(promotedContext, "new-ctx");
});

test("Bus follower heartbeat recovery swallows stale-context status updates", async () => {
  const events: unknown[] = [];
  let leaderStateCalls = 0;
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => {
      leaderStateCalls += 1;
      return leaderStateCalls === 1
        ? { kind: "active-elsewhere", lock: { pid: 99 } }
        : { kind: "inactive" };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => {
      throw new Error("This extension ctx is stale after session replacement");
    },
    promoteToLeader: async () => true,
    sleep: async () => undefined,
    promotionGraceMs: 0,
    recordRuntimeEvent: (category, error, details) => {
      events.push({ category, error, details });
    },
  });

  await handler(new Error("heartbeat failed"), "stale-ctx");

  assert.equal(registrationState.getTarget(), undefined);
  assert.equal(
    events.some(
      (event) =>
        typeof event === "object" &&
        event !== null &&
        (event as { details?: { phase?: string } }).details?.phase ===
          "follower-stale-context-status",
    ),
    true,
  );
});

test("Bus follower target replacement handler persists restored target", async () => {
  const staleTargets: unknown[] = [];
  const upserts: unknown[] = [];
  let persisted = false;
  let updated = false;
  let syncState = {};
  const events: unknown[] = [];
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const handler = createTelegramBusFollowerTargetReplacementHandler({
    topicTargetStore: {
      load: async () => undefined,
      list: () => [
        {
          profileKey: "manual:old",
          owner: { kind: "manual-follower", instanceId: "old" },
          instanceId: "inst-a",
          target: { chatId: 42, threadId: 10 },
          status: "active",
          createdAtMs: 1000,
          updatedAtMs: 1000,
          slot: "E",
          threadName: "Ember",
        },
      ],
      markStaleByTarget: (target) => {
        staleTargets.push(target);
        return true;
      },
      upsert: (record) => {
        upserts.push(record);
        return record;
      },
      persist: async () => {
        persisted = true;
      },
    },
    registrationState,
    instanceId: "inst-a",
    getManualFollowerProfileKey: () => "manual:new",
    manualFollowerOwnerId: "new",
    getSyncState: () => syncState,
    setSyncState: (state) => {
      syncState = state;
    },
    getNowMs: () => 2000,
    updateStatus: () => {
      updated = true;
    },
    recordRuntimeEvent: (_category, message, details) => {
      events.push({ message, details });
    },
  });
  await handler(
    {
      target: { chatId: 42, threadId: 11 },
      oldTarget: { chatId: 42, threadId: 10 },
      reason: "thread-restore",
    },
    "ctx",
  );
  assert.deepEqual(staleTargets, [{ chatId: 42, threadId: 10 }]);
  assert.equal(registrationState.getTarget()?.threadId, 11);
  assert.equal(persisted, true);
  assert.equal(updated, true);
  assert.deepEqual(syncState, {
    "target-bindings": {
      status: "fresh",
      updatedAtMs: 2000,
      lastReconcileAction: "follower-thread-restore",
    },
  });
  assert.deepEqual(upserts, [
    {
      profileKey: "manual:old",
      owner: { kind: "manual-follower", instanceId: "new" },
      target: { chatId: 42, threadId: 11 },
      status: "active",
      syncStatus: "open",
      createdAtMs: 1000,
      updatedAtMs: 2000,
      lastSyncObservedAtMs: 2000,
      lastReconcileAction: "follower-thread-restore",
      instanceId: "inst-a",
      slot: "E",
      threadName: "Ember",
      rerouteConfirmedAtMs: 2000,
    },
  ]);
  assert.deepEqual(events, [
    {
      message: "Telegram follower thread target replaced",
      details: {
        phase: "follower-thread-restore",
        chatId: 42,
        threadId: 11,
        oldThreadId: 10,
        slot: "E",
      },
    },
  ]);
});

test("Bus follower target replacement resolves named-profile fallback at call time", async () => {
  let activeProfileKey = "manual:default";
  const upserts: Array<{ profileKey: string }> = [];
  const registrationState = createTelegramBusFollowerRegistrationState();
  const handler = createTelegramBusFollowerTargetReplacementHandler({
    topicTargetStore: {
      load: async () => undefined,
      list: () => [],
      markStaleByTarget: () => false,
      upsert: (record) => {
        upserts.push(record);
        return record;
      },
      persist: async () => undefined,
    },
    registrationState,
    instanceId: "inst-a",
    getManualFollowerProfileKey: () => activeProfileKey,
    manualFollowerOwnerId: "owner-a",
    getSyncState: () => ({}),
    setSyncState: () => undefined,
    getNowMs: () => 2000,
    updateStatus: () => undefined,
  });
  activeProfileKey = "profile:work:manual-follower:owner-a";
  await handler(
    {
      target: { chatId: 42, threadId: 11 },
      reason: "thread-restore",
    },
    "ctx",
  );
  assert.equal(upserts[0]?.profileKey, "profile:work:manual-follower:owner-a");
});

test("Bus follower assembly wires receiver, recovery, and registration", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-assembly-"));
  const leaderSocketPath = join(dir, "leader.sock");
  const followerSocketPath = join(dir, "follower.sock");
  const registrationState = createTelegramBusFollowerRegistrationState();
  let requestSequence = 0;
  const leader = createTelegramBusLocalServer({
    socketPath: leaderSocketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: createTelegramBusFollowerRegistry(),
      provisionFollowerTarget: () => ({ chatId: 7, threadId: 42 }),
    }),
  });
  const assembly = createTelegramBusFollowerRuntimeAssembly<
    { cwd: string },
    unknown,
    unknown
  >({
    receiver: {
      socketPath: followerSocketPath,
      instanceId: "inst-a",
      getContext: () => ({ cwd: "/repo" }),
      handleForwardedCallback: () => undefined,
      handleForwardedReaction: () => undefined,
    },
    targetReplacement: {
      topicTargetStore: {
        load: async () => undefined,
        list: () => [],
        markStaleByTarget: () => false,
        upsert: (record) => record,
        persist: async () => undefined,
      },
      registrationState,
      instanceId: "inst-a",
      getManualFollowerProfileKey: () => "manual:a",
      manualFollowerOwnerId: "a",
      getSyncState: () => ({}),
      setSyncState: () => undefined,
      updateStatus: () => undefined,
    },
    recovery: {
      registrationState,
      getLeaderState: () => ({ kind: "inactive" }),
      setLifecyclePhase: () => undefined,
      updateStatus: () => undefined,
      promoteToLeader: async () => true,
      sleep: async () => undefined,
      promotionGraceMs: 1,
      recordRuntimeEvent: () => undefined,
    },
    registration: {
      instanceId: "inst-a",
      getFollowerBusSocketPath: () => followerSocketPath,
      getLeaderSocketPath: () => leaderSocketPath,
      registrationState,
      createRequestId: () => `inst-a:${++requestSequence}`,
      getSessionGeneration: () => 1,
    },
  });
  try {
    await leader.start();
    assert.equal(
      await assembly.registration.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: leaderSocketPath },
      ),
      true,
    );
    if (process.platform === "win32") {
      assert.equal(
        getTelegramBusTransportKind(
          resolveTelegramBusSocketPath(followerSocketPath),
        ),
        "pipe",
      );
    } else {
      assert.equal(
        existsSync(resolveTelegramBusSocketPath(followerSocketPath)),
        true,
      );
    }
    assert.deepEqual(registrationState.getTarget(), {
      chatId: 7,
      threadId: 42,
    });
  } finally {
    assembly.registration.stop();
    await assembly.receiver.stop();
    await leader.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Delayed stale registration rejection cannot deregister a replacement generation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-stale-register-"));
  const socketPath = join(dir, "bus.sock");
  const state = createTelegramBusFollowerRegistrationState();
  let releaseOld!: () => void;
  let observeOld!: () => void;
  const oldObserved = new Promise<void>((resolve) => {
    observeOld = resolve;
  });
  const oldGate = new Promise<void>((resolve) => {
    releaseOld = resolve;
  });
  const server = createTelegramBusLocalServer({
    socketPath,
    async handleEnvelope(envelope) {
      if (envelope.kind !== "follower.register") return undefined;
      if (envelope.registration.registrationGeneration === "generation-old") {
        observeOld();
        await oldGate;
        return {
          kind: "bus.ack",
          requestId: envelope.requestId,
          ok: false,
          message: "old registration rejected",
        };
      }
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: {
          target: { chatId: 7, threadId: 42 },
          slot: "F",
          threadName: "Fjord",
        },
      };
    },
  });
  let requestSequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `request-${++requestSequence}`,
    getSessionGeneration: () => 1,
    registrationState: state,
    heartbeatMs: 60_000,
  });
  let authorityChanges = 0;
  state.subscribeAuthorityChange(() => {
    authorityChanges += 1;
  });
  try {
    await server.start();
    const oldRegistration = follower.registerWithLeader(
      { cwd: "/repo" },
      { busSocketPath: socketPath },
      { registrationGeneration: "generation-old" },
    );
    await oldObserved;
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
        { registrationGeneration: "generation-new" },
      ),
      true,
    );
    assert.equal(state.getGeneration(), "generation-new");
    assert.equal(authorityChanges, 1);

    releaseOld();
    assert.equal(await oldRegistration, false);
    assert.equal(state.isRegistered(), true);
    assert.equal(state.getGeneration(), "generation-new");
    assert.equal(authorityChanges, 1);
  } finally {
    releaseOld();
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Delayed stale initial heartbeat cannot invalidate replacement registration or interaction authority", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-stale-heartbeat-"));
  const socketPath = join(dir, "bus.sock");
  const state = createTelegramBusFollowerRegistrationState();
  let releaseOldHeartbeat!: () => void;
  let observeOldHeartbeat!: () => void;
  const oldHeartbeatObserved = new Promise<void>((resolve) => {
    observeOldHeartbeat = resolve;
  });
  const oldHeartbeatGate = new Promise<void>((resolve) => {
    releaseOldHeartbeat = resolve;
  });
  const heartbeatAuth: Array<string | undefined> = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    async handleEnvelope(envelope) {
      if (envelope.kind === "follower.register") {
        return {
          kind: "bus.ack",
          requestId: envelope.requestId,
          ok: true,
          result: {
            target: {
              chatId: 7,
              threadId:
                envelope.registration.registrationGeneration === "generation-old"
                  ? 41
                  : 42,
            },
            slot: envelope.registration.registrationGeneration === "generation-old" ? "O" : "N",
            threadName:
              envelope.registration.registrationGeneration === "generation-old"
                ? "Olden"
                : "Novel",
          },
        };
      }
      if (envelope.kind !== "follower.heartbeat") return undefined;
      heartbeatAuth.push(envelope.auth);
      if (envelope.registrationGeneration === "generation-old") {
        observeOldHeartbeat();
        await oldHeartbeatGate;
        return {
          kind: "bus.ack",
          requestId: envelope.requestId,
          ok: false,
          message: "old heartbeat rejected",
        };
      }
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: { eligibleElectionSlots: ["N"] },
      };
    },
  });
  let requestSequence = 0;
  let recoveryCalls = 0;
  let heartbeatEvents = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `request-${++requestSequence}`,
    getSessionGeneration: () => 1,
    getLeaderAuthSecret: (leader) => leader.busSecret,
    registrationState: state,
    heartbeatMs: 60_000,
    onHeartbeatFailure: () => {
      recoveryCalls += 1;
    },
    recordRuntimeEvent: (_category, _error, details) => {
      if (details?.phase === "follower-heartbeat") heartbeatEvents += 1;
    },
  });
  let authorityChanges = 0;
  let replacementInteraction: ReturnType<
    typeof createTelegramInteractionRuntime
  > | undefined;
  state.subscribeAuthorityChange(() => {
    authorityChanges += 1;
    replacementInteraction?.invalidateAuthority();
  });
  let replacementAttempt: ReturnType<
    ReturnType<typeof createTelegramInteractionRuntime>["request"]
  > | undefined;
  try {
    await server.start();
    const oldRegistration = follower.registerWithLeader(
      { cwd: "/old" },
      { busSocketPath: socketPath, busSecret: "old-auth" },
      { registrationGeneration: "generation-old" },
    );
    await oldHeartbeatObserved;
    assert.equal(state.getGeneration(), "generation-old");

    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/new" },
        { busSocketPath: socketPath, busSecret: "new-auth" },
        { registrationGeneration: "generation-new" },
      ),
      true,
    );
    assert.equal(state.getGeneration(), "generation-new");
    assert.deepEqual(state.getTarget(), { chatId: 7, threadId: 42 });
    assert.deepEqual(state.getEligibleElectionSlots(), ["N"]);
    assert.equal(authorityChanges, 2);

    const snapshot: TelegramInteractionActiveTurnSnapshot = {
      turnId: "turn-new",
      target: { chatId: 7, threadId: 42 },
      profile: "default",
      transportGeneration: "transport-new",
      sessionGeneration: "session-new",
      authorityGeneration: "generation-new",
      sourceMessageIds: [],
    };
    replacementInteraction = createTelegramInteractionRuntime({
      generation: "interaction-new",
      captureActiveTurn: () => snapshot,
      isActive: (candidate) => candidate === snapshot,
      delivery: {
        async sendView() {
          return {
            ok: true,
            value: {
              target: snapshot.target,
              messageIds: [101],
              generation: "delivery-new",
            },
          };
        },
        async editView(handle) {
          return { ok: true, value: handle };
        },
        async deleteView() {
          return { ok: true, value: undefined };
        },
      },
    });
    replacementAttempt = replacementInteraction.request({
      question: "Replacement question",
      mode: { kind: "text" },
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(replacementInteraction.isPending(), true);

    releaseOldHeartbeat();
    assert.equal(await oldRegistration, false);
    assert.equal(state.isRegistered(), true);
    assert.equal(state.getGeneration(), "generation-new");
    assert.deepEqual(state.getTarget(), { chatId: 7, threadId: 42 });
    assert.deepEqual(state.getEligibleElectionSlots(), ["N"]);
    assert.equal(authorityChanges, 2);
    assert.equal(recoveryCalls, 0);
    assert.equal(heartbeatEvents, 0);
    assert.equal(replacementInteraction.isPending(), true);
    assert.deepEqual(heartbeatAuth, ["old-auth", "new-auth"]);

    replacementInteraction.shutdown();
    assert.deepEqual(await replacementAttempt, {
      handled: true,
      result: { status: "unavailable" },
    });
    replacementInteraction = undefined;
  } finally {
    releaseOldHeartbeat();
    replacementInteraction?.shutdown();
    if (replacementAttempt) await replacementAttempt;
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration state tracks successful registration and stop", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-state-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const state = createTelegramBusFollowerRegistrationState();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      provisionFollowerTarget() {
        return {
          chatId: -1007,
          threadId: 42,
          slot: "E",
          threadName: "Ember",
        };
      },
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getSessionGeneration: () => 1,
    getNowMs: () => 1000,
    registrationState: state,
  });
  try {
    await server.start();
    assert.equal(state.isRegistered(), false);
    assert.equal(state.getTarget(), undefined);
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(state.isRegistered(), true);
    assert.deepEqual(state.getTarget(), { chatId: -1007, threadId: 42 });
    assert.equal(state.getSlot(), "E");
    assert.equal(state.getThreadName(), "Ember");
    follower.stop();
    assert.equal(state.isRegistered(), false);
    assert.equal(state.getTarget(), undefined);
    assert.equal(state.getSlot(), undefined);
    assert.equal(state.getThreadName(), undefined);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower re-registration carries its last known target", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-follower-reload-target-"),
  );
  const socketPath = join(dir, "bus.sock");
  const state = createTelegramBusFollowerRegistrationState();
  const registrations: Array<{
    target?: unknown;
    slot?: string;
    threadName?: string;
  }> = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: createTelegramBusFollowerRegistry(),
      provisionFollowerTarget(registration) {
        registrations.push({
          target: registration.target,
          slot: registration.slot,
          threadName: registration.threadName,
        });
        return {
          chatId: 7,
          threadId: 42,
          slot: "E",
          threadName: "Ember",
        };
      },
    }),
  });
  let requestSequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:reload:${++requestSequence}`,
    getSessionGeneration: () => 1,
    registrationState: state,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    state.setRegistered(false);
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.deepEqual(registrations, [
      { target: undefined, slot: undefined, threadName: "repo" },
      {
        target: { chatId: 7, threadId: 42 },
        slot: "E",
        threadName: "Ember",
      },
    ]);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime retries while leader endpoint is starting", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-retry-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const state = createTelegramBusFollowerRegistrationState();
  const events: Array<Record<string, unknown> | undefined> = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      provisionFollowerTarget() {
        return { chatId: -1007, threadId: 42 };
      },
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getSessionGeneration: () => 1,
    getNowMs: () => 1000,
    registrationState: state,
    registrationTimeoutMs: 50,
    registrationRetryAttempts: 10,
    registrationRetryDelayMs: 10,
    recordRuntimeEvent(_category, _error, details) {
      events.push(details);
    },
  });
  try {
    setTimeout(() => {
      void server.start();
    }, 25);
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(state.isRegistered(), true);
    assert.deepEqual(state.getTarget(), { chatId: -1007, threadId: 42 });
    assert.equal(
      events.some((event) => event?.phase === "follower-register-client-retry"),
      true,
    );
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime waits for slow target provisioning", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-slow-register-"),
  );
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const state = createTelegramBusFollowerRegistrationState();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      async provisionFollowerTarget() {
        await new Promise((resolve) => setTimeout(resolve, 50));
        return { chatId: -1007, threadId: 42 };
      },
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getSessionGeneration: () => 1,
    getNowMs: () => 1000,
    registrationState: state,
    timeoutMs: 20,
    registrationTimeoutMs: 250,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(state.isRegistered(), true);
    assert.deepEqual(state.getTarget(), { chatId: -1007, threadId: 42 });
    assert.deepEqual(registry.get("inst-a")?.target, {
      chatId: -1007,
      threadId: 42,
    });
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime registers and explicitly disconnects", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  let disconnects = 0;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      getNowMs: () => 1000,
      onFollowerDisconnected() {
        disconnects += 1;
      },
    }),
  });
  let sequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++sequence}`,
    getSessionGeneration: () => 1,
    getNowMs: () => 1000,
    getPid: () => 123,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.deepEqual(registry.get("inst-a"), {
      instanceId: "inst-a",
      profileKey: "cwd:/repo",
      threadName: "repo",
      cwd: "/repo",
      pid: 123,
      registrationGeneration: "inst-a:1",
      sessionGeneration: 1,
      connectedAtMs: 1000,
      lastHeartbeatMs: 1000,
      target: undefined,
    });
    assert.equal(await follower.disconnectFromLeader?.(), true);
    assert.equal(disconnects, 1);
    assert.equal(registry.get("inst-a"), undefined);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime accepts explicit manual profile keys", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-profile-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getSessionGeneration: () => 1,
    getNowMs: () => 1000,
    getProfileKey: () => "manual:inst-a",
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(registry.get("inst-a")?.profileKey, "manual:inst-a");
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime reports heartbeat failure with active context", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-heartbeat-fail-"),
  );
  const socketPath = join(dir, "bus.sock");
  const failures: unknown[] = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: true,
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getSessionGeneration: () => 1,
    registrationState: createTelegramBusFollowerRegistrationState(),
    heartbeatMs: 10,
    timeoutMs: 50,
    onHeartbeatFailure(error, ctx) {
      failures.push({ error: String(error), ctx });
    },
  });
  try {
    await server.start();
    await follower.registerWithLeader(
      { cwd: "/repo" },
      { busSocketPath: socketPath },
    );
    await server.stop();
    await waitForCondition(() => failures.length > 0, 200);
    assert.deepEqual((failures[0] as { ctx: unknown }).ctx, { cwd: "/repo" });
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime reports rejected heartbeat with active context", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-heartbeat-reject-"),
  );
  const socketPath = join(dir, "bus.sock");
  const failures: unknown[] = [];
  let requestSequence = 0;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: envelope.kind === "follower.register",
      message:
        envelope.kind === "follower.register"
          ? undefined
          : "Unknown Telegram bus follower instance.",
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++requestSequence}`,
    getSessionGeneration: () => 1,
    registrationState: createTelegramBusFollowerRegistrationState(),
    heartbeatMs: 10,
    timeoutMs: 50,
    onHeartbeatFailure(error, ctx) {
      failures.push({ error: String(error), ctx });
    },
  });
  try {
    await server.start();
    await follower.registerWithLeader(
      { cwd: "/repo" },
      { busSocketPath: socketPath },
    );
    await waitForCondition(() => failures.length > 0, 200);
    assert.deepEqual(failures[0], {
      error: "Error: Unknown Telegram bus follower instance.",
      ctx: { cwd: "/repo" },
    });
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime heartbeats until stopped", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-heartbeat-"),
  );
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  let nowMs = 1000;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      getNowMs: () => nowMs,
    }),
  });
  let requestSequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++requestSequence}`,
    getSessionGeneration: () => 1,
    getNowMs: () => nowMs,
    heartbeatMs: 50,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    nowMs = 2000;
    await waitForCondition(
      () => registry.get("inst-a")?.lastHeartbeatMs === 2000,
      120,
    );
    follower.stop();
    nowMs = 3000;
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(registry.get("inst-a")?.lastHeartbeatMs, 2000);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime surfaces leader rejection reasons", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-reject-"));
  const socketPath = join(dir, "bus.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: () => ({
      kind: "bus.ack",
      requestId: "inst-a:1",
      ok: false,
      message: "Unauthorized Telegram bus envelope.",
    }),
  });
  const stopped: string[] = [];
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getSessionGeneration: () => 1,
    stopReceiving: () => {
      stopped.push("stop");
    },
  });
  try {
    await server.start();
    await assert.rejects(
      () =>
        follower.registerWithLeader(
          { cwd: "/repo" },
          { busSocketPath: socketPath },
        ),
      /Unauthorized Telegram bus envelope/,
    );
    assert.deepEqual(stopped, ["stop"]);
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime derives leader socket when lock omits it", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-derived-socket-"),
  );
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      getNowMs: () => 1000,
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getSessionGeneration: () => 1,
    getLeaderSocketPath: () => socketPath,
  });
  try {
    await server.start();
    assert.equal(await follower.registerWithLeader({ cwd: "/repo" }, {}), true);
    assert.equal(registry.get("inst-a")?.instanceId, "inst-a");
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API caller sends method calls and returns leader results", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-caller-"));
  const socketPath = join(dir, "bus.sock");
  const received: unknown[] = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => {
      received.push(envelope);
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: { message_id: 55 },
      };
    },
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    manualFollowerOwnerId: "owner-a",
    createRequestId: () => "inst-a:1",
    getProfile: () => "default",
    getTarget: () => ({ chatId: 1, threadId: 2 }),
    getRegistrationGeneration: () => "generation-a",
    getSessionGeneration: () => 3,
    getNowMs: () => 7000,
  });
  try {
    await server.start();
    assert.deepEqual(await callApi("sendRichMessage", [{ chat_id: 1 }]), {
      message_id: 55,
    });
    assert.deepEqual(received, [
      {
        kind: "follower.callApi",
        requestId: "inst-a:1",
        profile: "default",
        target: { chatId: 1, threadId: 2 },
        instanceId: "inst-a",
        manualFollowerOwnerId: "owner-a",
        registrationGeneration: "generation-a",
        followerSessionGeneration: 3,
        method: "sendRichMessage",
        args: [{ chat_id: 1 }],
        sentAtMs: 7000,
      },
    ]);
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API caller preserves structured commit-unknown errors", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-ambiguous-"));
  const socketPath = join(dir, "bus.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: false,
      message: "sendMessage response was lost",
      error: { code: "commit-unknown", method: "sendMessage" },
    }),
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    manualFollowerOwnerId: "owner-a",
    createRequestId: () => "inst-a:ambiguous:1",
    getProfile: () => "default",
    getTarget: () => ({ chatId: 1 }),
    getRegistrationGeneration: () => "generation-a",
    getSessionGeneration: () => 3,
  });
  try {
    await server.start();
    await assert.rejects(
      () => callApi("call", ["sendMessage", { chat_id: 1, text: "hello" }]),
      isTelegramApiCommitUnknownError,
    );
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API caller classifies non-idempotent acknowledgement loss as commit-unknown", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-ack-loss-"));
  const socketPath = join(dir, "bus.sock");
  let executions = 0;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => {
      executions += 1;
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: { message_id: 77 },
      };
    },
    shouldDropResponse: () => true,
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    manualFollowerOwnerId: "owner-a",
    createRequestId: () => "inst-a:ack-loss:1",
    getProfile: () => "default",
    getTarget: () => ({ chatId: 1 }),
    getRegistrationGeneration: () => "generation-a",
    getSessionGeneration: () => 3,
    timeoutMs: 10,
  });
  try {
    await server.start();
    await assert.rejects(
      () => callApi("call", ["sendMessage", { chat_id: 1, text: "hello" }]),
      isTelegramApiCommitUnknownError,
    );
    assert.equal(executions, 1);
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower session replacement preserves a same-process handoff", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 1, threadId: 2 },
    { slot: "B", threadName: "Beryl" },
  );
  const events: unknown[] = [];
  let suspended = false;
  const suspend = createTelegramBusFollowerSessionReplacementSuspender({
    registrationState,
    instanceId: "old-inst",
    async suspendPolling() {
      suspended = true;
      registrationState.setRegistered(false);
    },
    recordRuntimeEvent(category, message, details) {
      events.push({ category, message, details });
    },
    getPid: () => 10,
    getNowMs: () => 500,
  });

  await suspend();

  assert.equal(suspended, true);
  assert.equal(registrationState.isRegistered(), false);
  assert.deepEqual(getTelegramFollowerSessionHandoff(), {
    pid: 10,
    instanceId: "old-inst",
    createdAtMs: 500,
    target: { chatId: 1, threadId: 2 },
    slot: "B",
    threadName: "Beryl",
  });
  assert.deepEqual(events, [
    {
      category: "bus",
      message: "Telegram follower registration suspended for session replacement",
      details: {
        phase: "follower-session-handoff",
        instanceId: "old-inst",
        chatId: 1,
        threadId: 2,
      },
    },
  ]);
  setTelegramFollowerSessionHandoff(undefined);
});

test("Bus session replacement preserves the promoted leader binding", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  const events: unknown[] = [];
  const suspend = createTelegramBusFollowerSessionReplacementSuspender({
    registrationState,
    instanceId: "promoted-inst",
    suspendPolling: async () => undefined,
    isLeader: () => true,
    getLeaderBinding: () => ({
      target: { chatId: 1, threadId: 3 },
      slot: "C",
      threadName: "Cinder",
    }),
    getActiveContext: () => ({ cwd: "/repo" }),
    getActiveProfileName: () => "work",
    recordRuntimeEvent(category, message, details) {
      events.push({ category, message, details });
    },
    getPid: () => 10,
    getNowMs: () => 500,
  });

  try {
    await suspend();
    assert.deepEqual(getTelegramLeaderSessionHandoff(), {
      pid: 10,
      instanceId: "promoted-inst",
      createdAtMs: 500,
      profileKey: "profile:work:cwd:/repo",
      target: { chatId: 1, threadId: 3 },
      slot: "C",
      threadName: "Cinder",
    });
    assert.deepEqual(events, [
      {
        category: "bus",
        message: "Telegram leader binding suspended for session replacement",
        details: {
          phase: "leader-session-handoff",
          instanceId: "promoted-inst",
          chatId: 1,
          threadId: 3,
          slot: "C",
          threadName: "Cinder",
        },
      },
    ]);
  } finally {
    setTelegramLeaderSessionHandoff(undefined);
  }
});

test("Bus follower session refresh re-registers with the handed-off target", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  const registrations: unknown[] = [];
  const events: unknown[] = [];
  setTelegramFollowerSessionHandoff({
    pid: process.pid,
    instanceId: "old-inst",
    createdAtMs: Date.now(),
    target: { chatId: 1, threadId: 2 },
    slot: "B",
    threadName: "Beryl",
  });
  const refresh = createTelegramBusFollowerSessionRefreshHook({
    registrationState,
    registrationRuntime: {
      async registerWithLeader(ctx, leader, options) {
        registrations.push({ ctx, leader, options });
        registrationState.setRegistered(
          true,
          options?.target,
          { slot: "B", threadName: "Beryl" },
        );
        return true;
      },
      setContext: () => undefined,
    },
    getLeaderState: () => ({
      kind: "active-elsewhere",
      lock: { pid: 20, busSocketPath: "/tmp/leader.sock" },
    }),
    updateStatus: () => undefined,
    recordRuntimeEvent(category, message, details) {
      events.push({ category, message, details });
    },
  });

  await refresh({}, { cwd: "/repo" });

  assert.deepEqual(registrations, [
    {
      ctx: { cwd: "/repo" },
      leader: { pid: 20, busSocketPath: "/tmp/leader.sock" },
      options: { target: { chatId: 1, threadId: 2 } },
    },
  ]);
  assert.equal(registrationState.isRegistered(), true);
  assert.deepEqual(registrationState.getTarget(), { chatId: 1, threadId: 2 });
  assert.equal(getTelegramFollowerSessionHandoff(), undefined);
  assert.deepEqual(events, [
    {
      category: "bus",
      message: "Telegram follower registration restored after session replacement",
      details: {
        phase: "follower-session-restore",
        previousInstanceId: "old-inst",
      },
    },
    {
      category: "bus",
      message: "Telegram follower session context refreshed",
      details: { phase: "follower-session-refresh" },
    },
  ]);
});
