/**
 * Composed durable inbound process fixture
 * Zones: test infrastructure, recovery, polling, queue, multi-instance bus
 * Runs production recovery, polling, queue, and follower-receiver seams behind
 * deterministic IPC barriers so the parent can hard-kill real process state.
 */

import { createTelegramBusForwardedUpdateReceiverRuntime } from "../../lib/bus-follower.ts";
import { createInboundRecoveryRuntime, type TelegramInboundRecoveryUpdate } from "../../lib/inbound-recovery.ts";
import { runTelegramPollLoop } from "../../lib/polling.ts";
import {
  createTelegramQueueDispatchRuntime,
  createTelegramQueueStore,
  enqueueTelegramPromptTurnRuntime,
  type PendingTelegramTurn,
} from "../../lib/queue.ts";
import {
  openRecoveryStore,
  RecoveryProfileOperationGate,
  type RecoveryIdentity,
  type RecoveryInboundFaultId,
  type RecoveryReassignmentBindingValidation,
  type RecoveryStoreOpenOptions,
} from "../../lib/recovery.ts";

interface FixtureContext {
  generation: number;
}

interface RuntimeIdentityOptions {
  rootPath: string;
  target: { chatId: number; threadId?: number };
  owner:
    | { kind: "leader"; ownerId: string; leaderEpoch: string }
    | {
        kind: "manual-follower";
        ownerId: string;
        registrationGeneration: string;
      };
  sessionGeneration: number;
  selectedFault?: RecoveryInboundFaultId;
}

interface ProcessRequest {
  requestId: string;
  command: "status" | "rehydrate" | "reassign" | "gate-status";
}

const blockingWord = new Int32Array(new SharedArrayBuffer(4));

function parseFaultId(value: string | undefined): RecoveryInboundFaultId | undefined {
  switch (value) {
    case "IN-01":
    case "IN-02":
    case "IN-03":
    case "IN-04":
    case "IN-05":
    case "IN-06":
    case "IN-07":
    case "IN-08":
    case "IN-GROUP-01":
    case "IN-GROUP-02":
    case "IN-GROUP-03":
    case "DOWN-01":
    case "DOWN-02":
      return value;
    default:
      return undefined;
  }
}

function send(message: Record<string, unknown>): void {
  if (!process.send) throw new Error("fixture requires a Node IPC channel");
  process.send(message);
}

function blockAtFault(
  selectedFault: RecoveryInboundFaultId | undefined,
  faultId: RecoveryInboundFaultId,
): void {
  if (faultId !== selectedFault) return;
  send({ type: "fault-boundary", faultId });
  Atomics.wait(blockingWord, 0, 0);
}

function identitiesEqual(left: RecoveryIdentity, right: RecoveryIdentity): boolean {
  return (
    left.profile === right.profile &&
    left.target.chatId === right.target.chatId &&
    left.target.threadId === right.target.threadId &&
    left.sessionGeneration === right.sessionGeneration &&
    left.owner.kind === right.owner.kind &&
    left.owner.ownerId === right.owner.ownerId &&
    (left.owner.kind === "leader"
      ? right.owner.kind === "leader" &&
        left.owner.leaderEpoch === right.owner.leaderEpoch
      : right.owner.kind === "manual-follower" &&
        left.owner.registrationGeneration ===
          right.owner.registrationGeneration)
  );
}

function createRuntime(options: RuntimeIdentityOptions) {
  const ctx: FixtureContext = { generation: options.sessionGeneration };
  const identity: RecoveryIdentity = {
    profile: "default",
    target: { ...options.target },
    owner: { ...options.owner },
    sessionGeneration: options.sessionGeneration,
  };
  const validateReassignmentBinding = (
    input: RecoveryReassignmentBindingValidation,
  ): boolean =>
    input.expectedBinding === "new-owner" &&
    identitiesEqual(input.currentIdentity, identity) &&
    input.reassignment.profile === identity.profile &&
    input.reassignment.target.chatId === identity.target.chatId &&
    input.reassignment.target.threadId === identity.target.threadId &&
    input.reassignment.newSessionGeneration === identity.sessionGeneration &&
    input.reassignment.newOwner.kind === identity.owner.kind &&
    input.reassignment.newOwner.ownerId === identity.owner.ownerId &&
    (input.reassignment.newOwner.kind === "leader"
      ? identity.owner.kind === "leader" &&
        input.reassignment.newOwner.leaderEpoch === identity.owner.leaderEpoch
      : identity.owner.kind === "manual-follower" &&
        input.reassignment.newOwner.registrationGeneration ===
          identity.owner.registrationGeneration);
  const openStore = (storeOptions: RecoveryStoreOpenOptions) =>
    openRecoveryStore({
      ...storeOptions,
      rootPath: options.rootPath,
      fault: (faultId) => blockAtFault(options.selectedFault, faultId),
    });
  const runtime = createInboundRecoveryRuntime<
    TelegramInboundRecoveryUpdate,
    FixtureContext
  >({
    getProfile: () => "default",
    getAllowedUserId: () => 7,
    getCurrentInstanceId: () => identity.owner.ownerId,
    getSessionGeneration: () => options.sessionGeneration,
    isSessionActive: (candidate, generation) =>
      candidate === ctx && generation === options.sessionGeneration,
    resolveCurrentIdentity: (target) =>
      target.chatId === identity.target.chatId &&
      target.threadId === identity.target.threadId
        ? identity
        : undefined,
    resolveOperatorIdentity: () => identity,
    isIdentityAuthenticated: (candidate) => identitiesEqual(candidate, identity),
    validateReassignmentBinding,
    planResponsibility: () => ({ kind: "local", target: identity.target }),
    openStore,
  });
  return { ctx, identity, runtime };
}

function createUpdate(
  updateId: number,
  target: { chatId: number; threadId?: number },
): TelegramInboundRecoveryUpdate {
  return {
    update_id: updateId,
    message: {
      message_id: updateId * 10,
      ...(target.threadId === undefined
        ? {}
        : { message_thread_id: target.threadId }),
      chat: { id: target.chatId },
      from: { id: 7, is_bot: false },
    },
  };
}

function createTurn(update: TelegramInboundRecoveryUpdate): PendingTelegramTurn {
  const messageId = update.message?.message_id;
  if (messageId === undefined) throw new Error("fixture update lacks message id");
  const chatId = update.message?.chat?.id;
  if (chatId === undefined) throw new Error("fixture update lacks chat id");
  const target = {
    chatId,
    ...(update.message?.message_thread_id === undefined
      ? {}
      : { threadId: update.message.message_thread_id }),
  };
  return {
    kind: "prompt",
    chatId,
    target,
    replyToMessageId: messageId,
    sourceMessageIds: [messageId],
    queueOrder: update.update_id,
    queueLane: "default",
    laneOrder: update.update_id,
    queuedAttachments: [],
    content: [{ type: "text", text: `[telegram] process ${update.update_id}` }],
    historyText: `process ${update.update_id}`,
    statusSummary: `process ${update.update_id}`,
  };
}

function createQueueComposition(
  runtime: ReturnType<typeof createRuntime>["runtime"],
  ctx: FixtureContext,
  dispatchFromStatus = false,
) {
  const queue = createTelegramQueueStore<FixtureContext>();
  let piCalls = 0;
  const dispatch = createTelegramQueueDispatchRuntime<FixtureContext>({
    getQueuedItems: queue.getQueuedItems,
    setQueuedItems: queue.setQueuedItems,
    isCompactionInProgress: () => false,
    hasActiveTurn: () => false,
    hasDispatchPending: () => false,
    isIdle: () => true,
    hasPendingMessages: () => false,
    updateStatus: () => {},
    sendTextReply: async () => {},
    onPromptDispatchStart: () => {},
    sendUserMessage: () => {
      piCalls += 1;
    },
    onPromptDispatchFailure: () => {},
    claimPromptDispatch: runtime.claimTurnDispatch,
    onPromptDispatchFailedAfterClaim: runtime.markTurnDispatchFailed,
  });
  const enqueue = (value: TelegramInboundRecoveryUpdate) =>
    enqueueTelegramPromptTurnRuntime([value], {
      getQueuedItems: queue.getQueuedItems,
      setQueuedItems: queue.setQueuedItems,
      getFoldQueuedPromptsIntoHistory: () => false,
      setFoldQueuedPromptsIntoHistory: () => {},
      createTurn: async ([next]) => {
        if (!next?.message) throw new Error("fixture replay lacks message");
        return runtime.decorateTurn([next.message], createTurn(next));
      },
      updateStatus: () => {
        if (dispatchFromStatus) dispatch.dispatchNext(ctx);
      },
      dispatchNextQueuedTelegramTurn: () => {},
    });
  return {
    queue,
    enqueue,
    dispatch,
    getPiCalls: () => piCalls,
  };
}

async function runClassicCrash(rootPath: string, faultId: RecoveryInboundFaultId) {
  const composed = createRuntime({
    rootPath,
    target: { chatId: 7 },
    owner: {
      kind: "leader",
      ownerId: "process-owner",
      leaderEpoch: "process-epoch",
    },
    sessionGeneration: 1,
    selectedFault: faultId,
  });
  const queue = createQueueComposition(
    composed.runtime,
    composed.ctx,
    faultId === "IN-06",
  );
  const controller = new AbortController();
  let polls = 0;
  send({ type: "ready", mode: "classic-crash", faultId });
  await runTelegramPollLoop({
    ctx: composed.ctx,
    signal: controller.signal,
    config: { botToken: "123:process", lastUpdateId: 0 },
    deleteWebhook: async () => {},
    getUpdates: async () => {
      polls += 1;
      if (polls === 1) return [createUpdate(1, { chatId: 7 })];
      return new Promise(() => undefined);
    },
    persistConfig: async () => {},
    handleUpdate: (value) => queue.enqueue(value),
    durableInbound: composed.runtime,
    onErrorStatus: () => {},
    onStatusReset: () => {},
  });
}

async function runRehydrate(rootPath: string): Promise<void> {
  const composed = createRuntime({
    rootPath,
    target: { chatId: 7 },
    owner: {
      kind: "leader",
      ownerId: "process-owner",
      leaderEpoch: "process-epoch",
    },
    sessionGeneration: 1,
  });
  const queue = createQueueComposition(composed.runtime, composed.ctx);
  const prefix = await composed.runtime.initializeOffset({
    botToken: "123:process",
  });
  const before = composed.runtime.getRecoveryStatus();
  await composed.runtime.rehydrate(
    composed.ctx,
    (value) => queue.enqueue(value),
    (turn) => queue.queue.setQueuedItems([...queue.queue.getQueuedItems(), turn]),
  );
  const queueCount = queue.queue.getQueuedItems().length;
  queue.dispatch.dispatchNext(composed.ctx);
  send({
    type: "rehydrated",
    prefix: prefix ?? null,
    beforeCounts: before.counts,
    queueCount,
    piCalls: queue.getPiCalls(),
    remainingQueueCount: queue.queue.getQueuedItems().length,
  });
}

function parseForwardedUpdate(
  value: { update_id: number },
): TelegramInboundRecoveryUpdate {
  const result: TelegramInboundRecoveryUpdate = { update_id: value.update_id };
  if (!("message" in value) || !value.message || typeof value.message !== "object") {
    return result;
  }
  const message = value.message;
  if (!("message_id" in message) || typeof message.message_id !== "number") {
    return result;
  }
  if (!("chat" in message) || !message.chat || typeof message.chat !== "object") {
    return result;
  }
  if (!("id" in message.chat) || typeof message.chat.id !== "number") {
    return result;
  }
  result.message = {
    message_id: message.message_id,
    chat: { id: message.chat.id },
  };
  if (
    "message_thread_id" in message &&
    typeof message.message_thread_id === "number"
  ) {
    result.message.message_thread_id = message.message_thread_id;
  }
  if ("from" in message && message.from && typeof message.from === "object") {
    if (
      "id" in message.from &&
      typeof message.from.id === "number" &&
      "is_bot" in message.from &&
      typeof message.from.is_bot === "boolean"
    ) {
      result.message.from = {
        id: message.from.id,
        is_bot: message.from.is_bot,
      };
    }
  }
  return result;
}

async function runFollower(
  rootPath: string,
  socketPath: string,
  instanceId: string,
  threadId: number,
  sessionGeneration: number,
  registrationGeneration: string,
  ownerId: string,
  authSecret: string,
  selectedFault?: RecoveryInboundFaultId,
): Promise<void> {
  const target = { chatId: 7, threadId };
  const composed = createRuntime({
    rootPath,
    target,
    owner: {
      kind: "manual-follower",
      ownerId,
      registrationGeneration,
    },
    sessionGeneration,
    selectedFault,
  });
  const gate = new RecoveryProfileOperationGate();
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId,
    getAuthSecret: () => authSecret,
    getRegistrationGeneration: () => registrationGeneration,
    getContext: () => composed.ctx,
    getProfile: () => "default",
    getTarget: () => target,
    getSessionGeneration: () => sessionGeneration,
    manualFollowerOwnerId: ownerId,
    recoveryFence: {
      enter: (profile) => gate.enter(profile),
      beginFencing: (profile, generation) =>
        gate.beginFencing(profile, generation),
      awaitDrained: (profile, generation) =>
        gate.awaitDrained(profile, generation),
      resumeAfter: (profile, generation, resumeRuntime) =>
        gate.resumeAfter(profile, generation, resumeRuntime),
      suspendRuntime: () => {
        send({ type: "runtime-suspended", instanceId });
      },
      resumeRuntime: () => {
        send({ type: "runtime-resumed", instanceId });
      },
    },
    handleForwardedUpdate: (input, ctx) =>
      composed.runtime.admitForwardedUpdate(
        parseForwardedUpdate(input.update),
        ctx,
        input,
      ),
    handleForwardedCallback: () => {},
    handleForwardedReaction: () => {},
  });
  await receiver.start();
  send({
    type: "ready",
    mode: "follower",
    instanceId,
    registrationGeneration,
    target,
  });
  process.on("message", (message: unknown) => {
    if (
      !message ||
      typeof message !== "object" ||
      !("requestId" in message) ||
      typeof message.requestId !== "string" ||
      !("command" in message) ||
      typeof message.command !== "string"
    ) {
      return;
    }
    const request: ProcessRequest = {
      requestId: message.requestId,
      command:
        message.command === "status" ||
        message.command === "rehydrate" ||
        message.command === "reassign" ||
        message.command === "gate-status"
          ? message.command
          : "status",
    };
    void (async () => {
      if (request.command === "gate-status") {
        send({
          type: "response",
          requestId: request.requestId,
          gate: gate.getState("default"),
        });
        return;
      }
      const queue = createQueueComposition(composed.runtime, composed.ctx);
      let reassignmentState: string | undefined;
      if (request.command === "reassign") {
        const candidate = composed.runtime.getOrphanReassignmentCandidates()[0];
        if (!candidate) throw new Error("fixture has no orphan reassignment candidate");
        const reassignment = await composed.runtime.reassignForOperator(
          candidate.actionId,
          composed.ctx,
        );
        reassignmentState = reassignment.state;
      }
      if (request.command === "rehydrate" || request.command === "reassign") {
        await composed.runtime.rehydrate(
          composed.ctx,
          (value) => queue.enqueue(value),
          (turn) =>
            queue.queue.setQueuedItems([...queue.queue.getQueuedItems(), turn]),
        );
      }
      const status = composed.runtime.getRecoveryStatus();
      send({
        type: "response",
        requestId: request.requestId,
        counts: status.counts,
        itemActionIds: status.items.map((item) => item.actionId),
        orphanCount: composed.runtime.getOrphanReassignmentCandidates().length,
        queueCount: queue.queue.getQueuedItems().length,
        reassignmentState,
      });
    })().catch((error: unknown) => {
      send({
        type: "response",
        requestId: request.requestId,
        error: error instanceof Error ? error.message : String(error),
      });
    });
  });
}

async function runDowngradeCrash(
  rootPath: string,
  faultId: RecoveryInboundFaultId,
): Promise<void> {
  const composed = createRuntime({
    rootPath,
    target: { chatId: 7 },
    owner: {
      kind: "leader",
      ownerId: "downgrade-owner",
      leaderEpoch: "downgrade-epoch",
    },
    sessionGeneration: 1,
    selectedFault: faultId,
  });
  composed.runtime.getRecoveryStatus();
  send({ type: "ready", mode: "downgrade-crash", faultId });
  composed.runtime.beginDowngradeExclusive();
  composed.runtime.quarantineForDowngrade();
  send({ type: "unexpected-downgrade-return" });
}

async function main(): Promise<void> {
  const [mode, ...args] = process.argv.slice(2);
  if (mode === "classic-crash") {
    const [rootPath, faultText] = args;
    const faultId = parseFaultId(faultText);
    if (!rootPath || !faultId) throw new Error("classic-crash args missing");
    await runClassicCrash(rootPath, faultId);
    return;
  }
  if (mode === "rehydrate") {
    const [rootPath] = args;
    if (!rootPath) throw new Error("rehydrate root missing");
    await runRehydrate(rootPath);
    return;
  }
  if (mode === "follower") {
    const [
      rootPath,
      socketPath,
      instanceId,
      threadText,
      sessionText,
      registrationGeneration,
      ownerId,
      authSecret,
      selectedFault,
    ] = args;
    if (
      !rootPath ||
      !socketPath ||
      !instanceId ||
      !threadText ||
      !sessionText ||
      !registrationGeneration ||
      !ownerId ||
      !authSecret
    ) {
      throw new Error("follower args missing");
    }
    await runFollower(
      rootPath,
      socketPath,
      instanceId,
      Number(threadText),
      Number(sessionText),
      registrationGeneration,
      ownerId,
      authSecret,
      parseFaultId(selectedFault),
    );
    return;
  }
  if (mode === "downgrade-crash") {
    const [rootPath, faultText] = args;
    const faultId = parseFaultId(faultText);
    if (!rootPath || !faultId) throw new Error("downgrade-crash args missing");
    await runDowngradeCrash(rootPath, faultId);
    return;
  }
  throw new Error(`unknown fixture mode: ${mode ?? "missing"}`);
}

void main().catch((error: unknown) => {
  send({
    type: "fixture-error",
    error: error instanceof Error ? error.stack ?? error.message : String(error),
  });
  process.exitCode = 1;
});
