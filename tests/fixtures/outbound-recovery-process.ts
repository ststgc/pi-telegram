/*
 * Composed durable outbound process fixture
 * Zones: test infrastructure, recovery, outbound delivery, queue lifecycle
 * Runs production recovery, outbound worker/adapter, inbound rehydration, and
 * agent lifecycle seams behind IPC barriers so a parent can hard-kill state.
 */

import { readFile, readdir } from "node:fs/promises";

import {
  createInboundRecoveryRuntime,
  type TelegramInboundRecoveryUpdate,
} from "../../lib/inbound-recovery.ts";
import {
  commitTelegramDurableOutbound,
  createTelegramDurableOutboundUnitAdapter,
  createTelegramDurableOutboundWorker,
} from "../../lib/outbound-recovery.ts";
import {
  createTelegramDurableAgentLifecycleHooks,
  createTelegramQueueDispatchRuntime,
  createTelegramQueueStore,
  type PendingTelegramTurn,
} from "../../lib/queue.ts";
import {
  openRecoveryStore,
  parseRecoverySnapshot,
  type RecoveryIdentity,
  type RecoveryOutboundFaultId,
  type RecoveryStoreOpenOptions,
} from "../../lib/recovery.ts";
import {
  TelegramApiCommitUnknownError,
  TelegramApiHttpError,
} from "../../lib/telegram-api.ts";
import {
  InjectedReliabilityFaultError,
  ReliabilityFaultController,
  type ReliabilityFaultId,
} from "./reliability-faults.ts";

const PROFILE = "default";
const TARGET = { chatId: 700_007, threadId: 77 };
const IDENTITY: RecoveryIdentity = {
  profile: PROFILE,
  target: TARGET,
  owner: {
    kind: "manual-follower",
    ownerId: "outbound-process-owner",
    registrationGeneration: "outbound-process-registration",
  },
  sessionGeneration: 1,
};
const blockingWord = new Int32Array(new SharedArrayBuffer(4));

interface FixtureContext {
  generation: number;
}

interface RemoteResponse {
  type: "remote-response";
  requestId: string;
  outcome: "success" | "known-not-committed" | "commit-unknown";
  messageId?: number;
}

function send(message: Record<string, unknown>): void {
  if (!process.send) throw new Error("fixture requires a Node IPC channel");
  process.send(message);
}

function parseFaultId(value: string | undefined): ReliabilityFaultId | undefined {
  switch (value) {
    case "OUT-01":
    case "OUT-02":
    case "OUT-03":
    case "OUT-04":
    case "OUT-05":
    case "OUT-06":
      return value;
    default:
      return undefined;
  }
}

function identitiesEqual(left: RecoveryIdentity, right: RecoveryIdentity): boolean {
  return left.profile === right.profile &&
    left.target.chatId === right.target.chatId &&
    left.target.threadId === right.target.threadId &&
    left.sessionGeneration === right.sessionGeneration &&
    left.owner.kind === "manual-follower" &&
    right.owner.kind === "manual-follower" &&
    left.owner.ownerId === right.owner.ownerId &&
    left.owner.registrationGeneration === right.owner.registrationGeneration;
}

function injectAndBlock(
  controller: ReliabilityFaultController,
  faultId: ReliabilityFaultId,
): void {
  try {
    controller.hit(faultId);
  } catch (error) {
    if (!(error instanceof InjectedReliabilityFaultError)) throw error;
    controller.assertInjected();
    send({
      type: "fault-boundary",
      faultId,
      faultAsserted: true,
      seed: controller.seed,
    });
    Atomics.wait(blockingWord, 0, 0);
  }
}

function createRuntime(
  rootPath: string,
  fault?: (faultId: RecoveryOutboundFaultId) => void,
) {
  const ctx: FixtureContext = { generation: IDENTITY.sessionGeneration };
  const openStore = (options: RecoveryStoreOpenOptions) =>
    openRecoveryStore({
      ...options,
      rootPath,
      ...(fault ? { fault } : {}),
    });
  const runtime = createInboundRecoveryRuntime<
    TelegramInboundRecoveryUpdate,
    FixtureContext
  >({
    getProfile: () => PROFILE,
    getAllowedUserId: () => 7,
    getCurrentInstanceId: () => IDENTITY.owner.ownerId,
    getSessionGeneration: () => IDENTITY.sessionGeneration,
    isSessionActive: (candidate, generation) =>
      candidate === ctx && generation === IDENTITY.sessionGeneration,
    resolveCurrentIdentity: (target) =>
      target.chatId === TARGET.chatId && target.threadId === TARGET.threadId
        ? IDENTITY
        : undefined,
    resolveOperatorIdentity: () => IDENTITY,
    isIdentityAuthenticated: (candidate) => identitiesEqual(candidate, IDENTITY),
    planResponsibility: () => ({ kind: "local", target: TARGET }),
    openStore,
  });
  return { ctx, runtime };
}

function createUpdate(updateId: number): TelegramInboundRecoveryUpdate {
  return {
    update_id: updateId,
    message: {
      message_id: updateId * 10,
      message_thread_id: TARGET.threadId,
      chat: { id: TARGET.chatId },
      from: { id: 7, is_bot: false },
    },
  };
}

function createTurn(
  updateId: number,
  attachmentPath?: string,
): PendingTelegramTurn {
  const messageId = updateId * 10;
  return {
    kind: "prompt",
    chatId: TARGET.chatId,
    target: TARGET,
    replyToMessageId: messageId,
    sourceMessageIds: [messageId],
    queueOrder: updateId,
    queueLane: "default",
    laneOrder: updateId,
    queuedAttachments: attachmentPath
      ? [{ path: attachmentPath, fileName: "private-result.png" }]
      : [],
    content: [{ type: "text", text: `[telegram|thread:test] turn ${updateId}` }],
    historyText: `turn ${updateId}`,
    statusSummary: `turn ${updateId}`,
  };
}

async function requestRemoteMutation(): Promise<{ message_id: number }> {
  const requestId = `remote:${process.pid}:${Date.now()}`;
  return new Promise((resolve, reject) => {
    const onMessage = (message: unknown) => {
      if (!message || typeof message !== "object") return;
      const candidate = message as Partial<RemoteResponse>;
      if (candidate.type !== "remote-response" || candidate.requestId !== requestId) {
        return;
      }
      process.off("message", onMessage);
      if (candidate.outcome === "known-not-committed") {
        reject(new TelegramApiHttpError("fixture known rejection", 400, undefined));
        return;
      }
      if (candidate.outcome === "commit-unknown") {
        reject(
          new TelegramApiCommitUnknownError(
            "sendRichMessage",
            new Error("fixture response lost"),
            "response-lost",
          ),
        );
        return;
      }
      if (
        candidate.outcome !== "success" ||
        typeof candidate.messageId !== "number"
      ) {
        reject(new Error("fixture remote response is malformed"));
        return;
      }
      resolve({ message_id: candidate.messageId });
    };
    process.on("message", onMessage);
    send({ type: "remote-call", requestId });
  });
}

function createAdapter() {
  return createTelegramDurableOutboundUnitAdapter({
    gate: { canStart: () => true, isActive: () => true },
    sendMessage: requestRemoteMutation,
    sendRichMessage: requestRemoteMutation,
    sendMultipartBytes: requestRemoteMutation,
    answerGuestQuery: async () => {
      await requestRemoteMutation();
    },
    deleteMessage: async () => {
      await requestRemoteMutation();
    },
  });
}

async function snapshotEvidence(
  runtime: ReturnType<typeof createRuntime>["runtime"],
): Promise<Record<string, unknown>> {
  const store = runtime.getOutboundStore();
  const snapshot = parseRecoverySnapshot(await readFile(store.snapshotPath, "utf8"));
  const outbound = snapshot.outbound[0];
  const status = store.getStatus();
  const spoolFiles = await readdir(store.spoolDirectory);
  return {
    counts: status.counts,
    statusJson: JSON.stringify(status),
    outboundState: outbound?.state ?? null,
    receiptCount: outbound?.receipts.length ?? 0,
    outboundSpoolRefCount: outbound?.spoolRefs.length ?? 0,
    outboundSpoolBytes: outbound?.spoolRefs.reduce(
      (total, reference) => total + reference.byteLength,
      0,
    ) ?? 0,
    spoolFileCount: spoolFiles.length,
    quotaUsedBytes: status.quota.usedBytes,
  };
}

async function admitTurn(
  runtime: ReturnType<typeof createRuntime>["runtime"],
  ctx: FixtureContext,
  updateId: number,
  attachmentPath?: string,
): Promise<PendingTelegramTurn> {
  const update = createUpdate(updateId);
  await runtime.admitUpdate(update, ctx);
  return runtime.decorateTurn(
    [update.message!],
    createTurn(updateId, attachmentPath),
  );
}

async function runCrash(
  rootPath: string,
  attachmentPath: string,
  faultId: ReliabilityFaultId,
  seed: string,
): Promise<void> {
  const controller = new ReliabilityFaultController(faultId, { seed });
  let out03FaultObservations = 0;
  const composed = createRuntime(rootPath, (candidate) => {
    if (candidate === "OUT-03") {
      out03FaultObservations += 1;
      if (out03FaultObservations === 1) return;
    }
    injectAndBlock(controller, candidate);
  });
  const activeTurn = await admitTurn(
    composed.runtime,
    composed.ctx,
    1,
    attachmentPath,
  );
  if (!composed.runtime.claimTurnDispatch(activeTurn)) {
    throw new Error("fixture could not claim the active turn");
  }
  if (faultId === "OUT-06") {
    await admitTurn(composed.runtime, composed.ctx, 2);
  }

  let currentTurn: PendingTelegramTurn | undefined = activeTurn;
  const worker = createTelegramDurableOutboundWorker({
    getStore: composed.runtime.getOutboundStore,
    operationGate: composed.runtime.operationGate,
    adapter: createAdapter(),
    onTerminal: () => {
      send({ type: "unexpected-terminal-before-kill", faultId });
    },
  });
  const hooks = createTelegramDurableAgentLifecycleHooks<
    PendingTelegramTurn,
    FixtureContext,
    string
  >({
    setAbortHandler: () => {},
    getQueuedItems: () => [],
    hasPendingDispatch: () => false,
    hasActiveTurn: () => currentTurn !== undefined,
    resetToolExecutions: () => {},
    resetPendingModelSwitch: () => {},
    setQueuedItems: () => {},
    clearDispatchPending: () => {},
    setFoldQueuedPromptsIntoHistory: () => {},
    setActiveTurn: (turn) => {
      currentTurn = turn;
    },
    createPreviewState: () => {},
    startTypingLoop: () => {},
    getActiveTurn: () => currentTurn,
    extractAssistant: () => ({
      text: `PRIVATE-ANSWER-${seed}`,
      stopReason: "stop",
    }),
    resetRuntimeState: () => {
      currentTurn = undefined;
    },
    isSessionActive: (candidate) => candidate === composed.ctx,
    updateStatus: () => {},
    dispatchNextQueuedTelegramTurn: () => {},
    requestDeferredDispatchNextQueuedTelegramTurn: () => {},
    async handoffActiveTurn(turn, assistant) {
      if (faultId === "OUT-01") injectAndBlock(controller, "OUT-01");
      const recovery = turn.recovery;
      if (!recovery) throw new Error("fixture turn lacks recovery identity");
      const claim = composed.runtime.getTurnOutboundClaim(turn);
      const store = composed.runtime.getOutboundStore();
      const committed = await commitTelegramDurableOutbound({
        intentId: `final-v1:${recovery.turnId}`,
        turnId: recovery.turnId,
        sourceInboundRecordIds: recovery.recordIds,
        claim,
        replyToMessageId: turn.replyToMessageId,
        renderingMode: "rich",
        finalMarkdown: assistant.text ?? "",
        queuedAttachments: turn.queuedAttachments,
      }, {
        store,
        transformReply: async (text) => text,
      });
      const pending = store.activateOutbound(committed.record.recordId, claim);
      worker.register(pending, claim);
      composed.runtime.releaseTurnAfterOutboundHandoff(turn);
      return {
        startDelivery() {
          worker.schedule(pending.recordId);
        },
      };
    },
    completeTurnWithoutDelivery: composed.runtime.completeTurn,
    markTurnExecutionUncertain: composed.runtime.markTurnDispatchFailed,
    getActiveToolExecutions: () => 0,
    setActiveToolExecutions: () => {},
    triggerPendingModelSwitchAbort: () => {},
  });

  setInterval(() => {}, 1_000);
  send({ type: "ready", mode: "crash", faultId, seed });
  await hooks.onAgentEnd({ messages: ["semantic answer"] }, composed.ctx);
  await new Promise(() => undefined);
}

async function waitForContinue(): Promise<void> {
  await new Promise<void>((resolve) => {
    const onMessage = (message: unknown) => {
      if (
        !message ||
        typeof message !== "object" ||
        !("type" in message) ||
        message.type !== "continue"
      ) {
        return;
      }
      process.off("message", onMessage);
      resolve();
    };
    process.on("message", onMessage);
  });
}

async function runReopen(
  rootPath: string,
  faultId: ReliabilityFaultId,
  seed: string,
): Promise<void> {
  const composed = createRuntime(rootPath);
  const initialEvidence = await snapshotEvidence(composed.runtime);
  send({
    type: "reopened",
    faultId,
    seed,
    evidence: initialEvidence,
  });
  await waitForContinue();

  let terminalNotifications = 0;
  let resolveTerminal: (() => void) | undefined;
  const terminal = new Promise<void>((resolve) => {
    resolveTerminal = resolve;
  });
  const worker = createTelegramDurableOutboundWorker({
    getStore: composed.runtime.getOutboundStore,
    operationGate: composed.runtime.operationGate,
    adapter: createAdapter(),
    onTerminal: () => {
      terminalNotifications += 1;
      resolveTerminal?.();
    },
    startSuspended: true,
  });
  await composed.runtime.rehydrateOutbound(
    composed.ctx,
    (item, claim) => {
      worker.register(item.record, claim);
      worker.schedule(item.record.recordId);
    },
  );

  const queue = createTelegramQueueStore<FixtureContext>();
  await composed.runtime.rehydrate(
    composed.ctx,
    async () => ({ kind: "completed", reason: "fixture-unexpected-update" }),
    (turn) => queue.setQueuedItems([...queue.getQueuedItems(), turn]),
  );
  let nextTurnDispatches = 0;
  let dispatchPending = false;
  const dispatch = createTelegramQueueDispatchRuntime<FixtureContext>({
    getQueuedItems: queue.getQueuedItems,
    setQueuedItems: queue.setQueuedItems,
    isCompactionInProgress: () => false,
    hasActiveTurn: () => false,
    hasDispatchPending: () => dispatchPending,
    isIdle: () => true,
    hasPendingMessages: () => false,
    updateStatus: () => {},
    sendTextReply: async () => {},
    onPromptDispatchStart: () => {
      dispatchPending = true;
    },
    sendUserMessage: () => {
      nextTurnDispatches += 1;
    },
    onPromptDispatchFailure: () => {},
    claimPromptDispatch: composed.runtime.claimTurnDispatch,
    onPromptDispatchFailedAfterClaim: composed.runtime.markTurnDispatchFailed,
  });

  await worker.resume();
  if (faultId === "OUT-02" || faultId === "OUT-03") {
    await Promise.race([
      terminal,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("outbound recovery timed out")), 5_000)
      ),
    ]);
  }
  if (faultId === "OUT-06") {
    dispatch.dispatchNext(composed.ctx);
    dispatch.dispatchNext(composed.ctx);
  }
  await worker.suspend();
  const finalEvidence = await snapshotEvidence(composed.runtime);
  send({
    type: "final",
    faultId,
    seed,
    evidence: finalEvidence,
    terminalNotifications,
    nextTurnDispatches,
    remainingQueueCount: queue.getQueuedItems().length,
  });
  setInterval(() => {}, 1_000);
}

async function main(): Promise<void> {
  const [mode, rootPath, attachmentPath, faultText, seed] = process.argv.slice(2);
  const faultId = parseFaultId(faultText);
  if (!mode || !rootPath || !attachmentPath || !faultId || !seed) {
    throw new Error("outbound process fixture args missing");
  }
  if (mode === "crash") {
    await runCrash(rootPath, attachmentPath, faultId, seed);
    return;
  }
  if (mode === "reopen") {
    await runReopen(rootPath, faultId, seed);
    return;
  }
  throw new Error(`unknown outbound fixture mode: ${mode}`);
}

void main().catch((error: unknown) => {
  send({
    type: "fixture-error",
    error: error instanceof Error ? error.stack ?? error.message : String(error),
  });
  process.exitCode = 1;
});
