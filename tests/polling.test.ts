/**
 * Regression tests for the Telegram polling runtime domain
 * Covers polling request helpers, stop conditions, and the long-poll loop runtime in one suite
 */

import assert from "node:assert/strict";
import test from "node:test";

import {
  applyTelegramThreadCapability,
  buildTelegramInitialSyncRequest,
  buildTelegramLongPollRequest,
  createTelegramPollingActivityReader,
  createTelegramPollingController,
  createTelegramPollingControllerRuntime,
  createTelegramPollingControllerState,
  createTelegramPollingTerminalLeaseHandler,
  createTelegramPollLoopRunner,
  createTelegramThreadAwarePollingPorts,
  createTelegramThreadCapabilityStateRuntime,
  createTelegramThreadTargetObservationBinding,
  getLatestTelegramUpdateId,
  isTelegramGetUpdatesConflictError,
  isTelegramPollingControllerActive,
  runTelegramPollLoop,
  shouldStartTelegramPolling,
  shouldStopTelegramPolling,
  sleepTelegramPollingRetry,
  startTelegramPollingRuntime,
  stopTelegramPollingRuntime,
  TELEGRAM_ALLOWED_UPDATES,
} from "../lib/polling.ts";

const TEST_CONTEXT = "ctx";

test("Polling helpers build the initial sync request", () => {
  assert.deepEqual(buildTelegramInitialSyncRequest(), {
    offset: -1,
    limit: 1,
    timeout: 0,
  });
});

test("Polling helpers build long-poll requests with and without lastUpdateId", () => {
  assert.deepEqual(buildTelegramLongPollRequest(), {
    offset: undefined,
    limit: 10,
    timeout: 30,
    allowed_updates: TELEGRAM_ALLOWED_UPDATES,
  });
  assert.deepEqual(buildTelegramLongPollRequest(41), {
    offset: 42,
    limit: 10,
    timeout: 30,
    allowed_updates: TELEGRAM_ALLOWED_UPDATES,
  });
});

test("Polling helpers extract the latest update id", () => {
  assert.equal(getLatestTelegramUpdateId([]), undefined);
  assert.equal(
    getLatestTelegramUpdateId([{ update_id: 1 }, { update_id: 7 }]),
    7,
  );
});

test("Thread target observation binding supports late runtime composition", async () => {
  const events: string[] = [];
  const binding = createTelegramThreadTargetObservationBinding<string>();

  await binding.handle("before");
  binding.set(async (ctx) => {
    events.push(ctx);
  });
  await binding.handle("after");

  assert.deepEqual(events, ["after"]);
});

test("Thread capability state runtime owns transition flags", () => {
  const state = createTelegramThreadCapabilityStateRuntime();

  assert.equal(state.isBusPollingStarted(), false);
  assert.equal(state.isTopicModeUnavailable(), false);
  assert.equal(state.shouldForceFreshLeaderThread(), false);

  state.setBusPollingStarted(true);
  state.setTopicModeUnavailable(true);
  state.setForceFreshLeaderThread(true);

  assert.equal(state.isBusPollingStarted(), true);
  assert.equal(state.isTopicModeUnavailable(), true);
  assert.equal(state.shouldForceFreshLeaderThread(), true);
});

test("Thread-aware polling blocks follower takeover during Threaded Mode downgrade", async () => {
  const events: Array<{ category: string; details?: Record<string, unknown> }> = [];
  const callApi = async <TResponse,>(): Promise<TResponse> => ({}) as TResponse;
  const store = {
    async load() {},
    async persist() {},
    getBotState() {
      return { threadMode: "disabled" as const };
    },
    setBotState() {},
    list() {
      return [
        {
          status: "active",
          target: { chatId: 42, threadId: 7 },
        },
      ];
    },
  };
  const ports = createTelegramThreadAwarePollingPorts({
    getAllowedUserId: () => 42,
    callApi,
    topicTargetStore: store,
    isBusConfigured: () => true,
    isBusRuntimeEnabled: () => false,
    isTopicModeUnavailableError: () => false,
    getPollingStartedWithTelegramBus: () => false,
    setPollingStartedWithTelegramBus() {},
    setForceFreshLeaderThreadOnNextStart() {},
    setTopicModeUnavailable() {},
    startClassicPolling() {},
    async stopClassicPolling() {},
    async startBusLeaderPolling() {},
    async stopBusLeaderPolling() {},
    startLeaderHealth() {},
    stopLeaderHealth() {},
    registerFollowerWithLeader: async () => true,
    stopFollowerRegistration() {},
    recordEvent(category, _error, details) {
      events.push({ category, details });
    },
  });

  await assert.rejects(
    ports.registerFollowerWithOwner?.(TEST_CONTEXT, { pid: 1 }),
    /current leader remains the classic polling owner/u,
  );
  assert.deepEqual(events, [
    {
      category: "bus",
      details: {
        phase: "follower-register-thread-mode-disabled",
        reason: "active-thread-bindings-present",
      },
    },
  ]);
});

test("Thread capability downgrade retries classic restore after failure", async () => {
  let state: {
    threadMode?: "enabled" | "disabled" | "unknown";
    updatedAtMs?: number;
    lastReconcileAction?: string;
  } = {
    threadMode: "enabled",
    lastReconcileAction: "capability-monitor-enabled",
  };
  let pollingStartedWithBus = true;
  let classicStarts = 0;
  let persisted = 0;
  const events: Array<{ category: string; details?: Record<string, unknown> }> = [];
  const store = {
    async load() {},
    async persist() {
      persisted += 1;
    },
    getBotState() {
      return state;
    },
    setBotState(next: typeof state) {
      state = { ...state, ...next };
    },
    list() {
      return [
        {
          status: "active",
          target: { chatId: 42, threadId: 7 },
        },
      ];
    },
  };
  const deps = {
    getAllowedUserId: () => 42,
    callApi: async <TResponse,>(): Promise<TResponse> => ({}) as TResponse,
    topicTargetStore: store,
    isBusConfigured: () => true,
    ownsLock: () => true,
    getPollingStartedWithTelegramBus: () => pollingStartedWithBus,
    setPollingStartedWithTelegramBus(started: boolean) {
      pollingStartedWithBus = started;
    },
    setTopicModeUnavailable() {},
    stopFollowerRegistration() {},
    startClassicPolling() {
      classicStarts += 1;
      if (classicStarts === 1) throw new Error("classic unavailable");
    },
    async stopClassicPolling() {},
    async startBusPolling() {},
    async stopBusPolling() {},
    startLeaderHealth() {},
    stopLeaderHealth() {},
    isTopicModeUnavailableError: () => false,
    updateStatus() {},
    recordEvent(category: string, _error: unknown, details?: Record<string, unknown>) {
      events.push({ category, details });
    },
  };

  await applyTelegramThreadCapability(
    TEST_CONTEXT,
    false,
    "capability-monitor-disabled-confirmed",
    deps,
  );
  assert.equal(classicStarts, 1);
  assert.equal(
    state.lastReconcileAction,
    "capability-monitor-disabled-confirmed-classic-restore-failed",
  );
  assert.equal(pollingStartedWithBus, false);

  await applyTelegramThreadCapability(
    TEST_CONTEXT,
    false,
    "capability-monitor-disabled-confirmed",
    deps,
  );
  assert.equal(classicStarts, 2);
  assert.equal(state.lastReconcileAction, "capability-monitor-disabled-confirmed");
  assert.equal(persisted >= 3, true);
  assert.equal(events[0].details?.phase, "capability-monitor-disabled-confirmed-classic-restore");
});

test("Thread-aware polling still allows classic takeover path without thread bindings", async () => {
  const callApi = async <TResponse,>(): Promise<TResponse> => ({}) as TResponse;
  const store = {
    async load() {},
    async persist() {},
    getBotState() {
      return { threadMode: "disabled" as const };
    },
    setBotState() {},
    list() {
      return [];
    },
  };
  const ports = createTelegramThreadAwarePollingPorts({
    getAllowedUserId: () => 42,
    callApi,
    topicTargetStore: store,
    isBusConfigured: () => true,
    isBusRuntimeEnabled: () => false,
    isTopicModeUnavailableError: () => false,
    getPollingStartedWithTelegramBus: () => false,
    setPollingStartedWithTelegramBus() {},
    setForceFreshLeaderThreadOnNextStart() {},
    setTopicModeUnavailable() {},
    startClassicPolling() {},
    async stopClassicPolling() {},
    async startBusLeaderPolling() {},
    async stopBusLeaderPolling() {},
    startLeaderHealth() {},
    stopLeaderHealth() {},
    registerFollowerWithLeader: async () => true,
    stopFollowerRegistration() {},
    recordEvent() {},
  });

  assert.equal(
    await ports.registerFollowerWithOwner?.(TEST_CONTEXT, { pid: 1 }),
    undefined,
  );
});

test("Polling helpers start only when a bot token exists and polling is idle", () => {
  assert.equal(
    shouldStartTelegramPolling({
      hasBotToken: true,
      hasPollingPromise: false,
    }),
    true,
  );
  assert.equal(
    shouldStartTelegramPolling({
      hasBotToken: false,
      hasPollingPromise: false,
    }),
    false,
  );
  assert.equal(
    shouldStartTelegramPolling({
      hasBotToken: true,
      hasPollingPromise: true,
    }),
    false,
  );
});

test("Polling runtime starts and stops polling through state ports", async () => {
  const events: string[] = [];
  let pollingPromise: Promise<void> | undefined;
  let pollingController: AbortController | undefined;
  let finishPollLoop: (() => void) | undefined;
  const deps = {
    hasBotToken: () => true,
    getPollingPromise: () => pollingPromise,
    setPollingPromise: (promise: Promise<void> | undefined) => {
      pollingPromise = promise;
      events.push(`promise:${promise ? "set" : "clear"}`);
    },
    getPollingController: () => pollingController,
    setPollingController: (controller: AbortController | undefined) => {
      pollingController = controller;
      events.push(`controller:${controller ? "set" : "clear"}`);
    },
    stopTypingLoop: () => {
      events.push("typing:stop");
    },
    runPollLoop: async (_ctx: string, signal: AbortSignal) => {
      events.push(`run:${signal.aborted}`);
      await new Promise<void>((resolve) => {
        finishPollLoop = resolve;
      });
    },
    updateStatus: (ctx: string) => {
      events.push(`status:${ctx}`);
    },
  };
  startTelegramPollingRuntime("ctx", deps);
  assert.equal(!!pollingPromise, true);
  assert.equal(!!pollingController, true);
  const stopPromise = stopTelegramPollingRuntime(deps);
  assert.equal(pollingController?.signal.aborted, true);
  assert.equal(!!pollingController, true);
  finishPollLoop?.();
  await stopPromise;
  assert.deepEqual(events, [
    "controller:set",
    "run:false",
    "promise:set",
    "status:ctx",
    "typing:stop",
    "promise:clear",
    "controller:clear",
    "status:ctx",
  ]);
});

test("Polling runtime still aborts and settles when typing cleanup fails", async () => {
  const events: string[] = [];
  let pollingPromise: Promise<void> | undefined;
  let pollingController: AbortController | undefined;
  let finishPollLoop: (() => void) | undefined;
  const deps = {
    hasBotToken: () => true,
    getPollingPromise: () => pollingPromise,
    setPollingPromise: (promise: Promise<void> | undefined) => {
      pollingPromise = promise;
      events.push(`promise:${promise ? "set" : "clear"}`);
    },
    getPollingController: () => pollingController,
    setPollingController: (controller: AbortController | undefined) => {
      pollingController = controller;
      events.push(`controller:${controller ? "set" : "clear"}`);
    },
    stopTypingLoop: () => {
      events.push("typing:throw");
      throw new Error("typing cleanup failed");
    },
    runPollLoop: async (_ctx: string, signal: AbortSignal) => {
      await new Promise<void>((resolve) => {
        finishPollLoop = () => {
          events.push(`run-finish:${signal.aborted}`);
          resolve();
        };
      });
    },
    updateStatus: () => {},
    recordRuntimeEvent: (
      category: string,
      error: unknown,
      details?: Record<string, unknown>,
    ) => {
      events.push(
        `${category}:${error instanceof Error ? error.message : String(error)}:${details?.phase}`,
      );
    },
  };
  startTelegramPollingRuntime("ctx", deps);
  const stopPromise = stopTelegramPollingRuntime(deps);
  assert.equal(pollingController?.signal.aborted, true);
  finishPollLoop?.();
  await stopPromise;
  assert.deepEqual(events, [
    "controller:set",
    "promise:set",
    "typing:throw",
    "polling:typing cleanup failed:typing-stop",
    "run-finish:true",
    "promise:clear",
    "controller:clear",
  ]);
});

test("Polling runtime ignores stale-context status failures during cleanup", async () => {
  let pollingPromise: Promise<void> | undefined;
  let pollingController: AbortController | undefined;
  let statusCalls = 0;
  const runtimeEvents: string[] = [];
  const deps = {
    hasBotToken: () => true,
    getPollingPromise: () => pollingPromise,
    setPollingPromise: (promise: Promise<void> | undefined) => {
      pollingPromise = promise;
    },
    getPollingController: () => pollingController,
    setPollingController: (controller: AbortController | undefined) => {
      pollingController = controller;
    },
    stopTypingLoop: () => {},
    runPollLoop: async () => {},
    updateStatus: () => {
      statusCalls += 1;
      if (statusCalls > 1) throw new Error("stale ctx");
    },
    recordRuntimeEvent: (
      category: string,
      error: unknown,
      details?: Record<string, unknown>,
    ) => {
      const message = error instanceof Error ? error.message : String(error);
      runtimeEvents.push(`${category}:${message}:${details?.phase}`);
    },
  };
  startTelegramPollingRuntime("ctx", deps);
  await pollingPromise;
  assert.equal(statusCalls, 2);
  assert.equal(pollingPromise, undefined);
  assert.equal(pollingController, undefined);
  assert.deepEqual(runtimeEvents, ["polling:stale ctx:status-update"]);
});

test("Polling runtime ignores stale-context status failures during start", () => {
  let pollingPromise: Promise<void> | undefined;
  let pollingController: AbortController | undefined;
  const runtimeEvents: string[] = [];
  const deps = {
    hasBotToken: () => true,
    getPollingPromise: () => pollingPromise,
    setPollingPromise: (promise: Promise<void> | undefined) => {
      pollingPromise = promise;
    },
    getPollingController: () => pollingController,
    setPollingController: (controller: AbortController | undefined) => {
      pollingController = controller;
    },
    stopTypingLoop: () => {},
    runPollLoop: async () => {},
    updateStatus: () => {
      throw new Error("stale ctx");
    },
    recordRuntimeEvent: (
      category: string,
      error: unknown,
      details?: Record<string, unknown>,
    ) => {
      const message = error instanceof Error ? error.message : String(error);
      runtimeEvents.push(`${category}:${message}:${details?.phase}`);
    },
  };

  assert.doesNotThrow(() => startTelegramPollingRuntime("ctx", deps));
  assert.equal(!!pollingPromise, true);
  assert.equal(!!pollingController, true);
  assert.deepEqual(runtimeEvents, ["polling:stale ctx:status-update"]);
});

test("Polling controller owns polling promise and abort-controller state", async () => {
  const events: string[] = [];
  let finishPollLoop: (() => void) | undefined;
  const state = createTelegramPollingControllerState();
  const isPollingActive = createTelegramPollingActivityReader(state);
  const controller = createTelegramPollingController({
    state,
    hasBotToken: () => true,
    stopTypingLoop: () => {
      events.push("typing:stop");
    },
    runPollLoop: async (_ctx: string, signal: AbortSignal) => {
      events.push(`run:${signal.aborted}`);
      await new Promise<void>((resolve) => {
        finishPollLoop = resolve;
      });
    },
    updateStatus: (ctx: string) => {
      events.push(`status:${ctx}`);
    },
  });
  controller.start("ctx");
  assert.equal(controller.isActive(), true);
  assert.equal(isTelegramPollingControllerActive(state), true);
  assert.equal(isPollingActive(), true);
  controller.start("ctx");
  const stopPromise = controller.stop();
  finishPollLoop?.();
  await stopPromise;
  assert.equal(controller.isActive(), false);
  assert.equal(isTelegramPollingControllerActive(state), false);
  assert.equal(isPollingActive(), false);
  assert.deepEqual(events, [
    "run:false",
    "status:ctx",
    "typing:stop",
    "status:ctx",
  ]);
});

test("Polling controller runtime binds loop runner and controller state", async () => {
  const events: string[] = [];
  const state = createTelegramPollingControllerState();
  const controller = createTelegramPollingControllerRuntime({
    state,
    getConfig: () => ({ botToken: "123:abc" }),
    hasBotToken: () => true,
    deleteWebhook: async () => {
      events.push("deleteWebhook");
    },
    getUpdates: async () => {
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async () => {
      events.push("persist");
    },
    handleUpdate: async () => {
      events.push("handle");
    },
    stopTypingLoop: () => {
      events.push("typing:stop");
    },
    updateStatus: (_ctx: string, message?: string) => {
      events.push(`status:${message ?? "ok"}`);
    },
  });
  controller.start("ctx");
  assert.equal(controller.isActive(), true);
  await controller.stop();
  assert.equal(controller.isActive(), false);
  assert.deepEqual(events, [
    "deleteWebhook",
    "status:ok",
    "typing:stop",
    "status:ok",
  ]);
});

test("Polling helpers stop only for abort conditions", () => {
  assert.equal(shouldStopTelegramPolling(true, new Error("ignored")), true);
  assert.equal(
    shouldStopTelegramPolling(false, new DOMException("aborted", "AbortError")),
    true,
  );
  assert.equal(shouldStopTelegramPolling(false, new Error("network")), false);
});

test("Poll loop runner binds config, status, and transport ports", async () => {
  const config: { botToken: string; lastUpdateId?: number } = {
    botToken: "123:abc",
    lastUpdateId: 5,
  };
  const events: string[] = [];
  let calls = 0;
  const runPollLoop = createTelegramPollLoopRunner({
    getConfig: () => config,
    deleteWebhook: async () => {
      events.push("deleteWebhook");
    },
    getUpdates: async () => {
      calls += 1;
      if (calls === 1) return [{ update_id: 6 }];
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async (next) => {
      events.push(`persist:${next.lastUpdateId}`);
    },
    handleUpdate: async (update, ctx: string) => {
      events.push(`handle:${ctx}:${update.update_id}`);
    },
    updateStatus: (ctx, message) => {
      events.push(`status:${ctx}:${message ?? "ok"}`);
    },
    sleep: async () => {
      events.push("sleep");
    },
  });
  await runPollLoop("ctx", new AbortController().signal);
  assert.deepEqual(events, ["deleteWebhook", "handle:ctx:6", "persist:6"]);
});

test("Poll loop runner surfaces non-conflict API failures to its supervisor", async () => {
  const events: string[] = [];
  const runPollLoop = createTelegramPollLoopRunner({
    getConfig: () => ({ botToken: "123:abc", lastUpdateId: 1 }),
    deleteWebhook: async () => {},
    getUpdates: async () => { throw new Error("network down"); },
    persistConfig: async () => {},
    handleUpdate: async () => {},
    updateStatus: (_ctx: string, message?: string) => {
      events.push(`status:${message ?? "ok"}`);
    },
    sleep: async (ms) => {
      events.push(`sleep:${ms}`);
    },
  });
  await assert.rejects(
    () => runPollLoop("ctx", new AbortController().signal),
    /network down/u,
  );
  assert.deepEqual(events, []);
});

test("Poll loop initializes lastUpdateId and processes prepared update batches", async () => {
  const handled: number[] = [];
  const lifecycle: string[] = [];
  const config: { botToken: string; lastUpdateId?: number } = {
    botToken: "123:abc",
  };
  let getUpdatesCalls = 0;
  let persistCount = 0;
  const signal = new AbortController().signal;
  await runTelegramPollLoop({
    ctx: TEST_CONTEXT,
    signal,
    config,
    deleteWebhook: async () => {},
    getUpdates: async () => {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return [{ update_id: 5 }];
      }
      if (getUpdatesCalls === 2) {
        return [{ update_id: 6 }, { update_id: 7 }];
      }
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async () => {
      persistCount += 1;
    },
    handleUpdate: async (update) => {
      lifecycle.push(`handle:${update.update_id}`);
      handled.push(update.update_id);
    },
    prepareUpdateBatch: (updates) => {
      lifecycle.push(`batch:${updates.map((update) => update.update_id).join(",")}`);
    },
    onErrorStatus: () => {},
    onStatusReset: () => {},
    sleep: async () => {},
  });
  assert.equal(config.lastUpdateId, 7);
  assert.deepEqual(handled, [6, 7]);
  assert.deepEqual(lifecycle, ["batch:6,7", "handle:6", "handle:7"]);
  assert.equal(persistCount, 3);
});

test("Poll loop persists long-poll offsets only after handling updates", async () => {
  const config = { botToken: "123:abc", lastUpdateId: 5 };
  const handled: number[] = [];
  const persisted: number[] = [];
  let calls = 0;
  await runTelegramPollLoop({
    ctx: TEST_CONTEXT,
    signal: new AbortController().signal,
    config,
    deleteWebhook: async () => {},
    getUpdates: async () => {
      calls += 1;
      if (calls === 1) return [{ update_id: 6 }];
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async (next) => {
      persisted.push(next.lastUpdateId ?? -1);
    },
    handleUpdate: async (update) => {
      handled.push(update.update_id);
      throw new Error("handler failed");
    },
    onErrorStatus: () => {},
    onStatusReset: () => {},
    sleep: async () => {},
  });
  assert.deepEqual(handled, [6]);
  assert.equal(config.lastUpdateId, 5);
  assert.deepEqual(persisted, []);
});

test("Poll loop does not readmit an update after offset persistence restarts", async () => {
  const config = { botToken: "123:abc", lastUpdateId: 5 };
  const handled: number[] = [];
  let getUpdatesCalls = 0;
  let persistCalls = 0;
  const requestedOffsets: Array<number | undefined> = [];
  const persistedOffsets: Array<number | undefined> = [];
  const signal = new AbortController().signal;
  const run = createTelegramPollLoopRunner({
    getConfig: () => config,
    deleteWebhook: async () => {},
    getUpdates: async (body) => {
      requestedOffsets.push(body.offset as number | undefined);
      getUpdatesCalls += 1;
      if (getUpdatesCalls <= 2) return [{ update_id: 6 }];
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async (next) => {
      persistCalls += 1;
      persistedOffsets.push(next.lastUpdateId);
      if (persistCalls === 1) throw new Error("config commit failed");
    },
    handleUpdate: async (update) => {
      handled.push(update.update_id);
    },
    updateStatus: () => {},
    sleep: async () => {},
  });
  await assert.rejects(() => run(TEST_CONTEXT, signal), /config commit failed/u);
  await run(TEST_CONTEXT, signal);

  assert.deepEqual(handled, [6]);
  assert.equal(persistCalls, 2);
  assert.deepEqual(requestedOffsets.slice(0, 2), [6, 6]);
  assert.deepEqual(persistedOffsets, [6, 6]);
  assert.equal(config.lastUpdateId, 6);
});

test("Poll loop skips repeatedly failing updates after the configured threshold", async () => {
  const config = { botToken: "123:abc", lastUpdateId: 5 };
  const persisted: number[] = [];
  const statusMessages: string[] = [];
  const runtimeEvents: string[] = [];
  let calls = 0;
  await runTelegramPollLoop({
    ctx: TEST_CONTEXT,
    signal: new AbortController().signal,
    config,
    maxUpdateFailures: 2,
    deleteWebhook: async () => {},
    getUpdates: async () => {
      calls += 1;
      if (calls <= 2) return [{ update_id: 6 }];
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async (next) => {
      persisted.push(next.lastUpdateId ?? -1);
    },
    handleUpdate: async () => {
      throw new Error("handler failed");
    },
    onErrorStatus: (message) => {
      statusMessages.push(message);
    },
    onStatusReset: () => {
      statusMessages.push("reset");
    },
    sleep: async (ms) => {
      statusMessages.push(`sleep:${ms}`);
    },
    recordRuntimeEvent: (category, error, details) => {
      const message = error instanceof Error ? error.message : String(error);
      runtimeEvents.push(
        `${category}:${message}:${details?.phase}:${details?.failureCount}`,
      );
    },
  });
  assert.equal(config.lastUpdateId, 6);
  assert.deepEqual(persisted, [6]);
  assert.deepEqual(statusMessages, [
    "handler failed",
    "sleep:3000",
    "reset",
    "skipping Telegram update 6 after 2 failures: handler failed",
  ]);
  assert.deepEqual(runtimeEvents, [
    "polling:handler failed:handleUpdate:1",
    "polling:handler failed:handleUpdate:2",
  ]);
});

test("Polling supervisor restarts bootstrap failures within one active generation", async () => {
  const state = createTelegramPollingControllerState();
  const events: string[] = [];
  let bootstrapCalls = 0;
  const controller = createTelegramPollingControllerRuntime({
    state,
    getConfig: () => ({ botToken: "123:abc", lastUpdateId: 1 }),
    hasBotToken: () => true,
    deleteWebhook: async () => {
      bootstrapCalls += 1;
      if (bootstrapCalls === 1) throw new Error("bootstrap failed");
    },
    getUpdates: async (_body, signal) =>
      new Promise<never>((_resolve, reject) =>
        signal.addEventListener(
          "abort",
          () => reject(new DOMException("stop", "AbortError")),
          { once: true },
        )
      ),
    persistConfig: async () => {},
    handleUpdate: async () => {},
    stopTypingLoop: () => {},
    updateStatus: () => {},
    supervisorSleep: async () => {},
    restartSeed: "2026072202",
    recordRuntimeEvent: (_category, _error, details) => {
      if (details?.restartCount !== undefined)
        events.push(`${details.phase}:${details.restartCount}:${details.generation}`);
    },
  });
  controller.start(TEST_CONTEXT);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(controller.isActive(), true);
  assert.equal(bootstrapCalls, 2);
  assert.deepEqual(events, ["bootstrap:0:1"]);
  await controller.stop();
  assert.equal(controller.isActive(), false);
});

test("Polling supervisor terminalizes after bounded restart-sleep failures", async () => {
  const terminal: string[] = [];
  let restartSleeps = 0;
  const controller = createTelegramPollingControllerRuntime({
    getConfig: () => ({ botToken: "123:abc", lastUpdateId: 1 }),
    hasBotToken: () => true,
    deleteWebhook: async () => {},
    getUpdates: async () => { throw new Error("network"); },
    persistConfig: async () => {},
    handleUpdate: async () => {},
    stopTypingLoop: () => {},
    updateStatus: () => {},
    supervisorSleep: async () => {
      restartSleeps += 1;
      throw new Error("retry sleep failed");
    },
    maxRestarts: 1,
    restartSeed: "2026072202",
    onTerminalFailure: async (info) => {
      terminal.push(`${info.phase}:${info.restartCount}:${info.permanent}`);
    },
  });
  controller.start(TEST_CONTEXT);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(controller.isActive(), false);
  assert.equal(restartSleeps, 1);
  assert.deepEqual(terminal, ["retry-sleep:1:false"]);
});

test("Polling supervisor releases through the terminal hook for bootstrap and persistence failures", async () => {
  for (const scenario of ["bootstrap", "persist"] as const) {
    const terminal: string[] = [];
    const controller = createTelegramPollingControllerRuntime({
      getConfig: () => ({ botToken: "123:abc", lastUpdateId: 1 }),
      hasBotToken: () => true,
      deleteWebhook: async () => {
        if (scenario === "bootstrap") throw new Error("bootstrap failed");
      },
      getUpdates: async () => [{ update_id: 2 }],
      persistConfig: async () => {
        if (scenario === "persist") throw new Error("persist failed");
      },
      handleUpdate: async () => {},
      stopTypingLoop: () => {},
      updateStatus: () => {},
      maxRestarts: 0,
      supervisorSleep: async () => {},
      onTerminalFailure: async (info) => {
        terminal.push(info.phase);
      },
    });
    controller.start(TEST_CONTEXT);
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(terminal, [scenario]);
    assert.equal(controller.isActive(), false);
  }
});

test("Polling supervisor terminalizes missing runtime config without restart", async () => {
  const terminal: string[] = [];
  const controller = createTelegramPollingControllerRuntime({
    getConfig: () => ({}),
    hasBotToken: () => true,
    deleteWebhook: async () => {},
    getUpdates: async () => [],
    persistConfig: async () => {},
    handleUpdate: async () => {},
    stopTypingLoop: () => {},
    updateStatus: () => {},
    supervisorSleep: async () => {},
    onTerminalFailure: async (info) => {
      terminal.push(`${info.phase}:${info.restartCount}:${info.permanent}`);
    },
  });
  controller.start(TEST_CONTEXT);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(terminal, ["bootstrap:0:true"]);
});

test("Polling supervisor terminalizes permanent auth without restart", async () => {
  const terminal: string[] = [];
  let calls = 0;
  const authError = Object.assign(new Error("Unauthorized"), { status: 401 });
  const controller = createTelegramPollingControllerRuntime({
    getConfig: () => ({ botToken: "123:abc", lastUpdateId: 1 }),
    hasBotToken: () => true,
    deleteWebhook: async () => {},
    getUpdates: async () => { calls += 1; throw authError; },
    persistConfig: async () => {},
    handleUpdate: async () => {},
    stopTypingLoop: () => {},
    updateStatus: () => {},
    isPermanentError: (error) => error === authError,
    supervisorSleep: async () => {},
    onTerminalFailure: async (info) => {
      terminal.push(`${info.phase}:${info.restartCount}:${info.permanent}`);
    },
  });
  controller.start(TEST_CONTEXT);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(calls, 1);
  assert.deepEqual(terminal, ["api:0:true"]);
});

test("Polling terminalization blocks restart and stale lease handlers cannot stop a new generation", async () => {
  const state = createTelegramPollingControllerState();
  let fail = true;
  let calls = 0;
  let releaseHook: (() => void) | undefined;
  let markHookStarted: (() => void) | undefined;
  const hookStarted = new Promise<void>((resolve) => { markHookStarted = resolve; });
  const controller = createTelegramPollingControllerRuntime({
    state,
    getConfig: () => ({ botToken: "123:abc", lastUpdateId: 1 }),
    hasBotToken: () => true,
    deleteWebhook: async () => {},
    getUpdates: async (_body, signal) => {
      calls += 1;
      if (fail) throw new Error("terminal");
      return new Promise<never>((_resolve, reject) =>
        signal.addEventListener(
          "abort",
          () => reject(new DOMException("stop", "AbortError")),
          { once: true },
        )
      );
    },
    persistConfig: async () => {},
    handleUpdate: async () => {},
    stopTypingLoop: () => {},
    updateStatus: () => {},
    maxRestarts: 0,
    onTerminalFailure: async () => {
      markHookStarted?.();
      await new Promise<void>((resolve) => { releaseHook = resolve; });
    },
  });
  controller.start(TEST_CONTEXT);
  await hookStarted;
  assert.equal(state.terminalizingGeneration, 1);
  assert.equal(controller.isActive(), false);
  controller.start(TEST_CONTEXT);
  assert.equal(calls, 1);
  assert.equal(state.generation, 1);

  releaseHook?.();
  while (state.terminalizingGeneration !== undefined) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  fail = false;
  controller.start(TEST_CONTEXT);
  await Promise.resolve();
  assert.equal(state.generation, 2);
  assert.equal(controller.isActive(), true);

  let leaseStops = 0;
  const handler = createTelegramPollingTerminalLeaseHandler({
    state,
    terminalizeTransportLease: async () => { leaseStops += 1; },
  });
  assert.equal(
    await handler({
      phase: "api",
      restartCount: 0,
      generation: 1,
      startedAtMs: 1,
      failedAtMs: 2,
      permanent: false,
    }),
    false,
  );
  assert.equal(leaseStops, 0);
  await controller.stop();
});

test("Polling terminal lease handler releases only its matching generation", async () => {
  const state = createTelegramPollingControllerState();
  state.generation = 2;
  state.terminalizingGeneration = 2;
  let leaseStops = 0;
  const handler = createTelegramPollingTerminalLeaseHandler({
    state,
    terminalizeTransportLease: async () => { leaseStops += 1; },
  });
  assert.equal(
    await handler({
      phase: "api",
      restartCount: 1,
      generation: 2,
      startedAtMs: 1,
      failedAtMs: 2,
      permanent: false,
    }),
    true,
  );
  assert.equal(leaseStops, 1);
});

test("Polling supervisor explicit stop neither restarts nor overlaps generations", async () => {
  let activeLoops = 0;
  let maxActiveLoops = 0;
  let starts = 0;
  const controller = createTelegramPollingController({
    hasBotToken: () => true,
    stopTypingLoop: () => {},
    runPollLoop: async (_ctx: string, signal) => {
      starts += 1;
      activeLoops += 1;
      maxActiveLoops = Math.max(maxActiveLoops, activeLoops);
      await new Promise<void>((resolve) =>
        signal.addEventListener("abort", () => resolve(), { once: true })
      );
      activeLoops -= 1;
    },
    updateStatus: () => {},
  });
  controller.start(TEST_CONTEXT);
  controller.start(TEST_CONTEXT);
  await controller.stop();
  assert.equal(starts, 1);
  assert.equal(maxActiveLoops, 1);
  assert.equal(controller.isActive(), false);
});

test("Polling retry sleep resolves immediately when aborted", async () => {
  const controller = new AbortController();
  controller.abort();
  await sleepTelegramPollingRetry(3000, controller.signal);
});

test("Poll loop stops without status reset when aborted during handler retry sleep", async () => {
  const config = { botToken: "123:abc", lastUpdateId: 1 };
  const controller = new AbortController();
  const statusMessages: string[] = [];
  await runTelegramPollLoop({
    ctx: TEST_CONTEXT,
    signal: controller.signal,
    config,
    deleteWebhook: async () => {},
    getUpdates: async () => [{ update_id: 2 }],
    persistConfig: async () => {},
    handleUpdate: async () => { throw new Error("handler down"); },
    onErrorStatus: (message) => {
      statusMessages.push(`error:${message}`);
    },
    onStatusReset: () => {
      statusMessages.push("unexpected:reset");
    },
    sleep: async (_ms, signal) => {
      assert.equal(signal, controller.signal);
      controller.abort();
    },
  });
  assert.deepEqual(statusMessages, ["error:handler down"]);
});

test("Poll loop suppresses getUpdates conflicts while another long poll drains", async () => {
  const config = { botToken: "123:abc", lastUpdateId: 1 };
  const statusMessages: string[] = [];
  const runtimeEvents: string[] = [];
  let calls = 0;
  await runTelegramPollLoop({
    ctx: TEST_CONTEXT,
    signal: new AbortController().signal,
    config,
    deleteWebhook: async () => {},
    getUpdates: async () => {
      calls += 1;
      if (calls <= 4) {
        throw new Error(
          "Telegram API getUpdates failed: HTTP 409: Conflict: terminated by other getUpdates request; make sure that only one bot instance is running",
        );
      }
      throw new DOMException("stop", "AbortError");
    },
    persistConfig: async () => {},
    handleUpdate: async () => {},
    onErrorStatus: (message) => {
      statusMessages.push(`error:${message}`);
    },
    onStatusReset: () => {
      statusMessages.push("reset");
    },
    sleep: async (ms) => {
      statusMessages.push(`sleep:${ms}`);
    },
    recordRuntimeEvent: (category, error, details) => {
      const message = error instanceof Error ? error.message : String(error);
      runtimeEvents.push(`${category}:${message}:${details?.phase}`);
    },
  });
  assert.equal(
    isTelegramGetUpdatesConflictError(
      new Error("HTTP 409: Conflict: terminated by other getUpdates request"),
    ),
    true,
  );
  assert.deepEqual(statusMessages, [
    "sleep:1000",
    "sleep:1000",
    "sleep:3000",
    "sleep:3000",
  ]);
  assert.equal(runtimeEvents.length, 4);
});

test("Polling supervisor bounds repeated malformed API/body failures", async () => {
  const terminal: string[] = [];
  let calls = 0;
  const controller = createTelegramPollingControllerRuntime({
    getConfig: () => ({ botToken: "123:abc", lastUpdateId: 1 }),
    hasBotToken: () => true,
    deleteWebhook: async () => {},
    getUpdates: async () => {
      calls += 1;
      throw new Error("Telegram API getUpdates returned invalid JSON");
    },
    persistConfig: async () => {},
    handleUpdate: async () => {},
    stopTypingLoop: () => {},
    updateStatus: () => {},
    supervisorSleep: async () => {},
    maxRestarts: 2,
    restartSeed: "2026072202",
    onTerminalFailure: async (info) => {
      terminal.push(`${info.phase}:${info.restartCount}`);
    },
  });
  controller.start(TEST_CONTEXT);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(calls, 3);
  assert.deepEqual(terminal, ["api:2"]);
  assert.equal(controller.isActive(), false);
});
