/**
 * Regression tests for Telegram binding composition
 * Covers lifecycle binding delegation across composed runtimes
 */

import assert from "node:assert/strict";
import test from "node:test";

import {
  createTelegramAssistantOutputBindingRuntime,
  registerTelegramCommandsAndTools,
  registerTelegramLifecycleRuntimeHooks,
} from "../lib/bindings.ts";
import * as Runtime from "../lib/runtime.ts";
import type { ExtensionAPI, ExtensionContext } from "../lib/pi.ts";

type RegisteredBindingHandler = (
  event: unknown,
  ctx: ExtensionContext,
) => Promise<unknown> | unknown;

type RegisteredBindingTool = {
  name?: string;
  execute: (
    toolCallId: string,
    params: Record<string, string>,
  ) => Promise<unknown>;
};

function createBindingApiHarness() {
  const handlers = new Map<string, RegisteredBindingHandler>();
  const tools = new Map<string, RegisteredBindingTool>();
  const commands = new Map<string, unknown>();
  const api = {
    on: (event: string, handler: RegisteredBindingHandler) => {
      handlers.set(event, handler);
    },
    registerTool: (definition: RegisteredBindingTool) => {
      if (definition.name) tools.set(definition.name, definition);
    },
    registerCommand: (name: string, definition: unknown) => {
      commands.set(name, definition);
    },
  } as unknown as ExtensionAPI;
  return { api, handlers, tools, commands };
}

test("Assistant output binding composes admission, delivery, and observation", async () => {
  const sent: string[] = [];
  const binding = createTelegramAssistantOutputBindingRuntime({
    isEnabled: () => true,
    authority: {
      getPreferredTarget: () => ({ chatId: 7, threadId: 42 }),
      getFallbackChatId: () => 7,
      getTransportStamp: () => "stamp-1",
      isTransportStampActive: (stamp) => stamp === "stamp-1",
      ownsDirect: () => true,
      getDirectEpoch: () => 1,
      isFollowerRegistered: () => false,
      getFollowerGeneration: () => undefined,
    },
    sender: {
      sendMessage: async () => ({ message_id: 1 }),
      sendRichMessage: async (body) => {
        sent.push(body.rich_message.markdown ?? "");
        return { message_id: 2 };
      },
      editMessage: async () => undefined,
      getAssistantRenderingMode: () => "rich",
      execCommand: async (_command, _args, options) => ({
        stdout: options?.stdin ?? "",
        stderr: "",
        code: 0,
        killed: false,
      }),
    },
    recordRuntimeEvent: () => undefined,
  });
  binding.runtime.start();
  binding.observeEvent({
    type: "assistant-segment",
    activityId: "activity-1",
    sequence: 1,
    source: "local",
    timestamp: 1,
    contentIndex: 0,
    text: "public output",
    placement: "final",
  });
  await binding.runtime.waitForIdle();
  assert.deepEqual(sent, ["public output"]);
});

function getRequiredBindingHandler(
  handlers: Map<string, RegisteredBindingHandler>,
  name: string,
): RegisteredBindingHandler {
  const handler = handlers.get(name);
  assert.ok(handler, `Expected binding handler ${name}`);
  return handler;
}

test("Command binding does not expose a thread rename tool", () => {
  const harness = createBindingApiHarness();
  registerTelegramCommandsAndTools({
    pi: harness.api,
    configStore: {
      get: () => ({}),
      getAllowedUserId: () => 840585,
      getOutboundHandlers: () => [],
      hasBotToken: () => true,
      load: async () => {},
      persist: async () => {},
      set: () => {},
    },
    setup: { start: () => true, finish: () => {} },
    activeTurnRuntime: { get: () => undefined },
    lockedPollingRuntime: {
      start: async () => ({ ok: true }),
      stop: async () => undefined,
    },
    resumeDurableOutboundWorker: async () => {},
    getStatusLines: () => [],
    buttonActionStore: { register: () => "button-action" },
    sendMarkdownReply: async () => 1,
    callMultipart: async () => ({ ok: true }),
    getDefaultChatId: () => 840585,
    canSendDirect: () => true,
    updateStatus: () => {},
    recordRuntimeEvent: () => {},
  } as unknown as Parameters<typeof registerTelegramCommandsAndTools>[0]);
  assert.equal(harness.tools.has("telegram_rename_thread"), false);
});

test("Command binding rejects a missing profile without stopping active polling", async () => {
  const harness = createBindingApiHarness();
  const events: string[] = [];
  let activeProfileName: string | undefined = "active";
  registerTelegramCommandsAndTools({
    pi: harness.api,
    configStore: {
      get: () => ({ botToken: "active-token" }),
      getStoredConfig: () => ({
        profiles: { active: { botToken: "active-token" } },
      }),
      getActiveProfileName: () => activeProfileName,
      activateProfile: (profileName?: string) => {
        events.push(`activate:${profileName ?? "default"}`);
        activeProfileName = profileName;
        return true;
      },
      getAllowedUserId: () => 840585,
      getOutboundHandlers: () => [],
      hasBotToken: () => true,
      load: async () => {
        events.push("load");
      },
      persist: async () => {},
      set: () => {},
    },
    setup: { start: () => true, finish: () => {} },
    activeTurnRuntime: { get: () => undefined },
    lockedPollingRuntime: {
      start: async () => {
        events.push("start");
        return { ok: true };
      },
      stop: async () => {
        events.push("stop");
      },
    },
    getStatusLines: () => [],
    buttonActionStore: { register: () => "button-action" },
    sendMarkdownReply: async () => 1,
    callMultipart: async () => ({ ok: true }),
    getDefaultChatId: () => 840585,
    canSendDirect: () => true,
    updateStatus: () => {
      events.push("status");
    },
    recordRuntimeEvent: () => {},
    resumeDurableOutboundWorker: async () => {},
  } as unknown as Parameters<typeof registerTelegramCommandsAndTools>[0]);
  const connect = harness.commands.get("telegram-connect") as {
    handler: (args: string, ctx: ExtensionContext) => Promise<void>;
  };
  const notifications: string[] = [];
  await connect.handler("missing", {
    cwd: "/repo",
    ui: {
      notify: (message: string) => {
        notifications.push(message);
      },
    },
  } as unknown as ExtensionContext);
  assert.equal(activeProfileName, "active");
  assert.deepEqual(events, ["load", "status"]);
  assert.deepEqual(notifications, ['Profile "missing" not found.']);
});

test("Named profile connect completes old teardown before activating new identity", async () => {
  const harness = createBindingApiHarness();
  const events: string[] = [];
  let activeProfileName: string | undefined = "active";
  let authorityInvalidated = false;
  let stopCompleted = false;
  registerTelegramCommandsAndTools({
    pi: harness.api,
    configStore: {
      get: () => ({ botToken: `${activeProfileName}-token` }),
      getStoredConfig: () => ({
        profiles: {
          active: { botToken: "active-token" },
          work: { botToken: "work-token" },
        },
      }),
      getActiveProfileName: () => activeProfileName,
      activateProfile: (profileName?: string) => {
        assert.equal(stopCompleted, true);
        events.push(`activate:${profileName ?? "default"}`);
        activeProfileName = profileName;
        return true;
      },
      getAllowedUserId: () => 840585,
      getOutboundHandlers: () => [],
      hasBotToken: () => true,
      load: async () => events.push("load"),
      persist: async () => {},
      set: () => {},
    },
    setup: { start: () => true, finish: () => {} },
    activeTurnRuntime: { get: () => undefined },
    lockedPollingRuntime: {
      start: async () => {
        events.push(`start:${activeProfileName}`);
        return { ok: true };
      },
      stop: async () => {
        assert.equal(authorityInvalidated, true);
        events.push(`stop:${activeProfileName}`);
        await Promise.resolve();
        stopCompleted = true;
      },
    },
    invalidateTransportAuthority: () => {
      events.push(`invalidate:${activeProfileName}`);
      authorityInvalidated = true;
    },
    rebindTransportAuthority: () => {
      events.push(`rebind:${activeProfileName}`);
    },
    resumeDurableOutboundWorker: async () => {
      events.push("worker:resume");
    },
    getStatusLines: () => [],
    buttonActionStore: { register: () => "button-action" },
    sendMarkdownReply: async () => 1,
    callMultipart: async () => ({ ok: true }),
    getDefaultChatId: () => 840585,
    canSendDirect: () => true,
    updateStatus: () => events.push("status"),
    recordRuntimeEvent: () => {},
  } as unknown as Parameters<typeof registerTelegramCommandsAndTools>[0]);

  const connect = harness.commands.get("telegram-connect") as {
    handler: (args: string, ctx: ExtensionContext) => Promise<void>;
  };
  await connect.handler("work", {
    cwd: "/repo",
    ui: { notify: () => undefined },
  } as unknown as ExtensionContext);

  assert.equal(activeProfileName, "work");
  assert.deepEqual(events, [
    "load",
    "invalidate:active",
    "stop:active",
    "activate:work",
    "rebind:work",
    "start:work",
    "worker:resume",
    "status",
  ]);
});

test("Disconnect invalidates interaction authority before stop and rebinds the published state", async () => {
  const harness = createBindingApiHarness();
  const events: string[] = [];
  let invalidated = false;
  registerTelegramCommandsAndTools({
    pi: harness.api,
    configStore: {
      get: () => ({ botToken: "token" }),
      getStoredConfig: () => ({ profiles: { default: { botToken: "token" } } }),
      getActiveProfileName: () => undefined,
      activateProfile: () => true,
      getAllowedUserId: () => 840585,
      getOutboundHandlers: () => [],
      hasBotToken: () => true,
      load: async () => undefined,
      persist: async () => undefined,
      set: () => undefined,
    },
    setup: { start: () => true, finish: () => undefined },
    activeTurnRuntime: { get: () => undefined },
    lockedPollingRuntime: {
      start: async () => ({ ok: true }),
      stop: async () => {
        assert.equal(invalidated, true);
        events.push("stop");
      },
    },
    invalidateTransportAuthority: () => {
      invalidated = true;
      events.push("invalidate");
    },
    rebindTransportAuthority: () => events.push("rebind"),
    resumeDurableOutboundWorker: async () => undefined,
    getStatusLines: () => [],
    buttonActionStore: { register: () => "button-action" },
    sendMarkdownReply: async () => 1,
    callMultipart: async () => ({ ok: true }),
    getDefaultChatId: () => 840585,
    canSendDirect: () => true,
    updateStatus: () => events.push("status"),
    recordRuntimeEvent: () => undefined,
  } as unknown as Parameters<typeof registerTelegramCommandsAndTools>[0]);

  const disconnect = harness.commands.get("telegram-disconnect") as {
    handler: (args: string, ctx: ExtensionContext) => Promise<void>;
  };
  await disconnect.handler("", {
    cwd: "/repo",
    ui: { notify: () => undefined },
  } as unknown as ExtensionContext);

  assert.deepEqual(events, ["invalidate", "stop", "rebind", "status"]);
});

test("Named profile setup cancellation preserves the active runtime", async () => {
  const harness = createBindingApiHarness();
  const events: string[] = [];
  let activeProfileName: string | undefined = "active";
  registerTelegramCommandsAndTools({
    pi: harness.api,
    configStore: {
      get: () => ({ botToken: "active-token" }),
      getStoredConfig: () => ({
        profiles: { active: { botToken: "active-token" } },
      }),
      getActiveProfileName: () => activeProfileName,
      activateProfile: (profileName?: string) => {
        events.push(`activate:${profileName ?? "default"}`);
        activeProfileName = profileName;
        return true;
      },
      getAllowedUserId: () => 840585,
      getOutboundHandlers: () => [],
      hasBotToken: () => true,
      load: async () => undefined,
      persist: async () => {
        events.push("persist");
      },
      set: () => {
        events.push("set");
      },
    },
    setup: {
      start: () => {
        events.push("guard-start");
        return true;
      },
      finish: () => {
        events.push("guard-finish");
      },
    },
    activeTurnRuntime: { get: () => undefined },
    lockedPollingRuntime: {
      start: async () => {
        events.push("start");
        return { ok: true };
      },
      stop: async () => {
        events.push("stop");
      },
    },
    getStatusLines: () => [],
    buttonActionStore: { register: () => "button-action" },
    sendMarkdownReply: async () => 1,
    callMultipart: async () => ({ ok: true }),
    getDefaultChatId: () => 840585,
    canSendDirect: () => true,
    updateStatus: () => {
      events.push("status");
    },
    recordRuntimeEvent: () => {},
    resumeDurableOutboundWorker: async () => {},
  } as unknown as Parameters<typeof registerTelegramCommandsAndTools>[0]);
  const setupCommand = harness.commands.get("telegram-setup") as {
    handler: (args: string, ctx: ExtensionContext) => Promise<void>;
  };
  const notifications: string[] = [];
  await setupCommand.handler("newprofile", {
    cwd: "/repo",
    hasUI: true,
    ui: {
      input: async () => undefined,
      editor: async () => undefined,
      notify: (message: string) => {
        notifications.push(message);
      },
    },
  } as unknown as ExtensionContext);
  assert.equal(activeProfileName, "active");
  assert.deepEqual(events, ["guard-start", "guard-finish"]);
  assert.deepEqual(notifications, []);
});

test("Named profile setup invalidates before stop/publication and rebinds only after publication", async (t) => {
  const harness = createBindingApiHarness();
  const events: string[] = [];
  let invalidated = false;
  let activeProfileName: string | undefined = "active";
  const profiles: Record<string, { botToken?: string; botId?: number; botUsername?: string }> = {
    active: { botToken: "111111:ACTIVE" },
  };
  t.mock.method(globalThis, "fetch", async () =>
    new Response(JSON.stringify({
      ok: true,
      result: {
        id: 7,
        is_bot: true,
        first_name: "Bridge",
        username: "bridge_bot",
      },
    }), { status: 200, headers: { "content-type": "application/json" } }));
  registerTelegramCommandsAndTools({
    pi: harness.api,
    configStore: {
      get: () => ({ ...profiles[activeProfileName ?? "default"] }),
      getStoredConfig: () => ({ profiles: { ...profiles } }),
      getActiveProfileName: () => activeProfileName,
      activateProfile: (profileName?: string) => {
        events.push(`activate:${profileName ?? "default"}`);
        activeProfileName = profileName;
        return !profileName || !!profiles[profileName];
      },
      setProfile: (profileName: string, profile: (typeof profiles)[string]) => {
        assert.equal(invalidated, true);
        events.push(`publish:${profileName}`);
        profiles[profileName] = { ...profile };
      },
      getAllowedUserId: () => 840585,
      getOutboundHandlers: () => [],
      hasBotToken: () => !!profiles[activeProfileName ?? "default"]?.botToken,
      load: async () => events.push("load"),
      persist: async () => undefined,
      set: () => undefined,
    },
    persistConfig: async () => events.push(`persist:${activeProfileName}`),
    getPairingInstructions: async () => undefined,
    setup: { start: () => true, finish: () => events.push("guard:finish") },
    activeTurnRuntime: { get: () => undefined },
    lockedPollingRuntime: {
      start: async () => {
        events.push(`start:${activeProfileName}`);
        return { ok: true };
      },
      stop: async () => {
        assert.equal(invalidated, true);
        events.push(`stop:${activeProfileName}`);
      },
    },
    invalidateTransportAuthority: () => {
      invalidated = true;
      events.push(`invalidate:${activeProfileName}`);
    },
    rebindTransportAuthority: () => events.push(`rebind:${activeProfileName}`),
    resumeDurableOutboundWorker: async () => events.push("worker:resume"),
    getStatusLines: () => [],
    buttonActionStore: { register: () => "button-action" },
    sendMarkdownReply: async () => 1,
    callMultipart: async () => ({ ok: true }),
    getDefaultChatId: () => 840585,
    canSendDirect: () => true,
    updateStatus: () => events.push("status"),
    recordRuntimeEvent: () => undefined,
  } as unknown as Parameters<typeof registerTelegramCommandsAndTools>[0]);

  const setupCommand = harness.commands.get("telegram-setup") as {
    handler: (args: string, ctx: ExtensionContext) => Promise<void>;
  };
  await setupCommand.handler("work", {
    cwd: "/repo",
    hasUI: true,
    ui: {
      input: async () => "222222:WORK",
      editor: async () => "222222:WORK",
      notify: () => undefined,
    },
  } as unknown as ExtensionContext);

  assert.deepEqual(events.slice(0, 8), [
    "invalidate:active",
    "stop:active",
    "load",
    "publish:work",
    "activate:work",
    "persist:work",
    "rebind:work",
    "start:work",
  ]);
  assert.equal(activeProfileName, "work");
});

test("Named profile setup keeps its persisted profile active when rebind fails", async (t) => {
  const harness = createBindingApiHarness();
  const events: string[] = [];
  let activeProfileName: string | undefined = "active";
  const profiles: Record<string, { botToken?: string; botId?: number; botUsername?: string }> = {
    active: { botToken: "111111:ACTIVE" },
  };
  t.mock.method(globalThis, "fetch", async () =>
    new Response(JSON.stringify({
      ok: true,
      result: {
        id: 7,
        is_bot: true,
        first_name: "Bridge",
        username: "bridge_bot",
      },
    }), { status: 200, headers: { "content-type": "application/json" } }));
  registerTelegramCommandsAndTools({
    pi: harness.api,
    configStore: {
      get: () => ({ ...profiles[activeProfileName ?? "default"] }),
      getStoredConfig: () => ({ profiles: { ...profiles } }),
      getActiveProfileName: () => activeProfileName,
      activateProfile: (profileName?: string) => {
        events.push(`activate:${profileName ?? "default"}`);
        activeProfileName = profileName;
        return !profileName || !!profiles[profileName];
      },
      setProfile: (profileName: string, profile: (typeof profiles)[string]) => {
        events.push(`publish:${profileName}`);
        profiles[profileName] = { ...profile };
      },
      getAllowedUserId: () => 840585,
      getOutboundHandlers: () => [],
      hasBotToken: () => !!profiles[activeProfileName ?? "default"]?.botToken,
      load: async () => events.push("load"),
      persist: async () => undefined,
      set: () => undefined,
    },
    persistConfig: async () => events.push(`persist:${activeProfileName}`),
    getPairingInstructions: async () => undefined,
    setup: { start: () => true, finish: () => events.push("guard:finish") },
    activeTurnRuntime: { get: () => undefined },
    lockedPollingRuntime: {
      start: async () => {
        events.push(`start:${activeProfileName}`);
        return { ok: true };
      },
      stop: async () => events.push(`stop:${activeProfileName}`),
    },
    invalidateTransportAuthority: () => events.push(`invalidate:${activeProfileName}`),
    rebindTransportAuthority: () => {
      events.push(`rebind:${activeProfileName}`);
      throw new Error("rebind failed");
    },
    resumeDurableOutboundWorker: async () => events.push("worker:resume"),
    getStatusLines: () => [],
    buttonActionStore: { register: () => "button-action" },
    sendMarkdownReply: async () => 1,
    callMultipart: async () => ({ ok: true }),
    getDefaultChatId: () => 840585,
    canSendDirect: () => true,
    updateStatus: () => events.push("status"),
    recordRuntimeEvent: (category: string, error: unknown) =>
      events.push(`record:${category}:${error instanceof Error ? error.message : String(error)}`),
  } as unknown as Parameters<typeof registerTelegramCommandsAndTools>[0]);

  const setupCommand = harness.commands.get("telegram-setup") as {
    handler: (args: string, ctx: ExtensionContext) => Promise<void>;
  };
  await assert.rejects(
    setupCommand.handler("work", {
      cwd: "/repo",
      hasUI: true,
      ui: {
        input: async () => "222222:WORK",
        editor: async () => "222222:WORK",
        notify: () => undefined,
      },
    } as unknown as ExtensionContext),
    /rebind failed/,
  );

  assert.equal(activeProfileName, "work");
  assert.deepEqual(profiles.work, {
    botToken: "222222:WORK",
    botId: 7,
    botUsername: "bridge_bot",
  });
  assert.deepEqual(events, [
    "invalidate:active",
    "stop:active",
    "load",
    "publish:work",
    "activate:work",
    "persist:work",
    "rebind:work",
    "record:setup:rebind failed",
    "guard:finish",
  ]);
  assert.equal(events.some((event) => event.startsWith("start:")), false);
});

test("Lifecycle binding delegates shutdown to composed session runtime", async () => {
  const events: string[] = [];
  const harness = createBindingApiHarness();
  const runtime = Runtime.createTelegramBridgeRuntime();
  const deps: Parameters<typeof registerTelegramLifecycleRuntimeHooks>[0] = {
    pi: harness.api,
    activityRuntime: {
      recordInputSource: () => {},
      onAgentStart: () => {},
      onAssistantEvent: () => {},
      onToolStart: () => {},
      onToolUpdate: () => {},
      onToolEnd: () => {},
      onCompactionStart: () => {},
      onCompactionEnd: () => {},
      onCompactionAbandoned: () => {},
      onAgentEnd: () => {},
      onAgentSettled: () => {},
      onSessionShutdown: () => {},
    },
    interactionLifecycleRuntime: { invalidateAuthority: () => {} },
    assistantOutputRuntime: { start: () => {}, stop: () => {} },
    sessionLifecycleRuntime: {
      onSessionStart: async () => {
        events.push("session-start");
      },
      onSessionShutdown: async () => {
        events.push("composed-shutdown");
      },
      onModelSelect: () => {
        events.push("model-select");
      },
    },
    configStore: {
      get: () => ({}),
      getOutboundHandlers: () => [],
      hasBotToken: () => false,
      load: async () => {},
    },
    abort: runtime.abort,
    typing: runtime.typing,
    lifecycle: runtime.lifecycle,
    activeTurnRuntime: {
      clear: () => {},
      has: () => false,
      set: () => {},
      get: () => undefined,
      getChatId: () => undefined,
      getTarget: () => undefined,
      getReplyToMessageId: () => undefined,
      getGuestQueryId: () => undefined,
      getSourceMessageIds: () => undefined,
    },
    telegramQueueStore: {
      getQueuedItems: () => [],
      setQueuedItems: () => {},
    },
    modelSwitchController: {
      canOfferInFlightSwitch: () => false,
      stagePendingSwitch: () => {},
      clearPendingSwitch: () => {},
      queueContinuation: () => {},
      triggerPendingAbort: () => false,
      restartInterruptedTurn: () => false,
    },
    previewRuntime: {
      getState: () => undefined,
      setState: () => {},
      setPendingText: () => {},
      createState: () => ({ mode: "draft", pendingText: "", lastSentText: "" }),
      resetState: () => {},
      invalidate: () => {},
      clear: async () => {},
      flush: async () => {},
      scheduleFlush: () => {},
      finalize: async () => true,
      finalizeMarkdown: async () => true,
      onMessageStart: async () => {},
      onMessageUpdate: async () => {},
    },
    promptDispatchRuntime: {
      startTypingLoop: () => {
        events.push("typing:start");
      },
      onPromptDispatchStart: () => {},
      onPromptDispatchFailure: () => {},
    },
    deferredQueueDispatchRuntime: {
      bind: () => {},
      unbind: () => {},
      isBound: () => true,
      getGeneration: () => 1,
      isGenerationActive: () => true,
      request: () => {},
    },
    lockOwnershipGuard: { ownsContext: () => false },
    dispatchNextQueuedTelegramTurn: () => {},
    durableOutbound: {
      handoffActiveTurn: async () => ({ startDelivery: () => {} }),
      completeTurnWithoutDelivery: () => {},
      markTurnExecutionUncertain: () => {},
    },
    proactivePushTargetGetter: () => undefined,
    isProactivePushEnabled: () => false,
    canSendAgentActivity: () => false,
    isSessionContextActive: () => true,
    updateStatus: () => {},
    recordRuntimeEvent: () => {},
  };

  registerTelegramLifecycleRuntimeHooks(deps);
  await getRequiredBindingHandler(harness.handlers, "session_before_compact")(
    { type: "session_before_compact" },
    {} as ExtensionContext,
  );
  await getRequiredBindingHandler(harness.handlers, "session_shutdown")(
    {},
    {} as ExtensionContext,
  );

  assert.deepEqual(events, ["composed-shutdown"]);
});

test("Lifecycle binding routes native typing, previews, and normalized activity", async () => {
  const events: string[] = [];
  const harness = createBindingApiHarness();
  const runtime = Runtime.createTelegramBridgeRuntime();
  let activeTurn = false;
  const deps: Parameters<typeof registerTelegramLifecycleRuntimeHooks>[0] = {
    pi: harness.api,
    activityRuntime: {
      recordInputSource: (source: string) =>
        events.push(`activity:input:${source}`),
      onAgentStart: (target?: { chatId: number; threadId?: number }) =>
        events.push(
          `activity:agent-start:${target?.threadId ?? target?.chatId ?? "none"}`,
        ),
      onAssistantEvent: (event: { type: string }) =>
        events.push(`activity:assistant:${event.type}`),
      onToolStart: (event: { toolName: string }) =>
        events.push(`activity:tool-start:${event.toolName}`),
      onToolUpdate: (event: { toolName: string }) =>
        events.push(`activity:tool-update:${event.toolName}`),
      onToolEnd: (event: { toolName: string }) =>
        events.push(`activity:tool-end:${event.toolName}`),
      onCompactionStart: (reason: string) =>
        events.push(`activity:compact-start:${reason}`),
      onCompactionEnd: (reason: string) =>
        events.push(`activity:compact-end:${reason}`),
      onCompactionAbandoned: () =>
        events.push("activity:compact-abandoned"),
      onAgentEnd: () => events.push("activity:agent-end"),
      onAgentSettled: () => events.push("activity:agent-settled"),
      onSessionShutdown: () => events.push("activity:shutdown"),
    },
    interactionLifecycleRuntime: {
      invalidateAuthority: () => events.push("interaction:invalidate"),
    },
    assistantOutputRuntime: {
      start: () => events.push("assistant-output:start"),
      stop: () => events.push("assistant-output:stop"),
    },
    sessionLifecycleRuntime: {
      onSessionStart: async () => {},
      onSessionShutdown: async () => {},
      onModelSelect: () => {},
    },
    configStore: {
      get: () => ({}),
      getOutboundHandlers: () => [],
      hasBotToken: () => true,
      load: async () => {},
    },
    abort: runtime.abort,
    typing: runtime.typing,
    lifecycle: runtime.lifecycle,
    activeTurnRuntime: {
      clear: () => {},
      has: () => activeTurn,
      set: () => {},
      get: () =>
        activeTurn
          ? {
              kind: "prompt",
              chatId: 42,
              target: { chatId: 42, threadId: 9 },
              replyToMessageId: 8,
              sourceMessageIds: [8],
              queueOrder: 1,
              queueLane: "default",
              laneOrder: 1,
              queuedAttachments: [],
              content: [{ type: "text", text: "prompt" }],
              historyText: "prompt",
              statusSummary: "prompt",
            }
          : undefined,
      getChatId: () => activeTurn ? 42 : undefined,
      getTarget: () => activeTurn ? { chatId: 42, threadId: 9 } : undefined,
      getReplyToMessageId: () => activeTurn ? 8 : undefined,
      getGuestQueryId: () => undefined,
      getSourceMessageIds: () => activeTurn ? [8] : undefined,
    },
    telegramQueueStore: { getQueuedItems: () => [], setQueuedItems: () => {} },
    modelSwitchController: {
      canOfferInFlightSwitch: () => false,
      stagePendingSwitch: () => {},
      clearPendingSwitch: () => {},
      queueContinuation: () => {},
      triggerPendingAbort: () => false,
      restartInterruptedTurn: () => false,
    },
    previewRuntime: {
      getState: () => undefined,
      setState: () => {},
      setPendingText: () => {},
      createState: () => ({ mode: "draft", pendingText: "", lastSentText: "" }),
      resetState: () => {},
      invalidate: () => {},
      clear: async () => {},
      flush: async () => {},
      scheduleFlush: () => {},
      finalize: async () => true,
      finalizeMarkdown: async () => true,
      onMessageStart: async () => {
        events.push("preview:start");
      },
      onMessageUpdate: async () => {
        events.push("preview:update");
      },
    },
    promptDispatchRuntime: {
      startTypingLoop: (
        _ctx: ExtensionContext,
        chatId?: number,
        options?: { target?: { threadId?: number } },
      ) => {
        events.push(
          `typing:${chatId ?? "none"}:${options?.target?.threadId ?? "all"}`,
        );
      },
      onPromptDispatchStart: () => {},
      onPromptDispatchFailure: () => {},
    },
    deferredQueueDispatchRuntime: {
      bind: () => {},
      unbind: () => {},
      isBound: () => true,
      getGeneration: () => 1,
      isGenerationActive: () => true,
      request: () => {},
    },
    lockOwnershipGuard: { ownsContext: () => false },
    dispatchNextQueuedTelegramTurn: () => {},
    durableOutbound: {
      handoffActiveTurn: async () => ({ startDelivery: () => {} }),
      completeTurnWithoutDelivery: () => {},
      markTurnExecutionUncertain: () => {},
    },
    proactivePushTargetGetter: () => ({ chatId: 42, threadId: 8 }),
    isProactivePushEnabled: () => false,
    canSendAgentActivity: () => true,
    isSessionContextActive: () => true,
    updateStatus: () => {},
    recordRuntimeEvent: () => {},
  };
  registerTelegramLifecycleRuntimeHooks(deps);

  await getRequiredBindingHandler(harness.handlers, "agent_start")(
    { type: "agent_start" },
    { abort: () => undefined } as ExtensionContext,
  );
  activeTurn = true;
  await getRequiredBindingHandler(harness.handlers, "message_start")(
    { message: {} },
    {} as ExtensionContext,
  );
  await getRequiredBindingHandler(harness.handlers, "message_update")(
    {
      message: {},
      assistantMessageEvent: {
        type: "thinking_delta",
        contentIndex: 0,
        delta: "ponder <edge>",
      },
    },
    {} as ExtensionContext,
  );
  await getRequiredBindingHandler(harness.handlers, "tool_execution_start")(
    {
      type: "tool_execution_start",
      toolCallId: "1",
      toolName: "read",
      args: { path: "README.md" },
    },
    {} as ExtensionContext,
  );
  await getRequiredBindingHandler(harness.handlers, "session_before_compact")(
    { type: "session_before_compact" },
    {} as ExtensionContext,
  );
  await getRequiredBindingHandler(harness.handlers, "session_compact")(
    { type: "session_compact" },
    {} as ExtensionContext,
  );
  activeTurn = false;
  await getRequiredBindingHandler(harness.handlers, "session_before_compact")(
    { type: "session_before_compact" },
    {} as ExtensionContext,
  );
  await getRequiredBindingHandler(harness.handlers, "agent_end")(
    { messages: [] },
    {} as ExtensionContext,
  );

  assert.deepEqual(events, [
    "activity:agent-start:none",
    "typing:42:8",
    "typing:42:9",
    "preview:start",
    "typing:42:9",
    "activity:assistant:thinking_delta",
    "typing:42:9",
    "preview:update",
    "typing:42:9",
    "activity:tool-start:read",
    "activity:compact-start:unknown",
    "typing:42:9",
    "activity:compact-end:unknown",
    "activity:compact-start:unknown",
    "typing:42:8",
    "interaction:invalidate",
    "activity:agent-end",
  ]);
});
