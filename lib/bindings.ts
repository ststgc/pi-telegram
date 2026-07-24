/**
 * Telegram bridge binding composition
 * Zones: telegram, pi agent, orchestration
 * Owns pi-facing tool, command, and lifecycle hook registration for the entrypoint
 */

import * as Activity from "./activity.ts";
import * as Commands from "./commands.ts";
import * as Config from "./config.ts";
import * as Keyboard from "./keyboard.ts";
import * as Lifecycle from "./lifecycle.ts";
import * as Locks from "./locks.ts";
import * as Model from "./model.ts";
import * as OutboundAttachments from "./outbound-attachments.ts";
import * as OutboundHandlers from "./outbound.ts";
import * as Pi from "./pi.ts";
import * as Preview from "./preview.ts";
import * as Prompts from "./prompts.ts";
import * as Queue from "./queue.ts";
import * as Replies from "./replies.ts";
import * as Routing from "./routing.ts";
import * as Runtime from "./runtime.ts";
import * as Setup from "./setup.ts";
import * as Status from "./status.ts";
import * as TelegramApi from "./telegram-api.ts";

type ActivePiModel = NonNullable<Pi.ExtensionContext["model"]>;

type TelegramRuntimeEventRecorder = (
  category: string,
  error: unknown,
  details?: Record<string, unknown>,
) => void;

type TelegramBridgeStatusUpdater =
  Status.TelegramStatusRuntime<Pi.ExtensionContext>["updateStatus"];

export interface TelegramAssistantOutputBindingRuntime<TTransportStamp> {
  runtime: Activity.TelegramAssistantOutputRuntime;
  observeEvent: (event: Activity.TelegramActivityEvent) => void;
  authority: Routing.TelegramAssistantOutputAuthorityRuntime<TTransportStamp>;
}

export function createTelegramAssistantOutputBindingRuntime<
  TTransportStamp,
>(deps: {
  isEnabled: () => boolean;
  authority: {
    getPreferredTarget: () =>
      | OutboundAttachments.TelegramQueuedOutboundAttachmentTurnView["target"]
      | undefined;
    getFallbackChatId: () => number | undefined;
    getTransportStamp: () => TTransportStamp;
    isTransportStampActive: (stamp: TTransportStamp) => boolean;
    ownsDirect: () => boolean;
    getDirectEpoch: () => number | string | undefined;
    isFollowerRegistered: () => boolean;
    getFollowerGeneration: () => string | undefined;
  };
  sender: Parameters<
    typeof OutboundHandlers.createTelegramAssistantOutputSender<TTransportStamp>
  >[0];
  recordRuntimeEvent: TelegramRuntimeEventRecorder;
}): TelegramAssistantOutputBindingRuntime<TTransportStamp> {
  const authority = Routing.createTelegramAssistantOutputAuthorityRuntime(
    deps.authority,
  );
  const send =
    OutboundHandlers.createTelegramAssistantOutputSender<TTransportStamp>(
      deps.sender,
    );
  const runtime = Activity.createTelegramAssistantOutputRuntime({
    isEnabled: deps.isEnabled,
    ...authority,
    send,
    recordFailure(event, error) {
      deps.recordRuntimeEvent("proactive-push", error, {
        activityId: event.activityId,
        sequence: event.sequence,
        placement: event.placement,
      });
    },
  });
  return {
    runtime,
    authority,
    observeEvent(event) {
      if (event.type === "assistant-segment") runtime.accept(event);
    },
  };
}

interface TelegramCommandsAndToolsBindingDeps {
  pi: Pi.ExtensionAPI;
  configStore: Config.TelegramConfigStore;
  persistConfig: (config?: Config.TelegramConfig) => Promise<void>;
  getPairingInstructions: () => Promise<string | undefined>;
  setup: Setup.TelegramSetupGuard;
  activeTurnRuntime: Queue.TelegramActiveTurnStore<Queue.PendingTelegramTurn>;
  lockedPollingRuntime: Locks.TelegramLockedPollingRuntime<Pi.ExtensionContext>;
  resumeDurableOutboundWorker: () => Promise<void>;
  stopPolling?: () => Promise<void | string>;
  getDisconnectThreadName?: () => string | undefined;
  onTransportChanged?: () => Promise<void> | void;
  getStatusLines: (
    options?: Status.TelegramBridgeStatusLineOptions,
  ) => string[];
  buttonActionStore: OutboundHandlers.TelegramButtonActionStore;
  sendMarkdownReply: (
    chatId: number,
    replyToMessageId: number | undefined,
    markdown: string,
    options?: { replyMarkup?: unknown },
  ) => Promise<number | undefined>;
  callMultipart: OutboundHandlers.TelegramVoiceReplySenderDeps["sendMultipart"];
  getDefaultChatId: () => number | undefined;
  getDefaultTarget?: () => OutboundAttachments.TelegramQueuedOutboundAttachmentTurnView["target"];
  canSendDirect: () => boolean;
  updateStatus: TelegramBridgeStatusUpdater;
  recordRuntimeEvent: TelegramRuntimeEventRecorder;
}

export function registerTelegramCommandsAndTools({
  pi,
  configStore,
  persistConfig,
  getPairingInstructions,
  setup,
  activeTurnRuntime,
  lockedPollingRuntime,
  resumeDurableOutboundWorker,
  stopPolling,
  getDisconnectThreadName,
  onTransportChanged,
  getStatusLines,
  buttonActionStore,
  sendMarkdownReply,
  callMultipart,
  getDefaultChatId,
  getDefaultTarget,
  canSendDirect,
  recordRuntimeEvent,
  updateStatus,
}: TelegramCommandsAndToolsBindingDeps): void {
  OutboundAttachments.registerTelegramOutboundAttachmentTool(pi, {
    getActiveTurn: activeTurnRuntime.get,
    getDefaultChatId,
    getDefaultTarget,
    canSendDirect,
    sendMultipart: callMultipart,
    recordRuntimeEvent,
  });
  OutboundAttachments.registerTelegramOutboundMessageTool(pi, {
    getDefaultChatId,
    getDefaultTarget,
    canSendDirect,
    planMessage:
      OutboundHandlers.createTelegramOutboundReplyPlanner(buttonActionStore),
    sendMarkdownMessage: (chatId, markdown, options) =>
      sendMarkdownReply(chatId, undefined, markdown, options),
    recordRuntimeEvent,
  });
  Prompts.registerTelegramHelpTool(pi, {
    getActiveProfileName: configStore.getActiveProfileName,
  });
  const startPollingAndResumeOutbound: typeof lockedPollingRuntime.start =
    async (ctx, options) => {
      const result = await lockedPollingRuntime.start(ctx, options);
      if (result === undefined || result.ok) {
        await resumeDurableOutboundWorker();
      }
      return result;
    };
  Commands.registerTelegramBridgeCommands(pi, {
    promptForConfig: async (ctx, profileName) => {
      const nextProfileName = profileName ?? undefined;
      if (profileName && !Config.isValidTelegramProfileName(profileName)) {
        ctx.ui.notify(`Invalid Telegram profile name: ${profileName}`, "error");
        return;
      }
      const previousProfileName = configStore.getActiveProfileName();
      let setupConfigStore = configStore;
      let persistSetupConfig = persistConfig;
      if (!profileName) {
        if (previousProfileName !== nextProfileName) {
          await (stopPolling ?? lockedPollingRuntime.stop)();
        }
        configStore.activateProfile(undefined);
        await onTransportChanged?.();
      } else {
        const storedConfig = configStore.getStoredConfig();
        setupConfigStore = Config.createTelegramConfigStore({
          initialConfig: {
            ...storedConfig,
            profiles: {
              ...(storedConfig.profiles ?? {}),
              [profileName]: storedConfig.profiles?.[profileName] ?? {
                botToken: "",
              },
            },
          },
        });
        setupConfigStore.activateProfile(profileName);
        persistSetupConfig = async () => {
          try {
            if (previousProfileName !== profileName) {
              await (stopPolling ?? lockedPollingRuntime.stop)();
            }
            const profile = Config.getTelegramProfileFields(
              setupConfigStore.get(),
            );
            if (!profile) {
              throw new Error(
                `Telegram profile "${profileName}" has no token.`,
              );
            }
            await configStore.load();
            configStore.setProfile(profileName, profile);
            configStore.activateProfile(profileName);
            await onTransportChanged?.();
            await persistConfig(configStore.get());
          } catch (error) {
            await configStore.load().catch(() => undefined);
            configStore.activateProfile(previousProfileName);
            await onTransportChanged?.();
            throw error;
          }
        };
      }
      const runSetup = Setup.createTelegramSetupPromptRuntime({
        getConfig: setupConfigStore.get,
        setConfig: setupConfigStore.set,
        setupGuard: setup,
        getMe: TelegramApi.fetchTelegramBotIdentity,
        persistConfig: persistSetupConfig,
        getPairingInstructions,
        startPolling: startPollingAndResumeOutbound,
        updateStatus,
        recordRuntimeEvent,
      });
      const completion = await runSetup(ctx);
      if (profileName && completion.status === "success") {
        ctx.ui.notify(`Profile "${profileName}" saved and connected.`, "info");
      }
    },
    getStatusLines,
    reloadConfig: configStore.load,
    hasBotToken: configStore.hasBotToken,
    getPairingInstructions,
    startPolling: startPollingAndResumeOutbound,
    stopPolling: stopPolling ?? lockedPollingRuntime.stop,
    getDisconnectThreadName,
    updateStatus,
    getProfileNames: () =>
      Config.getTelegramProfileNames(configStore.getStoredConfig()),
    activateDefaultProfileConfig: async () => {
      const previousProfileName = configStore.getActiveProfileName();
      await configStore.load();
      if (previousProfileName) {
        await (stopPolling ?? lockedPollingRuntime.stop)();
      }
      configStore.activateProfile(undefined);
      await onTransportChanged?.();
    },
    activateProfileConfig: async (_ctx, profileName) => {
      const previousProfileName = configStore.getActiveProfileName();
      await configStore.load();
      if (!Config.isValidTelegramProfileName(profileName)) return false;
      const storedConfig = configStore.getStoredConfig();
      if (!storedConfig.profiles?.[profileName]) return false;
      if (previousProfileName !== profileName) {
        await (stopPolling ?? lockedPollingRuntime.stop)();
      }
      if (!configStore.activateProfile(profileName)) return false;
      await onTransportChanged?.();
      return true;
    },
  });
}

interface TelegramLifecycleBindingDeps {
  pi: Pi.ExtensionAPI;
  activityRuntime: Activity.TelegramActivityRuntime;
  assistantOutputRuntime: Pick<
    Activity.TelegramAssistantOutputRuntime,
    "start" | "stop"
  >;
  sessionLifecycleRuntime: Pick<
    Lifecycle.TelegramLifecycleRegistrationDeps,
    "onSessionStart" | "onSessionShutdown" | "onModelSelect"
  >;
  configStore: Pick<
    Config.TelegramConfigStore,
    "get" | "getOutboundHandlers" | "hasBotToken" | "load"
  >;
  abort: Runtime.TelegramRuntimeAbortPort;
  typing: Runtime.TelegramRuntimeTypingPort;
  lifecycle: Runtime.TelegramRuntimeLifecyclePort;
  activeTurnRuntime: Queue.TelegramActiveTurnStore<Queue.PendingTelegramTurn>;
  telegramQueueStore: Queue.TelegramQueueStore<Pi.ExtensionContext>;
  modelSwitchController: Model.TelegramModelSwitchController<
    Pi.ExtensionContext,
    Model.ScopedTelegramModel<ActivePiModel>
  >;
  previewRuntime: Preview.TelegramAssistantPreviewRuntime<
    Pi.AgentEndEvent["messages"][number],
    Keyboard.TelegramInlineKeyboardMarkup
  >;
  promptDispatchRuntime: Runtime.TelegramPromptDispatchRuntime<Pi.ExtensionContext>;
  deferredQueueDispatchRuntime: Queue.TelegramDeferredQueueDispatchRuntime<Pi.ExtensionContext>;
  lockOwnershipGuard: Pick<
    Locks.TelegramLockOwnershipGuard<Pi.ExtensionContext>,
    "ownsContext"
  >;
  dispatchNextQueuedTelegramTurn: (ctx: Pi.ExtensionContext) => void;
  durableOutbound: Pick<
    Queue.TelegramDurableAgentLifecycleHooksRuntimeDeps<
      Queue.PendingTelegramTurn,
      Pi.ExtensionContext,
      Pi.AgentEndEvent["messages"][number]
    >,
    | "handoffActiveTurn"
    | "completeTurnWithoutDelivery"
    | "markTurnExecutionUncertain"
  >;
  proactivePushTargetGetter: () => Queue.TelegramQueueTarget | undefined;
  isProactivePushEnabled: () => boolean;
  canSendAgentActivity: (ctx: Pi.ExtensionContext) => boolean;
  isSessionContextActive: (ctx: Pi.ExtensionContext) => boolean;
  updateStatus: TelegramBridgeStatusUpdater;
  recordRuntimeEvent: TelegramRuntimeEventRecorder;
}

export function registerTelegramLifecycleRuntimeHooks({
  pi,
  activityRuntime,
  assistantOutputRuntime,
  sessionLifecycleRuntime,
  configStore,
  abort,
  typing,
  lifecycle,
  activeTurnRuntime,
  telegramQueueStore,
  modelSwitchController,
  previewRuntime,
  promptDispatchRuntime,
  deferredQueueDispatchRuntime,
  lockOwnershipGuard,
  dispatchNextQueuedTelegramTurn,
  durableOutbound,
  proactivePushTargetGetter,
  isProactivePushEnabled,
  canSendAgentActivity,
  isSessionContextActive = () => true,
  updateStatus,
  recordRuntimeEvent,
}: TelegramLifecycleBindingDeps): void {
  const agentEndResetter = Runtime.createTelegramAgentEndResetter({
    abort,
    typing,
    clearActiveTurn: activeTurnRuntime.clear,
    resetToolExecutions: lifecycle.resetActiveToolExecutions,
    clearPendingModelSwitch: modelSwitchController.clearPendingSwitch,
    clearDispatchPending: lifecycle.clearDispatchPending,
  });
  const agentLifecycleHooks = Queue.createTelegramDurableAgentLifecycleHooks<
    Queue.PendingTelegramTurn,
    Pi.ExtensionContext,
    Pi.AgentEndEvent["messages"][number]
  >({
    setAbortHandler: Runtime.createTelegramContextAbortHandlerSetter(abort),
    getQueuedItems: telegramQueueStore.getQueuedItems,
    hasPendingDispatch: lifecycle.hasDispatchPending,
    hasActiveTurn: activeTurnRuntime.has,
    resetToolExecutions: lifecycle.resetActiveToolExecutions,
    resetPendingModelSwitch: modelSwitchController.clearPendingSwitch,
    setQueuedItems: telegramQueueStore.setQueuedItems,
    clearDispatchPending: lifecycle.clearDispatchPending,
    setFoldQueuedPromptsIntoHistory: lifecycle.setFoldQueuedPromptsIntoHistory,
    setActiveTurn: activeTurnRuntime.set,
    createPreviewState: previewRuntime.resetState,
    startTypingLoop: (ctx) => {
      const turn = activeTurnRuntime.get();
      promptDispatchRuntime.startTypingLoop(ctx, turn?.chatId, {
        target: turn?.target,
      });
    },
    updateStatus,
    getActiveTurn: activeTurnRuntime.get,
    loadConfig: configStore.load,
    extractAssistant: Replies.extractLatestAssistantMessageText,
    resetRuntimeState: agentEndResetter,
    isSessionActive: isSessionContextActive,
    waitForTypingIdle: typing.waitForIdle,
    dispatchNextQueuedTelegramTurn,
    requestDeferredDispatchNextQueuedTelegramTurn:
      deferredQueueDispatchRuntime.request,
    handoffActiveTurn: durableOutbound.handoffActiveTurn,
    completeTurnWithoutDelivery: durableOutbound.completeTurnWithoutDelivery,
    markTurnExecutionUncertain: durableOutbound.markTurnExecutionUncertain,
    recordRuntimeEvent,
    getActiveToolExecutions: lifecycle.getActiveToolExecutions,
    setActiveToolExecutions: lifecycle.setActiveToolExecutions,
    triggerPendingModelSwitchAbort: modelSwitchController.triggerPendingAbort,
  });
  Lifecycle.setResetTransportReplyDedup(Replies.resetTransportReplyDedup);
  const agentStartWithDedupReset = Lifecycle.createAgentStartDedupHook(
    agentLifecycleHooks.onAgentStart,
  );
  const startAgentActivityTypingLoop = (ctx: Pi.ExtensionContext): boolean => {
    if (!canSendAgentActivity(ctx)) return false;
    const turn = activeTurnRuntime.get();
    const target = turn?.target ?? proactivePushTargetGetter();
    promptDispatchRuntime.startTypingLoop(ctx, turn?.chatId ?? target?.chatId, {
      target,
    });
    return true;
  };
  const startActiveTurnTypingLoop = (ctx: Pi.ExtensionContext): void => {
    const turn = activeTurnRuntime.get();
    promptDispatchRuntime.startTypingLoop(ctx, turn?.chatId, {
      target: turn?.target,
    });
  };
  const compactionObserver = Lifecycle.createTelegramCompactionObserverRuntime({
    isContextActive: isSessionContextActive,
    setCompactionInProgress: lifecycle.setCompactionInProgress,
    updateStatus,
    startTypingLoop: startAgentActivityTypingLoop,
    stopTypingLoop: typing.stop,
    requestDeferredDispatchNextQueuedTelegramTurn:
      deferredQueueDispatchRuntime.request,
    dispatchNextQueuedTelegramTurn,
    recordRuntimeEvent,
    onCompactionAbandoned: activityRuntime.onCompactionAbandoned,
  });
  const messageActivityTypingHooks =
    Lifecycle.createTelegramMessageActivityTypingHooks({
      hasActiveTurn: activeTurnRuntime.has,
      startTypingLoop: startActiveTurnTypingLoop,
      onMessageStart: previewRuntime.onMessageStart,
      onMessageUpdate: previewRuntime.onMessageUpdate,
      recordRuntimeEvent,
    });
  const messageActivityHooks = messageActivityTypingHooks;
  Lifecycle.registerTelegramLifecycleHooks(pi, {
    isSessionActive: isSessionContextActive,
    ...sessionLifecycleRuntime,
    ...agentLifecycleHooks,
    onInput(event) {
      activityRuntime.recordInputSource(event.source ?? "unknown");
    },
    async onSessionStart(event, ctx) {
      previewRuntime.invalidate();
      assistantOutputRuntime.start();
      activityRuntime.onSessionStart?.();
      await sessionLifecycleRuntime.onSessionStart(event, ctx);
    },
    async onSessionShutdown(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      agentLifecycleHooks.clearRetainedAgentEnd();
      activityRuntime.onSessionShutdown();
      assistantOutputRuntime.stop();
      compactionObserver.onSessionShutdown();
      await sessionLifecycleRuntime.onSessionShutdown(event, ctx);
    },
    onSessionBeforeCompact(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      activityRuntime.onCompactionStart(Pi.getSessionCompactionReason(event));
      compactionObserver.onSessionBeforeCompact(event, ctx);
    },
    onSessionCompact(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      activityRuntime.onCompactionEnd(Pi.getSessionCompactionReason(event));
      compactionObserver.onSessionCompact(event, ctx);
    },
    async onAgentStart(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      await agentStartWithDedupReset(event, ctx);
      activityRuntime.onAgentStart(activeTurnRuntime.get()?.target);
      startAgentActivityTypingLoop(ctx);
    },
    async onToolExecutionStart(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      agentLifecycleHooks.onToolExecutionStart();
      activityRuntime.onToolStart({
        toolCallId: event.toolCallId,
        toolName: event.toolName,
        args: event.args,
      });
    },
    onToolExecutionUpdate(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      activityRuntime.onToolUpdate({
        toolCallId: event.toolCallId,
        toolName: event.toolName,
        update: event.partialResult,
      });
    },
    async onToolExecutionEnd(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      activityRuntime.onToolEnd({
        toolCallId: event.toolCallId,
        toolName: event.toolName,
        result: event.result,
        isError: event.isError,
      });
      agentLifecycleHooks.onToolExecutionEnd(event, ctx);
    },
    async onMessageStart(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      await messageActivityHooks.onMessageStart(event, ctx);
    },
    async onMessageUpdate(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      if (event.assistantMessageEvent) {
        activityRuntime.onAssistantEvent(
          event.assistantMessageEvent as Activity.TelegramAssistantStreamEvent,
        );
      }
      await messageActivityHooks.onMessageUpdate(event, ctx);
    },
    async onAgentEnd(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      activityRuntime.onAgentEnd();
      await agentLifecycleHooks.onAgentEnd(event, ctx);
    },
    async onAgentSettled(event, ctx) {
      if (!isSessionContextActive(ctx)) return;
      await agentLifecycleHooks.onAgentSettled(event, ctx);
      activityRuntime.onAgentSettled();
    },
    onBeforeAgentStart: Prompts.createTelegramProactiveBeforeAgentStartHook({
      isConfigured: configStore.hasBotToken,
      isProactivePushEnabled,
      isCurrentOwner: lockOwnershipGuard.ownsContext,
    }),
  });
}
