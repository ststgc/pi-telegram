/**
 * Telegram bridge extension entrypoint and orchestration layer
 * Zones: telegram, pi agent, orchestration
 * Keeps the runtime wiring in one place while delegating reusable domain logic to /lib modules
 */

import * as Activity from "./lib/activity.ts";
import * as Bindings from "./lib/bindings.ts";
import * as BusApi from "./lib/bus-api.ts";
import * as BusFollower from "./lib/bus-follower.ts";
import * as BusLeader from "./lib/bus-leader.ts";
import * as BusTransport from "./lib/bus-transport.ts";
import * as Bus from "./lib/bus.ts";
import * as CommandTemplates from "./lib/command-templates.ts";
import * as Commands from "./lib/commands.ts";
import * as Config from "./lib/config.ts";
import * as Delivery from "./lib/delivery.ts";
import * as Inbound from "./lib/inbound.ts";
import * as InboundRecovery from "./lib/inbound-recovery.ts";
import * as Lifecycle from "./lib/lifecycle.ts";
import * as Locks from "./lib/locks.ts";
import * as Logs from "./lib/logs.ts";
import * as Media from "./lib/media.ts";
import * as MenuQueue from "./lib/menu-queue.ts";
import * as MenuRecovery from "./lib/menu-recovery.ts";
import * as MenuSettings from "./lib/menu-settings.ts";
import * as Menu from "./lib/menu.ts";
import * as Model from "./lib/model.ts";
import * as Outbound from "./lib/outbound.ts";
import * as OutboundRecovery from "./lib/outbound-recovery.ts";
import * as Ownership from "./lib/ownership.ts";
import * as Pairing from "./lib/pairing.ts";
import * as Paths from "./lib/paths.ts";
import * as Pi from "./lib/pi.ts";
import * as Polling from "./lib/polling.ts";
import * as Preview from "./lib/preview.ts";
import * as PromptTemplates from "./lib/prompt-templates.ts";
import * as Queue from "./lib/queue.ts";
import * as Replies from "./lib/replies.ts";
import * as Recovery from "./lib/recovery.ts";
import * as Routing from "./lib/routing.ts";
import * as Runtime from "./lib/runtime.ts";
import * as Sections from "./lib/sections.ts";
import * as Status from "./lib/status.ts";
import * as Sync from "./lib/sync.ts";
import * as TelegramApi from "./lib/telegram-api.ts";
import * as TextGroups from "./lib/text-groups.ts";
import * as ThreadReconciler from "./lib/thread-reconciler.ts";
import * as Threads from "./lib/threads.ts";
import * as TimeInjection from "./lib/time-injection.ts";
import * as Updates from "./lib/updates.ts";
import * as Voice from "./lib/voice.ts";

type ActivePiModel = NonNullable<Pi.ExtensionContext["model"]>;

// --- Extension Runtime ---

export default function (pi: Pi.ExtensionAPI) {
  const piRuntime = Pi.createExtensionApiRuntimePorts(pi);
  const {
    getCommands,
    getThinkingLevel,
    sendUserMessage,
    setModel,
    setThinkingLevel,
  } = piRuntime;
  const bridgeRuntime = Runtime.createTelegramBridgeRuntime();
  const getActiveTelegramThreadProfile = function (): string | undefined {
    return configStore.getActiveProfileName();
  };
  const busProcessRuntime = Bus.createTelegramBusProcessRuntime({
    getActiveProfileName: getActiveTelegramThreadProfile,
    pid: process.pid,
    parentPid: process.ppid,
    createdAtMs: Date.now(),
  });
  const {
    instanceId: telegramInstanceId,
    manualFollowerOwnerId: telegramManualFollowerOwnerId,
    getLeaderSocketPath: getTelegramBusSocketPath,
    getFollowerSocketPath: getTelegramBusFollowerSocketPath,
  } = busProcessRuntime;
  const getTelegramManualFollowerProfileKey =
    BusFollower.createTelegramManualFollowerProfileKeyResolver({
      getActiveProfileName: getActiveTelegramThreadProfile,
      manualFollowerOwnerId: telegramManualFollowerOwnerId,
    });
  const telegramBusAuthSecret = Bus.createTelegramBusAuthSecret();
  let telegramActiveBusAuthSecret: string | undefined;
  let telegramBusLifecycleOverridePhase:
    Status.TelegramBridgeBusLifecyclePhase | undefined;
  const telegramBusFollowerRegistry = Bus.createTelegramBusFollowerRegistry();
  const telegramBusFollowerRegistrationState =
    BusFollower.createTelegramBusFollowerRegistrationState();
  const telegramBusLeaderState =
    Threads.createTelegramLeaderThreadStateRuntime();
  const telegramThreadCapabilityState =
    Polling.createTelegramThreadCapabilityStateRuntime();
  const telegramProvisioningActivity =
    Sync.createTelegramProvisioningActivityRuntime();
  const messageOwnershipRuntime =
    Ownership.createTelegramBusMessageOwnershipRuntime({
      instanceId: telegramInstanceId,
      getProfileKey() {
        return getActiveTelegramThreadProfile() ?? "default";
      },
      listFollowers: telegramBusFollowerRegistry.list,
    });
  const { abort, lifecycle, queue, setup, typing } = bridgeRuntime;
  const runtimeDiagnostics =
    Logs.createTelegramRuntimeDiagnosticsRuntime<Pi.ExtensionContext>();
  const runtimeEvents = runtimeDiagnostics.events;
  const recordRuntimeEvent = runtimeDiagnostics.recordRuntimeEvent;
  const configStore = Config.createTelegramConfigStore({ recordRuntimeEvent });
  const pairingRuntime = Pairing.createTelegramPairingRuntime({
    configStore,
    recordRuntimeEvent,
  });
  const isTelegramBusConfigured = function (): boolean {
    return true;
  };
  const isTelegramBusRuntimeEnabled = function (): boolean {
    return (
      isTelegramBusConfigured() &&
      !telegramThreadCapabilityState.isTopicModeUnavailable()
    );
  };
  Config.bindGlobalTelegramConfigRuntime(configStore);
  const configControls = Config.createTelegramConfigControls(configStore);
  const lockRuntime = Locks.createTelegramLockRuntime<Pi.ExtensionContext>({
    key: Locks.createTelegramLockKeyResolver(configStore),
    instanceId: telegramInstanceId,
    busSecret: telegramBusAuthSecret,
    staleHeartbeatMs: Locks.TELEGRAM_BUS_LEADER_STALE_HEARTBEAT_MS,
  });
  const threadStore = Threads.createTelegramTopicTargetStore({
    path: function () {
      return Threads.getTelegramTopicTargetsPath(
        undefined,
        configStore.getActiveProfileName(),
      );
    },
    canPersist: lockRuntime.owns,
    commitPersist: lockRuntime.commitIfOwned,
  });
  runtimeDiagnostics.bindStorage({
    getBotToken: configStore.getBotToken,
    getProfileName: configStore.getActiveProfileName,
    canReset: lockRuntime.owns,
    commitReset: lockRuntime.commitIfOwned,
  });
  const lockOwnershipGuard =
    Locks.createTelegramLockOwnershipGuard(lockRuntime);
  const getCurrentLeaderEpoch = lockRuntime.getOwnedLeaderEpoch;
  const telegramSessionContextStore =
    Lifecycle.createTelegramSessionContextStore<Pi.ExtensionContext>({
      getIdentity(ctx) {
        return ctx.sessionManager ?? ctx.cwd;
      },
    });
  const ownsTelegramDirectDelivery =
    Locks.createTelegramDirectDeliveryOwnershipChecker({
      lock: lockRuntime,
      contextStore: telegramSessionContextStore,
    });
  const activeTurnRuntime = Queue.createTelegramActiveTurnStore();
  const getAssignedTelegramTarget = function () {
    return (
      telegramBusFollowerRegistrationState.getTarget() ??
      telegramBusLeaderState.getTarget()
    );
  };
  const proactivePushTargetGetter =
    Config.createTelegramProactivePushTargetGetter({
      getActiveTurnTarget: activeTurnRuntime.getTarget,
      getAssignedTarget: getAssignedTelegramTarget,
      getAllowedUserId: configStore.getAllowedUserId,
    });
  const pairedProactivePushTargetGetter =
    Config.createTelegramProactivePushTargetGetter({
      getActiveTurnTarget() {
        return undefined;
      },
      getAssignedTarget: getAssignedTelegramTarget,
      getAllowedUserId: configStore.getAllowedUserId,
    });
  const proactivePushChatIdGetter =
    Config.createTelegramProactivePushChatIdGetter(proactivePushTargetGetter);
  const buttonActionStore = Outbound.createTelegramButtonActionStore();
  const pendingModelSwitchStore =
    Model.createPendingModelSwitchStore<
      Model.ScopedTelegramModel<ActivePiModel>
    >();
  const modelMenuRuntime = Menu.createTelegramModelMenuRuntime<ActivePiModel>();
  const sectionRegistry = Sections.createAndBindTelegramSectionRegistry();

  const timeInjectionRuntime = TimeInjection.createTimeInjectionRuntime({
    getConfig: Config.createTelegramTimeConfigGetter(configStore),
    recordRuntimeEvent,
  });
  Outbound.bindTelegramRuntimeEventRecorder(recordRuntimeEvent);
  const getContextModel = Pi.getExtensionContextModel;
  const isIdle = Pi.isExtensionContextIdle;
  const hasPendingMessages = Pi.hasExtensionContextPendingMessages;
  const compact = Pi.compactExtensionContext;
  const mediaGroupRuntime = Media.createTelegramMediaGroupController<
    TelegramApi.TelegramMessage,
    Pi.ExtensionContext
  >();
  const textGroupRuntime = TextGroups.createTelegramTextGroupController<
    TelegramApi.TelegramMessage,
    Pi.ExtensionContext
  >();
  const rawTelegramQueueStore =
    Queue.createTelegramQueueStore<Pi.ExtensionContext>();
  const telegramTransportStampRuntime =
    Queue.createTelegramTransportStampRuntime({
      getProfileName: configStore.getActiveProfileName,
      getBotToken: configStore.getBotToken,
    });
  const telegramQueueStore = Queue.createTelegramTransportStampedQueueStore(
    rawTelegramQueueStore,
    telegramTransportStampRuntime.getStamp,
  );
  const deferredQueueDispatchRuntime =
    Queue.createTelegramDeferredQueueDispatchRuntime<Pi.ExtensionContext>({
      delayMs: 50,
      recordRuntimeEvent,
    });
  const pollingControllerState = Polling.createTelegramPollingControllerState();
  const telegramSyncStateRuntime = Sync.createTelegramSyncStateRuntime();
  const threadReconciliationRuntime =
    ThreadReconciler.createThreadReconciliationRuntime({
      recordRuntimeEvent,
      scheduleSnapshotPersist: runtimeDiagnostics.scheduleSnapshotPersist,
    });
  const recordThreadReconciliationPlan = threadReconciliationRuntime.recordPlan;
  const persistTelegramConfigWithSync =
    Sync.createTelegramConfigSyncPersister<Config.TelegramConfig>({
      persist: configStore.persist,
      markConfigChange: telegramSyncStateRuntime.markConfigChange,
    });
  const persistTelegramPollingOffset =
    Config.createTelegramPollingOffsetPersister(
      configStore,
      persistTelegramConfigWithSync,
    );
  const currentInstanceThreadRuntime =
    Threads.createTelegramCurrentInstanceThreadRuntime({
      instanceId: telegramInstanceId,
      listRecords: threadStore.list,
      getPreferredTarget() {
        return (
          activeTurnRuntime.getTarget() ??
          telegramBusFollowerRegistrationState.getTarget() ??
          telegramBusLeaderState.getTarget()
        );
      },
      getFollower() {
        const target = telegramBusFollowerRegistrationState.getTarget();
        if (!target) return undefined;
        return {
          registered: telegramBusFollowerRegistrationState.isRegistered(),
          target,
          slot: telegramBusFollowerRegistrationState.getSlot(),
          threadName: telegramBusFollowerRegistrationState.getThreadName(),
        };
      },
      getLeader: telegramBusLeaderState.getIdentity,
    });
  const findCurrentThreadRecord = currentInstanceThreadRuntime.findRecord;
  const getCurrentInstanceThreadIdentity =
    currentInstanceThreadRuntime.getIdentity;
  const threadStatusProjectionRuntime =
    Threads.createTelegramThreadStatusProjectionRuntime({
      getThreadMode: function () {
        return threadStore.getBotState().threadMode;
      },
      isBusPollingStarted: telegramThreadCapabilityState.isBusPollingStarted,
      isFollowerRegistered: telegramBusFollowerRegistrationState.isRegistered,
      listFollowers: telegramBusFollowerRegistry.list,
      listRecords: threadStore.list,
      listReservations: threadStore.listReservations,
      listSyncObservations: threadStore.listSyncObservations,
      getLeaderSocketPath: getTelegramBusSocketPath,
      getFollowerSocketPath: getTelegramBusFollowerSocketPath,
      getTransportKind: BusTransport.getTelegramBusTransportKind,
      getFollowerTarget: telegramBusFollowerRegistrationState.getTarget,
      getFollowerSlot: telegramBusFollowerRegistrationState.getSlot,
      getFollowerThreadName: telegramBusFollowerRegistrationState.getThreadName,
      getCurrentIdentity: currentInstanceThreadRuntime.getRestorationIdentity,
    });
  let getRecoveryStatusProjection =
    function (): Status.TelegramBridgeRecoveryStatus | undefined {
      return undefined;
    };
  const statusRuntime = Status.createTelegramBridgeStatusRuntime<
    Pi.ExtensionContext,
    Queue.TelegramQueueItem<Pi.ExtensionContext>
  >({
    getConfig: configStore.get,
    getActiveProfileName: configStore.getActiveProfileName,
    getDiagnosticPaths: Paths.getTelegramDiagnosticsDisplayPaths,
    isPollingActive: Polling.createTelegramPollingActivityReader(
      pollingControllerState,
    ),
    getActiveSourceMessageIds: activeTurnRuntime.getSourceMessageIds,
    hasActiveTurn: activeTurnRuntime.has,
    hasDispatchPending: lifecycle.hasDispatchPending,
    isCompactionInProgress: lifecycle.isCompactionInProgress,
    getActiveToolExecutions: lifecycle.getActiveToolExecutions,
    hasPendingModelSwitch: pendingModelSwitchStore.has,
    getQueuedItems: telegramQueueStore.getQueuedItems,
    formatQueuedStatus: Queue.formatQueuedTelegramItemsStatus,
    getRecentRuntimeEvents: runtimeEvents.getEvents,
    getRuntimeLockState: lockRuntime.getStatusLabel,
    ...threadStatusProjectionRuntime,
    getBusLifecyclePhase() {
      return telegramBusLifecycleOverridePhase;
    },
    getBotThreadMode() {
      return threadStore.getBotState();
    },
    getSyncState: telegramSyncStateRuntime.getState,
    getThreadReconciliationState() {
      return threadReconciliationRuntime.getState();
    },
    getRecoveryStatus() {
      return getRecoveryStatusProjection();
    },
  });
  runtimeDiagnostics.bindStatus({
    instanceId: telegramInstanceId,
    updateStatus: statusRuntime.updateStatus,
    getStatusState: statusRuntime.getStatusState,
    async persistSnapshot(snapshot) {
      threadStore.setStatusSnapshot(snapshot);
      await threadStore.persist();
    },
  });
  const updateStatus = runtimeDiagnostics.updateStatus;
  const getStatusLines = runtimeDiagnostics.getStatusLines;
  const inboundHandlerRuntime = Inbound.createTelegramInboundHandlerRuntime({
    getHandlers: configStore.getInboundHandlers,
    execCommand: CommandTemplates.execCommandTemplate,
    getCwd: Pi.getExtensionContextCwd,
    recordRuntimeEvent,
  });

  // --- Telegram API ---

  const directTelegramApiRuntime =
    TelegramApi.createDefaultTelegramBridgeApiRuntime({
      getBotToken: configStore.getBotToken,
      recordRuntimeEvent,
    });
  const telegramBusFollowerClients =
    BusFollower.createTelegramBusFollowerClientRuntime<
      Pi.ExtensionContext,
      Updates.TelegramMessageReactionUpdated,
      Routing.TelegramRoutedCallbackQuery,
      Routing.TelegramRoutedMessage
    >({
      socketPath: getTelegramBusSocketPath,
      instanceId: telegramInstanceId,
      manualFollowerOwnerId: telegramManualFollowerOwnerId,
      getProfile() {
        return configStore.getActiveProfileName() ?? "default";
      },
      getTarget: telegramBusFollowerRegistrationState.getTarget,
      getSessionGeneration: telegramSessionContextStore.getGeneration,
      getApiAuthSecret() {
        return telegramActiveBusAuthSecret;
      },
      getForwardingAuthSecret() {
        return telegramBusAuthSecret;
      },
      getRegistrationGeneration:
        telegramBusFollowerRegistrationState.getGeneration,
      getForwardCommentBatchPosition:
        textGroupRuntime.getPreparedForwardingPosition,
      recordRuntimeEvent,
      timeoutMs: 30_000,
    });
  const telegramApiRuntime = BusApi.createTelegramBusAwareApiRuntime({
    directRuntime: directTelegramApiRuntime,
    ownsDirect() {
      return lockRuntime.owns();
    },
    getDefaultTarget: proactivePushTargetGetter,
    callFollowerApi: telegramBusFollowerClients.callApi,
  });
  const {
    call: callTelegramApi,
    callMultipart,
    deleteWebhook,
    getUpdates,
    setMyCommands,
    sendTypingAction,
    sendMessageDraft,
    sendMessage,
    sendRichMessage,
    sendRichMessageDraft,
    downloadFile: downloadTelegramBridgeFile,
    editMessageText: editTelegramMessageText,
    answerCallbackQuery,
    answerGuestQuery,
    deleteMessage: deleteTelegramMessage,
    prepareTempDir,
  } = telegramApiRuntime;

  // --- Message Delivery ---

  const promptDispatchRuntime = Runtime.createTelegramPromptDispatchRuntime({
    lifecycle,
    typing,
    getDefaultChatId: proactivePushChatIdGetter,
    sendTypingAction,
    sendAggregateTypingAction:
      BusApi.createTelegramAggregateTypingActionSender(telegramApiRuntime),
    updateStatus,
    recordRuntimeEvent,
  });
  const currentModelRuntime = Model.createCurrentModelRuntime({
    getContextModel,
    updateStatus,
  });
  const queueMutationRuntime = Queue.createTelegramQueueMutationController({
    ...telegramQueueStore,
    getNextPriorityReactionOrder: queue.getNextPriorityReactionOrder,
    incrementNextPriorityReactionOrder:
      queue.incrementNextPriorityReactionOrder,
    updateStatus,
  });
  const resolveRecoveryIdentity = function (
    target: { chatId: number; threadId?: number },
    ctx?: Pi.ExtensionContext,
  ): Recovery.RecoveryIdentity | undefined {
    const profile = configStore.getActiveProfileName() ?? "default";
    const sessionGeneration = telegramSessionContextStore.getGeneration();
    const followerTarget = telegramBusFollowerRegistrationState.getTarget();
    const followerGeneration =
      telegramBusFollowerRegistrationState.getGeneration();
    if (
      telegramBusFollowerRegistrationState.isRegistered() &&
      followerGeneration &&
      followerTarget?.chatId === target.chatId &&
      followerTarget.threadId === target.threadId
    ) {
      return {
        profile,
        target: { ...target },
        owner: {
          kind: "manual-follower",
          ownerId: telegramManualFollowerOwnerId,
          registrationGeneration: followerGeneration,
        },
        sessionGeneration,
      };
    }
    const leaderEpoch = lockRuntime.getOwnedLeaderEpoch();
    if (leaderEpoch === undefined || !lockRuntime.owns(ctx)) return undefined;
    return {
      profile,
      target: { ...target },
      owner: {
        kind: "leader",
        ownerId: telegramInstanceId,
        leaderEpoch: String(leaderEpoch),
      },
      sessionGeneration,
    };
  };
  const isRecoveryIdentityAuthenticated = function (
    identity: Recovery.RecoveryIdentity,
  ): boolean {
    if (
      identity.profile !==
      (configStore.getActiveProfileName() ?? "default")
    ) {
      return false;
    }
    if (identity.owner.kind === "leader") {
      const epoch = lockRuntime.getOwnedLeaderEpoch();
      return (
        identity.sessionGeneration ===
          telegramSessionContextStore.getGeneration() &&
        lockRuntime.owns() &&
        identity.owner.ownerId === telegramInstanceId &&
        epoch !== undefined &&
        identity.owner.leaderEpoch === String(epoch)
      );
    }
    const localTarget = telegramBusFollowerRegistrationState.getTarget();
    const localAuthenticated =
      identity.sessionGeneration === telegramSessionContextStore.getGeneration() &&
      telegramBusFollowerRegistrationState.isRegistered() &&
      identity.owner.ownerId === telegramManualFollowerOwnerId &&
      identity.owner.registrationGeneration ===
        telegramBusFollowerRegistrationState.getGeneration() &&
      localTarget?.chatId === identity.target.chatId &&
      localTarget.threadId === identity.target.threadId;
    if (localAuthenticated) return true;
    const registered = telegramBusFollowerRegistry.getByTarget(identity.target);
    return (
      registered?.manualFollowerOwnerId === identity.owner.ownerId &&
      registered.registrationGeneration ===
        identity.owner.registrationGeneration &&
      registered.target?.chatId === identity.target.chatId &&
      registered.target.threadId === identity.target.threadId
    );
  };
  const inboundRecoveryRuntime =
    InboundRecovery.createInboundRecoveryRuntime<
      TelegramApi.TelegramUpdate,
      Pi.ExtensionContext
    >({
      getProfile: configStore.getActiveProfileName,
      getAllowedUserId: configStore.getAllowedUserId,
      getCurrentInstanceId() {
        return telegramInstanceId;
      },
      getMessageOwnership: messageOwnershipRuntime.store.get,
      getTargetOwnership(target) {
        return Bus.getTelegramFollowerTargetOwnership({
          target,
          followers: telegramBusFollowerRegistry.list(),
        });
      },
      getSessionGeneration: telegramSessionContextStore.getGeneration,
      isSessionActive: telegramSessionContextStore.isCurrent,
      resolveCurrentIdentity: resolveRecoveryIdentity,
      resolveOperatorIdentity(ctx) {
        const target =
          telegramBusFollowerRegistrationState.getTarget() ??
          findCurrentThreadRecord()?.target ??
          telegramBusLeaderState.getTarget() ??
          (configStore.getAllowedUserId() !== undefined
            ? { chatId: configStore.getAllowedUserId()! }
            : undefined);
        return target ? resolveRecoveryIdentity(target, ctx) : undefined;
      },
      isIdentityAuthenticated: isRecoveryIdentityAuthenticated,
      refreshReassignmentBinding: threadStore.load,
      validateReassignmentBinding(validation) {
        const liveFollowers: InboundRecovery.RecoveryReassignmentLiveFollowerView[] =
          [...telegramBusFollowerRegistry.list()];
        const localFollowerTarget =
          telegramBusFollowerRegistrationState.getTarget();
        const localFollowerGeneration =
          telegramBusFollowerRegistrationState.getGeneration();
        if (
          telegramBusFollowerRegistrationState.isRegistered() &&
          telegramBusFollowerRegistrationState.hasFreshLeaderAck() &&
          localFollowerTarget &&
          localFollowerGeneration
        ) {
          liveFollowers.push({
            target: localFollowerTarget,
            manualFollowerOwnerId: telegramManualFollowerOwnerId,
            registrationGeneration: localFollowerGeneration,
          });
        }
        return InboundRecovery.validateRecoveryReassignmentBindingAuthority({
          validation,
          currentSessionGeneration:
            telegramSessionContextStore.getGeneration(),
          identityAuthenticated: isRecoveryIdentityAuthenticated(
            validation.currentIdentity,
          ),
          threadRecords: threadStore.list(),
          liveFollowers,
        });
      },
      forwardUpdate(input) {
        return telegramBusFollowerClients.foreignOwnedUpdateForwarder.forwardUpdate(
          input,
        );
      },
      isFollowerAdmissionCurrent(proof) {
        const follower = telegramBusFollowerRegistry.getByTarget(proof.target);
        return (
          !!follower &&
          follower.registrationGeneration === proof.registrationGeneration &&
          follower.manualFollowerOwnerId === proof.ownerId
        );
      },
      recordRuntimeEvent,
    });
  getRecoveryStatusProjection = function () {
    try {
      return MenuRecovery.projectTelegramRecoveryDeliverySummary(
        inboundRecoveryRuntime.getRecoveryStatus(),
      );
    } catch {
      return undefined;
    }
  };

  // --- Reply Runtime & Preview ---

  const replyRuntime = Replies.createTelegramRenderedMessageDeliveryRuntime({
    recordOwnership: messageOwnershipRuntime.recordLocal,
    sendMessage,
    sendRichMessage,
    getAssistantRenderingMode: configControls.getAssistantRenderingMode,
    editMessage: editTelegramMessageText,
  });
  const { replyTransport, editInteractiveMessage, sendInteractiveMessage } =
    replyRuntime;
  const deliveryTargetPolicyRuntime =
    Delivery.createTelegramDeliveryTargetPolicyRuntime({
      ownsDirect: lockRuntime.owns,
      isFollowerRegistered: telegramBusFollowerRegistrationState.isRegistered,
      getAllowedChatId: configStore.getAllowedUserId,
      getFollowerTarget: telegramBusFollowerRegistrationState.getTarget,
      getLeaderTarget: telegramBusLeaderState.getTarget,
      listThreadRecords: threadStore.list,
      getActiveTurnTarget: activeTurnRuntime.getTarget,
      getActiveGuestQueryId: activeTurnRuntime.getGuestQueryId,
    });
  const deliveryGenerationSeed = `${telegramInstanceId}:${Date.now()}`;
  const deliveryLifecycleRuntime =
    Delivery.createTelegramBridgeDeliveryLifecycleHooks({
      generationSeed: deliveryGenerationSeed,
      getTargetPolicyView: deliveryTargetPolicyRuntime.getTargetPolicyView,
      getTransportStamp: telegramTransportStampRuntime.getStamp,
      isTransportStampActive: telegramTransportStampRuntime.isActive,
      getActiveTurnTarget: deliveryTargetPolicyRuntime.getActiveTurnTarget,
      api: telegramApiRuntime,
      recordOwnership: messageOwnershipRuntime.recordLocal,
      recordFailure(operation, error, target) {
        recordRuntimeEvent("delivery", error, {
          operation,
          scope: target?.threadId === undefined ? "aggregate" : "thread",
        });
      },
    });
  const { sendTextReply, sendMarkdownReply } =
    Outbound.createTelegramOutboundTextReplyRuntime({
      sendTextReply: replyRuntime.sendTextReply,
      sendMarkdownReply: replyRuntime.sendMarkdownReply,
      execCommand: CommandTemplates.execCommandTemplate,
      getHandlers: configStore.getOutboundHandlers,
      recordRuntimeEvent,
    });
  const assistantOutputBindingRuntime =
    Bindings.createTelegramAssistantOutputBindingRuntime({
      isEnabled: configControls.isProactivePushEnabled,
      authority: {
        getPreferredTarget: proactivePushTargetGetter,
        getFallbackChatId: proactivePushChatIdGetter,
        getTransportStamp: telegramTransportStampRuntime.getStamp,
        isTransportStampActive: telegramTransportStampRuntime.isActive,
        ownsDirect: lockRuntime.owns,
        getDirectEpoch: lockRuntime.getOwnedLeaderEpoch,
        isFollowerRegistered: telegramBusFollowerRegistrationState.isRegistered,
        getFollowerGeneration:
          telegramBusFollowerRegistrationState.getGeneration,
      },
      sender: {
        recordOwnership: messageOwnershipRuntime.recordLocal,
        sendMessage,
        sendRichMessage,
        editMessage: editTelegramMessageText,
        getAssistantRenderingMode: configControls.getAssistantRenderingMode,
        execCommand: CommandTemplates.execCommandTemplate,
        getHandlers: configStore.getOutboundHandlers,
        recordRuntimeEvent,
      },
      recordRuntimeEvent,
    });
  const assistantOutputRuntime = assistantOutputBindingRuntime.runtime;
  const activityRuntime = Activity.createTelegramActivityBridgeRuntime({
    generation: deliveryGenerationSeed,
    observeEvent: assistantOutputBindingRuntime.observeEvent,
    recordFailure(handlerId, event, error) {
      recordRuntimeEvent("activity", error, {
        handlerId,
        eventType: event.type,
        activityId: event.activityId,
      });
    },
  });
  const dispatchNextQueuedTelegramTurn =
    Queue.createTelegramQueueDispatchRuntime({
      ...telegramQueueStore,
      isCompactionInProgress: lifecycle.isCompactionInProgress,
      hasActiveTurn: activeTurnRuntime.has,
      hasDispatchPending: lifecycle.hasDispatchPending,
      isIdle,
      hasPendingMessages,
      hasDispatchContext: deferredQueueDispatchRuntime.isBound,
      getDispatchGeneration: deferredQueueDispatchRuntime.getGeneration,
      isDispatchGenerationActive:
        deferredQueueDispatchRuntime.isGenerationActive,
      isQueueItemTransportActive(item) {
        return telegramTransportStampRuntime.isActive(item.transportStamp);
      },
      updateStatus,
      sendTextReply,
      recordRuntimeEvent,
      ...promptDispatchRuntime,
      sendUserMessage,
      claimPromptDispatch: inboundRecoveryRuntime.claimTurnDispatch,
      onPromptDispatchFailedAfterClaim:
        inboundRecoveryRuntime.markTurnDispatchFailed,
    }).dispatchNext;
  const queueDispatchWatchdogRuntime =
    Queue.createTelegramQueueDispatchWatchdogRuntime({
      hasQueuedItems: telegramQueueStore.hasQueuedItems,
      dispatchNextQueuedTelegramTurn,
      recordRuntimeEvent,
    });
  const nativeMarkdownDraftSender =
    TelegramApi.createTelegramAssistantDraftSender({
      getAssistantRenderingMode: configControls.getAssistantRenderingMode,
      renderMarkdownToHtmlDraft: Replies.renderTelegramMarkdownToHtmlDraft,
      sendMessageDraft,
      sendRichMessageDraft,
    });
  const previewRuntime = Preview.createTelegramAssistantPreviewRuntime({
    getActiveTurn: activeTurnRuntime.get,
    isAssistantMessage: Replies.isAssistantAgentMessage,
    getMessageText: Replies.getAgentMessageText,
    getDefaultReplyToMessageId: activeTurnRuntime.getReplyToMessageId,
    sendDraft: nativeMarkdownDraftSender,
    canSend: configControls.areDraftPreviewsEnabled,
    sendMarkdownReply,
    recordRuntimeEvent,
    ...replyTransport,
  });
  const runPreviewOrderedMutation = async function <TResult>(
    mutation: { (): Promise<TResult> },
  ): Promise<TResult> {
    const finalize = Preview.createTelegramNativeMarkdownPreviewReceiptFinalizer({
      getState: previewRuntime.getState,
      discard() {
        previewRuntime.setState(undefined);
      },
      async sendUnit() {
        return { value: await mutation() };
      },
    });
    const receipt = await finalize();
    if (!receipt) {
      throw new Error("Telegram preview generation changed before outbox send");
    }
    return receipt.value;
  };
  const sendDurableMultipartBytes =
    OutboundRecovery.createTelegramDurableOutboundMultipartBytesSender({
      tempDir: Paths.resolveTelegramTempDir(),
      callMultipart,
    });
  const durableOutboundAdapter =
    OutboundRecovery.createTelegramDurableOutboundUnitAdapter({
      gate: {
        canStart(identity) {
          return (
            inboundRecoveryRuntime.operationGate.getState(identity.profile)
              .phase === "active" &&
            isRecoveryIdentityAuthenticated(identity)
          );
        },
        isActive(identity) {
          return (
            inboundRecoveryRuntime.operationGate.getState(identity.profile)
              .phase === "active" &&
            isRecoveryIdentityAuthenticated(identity)
          );
        },
      },
      sendMessage(body) {
        return runPreviewOrderedMutation(function () {
          return sendMessage(body);
        });
      },
      sendRichMessage(body) {
        return runPreviewOrderedMutation(function () {
          return sendRichMessage(body);
        });
      },
      sendMultipartBytes(method, fields, fileField, bytes, fileName) {
        return runPreviewOrderedMutation(function () {
          return sendDurableMultipartBytes(
            method,
            fields,
            fileField,
            bytes,
            fileName,
          );
        });
      },
      answerGuestQuery(guestQueryId, text, options) {
        return runPreviewOrderedMutation(function () {
          return answerGuestQuery(guestQueryId, text, options);
        });
      },
      deleteMessage: deleteTelegramMessage,
    });
  const durableOutboundOperationFiles =
    OutboundRecovery.createTelegramDurableOutboundOperationFileRuntime({
      operationTempDir: Paths.resolveTelegramTempDir(),
      recordCleanupFailure(error, turnId) {
        recordRuntimeEvent("delivery", error, {
          phase: "guest-response-temp-cleanup",
          turnId,
        });
      },
    });
  const durableOutboundWorker =
    OutboundRecovery.createTelegramDurableOutboundWorker({
      getStore: inboundRecoveryRuntime.getOutboundStore,
      operationGate: inboundRecoveryRuntime.operationGate,
      adapter: durableOutboundAdapter,
      createReplyMarkup(buttons) {
        return {
          inline_keyboard: buttons.map(function (button) {
            return [
              {
                text: button.label,
                callback_data: buttonActionStore.register({
                  text: button.label,
                  prompt: button.prompt,
                }),
              },
            ];
          }),
        };
      },
      recordOwnership({ identity, messageId }) {
        messageOwnershipRuntime.recordLocal({
          chatId: identity.target.chatId,
          messageId,
          target: identity.target,
        });
      },
      async onTerminal({ record }) {
        await durableOutboundOperationFiles.resolveTerminal(record.turnId);
        const ctx = telegramSessionContextStore.get();
        if (!ctx || !telegramSessionContextStore.isCurrent(ctx)) return;
        updateStatus(ctx);
        deferredQueueDispatchRuntime.request(
          dispatchNextQueuedTelegramTurn,
        );
      },
      recordRuntimeEvent,
      startSuspended: true,
    });
  const scheduleDurableOutboundRecovery = function (
    item: Recovery.RecoveryOutboundDrainItem,
    claim: Recovery.RecoveryIdentityClaim,
  ): void {
    durableOutboundWorker.register(item.record, claim);
    durableOutboundWorker.schedule(item.record.recordId);
  };
  const durableOutboundLifecycle = {
    async handoffActiveTurn(
      turn: Queue.PendingTelegramTurn,
      assistant: Queue.TelegramAgentEndAssistantResult,
    ): Promise<Queue.TelegramDurableAgentEndHandoff> {
      const recovery = turn.recovery;
      if (!recovery) {
        throw new Error("Active Telegram turn has no durable inbound record");
      }
      const claim = inboundRecoveryRuntime.getTurnOutboundClaim(turn);
      const automaticVoice = Voice.isVoiceTurn(turn);
      const semanticReply = Outbound.planTelegramDurableOutboundReply(
        assistant.text ?? "",
        { automaticVoice },
      );
      if (semanticReply.markdown) {
        previewRuntime.setPendingText(semanticReply.markdown);
      }
      let generatedVoice: Outbound.TelegramDurableGeneratedVoiceSource[] = [];
      let voiceFallbackToText = false;
      if (semanticReply.voiceReplies.length > 0) {
        try {
          generatedVoice = await Outbound.generateTelegramDurableVoiceSources(
            semanticReply,
            {
              execCommand: CommandTemplates.execCommandTemplate,
              getHandlers: configStore.getOutboundHandlers,
              recordRuntimeEvent,
            },
          );
        } catch (error) {
          voiceFallbackToText = true;
          recordRuntimeEvent("voice", error, {
            phase: "durable-generation-fallback",
          });
        }
      }
      const options: OutboundRecovery.TelegramDurableOutboundPlanOptions = {
        intentId: `final-v1:${recovery.turnId}`,
        turnId: recovery.turnId,
        sourceInboundRecordIds: recovery.recordIds,
        claim,
        replyToMessageId: turn.replyToMessageId,
        renderingMode: configControls.getAssistantRenderingMode(),
        finalMarkdown: assistant.text ?? "",
        queuedAttachments: turn.queuedAttachments,
        generatedVoice,
        automaticVoice,
        voiceFallbackToText,
        ...(turn.guestQueryId
          ? {
              guestQueryId: turn.guestQueryId,
              guestStagingTarget: pairedProactivePushTargetGetter(),
            }
          : {}),
      };
      const store = inboundRecoveryRuntime.getOutboundStore();
      let item: Recovery.RecoveryOutboundDrainItem;
      let operationOwnedFiles: readonly OutboundRecovery.TelegramOperationOwnedPrivateFile[] = [];
      try {
        const committed = await OutboundRecovery.commitTelegramDurableOutbound(
          options,
          {
            store,
            operationTempDir: Paths.resolveTelegramTempDir(),
            transformReply(text) {
              return Outbound.transformTelegramOutboundText(text, {
                handlers: configStore.getOutboundHandlers(),
                execCommand: CommandTemplates.execCommandTemplate,
                recordRuntimeEvent,
              });
            },
          },
        );
        operationOwnedFiles = committed.operationOwnedFiles;
        item = {
          ...committed,
          record: store.activateOutbound(committed.record.recordId, claim),
        };
      } catch (error) {
        const recovered = store.listClaimableOutboundRecords(claim).find(
          function (candidate) {
            return candidate.record.turnId === recovery.turnId &&
              candidate.record.intentId === options.intentId;
          },
        );
        if (!recovered) throw error;
        item = {
          ...recovered,
          record: recovered.record.state === "planned"
            ? store.activateOutbound(recovered.record.recordId, claim)
            : recovered.record,
        };
      }
      if (operationOwnedFiles.length > 0) {
        durableOutboundOperationFiles.track(
          item.record.turnId,
          operationOwnedFiles,
        );
      }
      durableOutboundWorker.register(item.record, claim);
      inboundRecoveryRuntime.releaseTurnAfterOutboundHandoff(turn);
      return {
        startDelivery() {
          durableOutboundWorker.schedule(item.record.recordId);
        },
      };
    },
    completeTurnWithoutDelivery: inboundRecoveryRuntime.completeTurn,
    markTurnExecutionUncertain: inboundRecoveryRuntime.markTurnDispatchFailed,
  };

  // --- Model And Menu Setup ---

  const modelSwitchController =
    Model.createTelegramModelSwitchControllerRuntime({
      isIdle,
      getPendingModelSwitch: pendingModelSwitchStore.get,
      setPendingModelSwitch: pendingModelSwitchStore.set,
      getActiveTurn: activeTurnRuntime.get,
      getAbortHandler: abort.getHandler,
      hasAbortHandler: abort.hasHandler,
      getActiveToolExecutions: lifecycle.getActiveToolExecutions,
      allocateItemOrder: queue.allocateItemOrder,
      allocateControlOrder: queue.allocateControlOrder,
      appendQueuedItem: queueMutationRuntime.append,
      updateStatus,
    });
  const getQueueItemCount =
    Queue.createTelegramQueueItemCountGetter(telegramQueueStore);
  const getPromptTemplateCommands =
    PromptTemplates.createTelegramPromptTemplateCommandGetter({
      getCommands,
      getReservedCommandNames: Commands.getTelegramReservedCommandNames,
    });
  let recoveryMenuRuntime:
    | MenuRecovery.TelegramRecoveryMenuRuntime<Pi.ExtensionContext>
    | undefined;
  let runRecoveryDowngrade:
    | BusLeader.TelegramRecoveryDowngradeCoordinator<Pi.ExtensionContext>
    | undefined;
  let runDurableBusItem:
    | BusLeader.TelegramDurableBusItemRunner
    | undefined;
  const menuActions = Menu.createTelegramMenuActionRuntimeWithStateBuilder({
    runtime: modelMenuRuntime,
    createSettingsManager: Pi.createSettingsManager,
    getActiveModel: currentModelRuntime.get,
    getThinkingLevel,
    getQueueItemCount,
    getRecoveryItemCount() {
      return recoveryMenuRuntime?.getUnresolvedCount() ?? 0;
    },
    buildStatusHtml: Commands.createTelegramAppMenuHtmlBuilder({
      buildStatusHtml: Status.createTelegramStatusHtmlBuilder({
        getActiveModel: currentModelRuntime.get,
        isCompactionInProgress: lifecycle.isCompactionInProgress,
        getBridgeStatusLineState: statusRuntime.getStatusState,
      }),
      getPromptTemplateCommands,
    }),
    storeModelMenuState: modelMenuRuntime.storeState,
    isIdle,
    canOfferInFlightModelSwitch: modelSwitchController.canOfferInFlightSwitch,
    sendTextReply,
    editInteractiveMessage,
    sendInteractiveMessage,
    sectionRegistry,

    // Menu/status UI uses this to reflect whether the active Telegram turn expects voice delivery.
    isVoiceReplyActive: function () {
      const turn = activeTurnRuntime.get();
      return Voice.isVoiceTurn(turn);
    },
  });

  // --- Queue And Settings Menus ---

  const getQueueMenuState = Menu.createTelegramModelMenuStateBuilder({
    runtime: modelMenuRuntime,
    createSettingsManager: Pi.createSettingsManager,
    getActiveModel: currentModelRuntime.get,
  });
  const queueMenuRuntime = MenuQueue.createTelegramQueueMenuRuntime({
    telegramQueueStore,
    queueMutationRuntime,
    sendInteractiveMessage,
    editInteractiveMessage,
    answerCallbackQuery,
    getModelMenuState: getQueueMenuState,
    getStoredModelMenuState: modelMenuRuntime.getState,
    storeModelMenuState: modelMenuRuntime.storeState,
    updateStatusMessage: menuActions.updateStatusMessage,
    updateStatus,
  });
  const settingsMenuRuntime = MenuSettings.createTelegramSettingsMenuRuntime(
    {
      getModelMenuState: getQueueMenuState,
      getStoredModelMenuState: modelMenuRuntime.getState,
      storeModelMenuState: modelMenuRuntime.storeState,
      editInteractiveMessage,
      sendInteractiveMessage,
      answerCallbackQuery,
      ...configControls,
    },
    sectionRegistry,
  );

  // --- Polling ---

  const foreignOwnedUpdateForwarder =
    telegramBusFollowerClients.foreignOwnedUpdateForwarder;
  const followerTargetController = telegramBusFollowerClients.targetController;
  const restoreFollowerThreadTarget =
    Bus.createTelegramBusFollowerThreadRestoreHandler({
      followerRegistry: telegramBusFollowerRegistry,
      followerTargetController,
      onRestored() {
        telegramSyncStateRuntime.markSliceFresh("target-bindings", {
          nowMs: Date.now(),
          action: "follower-thread-restore",
        });
      },
    });
  const observedThreadTargetBinding =
    Polling.createTelegramThreadTargetObservationBinding<Pi.ExtensionContext>();
  const topicLifecycleSync =
    Sync.createTelegramObservedTopicLifecycleSyncHandler({
      topicTargetStore: threadStore,
      isBusEnabled: isTelegramBusRuntimeEnabled,
      callApi: callTelegramApi,
      isTopicProvisioningActive: telegramProvisioningActivity.isActive,
      getCurrentLeaderEpoch,
      getThreadReconciliationMachineState: threadReconciliationRuntime.getState,
      recordThreadReconciliationPlan,
      getSyncState: telegramSyncStateRuntime.getState,
      setSyncState: telegramSyncStateRuntime.setState,
      recordEvent: recordRuntimeEvent,
    });
  const inboundBusProjectionRuntime =
    Routing.createTelegramInboundBusProjectionRuntime({
      instanceId: telegramInstanceId,
      listFollowers: telegramBusFollowerRegistry.list,
      listThreadRecords: threadStore.list,
      getLeaderTarget: telegramBusLeaderState.getTarget,
      isFollowerRegistered: telegramBusFollowerRegistrationState.isRegistered,
      getFollowerTarget: telegramBusFollowerRegistrationState.getTarget,
      getCurrentIdentity: getCurrentInstanceThreadIdentity,
    });
  const inboundRouteRuntime = Routing.createTelegramInboundRouteRuntime({
    configStore,
    callApi: callTelegramApi,
    getCurrentInstanceId() {
      return telegramInstanceId;
    },
    getMessageOwnership: messageOwnershipRuntime.store.get,
    recordMessageOwnership: messageOwnershipRuntime.recordRouted,
    ...inboundBusProjectionRuntime,
    getCurrentLeaderEpoch,
    getThreadReconciliationMachineState: threadReconciliationRuntime.getState,
    recordThreadReconciliationPlan,
    handleTelegramTopicLifecycleUpdate: topicLifecycleSync,
    handleTelegramThreadTargetObserved(_target, ctx) {
      return observedThreadTargetBinding.handle(ctx);
    },
    foreignOwnedUpdateForwarder,
    replaceFollowerThreadTarget: restoreFollowerThreadTarget,
    bridgeRuntime,
    activeTurnRuntime,
    mediaGroupRuntime,
    textGroupRuntime,
    telegramQueueStore,
    queueMutationRuntime,
    decorateRecoveryTurn: inboundRecoveryRuntime.decorateTurn,
    runRecoveryOperation: inboundRecoveryRuntime.runGatedOperation,
    settleDeferredRecoveryMessages:
      inboundRecoveryRuntime.settleDeferredMessages,
    recordDeferredRecoveryFailure:
      inboundRecoveryRuntime.recordDeferredFailure,
    terminalizeDeletedRecoveryMessages:
      inboundRecoveryRuntime.terminalizeDeletedMessageIds,
    modelMenuRuntime,
    currentModelRuntime,
    modelSwitchController,
    menuActions,
    updateSettingsMenuMessage: settingsMenuRuntime.updateSettingsMenuMessage,
    openQueueMenu: queueMenuRuntime.openQueueMenu,
    queueMenuCallbackHandler: queueMenuRuntime.handleCallbackQuery,
    openSettingsMenu: settingsMenuRuntime.openSettingsMenu,
    settingsMenuCallbackHandler: settingsMenuRuntime.handleCallbackQuery,
    recoveryMenuCallbackHandler(query, ctx) {
      return (
        recoveryMenuRuntime?.handleCallbackQuery(query, ctx) ??
        Promise.resolve(false)
      );
    },
    sectionRegistry,
    buttonActionStore,
    inboundHandlerRuntime,
    threadStore,
    updateStatus,
    dispatchNextQueuedTelegramTurn,
    requestDeferredDispatchNextQueuedTelegramTurn:
      deferredQueueDispatchRuntime.request,
    hasDeferredDispatchContext: deferredQueueDispatchRuntime.isBound,
    startTypingLoop: promptDispatchRuntime.startTypingLoop,
    stopTypingLoop: typing.stop,
    answerCallbackQuery,
    editInteractiveMessage,
    sendInteractiveMessage,
    deleteMessage: deleteTelegramMessage,
    answerGuestQuery,
    sendTextReply,
    setMyCommands,
    getCommands,
    downloadFile: downloadTelegramBridgeFile,
    resolveTimeLine: timeInjectionRuntime.resolveLine,
    getThinkingLevel,
    setThinkingLevel,
    persistScopedModelPatterns: Pi.createScopedModelPatternPersister({
      createSettingsManager: Pi.createSettingsManager,
      clearCachedModelMenuInputs: modelMenuRuntime.clearCachedInputs,
    }),
    setModel,
    sendUserMessage,
    isIdle,
    hasPendingMessages,
    compact,
    recordRuntimeEvent,
  });
  const telegramUpdateHandle = Updates.createTelegramUpdateHandle({
    defaultHandle: inboundRouteRuntime.handleUpdate,
    pairingGate: {
      getAllowedUserId: configStore.getAllowedUserId,
      claim: pairingRuntime.claim,
      async sendGenericResponse(target) {
        await sendTextReply(target.chatId, target.messageId, target.text, {
          target: {
            chatId: target.chatId,
            ...(target.threadId !== undefined
              ? { threadId: target.threadId }
              : {}),
          },
        });
      },
      onPaired: updateStatus,
      recordSideEffectFailure(phase, error) {
        recordRuntimeEvent("pairing", error, { phase });
      },
    },
  });
  const promoteTelegramBusFollowerToLeader: BusFollower.TelegramBusFollowerPromotionHandler<Pi.ExtensionContext> =
    BusFollower.createTelegramBusFollowerPromotionHandler<Pi.ExtensionContext>({
      topicTargetStore: threadStore,
      instanceId: telegramInstanceId,
      getActiveProfileName: getActiveTelegramThreadProfile,
      async startLeader(ctx, election, onAcquired): Promise<boolean> {
        const result = await lockedPollingRuntime.start(ctx, {
          election,
          onAcquired,
        });
        return result.ok;
      },
      recordRuntimeEvent,
    });
  const forwardedRouteHandlers =
    BusFollower.createTelegramBusForwardedRouteHandlers<
      Pi.ExtensionContext,
      Updates.TelegramMessageReactionUpdated,
      Routing.TelegramRoutedCallbackQuery,
      Routing.TelegramRoutedMessage
    >({
      async handleUpdate(update, ctx) {
        await inboundRouteRuntime.handleUpdate(
          update as TelegramApi.TelegramUpdate,
          ctx,
        );
      },
      async handleAuthorizedReactionUpdate(reactionUpdate, ctx) {
        await inboundRouteRuntime.handleAuthorizedReactionUpdate(
          reactionUpdate,
          ctx,
        );
      },
    });
  const appendRecoveryTurn = function (
    turn: Queue.PendingTelegramTurn,
    ctx: Pi.ExtensionContext,
  ): boolean {
    const result = Queue.appendTelegramPromptTurnOnce(
      telegramQueueStore.getQueuedItems(),
      turn,
    );
    if (!result.appended) return false;
    telegramQueueStore.setQueuedItems(result.items);
    updateStatus(ctx);
    dispatchNextQueuedTelegramTurn(ctx);
    return true;
  };
  recoveryMenuRuntime = MenuRecovery.createTelegramRecoveryMenuRuntime({
    getStatus: inboundRecoveryRuntime.getRecoveryStatus,
    getOrphanCandidates:
      inboundRecoveryRuntime.getOrphanReassignmentCandidates,
    async drainSafe(ctx) {
      const count = await inboundRecoveryRuntime.drainSafeForOperator(
        ctx,
        telegramUpdateHandle,
        function (turn) {
          return appendRecoveryTurn(turn, ctx);
        },
        scheduleDurableOutboundRecovery,
      );
      const busItems = inboundRecoveryRuntime.getOutboundStore().drainSafeBus();
      if (busItems.length > 0 && !runDurableBusItem) {
        throw new Error("Durable bus recovery executor is unavailable");
      }
      for (const item of busItems) await runDurableBusItem!(item);
      return count + busItems.length;
    },
    async retryUncertain(actionId, ctx) {
      const item = MenuRecovery.findTelegramRecoveryStatusItem(
        inboundRecoveryRuntime.getRecoveryStatus(),
        actionId,
      );
      if (item?.family === "bus") {
        if (!runDurableBusItem) {
          throw new Error("Durable bus recovery executor is unavailable");
        }
        const retry = inboundRecoveryRuntime
          .getOutboundStore()
          .retryUncertainBusAction(
            actionId,
            telegramBusFollowerClients.createRequestId(),
          );
        const scheduled = await runDurableBusItem(retry);
        return { scheduled, duplicationWarning: true as const };
      }
      return inboundRecoveryRuntime.retryUncertainForOperator(
        actionId,
        ctx,
        telegramUpdateHandle,
        function (turn) {
          return appendRecoveryTurn(turn, ctx);
        },
        scheduleDurableOutboundRecovery,
      );
    },
    async discard(actionId, ctx) {
      const item = MenuRecovery.findTelegramRecoveryStatusItem(
        inboundRecoveryRuntime.getRecoveryStatus(),
        actionId,
      );
      if (item?.family === "bus") {
        inboundRecoveryRuntime.getOutboundStore().discardBusAction(actionId);
        return;
      }
      let discardedTurnId: string | undefined;
      inboundRecoveryRuntime.discardForOperator(
        actionId,
        ctx,
        function (turnId) {
          discardedTurnId = turnId;
          const current = telegramQueueStore.getQueuedItems();
          const remaining = current.filter(
            function (item) {
              return item.kind !== "prompt" || item.recovery?.turnId !== turnId;
            },
          );
          if (remaining.length !== current.length) {
            telegramQueueStore.setQueuedItems(remaining);
          }
        },
      );
      if (discardedTurnId) {
        await durableOutboundOperationFiles.resolveTerminal(discardedTurnId);
      }
      updateStatus(ctx);
      deferredQueueDispatchRuntime.request(
        dispatchNextQueuedTelegramTurn,
      );
    },
    async reassign(actionId, ctx) {
      await inboundRecoveryRuntime.reassignForOperator(actionId, ctx);
    },
    downgrade(ctx) {
      if (!runRecoveryDowngrade) {
        throw new Error("Recovery downgrade coordinator is unavailable");
      }
      return runRecoveryDowngrade(ctx);
    },
    getStoredModelMenuState: modelMenuRuntime.getState,
    editInteractiveMessage,
    answerCallbackQuery,
    recordRuntimeEvent,
  });

  const telegramBusFollowerAssembly: BusFollower.TelegramBusFollowerRuntimeAssembly<Pi.ExtensionContext> =
    BusFollower.createTelegramBusFollowerRuntimeAssembly<
      Pi.ExtensionContext,
      Updates.TelegramMessageReactionUpdated,
      Routing.TelegramRoutedCallbackQuery,
      Routing.TelegramRoutedMessage
    >({
      receiver: {
        socketPath: getTelegramBusFollowerSocketPath,
        instanceId: telegramInstanceId,
        getContext: telegramSessionContextStore.get,
        getProfile: configStore.getActiveProfileName,
        getTarget: telegramBusFollowerRegistrationState.getTarget,
        getSessionGeneration: telegramSessionContextStore.getGeneration,
        manualFollowerOwnerId: telegramManualFollowerOwnerId,
        getAuthSecret() {
          return telegramActiveBusAuthSecret;
        },
        recoveryFence: {
          enter() {
            return inboundRecoveryRuntime.enterOperation();
          },
          beginFencing(profile, fenceGeneration) {
            return inboundRecoveryRuntime.operationGate.beginFencing(
              profile,
              fenceGeneration,
            );
          },
          awaitDrained(profile, fenceGeneration) {
            return inboundRecoveryRuntime.operationGate.awaitDrained(
              profile,
              fenceGeneration,
            );
          },
          resumeAfter(profile, fenceGeneration, resumeRuntime) {
            return inboundRecoveryRuntime.operationGate.resumeAfter(
              profile,
              fenceGeneration,
              resumeRuntime,
            );
          },
          async suspendRuntime() {
            mediaGroupRuntime.suspend();
            textGroupRuntime.suspend();
            queueDispatchWatchdogRuntime.stop();
            deferredQueueDispatchRuntime.unbind();
            await durableOutboundWorker.suspend();
          },
          async resumeRuntime(ctx) {
            deferredQueueDispatchRuntime.bind(ctx);
            mediaGroupRuntime.resume(ctx);
            textGroupRuntime.resume(ctx);
            queueDispatchWatchdogRuntime.start(ctx);
            await durableOutboundWorker.resume();
            dispatchNextQueuedTelegramTurn(ctx);
          },
        },
        async handleForwardedUpdate(input, ctx) {
          const update = input.update as TelegramApi.TelegramUpdate;
          const proof = await inboundRecoveryRuntime.admitForwardedUpdate(
            update,
            ctx,
            input,
          );
          await inboundRecoveryRuntime.handleAdmittedUpdate(
            update,
            ctx,
            telegramUpdateHandle,
          );
          return proof;
        },
        ...forwardedRouteHandlers,
        prepareForwardedMessage: textGroupRuntime.prepareForwardedMessage,
        recordRuntimeEvent,
      },
      targetReplacement: {
        topicTargetStore: threadStore,
        registrationState: telegramBusFollowerRegistrationState,
        instanceId: telegramInstanceId,
        getManualFollowerProfileKey: getTelegramManualFollowerProfileKey,
        manualFollowerOwnerId: telegramManualFollowerOwnerId,
        getSyncState: telegramSyncStateRuntime.getState,
        setSyncState: telegramSyncStateRuntime.setState,
        updateStatus,
        recordRuntimeEvent,
      },
      recovery: {
        registrationState: telegramBusFollowerRegistrationState,
        getLeaderState: lockRuntime.getState,
        setLifecyclePhase(phase) {
          telegramBusLifecycleOverridePhase = phase;
        },
        updateStatus,
        promoteToLeader: promoteTelegramBusFollowerToLeader,
        getActiveContext: telegramSessionContextStore.get,
        recordRuntimeEvent,
      },
      registration: {
        instanceId: telegramInstanceId,
        manualFollowerOwnerId: telegramManualFollowerOwnerId,
        getFollowerBusSocketPath: getTelegramBusFollowerSocketPath,
        getLeaderSocketPath: getTelegramBusSocketPath,
        registrationState: telegramBusFollowerRegistrationState,
        isContextActive: telegramSessionContextStore.isCurrent,
        createRequestId: telegramBusFollowerClients.createRequestId,
        getLeaderAuthSecret(owner) {
          return owner.busSecret;
        },
        setActiveAuthSecret(secret) {
          telegramActiveBusAuthSecret = secret;
        },
        getProfileKey: getTelegramManualFollowerProfileKey,
        getSessionGeneration: telegramSessionContextStore.getGeneration,
        recordRuntimeEvent,
      },
    });
  const telegramBusFollowerRegistration =
    telegramBusFollowerAssembly.registration;
  const pollingTerminalLeaseBinding =
    Polling.createTelegramPollingTerminalLeaseBinding();
  const pollingRuntime = Polling.createTelegramPollingControllerRuntime({
    state: pollingControllerState,
    getConfig: configStore.get,
    hasBotToken: configStore.hasBotToken,
    deleteWebhook,
    getUpdates,
    persistConfig: persistTelegramPollingOffset,
    prepareUpdateBatch: textGroupRuntime.prepareUpdateBatch,
    handleUpdate: telegramUpdateHandle,
    durableInbound: inboundRecoveryRuntime,
    stopTypingLoop: typing.stop,
    updateStatus,
    isPermanentError: TelegramApi.isTelegramApiPermanentAuthError,
    async onTerminalFailure(info) {
      recordRuntimeEvent("polling", "Telegram polling terminalized", {
        phase: info.phase,
        restartCount: info.restartCount,
        generation: info.generation,
        startedAtMs: info.startedAtMs,
        failedAtMs: info.failedAtMs,
        permanent: info.permanent,
      });
      await pollingTerminalLeaseBinding.handle(info);
    },
    recordRuntimeEvent,
  });
  const recoverStaleTelegramTopicApiError =
    Sync.createTelegramStaleTopicApiErrorRecoveryRuntime({
      topicTargetStore: threadStore,
      getSyncState: telegramSyncStateRuntime.getState,
      setSyncState: telegramSyncStateRuntime.setState,
      recordEvent: recordRuntimeEvent,
    });
  const authorizeFollowerApiCall = Bus.createTelegramFollowerApiCallAuthorizer({
    isMessageOwned: messageOwnershipRuntime.isOwnedByFollower,
  });
  const telegramBusLeaderRuntime =
    BusLeader.createTelegramBusLeaderRuntimeAssembly<Pi.ExtensionContext>({
      runtime: {
        socketPath: getTelegramBusSocketPath,
        commitEndpointPublication(commit) {
          return lockRuntime.commitIfOwned(commit);
        },
        followerRegistry: telegramBusFollowerRegistry,
        authSecret: telegramBusAuthSecret,
        startPolling: pollingRuntime.start,
        stopPolling: pollingRuntime.stop,
        authorizeFollowerApiCall,
        recordFollowerMessageOwnership(record) {
          messageOwnershipRuntime.recordFollower(record);
        },
        verifyFollowerDurableAdmission({ proof }) {
          return inboundRecoveryRuntime.verifyFollowerAdmissionProof(proof);
        },
        enterRecoveryOperation: inboundRecoveryRuntime.enterOperation,
        durableBus: {
          getStore: inboundRecoveryRuntime.getOutboundStore,
          getProfile() {
            return configStore.getActiveProfileName() ?? "default";
          },
          getLeaderSessionGeneration:
            telegramSessionContextStore.getGeneration,
        },
      },
      getAllowedUserId: configStore.getAllowedUserId,
      instanceId: telegramInstanceId,
      getCwd: Pi.getExtensionContextCwd,
      getTelegramProfile: getActiveTelegramThreadProfile,
      shouldForceFreshUnnamed:
        telegramThreadCapabilityState.shouldForceFreshLeaderThread,
      topicTargetStore: threadStore,
      callApi(method, body) {
        return directTelegramApiRuntime.call(method, body);
      },
      callMultipart: directTelegramApiRuntime.callMultipart,
      downloadFile: directTelegramApiRuntime.downloadFile,
      recoverStaleTargetError: recoverStaleTelegramTopicApiError,
      getCurrentLeaderEpoch,
      getThreadReconciliationMachineState: threadReconciliationRuntime.getState,
      recordThreadReconciliationPlan,
      getSyncState: telegramSyncStateRuntime.getState,
      setSyncState: telegramSyncStateRuntime.setState,
      setLeaderTarget: telegramBusLeaderState.set,
      onProvisioningStart: telegramProvisioningActivity.start,
      onProvisioningEnd: telegramProvisioningActivity.end,
      recordRuntimeEvent,
    });
  runDurableBusItem = async function (item) {
    const envelope = item.payload.envelope;
    const response = await telegramBusLeaderRuntime.handleEnvelope({
      kind: "follower.callApi",
      requestId: envelope.requestId,
      auth: telegramBusAuthSecret,
      profile: envelope.profile,
      target: envelope.target,
      instanceId: envelope.instanceId,
      manualFollowerOwnerId: envelope.manualFollowerOwnerId,
      registrationGeneration: envelope.registrationGeneration,
      followerSessionGeneration: envelope.followerSessionGeneration,
      method: envelope.method,
      args: envelope.args,
      sentAtMs: Date.now(),
    });
    return response.kind === "bus.ack" && response.ok;
  };
  const telegramLeaderHealthRuntime = Sync.createTelegramLeaderHealthRuntime({
    callGetMe() {
      return directTelegramApiRuntime.call("getMe", {});
    },
    getSyncState: telegramSyncStateRuntime.getState,
    setSyncState: telegramSyncStateRuntime.setState,
    recordEvent: recordRuntimeEvent,
  });
  const telegramThreadCapabilityRuntime =
    Polling.createTelegramThreadCapabilityOrchestration<
      Pi.ExtensionContext,
      Locks.TelegramLockEntry
    >({
      state: telegramThreadCapabilityState,
      getAllowedUserId: configStore.getAllowedUserId,
      callApi: callTelegramApi,
      topicTargetStore: threadStore,
      isBusConfigured: isTelegramBusConfigured,
      isBusRuntimeEnabled: isTelegramBusRuntimeEnabled,
      ownsLock: lockRuntime.owns,
      startClassicPolling: pollingRuntime.start,
      stopClassicPolling: pollingRuntime.stop,
      startBusLeaderPolling: telegramBusLeaderRuntime.startPolling,
      stopBusLeaderPolling: telegramBusLeaderRuntime.stopPolling,
      startLeaderHealth: telegramLeaderHealthRuntime.start,
      stopLeaderHealth: telegramLeaderHealthRuntime.stop,
      registerFollowerWithLeader:
        telegramBusFollowerRegistration.registerWithLeader,
      stopFollowerRegistration: telegramBusFollowerRegistration.stop,
      isTopicModeUnavailableError: Threads.isTelegramTopicModeUnavailableError,
      updateStatus,
      recordEvent: recordRuntimeEvent,
    });
  const telegramThreadCapabilityMonitor =
    telegramThreadCapabilityRuntime.monitor;
  observedThreadTargetBinding.set(
    telegramThreadCapabilityRuntime.observeTarget,
  );
  const threadAwarePollingPorts = telegramThreadCapabilityRuntime.pollingPorts;
  const lockedPollingRuntime = Locks.createTelegramLockedPollingRuntime({
    lock: lockRuntime,
    hasBotToken: configStore.hasBotToken,
    canStartPolling: Pi.canStartPollingInExtensionContext,
    formatStartBlockedMessage: Pi.formatPollingStartBlockedByRunMode,
    startPolling: threadAwarePollingPorts.startPolling,
    stopPolling: threadAwarePollingPorts.stopPolling,
    registerFollowerWithOwner:
      threadAwarePollingPorts.registerFollowerWithOwner,
    stopFollowerRegistration: threadAwarePollingPorts.stopFollowerRegistration,
    updateStatus,
    recordRuntimeEvent,
  });
  runRecoveryDowngrade =
    BusLeader.createTelegramRecoveryDowngradeCoordinator<Pi.ExtensionContext>({
      canCoordinate: lockRuntime.owns,
      getCoordinationGeneration() {
        const epoch = lockRuntime.getOwnedLeaderEpoch();
        return lockRuntime.owns() && epoch !== undefined ? String(epoch) : undefined;
      },
      getProfile() {
        return configStore.getActiveProfileName() ?? "default";
      },
      getFollowers: telegramBusFollowerRegistry.list,
      getAuthSecret() {
        return telegramBusAuthSecret;
      },
      createRequestId: telegramBusFollowerClients.createRequestId,
      gate: inboundRecoveryRuntime.operationGate,
      preflight: inboundRecoveryRuntime.downgradePreflight,
      beginStoreExclusive: inboundRecoveryRuntime.beginDowngradeExclusive,
      quarantineStore: inboundRecoveryRuntime.quarantineForDowngrade,
      cancelStoreExclusive: inboundRecoveryRuntime.cancelDowngradeExclusive,
      stopPolling: pollingRuntime.stop,
      async suspendRuntime() {
        telegramThreadCapabilityMonitor.stop();
        mediaGroupRuntime.suspend();
        textGroupRuntime.suspend();
        queueDispatchWatchdogRuntime.stop();
        deferredQueueDispatchRuntime.unbind();
        await durableOutboundWorker.suspend();
      },
      async resumeRuntime(ctx) {
        deferredQueueDispatchRuntime.bind(ctx);
        mediaGroupRuntime.resume(ctx);
        textGroupRuntime.resume(ctx);
        queueDispatchWatchdogRuntime.start(ctx);
        await durableOutboundWorker.resume();
        await pollingRuntime.restartAfterStop(ctx);
        telegramThreadCapabilityMonitor.start(ctx);
        dispatchNextQueuedTelegramTurn(ctx);
      },
      recordRuntimeEvent,
    });
  pollingTerminalLeaseBinding.set(
    Polling.createTelegramPollingTerminalLeaseHandler({
      state: pollingControllerState,
      terminalizeTransportLease:
        lockedPollingRuntime.terminalizeTransportLease,
    }),
  );
  const disconnectTelegramAndDeleteCurrentThread =
    Sync.createTelegramManualThreadDisconnectHandler({
      instanceId: telegramInstanceId,
      getCurrentThreadRecord: findCurrentThreadRecord,
      topicTargetStore: threadStore,
      callApi: callTelegramApi,
      getCurrentLeaderEpoch,
      getLeaderTarget: telegramBusLeaderState.getTarget,
      clearLeaderTarget: telegramBusLeaderState.clear,
      disconnectFollowerThread:
        telegramBusFollowerRegistration.disconnectFromLeader,
      getSyncState: telegramSyncStateRuntime.getState,
      setSyncState: telegramSyncStateRuntime.setState,
      stopPolling: lockedPollingRuntime.stop,
      recordRuntimeEvent,
    });
  const baseSessionLifecycleRuntime =
    Lifecycle.createTelegramBridgeSessionLifecycleAssembly({
      contextStore: telegramSessionContextStore,
      queue: {
        getCurrentModel: getContextModel,
        loadConfig: configStore.load,
        setQueuedItems: telegramQueueStore.setQueuedItems,
        setCurrentModel: currentModelRuntime.set,
        setPendingModelSwitch: pendingModelSwitchStore.set,
        syncCounters: queue.syncCounters,
        syncFlags: lifecycle.syncFlags,
        bindDeferredDispatchContext: deferredQueueDispatchRuntime.bind,
        prepareTempDir,
        updateStatus,
        unbindDeferredDispatchContext: deferredQueueDispatchRuntime.unbind,
        clearModelMenuState: modelMenuRuntime.clear,
        getActiveTurnChatId: activeTurnRuntime.getChatId,
        getActiveTurnTarget: activeTurnRuntime.getTarget,
        clearPreview: previewRuntime.clear,
        clearActiveTurn: activeTurnRuntime.clear,
        clearAbort: abort.clearHandler,
        recordRuntimeEvent,
      },
      follower: {
        registrationState: telegramBusFollowerRegistrationState,
        registrationRuntime: telegramBusFollowerRegistration,
        instanceId: telegramInstanceId,
        suspendPolling: lockedPollingRuntime.suspend,
        isLeader: lockRuntime.owns,
        getLeaderBinding: currentInstanceThreadRuntime.getRestorationIdentity,
        getActiveContext: telegramSessionContextStore.get,
        getActiveProfileName: getActiveTelegramThreadProfile,
        getLeaderState: lockRuntime.getState,
        updateStatus,
        recordRuntimeEvent,
      },
      services: {
        recovery: {
          async onSessionStart(_event, ctx) {
            if (
              !lockRuntime.owns(ctx) &&
              !telegramBusFollowerRegistrationState.isRegistered()
            ) {
              return;
            }
            await inboundRecoveryRuntime.rehydrateOutbound(
              ctx,
              scheduleDurableOutboundRecovery,
            );
            await durableOutboundWorker.resume();
            await inboundRecoveryRuntime.rehydrate(
              ctx,
              function (update, recoveryCtx) {
                return telegramUpdateHandle(
                  update as TelegramApi.TelegramUpdate,
                  recoveryCtx,
                );
              },
              function (turn) {
                const result = Queue.appendTelegramPromptTurnOnce(
                  telegramQueueStore.getQueuedItems(),
                  turn,
                );
                if (result.appended) {
                  telegramQueueStore.setQueuedItems(result.items);
                }
              },
            );
          },
        },
        resumeGroupedInput(ctx) {
          mediaGroupRuntime.resume(ctx);
          textGroupRuntime.resume(ctx);
        },
        suspendGroupedInput: TextGroups.createTelegramGroupedInputClearer({
          clearMediaGroups: mediaGroupRuntime.suspend,
          clearTextGroups: textGroupRuntime.suspend,
        }),
        delivery: deliveryLifecycleRuntime,
        polling: lockedPollingRuntime,
        capabilityMonitor: telegramThreadCapabilityMonitor,
        queueWatchdog: queueDispatchWatchdogRuntime,
      },
    });
  const sessionLifecycleRuntime = {
    ...baseSessionLifecycleRuntime,
    async onSessionShutdown(
      event: Pi.SessionShutdownEvent,
      ctx: Pi.ExtensionContext,
    ) {
      if (telegramSessionContextStore.isCurrent(ctx)) {
        inboundRecoveryRuntime.publishSessionHandoffs(
          telegramSessionContextStore.getGeneration() + 2,
        );
      }
      await durableOutboundWorker.suspend();
      await baseSessionLifecycleRuntime.onSessionShutdown(event, ctx);
    },
    async onSessionStart(event: Pi.SessionStartEvent, ctx: Pi.ExtensionContext) {
      const previousContext = telegramSessionContextStore.get();
      if (
        previousContext &&
        telegramSessionContextStore.isCurrent(previousContext) &&
        (lockRuntime.owns(previousContext) ||
          telegramBusFollowerRegistrationState.isRegistered())
      ) {
        inboundRecoveryRuntime.publishSessionHandoffs(
          telegramSessionContextStore.getGeneration() + 1,
        );
      }
      await baseSessionLifecycleRuntime.onSessionStart(event, ctx);
      dispatchNextQueuedTelegramTurn(ctx);
    },
  };

  // --- Extension API Bindings ---

  Bindings.registerTelegramCommandsAndTools({
    pi,
    configStore,
    persistConfig: persistTelegramConfigWithSync,
    getPairingInstructions: pairingRuntime.getLocalInstructions,
    setup,
    activeTurnRuntime,
    lockedPollingRuntime,
    resumeDurableOutboundWorker: durableOutboundWorker.resume,
    stopPolling: disconnectTelegramAndDeleteCurrentThread,
    getDisconnectThreadName() {
      const record = findCurrentThreadRecord();
      if (!record?.target.threadId) return undefined;
      return record.threadName ?? "current Telegram thread";
    },
    onTransportChanged: deliveryLifecycleRuntime.onSessionStart,
    getStatusLines,
    buttonActionStore,
    sendMarkdownReply,
    callMultipart,
    getDefaultChatId: proactivePushChatIdGetter,
    getDefaultTarget: proactivePushTargetGetter,
    canSendDirect() {
      return (
        ownsTelegramDirectDelivery() ||
        telegramBusFollowerRegistrationState.isRegistered()
      );
    },
    updateStatus,
    recordRuntimeEvent,
  });

  // --- Lifecycle Hooks ---

  Bindings.registerTelegramLifecycleRuntimeHooks({
    pi,
    sessionLifecycleRuntime: {
      ...sessionLifecycleRuntime,
      onModelSelect: currentModelRuntime.onModelSelect,
    },
    activityRuntime,
    assistantOutputRuntime,
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
    durableOutbound: durableOutboundLifecycle,
    proactivePushTargetGetter,
    isProactivePushEnabled: configControls.isProactivePushEnabled,
    canSendAgentActivity(ctx) {
      return (
        lockOwnershipGuard.ownsContext(ctx) ||
        telegramBusFollowerRegistrationState.isRegistered()
      );
    },
    isSessionContextActive(ctx) {
      return telegramSessionContextStore.isCurrent(ctx);
    },
    updateStatus,
    recordRuntimeEvent,
  });
}
