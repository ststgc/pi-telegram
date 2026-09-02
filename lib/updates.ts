/**
 * Telegram updates domain helpers
 * Zones: telegram inbound, authorization, routing plans
 * Owns update extraction, authorization, classification, execution planning, runtime execution, and the public update-handler registry
 */

import {
  isTelegramFollowerDurableAdmissionAckV1,
  type TelegramFollowerDurableAdmissionAckV1,
} from "./bus.ts";
import {
  createTelegramPrivateTarget,
  createTelegramThreadTarget,
  type TelegramTarget,
} from "./target.ts";
import type { TelegramMessageOwnershipStore } from "./ownership.ts";
import {
  getTelegramAuthorizationState,
  isValidTelegramAllowedUserId,
  type TelegramAuthorizationState,
} from "./config.ts";
import {
  isTelegramPairingProofShapedUpdate,
  parseTelegramPairingCandidate,
  TELEGRAM_PAIRING_RESPONSE,
  type TelegramPairingClaimInput,
  type TelegramPairingClaimResult,
} from "./pairing.ts";

// --- Extraction ---

export interface TelegramReactionTypeEmoji {
  type: "emoji";
  emoji: string;
}

export interface TelegramReactionTypeNonEmoji {
  type: string;
}

export type TelegramReactionType =
  | TelegramReactionTypeEmoji
  | TelegramReactionTypeNonEmoji;

export const TELEGRAM_PRIORITY_REACTIONS = [
  { id: 10, name: "like", emoji: "👍" },
  { id: 11, name: "lightning", emoji: "⚡" },
  { id: 12, name: "heart", emoji: "❤" },
  { id: 13, name: "dove", emoji: "🕊" },
  { id: 14, name: "fire", emoji: "🔥" },
] as const;
export const TELEGRAM_REMOVAL_REACTIONS = [
  { id: 20, name: "dislike", emoji: "👎" },
  { id: 21, name: "ghost", emoji: "👻" },
  { id: 22, name: "broken-heart", emoji: "💔" },
  { id: 23, name: "poop", emoji: "💩" },
  { id: 24, name: "wastebasket", emoji: "🗑" },
] as const;
export const TELEGRAM_PRIORITY_REACTION_EMOJIS =
  TELEGRAM_PRIORITY_REACTIONS.map((reaction) => reaction.emoji);
export const TELEGRAM_REMOVAL_REACTION_EMOJIS = TELEGRAM_REMOVAL_REACTIONS.map(
  (reaction) => reaction.emoji,
);

export interface TelegramUpdateDeletion {
  deleted_business_messages?: {
    business_connection_id?: unknown;
    chat?: { id?: unknown };
    message_ids?: unknown;
  };
}

export interface TelegramDeletedMessagesScope {
  chatId?: number;
  businessConnectionId?: string;
}

function isTelegramMessageIdList(value: unknown): value is number[] {
  return Array.isArray(value) && value.every((item) => Number.isInteger(item));
}

export function normalizeTelegramReactionEmoji(emoji: string): string {
  return emoji.replace(/\uFE0F/g, "");
}

export function collectTelegramReactionEmojis(
  reactions: TelegramReactionType[],
): Set<string> {
  return new Set(
    reactions
      .filter(
        (reaction): reaction is TelegramReactionTypeEmoji =>
          reaction.type === "emoji",
      )
      .map((reaction) => normalizeTelegramReactionEmoji(reaction.emoji)),
  );
}

function hasAnyTelegramReactionEmoji(
  emojis: Set<string>,
  candidates: readonly string[],
): boolean {
  return candidates.some((emoji) => emojis.has(emoji));
}

function getAddedTelegramReactionEmoji(
  oldEmojis: Set<string>,
  newEmojis: Set<string>,
  candidates: readonly string[],
): string | undefined {
  return candidates.find(
    (emoji) => !oldEmojis.has(emoji) && newEmojis.has(emoji),
  );
}
function hasAddedTelegramReactionEmoji(
  oldEmojis: Set<string>,
  newEmojis: Set<string>,
  candidates: readonly string[],
): boolean {
  return !!getAddedTelegramReactionEmoji(oldEmojis, newEmojis, candidates);
}

export function extractDeletedTelegramMessageIds(
  update: TelegramUpdateDeletion,
): number[] {
  const deletedBusinessMessageIds =
    update.deleted_business_messages?.message_ids;
  if (isTelegramMessageIdList(deletedBusinessMessageIds)) {
    return deletedBusinessMessageIds;
  }
  return [];
}

// --- Routing ---

export interface TelegramUser {
  id: number;
  is_bot: boolean;
}

export interface TelegramChat {
  id?: number;
  type: string;
}

export interface TelegramUpdateMessage {
  chat: TelegramChat;
  from?: TelegramUser;
  message_id?: number;
  message_thread_id?: number;
  text?: string;
  reply_to_message?: TelegramUpdateMessage;
  forum_topic_created?: unknown;
  forum_topic_closed?: unknown;
  forum_topic_reopened?: unknown;
}

export type TelegramTopicLifecycleKind = "created" | "closed" | "reopened";

export interface TelegramTopicLifecycleUpdate<
  TMessage = TelegramUpdateMessage,
> {
  kind: TelegramTopicLifecycleKind;
  message: TMessage;
  target: TelegramTarget & { threadId: number };
}

export function getTelegramTopicLifecycleUpdate<
  TMessage extends TelegramUpdateMessage,
>(
  message: TMessage | undefined,
): TelegramTopicLifecycleUpdate<TMessage> | undefined {
  if (
    !message ||
    typeof message.chat.id !== "number" ||
    typeof message.message_thread_id !== "number"
  ) {
    return undefined;
  }
  const target: TelegramTarget & { threadId: number } = {
    ...createTelegramThreadTarget(message.chat.id, message.message_thread_id),
    threadId: message.message_thread_id,
  };
  if (message.forum_topic_created !== undefined) {
    return { kind: "created", message, target };
  }
  if (message.forum_topic_closed !== undefined) {
    return { kind: "closed", message, target };
  }
  if (message.forum_topic_reopened !== undefined) {
    return { kind: "reopened", message, target };
  }
  return undefined;
}

export interface TelegramCallbackQuery {
  id?: string;
  from: TelegramUser;
  message?: TelegramUpdateMessage;
  data?: string;
}

export interface TelegramGuestMessage {
  guest_query_id: string;
  chat: TelegramChat;
  from?: TelegramUser;
  message_id?: number;
  text?: string;
  reply_to_message?: TelegramUpdateMessage;
}

export function getTelegramMessageTarget(
  message: TelegramUpdateMessage,
): TelegramTarget | undefined {
  if (typeof message.chat.id !== "number") return undefined;
  return typeof message.message_thread_id === "number"
    ? createTelegramThreadTarget(message.chat.id, message.message_thread_id)
    : createTelegramPrivateTarget(message.chat.id);
}

export interface TelegramUpdateRouting {
  update_id?: number;
  message?: TelegramUpdateMessage;
  edited_message?: TelegramUpdateMessage;
  callback_query?: TelegramCallbackQuery;
  guest_message?: TelegramGuestMessage;
}

export function getAuthorizedTelegramCallbackQuery(
  update: TelegramUpdateRouting,
  allowedUserId?: number,
): TelegramCallbackQuery | undefined {
  const query = update.callback_query;
  if (!query || query.from.is_bot) return undefined;
  const message = query.message;
  if (!message) return undefined;
  if (message.chat.type === "private") return query;
  return query.from.id === allowedUserId ? query : undefined;
}

export function getAuthorizedTelegramMessage(
  update: TelegramUpdateRouting,
  allowedUserId?: number,
): TelegramUpdateMessage | undefined {
  const message = update.message;
  if (!message || !message.from || message.from.is_bot) return undefined;
  if (message.chat.type === "private") return message;
  return message.from.id === allowedUserId ? message : undefined;
}

export function getAuthorizedTelegramEditedMessage(
  update: TelegramUpdateRouting,
  allowedUserId?: number,
): TelegramUpdateMessage | undefined {
  const message = update.edited_message;
  if (!message || !message.from || message.from.is_bot) return undefined;
  if (message.chat.type === "private") return message;
  return message.from.id === allowedUserId ? message : undefined;
}

export function getAuthorizedTelegramGuestMessage(
  update: TelegramUpdateRouting,
): TelegramGuestMessage | undefined {
  const guestMessage = update.guest_message;
  if (!guestMessage || !guestMessage.from || guestMessage.from.is_bot) {
    return undefined;
  }
  return guestMessage;
}

// --- Flow ---

export interface TelegramMessageOwnershipView {
  instanceId: string;
  ownerGeneration?: string;
  target?: TelegramTarget;
  purpose?: "interaction";
}

export type TelegramMessageOwnershipLookup = (
  chatId: number,
  messageId: number,
) => TelegramMessageOwnershipView | undefined;

export type TelegramMessageOwnershipClassificationLookup = (
  chatId: number,
  messageId: number,
) => { purpose?: "interaction" } | undefined;

export interface TelegramTargetOwnershipView {
  instanceId: string;
  ownerGeneration?: string;
}

export type TelegramTargetOwnershipLookup = (
  target: TelegramTarget,
) => TelegramTargetOwnershipView | undefined;

export type TelegramFollowerForwardingResult =
  | boolean
  | TelegramFollowerDurableAdmissionAckV1;

export interface TelegramForeignOwnedUpdateForwarder<
  TContext,
  TReactionUpdate extends TelegramMessageReactionUpdated =
    TelegramMessageReactionUpdated,
  TCallbackQuery extends TelegramCallbackQuery = TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage = TelegramUpdateMessage,
> {
  forwardUpdate?: (input: {
    update: { update_id: number };
    profile: string;
    target: TelegramTarget;
    ownership: TelegramMessageOwnershipView;
  }) => Promise<TelegramFollowerForwardingResult | undefined> | TelegramFollowerForwardingResult | undefined;
  forwardCallback?: (input: {
    query: TCallbackQuery;
    ownership: TelegramMessageOwnershipView;
    ctx: TContext;
  }) => Promise<TelegramFollowerForwardingResult> | TelegramFollowerForwardingResult;
  forwardReaction?: (input: {
    reactionUpdate: TReactionUpdate;
    ownership: TelegramMessageOwnershipView;
    ctx: TContext;
  }) => Promise<TelegramFollowerForwardingResult> | TelegramFollowerForwardingResult;
  forwardMessage?: (input: {
    message: TMessage;
    ownership: TelegramTargetOwnershipView;
    ctx: TContext;
  }) => Promise<TelegramFollowerForwardingResult> | TelegramFollowerForwardingResult;
  forwardEditedMessage?: (input: {
    message: TMessage;
    ownership: TelegramTargetOwnershipView;
    ctx: TContext;
  }) => Promise<TelegramFollowerForwardingResult> | TelegramFollowerForwardingResult;
}

export interface TelegramMessageReactionUpdated {
  chat: { id?: number; type: string };
  user?: TelegramUser;
  message_id: number;
  old_reaction: TelegramReactionType[];
  new_reaction: TelegramReactionType[];
}

export interface TelegramUpdateFlow
  extends TelegramUpdateRouting, TelegramUpdateDeletion {
  update_id?: number;
  message_reaction?: TelegramMessageReactionUpdated;
}

export type TelegramUpdateFlowAction<
  TReactionUpdate extends TelegramMessageReactionUpdated =
    TelegramMessageReactionUpdated,
  TCallbackQuery extends TelegramCallbackQuery = TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage = TelegramUpdateMessage,
  TGuestMessage extends TelegramGuestMessage = TelegramGuestMessage,
> =
  | { kind: "ignore" }
  | {
      kind: "deleted";
      messageIds: number[];
      scope: TelegramDeletedMessagesScope;
    }
  | { kind: "reaction"; reactionUpdate: TReactionUpdate }
  | {
      kind: "topic-lifecycle";
      lifecycle: TelegramTopicLifecycleUpdate<TMessage>;
    }
  | {
      kind: "callback";
      query: TCallbackQuery;
      authorization: TelegramAuthorizationState;
    }
  | {
      kind: "message";
      message: TMessage & { from: TelegramUser };
      authorization: TelegramAuthorizationState;
    }
  | {
      kind: "edited-message";
      message: TMessage & { from: TelegramUser };
      authorization: TelegramAuthorizationState;
    }
  | {
      kind: "guest";
      guestMessage: TGuestMessage & { from: TelegramUser };
      authorization: TelegramAuthorizationState;
    };

export function buildTelegramUpdateFlowAction<
  TUpdate extends TelegramUpdateFlow,
>(
  update: TUpdate,
  allowedUserId?: number,
): TelegramUpdateFlowAction<
  NonNullable<TUpdate["message_reaction"]>,
  NonNullable<TUpdate["callback_query"]>,
  NonNullable<TUpdate["message"] | TUpdate["edited_message"]>,
  NonNullable<TUpdate["guest_message"]>
> {
  const deletedMessageIds = extractDeletedTelegramMessageIds(update);
  if (deletedMessageIds.length > 0) {
    const deleted = update.deleted_business_messages;
    return {
      kind: "deleted",
      messageIds: deletedMessageIds,
      scope: {
        ...(typeof deleted?.chat?.id === "number"
          ? { chatId: deleted.chat.id }
          : {}),
        ...(typeof deleted?.business_connection_id === "string"
          ? { businessConnectionId: deleted.business_connection_id }
          : {}),
      },
    };
  }
  if (update.message_reaction) {
    return { kind: "reaction", reactionUpdate: update.message_reaction };
  }
  const topicLifecycle = getTelegramTopicLifecycleUpdate(update.message);
  if (topicLifecycle) {
    return { kind: "topic-lifecycle", lifecycle: topicLifecycle };
  }
  const query = getAuthorizedTelegramCallbackQuery(update, allowedUserId);
  if (query) {
    return {
      kind: "callback",
      query: query as NonNullable<TUpdate["callback_query"]>,
      authorization: getTelegramAuthorizationState(
        query.from.id,
        allowedUserId,
      ),
    };
  }
  const message = getAuthorizedTelegramMessage(update, allowedUserId);
  if (message?.from) {
    return {
      kind: "message",
      message: message as NonNullable<
        TUpdate["message"] | TUpdate["edited_message"]
      > & { from: TelegramUser },
      authorization: getTelegramAuthorizationState(
        message.from.id,
        allowedUserId,
      ),
    };
  }
  const editedMessage = getAuthorizedTelegramEditedMessage(
    update,
    allowedUserId,
  );
  if (editedMessage?.from) {
    return {
      kind: "edited-message",
      message: editedMessage as NonNullable<
        TUpdate["message"] | TUpdate["edited_message"]
      > & { from: TelegramUser },
      authorization: getTelegramAuthorizationState(
        editedMessage.from.id,
        allowedUserId,
      ),
    };
  }
  const guestMessage = getAuthorizedTelegramGuestMessage(update);
  if (guestMessage?.from) {
    return {
      kind: "guest",
      guestMessage: guestMessage as NonNullable<TUpdate["guest_message"]> & {
        from: TelegramUser;
      },
      authorization: getTelegramAuthorizationState(
        guestMessage.from.id,
        allowedUserId,
      ),
    };
  }
  return { kind: "ignore" };
}

// --- Execution Planning ---

export type TelegramUpdateExecutionPlan<
  TReactionUpdate extends TelegramMessageReactionUpdated =
    TelegramMessageReactionUpdated,
  TCallbackQuery extends TelegramCallbackQuery = TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage = TelegramUpdateMessage,
  TGuestMessage extends TelegramGuestMessage = TelegramGuestMessage,
> =
  | { kind: "ignore" }
  | {
      kind: "deleted";
      messageIds: number[];
      scope: TelegramDeletedMessagesScope;
    }
  | {
      kind: "reaction";
      reactionUpdate: TReactionUpdate;
    }
  | {
      kind: "topic-lifecycle";
      lifecycle: TelegramTopicLifecycleUpdate<TMessage>;
    }
  | {
      kind: "callback";
      query: TCallbackQuery;
      shouldDeny: boolean;
    }
  | {
      kind: "message";
      message: TMessage & { from: TelegramUser };
      shouldDeny: boolean;
    }
  | {
      kind: "edited-message";
      message: TMessage & { from: TelegramUser };
      shouldDeny: boolean;
    }
  | {
      kind: "guest";
      guestMessage: TGuestMessage & { from: TelegramUser };
      shouldDeny: boolean;
    };

export function buildTelegramUpdateExecutionPlan<
  TReactionUpdate extends TelegramMessageReactionUpdated,
  TCallbackQuery extends TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage,
  TGuestMessage extends TelegramGuestMessage,
>(
  action: TelegramUpdateFlowAction<
    TReactionUpdate,
    TCallbackQuery,
    TMessage,
    TGuestMessage
  >,
): TelegramUpdateExecutionPlan<
  TReactionUpdate,
  TCallbackQuery,
  TMessage,
  TGuestMessage
> {
  switch (action.kind) {
    case "ignore":
      return { kind: "ignore" };
    case "deleted":
      return {
        kind: "deleted",
        messageIds: action.messageIds,
        scope: action.scope,
      };
    case "reaction":
      return { kind: "reaction", reactionUpdate: action.reactionUpdate };
    case "topic-lifecycle":
      return { kind: "topic-lifecycle", lifecycle: action.lifecycle };
    case "callback":
      return {
        kind: "callback",
        query: action.query,
        shouldDeny: action.authorization.kind === "deny",
      };
    case "message":
      return {
        kind: "message",
        message: action.message,
        shouldDeny: action.authorization.kind === "deny",
      };
    case "edited-message":
      return {
        kind: "edited-message",
        message: action.message,
        shouldDeny: action.authorization.kind === "deny",
      };
    case "guest":
      return {
        kind: "guest",
        guestMessage: action.guestMessage,
        // Guest mode is an extension of an already paired bridge, not a pairing surface.
        shouldDeny: action.authorization.kind !== "allow",
      };
  }
}

export function buildTelegramUpdateExecutionPlanFromUpdate<
  TUpdate extends TelegramUpdateFlow,
>(
  update: TUpdate,
  allowedUserId?: number,
): TelegramUpdateExecutionPlan<
  NonNullable<TUpdate["message_reaction"]>,
  NonNullable<TUpdate["callback_query"]>,
  NonNullable<TUpdate["message"] | TUpdate["edited_message"]>
> {
  return buildTelegramUpdateExecutionPlan(
    buildTelegramUpdateFlowAction(update, allowedUserId),
  );
}

// --- Runtime ---

export type TelegramInboundHandlingOutcome =
  | { kind: "prompt-materialized"; turnId: string; recordIds: string[] }
  | {
      kind: "deferred";
      reason:
        | "text-group"
        | "media-group"
        | "operator-reroute"
        | "follower-admission-pending"
        | "session-replay";
      key: string;
    }
  | {
      kind: "completed";
      reason:
        | "ignored"
        | "deleted"
        | "reaction"
        | "topic-lifecycle"
        | "callback"
        | "guest"
        | "command"
        | "menu"
        | "public-handler"
        | "unauthorized"
        | "unsupported";
    }
  | {
      kind: "follower-admitted";
      admission: TelegramFollowerDurableAdmissionAckV1;
    };

export const TELEGRAM_UNTARGETED_RECOVERY_CHAT_ID = Number.MIN_SAFE_INTEGER;

export type TelegramDurableInboundResponsibility =
  | { kind: "local"; target: TelegramTarget }
  | {
      kind: "follower";
      target: TelegramTarget;
      instanceId: string;
      registrationGeneration: string;
    }
  | {
      kind: "terminal";
      target: TelegramTarget;
      reason: "ignored" | "unauthorized" | "unsupported";
      pairingProof: boolean;
    };

function getTelegramDurableUpdateTarget(
  update: TelegramUpdateFlow,
  fallbackChatId: number | undefined,
): TelegramTarget | undefined {
  const message =
    update.message ??
    update.edited_message ??
    update.callback_query?.message ??
    update.message_reaction;
  const deleted = update.deleted_business_messages as
    | { chat?: { id?: number } }
    | undefined;
  const chatId =
    message?.chat?.id ??
    deleted?.chat?.id ??
    update.guest_message?.chat?.id ??
    fallbackChatId;
  if (typeof chatId !== "number") return undefined;
  const threadId =
    (message as { message_thread_id?: unknown } | undefined)
      ?.message_thread_id ??
    (update.guest_message as { message_thread_id?: unknown } | undefined)
      ?.message_thread_id;
  return typeof threadId === "number" ? { chatId, threadId } : { chatId };
}

/** Side-effect-free owner decision used before admission and public handlers. */
export function planTelegramDurableInboundResponsibility(
  update: TelegramUpdateFlow,
  input: {
    allowedUserId?: number;
    currentInstanceId?: string;
    getMessageOwnership?: TelegramMessageOwnershipLookup;
    getTargetOwnership?: TelegramTargetOwnershipLookup;
  },
): TelegramDurableInboundResponsibility {
  const resolvedTarget = getTelegramDurableUpdateTarget(
    update,
    input.allowedUserId,
  );
  const target = resolvedTarget ?? {
    chatId: TELEGRAM_UNTARGETED_RECOVERY_CHAT_ID,
  };
  const pairingProof = isTelegramPairingProofShapedUpdate(update);
  const senderId = getTelegramUpdateSenderId(update);
  const supported = Boolean(
    update.message ||
      update.edited_message ||
      update.callback_query ||
      update.message_reaction ||
      update.deleted_business_messages ||
      update.guest_message,
  );
  const unauthorized =
    !isValidTelegramAllowedUserId(input.allowedUserId) ||
    (senderId !== undefined && senderId !== input.allowedUserId);
  const botAuthored = Boolean(
    update.message?.from?.is_bot ||
      update.edited_message?.from?.is_bot ||
      update.callback_query?.from?.is_bot,
  );
  if (!resolvedTarget || pairingProof || unauthorized || !supported || botAuthored) {
    return {
      kind: "terminal",
      target,
      pairingProof,
      reason: !resolvedTarget || !supported
        ? "unsupported"
        : unauthorized
          ? "unauthorized"
          : "ignored",
    };
  }
  const ownedMessage = update.callback_query?.message ?? update.message_reaction;
  const messageOwnership =
    typeof ownedMessage?.chat?.id === "number" &&
    typeof ownedMessage.message_id === "number"
      ? input.getMessageOwnership?.(
          ownedMessage.chat.id,
          ownedMessage.message_id,
        )
      : undefined;
  const ownership = messageOwnership ?? input.getTargetOwnership?.(target);
  if (ownership && ownership.instanceId !== input.currentInstanceId) {
    if (!ownership.ownerGeneration) {
      return {
        kind: "terminal",
        target,
        pairingProof: false,
        reason: "unsupported",
      };
    }
    return {
      kind: "follower",
      target,
      instanceId: ownership.instanceId,
      registrationGeneration: ownership.ownerGeneration,
    };
  }
  return { kind: "local", target };
}

export type TelegramMessageOwnershipRecorderInput = Parameters<
  TelegramMessageOwnershipStore["record"]
>[0];

export type TelegramMessageOwnershipRecorder = (
  input: TelegramMessageOwnershipRecorderInput,
) => void;

export interface TelegramUpdateRuntimeDeps<
  TContext = unknown,
  TReactionUpdate extends TelegramMessageReactionUpdated =
    TelegramMessageReactionUpdated,
  TCallbackQuery extends TelegramCallbackQuery = TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage = TelegramUpdateMessage,
> {
  ctx: TContext;
  getEffectiveProfile?: () => string;
  getCurrentInstanceId?: () => string | undefined;
  getMessageOwnership?: TelegramMessageOwnershipLookup;
  getTargetOwnership?: TelegramTargetOwnershipLookup;
  recordMessageOwnership?: TelegramMessageOwnershipRecorder;
  foreignOwnedUpdateForwarder?: TelegramForeignOwnedUpdateForwarder<
    TContext,
    TReactionUpdate,
    TCallbackQuery,
    TMessage
  >;
  removePendingMediaGroupMessages: (
    messageIds: number[],
    scope?: { chatId?: number; threadId?: number },
  ) => void;
  removeQueuedTelegramTurnsByMessageIds: (
    messageIds: number[],
    ctx: TContext,
    scope?: {
      profile?: string;
      chatId?: number;
      threadId?: number;
      exactThreadId?: number | null;
      businessConnectionId?: string;
    },
  ) => number;
  resolveQueuedTelegramMessageThreadId?: (
    messageId: number,
    scope: {
      profile?: string;
      chatId?: number;
      businessConnectionId?: string;
    },
  ) => number | null | undefined;
  handleAuthorizedTelegramReactionUpdate: (
    reactionUpdate: TReactionUpdate,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  handleTelegramTopicLifecycleUpdate?: (
    lifecycle: TelegramTopicLifecycleUpdate<TMessage>,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome> | TelegramInboundHandlingOutcome;
  answerCallbackQuery: (
    callbackQueryId: string,
    text?: string,
  ) => Promise<void>;
  answerGuestQuery: (guestQueryId: string, text?: string) => Promise<void>;
  handleAuthorizedTelegramCallbackQuery: (
    query: TCallbackQuery,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  sendTextReply: (
    chatId: number,
    replyToMessageId: number,
    text: string,
    options?: { target?: { chatId: number; threadId?: number } },
  ) => Promise<number | undefined>;
  handleAuthorizedTelegramMessage: (
    message: TMessage,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  handleAuthorizedTelegramEditedMessage: (
    message: TMessage,
    ctx: TContext,
  ) => TelegramInboundHandlingOutcome | Promise<TelegramInboundHandlingOutcome>;
  handleAuthorizedTelegramGuestMessage?: (
    guestMessage: TelegramGuestMessage & { from: TelegramUser },
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  /** Called when the owner writes in an unbound thread no live instance owns. */
  handleUnboundTelegramTopicMessage?: (
    message: TMessage & { from: TelegramUser },
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
}

export interface TelegramUpdateRuntimeControllerDeps<
  TContext = unknown,
  TCallbackQuery extends TelegramCallbackQuery = TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage = TelegramUpdateMessage,
> {
  getAllowedUserId: () => number | undefined;
  getEffectiveProfile?: () => string;
  getCurrentInstanceId?: () => string | undefined;
  getMessageOwnership?: TelegramMessageOwnershipLookup;
  getTargetOwnership?: TelegramTargetOwnershipLookup;
  recordMessageOwnership?: TelegramMessageOwnershipRecorder;
  foreignOwnedUpdateForwarder?: TelegramForeignOwnedUpdateForwarder<
    TContext,
    TelegramMessageReactionUpdated,
    TCallbackQuery,
    TMessage
  >;
  removePendingMediaGroupMessages: (
    messageIds: number[],
    scope?: { chatId?: number; threadId?: number },
  ) => void;
  removeQueuedTelegramTurnsByMessageIds: (
    messageIds: number[],
    ctx: TContext,
    scope?: {
      profile?: string;
      chatId?: number;
      threadId?: number;
      exactThreadId?: number | null;
      businessConnectionId?: string;
    },
  ) => number;
  resolveQueuedTelegramMessageThreadId?: (
    messageId: number,
    scope: {
      profile?: string;
      chatId?: number;
      businessConnectionId?: string;
    },
  ) => number | null | undefined;
  clearQueuedTelegramTurnPriorityByMessageId: (
    messageId: number,
    ctx: TContext,
    scope?: { chatId?: number; threadId?: number },
  ) => boolean;
  prioritizeQueuedTelegramTurnByMessageId: (
    messageId: number,
    ctx: TContext,
    priorityEmoji?: string,
    scope?: { chatId?: number; threadId?: number },
  ) => boolean;
  answerCallbackQuery: (
    callbackQueryId: string,
    text?: string,
  ) => Promise<void>;
  answerGuestQuery: (guestQueryId: string, text?: string) => Promise<void>;
  handleAuthorizedTelegramCallbackQuery: (
    query: TCallbackQuery,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  sendTextReply: (
    chatId: number,
    replyToMessageId: number,
    text: string,
    options?: { target?: { chatId: number; threadId?: number } },
  ) => Promise<number | undefined>;
  handleAuthorizedTelegramMessage: (
    message: TMessage,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  handleAuthorizedTelegramEditedMessage: (
    message: TMessage,
    ctx: TContext,
  ) => TelegramInboundHandlingOutcome | Promise<TelegramInboundHandlingOutcome>;
  handleAuthorizedTelegramGuestMessage?: (
    guestMessage: TelegramGuestMessage & { from: TelegramUser },
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  handleTelegramTopicLifecycleUpdate?: (
    lifecycle: TelegramTopicLifecycleUpdate<TMessage>,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome> | TelegramInboundHandlingOutcome;
  /** Called when the owner writes in an unbound thread no live instance owns. */
  handleUnboundTelegramTopicMessage?: (
    message: TMessage & { from: TelegramUser },
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
}

export interface TelegramUpdateRuntimeController<
  TContext = unknown,
  TUpdate extends TelegramUpdateFlow = TelegramUpdateFlow,
> {
  handleAuthorizedReactionUpdate: (
    reactionUpdate: NonNullable<TUpdate["message_reaction"]>,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  handleUpdate: (
    update: TUpdate,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
}

function getTelegramCallbackQueryId(
  query: TelegramCallbackQuery,
): string | undefined {
  return typeof query.id === "string" ? query.id : undefined;
}

function getTelegramMessageReplyTarget(
  message: TelegramUpdateMessage,
): { chatId: number; messageId: number; threadId?: number } | undefined {
  if (
    typeof message.chat.id !== "number" ||
    typeof message.message_id !== "number"
  ) {
    return undefined;
  }
  return {
    chatId: message.chat.id,
    messageId: message.message_id,
    ...(typeof message.message_thread_id === "number"
      ? { threadId: message.message_thread_id }
      : {}),
  };
}

function getForeignTelegramMessageOwnership(
  target: { chatId: number; messageId: number } | undefined,
  deps: {
    getCurrentInstanceId?: () => string | undefined;
    getMessageOwnership?: TelegramMessageOwnershipLookup;
  },
): TelegramMessageOwnershipView | undefined {
  if (!target || !deps.getMessageOwnership || !deps.getCurrentInstanceId) {
    return undefined;
  }
  const currentInstanceId = deps.getCurrentInstanceId();
  if (!currentInstanceId) return undefined;
  const ownership = deps.getMessageOwnership(target.chatId, target.messageId);
  return ownership && ownership.instanceId !== currentInstanceId
    ? ownership
    : undefined;
}

function getForeignTelegramCallbackOwnership(
  query: TelegramCallbackQuery,
  deps: {
    getCurrentInstanceId?: () => string | undefined;
    getMessageOwnership?: TelegramMessageOwnershipLookup;
    getTargetOwnership?: TelegramTargetOwnershipLookup;
  },
): TelegramMessageOwnershipView | undefined {
  return (
    getForeignTelegramMessageOwnership(
      getTelegramCallbackMessageTarget(query),
      deps,
    ) ??
    getForeignTelegramTargetOwnership(
      query.message ? getTelegramMessageTarget(query.message) : undefined,
      deps,
    )
  );
}

function getTelegramCallbackMessageTarget(
  query: TelegramCallbackQuery,
): { chatId: number; messageId: number } | undefined {
  return query.message
    ? getTelegramMessageReplyTarget(query.message)
    : undefined;
}

function getTelegramReactionMessageTarget(
  reactionUpdate: TelegramMessageReactionUpdated,
): { chatId: number; messageId: number } | undefined {
  return typeof reactionUpdate.chat.id === "number"
    ? { chatId: reactionUpdate.chat.id, messageId: reactionUpdate.message_id }
    : undefined;
}

function getForeignTelegramTargetOwnership(
  target: TelegramTarget | undefined,
  deps: {
    getCurrentInstanceId?: () => string | undefined;
    getTargetOwnership?: TelegramTargetOwnershipLookup;
  },
): TelegramTargetOwnershipView | undefined {
  if (!target || !deps.getTargetOwnership || !deps.getCurrentInstanceId) {
    return undefined;
  }
  const currentInstanceId = deps.getCurrentInstanceId();
  if (!currentInstanceId) return undefined;
  const ownership = deps.getTargetOwnership(target);
  return ownership && ownership.instanceId !== currentInstanceId
    ? ownership
    : undefined;
}

export async function executeTelegramUpdate<
  TUpdate extends TelegramUpdateFlow,
  TContext = unknown,
>(
  update: TUpdate,
  allowedUserId: number | undefined,
  deps: TelegramUpdateRuntimeDeps<
    TContext,
    NonNullable<TUpdate["message_reaction"]>,
    NonNullable<TUpdate["callback_query"]>,
    NonNullable<TUpdate["message"] | TUpdate["edited_message"]>
  >,
): Promise<TelegramInboundHandlingOutcome> {
  return executeTelegramUpdatePlan(
    buildTelegramUpdateExecutionPlanFromUpdate(update, allowedUserId),
    deps,
    { updateId: update.update_id },
  );
}

export type TelegramPairedUpdateRuntimeControllerDeps<
  TContext = unknown,
  TUpdate extends TelegramUpdateFlow = TelegramUpdateFlow,
> = TelegramUpdateRuntimeControllerDeps<
  TContext,
  NonNullable<TUpdate["callback_query"]>,
  NonNullable<TUpdate["message"] | TUpdate["edited_message"]>
>;

export function createTelegramPairedUpdateRuntime<
  TContext = unknown,
  TUpdate extends TelegramUpdateFlow = TelegramUpdateFlow,
>(
  deps: TelegramPairedUpdateRuntimeControllerDeps<TContext, TUpdate>,
): TelegramUpdateRuntimeController<TContext, TUpdate> {
  return createTelegramUpdateRuntime({
    getAllowedUserId: deps.getAllowedUserId,
    getEffectiveProfile: deps.getEffectiveProfile,
    getCurrentInstanceId: deps.getCurrentInstanceId,
    getMessageOwnership: deps.getMessageOwnership,
    getTargetOwnership: deps.getTargetOwnership,
    recordMessageOwnership: deps.recordMessageOwnership,
    handleTelegramTopicLifecycleUpdate: deps.handleTelegramTopicLifecycleUpdate,
    foreignOwnedUpdateForwarder: deps.foreignOwnedUpdateForwarder,
    removePendingMediaGroupMessages: deps.removePendingMediaGroupMessages,
    removeQueuedTelegramTurnsByMessageIds:
      deps.removeQueuedTelegramTurnsByMessageIds,
    resolveQueuedTelegramMessageThreadId:
      deps.resolveQueuedTelegramMessageThreadId,
    clearQueuedTelegramTurnPriorityByMessageId:
      deps.clearQueuedTelegramTurnPriorityByMessageId,
    prioritizeQueuedTelegramTurnByMessageId:
      deps.prioritizeQueuedTelegramTurnByMessageId,
    answerCallbackQuery: deps.answerCallbackQuery,
    answerGuestQuery: deps.answerGuestQuery,
    handleAuthorizedTelegramCallbackQuery:
      deps.handleAuthorizedTelegramCallbackQuery,
    sendTextReply: deps.sendTextReply,
    handleAuthorizedTelegramMessage: deps.handleAuthorizedTelegramMessage,
    handleAuthorizedTelegramEditedMessage:
      deps.handleAuthorizedTelegramEditedMessage,
    handleAuthorizedTelegramGuestMessage:
      deps.handleAuthorizedTelegramGuestMessage,
    handleUnboundTelegramTopicMessage: deps.handleUnboundTelegramTopicMessage,
  });
}

export function createTelegramUpdateRuntime<
  TContext = unknown,
  TUpdate extends TelegramUpdateFlow = TelegramUpdateFlow,
>(
  deps: TelegramUpdateRuntimeControllerDeps<
    TContext,
    NonNullable<TUpdate["callback_query"]>,
    NonNullable<TUpdate["message"] | TUpdate["edited_message"]>
  >,
): TelegramUpdateRuntimeController<TContext, TUpdate> {
  const handleAuthorizedReactionUpdate = async (
    reactionUpdate: NonNullable<TUpdate["message_reaction"]>,
    ctx: TContext,
  ): Promise<TelegramInboundHandlingOutcome> => {
    return handleAuthorizedTelegramReactionUpdate(reactionUpdate, {
      allowedUserId: deps.getAllowedUserId(),
      ctx,
      removePendingMediaGroupMessages: deps.removePendingMediaGroupMessages,
      removeQueuedTelegramTurnsByMessageIds:
        deps.removeQueuedTelegramTurnsByMessageIds,
      getCurrentInstanceId: deps.getCurrentInstanceId,
      getMessageOwnership: deps.getMessageOwnership,
      foreignOwnedUpdateForwarder: deps.foreignOwnedUpdateForwarder,
      clearQueuedTelegramTurnPriorityByMessageId:
        deps.clearQueuedTelegramTurnPriorityByMessageId,
      prioritizeQueuedTelegramTurnByMessageId:
        deps.prioritizeQueuedTelegramTurnByMessageId,
    });
  };
  return {
    handleAuthorizedReactionUpdate,
    handleUpdate: (update, ctx) =>
      executeTelegramUpdate(update, deps.getAllowedUserId(), {
        ctx,
        getEffectiveProfile: deps.getEffectiveProfile,
        getCurrentInstanceId: deps.getCurrentInstanceId,
        getMessageOwnership: deps.getMessageOwnership,
        getTargetOwnership: deps.getTargetOwnership,
        recordMessageOwnership: deps.recordMessageOwnership,
        foreignOwnedUpdateForwarder: deps.foreignOwnedUpdateForwarder,
        removePendingMediaGroupMessages: deps.removePendingMediaGroupMessages,
        removeQueuedTelegramTurnsByMessageIds:
          deps.removeQueuedTelegramTurnsByMessageIds,
        resolveQueuedTelegramMessageThreadId:
          deps.resolveQueuedTelegramMessageThreadId,
        handleAuthorizedTelegramReactionUpdate: handleAuthorizedReactionUpdate,
        handleTelegramTopicLifecycleUpdate:
          deps.handleTelegramTopicLifecycleUpdate,
        answerCallbackQuery: deps.answerCallbackQuery,
        answerGuestQuery: deps.answerGuestQuery,
        handleAuthorizedTelegramCallbackQuery:
          deps.handleAuthorizedTelegramCallbackQuery,
        sendTextReply: deps.sendTextReply,
        handleAuthorizedTelegramMessage: deps.handleAuthorizedTelegramMessage,
        handleAuthorizedTelegramEditedMessage:
          deps.handleAuthorizedTelegramEditedMessage,
        handleAuthorizedTelegramGuestMessage:
          deps.handleAuthorizedTelegramGuestMessage,
        handleUnboundTelegramTopicMessage:
          deps.handleUnboundTelegramTopicMessage,
      }),
  };
}

export interface AuthorizedTelegramReactionUpdateDeps<TContext> {
  allowedUserId?: number;
  ctx: TContext;
  getCurrentInstanceId?: () => string | undefined;
  getMessageOwnership?: TelegramMessageOwnershipLookup;
  foreignOwnedUpdateForwarder?: TelegramForeignOwnedUpdateForwarder<TContext>;
  removePendingMediaGroupMessages: (messageIds: number[]) => void;
  removeQueuedTelegramTurnsByMessageIds: (
    messageIds: number[],
    ctx: TContext,
    scope?: { chatId?: number; threadId?: number },
  ) => number;
  clearQueuedTelegramTurnPriorityByMessageId: (
    messageId: number,
    ctx: TContext,
    scope?: { chatId?: number; threadId?: number },
  ) => boolean;
  prioritizeQueuedTelegramTurnByMessageId: (
    messageId: number,
    ctx: TContext,
    priorityEmoji?: string,
    scope?: { chatId?: number; threadId?: number },
  ) => boolean;
}

export async function handleAuthorizedTelegramReactionUpdate<TContext>(
  reactionUpdate: TelegramMessageReactionUpdated,
  deps: AuthorizedTelegramReactionUpdateDeps<TContext>,
): Promise<TelegramInboundHandlingOutcome> {
  const reactionTarget = getTelegramReactionMessageTarget(reactionUpdate);
  const foreignOwnership = getForeignTelegramMessageOwnership(
    reactionTarget,
    deps,
  );
  if (foreignOwnership) {
    const forwardingResult = await deps.foreignOwnedUpdateForwarder?.forwardReaction?.({
      reactionUpdate,
      ownership: foreignOwnership,
      ctx: deps.ctx,
    });
    return followerForwardingOutcome(
      forwardingResult,
      reactionTarget ? { chatId: reactionTarget.chatId } : undefined,
      reactionUpdate.message_id,
    );
  }
  const reactionUser = reactionUpdate.user;
  if (!reactionUser || reactionUser.is_bot) {
    return { kind: "completed", reason: "reaction" };
  }
  if (
    reactionUpdate.chat.type !== "private" &&
    reactionUser.id !== deps.allowedUserId
  ) {
    return { kind: "completed", reason: "reaction" };
  }
  const reactionScope =
    typeof reactionUpdate.chat.id === "number"
      ? { chatId: reactionUpdate.chat.id }
      : undefined;
  const oldEmojis = collectTelegramReactionEmojis(reactionUpdate.old_reaction);
  const newEmojis = collectTelegramReactionEmojis(reactionUpdate.new_reaction);
  if (
    hasAddedTelegramReactionEmoji(
      oldEmojis,
      newEmojis,
      TELEGRAM_REMOVAL_REACTION_EMOJIS,
    )
  ) {
    deps.removePendingMediaGroupMessages([reactionUpdate.message_id]);
    deps.removeQueuedTelegramTurnsByMessageIds(
      [reactionUpdate.message_id],
      deps.ctx,
      reactionScope,
    );
    return { kind: "completed", reason: "reaction" };
  }
  const hadPriorityReaction = hasAnyTelegramReactionEmoji(
    oldEmojis,
    TELEGRAM_PRIORITY_REACTION_EMOJIS,
  );
  const hasPriorityReaction = hasAnyTelegramReactionEmoji(
    newEmojis,
    TELEGRAM_PRIORITY_REACTION_EMOJIS,
  );
  if (hadPriorityReaction && !hasPriorityReaction) {
    deps.clearQueuedTelegramTurnPriorityByMessageId(
      reactionUpdate.message_id,
      deps.ctx,
      reactionScope,
    );
  }
  const addedPriorityEmoji = getAddedTelegramReactionEmoji(
    oldEmojis,
    newEmojis,
    TELEGRAM_PRIORITY_REACTION_EMOJIS,
  );
  if (addedPriorityEmoji) {
    deps.prioritizeQueuedTelegramTurnByMessageId(
      reactionUpdate.message_id,
      deps.ctx,
      addedPriorityEmoji,
      reactionScope,
    );
  }
  return { kind: "completed", reason: "reaction" };
}

function isTelegramStaleContextError(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.message.includes("stale after session") ||
      error.message.includes("stale ctx"))
  );
}

function stableUpdateIdentityKey(
  plan: TelegramUpdateExecutionPlan,
  updateId?: number,
): string {
  if (Number.isSafeInteger(updateId) && updateId !== undefined && updateId >= 0) {
    return `update:${updateId}`;
  }
  switch (plan.kind) {
    case "message":
    case "edited-message":
    case "topic-lifecycle": {
      const message = plan.kind === "topic-lifecycle" ? plan.lifecycle.message : plan.message;
      const target = getTelegramMessageTarget(message);
      return `message:${target?.chatId ?? "unknown"}:${target?.threadId ?? 0}:${message.message_id ?? "unknown"}`;
    }
    case "callback": {
      const target = plan.query.message
        ? getTelegramMessageTarget(plan.query.message)
        : undefined;
      return `callback:${target?.chatId ?? "unknown"}:${target?.threadId ?? 0}:${plan.query.message?.message_id ?? plan.query.id ?? "unknown"}`;
    }
    case "reaction":
      return `reaction:${plan.reactionUpdate.chat.id ?? "unknown"}:${plan.reactionUpdate.message_id}`;
    case "guest":
      return `guest:${plan.guestMessage.chat.id ?? "unknown"}:${plan.guestMessage.message_id ?? plan.guestMessage.guest_query_id}`;
    case "deleted":
      return `deleted:${plan.messageIds.join(",") || "unknown"}`;
    case "ignore":
      return "ignored:update";
  }
}

function followerForwardingOutcome(
  result: TelegramFollowerForwardingResult | undefined,
  target: TelegramTarget | undefined,
  messageId: number | undefined,
  updateId?: number,
): TelegramInboundHandlingOutcome {
  if (
    isTelegramFollowerDurableAdmissionAckV1(result) &&
    (!target ||
      (result.target.chatId === target.chatId &&
        result.target.threadId === target.threadId)) &&
    (updateId === undefined || result.updateId === updateId)
  ) {
    return { kind: "follower-admitted", admission: result };
  }
  const targetKey = target
    ? `${target.chatId}:${target.threadId ?? 0}`
    : "unknown:0";
  const sourceKey = Number.isSafeInteger(updateId)
    ? `update:${updateId}`
    : `message:${messageId ?? "unknown"}`;
  return {
    kind: "deferred",
    reason: "follower-admission-pending",
    key: `follower:${targetKey}:${sourceKey}`,
  };
}

export async function executeTelegramUpdatePlan<
  TContext = unknown,
  TReactionUpdate extends TelegramMessageReactionUpdated =
    TelegramMessageReactionUpdated,
  TCallbackQuery extends TelegramCallbackQuery = TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage = TelegramUpdateMessage,
>(
  plan: TelegramUpdateExecutionPlan<TReactionUpdate, TCallbackQuery, TMessage>,
  deps: TelegramUpdateRuntimeDeps<
    TContext,
    TReactionUpdate,
    TCallbackQuery,
    TMessage
  >,
  executionIdentity: { updateId?: number } = {},
): Promise<TelegramInboundHandlingOutcome> {
  try {
    switch (plan.kind) {
      case "ignore":
        return { kind: "completed", reason: "ignored" };
      case "deleted": {
        if (
          plan.scope.chatId === undefined &&
          plan.scope.businessConnectionId === undefined
        ) {
          deps.removePendingMediaGroupMessages(plan.messageIds);
          deps.removeQueuedTelegramTurnsByMessageIds(plan.messageIds, deps.ctx);
          return { kind: "completed", reason: "deleted" };
        }
        const profile = deps.getEffectiveProfile?.() ?? "default";
        for (const messageId of plan.messageIds) {
          const ownership =
            plan.scope.chatId === undefined
              ? undefined
              : deps.getMessageOwnership?.(plan.scope.chatId, messageId);
          const queueScope = {
            profile,
            ...(plan.scope.chatId !== undefined
              ? { chatId: plan.scope.chatId }
              : {}),
            ...(plan.scope.businessConnectionId
              ? { businessConnectionId: plan.scope.businessConnectionId }
              : {}),
          };
          const representedThreadId = ownership
            ? (ownership.target?.threadId ?? null)
            : deps.resolveQueuedTelegramMessageThreadId?.(
                messageId,
                queueScope,
              );
          const targetScope = {
            ...(plan.scope.chatId !== undefined
              ? { chatId: plan.scope.chatId }
              : {}),
            ...(typeof representedThreadId === "number"
              ? { threadId: representedThreadId }
              : {}),
          };
          if (!plan.scope.businessConnectionId) {
            deps.removePendingMediaGroupMessages([messageId], targetScope);
          }
          deps.removeQueuedTelegramTurnsByMessageIds([messageId], deps.ctx, {
            ...queueScope,
            ...targetScope,
            ...(representedThreadId !== undefined
              ? { exactThreadId: representedThreadId }
              : {}),
          });
        }
        return { kind: "completed", reason: "deleted" };
      }
      case "reaction":
        return deps.handleAuthorizedTelegramReactionUpdate(
          plan.reactionUpdate,
          deps.ctx,
        );
      case "topic-lifecycle":
        return (
          (await deps.handleTelegramTopicLifecycleUpdate?.(
            plan.lifecycle,
            deps.ctx,
          )) ?? { kind: "completed", reason: "topic-lifecycle" }
        );
      case "callback": {
        const foreignOwnership = getForeignTelegramCallbackOwnership(
          plan.query,
          deps,
        );
        if (foreignOwnership) {
          const forwardingResult =
            await deps.foreignOwnedUpdateForwarder?.forwardCallback?.({
              query: plan.query,
              ownership: foreignOwnership,
              ctx: deps.ctx,
            });
          if (!forwardingResult) {
            const callbackQueryId = getTelegramCallbackQueryId(plan.query);
            if (callbackQueryId) {
              await deps.answerCallbackQuery(
                callbackQueryId,
                "This Telegram message belongs to another Pi instance.",
              );
            }
          }
          const callbackTarget = plan.query.message
            ? getTelegramMessageTarget(plan.query.message)
            : undefined;
          return followerForwardingOutcome(
            forwardingResult,
            callbackTarget,
            plan.query.message?.message_id,
            executionIdentity.updateId,
          );
        }
        if (plan.shouldDeny) {
          const callbackQueryId = getTelegramCallbackQueryId(plan.query);
          if (callbackQueryId) {
            await deps.answerCallbackQuery(
              callbackQueryId,
              "This bot is not authorized for your account.",
            );
          }
          return { kind: "completed", reason: "unauthorized" };
        }
        return await deps.handleAuthorizedTelegramCallbackQuery(
          plan.query,
          deps.ctx,
        );
      }
      case "guest":
        if (plan.shouldDeny) {
          await deps.answerGuestQuery(
            plan.guestMessage.guest_query_id,
            "🚫 Access denied.",
          );
          return { kind: "completed", reason: "unauthorized" };
        }
        return deps.handleAuthorizedTelegramGuestMessage
          ? await deps.handleAuthorizedTelegramGuestMessage(
              plan.guestMessage,
              deps.ctx,
            )
          : { kind: "completed", reason: "guest" };
      case "message":
      case "edited-message": {
        const foreignMessageOwnership = getForeignTelegramMessageOwnership(
          getTelegramMessageReplyTarget(plan.message),
          deps,
        );
        if (foreignMessageOwnership) {
          const forwardingResult = plan.kind === "edited-message"
            ? await deps.foreignOwnedUpdateForwarder?.forwardEditedMessage?.({
                message: plan.message,
                ownership: foreignMessageOwnership,
                ctx: deps.ctx,
              })
            : await deps.foreignOwnedUpdateForwarder?.forwardMessage?.({
                message: plan.message,
                ownership: foreignMessageOwnership,
                ctx: deps.ctx,
              });
          return followerForwardingOutcome(
            forwardingResult,
            getTelegramMessageTarget(plan.message),
            plan.message.message_id,
            executionIdentity.updateId,
          );
        }
        const messageTarget = getTelegramMessageTarget(plan.message);
        const foreignTargetOwnership = getForeignTelegramTargetOwnership(
          messageTarget,
          deps,
        );
        if (foreignTargetOwnership) {
          if (typeof plan.message.message_id === "number") {
            deps.recordMessageOwnership?.({
              chatId: messageTarget!.chatId,
              messageId: plan.message.message_id,
              target: messageTarget,
              instanceId: foreignTargetOwnership.instanceId,
            });
          }
          const forwardingResult = plan.kind === "edited-message"
            ? await deps.foreignOwnedUpdateForwarder?.forwardEditedMessage?.({
                message: plan.message,
                ownership: foreignTargetOwnership,
                ctx: deps.ctx,
              })
            : await deps.foreignOwnedUpdateForwarder?.forwardMessage?.({
                message: plan.message,
                ownership: foreignTargetOwnership,
                ctx: deps.ctx,
              });
          return followerForwardingOutcome(
            forwardingResult,
            messageTarget,
            plan.message.message_id,
            executionIdentity.updateId,
          );
        }
        if (
          plan.kind === "message" &&
          messageTarget?.threadId != null &&
          deps.handleUnboundTelegramTopicMessage
        ) {
          return await deps.handleUnboundTelegramTopicMessage(
            plan.message,
            deps.ctx,
          );
        }
        const replyTarget = getTelegramMessageReplyTarget(plan.message);
        if (plan.shouldDeny) {
          if (replyTarget) {
            await deps.sendTextReply(
              replyTarget.chatId,
              replyTarget.messageId,
              "This bot is not authorized for your account.",
              { target: replyTarget },
            );
          }
          return { kind: "completed", reason: "unauthorized" };
        }
        return plan.kind === "edited-message"
          ? await deps.handleAuthorizedTelegramEditedMessage(
              plan.message,
              deps.ctx,
            )
          : await deps.handleAuthorizedTelegramMessage(
              plan.message,
              deps.ctx,
            );
      }
    }
  } catch (error) {
    if (!isTelegramStaleContextError(error)) throw error;
    return {
      kind: "deferred",
      reason: "session-replay",
      key: `session-replay:${stableUpdateIdentityKey(plan, executionIdentity.updateId)}`,
    };
  }
}

// --- Public update handler registry ---

/**
 * Verdict returned by a public Telegram update handler.
 *
 * - `"consume"` — the handler processed this update; pi-telegram skips default routing.
 * - `"pass"` (or `void`/`undefined`) — pi-telegram routes the update normally.
 */
export type TelegramUpdateHandlerVerdict = "consume" | "pass";

export type TelegramUpdateHandler = (
  update: unknown,
) =>
  | TelegramUpdateHandlerVerdict
  | void
  | Promise<TelegramUpdateHandlerVerdict | void>;

export interface TelegramUpdateHandlerRegistry {
  /** Schema version of this registry shape. */
  readonly version: 1;
  /**
   * Register an update handler. Returns a disposer that removes it.
   *
   * Handlers are invoked in registration order on every Telegram update,
   * before pi-telegram's own routing. The first handler that returns
   * `"consume"` wins and stops the chain for that update.
   */
  add: (handler: TelegramUpdateHandler) => () => void;
  /**
   * Run all registered handlers against an update.
   *
   * Used by pi-telegram's polling runtime; extension consumers should call
   * {@link registerTelegramUpdateHandler} or `add` instead of dispatching directly.
   */
  dispatch: (
    update: unknown,
    recordFailure?: (handlerId: string, handlerCategory: string) => void,
  ) => Promise<TelegramUpdateHandlerVerdict>;
}

const UPDATE_HANDLER_REGISTRY_KEY = "__piTelegramUpdateHandlerRegistry__";

function isValidV1UpdateHandlerRegistry(
  candidate: unknown,
): candidate is TelegramUpdateHandlerRegistry {
  if (!candidate || typeof candidate !== "object") return false;
  const r = candidate as Partial<TelegramUpdateHandlerRegistry>;
  return (
    r.version === 1 &&
    typeof r.add === "function" &&
    typeof r.dispatch === "function"
  );
}

function getOrCreateUpdateHandlerRegistry(): TelegramUpdateHandlerRegistry {
  const g = globalThis as Record<string, unknown>;
  const existing = g[UPDATE_HANDLER_REGISTRY_KEY];
  if (isValidV1UpdateHandlerRegistry(existing)) return existing;
  const handlers = new Map<
    TelegramUpdateHandler,
    { id: string; handler: TelegramUpdateHandler }
  >();
  let nextHandlerId = 0;
  const registry: TelegramUpdateHandlerRegistry = {
    version: 1,
    add(handler) {
      const entry = {
        id: `update-${nextHandlerId++}`,
        handler,
      };
      handlers.set(handler, entry);
      return () => handlers.delete(handler);
    },
    async dispatch(update, recordFailure) {
      for (const entry of handlers.values()) {
        try {
          const result = await entry.handler(update);
          if (result === "consume") return "consume";
        } catch {
          recordFailure?.(entry.id, "update");
        }
      }
      return "pass";
    },
  };
  g[UPDATE_HANDLER_REGISTRY_KEY] = registry;
  return registry;
}

/**
 * Called by pi-telegram's own runtime to obtain the registry it dispatches
 * through. Extension consumers should not call this; use
 * {@link registerTelegramUpdateHandler} instead.
 */
export function getTelegramUpdateHandlerRegistry(): TelegramUpdateHandlerRegistry {
  return getOrCreateUpdateHandlerRegistry();
}

export interface TelegramUnpairedUpdateGateDeps<TContext> {
  getAllowedUserId: () => number | undefined;
  claim: (input: TelegramPairingClaimInput) => Promise<TelegramPairingClaimResult>;
  sendGenericResponse: (target: {
    chatId: number;
    messageId: number;
    threadId?: number;
    text: typeof TELEGRAM_PAIRING_RESPONSE;
  }) => Promise<void>;
  onPaired: (ctx: TContext) => Promise<void> | void;
  recordSideEffectFailure: (
    phase: "response" | "on-paired",
    error: unknown,
  ) => void;
}

/** @internal */
export interface TelegramInteractionPriorityHandleDeps<
  TContext,
  TCallbackQuery extends TelegramCallbackQuery = TelegramCallbackQuery,
  TMessage extends TelegramUpdateMessage = TelegramUpdateMessage,
> {
  getCurrentInstanceId(): string | undefined;
  getCurrentProfile(): string;
  getMessageOwnership: TelegramMessageOwnershipLookup;
  classifyMessageOwnership: TelegramMessageOwnershipClassificationLookup;
  isInteractionPending(): boolean;
  foreignOwnedUpdateForwarder?: TelegramForeignOwnedUpdateForwarder<
    TContext,
    TelegramMessageReactionUpdated,
    TCallbackQuery,
    TMessage
  >;
  prepareCallback(input: {
    kind: "callback";
    target: TelegramTarget;
    callbackData: string;
    messageId: number;
  }): {
    acknowledgement: string;
    commit(): Promise<{ handled: boolean; message?: string }>;
  };
  handleInput(input: {
    kind: "text";
    target: TelegramTarget;
    text: string;
    messageId: number;
    replyToMessageId: number;
  }): Promise<{ handled: boolean; message?: string }>;
  answerCallbackQuery(callbackQueryId: string, text: string): Promise<unknown>;
}

function areTelegramPriorityTargetsEqual(
  left: TelegramTarget,
  right: TelegramTarget,
): boolean {
  return left.chatId === right.chatId && left.threadId === right.threadId;
}

/**
 * Private interaction answer route. The caller must invoke this only after the
 * exact pairing-proof and paired-sender authorization boundary.
 * @internal
 */
export function createTelegramInteractionPriorityHandle<
  TUpdate extends TelegramUpdateRouting,
  TContext,
  TCallbackQuery extends TelegramCallbackQuery = NonNullable<
    TUpdate["callback_query"]
  >,
  TMessage extends TelegramUpdateMessage = NonNullable<TUpdate["message"]>,
>(deps: TelegramInteractionPriorityHandleDeps<
  TContext,
  TCallbackQuery,
  TMessage
>): (
  update: TUpdate,
  ctx: TContext,
) => Promise<
  "pass" | "route-to-default" | TelegramInboundHandlingOutcome
> {
  const expired = "This interaction has expired.";
  const completed = (): TelegramInboundHandlingOutcome => ({
    kind: "completed",
    reason: "callback",
  });
  return async (update, _ctx) => {
    const query = update.callback_query as TCallbackQuery | undefined;
    if (query?.data?.startsWith("interact:")) {
      const callbackId = getTelegramCallbackQueryId(query);
      const callbackTarget = query.message
        ? getTelegramMessageTarget(query.message)
        : undefined;
      const callbackMessage = getTelegramCallbackMessageTarget(query);
      const ownership = callbackMessage
        ? deps.getMessageOwnership(
            callbackMessage.chatId,
            callbackMessage.messageId,
          )
        : undefined;
      const hasExplicitCallbackThread =
        query.message?.message_thread_id !== undefined;
      if (
        callbackTarget &&
        ownership?.target &&
        (callbackTarget.chatId !== ownership.target.chatId ||
          (hasExplicitCallbackThread &&
            !areTelegramPriorityTargetsEqual(callbackTarget, ownership.target)))
      ) {
        if (callbackId) await deps.answerCallbackQuery(callbackId, expired);
        return completed();
      }
      const target = ownership?.target ?? callbackTarget;
      const messageId = callbackMessage?.messageId;
      const currentInstanceId = deps.getCurrentInstanceId();
      if (
        ownership &&
        currentInstanceId &&
        ownership.instanceId !== currentInstanceId
      ) {
        const updateId = (update as { update_id?: unknown }).update_id;
        const forwarded =
          ownership.ownerGeneration &&
          target &&
          Number.isSafeInteger(updateId) &&
          (updateId as number) >= 0
            ? await deps.foreignOwnedUpdateForwarder?.forwardUpdate?.({
                update: update as TUpdate & { update_id: number },
                profile: deps.getCurrentProfile(),
                target,
                ownership,
              })
            : undefined;
        if (!forwarded && callbackId) {
          await deps.answerCallbackQuery(callbackId, expired);
        }
        return completed();
      }
      if (!target || messageId === undefined) {
        if (callbackId) await deps.answerCallbackQuery(callbackId, expired);
        return completed();
      }
      const prepared = deps.prepareCallback({
        kind: "callback",
        target,
        callbackData: query.data,
        messageId,
      });
      if (callbackId) {
        try {
          await deps.answerCallbackQuery(
            callbackId,
            prepared.acknowledgement,
          );
        } catch {
          // The one-use callback remains consumed even if Telegram rejects ACK.
        }
      }
      await prepared.commit();
      return completed();
    }

    const message = update.message as TMessage | undefined;
    if (!message || typeof message.text !== "string") return "pass";
    const command = message.text
      .trimStart()
      .split(/\s+/, 1)[0]
      ?.split("@", 1)[0];
    if (command?.startsWith("/")) {
      return deps.isInteractionPending() &&
          (command === "/abort" || command === "/stop")
        ? "route-to-default"
        : "pass";
    }
    const reply = message.reply_to_message;
    if (
      !reply?.from?.is_bot ||
      typeof message.chat.id !== "number" ||
      typeof message.message_id !== "number" ||
      typeof reply.message_id !== "number"
    ) {
      return "pass";
    }
    const ownership = deps.getMessageOwnership(
      message.chat.id,
      reply.message_id,
    );
    if (ownership?.purpose !== "interaction") {
      return deps.classifyMessageOwnership(
        message.chat.id,
        reply.message_id,
      )?.purpose === "interaction"
        ? completed()
        : "pass";
    }
    const messageTarget = getTelegramMessageTarget(message);
    const ownershipTarget = ownership.target;
    if (
      !messageTarget ||
      (ownershipTarget &&
        !areTelegramPriorityTargetsEqual(messageTarget, ownershipTarget))
    ) {
      return completed();
    }
    const currentInstanceId = deps.getCurrentInstanceId();
    if (!currentInstanceId) return completed();
    if (ownership.instanceId !== currentInstanceId) {
      const updateId = (update as { update_id?: unknown }).update_id;
      if (
        ownership.ownerGeneration &&
        Number.isSafeInteger(updateId) &&
        (updateId as number) >= 0
      ) {
        await deps.foreignOwnedUpdateForwarder?.forwardUpdate?.({
          update: update as TUpdate & { update_id: number },
          profile: deps.getCurrentProfile(),
          target: ownershipTarget ?? messageTarget,
          ownership,
        });
      }
      return completed();
    }
    await deps.handleInput({
      kind: "text",
      target: ownershipTarget ?? messageTarget,
      text: message.text,
      messageId: message.message_id,
      replyToMessageId: reply.message_id,
    });
    return completed();
  };
}

export interface TelegramUpdateHandlerWrapDeps<TUpdate, TContext> {
  defaultHandle: (
    update: TUpdate,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  priorityHandle?: (
    update: TUpdate,
    ctx: TContext,
  ) => Promise<
    "pass" | "route-to-default" | TelegramInboundHandlingOutcome
  >;
  pairingGate: TelegramUnpairedUpdateGateDeps<TContext>;
  registry?: TelegramUpdateHandlerRegistry;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
}

function getTelegramUpdateSenderId(update: unknown): number | undefined {
  if (!update || typeof update !== "object" || Array.isArray(update)) {
    return undefined;
  }
  const value = update as Record<string, unknown>;
  const candidates = [
    value.message,
    value.edited_message,
    value.callback_query,
    value.message_reaction,
    value.guest_message,
  ];
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
      continue;
    }
    const from = (candidate as Record<string, unknown>).from ??
      (candidate as Record<string, unknown>).user;
    if (!from || typeof from !== "object" || Array.isArray(from)) continue;
    const senderId = (from as Record<string, unknown>).id;
    if (typeof senderId === "number" && Number.isSafeInteger(senderId)) {
      return senderId;
    }
  }
  return undefined;
}

/**
 * Wrap default polling with the pairing proof gate, authorization boundary, and public registry.
 * Proof-shaped and unauthorized updates never cross the public handler membrane;
 * paired authorized handler ordering remains unchanged.
 */
export function createTelegramUpdateHandle<TUpdate, TContext>(
  deps: TelegramUpdateHandlerWrapDeps<TUpdate, TContext>,
): (
  update: TUpdate,
  ctx: TContext,
) => Promise<TelegramInboundHandlingOutcome> {
  const registry = deps.registry ?? getOrCreateUpdateHandlerRegistry();
  const { defaultHandle, pairingGate } = deps;
  return async (update, ctx) => {
    const proofShaped = isTelegramPairingProofShapedUpdate(update);
    const pairingCandidate = proofShaped
      ? parseTelegramPairingCandidate(update)
      : undefined;
    if (proofShaped) {
      if (isValidTelegramAllowedUserId(pairingGate.getAllowedUserId())) {
        return { kind: "completed", reason: "ignored" };
      }
      if (!pairingCandidate) {
        return { kind: "completed", reason: "unauthorized" };
      }
      const result = await pairingGate.claim({
        senderId: pairingCandidate.senderId,
        code: pairingCandidate.code,
      });
      try {
        await pairingGate.sendGenericResponse({
          chatId: pairingCandidate.chatId,
          messageId: pairingCandidate.messageId,
          ...(pairingCandidate.threadId !== undefined
            ? { threadId: pairingCandidate.threadId }
            : {}),
          text: TELEGRAM_PAIRING_RESPONSE,
        });
      } catch (error) {
        try {
          pairingGate.recordSideEffectFailure("response", error);
        } catch {
          // Diagnostics must not reject an already admitted pairing claim.
        }
      }
      if (result.kind === "claimed") {
        try {
          await pairingGate.onPaired(ctx);
        } catch (error) {
          try {
            pairingGate.recordSideEffectFailure("on-paired", error);
          } catch {
            // Diagnostics must not reject an already admitted pairing claim.
          }
        }
      }
      return { kind: "completed", reason: "unauthorized" };
    }
    const allowedUserId = pairingGate.getAllowedUserId();
    if (!isValidTelegramAllowedUserId(allowedUserId)) {
      return { kind: "completed", reason: "unauthorized" };
    }
    const senderId = getTelegramUpdateSenderId(update);
    if (senderId !== undefined && senderId !== allowedUserId) {
      return { kind: "completed", reason: "unauthorized" };
    }
    if (senderId === allowedUserId) {
      const priorityOutcome = await deps.priorityHandle?.(update, ctx);
      if (priorityOutcome === "route-to-default") {
        return defaultHandle(update, ctx);
      }
      if (priorityOutcome && priorityOutcome !== "pass") {
        return priorityOutcome;
      }
      const verdict = await registry.dispatch(
        update,
        (handlerId, handlerCategory) => {
          deps.recordRuntimeEvent?.(
            "public-handler",
            new Error("Public handler failed"),
            { handlerId, handlerCategory },
          );
        },
      );
      if (verdict === "consume") {
        return { kind: "completed", reason: "public-handler" };
      }
    }
    return defaultHandle(update, ctx);
  };
}

/**
 * Register a handler that runs before pi-telegram routes a Telegram update
 * through its built-in handlers.
 *
 * This is the low-level public surface for extensions that share the same bot
 * and Pi process with pi-telegram.
 */
export function registerTelegramUpdateHandler(
  handler: TelegramUpdateHandler,
): () => void {
  return getOrCreateUpdateHandlerRegistry().add(handler);
}
