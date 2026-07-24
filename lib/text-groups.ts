/**
 * Telegram text-group coalescing helpers
 * Zones: telegram inbound, queue admission, split-message recovery
 * Owns conservative delayed grouping for Telegram text messages that look like automatic long-message splits
 */

import { setTimeout as waitForTimeout } from "node:timers/promises";

import type { TelegramInboundHandlingOutcome } from "./updates.ts";
import {
  extractTelegramMessageText,
  type TelegramMessageForwardOrigin,
  type TelegramMessageUser,
  type TelegramRichMessage,
} from "./media.ts";

const TELEGRAM_TEXT_GROUP_DEBOUNCE_MS = 1000;
const TELEGRAM_TEXT_GROUP_MIN_SPLIT_LENGTH = 3600;
const TELEGRAM_TEXT_GROUP_MAX_MESSAGE_ID_GAP = 12;

export interface TelegramTextGroupMessage {
  message_id: number;
  media_group_id?: string;
  chat: { id: number };
  message_thread_id?: number;
  from?: { id?: number; is_bot?: boolean };
  text?: string;
  caption?: string;
  rich_message?: TelegramRichMessage;
  forward_origin?: TelegramMessageForwardOrigin;
  forward_from?: TelegramMessageUser;
  forward_sender_name?: string;
}

export interface TelegramTextGroupState<TMessage, TContext = unknown> {
  key?: string;
  messages: TMessage[];
  context?: TContext;
  flushTimer?: ReturnType<typeof setTimeout>;
  dispatching?: boolean;
  suspended?: boolean;
  reschedule?: (delayMs?: number) => void;
  dispatchLimit?: number;
  forwardPairCandidate?: TelegramForwardCommentBatchPosition;
  onSettled?: (
    messages: TMessage[],
    outcome: TelegramInboundHandlingOutcome,
  ) => void | Promise<void>;
  onFailed?: (messages: TMessage[], error: unknown) => void | Promise<void>;
}

export type TelegramForwardCommentBatchPosition = "comment" | "forward";

export interface TelegramTextGroupController<TMessage, TContext = unknown> {
  prepareUpdateBatch: (
    updates: readonly { message?: TMessage }[],
  ) => void;
  getPreparedForwardingPosition: (
    message: TelegramTextGroupMessage,
  ) => TelegramForwardCommentBatchPosition | undefined;
  prepareForwardedMessage: (
    message: TelegramTextGroupMessage,
    position: TelegramForwardCommentBatchPosition,
  ) => void;
  queueMessage: (options: {
    message: TMessage;
    context: TContext;
    dispatchMessages: (
      messages: TMessage[],
      ctx: TContext,
    ) => Promise<TelegramInboundHandlingOutcome>;
    onSettled?: (
      messages: TMessage[],
      outcome: TelegramInboundHandlingOutcome,
    ) => void | Promise<void>;
    onFailed?: (messages: TMessage[], error: unknown) => void | Promise<void>;
  }) => boolean;
  removeMessages: (
    messageIds: number[],
    scope?: { chatId?: number; threadId?: number },
  ) => number;
  suspend: () => void;
  resume: (context: TContext) => void;
  clear: () => void;
}

export interface TelegramTextGroupControllerOptions {
  debounceMs?: number;
  forwardCommentWaitMs?: number | false;
  minSplitLength?: number;
  setTimer?: (
    callback: () => void,
    ms: number,
  ) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}

export interface TelegramTextGroupDispatchRuntime<
  TMessage extends TelegramTextGroupMessage,
  TContext,
> {
  handleMessage: (
    message: TMessage,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
}

export interface TelegramGroupedInputClearerDeps {
  clearMediaGroups: () => void;
  clearTextGroups: () => void;
}

function extractTelegramTextGroupText(
  message: TelegramTextGroupMessage,
): string {
  return extractTelegramMessageText(message);
}

function isTelegramForwardedMessage(
  message: TelegramTextGroupMessage,
): boolean {
  return (
    message.forward_origin !== undefined ||
    message.forward_from !== undefined ||
    typeof message.forward_sender_name === "string"
  );
}

function isTelegramTextGroupCommand(text: string): boolean {
  return text.trimStart().startsWith("/");
}

function isTelegramTextGroupClearingCommand(text: string): boolean {
  const command = text.trimStart().split(/\s+/, 1)[0]?.split("@", 1)[0];
  return command === "/stop";
}

function getTelegramTextGroupMessageIdentity(
  message: TelegramTextGroupMessage,
): string {
  const threadKey =
    typeof message.message_thread_id === "number"
      ? `thread:${message.message_thread_id}`
      : "private";
  return `${message.chat.id}:${threadKey}:${message.message_id}`;
}

export function getTelegramTextGroupKey(
  message: TelegramTextGroupMessage,
): string | undefined {
  if (message.media_group_id) return undefined;
  if (!message.from || message.from.is_bot) return undefined;
  if (
    !extractTelegramTextGroupText(message) &&
    !isTelegramForwardedMessage(message)
  ) {
    return undefined;
  }
  const threadKey =
    typeof message.message_thread_id === "number"
      ? `thread:${message.message_thread_id}`
      : "private";
  return `${message.chat.id}:${threadKey}:${message.from.id}`;
}

function canStartTelegramTextGroup(
  message: TelegramTextGroupMessage,
  minSplitLength: number,
): boolean {
  const text = extractTelegramTextGroupText(message);
  return text.length >= minSplitLength && !isTelegramTextGroupCommand(text);
}

function canAppendTelegramTextGroupMessage<
  TMessage extends TelegramTextGroupMessage,
>(
  state: TelegramTextGroupState<TMessage, unknown>,
  message: TMessage,
): boolean {
  const text = extractTelegramTextGroupText(message);
  const previous = state.messages.at(-1);
  return (
    !!previous &&
    message.message_id > previous.message_id &&
    message.message_id <=
      previous.message_id + TELEGRAM_TEXT_GROUP_MAX_MESSAGE_ID_GAP &&
    (text.length > 0 || isTelegramForwardedMessage(message)) &&
    !isTelegramTextGroupCommand(text)
  );
}

export function queueTelegramTextGroupMessage<
  TMessage extends TelegramTextGroupMessage,
  TContext = unknown,
>(options: {
  message: TMessage;
  context: TContext;
  groups: Map<string, TelegramTextGroupState<TMessage, TContext>>;
  debounceMs: number;
  minSplitLength: number;
  setTimer: (callback: () => void, ms: number) => ReturnType<typeof setTimeout>;
  clearTimer: (timer: ReturnType<typeof setTimeout>) => void;
  dispatchMessages: (
    messages: TMessage[],
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  onSettled?: (
    messages: TMessage[],
    outcome: TelegramInboundHandlingOutcome,
  ) => void | Promise<void>;
  onFailed?: (messages: TMessage[], error: unknown) => void | Promise<void>;
  forceStart?: boolean;
  dispatchImmediately?: boolean;
  forwardPairCandidate?: TelegramForwardCommentBatchPosition;
  delayMs?: number;
}): boolean {
  const key = getTelegramTextGroupKey(options.message);
  if (!key) return false;
  const existing = options.groups.get(key);
  if (existing?.messages.some(
    (message) => message.message_id === options.message.message_id,
  )) return true;
  if (
    !existing &&
    !options.forceStart &&
    !canStartTelegramTextGroup(options.message, options.minSplitLength)
  )
    return false;
  if (existing && !canAppendTelegramTextGroupMessage(existing, options.message))
    return false;
  const state = existing ?? { key, messages: [] };
  state.key = key;
  state.messages.push(options.message);
  state.context = options.context;
  state.forwardPairCandidate = options.forwardPairCandidate;
  state.onSettled = options.onSettled;
  state.onFailed = options.onFailed;
  const dispatchQueued = (): void => {
      state.flushTimer = undefined;
      const queued = options.groups.get(key);
      if (!queued || queued.context === undefined) return;
      if (queued.dispatching) {
        scheduleDispatch();
        return;
      }
      const dispatchCount = queued.dispatchLimit ?? queued.messages.length;
      queued.dispatchLimit = undefined;
      const dispatchedMessages = queued.messages.slice(0, dispatchCount);
      const dispatchedIds = new Set(
        dispatchedMessages.map((message) => message.message_id),
      );
      queued.dispatching = true;
      let dispatchResult: Promise<TelegramInboundHandlingOutcome>;
      try {
        dispatchResult = options.dispatchMessages(
          dispatchedMessages,
          queued.context,
        );
      } catch (error) {
        dispatchResult = Promise.reject(error);
      }
      void Promise.resolve(dispatchResult).then(async (outcome) => {
          const normalizedOutcome =
            outcome ?? ({ kind: "completed", reason: "ignored" } as const);
          if (queued.onSettled) {
            await queued.onSettled(dispatchedMessages, normalizedOutcome);
          }
          if (options.groups.get(key) !== queued) return;
          queued.dispatching = false;
          if (normalizedOutcome.kind === "deferred") {
            queued.suspended = true;
            return;
          }
          queued.messages = queued.messages.filter(
            (message) => !dispatchedIds.has(message.message_id),
          );
          if (queued.messages.length === 0) options.groups.delete(key);
          else if (!queued.flushTimer) scheduleDispatch();
        }).catch(async (error) => {
          if (options.groups.get(key) !== queued) return;
          queued.dispatching = false;
          try {
            if (queued.onFailed) {
              await queued.onFailed(dispatchedMessages, error);
            }
          } finally {
            if (!queued.flushTimer) scheduleDispatch();
          }
        });
  };
  const scheduleDispatch = (delayMs = options.debounceMs): void => {
    if (state.suspended) return;
    state.flushTimer = options.setTimer(dispatchQueued, delayMs);
    state.flushTimer.unref?.();
  };
  state.reschedule = scheduleDispatch;
  if (state.flushTimer) options.clearTimer(state.flushTimer);
  scheduleDispatch(
    options.dispatchImmediately ? 0 : (options.delayMs ?? options.debounceMs),
  );
  options.groups.set(key, state);
  return true;
}

export function createTelegramTextGroupController<
  TMessage extends TelegramTextGroupMessage,
  TContext = unknown,
>(
  options: TelegramTextGroupControllerOptions = {},
): TelegramTextGroupController<TMessage, TContext> {
  const groups = new Map<string, TelegramTextGroupState<TMessage, TContext>>();
  const plannedForwardCommentStarts = new Set<string>();
  const plannedForwardCommentEnds = new Set<string>();
  const debounceMs = options.debounceMs ?? TELEGRAM_TEXT_GROUP_DEBOUNCE_MS;
  const minSplitLength =
    options.minSplitLength ?? TELEGRAM_TEXT_GROUP_MIN_SPLIT_LENGTH;
  const forwardCommentWaitMs =
    options.forwardCommentWaitMs === undefined
      ? debounceMs
      : options.forwardCommentWaitMs;
  const setTimer =
    options.setTimer ??
    ((callback: () => void, ms: number): ReturnType<typeof setTimeout> => {
      const controller = new AbortController();
      void waitForTimeout(ms, undefined, {
        signal: controller.signal,
      }).then(callback, () => undefined);
      return controller as unknown as ReturnType<typeof setTimeout>;
    });
  const clearTimer =
    options.clearTimer ??
    (options.setTimer
      ? clearTimeout
      : (timer: ReturnType<typeof setTimeout>): void => {
          (timer as unknown as AbortController).abort();
        });
  return {
    prepareUpdateBatch(updates) {
      for (let index = 0; index + 1 < updates.length; index += 1) {
        const comment = updates[index]?.message;
        const forwarded = updates[index + 1]?.message;
        if (!comment || !forwarded) continue;
        const commentText = extractTelegramTextGroupText(comment);
        const commentKey = getTelegramTextGroupKey(comment);
        const forwardedKey = getTelegramTextGroupKey(forwarded);
        if (
          !commentKey ||
          commentKey !== forwardedKey ||
          !commentText ||
          isTelegramTextGroupCommand(commentText) ||
          isTelegramForwardedMessage(comment) ||
          !isTelegramForwardedMessage(forwarded) ||
          forwarded.message_id <= comment.message_id ||
          forwarded.message_id >
            comment.message_id + TELEGRAM_TEXT_GROUP_MAX_MESSAGE_ID_GAP
        ) {
          continue;
        }
        plannedForwardCommentStarts.add(
          getTelegramTextGroupMessageIdentity(comment),
        );
        plannedForwardCommentEnds.add(
          getTelegramTextGroupMessageIdentity(forwarded),
        );
      }
    },
    getPreparedForwardingPosition(message) {
      const identity = getTelegramTextGroupMessageIdentity(message);
      if (plannedForwardCommentStarts.has(identity)) return "comment";
      if (plannedForwardCommentEnds.has(identity)) return "forward";
      return undefined;
    },
    prepareForwardedMessage(message, position) {
      const identity = getTelegramTextGroupMessageIdentity(message);
      if (position === "comment") plannedForwardCommentStarts.add(identity);
      else plannedForwardCommentEnds.add(identity);
    },
    queueMessage: ({
      message,
      context,
      dispatchMessages,
      onSettled,
      onFailed,
    }) => {
      const identity = getTelegramTextGroupMessageIdentity(message);
      const key = getTelegramTextGroupKey(message);
      const plannedStart = plannedForwardCommentStarts.delete(identity);
      const forwarded = isTelegramForwardedMessage(message);
      const existing = key ? groups.get(key) : undefined;
      const text = extractTelegramTextGroupText(message);
      if (existing && isTelegramTextGroupClearingCommand(text)) {
        if (existing.flushTimer) clearTimer(existing.flushTimer);
        groups.delete(key!);
      }
      const candidatePosition: TelegramForwardCommentBatchPosition = forwarded
        ? "forward"
        : "comment";
      const existingCandidate = existing?.forwardPairCandidate;
      const pairCompleted =
        existingCandidate !== undefined &&
        existingCandidate !== candidatePosition;
      const separateFromCandidate =
        existingCandidate === candidatePosition &&
        !isTelegramTextGroupCommand(text);
      if (separateFromCandidate && existing) {
        existing.dispatchLimit = existing.messages.length;
      }
      const forceStart =
        plannedStart ||
        (forwardCommentWaitMs !== false &&
          !!key &&
          (forwarded ||
            (typeof message.text === "string" &&
              !isTelegramTextGroupCommand(
                extractTelegramTextGroupText(message),
              ))));
      const dispatchImmediately =
        pairCompleted ||
        separateFromCandidate ||
        plannedForwardCommentEnds.delete(identity) ||
        (forwarded && !!key && groups.has(key));
      return queueTelegramTextGroupMessage({
        message,
        context,
        groups,
        debounceMs,
        minSplitLength,
        setTimer,
        clearTimer,
        dispatchMessages,
        onSettled,
        onFailed,
        forceStart,
        dispatchImmediately,
        forwardPairCandidate:
          forceStart && !canStartTelegramTextGroup(message, minSplitLength)
            ? candidatePosition
            : undefined,
        delayMs:
          forceStart && !canStartTelegramTextGroup(message, minSplitLength)
            ? forwardCommentWaitMs === false
              ? undefined
              : forwardCommentWaitMs
            : undefined,
      });
    },
    removeMessages: (messageIds, scope) => {
      if (messageIds.length === 0 || groups.size === 0) return 0;
      const deleted = new Set(messageIds);
      let removed = 0;
      for (const [key, state] of groups) {
        const retained = state.messages.filter(
          (message) =>
            !deleted.has(message.message_id) ||
            (scope?.chatId !== undefined && message.chat.id !== scope.chatId) ||
            (scope?.threadId !== undefined &&
              message.message_thread_id !== scope.threadId),
        );
        removed += state.messages.length - retained.length;
        if (retained.length === state.messages.length) continue;
        state.messages = retained;
        if (retained.length === 0 && !state.dispatching) {
          if (state.flushTimer) clearTimer(state.flushTimer);
          groups.delete(key);
        }
      }
      return removed;
    },
    suspend: () => {
      for (const state of groups.values()) {
        state.suspended = true;
        if (state.flushTimer) clearTimer(state.flushTimer);
        state.flushTimer = undefined;
      }
    },
    resume: (context) => {
      for (const state of groups.values()) {
        state.context = context;
        state.suspended = false;
        if (!state.dispatching && !state.flushTimer) state.reschedule?.();
      }
    },
    clear: () => {
      for (const state of groups.values()) {
        if (state.flushTimer) clearTimer(state.flushTimer);
      }
      groups.clear();
      plannedForwardCommentStarts.clear();
      plannedForwardCommentEnds.clear();
    },
  };
}

export function createTelegramTextGroupDispatchRuntime<
  TMessage extends TelegramTextGroupMessage,
  TContext,
>(deps: {
  textGroups: TelegramTextGroupController<TMessage, TContext>;
  dispatchMessages: (
    messages: TMessage[],
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  dispatchSingleMessage: (
    message: TMessage,
    ctx: TContext,
  ) => Promise<TelegramInboundHandlingOutcome>;
  onSettled?: (
    messages: TMessage[],
    outcome: TelegramInboundHandlingOutcome,
  ) => void | Promise<void>;
  onFailed?: (messages: TMessage[], error: unknown) => void | Promise<void>;
}): TelegramTextGroupDispatchRuntime<TMessage, TContext> {
  return {
    handleMessage: async (message, ctx) => {
      const queuedTextGroup = deps.textGroups.queueMessage({
        message,
        context: ctx,
        dispatchMessages: (messages, queuedCtx) =>
          deps.dispatchMessages(messages, queuedCtx),
        onSettled: deps.onSettled,
        onFailed: deps.onFailed,
      });
      if (queuedTextGroup) {
        return {
          kind: "deferred",
          reason: "text-group",
          key: getTelegramTextGroupKey(message)!,
        };
      }
      return deps.dispatchSingleMessage(message, ctx);
    },
  };
}

export function createTelegramGroupedInputClearer(
  deps: TelegramGroupedInputClearerDeps,
): () => void {
  return () => {
    deps.clearMediaGroups();
    deps.clearTextGroups();
  };
}
