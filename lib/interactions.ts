/**
 * Telegram-owned active-turn interaction runtime
 * Zones: telegram interaction, extension API, runtime binding
 * Owns request normalization, classic single-question state, snapshot/authority lifecycle sequencing, opaque callback tokens, and idempotent terminal cleanup while inbound routing and waiting-activity policy remain outside this domain
 */

import { randomBytes } from "node:crypto";

import type {
  TelegramDeliveryFailureReason,
  TelegramDeliveryHandle,
  TelegramDeliveryMutationOptions,
  TelegramDeliveryResult,
  TelegramDeliveryTarget,
  TelegramDeliveryView,
} from "./delivery.ts";
import type {
  TelegramRuntimeTypingPort,
  TelegramTypingWaitingLease,
} from "./runtime.ts";
import {
  assertTelegramCallbackData,
  getTelegramCallbackDataByteLength,
  type TelegramInlineKeyboardMarkup,
} from "./keyboard.ts";

const TELEGRAM_INTERACTION_RUNTIME_KEY = "__piTelegramInteractionRuntime__";
const DEFAULT_TIMEOUT_MS = 900_000;
const MIN_TIMEOUT_MS = 1_000;
const MAX_TIMEOUT_MS = 3_600_000;
const MAX_ANSWER_BYTES = 48 * 1024;
const MAX_CALLBACK_BYTES = 40;
const FINALIZATION_TIMEOUT_MS = 2_000;
const CALLBACK_TOKEN_PATTERN = /^[A-Za-z0-9_-]{16}$/;
const CALLBACK_PATTERN =
  /^interact:([A-Za-z0-9_-]{16}):(pick|toggle|submit|other|cancel)(?::([0-9a-z]+))?$/;

export interface TelegramInteractionOption {
  label: string;
  value?: string;
  description?: string;
}

export type TelegramInteractionMode =
  | { kind: "text" }
  | {
      kind: "single-select";
      options: readonly TelegramInteractionOption[];
    }
  | {
      kind: "multi-select";
      options: readonly TelegramInteractionOption[];
    };

export interface TelegramInteractionRequest {
  question: string;
  details?: string;
  mode: TelegramInteractionMode;
  timeoutMs?: number;
  signal?: AbortSignal;
}

export type TelegramInteractionAnswer =
  | { type: "text"; label: string; value: string }
  | {
      type: "option";
      index: number;
      label: string;
      value: string;
    }
  | { type: "other"; label: string; value: string };

export type TelegramInteractionAttempt =
  | {
      handled: false;
      reason: "no-active-telegram-turn" | "runtime-unavailable";
    }
  | { handled: true; result: TelegramInteractionResult };

export type TelegramInteractionResult =
  | {
      status: "answered";
      answers: readonly TelegramInteractionAnswer[];
    }
  | {
      status: "cancelled" | "timed-out" | "unavailable";
      message?: string;
    };

interface NormalizedTelegramInteractionOption {
  label: string;
  value: string;
  description?: string;
}

interface NormalizedTelegramInteractionRequest {
  question: string;
  details?: string;
  mode:
    | { kind: "text" }
    | {
        kind: "single-select" | "multi-select";
        options: readonly NormalizedTelegramInteractionOption[];
      };
  timeoutMs: number;
  signal?: AbortSignal;
}

/** @internal */
export interface TelegramInteractionActiveTurnSnapshot {
  readonly turnId: string;
  readonly target: TelegramDeliveryTarget;
  readonly sourceMessageId?: number;
  readonly sourceMessageIds: readonly number[];
  readonly profile: string;
  readonly transportGeneration: string;
  readonly sessionGeneration: string;
  readonly authorityGeneration: string;
}

/** @internal */
export interface TelegramInteractionInputIdentity {
  readonly target: TelegramDeliveryTarget;
  readonly profile: string;
  readonly transportGeneration: string;
  readonly sessionGeneration: string;
  readonly authorityGeneration: string;
}

/** @internal */
export interface TelegramInteractionCallbackInput
  extends TelegramInteractionInputIdentity {
  kind: "callback";
  callbackData: string;
  messageId: number;
}

/** @internal */
export interface TelegramInteractionTextInput
  extends TelegramInteractionInputIdentity {
  kind: "text";
  text: string;
  messageId: number;
  replyToMessageId: number;
}

/** @internal */
export type TelegramInteractionInput =
  | TelegramInteractionCallbackInput
  | TelegramInteractionTextInput;

/** @internal */
export type TelegramInteractionRoutedInput =
  | {
      kind: "callback";
      target: TelegramDeliveryTarget;
      callbackData: string;
      messageId: number;
    }
  | {
      kind: "text";
      target: TelegramDeliveryTarget;
      text: string;
      messageId: number;
      replyToMessageId: number;
    };

/** @internal */
export type TelegramInteractionInputOutcome =
  | { handled: false }
  | {
      handled: true;
      accepted: boolean;
      settled: boolean;
      message?: string;
    };

/** @internal */
export interface TelegramInteractionPreparedCallback {
  readonly acknowledgement: string;
  commit(): Promise<TelegramInteractionInputOutcome>;
}

/** @internal */
export interface TelegramInteractionDeliveryPort {
  sendView(
    view: TelegramDeliveryView,
    options: {
      scope: { kind: "active-turn" };
      replyToMessageId?: number;
    },
  ): Promise<TelegramDeliveryResult<TelegramDeliveryHandle>>;
  editView(
    handle: TelegramDeliveryHandle,
    view: TelegramDeliveryView,
    options?: TelegramDeliveryMutationOptions,
  ): Promise<TelegramDeliveryResult<TelegramDeliveryHandle>>;
  deleteView(
    handle: TelegramDeliveryHandle,
    options?: TelegramDeliveryMutationOptions,
  ): Promise<TelegramDeliveryResult<void>>;
}

/** @internal */
export interface TelegramInteractionRuntimeDeps {
  generation: string;
  captureActiveTurn(): TelegramInteractionActiveTurnSnapshot | undefined;
  isActive(snapshot: TelegramInteractionActiveTurnSnapshot): boolean;
  delivery: TelegramInteractionDeliveryPort;
  waiting?: Pick<
    TelegramRuntimeTypingPort,
    "acquireWaitingLease" | "clearWaitingLease"
  >;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
  createToken?: () => string;
  createCorrelationId?: () => string;
  getNowMs?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => unknown;
  clearTimer?: (timer: unknown) => void;
  finalizationTimeoutMs?: number;
}

/** @internal */
export interface TelegramInteractionRuntime {
  readonly generation: string;
  request(request: TelegramInteractionRequest): Promise<TelegramInteractionAttempt>;
  prepareCallback(
    input: TelegramInteractionCallbackInput,
  ): TelegramInteractionPreparedCallback;
  handleInput(input: TelegramInteractionInput): Promise<TelegramInteractionInputOutcome>;
  isPending(): boolean;
  invalidateAuthority(snapshot?: TelegramInteractionActiveTurnSnapshot): void;
  shutdown(): void;
}

interface CurrentInteraction {
  readonly snapshot: TelegramInteractionActiveTurnSnapshot;
  readonly request: NormalizedTelegramInteractionRequest;
  readonly resolve: (attempt: TelegramInteractionAttempt) => void;
  readonly correlationId: string;
  readonly startedAtMs: number;
  phase: "claimed" | "updating" | "awaiting-choice" | "awaiting-text";
  pendingPhase?: "awaiting-choice" | "awaiting-text";
  token?: string;
  handle?: TelegramDeliveryHandle;
  replyAnchorMessageId?: number;
  selectedIndexes: Set<number>;
  timeout?: unknown;
  abortListener?: () => void;
  mutationChain: Promise<void>;
  mutationAmbiguous: boolean;
  finalizationExpired: boolean;
  waitingLease?: TelegramTypingWaitingLease;
  waitingLeasePromise?: Promise<TelegramTypingWaitingLease>;
  waitingRelease?: { resumeIfActive: boolean };
  settled: boolean;
}

interface ParsedCallback {
  token: string;
  action: "pick" | "toggle" | "submit" | "other" | "cancel";
  index?: number;
}

interface InteractionRuntimeRegistry {
  runtime?: TelegramInteractionRuntime;
}

function getInteractionRuntimeRegistry(): InteractionRuntimeRegistry {
  const globals = globalThis as Record<string, unknown>;
  const existing = globals[TELEGRAM_INTERACTION_RUNTIME_KEY];
  if (existing && typeof existing === "object" && "runtime" in existing) {
    return existing as InteractionRuntimeRegistry;
  }
  const registry: InteractionRuntimeRegistry = {};
  globals[TELEGRAM_INTERACTION_RUNTIME_KEY] = registry;
  return registry;
}

function unavailable(message?: string): TelegramInteractionAttempt {
  return {
    handled: true,
    result: {
      status: "unavailable",
      ...(message ? { message } : {}),
    },
  };
}

function normalizeOptionalText(
  value: unknown,
  maxLength: number,
): string | undefined | null {
  if (value === undefined) return undefined;
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  if (normalized.length === 0) return undefined;
  return normalized.length <= maxLength ? normalized : null;
}

function isAbortSignal(value: unknown): value is AbortSignal {
  return (
    typeof value === "object" &&
    value !== null &&
    "aborted" in value &&
    typeof value.aborted === "boolean" &&
    "addEventListener" in value &&
    typeof value.addEventListener === "function" &&
    "removeEventListener" in value &&
    typeof value.removeEventListener === "function"
  );
}

function normalizeRequest(
  request: TelegramInteractionRequest,
): NormalizedTelegramInteractionRequest | undefined {
  if (!request || typeof request !== "object") return undefined;
  if (typeof request.question !== "string") return undefined;
  const question = request.question.trim();
  if (question.length === 0 || question.length > 4_000) return undefined;
  const details = normalizeOptionalText(request.details, 8_000);
  if (details === null) return undefined;
  const timeoutMs = request.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  if (
    !Number.isFinite(timeoutMs) ||
    !Number.isInteger(timeoutMs) ||
    timeoutMs < MIN_TIMEOUT_MS ||
    timeoutMs > MAX_TIMEOUT_MS
  ) {
    return undefined;
  }
  if (request.signal !== undefined && !isAbortSignal(request.signal)) {
    return undefined;
  }
  const mode = request.mode as unknown;
  if (!mode || typeof mode !== "object" || !("kind" in mode)) return undefined;
  const kind = (mode as { kind?: unknown }).kind;
  if (kind === "text") {
    if ("options" in mode) return undefined;
    return {
      question,
      ...(details ? { details } : {}),
      mode: { kind },
      timeoutMs,
      ...(request.signal ? { signal: request.signal } : {}),
    };
  }
  if (kind !== "single-select" && kind !== "multi-select") return undefined;
  const options = (mode as { options?: unknown }).options;
  if (!Array.isArray(options) || options.length < 1 || options.length > 20) {
    return undefined;
  }
  const normalizedOptions: NormalizedTelegramInteractionOption[] = [];
  for (const option of options) {
    if (!option || typeof option !== "object") return undefined;
    const candidate = option as {
      label?: unknown;
      value?: unknown;
      description?: unknown;
    };
    if (typeof candidate.label !== "string") return undefined;
    const label = candidate.label.trim();
    if (label.length === 0 || label.length > 1_000) return undefined;
    if (
      candidate.value !== undefined &&
      (typeof candidate.value !== "string" || candidate.value.length > 1_000)
    ) {
      return undefined;
    }
    const description = normalizeOptionalText(candidate.description, 2_000);
    if (description === null) return undefined;
    normalizedOptions.push({
      label,
      value: candidate.value === undefined ? label : candidate.value,
      ...(description ? { description } : {}),
    });
  }
  return {
    question,
    ...(details ? { details } : {}),
    mode: { kind, options: Object.freeze(normalizedOptions) },
    timeoutMs,
    ...(request.signal ? { signal: request.signal } : {}),
  };
}

function sameTarget(
  left: TelegramDeliveryTarget,
  right: TelegramDeliveryTarget,
): boolean {
  return left.chatId === right.chatId && left.threadId === right.threadId;
}

function sameInputIdentity(
  snapshot: TelegramInteractionActiveTurnSnapshot,
  input: TelegramInteractionInputIdentity,
): boolean {
  return (
    sameTarget(snapshot.target, input.target) &&
    snapshot.profile === input.profile &&
    snapshot.transportGeneration === input.transportGeneration &&
    snapshot.sessionGeneration === input.sessionGeneration &&
    snapshot.authorityGeneration === input.authorityGeneration
  );
}

function createOpaqueToken(): string {
  return randomBytes(12).toString("base64url");
}

function createInteractionCorrelationId(): string {
  return randomBytes(9).toString("base64url");
}

function createCallbackData(
  token: string,
  action: ParsedCallback["action"],
  index?: number,
): string {
  const callbackData = `interact:${token}:${action}${index === undefined ? "" : `:${index.toString(36)}`}`;
  if (getTelegramCallbackDataByteLength(callbackData) > MAX_CALLBACK_BYTES) {
    throw new Error("Telegram interaction callback_data exceeds 40 bytes.");
  }
  return assertTelegramCallbackData(
    callbackData,
    "Telegram interaction callback_data",
  );
}

/** @internal */
export function parseTelegramInteractionCallbackData(
  callbackData: string,
): ParsedCallback | undefined {
  if (
    typeof callbackData !== "string" ||
    getTelegramCallbackDataByteLength(callbackData) > MAX_CALLBACK_BYTES
  ) {
    return undefined;
  }
  const match = CALLBACK_PATTERN.exec(callbackData);
  if (!match) return undefined;
  const [, token, action, encodedIndex] = match;
  if (!token || !CALLBACK_TOKEN_PATTERN.test(token) || !action) return undefined;
  const needsIndex = action === "pick" || action === "toggle";
  if (needsIndex !== (encodedIndex !== undefined)) return undefined;
  if (!needsIndex) return { token, action: action as ParsedCallback["action"] };
  if (!encodedIndex || encodedIndex.length > 2) return undefined;
  const index = Number.parseInt(encodedIndex, 36);
  if (!Number.isSafeInteger(index) || index < 0) return undefined;
  return { token, action: action as ParsedCallback["action"], index };
}

function truncateButtonLabel(label: string, maxLength = 56): string {
  return label.length <= maxLength ? label : `${label.slice(0, maxLength - 1)}…`;
}

function renderInteractionText(current: CurrentInteraction, terminal?: string): string {
  const { request } = current;
  const visiblePhase = current.pendingPhase ?? current.phase;
  const sections = [request.question];
  if (request.details) sections.push(request.details);
  if (request.mode.kind !== "text") {
    const optionLines = request.mode.options.map((option, index) => {
      const selected = current.selectedIndexes.has(index) ? "[x]" : "[ ]";
      const prefix = request.mode.kind === "multi-select" ? `${selected} ` : "";
      return `${prefix}${index + 1}. ${option.label}${option.description ? `\n   ${option.description}` : ""}`;
    });
    sections.push(optionLines.join("\n"));
  }
  if (visiblePhase === "awaiting-text" && request.mode.kind !== "text") {
    sections.push("Reply to this message with the Other answer.");
  } else if (visiblePhase === "awaiting-text") {
    sections.push("Reply to this message with your answer.");
  }
  if (terminal) sections.push(terminal);
  return sections.join("\n\n");
}

function renderKeyboard(current: CurrentInteraction): TelegramInlineKeyboardMarkup {
  const token = current.token;
  if (!token) return { inline_keyboard: [] };
  const visiblePhase = current.pendingPhase ?? current.phase;
  const rows: TelegramInlineKeyboardMarkup["inline_keyboard"] = [];
  if (
    visiblePhase === "awaiting-choice" &&
    current.request.mode.kind !== "text"
  ) {
    current.request.mode.options.forEach((option, index) => {
      const selectionPrefix =
        current.request.mode.kind === "multi-select" && current.selectedIndexes.has(index)
          ? "[x] "
          : "";
      rows.push([
        {
          text: `${selectionPrefix}${index + 1}. ${truncateButtonLabel(option.label)}`,
          callback_data: createCallbackData(
            token,
            current.request.mode.kind === "single-select" ? "pick" : "toggle",
            index,
          ),
        },
      ]);
    });
    if (current.request.mode.kind === "multi-select") {
      rows.push([
        { text: "Submit", callback_data: createCallbackData(token, "submit") },
        { text: "Other", callback_data: createCallbackData(token, "other") },
      ]);
    } else {
      rows.push([
        { text: "Other", callback_data: createCallbackData(token, "other") },
      ]);
    }
  }
  rows.push([
    { text: "Cancel", callback_data: createCallbackData(token, "cancel") },
  ]);
  return { inline_keyboard: rows };
}

function renderInteractionView(
  current: CurrentInteraction,
  terminal?: string,
): TelegramDeliveryView {
  return {
    text: renderInteractionText(current, terminal),
    parseMode: "plain",
    replyMarkup: terminal ? { inline_keyboard: [] } : renderKeyboard(current),
  };
}

function getLastMessageId(handle: TelegramDeliveryHandle): number | undefined {
  return handle.messageIds.at(-1);
}

function normalizeAnswerText(value: string): string | undefined {
  const normalized = value.trim();
  if (normalized.length === 0) return undefined;
  return new TextEncoder().encode(normalized).byteLength <= MAX_ANSWER_BYTES
    ? normalized
    : undefined;
}

function unrefTimer(timer: unknown): void {
  if (
    timer &&
    typeof timer === "object" &&
    "unref" in timer &&
    typeof timer.unref === "function"
  ) {
    timer.unref();
  }
}

function terminalLabel(result: TelegramInteractionResult): string {
  switch (result.status) {
    case "answered":
      return "Answered.";
    case "cancelled":
      return "Cancelled.";
    case "timed-out":
      return "Timed out.";
    case "unavailable":
      return "Unavailable.";
  }
}

/** @internal */
export function createTelegramInteractionRuntime(
  deps: TelegramInteractionRuntimeDeps,
): TelegramInteractionRuntime {
  let active = true;
  let current: CurrentInteraction | undefined;
  const setTimer = deps.setTimer ?? setTimeout;
  const clearTimer =
    deps.clearTimer ??
    ((timer: unknown) => clearTimeout(timer as ReturnType<typeof setTimeout>));
  const createToken = deps.createToken ?? createOpaqueToken;
  const createCorrelationId =
    deps.createCorrelationId ?? createInteractionCorrelationId;
  const getNowMs = deps.getNowMs ?? Date.now;

  const recordInteractionDiagnostic = (
    interaction: CurrentInteraction,
    phase: string,
    resultClass: "pending" | TelegramInteractionResult["status"],
    failureClass?: TelegramDeliveryFailureReason | "throw" | "timeout",
  ): void => {
    try {
      deps.recordRuntimeEvent?.(
        "interaction",
        failureClass
          ? "Telegram interaction operation failed"
          : "Telegram interaction lifecycle transition",
        {
          phase,
          mode: interaction.request.mode.kind,
          resultClass,
          ...(failureClass ? { failureClass } : {}),
          correlationId: interaction.correlationId,
          durationMs: Math.min(
            MAX_TIMEOUT_MS,
            Math.max(0, getNowMs() - interaction.startedAtMs),
          ),
        },
      );
    } catch {
      // Diagnostics must never affect interaction settlement.
    }
  };

  const recordInteractionPhase = (
    interaction: CurrentInteraction,
    phase: string,
    resultClass: "pending" | TelegramInteractionResult["status"],
  ): void => recordInteractionDiagnostic(interaction, phase, resultClass);

  const rotateToken = (interaction: CurrentInteraction): boolean => {
    const token = createToken();
    if (!CALLBACK_TOKEN_PATTERN.test(token)) return false;
    interaction.token = token;
    return true;
  };

  const publishHandle = (
    interaction: CurrentInteraction,
    handle: TelegramDeliveryHandle | undefined,
  ): void => {
    if (!handle) return;
    interaction.handle = handle;
    interaction.replyAnchorMessageId = getLastMessageId(handle);
  };

  const queueMutation = (
    interaction: CurrentInteraction,
    operation: () => Promise<void>,
  ): Promise<void> => {
    const queued = interaction.mutationChain.then(operation, operation);
    interaction.mutationChain = queued.catch(() => undefined);
    return queued;
  };

  const runBoundedFinalization = (
    interaction: CurrentInteraction,
    result: TelegramInteractionResult,
  ): void => {
    const timeoutMs = deps.finalizationTimeoutMs ?? FINALIZATION_TIMEOUT_MS;
    const finalizationController = new AbortController();
    let timer: unknown;
    const timeout = new Promise<undefined>((resolve) => {
      timer = setTimer(() => {
        interaction.finalizationExpired = true;
        finalizationController.abort();
        recordInteractionDiagnostic(
          interaction,
          "terminal-finalization",
          result.status,
          "timeout",
        );
        resolve(undefined);
      }, timeoutMs);
      unrefTimer(timer);
    });
    const finalize = queueMutation(interaction, async () => {
      if (
        interaction.finalizationExpired ||
        interaction.mutationAmbiguous ||
        !interaction.handle
      ) {
        return;
      }
      let edited: TelegramDeliveryResult<TelegramDeliveryHandle>;
      try {
        edited = await deps.delivery.editView(
          interaction.handle,
          renderInteractionView(interaction, terminalLabel(result)),
          { signal: finalizationController.signal },
        );
      } catch {
        recordInteractionDiagnostic(
          interaction,
          "terminal-edit",
          result.status,
          "throw",
        );
        return;
      }
      if (edited.ok) {
        publishHandle(interaction, edited.value);
        return;
      }
      recordInteractionDiagnostic(
        interaction,
        "terminal-edit",
        result.status,
        edited.reason,
      );
      publishHandle(interaction, edited.partial);
      if (edited.reason === "commit-unknown") {
        interaction.mutationAmbiguous = true;
        return;
      }
      if (!interaction.handle) return;
      let deleted: TelegramDeliveryResult<void>;
      try {
        deleted = await deps.delivery.deleteView(interaction.handle, {
          signal: finalizationController.signal,
        });
      } catch {
        recordInteractionDiagnostic(
          interaction,
          "terminal-delete",
          result.status,
          "throw",
        );
        return;
      }
      if (!deleted.ok) {
        recordInteractionDiagnostic(
          interaction,
          "terminal-delete",
          result.status,
          deleted.reason,
        );
        if (deleted.reason === "commit-unknown") {
          interaction.mutationAmbiguous = true;
        }
      }
    }).catch(() => undefined);
    void Promise.race([finalize, timeout]).finally(() => {
      if (timer !== undefined) clearTimer(timer);
    });
  };

  const releaseWaiting = (
    interaction: CurrentInteraction,
    resumeIfActive: boolean,
  ): void => {
    if (interaction.waitingRelease) return;
    interaction.waitingRelease = { resumeIfActive };
    if (interaction.waitingLease) {
      interaction.waitingLease.release({ resumeIfActive });
      interaction.waitingLease = undefined;
      return;
    }
    deps.waiting?.clearWaitingLease();
  };

  const settle = (
    interaction: CurrentInteraction,
    result: TelegramInteractionResult,
  ): boolean => {
    if (interaction.settled) return false;
    interaction.settled = true;
    interaction.token = undefined;
    if (interaction.timeout !== undefined) {
      clearTimer(interaction.timeout);
      interaction.timeout = undefined;
    }
    if (interaction.abortListener && interaction.request.signal) {
      interaction.request.signal.removeEventListener(
        "abort",
        interaction.abortListener,
      );
      interaction.abortListener = undefined;
    }
    if (current === interaction) current = undefined;
    releaseWaiting(interaction, result.status === "answered");
    recordInteractionPhase(interaction, "waiting-release", result.status);
    interaction.resolve({ handled: true, result });
    runBoundedFinalization(interaction, result);
    return true;
  };

  const settleUnavailable = (
    interaction: CurrentInteraction,
    message?: string,
  ): boolean =>
    settle(interaction, {
      status: "unavailable",
      ...(message ? { message } : {}),
    });

  const updateView = (
    interaction: CurrentInteraction,
    nextPhase: "awaiting-choice" | "awaiting-text",
  ): void => {
    interaction.phase = "updating";
    interaction.pendingPhase = nextPhase;
    void queueMutation(interaction, async () => {
      if (interaction.settled || !interaction.handle) return;
      let edited: TelegramDeliveryResult<TelegramDeliveryHandle>;
      try {
        edited = await deps.delivery.editView(
          interaction.handle,
          renderInteractionView(interaction),
        );
      } catch {
        recordInteractionDiagnostic(
          interaction,
          "update-edit",
          "unavailable",
          "throw",
        );
        settleUnavailable(interaction);
        return;
      }
      if (!edited.ok) {
        recordInteractionDiagnostic(
          interaction,
          "update-edit",
          "unavailable",
          edited.reason,
        );
        publishHandle(interaction, edited.partial);
        if (edited.reason === "commit-unknown") {
          interaction.mutationAmbiguous = true;
        }
        settleUnavailable(interaction);
        return;
      }
      publishHandle(interaction, edited.value);
      if (interaction.settled) return;
      if (!interaction.replyAnchorMessageId) {
        settleUnavailable(interaction);
        return;
      }
      interaction.pendingPhase = undefined;
      interaction.phase = nextPhase;
    }).catch(() => settleUnavailable(interaction));
  };

  const startInteraction = async (interaction: CurrentInteraction): Promise<void> => {
    try {
      interaction.waitingLease = await interaction.waitingLeasePromise;
    } catch {
      recordInteractionPhase(
        interaction,
        "waiting-acquire-failed",
        "unavailable",
      );
      settleUnavailable(interaction);
      return;
    }
    const acquiredLease = interaction.waitingLease;
    if (interaction.waitingRelease && acquiredLease) {
      acquiredLease.release(interaction.waitingRelease);
      interaction.waitingLease = undefined;
    }
    if (interaction.settled) return;
    recordInteractionPhase(interaction, "waiting-acquired", "pending");
    if (interaction.request.signal?.aborted) {
      settle(interaction, { status: "cancelled" });
      return;
    }
    if (!rotateToken(interaction)) {
      settleUnavailable(interaction);
      return;
    }
    interaction.phase =
      interaction.request.mode.kind === "text" ? "awaiting-text" : "awaiting-choice";
    let sent: TelegramDeliveryResult<TelegramDeliveryHandle>;
    try {
      sent = await deps.delivery.sendView(renderInteractionView(interaction), {
        scope: { kind: "active-turn" },
        ...(interaction.snapshot.sourceMessageId !== undefined
          ? { replyToMessageId: interaction.snapshot.sourceMessageId }
          : {}),
      });
    } catch {
      recordInteractionPhase(interaction, "question-render", "unavailable");
      settleUnavailable(interaction);
      return;
    }
    if (!sent.ok) {
      recordInteractionPhase(interaction, "question-render", "unavailable");
      publishHandle(interaction, sent.partial);
      if (sent.reason === "commit-unknown") {
        interaction.mutationAmbiguous = true;
      }
      settleUnavailable(interaction);
      return;
    }
    publishHandle(interaction, sent.value);
    if (interaction.settled) return;
    if (
      !interaction.replyAnchorMessageId ||
      !deps.isActive(interaction.snapshot)
    ) {
      settleUnavailable(interaction);
    }
  };

  const runtime: TelegramInteractionRuntime = {
    generation: deps.generation,
    request(request) {
      if (!active) {
        return Promise.resolve({ handled: false, reason: "runtime-unavailable" });
      }
      const snapshot = deps.captureActiveTurn();
      if (!snapshot) {
        return Promise.resolve({
          handled: false,
          reason: "no-active-telegram-turn",
        });
      }
      if (current) {
        return Promise.resolve(
          unavailable("Another interaction is active."),
        );
      }
      const normalized = normalizeRequest(request);
      if (!normalized) return Promise.resolve(unavailable());
      let resolveAttempt!: (attempt: TelegramInteractionAttempt) => void;
      const attempt = new Promise<TelegramInteractionAttempt>((resolve) => {
        resolveAttempt = resolve;
      });
      const interaction: CurrentInteraction = {
        snapshot,
        request: normalized,
        resolve: resolveAttempt,
        correlationId: createCorrelationId(),
        startedAtMs: getNowMs(),
        phase: "claimed",
        selectedIndexes: new Set<number>(),
        mutationChain: Promise.resolve(),
        mutationAmbiguous: false,
        finalizationExpired: false,
        settled: false,
      };
      current = interaction;
      recordInteractionPhase(interaction, "waiting-acquire", "pending");
      interaction.waitingLeasePromise = deps.waiting
        ? deps.waiting.acquireWaitingLease({
            correlationId: interaction.correlationId,
            isActive: () => deps.isActive(interaction.snapshot),
          })
        : Promise.resolve({ release: () => {} });
      interaction.abortListener = () => {
        settle(interaction, { status: "cancelled" });
      };
      normalized.signal?.addEventListener("abort", interaction.abortListener, {
        once: true,
      });
      interaction.timeout = setTimer(() => {
        settle(interaction, { status: "timed-out" });
      }, normalized.timeoutMs);
      unrefTimer(interaction.timeout);
      void queueMutation(interaction, () => startInteraction(interaction)).catch(
        () => settleUnavailable(interaction),
      );
      return attempt;
    },
    prepareCallback(input) {
      const expiredOutcome = (): TelegramInteractionInputOutcome => ({
        handled: true,
        accepted: false,
        settled: false,
        message: "This interaction has expired.",
      });
      const reject = (): TelegramInteractionPreparedCallback => ({
        acknowledgement: "This interaction has expired.",
        commit: async () => expiredOutcome(),
      });
      const interaction = current;
      if (!interaction || !sameInputIdentity(interaction.snapshot, input)) {
        return reject();
      }
      if (!deps.isActive(interaction.snapshot)) {
        return {
          acknowledgement: "This interaction has expired.",
          async commit() {
            const settled = current === interaction && settleUnavailable(interaction);
            return {
              handled: true,
              accepted: false,
              settled,
              message: "This interaction has expired.",
            };
          },
        };
      }
      const anchor = interaction.replyAnchorMessageId;
      if (
        !anchor ||
        interaction.phase === "claimed" ||
        interaction.phase === "updating" ||
        input.messageId !== anchor
      ) {
        return reject();
      }
      const parsed = parseTelegramInteractionCallbackData(input.callbackData);
      if (!parsed || parsed.token !== interaction.token) return reject();
      const mode = interaction.request.mode;
      const validAction =
        parsed.action === "cancel" ||
        (parsed.action === "other" && mode.kind !== "text") ||
        (parsed.action === "pick" &&
          mode.kind === "single-select" &&
          parsed.index !== undefined &&
          parsed.index < mode.options.length) ||
        (parsed.action === "toggle" &&
          mode.kind === "multi-select" &&
          parsed.index !== undefined &&
          parsed.index < mode.options.length) ||
        (parsed.action === "submit" && mode.kind === "multi-select");
      if (!validAction) return reject();

      // Claim the one-use token before acknowledgement. The commit is the only
      // seam allowed to mutate the view or settle the pending interaction.
      interaction.token = undefined;
      const acknowledgement =
        parsed.action === "submit" && interaction.selectedIndexes.size === 0
          ? "Select at least one option before submitting."
          : "";
      return {
        acknowledgement,
        async commit() {
          if (interaction.settled || current !== interaction) {
            return expiredOutcome();
          }
          if (!deps.isActive(interaction.snapshot)) {
            settleUnavailable(interaction);
            return { handled: true, accepted: false, settled: true };
          }
          if (parsed.action === "cancel") {
            settle(interaction, { status: "cancelled" });
            return { handled: true, accepted: true, settled: true };
          }
          if (parsed.action === "other") {
            if (!rotateToken(interaction)) {
              settleUnavailable(interaction);
              return { handled: true, accepted: false, settled: true };
            }
            updateView(interaction, "awaiting-text");
            return { handled: true, accepted: true, settled: false };
          }
          if (parsed.action === "pick") {
            const option =
              interaction.request.mode.kind === "single-select" &&
              parsed.index !== undefined
                ? interaction.request.mode.options[parsed.index]
                : undefined;
            if (!option) return expiredOutcome();
            settle(interaction, {
              status: "answered",
              answers: [
                {
                  type: "option",
                  index: parsed.index! + 1,
                  label: option.label,
                  value: option.value,
                },
              ],
            });
            return { handled: true, accepted: true, settled: true };
          }
          const multiMode = interaction.request.mode;
          if (multiMode.kind !== "multi-select") return expiredOutcome();
          if (parsed.action === "toggle") {
            const index = parsed.index!;
            if (interaction.selectedIndexes.has(index)) {
              interaction.selectedIndexes.delete(index);
            } else {
              interaction.selectedIndexes.add(index);
            }
            if (!rotateToken(interaction)) {
              settleUnavailable(interaction);
              return { handled: true, accepted: false, settled: true };
            }
            updateView(interaction, "awaiting-choice");
            return { handled: true, accepted: true, settled: false };
          }
          if (interaction.selectedIndexes.size === 0) {
            if (!rotateToken(interaction)) {
              settleUnavailable(interaction);
              return { handled: true, accepted: false, settled: true };
            }
            updateView(interaction, "awaiting-choice");
            return {
              handled: true,
              accepted: false,
              settled: false,
              message: acknowledgement,
            };
          }
          const answers = [...interaction.selectedIndexes]
            .sort((left, right) => left - right)
            .map((index): TelegramInteractionAnswer => {
              const option = multiMode.options[index]!;
              return {
                type: "option",
                index: index + 1,
                label: option.label,
                value: option.value,
              };
            });
          settle(interaction, { status: "answered", answers });
          return { handled: true, accepted: true, settled: true };
        },
      };
    },
    async handleInput(input) {
      if (input.kind === "callback") {
        return runtime.prepareCallback(input).commit();
      }
      const interaction = current;
      if (!interaction) return { handled: false };
      if (!sameInputIdentity(interaction.snapshot, input)) {
        return { handled: true, accepted: false, settled: false };
      }
      if (!deps.isActive(interaction.snapshot)) {
        settleUnavailable(interaction);
        return { handled: true, accepted: false, settled: true };
      }
      const anchor = interaction.replyAnchorMessageId;
      if (!anchor || interaction.phase === "claimed" || interaction.phase === "updating") {
        return { handled: true, accepted: false, settled: false };
      }
      if (
        interaction.phase !== "awaiting-text" ||
        input.replyToMessageId !== anchor ||
        input.messageId <= anchor ||
        interaction.snapshot.sourceMessageIds.includes(input.messageId)
      ) {
        return { handled: true, accepted: false, settled: false };
      }
      const answer = normalizeAnswerText(input.text);
      if (!answer) {
        return {
          handled: true,
          accepted: false,
          settled: false,
          message: "Reply with a non-empty text answer up to 48 KiB.",
        };
      }
      const type = interaction.request.mode.kind === "text" ? "text" : "other";
      settle(interaction, {
        status: "answered",
        answers: [{ type, label: answer, value: answer }],
      });
      return { handled: true, accepted: true, settled: true };
    },
    isPending() {
      return current !== undefined && !current.settled;
    },
    invalidateAuthority(snapshot) {
      const interaction = current;
      if (!interaction) return;
      if (snapshot && snapshot !== interaction.snapshot) return;
      settleUnavailable(interaction);
    },
    shutdown() {
      if (!active) return;
      active = false;
      const interaction = current;
      if (interaction) settleUnavailable(interaction);
    },
  };
  return runtime;
}

/** @internal */
export function bindTelegramInteractionRuntime(
  runtime: TelegramInteractionRuntime,
): () => void {
  const registry = getInteractionRuntimeRegistry();
  if (registry.runtime !== runtime) registry.runtime?.shutdown();
  registry.runtime = runtime;
  return () => {
    if (registry.runtime !== runtime) return;
    registry.runtime = undefined;
    runtime.shutdown();
  };
}

/** @internal */
export function clearTelegramInteractionRuntime(): void {
  const registry = getInteractionRuntimeRegistry();
  registry.runtime?.shutdown();
  registry.runtime = undefined;
}

/** @internal */
export function createTelegramInteractionLifecycleHooks(
  createRuntime: () => TelegramInteractionRuntime,
): TelegramInteractionLifecycleRuntime {
  let runtime: TelegramInteractionRuntime | undefined;
  let unbind: (() => void) | undefined;
  const stop = () => {
    unbind?.();
    unbind = undefined;
    runtime?.shutdown();
    runtime = undefined;
  };
  return {
    async onSessionStart() {
      stop();
      runtime = createRuntime();
      unbind = bindTelegramInteractionRuntime(runtime);
    },
    async onSessionShutdown() {
      stop();
    },
    invalidateAuthority() {
      runtime?.invalidateAuthority();
    },
    prepareCallback(input) {
      return runtime?.prepareCallback(input) ?? {
        acknowledgement: "This interaction has expired.",
        commit: async () => ({
          handled: true,
          accepted: false,
          settled: false,
          message: "This interaction has expired.",
        }),
      };
    },
    handleInput(input) {
      return runtime?.handleInput(input) ?? Promise.resolve({
        handled: true,
        accepted: false,
        settled: false,
        ...(input.kind === "callback"
          ? { message: "This interaction has expired." }
          : {}),
      });
    },
    isPending() {
      return runtime?.isPending() ?? false;
    },
  };
}

/** @internal */
export interface TelegramInteractionLifecycleRuntime {
  onSessionStart(): Promise<void>;
  onSessionShutdown(): Promise<void>;
  invalidateAuthority(): void;
  isPending(): boolean;
  prepareCallback(
    input: TelegramInteractionCallbackInput,
  ): TelegramInteractionPreparedCallback;
  handleInput(input: TelegramInteractionInput): Promise<TelegramInteractionInputOutcome>;
}

/** @internal */
export interface TelegramInteractionBridgeLifecycleRuntime
  extends Omit<
    TelegramInteractionLifecycleRuntime,
    "prepareCallback" | "handleInput"
  > {
  rebindAuthority(): Promise<void>;
  prepareRoutedCallback(
    input: Extract<TelegramInteractionRoutedInput, { kind: "callback" }>,
  ): TelegramInteractionPreparedCallback;
  handleRoutedInput(
    input: TelegramInteractionRoutedInput,
  ): Promise<TelegramInteractionInputOutcome>;
}

interface TelegramInteractionTurnView<TTransportStamp> {
  readonly queueOrder: number;
  readonly laneOrder: number;
  readonly replyToMessageId: number;
  readonly sourceMessageIds: readonly number[];
  readonly transportStamp?: TTransportStamp;
}

interface TelegramInteractionTransportStampView {
  readonly profile: string;
  readonly generation: string;
}

interface TelegramInteractionAuthorityView {
  readonly route: "direct" | "follower" | "none";
  readonly directEpoch?: number | string;
  readonly followerGeneration?: string;
}

/** @internal */
export function createTelegramInteractionBridgeLifecycleRuntime<
  TTransportStamp extends TelegramInteractionTransportStampView,
  TTurn extends TelegramInteractionTurnView<TTransportStamp>,
  TAuthority extends TelegramInteractionAuthorityView,
>(deps: {
  generationSeed: string;
  activeTurn: {
    get(): TTurn | undefined;
    getTarget(): TelegramDeliveryTarget | undefined;
  };
  session: {
    getGeneration(): number | string;
  };
  transport: {
    getStamp(): TTransportStamp;
    isActive(stamp: TTransportStamp): boolean;
  };
  authority: {
    capture(): TAuthority;
    isActive(authority: TAuthority): boolean;
  };
  delivery: TelegramInteractionDeliveryPort;
  waiting?: Pick<
    TelegramRuntimeTypingPort,
    "acquireWaitingLease" | "clearWaitingLease"
  >;
  recordRuntimeEvent?: TelegramInteractionRuntimeDeps["recordRuntimeEvent"];
  deliveryLifecycle: {
    onSessionStart(): Promise<void>;
    onSessionShutdown(): Promise<void>;
  };
}): TelegramInteractionBridgeLifecycleRuntime {
  let generationSequence = 0;
  const getTurnId = (turn: TTurn): string =>
    `${turn.queueOrder}:${turn.laneOrder}:${turn.replyToMessageId}`;
  const getAuthorityGeneration = (authority: TAuthority): string =>
    authority.route === "direct"
      ? `direct:${String(authority.directEpoch ?? "missing")}`
      : authority.route === "follower"
        ? `follower:${authority.followerGeneration ?? "missing"}`
        : "none";
  const interactionLifecycle = createTelegramInteractionLifecycleHooks(() =>
    createTelegramInteractionRuntime({
      generation: `${deps.generationSeed}:${++generationSequence}`,
      captureActiveTurn() {
        const turn = deps.activeTurn.get();
        const target = deps.activeTurn.getTarget();
        if (!turn || !target) return undefined;
        const transportStamp = turn.transportStamp ?? deps.transport.getStamp();
        const authority = deps.authority.capture();
        if (
          !deps.transport.isActive(transportStamp) ||
          !deps.authority.isActive(authority)
        ) {
          return undefined;
        }
        return {
          turnId: getTurnId(turn),
          target: { ...target },
          sourceMessageId: turn.replyToMessageId,
          sourceMessageIds: [...turn.sourceMessageIds],
          profile: transportStamp.profile,
          transportGeneration: transportStamp.generation,
          sessionGeneration: String(deps.session.getGeneration()),
          authorityGeneration: getAuthorityGeneration(authority),
        };
      },
      isActive(snapshot) {
        const turn = deps.activeTurn.get();
        const target = deps.activeTurn.getTarget();
        if (
          !turn ||
          !target ||
          snapshot.turnId !== getTurnId(turn) ||
          target.chatId !== snapshot.target.chatId ||
          target.threadId !== snapshot.target.threadId ||
          String(deps.session.getGeneration()) !== snapshot.sessionGeneration
        ) {
          return false;
        }
        const transportStamp = turn.transportStamp ?? deps.transport.getStamp();
        if (
          transportStamp.profile !== snapshot.profile ||
          transportStamp.generation !== snapshot.transportGeneration ||
          !deps.transport.isActive(transportStamp)
        ) {
          return false;
        }
        const authority = deps.authority.capture();
        return (
          getAuthorityGeneration(authority) === snapshot.authorityGeneration &&
          deps.authority.isActive(authority)
        );
      },
      delivery: deps.delivery,
      waiting: deps.waiting,
      recordRuntimeEvent: deps.recordRuntimeEvent,
    }),
  );
  return {
    async onSessionStart() {
      await deps.deliveryLifecycle.onSessionStart();
      await interactionLifecycle.onSessionStart();
    },
    async onSessionShutdown() {
      await interactionLifecycle.onSessionShutdown();
      await deps.deliveryLifecycle.onSessionShutdown();
    },
    invalidateAuthority: interactionLifecycle.invalidateAuthority,
    isPending: interactionLifecycle.isPending,
    prepareRoutedCallback(input) {
      const transportStamp = deps.transport.getStamp();
      const authority = deps.authority.capture();
      return interactionLifecycle.prepareCallback({
        ...input,
        profile: transportStamp.profile,
        transportGeneration: transportStamp.generation,
        sessionGeneration: String(deps.session.getGeneration()),
        authorityGeneration: getAuthorityGeneration(authority),
      });
    },
    handleRoutedInput(input) {
      const transportStamp = deps.transport.getStamp();
      const authority = deps.authority.capture();
      return interactionLifecycle.handleInput({
        ...input,
        profile: transportStamp.profile,
        transportGeneration: transportStamp.generation,
        sessionGeneration: String(deps.session.getGeneration()),
        authorityGeneration: getAuthorityGeneration(authority),
      });
    },
    async rebindAuthority() {
      await interactionLifecycle.onSessionShutdown();
      await deps.deliveryLifecycle.onSessionStart();
      await interactionLifecycle.onSessionStart();
    },
  };
}

export function requestTelegramInteraction(
  request: TelegramInteractionRequest,
): Promise<TelegramInteractionAttempt> {
  const runtime = getInteractionRuntimeRegistry().runtime;
  if (!runtime) {
    return Promise.resolve({ handled: false, reason: "runtime-unavailable" });
  }
  try {
    return runtime.request(request);
  } catch {
    return Promise.resolve({ handled: false, reason: "runtime-unavailable" });
  }
}
