/**
 * Telegram-owned interaction runtime regressions
 * Zones: telegram interaction, extension API, classic runtime fixtures
 * Protects the frozen public contract, Delivery-backed question lifecycle, exact classic authority, one-use callbacks, and single-settlement cleanup without exercising later update routing or follower forwarding
 */

import assert from "node:assert/strict";
import test from "node:test";

import { requestTelegramInteraction } from "../api/interactions.ts";
import {
  createTelegramDeliveryRuntime,
  type TelegramDeliveryHandle,
  type TelegramDeliveryResult,
  type TelegramDeliveryView,
} from "../lib/delivery.ts";
import * as Runtime from "../lib/runtime.ts";
import {
  bindTelegramInteractionRuntime,
  clearTelegramInteractionRuntime,
  createTelegramInteractionLifecycleHooks,
  createTelegramInteractionRuntime,
  parseTelegramInteractionCallbackData,
  type TelegramInteractionActiveTurnSnapshot,
  type TelegramInteractionAnswer,
  type TelegramInteractionAttempt,
  type TelegramInteractionInputIdentity,
  type TelegramInteractionRequest,
  type TelegramInteractionRuntime,
} from "../lib/interactions.ts";

const TARGET = { chatId: 42 } as const;
const SNAPSHOT: TelegramInteractionActiveTurnSnapshot = {
  turnId: "turn-1",
  target: TARGET,
  sourceMessageId: 50,
  sourceMessageIds: [50],
  profile: "default",
  transportGeneration: "transport-1",
  sessionGeneration: "session-1",
  authorityGeneration: "owner-1",
};
const IDENTITY: TelegramInteractionInputIdentity = {
  target: TARGET,
  profile: "default",
  transportGeneration: "transport-1",
  sessionGeneration: "session-1",
  authorityGeneration: "owner-1",
};
const TOKEN_SEQUENCE = [
  "AbCdEf0123_-xyZ9",
  "BcDeFg1234_-yzA0",
  "CdEfGh2345_-zaB1",
  "DeFgHi3456_-abC2",
  "EfGhIj4567_-bcD3",
] as const;
const TEXT_REQUEST = {
  question: "Which direction should we take?",
  details: "Choose before work continues.",
  mode: { kind: "text" },
  timeoutMs: 1_000,
} as const satisfies TelegramInteractionRequest;

interface RecordedSend {
  view: TelegramDeliveryView;
  replyToMessageId?: number;
}

interface RecordedEdit {
  handle: TelegramDeliveryHandle;
  view: TelegramDeliveryView;
}

interface FixtureOptions {
  active?: boolean;
  authorityActive?: boolean;
  sendResult?: TelegramDeliveryResult<TelegramDeliveryHandle>;
  editMessageIds?: readonly number[];
  updateEditResult?: TelegramDeliveryResult<TelegramDeliveryHandle>;
  updateEditThrow?: boolean;
  finalEditResult?: TelegramDeliveryResult<TelegramDeliveryHandle>;
  finalEditThrow?: boolean;
  deleteResult?: TelegramDeliveryResult<void>;
  deleteThrow?: boolean;
  holdUpdateEdit?: boolean;
  holdFinalEdit?: boolean;
  timers?: FixtureTimers;
  waiting?: Pick<
    Runtime.TelegramRuntimeTypingPort,
    "acquireWaitingLease" | "clearWaitingLease"
  >;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
  createCorrelationId?: () => string;
  getNowMs?: () => number;
}

class FixtureTimers {
  readonly scheduled: Array<{
    callback: () => void;
    delayMs: number;
    cleared: boolean;
  }> = [];

  readonly set = (callback: () => void, delayMs: number): object => {
    const entry = { callback, delayMs, cleared: false };
    this.scheduled.push(entry);
    return entry;
  };

  readonly clear = (timer: unknown): void => {
    const entry = timer as { cleared?: boolean };
    entry.cleared = true;
  };

  fire(delayMs: number): void {
    const entry = this.scheduled.find(
      (candidate) => candidate.delayMs === delayMs && !candidate.cleared,
    );
    assert.ok(entry, `expected an active ${delayMs}ms timer`);
    entry.cleared = true;
    entry.callback();
  }
}

class InteractionFixture {
  readonly sends: RecordedSend[] = [];
  readonly edits: RecordedEdit[] = [];
  readonly deletes: TelegramDeliveryHandle[] = [];
  readonly timers: FixtureTimers;
  readonly runtime: TelegramInteractionRuntime;
  active: boolean;
  authorityActive: boolean;
  snapshot: TelegramInteractionActiveTurnSnapshot | undefined = SNAPSHOT;
  private readonly editMessageIds?: readonly number[];
  private tokenIndex = 0;
  private sendWaiters: Array<() => void> = [];
  private updateEditRelease?: () => void;
  private finalEditRelease?: () => void;
  private latestAnchor = 101;
  private readonly holdUpdateEdit: boolean;
  private readonly holdFinalEdit: boolean;
  private readonly updateEditResult?: TelegramDeliveryResult<TelegramDeliveryHandle>;
  private readonly updateEditThrow: boolean;
  private readonly finalEditResult?: TelegramDeliveryResult<TelegramDeliveryHandle>;
  private readonly finalEditThrow: boolean;
  private readonly deleteResult?: TelegramDeliveryResult<void>;
  private readonly deleteThrow: boolean;
  readonly unbind: () => void;

  constructor(options: FixtureOptions = {}) {
    this.active = options.active ?? true;
    this.authorityActive = options.authorityActive ?? true;
    this.editMessageIds = options.editMessageIds;
    this.updateEditResult = options.updateEditResult;
    this.updateEditThrow = options.updateEditThrow ?? false;
    this.finalEditResult = options.finalEditResult;
    this.finalEditThrow = options.finalEditThrow ?? false;
    this.deleteResult = options.deleteResult;
    this.deleteThrow = options.deleteThrow ?? false;
    this.holdUpdateEdit = options.holdUpdateEdit ?? false;
    this.holdFinalEdit = options.holdFinalEdit ?? false;
    this.timers = options.timers ?? new FixtureTimers();
    const defaultHandle = this.handle([101]);
    const sendResult = options.sendResult ?? {
      ok: true as const,
      value: defaultHandle,
    };
    this.runtime = createTelegramInteractionRuntime({
      generation: "interaction-1",
      captureActiveTurn: () => (this.active ? this.snapshot : undefined),
      isActive: (snapshot) =>
        this.active && this.authorityActive && snapshot === this.snapshot,
      createToken: () =>
        TOKEN_SEQUENCE[this.tokenIndex++ % TOKEN_SEQUENCE.length]!,
      setTimer: this.timers.set,
      clearTimer: this.timers.clear,
      finalizationTimeoutMs: 2_000,
      waiting: options.waiting,
      recordRuntimeEvent: options.recordRuntimeEvent,
      createCorrelationId: options.createCorrelationId,
      getNowMs: options.getNowMs,
      delivery: {
        sendView: async (view, options) => {
          this.sends.push({
            view,
            ...(options.replyToMessageId !== undefined
              ? { replyToMessageId: options.replyToMessageId }
              : {}),
          });
          for (const resolve of this.sendWaiters.splice(0)) resolve();
          return sendResult;
        },
        editView: async (handle, view) => {
          this.edits.push({ handle, view });
          const terminal = /(?:Answered|Cancelled|Timed out|Unavailable)\.$/.test(
            view.text,
          );
          if (this.holdUpdateEdit && !terminal) {
            await new Promise<void>((resolve) => {
              this.updateEditRelease = resolve;
            });
          }
          if (this.holdFinalEdit && terminal) {
            await new Promise<void>((resolve) => {
              this.finalEditRelease = resolve;
            });
          }
          if (!terminal && this.updateEditThrow) {
            throw new Error("private update exception body 42 101");
          }
          if (terminal && this.finalEditThrow) {
            throw new Error("private terminal exception body 42 101");
          }
          if (terminal && this.finalEditResult) return this.finalEditResult;
          if (!terminal && this.updateEditResult) {
            if (this.updateEditResult.ok) {
              this.latestAnchor = getFixtureLastMessageId(this.updateEditResult.value);
            }
            return this.updateEditResult;
          }
          const value = this.handle(this.editMessageIds ?? handle.messageIds);
          this.latestAnchor = getFixtureLastMessageId(value);
          return { ok: true, value };
        },
        deleteView: async (handle) => {
          this.deletes.push(handle);
          if (this.deleteThrow) {
            throw new Error("private delete exception body 42 101");
          }
          return this.deleteResult ?? { ok: true, value: undefined };
        },
      },
    });
    this.unbind = bindTelegramInteractionRuntime(this.runtime);
  }

  handle(messageIds: readonly number[]): TelegramDeliveryHandle {
    return {
      target: TARGET,
      messageIds,
      generation: "delivery-1",
    };
  }

  async waitForSend(): Promise<RecordedSend> {
    if (this.sends.length === 0) {
      await new Promise<void>((resolve) => this.sendWaiters.push(resolve));
    }
    const send = this.sends.at(-1);
    assert.ok(send);
    await Promise.resolve();
    return send;
  }

  latestCallback(action: string, index?: number): string {
    const view = this.edits.at(-1)?.view ?? this.sends.at(-1)?.view;
    assert.ok(view?.replyMarkup);
    const suffix = `${action}${index === undefined ? "" : `:${index.toString(36)}`}`;
    for (const row of view.replyMarkup.inline_keyboard) {
      for (const button of row) {
        if (button.callback_data.endsWith(`:${suffix}`)) {
          return button.callback_data;
        }
      }
    }
    assert.fail(`missing ${suffix} callback`);
  }

  callback(callbackData: string, overrides: Partial<TelegramInteractionInputIdentity> = {}) {
    return this.runtime.handleInput({
      kind: "callback",
      ...IDENTITY,
      ...overrides,
      callbackData,
      messageId: this.currentAnchor(),
    });
  }

  text(
    text: string,
    options: {
      messageId?: number;
      replyToMessageId?: number;
      identity?: Partial<TelegramInteractionInputIdentity>;
    } = {},
  ) {
    return this.runtime.handleInput({
      kind: "text",
      ...IDENTITY,
      ...options.identity,
      text,
      messageId: options.messageId ?? this.currentAnchor() + 1,
      replyToMessageId: options.replyToMessageId ?? this.currentAnchor(),
    });
  }

  currentAnchor(): number {
    return this.latestAnchor;
  }

  releaseUpdateEdit(): void {
    this.updateEditRelease?.();
  }

  releaseFinalEdit(): void {
    this.finalEditRelease?.();
  }
}

function getFixtureLastMessageId(handle: TelegramDeliveryHandle): number {
  const messageId = handle.messageIds.at(-1);
  assert.ok(messageId);
  return messageId;
}

function answered(
  answers: readonly TelegramInteractionAnswer[],
): TelegramInteractionAttempt {
  return { handled: true, result: { status: "answered", answers } };
}

function assertAccepted(
  outcome: Awaited<ReturnType<InteractionFixture["text"]>>,
  expected: boolean,
): void {
  assert.equal(outcome.handled, true);
  if (outcome.handled) assert.equal(outcome.accepted, expected);
}

async function flushAsyncWork(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

test.afterEach(() => clearTelegramInteractionRuntime());

test("public request resolves the current process-global runtime on every call", async () => {
  assert.deepEqual(await requestTelegramInteraction(TEXT_REQUEST), {
    handled: false,
    reason: "runtime-unavailable",
  });

  const fixture = new InteractionFixture();
  const pending = requestTelegramInteraction(TEXT_REQUEST);
  await fixture.waitForSend();
  await fixture.text("Proceed");
  assert.deepEqual(
    await pending,
    answered([{ type: "text", label: "Proceed", value: "Proceed" }]),
  );
});

test("a bound runtime without an active Telegram turn does not claim a surface", async () => {
  new InteractionFixture({ active: false });
  assert.deepEqual(await requestTelegramInteraction(TEXT_REQUEST), {
    handled: false,
    reason: "no-active-telegram-turn",
  });
});

test("request normalization enforces frozen UTF-16, mode, option, and timeout bounds", async () => {
  const invalidRequests: TelegramInteractionRequest[] = [
    { question: " ", mode: { kind: "text" } },
    { question: "q".repeat(4_001), mode: { kind: "text" } },
    { question: "q", details: "d".repeat(8_001), mode: { kind: "text" } },
    { question: "q", mode: { kind: "text", options: [] } as never },
    { question: "q", mode: { kind: "single-select", options: [] } },
    {
      question: "q",
      mode: {
        kind: "single-select",
        options: Array.from({ length: 21 }, () => ({ label: "option" })),
      },
    },
    {
      question: "q",
      mode: { kind: "single-select", options: [{ label: " " }] },
    },
    {
      question: "q",
      mode: {
        kind: "single-select",
        options: [{ label: "x".repeat(1_001) }],
      },
    },
    { question: "q", mode: { kind: "text" }, timeoutMs: 999 },
    { question: "q", mode: { kind: "text" }, timeoutMs: 3_600_001 },
    { question: "q", mode: { kind: "text" }, timeoutMs: 1_000.5 },
  ];

  for (const request of invalidRequests) {
    const fixture = new InteractionFixture();
    assert.deepEqual(await requestTelegramInteraction(request), {
      handled: true,
      result: { status: "unavailable" },
    });
    assert.equal(fixture.sends.length, 0);
    clearTelegramInteractionRuntime();
  }

  const fixture = new InteractionFixture();
  const pending = requestTelegramInteraction({
    question: `  ${"q".repeat(4_000)}  `,
    details: ` ${"d".repeat(8_000)} `,
    mode: {
      kind: "single-select",
      options: Array.from({ length: 20 }, (_, index) => ({
        label: ` ${String(index).padEnd(1_000, "l")} `,
        value: index === 0 ? "" : "v".repeat(1_000),
        description: ` ${"d".repeat(2_000)} `,
      })),
    },
    timeoutMs: 3_600_000,
  });
  await fixture.waitForSend();
  await fixture.callback(fixture.latestCallback("pick", 0));
  const result = await pending;
  assert.equal(result.handled, true);
  if (result.handled && result.result.status === "answered") {
    assert.equal(result.result.answers[0]?.type, "option");
    assert.equal(result.result.answers[0]?.value, "");
  }
});

test("default timeout is exactly fifteen minutes", async () => {
  const timers = new FixtureTimers();
  const fixture = new InteractionFixture({ timers });
  const pending = requestTelegramInteraction({
    question: "Wait?",
    mode: { kind: "text" },
  });
  await fixture.waitForSend();
  assert.equal(timers.scheduled[0]?.delayMs, 900_000);
  timers.fire(900_000);
  assert.deepEqual(await pending, {
    handled: true,
    result: { status: "timed-out" },
  });
});

test("one active turn permits one atomic claim", async () => {
  const fixture = new InteractionFixture();
  const first = requestTelegramInteraction(TEXT_REQUEST);
  await fixture.waitForSend();
  const competing = requestTelegramInteraction({
    question: "Competing question",
    mode: { kind: "text" },
  });
  assert.deepEqual(await competing, {
    handled: true,
    result: { status: "unavailable", message: "Another interaction is active." },
  });
  assert.equal(fixture.sends.length, 1);
  await fixture.text("First");
  assert.equal((await first).handled, true);
});

test("single-select renders no machine values and resolves from source index", async () => {
  const fixture = new InteractionFixture();
  const pending = requestTelegramInteraction({
    question: "Pick one",
    mode: {
      kind: "single-select",
      options: [
        { label: "Alpha", value: "machine-secret-a", description: "First" },
        { label: "Alpha", value: "machine-secret-b", description: "Second" },
      ],
    },
  });
  const send = await fixture.waitForSend();
  assert.equal(send.replyToMessageId, 50);
  assert.match(send.view.text, /Alpha/);
  assert.doesNotMatch(JSON.stringify(send.view), /machine-secret/);
  const callback = fixture.latestCallback("pick", 1);
  assert.match(
    callback,
    /^interact:[A-Za-z0-9_-]{16}:pick:[0-9a-z]+$/,
  );
  assert.ok(Buffer.byteLength(callback, "ascii") <= 40);
  const outcome = await fixture.callback(callback);
  assert.deepEqual(outcome, { handled: true, accepted: true, settled: true });
  assert.deepEqual(
    await pending,
    answered([
      {
        type: "option",
        index: 2,
        label: "Alpha",
        value: "machine-secret-b",
      },
    ]),
  );
  assert.deepEqual(await fixture.callback(callback), {
    handled: true,
    accepted: false,
    settled: false,
    message: "This interaction has expired.",
  });
});

test("callback grammar is exact, bounded, action-aware, and range checked", () => {
  assert.deepEqual(
    parseTelegramInteractionCallbackData("interact:AbCdEf0123_-xyZ9:pick:j"),
    { token: "AbCdEf0123_-xyZ9", action: "pick", index: 19 },
  );
  for (const invalid of [
    "interact:short:pick:0",
    "interact:AbCdEf0123_-xyZ9:pick",
    "interact:AbCdEf0123_-xyZ9:submit:0",
    "interact:AbCdEf0123_-xyZ9:toggle:-1",
    "interact:AbCdEf0123_-xyZ9:unknown",
    "interact:AbCdEf0123_-xyZ9:pick:000",
    `interact:AbCdEf0123_-xyZ9:pick:${"z".repeat(20)}`,
  ]) {
    assert.equal(parseTelegramInteractionCallbackData(invalid), undefined);
  }
});

test("replayed and wrong-anchor callbacks cannot resolve a claim", async () => {
  const fixture = new InteractionFixture();
  const pending = requestTelegramInteraction({
    question: "Pick many",
    mode: {
      kind: "multi-select",
      options: [{ label: "A" }, { label: "B" }],
    },
  });
  await fixture.waitForSend();
  const firstToken = fixture.latestCallback("toggle", 0);
  assert.deepEqual(
    await fixture.callback(firstToken.replace(":toggle:0", ":pick:0")),
    {
      handled: true,
      accepted: false,
      settled: false,
      message: "This interaction has expired.",
    },
  );
  assert.deepEqual(await fixture.callback(firstToken), {
    handled: true,
    accepted: true,
    settled: false,
  });
  await flushAsyncWork();
  assert.deepEqual(await fixture.callback(firstToken), {
    handled: true,
    accepted: false,
    settled: false,
    message: "This interaction has expired.",
  });
  const currentToken = fixture.latestCallback("submit");
  const wrongAnchor = await fixture.runtime.handleInput({
    kind: "callback",
    ...IDENTITY,
    callbackData: currentToken,
    messageId: 999,
  });
  assert.deepEqual(wrongAnchor, {
    handled: true,
    accepted: false,
    settled: false,
    message: "This interaction has expired.",
  });
  await fixture.callback(currentToken);
  assert.equal((await pending).handled, true);
});

test("multi-select rotates one-use tokens and returns selected options in source order", async () => {
  const fixture = new InteractionFixture();
  const pending = requestTelegramInteraction({
    question: "Pick many",
    mode: {
      kind: "multi-select",
      options: [
        { label: "A", value: "a" },
        { label: "B", value: "b" },
        { label: "C", value: "c" },
      ],
    },
  });
  await fixture.waitForSend();
  const emptySubmit = fixture.latestCallback("submit");
  assert.deepEqual(await fixture.callback(emptySubmit), {
    handled: true,
    accepted: false,
    settled: false,
    message: "Select at least one option before submitting.",
  });
  await flushAsyncWork();

  await fixture.callback(fixture.latestCallback("toggle", 2));
  await flushAsyncWork();
  await fixture.callback(fixture.latestCallback("toggle", 0));
  await flushAsyncWork();
  await fixture.callback(fixture.latestCallback("submit"));

  assert.deepEqual(
    await pending,
    answered([
      { type: "option", index: 1, label: "A", value: "a" },
      { type: "option", index: 3, label: "C", value: "c" },
    ]),
  );
  assert.match(fixture.edits[1]?.view.text ?? "", /\[x\] 3\. C/);
});

test("Other blocks old-anchor text until its edit publishes the final anchor", async () => {
  const fixture = new InteractionFixture({
    editMessageIds: [201, 202],
    holdUpdateEdit: true,
  });
  const pending = requestTelegramInteraction({
    question: "Pick or explain",
    mode: { kind: "single-select", options: [{ label: "Known" }] },
  });
  await fixture.waitForSend();
  await fixture.callback(fixture.latestCallback("other"));
  await flushAsyncWork();
  assert.equal(fixture.edits.length, 1);
  assert.deepEqual(await fixture.text("old anchor", { replyToMessageId: 101 }), {
    handled: true,
    accepted: false,
    settled: false,
  });
  fixture.releaseUpdateEdit();
  await flushAsyncWork();
  assert.deepEqual(await fixture.text("still old", { replyToMessageId: 101 }), {
    handled: true,
    accepted: false,
    settled: false,
  });
  assert.deepEqual(await fixture.text("  A different answer  "), {
    handled: true,
    accepted: true,
    settled: true,
  });
  assert.deepEqual(
    await pending,
    answered([
      {
        type: "other",
        label: "A different answer",
        value: "A different answer",
      },
    ]),
  );
});

test("text answers reject every wrong target and stale generation identity", async () => {
  const staleIdentities: Array<Partial<TelegramInteractionInputIdentity>> = [
    { target: { chatId: 43 } },
    { profile: "other" },
    { transportGeneration: "transport-stale" },
    { sessionGeneration: "session-stale" },
    { authorityGeneration: "owner-stale" },
  ];
  for (const identity of staleIdentities) {
    const fixture = new InteractionFixture();
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    assertAccepted(await fixture.text("denied", { identity }), false);
    assertAccepted(await fixture.text("accepted"), true);
    assert.equal((await pending).handled, true);
    clearTelegramInteractionRuntime();
  }
});

test("text answers require current reply anchor, freshness, and 48 KiB", async () => {
  const fixture = new InteractionFixture();
  const pending = requestTelegramInteraction(TEXT_REQUEST);
  await fixture.waitForSend();
  assertAccepted(await fixture.text("old", { messageId: 50 }), false);
  assertAccepted(
    await fixture.text("wrong anchor", { replyToMessageId: 50 }),
    false,
  );
  assertAccepted(await fixture.text(" "), false);
  assertAccepted(await fixture.text("x".repeat(48 * 1024 + 1)), false);
  assertAccepted(await fixture.text(" valid "), true);
  assert.deepEqual(
    await pending,
    answered([{ type: "text", label: "valid", value: "valid" }]),
  );
});

test("cancel, AbortSignal, timeout, and answer races settle once", async (t) => {
  await t.test("cancel callback", async () => {
    const fixture = new InteractionFixture();
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    await fixture.callback(fixture.latestCallback("cancel"));
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "cancelled" },
    });
    clearTelegramInteractionRuntime();
  });

  await t.test("answer wins later abort and timeout", async () => {
    const timers = new FixtureTimers();
    const controller = new AbortController();
    const fixture = new InteractionFixture({ timers });
    const pending = requestTelegramInteraction({
      ...TEXT_REQUEST,
      signal: controller.signal,
    });
    await fixture.waitForSend();
    await fixture.text("Answer");
    controller.abort();
    assert.equal(timers.scheduled[0]?.cleared, true);
    assert.deepEqual(
      await pending,
      answered([{ type: "text", label: "Answer", value: "Answer" }]),
    );
    clearTelegramInteractionRuntime();
  });

  await t.test("abort wins later answer", async () => {
    const controller = new AbortController();
    const fixture = new InteractionFixture();
    const pending = requestTelegramInteraction({
      ...TEXT_REQUEST,
      signal: controller.signal,
    });
    await fixture.waitForSend();
    controller.abort();
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "cancelled" },
    });
    assert.deepEqual(await fixture.text("late"), { handled: false });
    clearTelegramInteractionRuntime();
  });

  await t.test("timeout wins later answer", async () => {
    const timers = new FixtureTimers();
    const fixture = new InteractionFixture({ timers });
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    timers.fire(1_000);
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "timed-out" },
    });
    assert.deepEqual(await fixture.text("late"), { handled: false });
    clearTelegramInteractionRuntime();
  });
});

test("growing update races retain the returned handle for terminal cleanup", async () => {
  const controller = new AbortController();
  const fixture = new InteractionFixture({
    editMessageIds: [101, 202],
    holdUpdateEdit: true,
  });
  const pending = requestTelegramInteraction({
    question: "Pick many",
    mode: { kind: "multi-select", options: [{ label: "A" }] },
    signal: controller.signal,
  });
  await fixture.waitForSend();
  await fixture.callback(fixture.latestCallback("toggle", 0));
  await flushAsyncWork();
  assert.equal(fixture.edits.length, 1);
  controller.abort();
  assert.deepEqual(await pending, {
    handled: true,
    result: { status: "cancelled" },
  });
  fixture.releaseUpdateEdit();
  await new Promise<void>((resolve) => setImmediate(resolve));
  await flushAsyncWork();
  assert.equal(fixture.edits.length, 2);
  assert.deepEqual(fixture.edits[1]?.handle.messageIds, [101, 202]);
  assert.deepEqual(fixture.edits[1]?.view.replyMarkup, { inline_keyboard: [] });
});

test("commit-unknown update issues no follow-up mutation", async () => {
  const partial: TelegramDeliveryHandle = {
    target: TARGET,
    messageIds: [101, 202],
    generation: "delivery-1",
  };
  const fixture = new InteractionFixture({
    updateEditResult: {
      ok: false,
      reason: "commit-unknown",
      message: "ambiguous edit",
      partial,
    },
  });
  const pending = requestTelegramInteraction({
    question: "Pick many",
    mode: { kind: "multi-select", options: [{ label: "A" }] },
  });
  await fixture.waitForSend();
  await fixture.callback(fixture.latestCallback("toggle", 0));
  assert.deepEqual(await pending, {
    handled: true,
    result: { status: "unavailable" },
  });
  await flushAsyncWork();
  assert.equal(fixture.edits.length, 1);
  assert.equal(fixture.deletes.length, 0);
});

test("stale generation and explicit authority invalidation settle handled unavailable", async (t) => {
  await t.test("stale authority observed by an answer", async () => {
    const fixture = new InteractionFixture();
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    fixture.authorityActive = false;
    assert.deepEqual(await fixture.text("denied"), {
      handled: true,
      accepted: false,
      settled: true,
    });
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "unavailable" },
    });
    clearTelegramInteractionRuntime();
  });

  await t.test("explicit invalidation", async () => {
    const fixture = new InteractionFixture();
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    fixture.runtime.invalidateAuthority(SNAPSHOT);
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "unavailable" },
    });
    clearTelegramInteractionRuntime();
  });
});

test("post-claim transport and commit-unknown failures never fall back", async (t) => {
  for (const reason of ["transport-failed", "commit-unknown"] as const) {
    await t.test(reason, async () => {
      const partial = {
        target: TARGET,
        messageIds: [101],
        generation: "delivery-1",
      };
      const fixture = new InteractionFixture({
        sendResult: {
          ok: false,
          reason,
          message: "redacted transport failure",
          partial,
        },
      });
      assert.deepEqual(await requestTelegramInteraction(TEXT_REQUEST), {
        handled: true,
        result: { status: "unavailable" },
      });
      await flushAsyncWork();
      assert.equal(fixture.sends.length, 1);
      assert.equal(fixture.edits.length, reason === "commit-unknown" ? 0 : 1);
      clearTelegramInteractionRuntime();
    });
  }
});

test("runtime replacement, unbind, and session lifecycle shutdown settle once", async (t) => {
  await t.test("replacement", async () => {
    const first = new InteractionFixture();
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await first.waitForSend();
    new InteractionFixture();
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "unavailable" },
    });
    clearTelegramInteractionRuntime();
  });

  await t.test("pending unbind settles exactly once as handled unavailable", async () => {
    const fixture = new InteractionFixture();
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    fixture.unbind();
    fixture.unbind();
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "unavailable" },
    });
    assert.deepEqual(await requestTelegramInteraction(TEXT_REQUEST), {
      handled: false,
      reason: "runtime-unavailable",
    });
  });

  await t.test("session shutdown", async () => {
    clearTelegramInteractionRuntime();
    // Use a lifecycle-owned runtime without the fixture's eager global binding.
    const timers = new FixtureTimers();
    const lifecycleRuntime = createTelegramInteractionRuntime({
      generation: "lifecycle-1",
      captureActiveTurn: () => SNAPSHOT,
      isActive: () => true,
      createToken: () => TOKEN_SEQUENCE[0],
      setTimer: timers.set,
      clearTimer: timers.clear,
      delivery: {
        async sendView() {
          return {
            ok: true,
            value: { target: TARGET, messageIds: [101], generation: "delivery-1" },
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
    const ownedLifecycle = createTelegramInteractionLifecycleHooks(
      () => lifecycleRuntime,
    );
    await ownedLifecycle.onSessionStart();
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await flushAsyncWork();
    await ownedLifecycle.onSessionShutdown();
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "unavailable" },
    });
  });
});

test("question rendering waits for the typing drain and answer resumes the same live activity", async () => {
  const typingRuntime = Runtime.createTelegramBridgeRuntime();
  const actions: string[] = [];
  let releaseFirst!: () => void;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  let sendCount = 0;
  typingRuntime.typing.start({
    chatId: 42,
    target: { chatId: 42, threadId: 9 },
    intervalMs: 60_000,
    async sendTypingAction() {
      sendCount += 1;
      actions.push(`direct:${sendCount}`);
      if (sendCount === 1) await firstGate;
    },
    async sendAggregateTypingAction() {
      actions.push("aggregate");
    },
  });
  await Promise.resolve();
  const fixture = new InteractionFixture({ waiting: typingRuntime.typing });
  const pending = requestTelegramInteraction(TEXT_REQUEST);

  await new Promise<void>((resolve) => setTimeout(resolve, 10));
  assert.equal(fixture.sends.length, 0);
  assert.equal(typingRuntime.typing.isWaiting(), true);
  releaseFirst();
  await fixture.waitForSend();
  assert.deepEqual(actions, ["direct:1"]);

  await fixture.text("Continue");
  assert.deepEqual(await pending, answered([
    { type: "text", label: "Continue", value: "Continue" },
  ]));
  await flushAsyncWork();
  assert.equal(typingRuntime.typing.isWaiting(), false);
  assert.deepEqual(actions, ["direct:1", "direct:2", "aggregate"]);
  typingRuntime.typing.stop();
});

test("non-answer terminal paths clear waiting without resuming activity", async (t) => {
  await t.test("tool abort", async () => {
    const typingRuntime = Runtime.createTelegramBridgeRuntime();
    const actions: string[] = [];
    typingRuntime.typing.start({
      chatId: 42,
      intervalMs: 60_000,
      async sendTypingAction() {
        actions.push("typing");
      },
    });
    await flushAsyncWork();
    const controller = new AbortController();
    const fixture = new InteractionFixture({ waiting: typingRuntime.typing });
    const pending = requestTelegramInteraction({
      ...TEXT_REQUEST,
      signal: controller.signal,
    });
    await fixture.waitForSend();
    controller.abort();
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "cancelled" },
    });
    await flushAsyncWork();
    assert.equal(typingRuntime.typing.isWaiting(), false);
    assert.deepEqual(actions, ["typing"]);
  });

  await t.test("timeout", async () => {
    const typingRuntime = Runtime.createTelegramBridgeRuntime();
    const timers = new FixtureTimers();
    const fixture = new InteractionFixture({
      waiting: typingRuntime.typing,
      timers,
    });
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    timers.fire(1_000);
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "timed-out" },
    });
    assert.equal(typingRuntime.typing.isWaiting(), false);
  });

  await t.test("authority invalidation and unbind", async () => {
    const typingRuntime = Runtime.createTelegramBridgeRuntime();
    const fixture = new InteractionFixture({ waiting: typingRuntime.typing });
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    fixture.runtime.invalidateAuthority();
    fixture.unbind();
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "unavailable" },
    });
    assert.equal(typingRuntime.typing.isWaiting(), false);
  });
});

test("waiting diagnostics contain metadata only", async () => {
  const typingRuntime = Runtime.createTelegramBridgeRuntime();
  const events: Array<Record<string, unknown>> = [];
  let nowMs = 100;
  const fixture = new InteractionFixture({
    waiting: typingRuntime.typing,
    createCorrelationId: () => "correlation-safe",
    getNowMs: () => nowMs,
    recordRuntimeEvent(_category, error, details) {
      events.push({ error, ...details });
    },
  });
  const pending = requestTelegramInteraction({
    question: "private question body",
    mode: {
      kind: "single-select",
      options: [{ label: "private option", value: "private value" }],
    },
  });
  await fixture.waitForSend();
  nowMs = 150;
  await fixture.callback(fixture.latestCallback("pick", 0));
  await pending;

  assert.ok(events.length >= 3);
  for (const event of events) {
    assert.deepEqual(
      Object.keys(event).sort(),
      [
        "correlationId",
        "durationMs",
        "error",
        "mode",
        "phase",
        "resultClass",
      ],
    );
  }
  const serialized = JSON.stringify(events);
  assert.doesNotMatch(serialized, /private question|private option|private value|42|101/);
  assert.match(serialized, /correlation-safe/);
});

test("interaction mutation failures emit only bounded metadata and diagnostics stay fail-soft", async (t) => {
  await t.test("update edit throw", async () => {
    const events: Array<Record<string, unknown>> = [];
    const fixture = new InteractionFixture({
      updateEditThrow: true,
      createCorrelationId: () => "correlation-update",
      recordRuntimeEvent(_category, error, details) {
        events.push({ error, ...details });
      },
    });
    const pending = requestTelegramInteraction({
      question: "private update question",
      mode: { kind: "multi-select", options: [{ label: "private update option" }] },
    });
    await fixture.waitForSend();
    await fixture.callback(fixture.latestCallback("toggle", 0));
    assert.deepEqual(await pending, {
      handled: true,
      result: { status: "unavailable" },
    });
    await flushAsyncWork();
    assert.ok(events.some((event) =>
      event.phase === "update-edit" && event.failureClass === "throw"));
    assert.doesNotMatch(
      JSON.stringify(events),
      /private update question|private update option|exception body|42|101/,
    );
  });

  await t.test("terminal edit failure and delete throw", async () => {
    const events: Array<Record<string, unknown>> = [];
    const fixture = new InteractionFixture({
      finalEditResult: {
        ok: false,
        reason: "transport-failed",
        message: "private terminal failure body 42 101",
      },
      deleteThrow: true,
      createCorrelationId: () => "correlation-terminal",
      recordRuntimeEvent(_category, error, details) {
        events.push({ error, ...details });
      },
    });
    const pending = requestTelegramInteraction({
      question: "private terminal question",
      mode: { kind: "text" },
    });
    await fixture.waitForSend();
    await fixture.text("private terminal answer");
    assert.equal((await pending).handled, true);
    await flushAsyncWork();
    assert.ok(events.some((event) =>
      event.phase === "terminal-edit" &&
      event.failureClass === "transport-failed"));
    assert.ok(events.some((event) =>
      event.phase === "terminal-delete" && event.failureClass === "throw"));
    assert.doesNotMatch(
      JSON.stringify(events),
      /private terminal question|private terminal answer|failure body|exception body|42|101/,
    );
  });

  await t.test("terminal timeout and throwing recorder", async () => {
    const timers = new FixtureTimers();
    const events: Array<Record<string, unknown>> = [];
    const fixture = new InteractionFixture({
      timers,
      holdFinalEdit: true,
      createCorrelationId: () => "correlation-timeout",
      recordRuntimeEvent(_category, error, details) {
        events.push({ error, ...details });
      },
    });
    const pending = requestTelegramInteraction(TEXT_REQUEST);
    await fixture.waitForSend();
    await fixture.text("answer");
    await pending;
    timers.fire(2_000);
    await flushAsyncWork();
    assert.ok(events.some((event) =>
      event.phase === "terminal-finalization" && event.failureClass === "timeout"));
    fixture.releaseFinalEdit();

    const failSoft = new InteractionFixture({
      recordRuntimeEvent() {
        throw new Error("diagnostics unavailable");
      },
    });
    const failSoftPending = requestTelegramInteraction(TEXT_REQUEST);
    await failSoft.waitForSend();
    failSoft.runtime.invalidateAuthority();
    assert.deepEqual(await failSoftPending, {
      handled: true,
      result: { status: "unavailable" },
    });
  });
});

test("abort while waiting acquisition drains releases the eventual lease", async () => {
  const controller = new AbortController();
  let finishAcquire!: (lease: { release(options: { resumeIfActive: boolean }): void }) => void;
  const acquired = new Promise<{ release(options: { resumeIfActive: boolean }): void }>(
    (resolve) => {
      finishAcquire = resolve;
    },
  );
  const releases: boolean[] = [];
  const fixture = new InteractionFixture({
    waiting: {
      acquireWaitingLease: async () => acquired,
      clearWaitingLease: () => false,
    },
  });
  const pending = requestTelegramInteraction({ ...TEXT_REQUEST, signal: controller.signal });
  controller.abort();
  assert.deepEqual(await pending, {
    handled: true,
    result: { status: "cancelled" },
  });
  finishAcquire({
    release(options) {
      releases.push(options.resumeIfActive);
    },
  });
  await flushAsyncWork();
  assert.deepEqual(releases, [false]);
  assert.equal(fixture.sends.length, 0);
});

test("expired terminal finalization releases Delivery so the next question can render", async () => {
  const timers = new FixtureTimers();
  let nextMessageId = 100;
  let sendCount = 0;
  let releaseTerminalEdit!: () => void;
  const terminalEditGate = new Promise<void>((resolve) => {
    releaseTerminalEdit = resolve;
  });
  let markTerminalStarted!: () => void;
  const terminalStarted = new Promise<void>((resolve) => {
    markTerminalStarted = resolve;
  });
  let markSecondSend!: () => void;
  const secondSend = new Promise<void>((resolve) => {
    markSecondSend = resolve;
  });
  const delivery = createTelegramDeliveryRuntime({
    generation: "delivery-1",
    getActiveTurnTarget: () => TARGET,
    getInstanceTarget: () => TARGET,
    getAggregateTarget: () => undefined,
    isExplicitTargetAuthorized: (target) => target.chatId === TARGET.chatId,
    renderView: (view) => [{ text: view.text, parseMode: "plain" }],
    async sendChunk() {
      sendCount += 1;
      if (sendCount === 2) markSecondSend();
      return ++nextMessageId;
    },
    async editChunk(_target, _messageId, chunk) {
      if (chunk.text.endsWith("Answered.")) {
        markTerminalStarted();
        await terminalEditGate;
      }
    },
    async deleteMessage() {},
    async sendChatAction() {},
  });
  const runtime = createTelegramInteractionRuntime({
    generation: "interaction-queue-release",
    captureActiveTurn: () => SNAPSHOT,
    isActive: (snapshot) => snapshot === SNAPSHOT,
    createToken: () => TOKEN_SEQUENCE[0],
    setTimer: timers.set,
    clearTimer: timers.clear,
    finalizationTimeoutMs: 2_000,
    delivery,
  });
  bindTelegramInteractionRuntime(runtime);

  const first = requestTelegramInteraction(TEXT_REQUEST);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(sendCount, 1);
  await runtime.handleInput({
    kind: "text",
    ...IDENTITY,
    text: "First answer",
    messageId: 102,
    replyToMessageId: 101,
  });
  assert.equal((await first).handled, true);
  await terminalStarted;

  timers.fire(2_000);
  const second = requestTelegramInteraction(TEXT_REQUEST);
  await Promise.race([
    secondSend,
    new Promise<never>((_resolve, reject) =>
      setTimeout(() => reject(new Error("next interaction stayed blocked")), 250),
    ),
  ]);
  assert.equal(sendCount, 2);

  runtime.invalidateAuthority();
  assert.deepEqual(await second, {
    handled: true,
    result: { status: "unavailable" },
  });
  releaseTerminalEdit();
  await flushAsyncWork();
  runtime.shutdown();
  delivery.shutdown();
});

test("answered result settles before bounded best-effort UI finalization", async () => {
  const fixture = new InteractionFixture({ holdFinalEdit: true });
  const pending = requestTelegramInteraction(TEXT_REQUEST);
  await fixture.waitForSend();
  const inputOutcome = await fixture.text("Fast");
  assert.deepEqual(inputOutcome, {
    handled: true,
    accepted: true,
    settled: true,
  });
  assert.deepEqual(
    await pending,
    answered([{ type: "text", label: "Fast", value: "Fast" }]),
  );
  assert.equal(fixture.edits.length, 1);
  assert.deepEqual(fixture.edits[0]?.view.replyMarkup, { inline_keyboard: [] });
  fixture.releaseFinalEdit();
});
