/**
 * Regression tests for the Telegram updates domain
 * Covers extraction, authorization, flow classification, execution planning, and runtime execution in one suite
 */

import assert from "node:assert/strict";
import test from "node:test";

import {
  buildTelegramUpdateExecutionPlan,
  buildTelegramUpdateExecutionPlanFromUpdate,
  buildTelegramUpdateFlowAction,
  collectTelegramReactionEmojis,
  createTelegramPairedUpdateRuntime,
  createTelegramUpdateHandle,
  createTelegramUpdateRuntime,
  executeTelegramUpdate,
  executeTelegramUpdatePlan,
  extractDeletedTelegramMessageIds,
  getAuthorizedTelegramCallbackQuery,
  getAuthorizedTelegramEditedMessage,
  getAuthorizedTelegramGuestMessage,
  getAuthorizedTelegramMessage,
  getTelegramMessageTarget,
  getTelegramTopicLifecycleUpdate,
  getTelegramUpdateHandlerRegistry,
  handleAuthorizedTelegramReactionUpdate,
  normalizeTelegramReactionEmoji,
  planTelegramDurableInboundResponsibility,
  registerTelegramUpdateHandler,
  TELEGRAM_UNTARGETED_RECOVERY_CHAT_ID,
  TELEGRAM_PRIORITY_REACTION_EMOJIS,
  TELEGRAM_PRIORITY_REACTIONS,
  TELEGRAM_REMOVAL_REACTION_EMOJIS,
  TELEGRAM_REMOVAL_REACTIONS,
  type TelegramInboundHandlingOutcome,
  type TelegramUpdateHandler,
  type TelegramUpdateHandlerRegistry,
} from "../lib/updates.ts";

const TEST_CONTEXT = "ctx";

type CompletedReason = Extract<
  TelegramInboundHandlingOutcome,
  { kind: "completed" }
>["reason"];

function completed(reason: CompletedReason): TelegramInboundHandlingOutcome {
  return { kind: "completed", reason };
}

function promptMaterialized(): TelegramInboundHandlingOutcome {
  return { kind: "prompt-materialized", turnId: "test-turn", recordIds: [] };
}

function operatorRerouteDeferred(): TelegramInboundHandlingOutcome {
  return {
    kind: "deferred",
    reason: "operator-reroute",
    key: "test-unbound-topic",
  };
}

const REGISTRY_KEY = "__piTelegramUpdateHandlerRegistry__";

function clearGlobalRegistry(): void {
  delete (globalThis as Record<string, unknown>)[REGISTRY_KEY];
}

function createPairedGate() {
  return {
    getAllowedUserId: () => 7,
    claim: async () => ({ kind: "rejected" as const }),
    sendGenericResponse: async () => {},
    onPaired: () => {},
    recordSideEffectFailure: () => {},
  };
}

function getGlobalRegistry(): TelegramUpdateHandlerRegistry | undefined {
  return (globalThis as Record<string, unknown>)[REGISTRY_KEY] as
    | TelegramUpdateHandlerRegistry
    | undefined;
}

test("Update helpers normalize emoji reactions and collect emoji-only entries", () => {
  assert.equal(normalizeTelegramReactionEmoji("👍️"), "👍");
  const emojis = collectTelegramReactionEmojis([
    { type: "emoji", emoji: "👍️" },
    { type: "emoji", emoji: "👎" },
    { type: "custom_emoji" },
  ]);
  assert.deepEqual([...emojis], ["👍", "👎"]);
  assert.deepEqual(
    TELEGRAM_PRIORITY_REACTIONS.map((reaction) => [
      reaction.id,
      reaction.name,
      reaction.emoji,
    ]),
    [
      [10, "like", "👍"],
      [11, "lightning", "⚡"],
      [12, "heart", "❤"],
      [13, "dove", "🕊"],
      [14, "fire", "🔥"],
    ],
  );
  assert.deepEqual(
    TELEGRAM_REMOVAL_REACTIONS.map((reaction) => reaction.id),
    [20, 21, 22, 23, 24],
  );
  assert.deepEqual(TELEGRAM_PRIORITY_REACTION_EMOJIS, [
    "👍",
    "⚡",
    "❤",
    "🕊",
    "🔥",
  ]);
  assert.deepEqual(TELEGRAM_REMOVAL_REACTION_EMOJIS, [
    "👎",
    "👻",
    "💔",
    "💩",
    "🗑",
  ]);
});

test("Update helpers extract topic lifecycle service messages", () => {
  assert.deepEqual(
    getTelegramTopicLifecycleUpdate({
      chat: { id: 7, type: "private" },
      message_id: 1,
      message_thread_id: 42,
      forum_topic_closed: {},
    }),
    {
      kind: "closed",
      message: {
        chat: { id: 7, type: "private" },
        message_id: 1,
        message_thread_id: 42,
        forum_topic_closed: {},
      },
      target: { chatId: 7, threadId: 42 },
    },
  );
  assert.equal(
    getTelegramTopicLifecycleUpdate({
      chat: { id: 7, type: "private" },
      message_id: 1,
    }),
    undefined,
  );
});

test("Update helpers extract private and thread targets from messages", () => {
  assert.deepEqual(
    getTelegramMessageTarget({
      chat: { id: 7, type: "private" },
      message_id: 1,
    }),
    { chatId: 7 },
  );
  assert.deepEqual(
    getTelegramMessageTarget({
      chat: { id: -1007, type: "supergroup" },
      message_id: 1,
      message_thread_id: 42,
    }),
    { chatId: -1007, threadId: 42 },
  );
  assert.equal(
    getTelegramMessageTarget({ chat: { type: "private" }, message_id: 1 }),
    undefined,
  );
});

test("Durable responsibility accepts human callbacks on bot-authored messages and terminals targetless updates", () => {
  assert.deepEqual(
    planTelegramDurableInboundResponsibility(
      {
        callback_query: {
          id: "callback-1",
          from: { id: 7, is_bot: false },
          message: {
            chat: { id: 7, type: "private" },
            from: { id: 99, is_bot: true },
            message_id: 10,
          },
        },
      },
      { allowedUserId: 7 },
    ),
    { kind: "local", target: { chatId: 7 } },
  );
  assert.deepEqual(
    planTelegramDurableInboundResponsibility(
      {},
      {},
    ),
    {
      kind: "terminal",
      target: { chatId: TELEGRAM_UNTARGETED_RECOVERY_CHAT_ID },
      pairingProof: false,
      reason: "unsupported",
    },
  );
});

test("Update helpers extract deleted business-message ids only from Bot API shapes", () => {
  assert.deepEqual(
    extractDeletedTelegramMessageIds({
      deleted_business_messages: { message_ids: [1, 2] },
    }),
    [1, 2],
  );
  assert.deepEqual(
    extractDeletedTelegramMessageIds({
      deleted_business_messages: { message_ids: [3, "bad"] },
    }),
    [],
  );
  assert.deepEqual(extractDeletedTelegramMessageIds({}), []);
});

test("Paired update runtime preserves configured-owner routing", async () => {
  const events: string[] = [];
  const runtime = createTelegramPairedUpdateRuntime({
    getAllowedUserId: () => 42,
    removePendingMediaGroupMessages: () => {},
    removeQueuedTelegramTurnsByMessageIds: () => 0,
    clearQueuedTelegramTurnPriorityByMessageId: () => false,
    prioritizeQueuedTelegramTurnByMessageId: () => false,
    answerCallbackQuery: async () => {},
    answerGuestQuery: async () => {},
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => undefined,
    handleAuthorizedTelegramMessage: async (message, ctx: string) => {
      events.push(`message:${ctx}:${message.message_id ?? "none"}`);
      return promptMaterialized();
    },
    handleAuthorizedTelegramEditedMessage: () => completed("ignored"),
  });
  await runtime.handleUpdate(
    {
      message: {
        chat: { id: 1, type: "private" },
        from: { id: 42, is_bot: false },
        message_id: 10,
      },
    },
    "ctx",
  );
  assert.deepEqual(events, ["message:ctx:10"]);
});

test("Paired update runtime preserves follower target ownership forwarding", async () => {
  const events: string[] = [];
  const runtime = createTelegramPairedUpdateRuntime({
    getAllowedUserId: () => 7,
    getCurrentInstanceId: () => "leader",
    getTargetOwnership: () => ({ instanceId: "follower" }),
    foreignOwnedUpdateForwarder: {
      forwardMessage: async () => {
        events.push("forward:follower");
        return true;
      },
    },
    removePendingMediaGroupMessages: () => {},
    removeQueuedTelegramTurnsByMessageIds: () => 0,
    clearQueuedTelegramTurnPriorityByMessageId: () => false,
    prioritizeQueuedTelegramTurnByMessageId: () => false,
    answerCallbackQuery: async () => {},
    answerGuestQuery: async () => {},
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => undefined,
    handleAuthorizedTelegramMessage: async () => {
      events.push("message");
      return promptMaterialized();
    },
    handleAuthorizedTelegramEditedMessage: () => completed("ignored"),
  });
  await runtime.handleUpdate(
    {
      message: {
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        message_id: 11,
        message_thread_id: 42,
      },
    },
    TEST_CONTEXT,
  );
  assert.deepEqual(events, ["forward:follower"]);
});

test("Paired update runtime preserves topic lifecycle handling", async () => {
  const events: string[] = [];
  const runtime = createTelegramPairedUpdateRuntime({
    getAllowedUserId: () => 7,
    removePendingMediaGroupMessages: () => {},
    removeQueuedTelegramTurnsByMessageIds: () => 0,
    clearQueuedTelegramTurnPriorityByMessageId: () => false,
    prioritizeQueuedTelegramTurnByMessageId: () => false,
    answerCallbackQuery: async () => {},
    answerGuestQuery: async () => {},
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => undefined,
    handleAuthorizedTelegramMessage: async () => promptMaterialized(),
    handleAuthorizedTelegramEditedMessage: () => completed("ignored"),
    handleTelegramTopicLifecycleUpdate: async (lifecycle) => {
      events.push(`lifecycle:${lifecycle.kind}:${lifecycle.target.threadId}`);
      return completed("topic-lifecycle");
    },
  });

  await runtime.handleUpdate(
    {
      message: {
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        message_id: 12,
        message_thread_id: 43,
        forum_topic_created: {},
      },
    },
    TEST_CONTEXT,
  );

  assert.deepEqual(events, ["lifecycle:created:43"]);
});

test("Update routing extracts private and authorized group human callback queries", () => {
  assert.equal(
    getAuthorizedTelegramCallbackQuery({
      callback_query: {
        from: { id: 1, is_bot: true },
        message: { chat: { type: "private" } },
      },
    }),
    undefined,
  );
  assert.equal(
    getAuthorizedTelegramCallbackQuery(
      {
        callback_query: {
          from: { id: 1, is_bot: false },
          message: { chat: { type: "supergroup" } },
        },
      },
      7,
    ),
    undefined,
  );
  const query = getAuthorizedTelegramCallbackQuery({
    callback_query: {
      from: { id: 1, is_bot: false },
      message: { chat: { type: "private" } },
    },
  });
  assert.ok(query);
  assert.ok(
    getAuthorizedTelegramCallbackQuery(
      {
        callback_query: {
          from: { id: 7, is_bot: false },
          message: { chat: { type: "supergroup" } },
        },
      },
      7,
    ),
  );
});

test("Update routing extracts private human messages and edited messages separately", () => {
  assert.equal(
    getAuthorizedTelegramMessage({
      message: {
        chat: { type: "group" },
        from: { id: 1, is_bot: false },
      },
    }),
    undefined,
  );
  assert.ok(
    getAuthorizedTelegramMessage(
      {
        message: {
          chat: { type: "supergroup" },
          from: { id: 7, is_bot: false },
        },
      },
      7,
    ),
  );
  assert.ok(
    getAuthorizedTelegramEditedMessage(
      {
        edited_message: {
          chat: { type: "supergroup" },
          from: { id: 7, is_bot: false },
        },
      },
      7,
    ),
  );
  const directMessage = getAuthorizedTelegramMessage({
    message: {
      chat: { type: "private" },
      from: { id: 1, is_bot: false },
    },
  });
  assert.ok(directMessage);
  const editedMessage = getAuthorizedTelegramEditedMessage({
    edited_message: {
      chat: { type: "private" },
      from: { id: 1, is_bot: false },
    },
  });
  assert.ok(editedMessage);
});

test("Update routing extracts guest messages without private chat filter", () => {
  assert.equal(
    getAuthorizedTelegramGuestMessage({
      guest_message: {
        guest_query_id: "gq-1",
        chat: { type: "supergroup" },
        from: { id: 1, is_bot: true },
      },
    }),
    undefined,
  );
  const guestMessage = getAuthorizedTelegramGuestMessage({
    guest_message: {
      guest_query_id: "gq-1",
      chat: { type: "supergroup" },
      from: { id: 1, is_bot: false },
    },
  });
  assert.ok(guestMessage);
  assert.equal(guestMessage.guest_query_id, "gq-1");
});

test("Update flow prioritizes deleted business-message handling over other update kinds", () => {
  const action = buildTelegramUpdateFlowAction(
    {
      deleted_business_messages: { message_ids: [1, 2] },
      message_reaction: {
        chat: { type: "private" },
        user: { id: 1, is_bot: false },
        message_id: 1,
        old_reaction: [],
        new_reaction: [],
      },
    },
    1,
  );
  assert.deepEqual(action, { kind: "deleted", messageIds: [1, 2], scope: {} });
});

test("Update flow detects topic lifecycle before prompt routing", () => {
  const action = buildTelegramUpdateFlowAction(
    {
      message: {
        chat: { id: 7, type: "private" },
        message_id: 1,
        message_thread_id: 42,
        forum_topic_reopened: {},
      },
    },
    7,
  );
  assert.equal(action.kind, "topic-lifecycle");
  assert.equal(
    action.kind === "topic-lifecycle" ? action.lifecycle.kind : undefined,
    "reopened",
  );
  assert.deepEqual(
    action.kind === "topic-lifecycle" ? action.lifecycle.target : undefined,
    { chatId: 7, threadId: 42 },
  );
});

test("Update flow returns authorized callback, message, and edit actions", () => {
  const callbackAction = buildTelegramUpdateFlowAction(
    {
      callback_query: {
        from: { id: 7, is_bot: false },
        message: { chat: { type: "private" } },
      },
    },
    7,
  );
  assert.equal(callbackAction.kind, "callback");
  assert.deepEqual(
    callbackAction.kind === "callback"
      ? callbackAction.authorization
      : undefined,
    { kind: "allow" },
  );
  const messageAction = buildTelegramUpdateFlowAction({
    message: {
      chat: { type: "private" },
      from: { id: 9, is_bot: false },
    },
  });
  assert.equal(messageAction.kind, "message");
  assert.deepEqual(
    messageAction.kind === "message" ? messageAction.authorization : undefined,
    { kind: "deny" },
  );
  const editAction = buildTelegramUpdateFlowAction(
    {
      edited_message: {
        chat: { type: "private" },
        from: { id: 9, is_bot: false },
      },
    },
    9,
  );
  assert.equal(editAction.kind, "edited-message");
});

test("Update flow classifies guest messages with authorization", () => {
  const guestAction = buildTelegramUpdateFlowAction(
    {
      guest_message: {
        guest_query_id: "gq-1",
        chat: { type: "supergroup" },
        from: { id: 5, is_bot: false },
      },
    },
    5,
  );
  assert.equal(guestAction.kind, "guest");
  assert.deepEqual(
    guestAction.kind === "guest" ? guestAction.authorization : undefined,
    { kind: "allow" },
  );
  const guestDeny = buildTelegramUpdateFlowAction(
    {
      guest_message: {
        guest_query_id: "gq-2",
        chat: { type: "supergroup" },
        from: { id: 6, is_bot: false },
      },
    },
    5,
  );
  assert.equal(guestDeny.kind, "guest");
  assert.deepEqual(
    guestDeny.kind === "guest" ? guestDeny.authorization : undefined,
    { kind: "deny" },
  );
});

test("Update flow ignores unauthorized transport shapes and preserves reaction events", () => {
  const reactionAction = buildTelegramUpdateFlowAction({
    message_reaction: {
      chat: { type: "private" },
      user: { id: 1, is_bot: false },
      message_id: 1,
      old_reaction: [],
      new_reaction: [],
    },
  });
  assert.equal(reactionAction.kind, "reaction");
  const ignored = buildTelegramUpdateFlowAction({
    callback_query: {
      from: { id: 1, is_bot: true },
      message: { chat: { type: "private" } },
    },
  });
  assert.deepEqual(ignored, { kind: "ignore" });
});

test("Update execution plan maps callback and message authorization to side-effect flags", () => {
  const callbackPlan = buildTelegramUpdateExecutionPlan({
    kind: "callback",
    query: {
      from: { id: 1, is_bot: false },
      message: { chat: { type: "private" } },
    },
    authorization: { kind: "deny" },
  });
  assert.deepEqual(callbackPlan, {
    kind: "callback",
    query: {
      from: { id: 1, is_bot: false },
      message: { chat: { type: "private" } },
    },
    shouldDeny: true,
  });
  const messagePlan = buildTelegramUpdateExecutionPlan({
    kind: "message",
    message: {
      chat: { type: "private" },
      from: { id: 2, is_bot: false },
    },
    authorization: { kind: "deny" },
  });
  assert.equal(messagePlan.kind, "message");
  assert.equal(messagePlan.shouldDeny, true);
});

test("Update execution plan preserves deleted and reaction actions", () => {
  assert.deepEqual(
    buildTelegramUpdateExecutionPlan({ kind: "deleted", messageIds: [1, 2], scope: {} }),
    { kind: "deleted", messageIds: [1, 2], scope: {} },
  );
  const reactionUpdate = {
    chat: { type: "private" },
    user: { id: 1, is_bot: false },
    message_id: 1,
    old_reaction: [],
    new_reaction: [],
  };
  assert.deepEqual(
    buildTelegramUpdateExecutionPlan({
      kind: "reaction",
      reactionUpdate,
    }),
    { kind: "reaction", reactionUpdate },
  );
});

test("Update execution plan maps guest authorization to deny flag", () => {
  const guestMessage = {
    guest_query_id: "gq-1",
    chat: { type: "supergroup" },
    from: { id: 1, is_bot: false },
  };
  const guestPlan = buildTelegramUpdateExecutionPlan({
    kind: "guest",
    guestMessage,
    authorization: { kind: "allow" },
  });
  assert.deepEqual(guestPlan, {
    kind: "guest",
    guestMessage,
    shouldDeny: false,
  });
  const unpairedGuestPlan = buildTelegramUpdateExecutionPlan({
    kind: "guest",
    guestMessage,
    authorization: { kind: "deny" },
  });
  assert.deepEqual(unpairedGuestPlan, {
    kind: "guest",
    guestMessage,
    shouldDeny: true,
  });
});

test("Update execution plan can be built directly from updates", () => {
  const plan = buildTelegramUpdateExecutionPlanFromUpdate(
    {
      callback_query: {
        from: { id: 4, is_bot: false },
        message: { chat: { type: "private" } },
      },
    },
    5,
  );
  assert.equal(plan.kind, "callback");
  assert.equal(plan.kind === "callback" ? plan.shouldDeny : false, true);
});

test("Update runtime controller binds update and reaction ports", async () => {
  const events: string[] = [];
  const runtime = createTelegramUpdateRuntime({
    getAllowedUserId: () => 42,
    removePendingMediaGroupMessages: (messageIds) => {
      events.push(`media:${messageIds.join(",")}`);
    },
    removeQueuedTelegramTurnsByMessageIds: (messageIds, ctx: string) => {
      events.push(`remove:${ctx}:${messageIds.join(",")}`);
      return messageIds.length;
    },
    clearQueuedTelegramTurnPriorityByMessageId: (messageId, ctx: string) => {
      events.push(`clear:${ctx}:${messageId}`);
      return true;
    },
    prioritizeQueuedTelegramTurnByMessageId: (messageId, ctx: string) => {
      events.push(`priority:${ctx}:${messageId}`);
      return true;
    },
    answerCallbackQuery: async (id, text) => {
      events.push(`answer:${id}:${text ?? ""}`);
    },
    answerGuestQuery: async (id, text) => {
      events.push(`guest-answer:${id}:${text ?? ""}`);
    },
    handleAuthorizedTelegramCallbackQuery: async () => {
      events.push("callback");
      return completed("callback");
    },
    sendTextReply: async (chatId, replyToMessageId, text) => {
      events.push(`reply:${chatId}:${replyToMessageId}:${text}`);
      return 1;
    },
    handleAuthorizedTelegramMessage: async (message, ctx: string) => {
      events.push(`message:${ctx}:${message.message_id ?? "none"}`);
      return promptMaterialized();
    },
    handleAuthorizedTelegramEditedMessage: async (message, ctx: string) => {
      events.push(`edit:${ctx}:${message.message_id ?? "none"}`);
      return completed("ignored");
    },
  });
  await runtime.handleAuthorizedReactionUpdate(
    {
      chat: { type: "private" },
      message_id: 9,
      user: { id: 42, is_bot: false },
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👍" }],
    },
    "ctx",
  );
  await runtime.handleUpdate(
    {
      message: {
        chat: { id: 1, type: "private" },
        from: { id: 42, is_bot: false },
        message_id: 10,
      },
    },
    "ctx",
  );
  assert.deepEqual(events, ["priority:ctx:9", "message:ctx:10"]);
});

test("Update runtime routes guest messages through guest handler", async () => {
  const events: string[] = [];
  const runtime = createTelegramUpdateRuntime({
    getAllowedUserId: () => 42,
    removePendingMediaGroupMessages: () => {},
    removeQueuedTelegramTurnsByMessageIds: () => 0,
    clearQueuedTelegramTurnPriorityByMessageId: () => true,
    prioritizeQueuedTelegramTurnByMessageId: () => true,
    answerCallbackQuery: async () => {},
    answerGuestQuery: async () => {},
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => 1,
    handleAuthorizedTelegramMessage: async () => promptMaterialized(),
    handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    handleAuthorizedTelegramGuestMessage: async (
      guestMessage,
      _ctx: string,
    ) => {
      events.push(`guest:${guestMessage.guest_query_id}`);
      return completed("guest");
    },
  });
  await runtime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "gq-1",
        chat: { type: "supergroup" },
        from: { id: 42, is_bot: false },
      },
    },
    "ctx",
  );
  assert.deepEqual(events, ["guest:gq-1"]);
});

test("Update runtime denies guest messages before pairing", async () => {
  const events: string[] = [];
  const runtime = createTelegramUpdateRuntime({
    getAllowedUserId: () => undefined,
    removePendingMediaGroupMessages: () => {},
    removeQueuedTelegramTurnsByMessageIds: () => 0,
    clearQueuedTelegramTurnPriorityByMessageId: () => true,
    prioritizeQueuedTelegramTurnByMessageId: () => true,
    answerCallbackQuery: async () => {},
    answerGuestQuery: async (id, text) => {
      events.push(`guest-deny:${id}:${text ?? ""}`);
    },
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => 1,
    handleAuthorizedTelegramMessage: async () => promptMaterialized(),
    handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    handleAuthorizedTelegramGuestMessage: async () => {
      events.push("guest-handled");
      return completed("guest");
    },
  });
  await runtime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "gq-unpaired",
        chat: { type: "supergroup" },
        from: { id: 42, is_bot: false },
      },
    },
    "ctx",
  );
  assert.deepEqual(events, ["guest-deny:gq-unpaired:🚫 Access denied."]);
});

test("Update runtime answers guest query with access denied for unauthorized users", async () => {
  const events: string[] = [];
  const runtime = createTelegramUpdateRuntime({
    getAllowedUserId: () => 42,
    removePendingMediaGroupMessages: () => {},
    removeQueuedTelegramTurnsByMessageIds: () => 0,
    clearQueuedTelegramTurnPriorityByMessageId: () => true,
    prioritizeQueuedTelegramTurnByMessageId: () => true,
    answerCallbackQuery: async () => {},
    answerGuestQuery: async (id, text) => {
      events.push(`guest-deny:${id}:${text ?? ""}`);
    },
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => 1,
    handleAuthorizedTelegramMessage: async () => promptMaterialized(),
    handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    handleAuthorizedTelegramGuestMessage: async () => {
      events.push("guest-handled");
      return completed("guest");
    },
  });
  await runtime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "gq-deny",
        chat: { type: "supergroup" },
        from: { id: 99, is_bot: false },
      },
    },
    "ctx",
  );
  assert.deepEqual(events, ["guest-deny:gq-deny:🚫 Access denied."]);
});

test("Update runtime handles authorized reaction priority and removal effects", async () => {
  const events: string[] = [];
  const deps = {
    allowedUserId: 7,
    ctx: TEST_CONTEXT,
    removePendingMediaGroupMessages: (ids: number[]) => {
      events.push(`media:${ids.join(",")}`);
    },
    removeQueuedTelegramTurnsByMessageIds: (ids: number[]) => {
      events.push(`remove:${ids.join(",")}`);
      return ids.length;
    },
    clearQueuedTelegramTurnPriorityByMessageId: (id: number) => {
      events.push(`clear:${id}`);
      return true;
    },
    prioritizeQueuedTelegramTurnByMessageId: (
      id: number,
      _ctx: string,
      emoji?: string,
    ) => {
      events.push(`prioritize:${id}:${emoji}`);
      return true;
    },
  };
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 10,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👍️" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 11,
      old_reaction: [{ type: "emoji", emoji: "👍" }],
      new_reaction: [],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 12,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👎" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 13,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "⚡" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 14,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "❤️" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 15,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "🕊️" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 16,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "🔥" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 17,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👻" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 18,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "💔" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 19,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "💩" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 20,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "🗑️" }],
    },
    deps,
  );
  assert.deepEqual(events, [
    "prioritize:10:👍",
    "clear:11",
    "media:12",
    "remove:12",
    "prioritize:13:⚡",
    "prioritize:14:❤",
    "prioritize:15:🕊",
    "prioritize:16:🔥",
    "media:17",
    "remove:17",
    "media:18",
    "remove:18",
    "media:19",
    "remove:19",
    "media:20",
    "remove:20",
  ]);
});

test("Update runtime handles authorized group reactions and ignores other users", async () => {
  const events: string[] = [];
  const deps = {
    allowedUserId: 7,
    ctx: TEST_CONTEXT,
    removePendingMediaGroupMessages: (ids: number[]) => {
      events.push(`media:${ids.join(",")}`);
    },
    removeQueuedTelegramTurnsByMessageIds: (
      ids: number[],
      _ctx: string,
      scope?: { chatId?: number },
    ) => {
      events.push(`remove:${ids.join(",")}:${scope?.chatId ?? "none"}`);
      return ids.length;
    },
    clearQueuedTelegramTurnPriorityByMessageId: () => false,
    prioritizeQueuedTelegramTurnByMessageId: () => false,
  };

  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { id: -1001, type: "supergroup" },
      user: { id: 1, is_bot: false },
      message_id: 30,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👎" }],
    },
    deps,
  );
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { id: -1001, type: "supergroup" },
      user: { id: 7, is_bot: false },
      message_id: 31,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👎" }],
    },
    deps,
  );

  assert.deepEqual(events, ["media:31", "remove:31:-1001"]);
});

test("Update runtime ignores reactions owned by another instance", async () => {
  const events: string[] = [];
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { id: 7, type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 10,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👍" }],
    },
    {
      allowedUserId: 7,
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "instance-a",
      getMessageOwnership: () => ({ instanceId: "instance-b" }),
      removePendingMediaGroupMessages: () => {
        events.push("media");
      },
      removeQueuedTelegramTurnsByMessageIds: () => {
        events.push("remove");
        return 0;
      },
      clearQueuedTelegramTurnPriorityByMessageId: () => {
        events.push("clear");
        return false;
      },
      prioritizeQueuedTelegramTurnByMessageId: () => {
        events.push("prioritize");
        return false;
      },
    },
  );
  assert.deepEqual(events, []);
});

test("Update runtime forwards reactions owned by another instance", async () => {
  const events: string[] = [];
  await handleAuthorizedTelegramReactionUpdate(
    {
      chat: { id: 7, type: "private" },
      user: { id: 7, is_bot: false },
      message_id: 10,
      old_reaction: [],
      new_reaction: [{ type: "emoji", emoji: "👍" }],
    },
    {
      allowedUserId: 7,
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "instance-a",
      getMessageOwnership: () => ({ instanceId: "instance-b" }),
      foreignOwnedUpdateForwarder: {
        forwardReaction: ({ ownership, ctx }) => {
          events.push(`forward:${ownership.instanceId}:${ctx}`);
          return true;
        },
      },
      removePendingMediaGroupMessages: () => {
        events.push("media");
      },
      removeQueuedTelegramTurnsByMessageIds: () => {
        events.push("remove");
        return 0;
      },
      clearQueuedTelegramTurnPriorityByMessageId: () => {
        events.push("clear");
        return false;
      },
      prioritizeQueuedTelegramTurnByMessageId: () => {
        events.push("prioritize");
        return false;
      },
    },
  );
  assert.deepEqual(events, ["forward:instance-b:ctx"]);
});

test("Update runtime records forwarded message ownership for later reactions", async () => {
  const events: string[] = [];
  const ownership = new Map<string, { instanceId: string }>();
  const runtime = createTelegramUpdateRuntime({
    getAllowedUserId: () => 7,
    getCurrentInstanceId: () => "leader",
    getMessageOwnership: (chatId, messageId) =>
      ownership.get(`${chatId}:${messageId}`),
    getTargetOwnership: (target) =>
      target.chatId === 7 && target.threadId === 44
        ? { instanceId: "follower" }
        : undefined,
    recordMessageOwnership: (record) => {
      events.push(
        `record:${record.chatId}:${record.messageId}:${record.target?.threadId}:${record.instanceId}`,
      );
      ownership.set(`${record.chatId}:${record.messageId}`, {
        instanceId: record.instanceId,
      });
    },
    foreignOwnedUpdateForwarder: {
      forwardMessage: ({ ownership }) => {
        events.push(`forward-message:${ownership.instanceId}`);
        return true;
      },
      forwardReaction: ({ ownership }) => {
        events.push(`forward-reaction:${ownership.instanceId}`);
        return true;
      },
    },
    removePendingMediaGroupMessages: () => {
      events.push("media");
    },
    removeQueuedTelegramTurnsByMessageIds: () => {
      events.push("remove");
      return 0;
    },
    clearQueuedTelegramTurnPriorityByMessageId: () => false,
    prioritizeQueuedTelegramTurnByMessageId: () => false,
    answerCallbackQuery: async () => {},
    answerGuestQuery: async () => {},
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => undefined,
    handleAuthorizedTelegramMessage: async () => promptMaterialized(),
    handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
  });

  await runtime.handleUpdate(
    {
      message: {
        chat: { id: 7, type: "private" },
        from: { id: 7, is_bot: false },
        message_id: 100,
        message_thread_id: 44,
      },
    },
    TEST_CONTEXT,
  );
  await runtime.handleUpdate(
    {
      message_reaction: {
        chat: { id: 7, type: "private" },
        user: { id: 7, is_bot: false },
        message_id: 100,
        old_reaction: [],
        new_reaction: [{ type: "emoji", emoji: "👎" }],
      },
    },
    TEST_CONTEXT,
  );

  assert.deepEqual(events, [
    "record:7:100:44:follower",
    "forward-message:follower",
    "forward-reaction:follower",
  ]);
});

test("Update runtime executes delete and reaction plans through the right side effects", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    { kind: "deleted", messageIds: [1, 2], scope: {} },
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: (ids) => {
        events.push(`media:${ids.join(",")}`);
      },
      removeQueuedTelegramTurnsByMessageIds: (ids) => {
        events.push(`queue:${ids.join(",")}`);
        return ids.length;
      },
      handleAuthorizedTelegramReactionUpdate: async () => {
        events.push("reaction");
        return completed("reaction");
      },
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => promptMaterialized(),
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    },
  );
  assert.deepEqual(events, ["media:1,2", "queue:1,2"]);
});

test("Update runtime can execute directly from raw updates", async () => {
  const events: string[] = [];
  await executeTelegramUpdate(
    {
      message: {
        chat: { id: 10, type: "private" },
        message_id: 20,
        message_thread_id: 77,
        from: { id: 7, is_bot: false },
      },
    },
    undefined,
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async (_chatId, _replyToMessageId, text, options) => {
        events.push(
          `reply:${text}:${options?.target?.chatId}:${options?.target?.threadId}`,
        );
        return undefined;
      },
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => {
        events.push("edited-message");
        return completed("ignored");
      },
    },
  );
  assert.deepEqual(events, [
    "reply:This bot is not authorized for your account.:10:77",
  ]);
});

test("Update runtime swallows only stale context execution errors", async () => {
  const baseDeps = {
    ctx: TEST_CONTEXT,
    removePendingMediaGroupMessages: () => {},
    removeQueuedTelegramTurnsByMessageIds: () => 0,
    handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
    answerCallbackQuery: async () => {},
    answerGuestQuery: async () => {},
    handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
    sendTextReply: async () => undefined,
    handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
  };
  const plan = {
    kind: "message" as const,
    message: {
      chat: { id: 10, type: "private" as const },
      message_id: 20,
      from: { id: 7, is_bot: false },
    },
    shouldDeny: false,
  };
  assert.deepEqual(
    await executeTelegramUpdatePlan(plan, {
      ...baseDeps,
      handleAuthorizedTelegramMessage: async () => {
        throw new Error("ctx is stale after session reload");
      },
    }),
    {
      kind: "deferred",
      reason: "session-replay",
      key: "session-replay:message:10:0:20",
    },
  );
  await assert.rejects(
    () =>
      executeTelegramUpdatePlan(plan, {
        ...baseDeps,
        handleAuthorizedTelegramMessage: async () => {
          throw new Error("message handler broke");
        },
      }),
    /message handler broke/,
  );
});

test("Update runtime routes edited messages without creating normal message turns", async () => {
  const events: string[] = [];
  await executeTelegramUpdate(
    {
      edited_message: {
        chat: { id: 10, type: "private" },
        message_id: 20,
        from: { id: 7, is_bot: false },
      },
    },
    7,
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => {
        events.push("edited-message");
        return completed("ignored");
      },
    },
  );
  assert.deepEqual(events, ["edited-message"]);
});

test("Update runtime answers callbacks owned by another instance without handling", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "callback",
      query: {
        id: "cb-foreign",
        from: { id: 7, is_bot: false },
        message: { chat: { id: 10, type: "private" }, message_id: 99 },
      },
      shouldDeny: false,
    },
    {
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "instance-a",
      getMessageOwnership: () => ({ instanceId: "instance-b" }),
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async (id, text) => {
        events.push(`answer:${id}:${text}`);
      },
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => {
        events.push("callback");
        return completed("callback");
      },
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => promptMaterialized(),
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    },
  );
  assert.deepEqual(events, [
    "answer:cb-foreign:This Telegram message belongs to another Pi instance.",
  ]);
});

test("Update runtime forwards callbacks owned by another instance", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "callback",
      query: {
        id: "cb-foreign",
        from: { id: 7, is_bot: false },
        message: { chat: { id: 10, type: "private" }, message_id: 99 },
      },
      shouldDeny: false,
    },
    {
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "instance-a",
      getMessageOwnership: () => ({ instanceId: "instance-b" }),
      foreignOwnedUpdateForwarder: {
        forwardCallback: ({ ownership, ctx }) => {
          events.push(`forward:${ownership.instanceId}:${ctx}`);
          return true;
        },
      },
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async (id, text) => {
        events.push(`answer:${id}:${text}`);
      },
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => {
        events.push("callback");
        return completed("callback");
      },
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => promptMaterialized(),
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    },
  );
  assert.deepEqual(events, ["forward:instance-b:ctx"]);
});

test("Update runtime forwards callbacks from threads owned by another target instance", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "callback",
      query: {
        id: "cb-thread",
        from: { id: 7, is_bot: false },
        message: {
          chat: { id: 10, type: "private" },
          message_id: 99,
          message_thread_id: 42,
        },
      },
      shouldDeny: false,
    },
    {
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "leader",
      getMessageOwnership: () => undefined,
      getTargetOwnership: (target) =>
        target.chatId === 10 && target.threadId === 42
          ? { instanceId: "follower" }
          : undefined,
      foreignOwnedUpdateForwarder: {
        forwardCallback: ({ ownership, ctx }) => {
          events.push(`forward:${ownership.instanceId}:${ctx}`);
          return true;
        },
      },
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async (id, text) => {
        events.push(`answer:${id}:${text}`);
      },
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => {
        events.push("callback");
        return completed("callback");
      },
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => promptMaterialized(),
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    },
  );
  assert.deepEqual(events, ["forward:follower:ctx"]);
});

test("Update runtime forwards messages owned by another target instance", async () => {
  const events: string[] = [];
  await executeTelegramUpdate(
    {
      message: {
        chat: { id: -10010, type: "supergroup" },
        message_thread_id: 55,
        message_id: 20,
        from: { id: 7, is_bot: false },
      },
    },
    7,
    {
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "instance-a",
      getTargetOwnership: (target) => {
        events.push(`target:${target.chatId}:${target.threadId}`);
        return { instanceId: "instance-b" };
      },
      foreignOwnedUpdateForwarder: {
        forwardMessage: ({ message, ownership, ctx }) => {
          events.push(
            `forward:${ownership.instanceId}:${ctx}:${(message as { message_id?: number }).message_id}`,
          );
          return true;
        },
      },
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
    },
  );
  assert.deepEqual(events, ["target:-10010:55", "forward:instance-b:ctx:20"]);
});

test("Update runtime forwards edited messages owned by another message instance", async () => {
  const events: string[] = [];
  await executeTelegramUpdate(
    {
      edited_message: {
        chat: { id: 7, type: "private" },
        message_id: 21,
        from: { id: 7, is_bot: false },
      },
    },
    7,
    {
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "instance-a",
      getMessageOwnership: () => ({ instanceId: "instance-b" }),
      foreignOwnedUpdateForwarder: {
        forwardEditedMessage: ({ message, ownership, ctx }) => {
          events.push(
            `forward-edit:${ownership.instanceId}:${ctx}:${(message as { message_id?: number }).message_id}`,
          );
          return true;
        },
      },
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => promptMaterialized(),
      handleAuthorizedTelegramEditedMessage: async () => {
        events.push("edited-message");
        return completed("ignored");
      },
    },
  );
  assert.deepEqual(events, ["forward-edit:instance-b:ctx:21"]);
});

test("Update runtime forwards edited messages owned by another target instance", async () => {
  const events: string[] = [];
  await executeTelegramUpdate(
    {
      edited_message: {
        chat: { id: -10010, type: "supergroup" },
        message_thread_id: 55,
        message_id: 21,
        from: { id: 7, is_bot: false },
      },
    },
    7,
    {
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "instance-a",
      getTargetOwnership: () => ({ instanceId: "instance-b" }),
      foreignOwnedUpdateForwarder: {
        forwardEditedMessage: ({ message, ownership, ctx }) => {
          events.push(
            `forward-edit:${ownership.instanceId}:${ctx}:${(message as { message_id?: number }).message_id}`,
          );
          return true;
        },
      },
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => promptMaterialized(),
      handleAuthorizedTelegramEditedMessage: async () => {
        events.push("edited-message");
        return completed("ignored");
      },
    },
  );
  assert.deepEqual(events, ["forward-edit:instance-b:ctx:21"]);
});

test("Update runtime keeps unauthorized message replies in the source thread", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "message",
      message: {
        chat: { id: 7, type: "private" },
        from: { id: 2, is_bot: false },
        message_id: 9,
        message_thread_id: 44,
      },
      shouldDeny: true,
    },
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async (chatId, replyToMessageId, text, options) => {
        events.push(
          `reply:${chatId}:${replyToMessageId}:${text}:${options?.target?.chatId}:${options?.target?.threadId}`,
        );
        return undefined;
      },
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => {
        events.push("edited-message");
        return completed("ignored");
      },
    },
  );

  assert.deepEqual(events, [
    "reply:7:9:This bot is not authorized for your account.:7:44",
  ]);
});

test("Update runtime handles callback deny and message pair flows", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "callback",
      query: {
        id: "cb",
        from: { id: 1, is_bot: false },
        message: { chat: { type: "private" } },
      },
      shouldDeny: true,
    },
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async (id, text) => {
        events.push(`answer:${id}:${text}`);
      },
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => {
        events.push("callback");
        return completed("callback");
      },
      sendTextReply: async (chatId, replyToMessageId, text) => {
        events.push(`reply:${chatId}:${replyToMessageId}:${text}`);
        return undefined;
      },
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => {
        events.push("edited-message");
        return completed("ignored");
      },
    },
  );
  await executeTelegramUpdatePlan(
    {
      kind: "message",
      message: {
        chat: { id: 7, type: "private" },
        from: { id: 2, is_bot: false },
        message_id: 9,
        message_thread_id: 44,
      },
      shouldDeny: false,
    },
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async (chatId, replyToMessageId, text, options) => {
        events.push(
          `reply:${chatId}:${replyToMessageId}:${text}:${options?.target?.chatId}:${options?.target?.threadId}`,
        );
        return undefined;
      },
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => {
        events.push("edited-message");
        return completed("ignored");
      },
    },
  );
  assert.deepEqual(events, [
    "answer:cb:This bot is not authorized for your account.",
    "message",
  ]);
});

test("executeTelegramUpdatePlan with handleUnboundTelegramTopicMessage calls unbound handler for message with threadId", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "message",
      message: {
        message_id: 42,
        chat: { id: 1, type: "private" },
        message_thread_id: 100,
        from: { id: 1, is_bot: false, first_name: "Test" },
        date: 1000,
        text: "hi",
      },
      shouldDeny: false,
    },
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
      handleUnboundTelegramTopicMessage: async () => {
        events.push("unbound-topic");
        return operatorRerouteDeferred();
      },
    },
  );
  assert.deepEqual(events, ["unbound-topic"]);
});

test("executeTelegramUpdatePlan with handleUnboundTelegramTopicMessage falls through for message without threadId", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "message",
      message: {
        message_id: 43,
        chat: { id: 1, type: "private" },
        from: { id: 1, is_bot: false, first_name: "Test" },
        date: 1001,
        text: "hi",
      },
      shouldDeny: false,
    },
    {
      ctx: TEST_CONTEXT,
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
      handleUnboundTelegramTopicMessage: async () => {
        events.push("unbound-topic");
        return operatorRerouteDeferred();
      },
    },
  );
  assert.deepEqual(events, ["message"]);
});

test("executeTelegramUpdatePlan with foreign target ownership skips unbound handler", async () => {
  const events: string[] = [];
  await executeTelegramUpdatePlan(
    {
      kind: "message",
      message: {
        message_id: 44,
        chat: { id: 2, type: "private" },
        message_thread_id: 200,
        from: { id: 1, is_bot: false, first_name: "Test" },
        date: 1002,
        text: "hi",
      },
      shouldDeny: false,
    },
    {
      ctx: TEST_CONTEXT,
      getCurrentInstanceId: () => "current",
      getTargetOwnership: () => ({ instanceId: "other" }),
      foreignOwnedUpdateForwarder: {
        forwardMessage: async () => {
          events.push("forwarded");
          return true;
        },
      },
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async () => {
        events.push("message");
        return promptMaterialized();
      },
      handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
      handleUnboundTelegramTopicMessage: async () => {
        events.push("unbound-topic");
        return operatorRerouteDeferred();
      },
    },
  );
  assert.deepEqual(events, ["forwarded"]);
});

test("Registry is created lazily on first access and reused", () => {
  clearGlobalRegistry();
  assert.equal(getGlobalRegistry(), undefined);
  const first = getTelegramUpdateHandlerRegistry();
  assert.equal(first.version, 1);
  const second = getTelegramUpdateHandlerRegistry();
  assert.equal(first, second);
  assert.equal(getGlobalRegistry(), first);
  clearGlobalRegistry();
});

test("Registry is shared across import paths via globalThis", () => {
  clearGlobalRegistry();
  const fromHelper = getTelegramUpdateHandlerRegistry();
  const fromGlobal = getGlobalRegistry();
  assert.equal(fromHelper, fromGlobal);
  clearGlobalRegistry();
});

test("Dispatch returns 'pass' when no handlers are registered", async () => {
  clearGlobalRegistry();
  const registry = getTelegramUpdateHandlerRegistry();
  const verdict = await registry.dispatch({ update_id: 1 });
  assert.equal(verdict, "pass");
  clearGlobalRegistry();
});

test("registerTelegramUpdateHandler registers handlers and disposer removes them", async () => {
  clearGlobalRegistry();
  const seen: unknown[] = [];
  const handler: TelegramUpdateHandler = (update) => {
    seen.push(update);
    return "pass";
  };
  const off = registerTelegramUpdateHandler(handler);
  await getTelegramUpdateHandlerRegistry().dispatch({ update_id: 1 });
  assert.deepEqual(seen, [{ update_id: 1 }]);
  off();
  await getTelegramUpdateHandlerRegistry().dispatch({ update_id: 2 });
  assert.deepEqual(seen, [{ update_id: 1 }]);
  clearGlobalRegistry();
});

test("Consume short-circuits later handlers and bubbles up to dispatch", async () => {
  clearGlobalRegistry();
  const calls: string[] = [];
  const off1 = registerTelegramUpdateHandler((update) => {
    calls.push("first");
    const cb = (update as { callback_query?: { data?: string } })
      .callback_query;
    if (cb?.data === "myext:ok") return "consume";
    return "pass";
  });
  const off2 = registerTelegramUpdateHandler(() => {
    calls.push("second");
    return "pass";
  });
  const consumed = await getTelegramUpdateHandlerRegistry().dispatch({
    callback_query: { data: "myext:ok" },
  });
  assert.equal(consumed, "consume");
  assert.deepEqual(calls, ["first"]);

  calls.length = 0;
  const passed = await getTelegramUpdateHandlerRegistry().dispatch({
    callback_query: { data: "other" },
  });
  assert.equal(passed, "pass");
  assert.deepEqual(calls, ["first", "second"]);
  off1();
  off2();
  clearGlobalRegistry();
});

test("Handler errors do not break polling and do not consume the update", async () => {
  clearGlobalRegistry();
  const calls: string[] = [];
  const offThrow = registerTelegramUpdateHandler(() => {
    calls.push("thrower");
    throw new Error("boom");
  });
  const offAfter = registerTelegramUpdateHandler(() => {
    calls.push("after");
    return "pass";
  });
  const verdict = await getTelegramUpdateHandlerRegistry().dispatch({
    update_id: 1,
  });
  assert.equal(verdict, "pass");
  assert.deepEqual(calls, ["thrower", "after"]);
  offThrow();
  offAfter();
  clearGlobalRegistry();
});

test("Void/undefined return values are treated as 'pass'", async () => {
  clearGlobalRegistry();
  const off = registerTelegramUpdateHandler(() => undefined);
  const verdict = await getTelegramUpdateHandlerRegistry().dispatch({
    update_id: 1,
  });
  assert.equal(verdict, "pass");
  off();
  clearGlobalRegistry();
});

test("createTelegramUpdateHandle skips defaultHandle on consume", async () => {
  clearGlobalRegistry();
  const defaultCalls: number[] = [];
  const defaultHandle = async (update: {
    update_id: number;
    message?: { from: { id: number; is_bot: boolean } };
  }) => {
    defaultCalls.push(update.update_id);
    return completed("ignored");
  };
  const off = registerTelegramUpdateHandler((update) => {
    const id = (update as { update_id?: number }).update_id;
    return id === 99 ? "consume" : "pass";
  });
  const handler = createTelegramUpdateHandle({
    defaultHandle,
    pairingGate: createPairedGate(),
  });
  await handler(
    { update_id: 1, message: { from: { id: 7, is_bot: false } } },
    undefined,
  );
  await handler(
    { update_id: 99, message: { from: { id: 7, is_bot: false } } },
    undefined,
  );
  await handler(
    { update_id: 2, message: { from: { id: 7, is_bot: false } } },
    undefined,
  );
  assert.deepEqual(defaultCalls, [1, 2]);
  off();
  clearGlobalRegistry();
});

test("createTelegramUpdateHandle calls defaultHandle when no handlers registered", async () => {
  clearGlobalRegistry();
  const defaultCalls: unknown[] = [];
  const defaultHandle = async (update: { update_id: number }, ctx: string) => {
    defaultCalls.push({ update, ctx });
    return completed("ignored");
  };
  const handler = createTelegramUpdateHandle({
    defaultHandle,
    pairingGate: createPairedGate(),
  });
  await handler({ update_id: 7 }, "ctx");
  assert.deepEqual(defaultCalls, [{ update: { update_id: 7 }, ctx: "ctx" }]);
  clearGlobalRegistry();
});

test("Pre-existing docs-style registry missing 'dispatch' is replaced with a valid one", async () => {
  clearGlobalRegistry();
  const docsHandlers = new Set<TelegramUpdateHandler>();
  const docsStyle = {
    version: 1,
    add(handler: TelegramUpdateHandler) {
      docsHandlers.add(handler);
      return () => docsHandlers.delete(handler);
    },
  };
  (globalThis as Record<string, unknown>)[REGISTRY_KEY] = docsStyle;

  const registry = getTelegramUpdateHandlerRegistry();
  assert.notEqual(registry, docsStyle as unknown);
  assert.equal(registry.version, 1);
  assert.equal(typeof registry.add, "function");
  assert.equal(typeof registry.dispatch, "function");
  const verdict = await registry.dispatch({ update_id: 1 });
  assert.equal(verdict, "pass");
  assert.equal(getGlobalRegistry(), registry);
  clearGlobalRegistry();
});

test("Pre-existing malformed registry (wrong types) is replaced", async () => {
  clearGlobalRegistry();
  const malformed = {
    version: 1,
    add: "not a function",
    dispatch: 42,
  };
  (globalThis as Record<string, unknown>)[REGISTRY_KEY] = malformed;

  const registry = getTelegramUpdateHandlerRegistry();
  assert.notEqual(registry, malformed as unknown);
  assert.equal(typeof registry.add, "function");
  assert.equal(typeof registry.dispatch, "function");
  const verdict = await registry.dispatch({ update_id: 1 });
  assert.equal(verdict, "pass");
  clearGlobalRegistry();
});

test("Pre-existing registry with future version is replaced (v1 runtime, v2 squatter)", () => {
  clearGlobalRegistry();
  const futureShape = {
    version: 2,
    add: () => () => {},
    dispatch: async () => "pass" as const,
  };
  (globalThis as Record<string, unknown>)[REGISTRY_KEY] = futureShape;

  const registry = getTelegramUpdateHandlerRegistry();
  assert.notEqual(registry, futureShape as unknown);
  assert.equal(registry.version, 1);
  clearGlobalRegistry();
});

test("Pre-existing fully-formed v1 registry from a layered extension is reused", async () => {
  clearGlobalRegistry();
  const handlers = new Set<TelegramUpdateHandler>();
  const layered: TelegramUpdateHandlerRegistry = {
    version: 1,
    add(handler) {
      handlers.add(handler);
      return () => handlers.delete(handler);
    },
    async dispatch(update) {
      for (const handler of handlers) {
        const result = await handler(update);
        if (result === "consume") return "consume";
      }
      return "pass";
    },
  };
  (globalThis as Record<string, unknown>)[REGISTRY_KEY] = layered;

  const registry = getTelegramUpdateHandlerRegistry();
  assert.equal(registry, layered);

  const seen: unknown[] = [];
  const off = registerTelegramUpdateHandler((update) => {
    seen.push(update);
    return "pass";
  });
  await registry.dispatch({ update_id: 1 });
  assert.deepEqual(seen, [{ update_id: 1 }]);
  off();
  clearGlobalRegistry();
});

test("Pre-existing non-object value at registry key is replaced", () => {
  clearGlobalRegistry();
  (globalThis as Record<string, unknown>)[REGISTRY_KEY] = "not an object";
  const registry = getTelegramUpdateHandlerRegistry();
  assert.equal(registry.version, 1);
  assert.equal(typeof registry.dispatch, "function");
  clearGlobalRegistry();
});

test("createTelegramUpdateHandle accepts an explicit registry override", async () => {
  clearGlobalRegistry();
  const seen: unknown[] = [];
  const customRegistry: TelegramUpdateHandlerRegistry = {
    version: 1,
    add: () => () => {},
    async dispatch(update) {
      seen.push(update);
      return "consume";
    },
  };
  const defaultCalls: unknown[] = [];
  const handler = createTelegramUpdateHandle({
    pairingGate: createPairedGate(),
    defaultHandle: async (update) => {
      defaultCalls.push(update);
      return completed("ignored");
    },
    registry: customRegistry,
  });
  const update = {
    update_id: 1,
    message: { from: { id: 7, is_bot: false } },
  };
  await handler(update, undefined);
  assert.deepEqual(seen, [update]);
  assert.deepEqual(defaultCalls, []);
  assert.equal(getGlobalRegistry(), undefined);
  clearGlobalRegistry();
});

test("Unpaired gate hides proofs and rejects from public/default handlers before dispatch", async () => {
  const code = "01".repeat(16);
  let allowedUserId: number | undefined;
  const publicSeen: unknown[] = [];
  const defaultSeen: unknown[] = [];
  const responses: Array<{
    text: string;
    chatId: number;
    messageId: number;
    threadId?: number;
  }> = [];
  const claims: Array<{ senderId: number; code: string }> = [];
  const registry: TelegramUpdateHandlerRegistry = {
    version: 1,
    add: () => () => {},
    async dispatch(update) {
      publicSeen.push(update);
      return "consume";
    },
  };
  const handler = createTelegramUpdateHandle({
    registry,
    defaultHandle: async (update) => {
      defaultSeen.push(update);
      return completed("ignored");
    },
    pairingGate: {
      getAllowedUserId: () => allowedUserId,
      async claim(input) {
        claims.push(input);
        if (input.code === code) {
          allowedUserId = input.senderId;
          return { kind: "claimed" };
        }
        return { kind: "rejected" };
      },
      async sendGenericResponse(target) {
        responses.push(target);
      },
      onPaired: () => {},
      recordSideEffectFailure: () => {},
    },
  });
  const rejected = [
    { update_id: 1, callback_query: {} },
    { update_id: 2, edited_message: {} },
    {
      update_id: 3,
      message: {
        message_id: 1,
        chat: { id: 7, type: "private" },
        from: { id: 7, is_bot: false },
        text: `/start  ${code}`,
      },
    },
    {
      update_id: 4,
      message: {
        message_id: 1,
        chat: { id: -7, type: "group" },
        from: { id: 7, is_bot: false },
        text: `/start ${code}`,
      },
    },
  ];
  for (const update of rejected) await handler(update, "ctx");
  assert.deepEqual(publicSeen, []);
  assert.deepEqual(defaultSeen, []);
  assert.deepEqual(claims, []);
  assert.deepEqual(responses, []);

  const proof = {
    update_id: 5,
    message: {
      message_id: 9,
      message_thread_id: 12,
      is_topic_message: true,
      date: 1,
      chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false },
      text: `/start ${code}`,
    },
  };
  await handler(proof, "ctx");
  assert.deepEqual(claims, [{ senderId: 7, code }]);
  assert.deepEqual(publicSeen, []);
  assert.deepEqual(defaultSeen, []);
  assert.deepEqual(responses, [
    {
      text: "Pairing request received.",
      chatId: 7,
      messageId: 9,
      threadId: 12,
    },
  ]);

  await handler(proof, "ctx");
  assert.deepEqual(claims, [{ senderId: 7, code }]);
  assert.equal(responses.length, 1);
  assert.deepEqual(publicSeen, []);
  assert.deepEqual(defaultSeen, []);

  const pairedOwnerUpdate = {
    update_id: 6,
    message: { from: { id: 7, is_bot: false } },
  };
  await handler(pairedOwnerUpdate, "ctx");
  assert.deepEqual(publicSeen, [pairedOwnerUpdate]);
  assert.deepEqual(defaultSeen, []);
});

test("Paired unauthorized senders are denied before public handler dispatch", async () => {
  const publicSeen: unknown[] = [];
  const defaultSeen: unknown[] = [];
  const handler = createTelegramUpdateHandle({
    registry: {
      version: 1,
      add: () => () => {},
      async dispatch(update) {
        publicSeen.push(update);
        return "pass";
      },
    },
    defaultHandle: async (update) => {
      defaultSeen.push(update);
      return completed("ignored");
    },
    pairingGate: createPairedGate(),
  });
  const denied = await handler(
    {
      update_id: 70,
      message: {
        chat: { id: 70, type: "private" },
        message_id: 1,
        from: { id: 99, is_bot: false },
      },
    },
    "ctx",
  );
  assert.deepEqual(denied, completed("unauthorized"));
  assert.deepEqual(publicSeen, []);
  assert.deepEqual(defaultSeen, []);

  await handler(
    {
      update_id: 71,
      message: {
        chat: { id: 7, type: "private" },
        message_id: 2,
        from: { id: 7, is_bot: false },
      },
    },
    "ctx",
  );
  assert.equal(publicSeen.length, 1);
  assert.equal(defaultSeen.length, 1);
});

test("Public handlers require an exact owner sender while senderless terminal updates stay built-in", async () => {
  const publicSeen: unknown[] = [];
  const defaultSeen: unknown[] = [];
  const handler = createTelegramUpdateHandle({
    registry: {
      version: 1,
      add: () => () => {},
      async dispatch(update) {
        publicSeen.push(update);
        return "pass";
      },
    },
    defaultHandle: async (update) => {
      defaultSeen.push(update);
      return completed("ignored");
    },
    pairingGate: createPairedGate(),
  });
  const deleted = {
    update_id: 72,
    deleted_business_messages: { chat: { id: 7 }, message_ids: [3] },
  };
  const futureShape = {
    update_id: 73,
    future_update: { from: { id: 7, is_bot: false } },
  };
  await handler(deleted, "ctx");
  await handler(futureShape, "ctx");
  assert.deepEqual(publicSeen, []);
  assert.deepEqual(defaultSeen, [deleted, futureShape]);
});

test("Proof-shaped start text with extra Bot fields is suppressed before strict claim and routing", async () => {
  const code = "03".repeat(16);
  for (const allowedUserId of [undefined, 7]) {
    let claims = 0;
    let publicCalls = 0;
    let defaultCalls = 0;
    const handler = createTelegramUpdateHandle({
      registry: {
        version: 1,
        add: () => () => {},
        async dispatch() {
          publicCalls += 1;
          return "pass";
        },
      },
      defaultHandle: async () => {
        defaultCalls += 1;
        return completed("ignored");
      },
      pairingGate: {
        getAllowedUserId: () => allowedUserId,
        async claim() {
          claims += 1;
          return { kind: "rejected" };
        },
        sendGenericResponse: async () => {},
        onPaired: () => {},
        recordSideEffectFailure: () => {},
      },
    });
    await handler(
      {
        update_id: 74,
        message: {
          message_id: 4,
          chat: { id: 7, type: "private" },
          from: { id: 7, is_bot: false },
          text: `/start ${code}`,
          future_bot_field: true,
        },
      },
      "ctx",
    );
    assert.equal(claims, 0);
    assert.equal(publicCalls, 0);
    assert.equal(defaultCalls, 0);
  }
});

test("Legacy or malformed follower forwarding stays retryable with a stable key", async () => {
  for (const forwardingResult of [undefined, false, true, { version: 1 }]) {
    const outcome = await executeTelegramUpdate(
      {
        update_id: 88,
        message: {
          chat: { id: 7, type: "private" },
          message_id: 44,
          message_thread_id: 9,
          from: { id: 7, is_bot: false },
        },
      },
      7,
      {
        ctx: TEST_CONTEXT,
        getCurrentInstanceId: () => "leader",
        getTargetOwnership: () => ({ instanceId: "follower" }),
        foreignOwnedUpdateForwarder: {
          forwardMessage: async () => forwardingResult as boolean,
        },
        removePendingMediaGroupMessages: () => {},
        removeQueuedTelegramTurnsByMessageIds: () => 0,
        handleAuthorizedTelegramReactionUpdate: async () => completed("reaction"),
        answerCallbackQuery: async () => {},
        answerGuestQuery: async () => {},
        handleAuthorizedTelegramCallbackQuery: async () => completed("callback"),
        sendTextReply: async () => undefined,
        handleAuthorizedTelegramMessage: async () => promptMaterialized(),
        handleAuthorizedTelegramEditedMessage: async () => completed("ignored"),
      },
    );
    assert.deepEqual(outcome, {
      kind: "deferred",
      reason: "follower-admission-pending",
      key: "follower:7:9:update:88",
    });
  }
});

test("Pairing response and status failures are redacted best-effort side effects", async () => {
  const code = "02".repeat(16);
  const phases: string[] = [];
  let defaultCalls = 0;
  let publicCalls = 0;
  const handler = createTelegramUpdateHandle({
    registry: {
      version: 1,
      add: () => () => {},
      async dispatch() {
        publicCalls += 1;
        return "pass";
      },
    },
    defaultHandle: async () => {
      defaultCalls += 1;
      return completed("ignored");
    },
    pairingGate: {
      getAllowedUserId: () => undefined,
      claim: async () => ({ kind: "claimed" }),
      sendGenericResponse: async () => {
        throw new Error("response failed");
      },
      onPaired: async () => {
        throw new Error("status failed");
      },
      recordSideEffectFailure: (phase) => {
        phases.push(phase);
      },
    },
  });
  await assert.doesNotReject(() =>
    handler(
      {
        update_id: 7,
        message: {
          message_id: 10,
          date: 1,
          chat: { id: 7, type: "private" },
          from: { id: 7, is_bot: false },
          text: `/start ${code}`,
        },
      },
      "ctx",
    ),
  );
  assert.deepEqual(phases, ["response", "on-paired"]);
  assert.equal(publicCalls, 0);
  assert.equal(defaultCalls, 0);
});

test("Public update handler failures are isolated and expose only generated id/category", async () => {
  const failures: Array<{ id: string; category: string }> = [];
  const registry = getTelegramUpdateHandlerRegistry();
  const dispose = registerTelegramUpdateHandler(async () => {
    throw new Error("secret update payload");
  });
  try {
    assert.equal(await registry.dispatch({}, (id, category) => failures.push({ id, category })), "pass");
    assert.equal(failures.length, 1);
    assert.match(failures[0]!.id, /^update-/);
    assert.equal(failures[0]!.category, "update");
    assert.equal(JSON.stringify(failures).includes("secret"), false);
  } finally {
    dispose();
  }
});
