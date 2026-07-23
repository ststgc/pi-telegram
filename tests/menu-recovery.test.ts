/**
 * Regression tests for the Telegram durable-recovery operator menu
 * Exercises strict metadata projection, confirmations, action refresh, and failure recording
 */

import assert from "node:assert/strict";
import test from "node:test";

import {
  buildTelegramRecoveryMenuReplyMarkup,
  buildTelegramRecoveryMenuText,
  createTelegramRecoveryMenuRuntime,
  projectTelegramRecoveryOperatorStatus,
} from "../lib/menu-recovery.ts";
import type {
  RecoveryMetadataStatus,
  RecoveryOrphanReassignmentCandidate,
} from "../lib/recovery.ts";

function makeStatus(): RecoveryMetadataStatus {
  return {
    profile: "default",
    mode: "active",
    admissionEnabled: true,
    committedUpdateId: 987654,
    counts: {} as RecoveryMetadataStatus["counts"],
    quota: {
      payloadBytes: 10,
      spoolBytes: 20,
      recordBytes: 30,
      reservedBytes: 0,
      totalBytes: 60,
      limitBytes: 512 * 1024 * 1024,
    },
    oldestUnresolvedAgeMs: 90_000,
    items: [
      {
        id: "forbidden-record-id",
        actionId: "handle_retry",
        family: "inbound",
        state: "execution-uncertain",
        ageMs: 90_000,
        requiredAction: "retry-or-discard",
        target: { chatId: 123456, threadId: 789 },
        payload: "secret prompt",
        path: "/private/secret",
      } as RecoveryMetadataStatus["items"][number],
      {
        id: "another-record-id",
        actionId: "handle_drain",
        family: "inbound",
        state: "admitted",
        ageMs: 1_000,
        requiredAction: "drain",
      },
      {
        id: "bus-record-id",
        actionId: "handle_bus",
        family: "bus",
        state: "bus-uncertain",
        ageMs: 2_000,
        requiredAction: "retry-or-discard",
      },
    ],
    incidents: [
      "orphaned-dispatch-marked-uncertain",
      "token=super-secret target=123456",
    ],
  };
}

function makeOrphans(): RecoveryOrphanReassignmentCandidate[] {
  return [
    {
      actionId: "orphan_handle",
      unresolvedCount: 2,
      oldestAgeMs: 5_000,
      states: ["admitted", "pending"],
      target: { chatId: 123456 },
      ownerId: "forbidden-owner",
    } as RecoveryOrphanReassignmentCandidate,
  ];
}

test("recovery operator projection serializes only approved metadata", () => {
  const projected = projectTelegramRecoveryOperatorStatus(
    makeStatus(),
    makeOrphans(),
  );
  const serialized = JSON.stringify(projected);

  assert.deepEqual(projected.familyCounts, {
    inbound: 2,
    outbound: 0,
    bus: 1,
  });
  assert.deepEqual(projected.stateCounts, {
    "execution-uncertain": 1,
    admitted: 1,
    "bus-uncertain": 1,
  });
  assert.deepEqual(projected.incidents, [
    "orphaned-dispatch-marked-uncertain",
    "recovery-incident",
  ]);
  assert.deepEqual(Object.keys(projected.items[0]!).sort(), [
    "handle",
    "requiredAction",
  ]);
  assert.deepEqual(Object.keys(projected.orphanCandidates[0]!).sort(), [
    "handle",
    "requiredAction",
  ]);
  assert.match(serialized, /handle_retry/);
  assert.match(serialized, /orphan_handle/);
  assert.doesNotMatch(
    serialized,
    /987654|123456|789|forbidden-record-id|another-record-id|bus-record-id|forbidden-owner|secret prompt|private\/secret|super-secret|committedUpdateId|admissionEnabled|payloadBytes|spoolBytes|recordBytes|target|ownerId|recordId|turnId|payload|path|token/,
  );
});

test("recovery menu renders opaque handles and only inbound supported actions", () => {
  const projected = projectTelegramRecoveryOperatorStatus(
    makeStatus(),
    makeOrphans(),
  );
  const html = buildTelegramRecoveryMenuText(projected);
  const callbacks = buildTelegramRecoveryMenuReplyMarkup(projected)
    .inline_keyboard.flat()
    .map((button) => button.callback_data);

  assert.match(html, /profile/);
  assert.match(html, /handle_retry/);
  assert.match(html, /retry or discard/);
  assert.doesNotMatch(html, /handle_bus/);
  assert.ok(callbacks.includes("recovery:ask:drain"));
  assert.ok(callbacks.includes("recovery:ask:retry:handle_retry"));
  assert.ok(callbacks.includes("recovery:ask:discard:handle_retry"));
  assert.ok(callbacks.includes("recovery:ask:reassign:orphan_handle"));
  assert.ok(callbacks.includes("recovery:ask:downgrade"));
  assert.equal(
    callbacks.some((callback) => callback.includes("handle_bus")),
    false,
  );
  assert.ok(callbacks.every((callback) => Buffer.byteLength(callback) <= 64));
});

test("recovery actions require confirmation, warn on retry, and refresh", async () => {
  const events: string[] = [];
  const answers: string[] = [];
  const edits: string[] = [];
  const state = {
    chatId: 1,
    messageId: 2,
    page: 0,
    scope: "all" as const,
    scopedModels: [],
    allModels: [],
    mode: "status" as const,
  };
  const runtime = createTelegramRecoveryMenuRuntime({
    getStatus: makeStatus,
    getOrphanCandidates: makeOrphans,
    drainSafe: async () => {
      events.push("drain");
      return 1;
    },
    retryUncertain: async (handle) => {
      events.push(`retry:${handle}`);
      return { appended: true, duplicationWarning: true };
    },
    discard: (handle) => {
      events.push(`discard:${handle}`);
    },
    reassign: async (handle) => {
      events.push(`reassign:${handle}`);
    },
    downgrade: async () => ({ status: "blocked", blockerCount: 3 }),
    getStoredModelMenuState: () => state,
    editInteractiveMessage: async (_chatId, _messageId, text) => {
      edits.push(text);
    },
    answerCallbackQuery: async (_id, text) => {
      answers.push(text ?? "answered");
    },
  });

  assert.equal(runtime.getUnresolvedCount(), 3);
  assert.equal(
    await runtime.handleCallbackQuery(
      { id: "ask", data: "recovery:ask:retry:handle_retry" },
      {},
    ),
    true,
  );
  assert.deepEqual(events, []);
  assert.match(edits.at(-1)!, /duplicate work/);

  await runtime.handleCallbackQuery(
    { id: "confirm", data: "recovery:confirm:retry:handle_retry" },
    {},
  );
  assert.deepEqual(events, ["retry:handle_retry"]);
  assert.match(answers.at(-1)!, /Duplicate execution remains possible/);
  assert.match(edits.at(-1)!, /Durable recovery/);

  await runtime.handleCallbackQuery(
    { id: "preflight", data: "recovery:confirm:downgrade" },
    {},
  );
  assert.equal(answers.at(-1), "Recovery downgrade blocked (3).");

  await runtime.handleCallbackQuery(
    { id: "wrong-action", data: "recovery:confirm:retry" },
    {},
  );
  assert.equal(answers.at(-1), "Invalid recovery action.");
  assert.deepEqual(events, ["retry:handle_retry"]);
});

test("recovery status-count failures log only a fixed sanitized error", () => {
  const events: Array<{ message: string; action: unknown }> = [];
  const runtime = createTelegramRecoveryMenuRuntime({
    getStatus() {
      throw new Error("private token=secret chat=123456");
    },
    getOrphanCandidates: () => [],
    drainSafe: async () => 0,
    retryUncertain: async () => ({
      appended: false,
      duplicationWarning: true,
    }),
    discard: () => {},
    reassign: async () => {},
    downgrade: async () => ({ status: "downgraded" }),
    getStoredModelMenuState: () => undefined,
    editInteractiveMessage: async () => {},
    answerCallbackQuery: async () => {},
    recordRuntimeEvent: (_category, error, details) => {
      events.push({
        message: error instanceof Error ? error.message : String(error),
        action: details?.action,
      });
    },
  });

  assert.equal(runtime.getUnresolvedCount(), 0);
  assert.deepEqual(events, [
    {
      message: "Telegram recovery operation failed (status-count)",
      action: "status-count",
    },
  ]);
  assert.doesNotMatch(JSON.stringify(events), /secret|123456/);
});

test("recovery action failures are recorded and followed by status refresh", async () => {
  const events: string[] = [];
  const edits: string[] = [];
  const answers: string[] = [];
  const runtime = createTelegramRecoveryMenuRuntime({
    getStatus: makeStatus,
    getOrphanCandidates: makeOrphans,
    drainSafe: async () => 0,
    retryUncertain: async () => {
      throw new Error("private target 123456");
    },
    discard: () => {},
    reassign: async () => {},
    downgrade: async () => ({ status: "downgraded" }),
    getStoredModelMenuState: () => ({
      chatId: 1,
      messageId: 2,
      page: 0,
      scope: "all",
      scopedModels: [],
      allModels: [],
      mode: "recovery",
    }),
    editInteractiveMessage: async (_chatId, _messageId, text) => {
      edits.push(text);
    },
    answerCallbackQuery: async (_id, text) => {
      answers.push(text ?? "");
    },
    recordRuntimeEvent: (_category, _error, details) => {
      events.push(String(details?.action));
    },
  });

  await runtime.handleCallbackQuery(
    { id: "failure", data: "recovery:confirm:retry:handle_retry" },
    {},
  );
  assert.deepEqual(events, ["retry"]);
  assert.equal(answers.at(-1), "Recovery action failed. Check diagnostics.");
  assert.match(edits.at(-1)!, /Durable recovery/);
  assert.doesNotMatch(answers.join(" "), /123456/);
});
