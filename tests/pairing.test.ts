/**
 * Regression tests for secure Telegram pairing
 * Covers verifier persistence, expiry, rate limiting, modes, restart recovery, and atomic single-winner claims
 */

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  createTelegramConfigStore,
  readTelegramConfig,
} from "../lib/config.ts";
import {
  TELEGRAM_PAIRING_EXPIRY_MS,
  createTelegramPairingRuntime,
  parseTelegramPairingCandidate,
} from "../lib/pairing.ts";

function deterministicHash(code: string, salt: string): string {
  return createHash("sha256").update(salt).update("\0").update(code).digest("base64url");
}

function sequenceRandom() {
  let value = 1;
  let calls = 0;
  return {
    get calls() { return calls; },
    bytes(size: number) {
      calls += 1;
      return Uint8Array.from({ length: size }, () => value++ & 0xff);
    },
  };
}

async function createHarness(nowMs = 1_000) {
  const agentDir = await mkdtemp(join(tmpdir(), "pi-telegram-pairing-"));
  const configPath = join(agentDir, "telegram.json");
  const attemptsPath = join(agentDir, "tmp", "telegram", "pairing-attempts.json");
  const store = createTelegramConfigStore({
    initialConfig: { profiles: { default: { botToken: "token" } } },
    agentDir,
    configPath,
  });
  await store.persist(store.get());
  const clock = { now: () => nowMs };
  const random = sequenceRandom();
  const runtime = createTelegramPairingRuntime({
    configStore: store,
    agentDir,
    attemptsPath: () => attemptsPath,
    clock,
    random,
    hasher: { hash: deterministicHash },
  });
  return { agentDir, configPath, attemptsPath, store, clock, random, runtime };
}

test("Pairing parser admits only exact private-human text proof shapes", () => {
  const code = "01".repeat(16);
  const valid = { update_id: 1, message: { message_id: 2, date: 3, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: `/start ${code}`, entities: [] } };
  assert.deepEqual(parseTelegramPairingCandidate(valid), { senderId: 7, chatId: 7, messageId: 2, code });
  assert.deepEqual(
    parseTelegramPairingCandidate({
      ...valid,
      message: {
        ...valid.message,
        message_thread_id: 12,
        is_topic_message: true,
      },
    }),
    { senderId: 7, chatId: 7, messageId: 2, threadId: 12, code },
  );
  for (const update of [
    { ...valid, message: { ...valid.message, text: `/start  ${code}` } },
    { ...valid, message: { ...valid.message, text: `/start ${code} extra` } },
    { ...valid, message: { ...valid.message, text: `/start@bot ${code}` } },
    { ...valid, message: { ...valid.message, text: `/start ${code}\n` } },
    { ...valid, message: { ...valid.message, text: "/start short" } },
    { ...valid, message: { ...valid.message, chat: { id: -7, type: "group" } } },
    { ...valid, message: { ...valid.message, from: { id: 7, is_bot: true } } },
    { ...valid, message: { ...valid.message, document: {}, text: `/start ${code}` } },
    { ...valid, message: { ...valid.message, message_thread_id: 0, is_topic_message: true } },
    { ...valid, message: { ...valid.message, is_topic_message: true } },
    { ...valid, message: { ...valid.message, message_thread_id: 12 } },
    { ...valid, message: { ...valid.message, message_thread_id: 12, is_topic_message: false } },
    { update_id: 1, edited_message: valid.message },
    { update_id: 1, callback_query: { from: valid.message.from } },
    { update_id: 1, message: valid.message, message_reaction: {} },
  ]) assert.equal(parseTelegramPairingCandidate(update), undefined);
});

test("Pairing persists only a salted verifier and survives restart without raw code", async () => {
  const h = await createHarness();
  const begin = await h.runtime.begin();
  assert.equal(begin.kind, "ready");
  if (begin.kind !== "ready") return;
  const raw = await readFile(h.configPath, "utf8");
  assert.equal(raw.includes(begin.code), false);
  const persisted = await readTelegramConfig(h.configPath);
  const pairing = persisted.profiles?.default?.pairing;
  assert.ok(pairing);
  assert.equal(pairing.createdAtMs, 1_000);
  assert.equal(pairing.expiresAtMs, 1_000 + TELEGRAM_PAIRING_EXPIRY_MS);
  assert.notEqual(pairing.verifier, begin.code);
  assert.notEqual(pairing.salt, begin.code);
  assert.equal((await stat(h.configPath)).mode & 0o777, 0o600);

  const restarted = createTelegramConfigStore({ agentDir: h.agentDir, configPath: h.configPath });
  await restarted.load();
  const restartRandom = sequenceRandom();
  const runtime = createTelegramPairingRuntime({
    configStore: restarted,
    agentDir: h.agentDir,
    attemptsPath: () => h.attemptsPath,
    clock: h.clock,
    random: restartRandom,
    hasher: { hash: deterministicHash },
  });
  assert.deepEqual(await runtime.begin(), {
    kind: "pending",
    createdAtMs: 1_000,
    expiresAtMs: 1_000 + TELEGRAM_PAIRING_EXPIRY_MS,
  });
  const pendingInstructions = await runtime.getLocalInstructions();
  assert.match(pendingInstructions ?? "", /already pending/);
  assert.equal(pendingInstructions?.includes(begin.code), false);
  assert.equal(restartRandom.calls, 0);
  assert.deepEqual(await runtime.claim({ senderId: 77, code: begin.code }), { kind: "claimed" });
  const claimed = await readTelegramConfig(h.configPath);
  assert.equal(claimed.profiles?.default?.allowedUserId, 77);
  assert.equal(claimed.profiles?.default?.pairing, undefined);
});

test("Pairing begin redisplays only its process-local unexpired proof", async () => {
  const h = await createHarness();
  const first = await h.runtime.begin();
  assert.equal(first.kind, "ready");
  if (first.kind !== "ready") return;
  assert.deepEqual(await h.runtime.begin(), first);
  assert.match(await h.runtime.getLocalInstructions() ?? "", new RegExp(first.code));
  assert.equal(h.random.calls, 2);
});

test("Concurrent runtimes preserve one unexpired verifier without rotating it", async () => {
  const h = await createHarness();
  const secondStore = createTelegramConfigStore({
    agentDir: h.agentDir,
    configPath: h.configPath,
  });
  await secondStore.load();
  const secondRandom = sequenceRandom();
  const second = createTelegramPairingRuntime({
    configStore: secondStore,
    agentDir: h.agentDir,
    attemptsPath: () => h.attemptsPath,
    clock: h.clock,
    random: secondRandom,
    hasher: { hash: deterministicHash },
  });
  const [left, right] = await Promise.all([h.runtime.begin(), second.begin()]);
  assert.deepEqual([left.kind, right.kind].sort(), ["pending", "ready"]);
  const ready = left.kind === "ready" ? left : right.kind === "ready" ? right : undefined;
  assert.ok(ready);
  const persisted = await readTelegramConfig(h.configPath);
  assert.equal(persisted.profiles?.default?.pairing?.createdAtMs, 1_000);
  assert.equal(h.random.calls + secondRandom.calls, 2);
  const pendingRuntime = left.kind === "pending" ? h.runtime : second;
  const instructions = await pendingRuntime.getLocalInstructions();
  assert.match(instructions ?? "", /already pending/);
  assert.equal(instructions?.includes(ready.code), false);
});

test("Expired verifier may be replaced by a fresh local proof", async () => {
  const h = await createHarness();
  const first = await h.runtime.begin();
  assert.equal(first.kind, "ready");
  if (first.kind !== "ready") return;
  h.clock.now = () => 1_000 + TELEGRAM_PAIRING_EXPIRY_MS;
  const replacement = await h.runtime.begin();
  assert.equal(replacement.kind, "ready");
  if (replacement.kind !== "ready") return;
  assert.notEqual(replacement.code, first.code);
  assert.equal(replacement.createdAtMs, 1_000 + TELEGRAM_PAIRING_EXPIRY_MS);
  assert.equal(h.random.calls, 4);
});

test("Pairing claim fails closed when persisted verifier becomes malformed", async () => {
  const h = await createHarness();
  const begin = await h.runtime.begin();
  assert.equal(begin.kind, "ready");
  if (begin.kind !== "ready") return;
  await writeFile(
    h.configPath,
    `${JSON.stringify({
      profiles: {
        default: {
          botToken: "token",
          pairing: { verifier: "malformed" },
        },
      },
    })}\n`,
    { encoding: "utf8", mode: 0o600 },
  );
  const diagnostics: Array<{
    category: string;
    message: string;
    phase: string;
  }> = [];
  const runtime = createTelegramPairingRuntime({
    configStore: h.store,
    agentDir: h.agentDir,
    attemptsPath: () => h.attemptsPath,
    clock: h.clock,
    random: sequenceRandom(),
    hasher: { hash: deterministicHash },
    recordRuntimeEvent: (category, error, details) => {
      diagnostics.push({
        category,
        message: error instanceof Error ? error.message : String(error),
        phase: String(details?.phase),
      });
    },
  });

  await assert.doesNotReject(async () => {
    assert.deepEqual(
      await runtime.claim({ senderId: 77, code: begin.code }),
      { kind: "rejected" },
    );
  });
  assert.deepEqual(diagnostics, [
    {
      category: "pairing",
      message: "Invalid Telegram pairing verifier at profiles.default",
      phase: "claim-transaction",
    },
  ]);
  assert.equal(JSON.stringify(diagnostics).includes(begin.code), false);

  const throwingDiagnosticsRuntime = createTelegramPairingRuntime({
    configStore: h.store,
    agentDir: h.agentDir,
    attemptsPath: () => h.attemptsPath,
    clock: h.clock,
    random: sequenceRandom(),
    hasher: { hash: deterministicHash },
    recordRuntimeEvent: () => {
      throw new Error("diagnostics unavailable");
    },
  });
  await assert.doesNotReject(async () => {
    assert.deepEqual(
      await throwingDiagnosticsRuntime.claim({ senderId: 78, code: begin.code }),
      { kind: "rejected" },
    );
  });
});

test("Pairing attempt limits are isolated by active profile", async () => {
  const agentDir = await mkdtemp(join(tmpdir(), "pi-telegram-pairing-profiles-"));
  const configPath = join(agentDir, "telegram.json");
  const store = createTelegramConfigStore({
    initialConfig: {
      profiles: {
        default: { botToken: "default-token" },
        work: { botToken: "work-token" },
      },
    },
    agentDir,
    configPath,
  });
  await store.persist(store.get());
  const runtime = createTelegramPairingRuntime({
    configStore: store,
    agentDir,
    clock: { now: () => 1_000 },
    random: sequenceRandom(),
    hasher: { hash: deterministicHash },
  });
  assert.equal((await runtime.begin()).kind, "ready");
  assert.deepEqual(
    await runtime.claim({ senderId: 11, code: "ff".repeat(16) }),
    { kind: "rejected" },
  );
  assert.equal(store.activateProfile("work"), true);
  assert.equal((await runtime.begin()).kind, "ready");
  assert.deepEqual(
    await runtime.claim({ senderId: 22, code: "ee".repeat(16) }),
    { kind: "rejected" },
  );

  const defaultAttempts = await readFile(
    join(agentDir, "tmp", "telegram", "pairing-attempts.json"),
    "utf8",
  );
  const workAttempts = await readFile(
    join(agentDir, "tmp", "telegram", "pairing-attempts.work.json"),
    "utf8",
  );
  assert.match(defaultAttempts, /"senderId": 11/u);
  assert.doesNotMatch(defaultAttempts, /"senderId": 22/u);
  assert.match(workAttempts, /"senderId": 22/u);
  assert.doesNotMatch(workAttempts, /"senderId": 11/u);
});

test("Existing paired profiles never generate or replace pairing proof", async () => {
  const h = await createHarness();
  h.store.setAllowedUserId(42);
  await h.store.persist();
  const before = await readFile(h.configPath, "utf8");
  assert.deepEqual(await h.runtime.begin(), { kind: "already-paired" });
  assert.equal(h.random.calls, 0);
  assert.equal(await readFile(h.configPath, "utf8"), before);
});

test("Pairing enforces five attempts per sender, private modes, and expiry cleanup", async () => {
  const h = await createHarness();
  const begin = await h.runtime.begin();
  assert.equal(begin.kind, "ready");
  if (begin.kind !== "ready") return;
  const wrong = "ff".repeat(16);
  for (let i = 0; i < 5; i += 1) {
    assert.deepEqual(await h.runtime.claim({ senderId: 9, code: wrong }), { kind: "rejected" });
  }
  assert.deepEqual(await h.runtime.claim({ senderId: 9, code: begin.code }), { kind: "limited" });
  assert.equal((await stat(join(h.agentDir, "tmp", "telegram"))).mode & 0o777, 0o700);
  assert.equal((await stat(h.attemptsPath)).mode & 0o777, 0o600);
  const state = await readFile(h.attemptsPath, "utf8");
  assert.equal(state.includes(begin.code), false);
  assert.equal(state.includes(wrong), false);
  h.clock.now = () => 1_000 + TELEGRAM_PAIRING_EXPIRY_MS;
  assert.deepEqual(await h.runtime.claim({ senderId: 10, code: begin.code }), { kind: "expired" });
  const config = await readTelegramConfig(h.configPath);
  assert.equal(config.profiles?.default?.pairing, undefined);
});

test("Concurrent claims have one winner and preserve unrelated config writes", async () => {
  const h = await createHarness();
  const begin = await h.runtime.begin();
  assert.equal(begin.kind, "ready");
  if (begin.kind !== "ready") return;
  const secondStore = createTelegramConfigStore({ agentDir: h.agentDir, configPath: h.configPath });
  await secondStore.load();
  const second = createTelegramPairingRuntime({
    configStore: secondStore,
    agentDir: h.agentDir,
    attemptsPath: () => h.attemptsPath,
    clock: h.clock,
    random: sequenceRandom(),
    hasher: { hash: deterministicHash },
  });
  secondStore.update((config) => { config.assistant = { proactivePush: false }; });
  const [, firstClaim, secondClaim] = await Promise.all([
    secondStore.persist(),
    h.runtime.claim({ senderId: 71, code: begin.code }),
    second.claim({ senderId: 72, code: begin.code }),
  ]);
  assert.equal([firstClaim.kind, secondClaim.kind].filter((kind) => kind === "claimed").length, 1);
  const config = await readTelegramConfig(h.configPath);
  assert.ok(config.profiles?.default?.allowedUserId === 71 || config.profiles?.default?.allowedUserId === 72);
  assert.equal(config.assistant?.proactivePush, false);
  assert.equal(config.profiles?.default?.pairing, undefined);
});
