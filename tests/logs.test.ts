/**
 * Regression tests for append-only Telegram runtime diagnostics segments
 * Covers writer isolation, unique rotation, leases, retention, caps, redaction, modes, and concurrent append
 */

import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { chmod, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  createTelegramRuntimeJsonlLog,
  getTelegramPreviousRuntimeLogPath,
  getTelegramRuntimeLogPath,
  TELEGRAM_RUNTIME_LOG_MAX_SEGMENTS,
  TELEGRAM_RUNTIME_LOG_MAX_TOTAL_BYTES,
  TELEGRAM_RUNTIME_LOG_RETENTION_MS,
} from "../lib/logs.ts";

async function waitForSegments(dir: string, count = 1): Promise<string[]> {
  const deadline = Date.now() + 3_000;
  do {
    const names = (await readdir(dir).catch(() => []))
      .filter((name) => name.includes(".segment-") && name.endsWith(".jsonl"));
    if (names.length >= count) return names.map((name) => join(dir, name));
    await new Promise((resolve) => setTimeout(resolve, 10));
  } while (Date.now() < deadline);
  throw new Error(`Expected ${count} diagnostics segments`);
}

async function readLines(path: string): Promise<Array<Record<string, unknown>>> {
  return (await readFile(path, "utf8")).trim().split("\n").filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

test("Runtime log display paths remain profile-scoped while segments are append-only", () => {
  assert.equal(getTelegramRuntimeLogPath("/agent"), join("/agent", "tmp", "telegram", "logs.jsonl"));
  assert.equal(getTelegramPreviousRuntimeLogPath("/agent", "work"), join("/agent", "tmp", "telegram", "logs.work._prev.jsonl"));
  assert.equal(TELEGRAM_RUNTIME_LOG_RETENTION_MS, 14 * 24 * 60 * 60 * 1000);
  assert.equal(TELEGRAM_RUNTIME_LOG_MAX_SEGMENTS, 100);
  assert.equal(TELEGRAM_RUNTIME_LOG_MAX_TOTAL_BYTES, 100 * 1024 * 1024);
});

test("Runtime log writes boundary and redacted event records to a private instance segment", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-segment-"));
  try {
    const base = join(dir, "logs.work.jsonl");
    const log = createTelegramRuntimeJsonlLog({
      path: base,
      instanceId: "instance-a",
      writerGeneration: "generation-a",
    });
    log.reset("extension-start", { profile: "work" });
    log.record({ at: 2, category: "api", message: "failed", details: { botToken: "secret", phase: "test" } });
    const [segment] = await waitForSegments(dir);
    const lines = await readLines(segment!);
    assert.equal(lines[0]?.kind, "boundary");
    assert.ok(lines.some((line) => line.boundary === "extension-start"));
    assert.deepEqual(lines.at(-1)?.details, { botToken: "<redacted>", phase: "test" });
    if (process.platform !== "win32") {
      assert.equal((await stat(dir)).mode & 0o777, 0o700);
      assert.equal((await stat(segment!)).mode & 0o777, 0o600);
    }
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Runtime rotations use unique append-only names", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-rotate-"));
  try {
    const log = createTelegramRuntimeJsonlLog({
      path: join(dir, "logs.jsonl"), instanceId: "a", writerGeneration: "g",
      maxBytes: 180,
    });
    for (let index = 0; index < 8; index += 1) {
      log.record({ at: index, category: "rotate", message: `event-${index}-${"x".repeat(70)}` });
    }
    const segments = await waitForSegments(dir, 3);
    assert.equal(new Set(segments).size, segments.length);
    assert.equal(existsSync(join(dir, "logs.jsonl")), false);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Concurrent writers keep separate generation segments without lost events", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-concurrent-"));
  const base = join(dir, "logs.jsonl");
  try {
    const writers = ["a", "b"].map((worker) => ({
      worker,
      log: createTelegramRuntimeJsonlLog({
        path: base,
        instanceId: worker,
        writerGeneration: worker,
      }),
    }));
    for (let index = 0; index < 30; index += 1) {
      for (const { worker, log } of writers) {
        log.record({ at: index, category: worker, message: String(index) });
      }
    }
    const segments = await waitForSegments(dir, 2);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const lines = (await Promise.all(segments.map(readLines))).flat();
    for (const worker of ["a", "b"]) {
      assert.deepEqual(
        lines.filter((line) => line.category === worker).map((line) => line.message),
        Array.from({ length: 30 }, (_, index) => String(index)),
      );
    }
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Retention reclassifies stale live leases and never deletes live segments", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-live-"));
  try {
    const base = join(dir, "logs.jsonl");
    const live = join(dir, "logs.segment-old-old-000000-id.jsonl");
    await writeFile(live, "{}\n", { mode: 0o600 });
    await writeFile(`${live}.lease.json`, JSON.stringify({ version: 1, pid: 44, instanceId: "old", writerGeneration: "old", heartbeatAt: 0 }));
    const log = createTelegramRuntimeJsonlLog({ path: base, instanceId: "new", writerGeneration: "new", getNowMs: () => TELEGRAM_RUNTIME_LOG_RETENTION_MS + 10, isProcessAlive: (pid) => pid === 44, maxAgeMs: 1, maxSegments: 1 });
    log.record({ at: 1, category: "cap", message: "dropped" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(existsSync(live), true);
    assert.equal(log.getIncident()?.kind, "diagnostics-overflow");
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Retention removes dead expired segments before admitting a replacement", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-dead-"));
  try {
    const base = join(dir, "logs.jsonl");
    const dead = join(dir, "logs.segment-old-old-000000-id.jsonl");
    await writeFile(dead, "{}\n");
    await chmod(dead, 0o600);
    await writeFile(`${dead}.lease.json`, JSON.stringify({ version: 1, pid: 55, instanceId: "old", writerGeneration: "old", heartbeatAt: 0 }));
    const log = createTelegramRuntimeJsonlLog({ path: base, instanceId: "new", writerGeneration: "new", getNowMs: () => TELEGRAM_RUNTIME_LOG_RETENTION_MS + 10, isProcessAlive: () => false, maxAgeMs: 1, maxSegments: 1 });
    log.record({ at: 1, category: "new", message: "kept" });
    await waitForSegments(dir, 1);
    assert.equal(existsSync(dead), false);
    assert.equal(log.getIncident(), undefined);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Live cap overflow drops new diagnostics and retains an in-memory incident", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-overflow-"));
  try {
    const log = createTelegramRuntimeJsonlLog({
      path: join(dir, "logs.jsonl"), instanceId: "a", writerGeneration: "g",
      maxBytes: 300, maxSegments: 1, maxTotalBytes: 400,
      isProcessAlive: () => true,
    });
    log.record({ at: 1, category: "cap", message: "x".repeat(80) });
    await waitForSegments(dir);
    log.record({ at: 2, category: "cap", message: "y".repeat(200) });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(log.getIncident()?.kind, "diagnostics-overflow");
    assert.equal(log.getIncident()?.dropped, 1);
    assert.equal((await waitForSegments(dir)).length, 1);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
