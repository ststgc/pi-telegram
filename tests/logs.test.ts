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

import { runNodeEval } from "./fixtures/node-eval.ts";
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

test("Real cross-process writers keep separate segments without lost events", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-process-writers-"));
  const base = join(dir, "logs.jsonl");
  const startPath = join(dir, "start");
  const moduleUrl = new URL("../lib/logs.ts", import.meta.url).href;
  const children = ["a", "b"].map((worker) => {
    const readyPath = join(dir, `ready-${worker}`);
    const source = `
      import { existsSync, writeFileSync } from "node:fs";
      import { createTelegramRuntimeJsonlLog } from ${JSON.stringify(moduleUrl)};
      const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
      const log = createTelegramRuntimeJsonlLog({
        path: process.env.LOG_PATH,
        instanceId: process.env.WORKER,
      });
      writeFileSync(process.env.READY_PATH, "ready");
      while (!existsSync(process.env.START_PATH)) sleep(2);
      for (let index = 0; index < 25; index += 1) {
        log.record({ at: index, category: process.env.WORKER, message: String(index) });
      }
      await log.retireGeneration("process-exit");
    `;
    const done = runNodeEval(source, {
      env: {
        LOG_PATH: base,
        READY_PATH: readyPath,
        START_PATH: startPath,
        WORKER: worker,
      },
    }).then(({ code, stderr }) => {
      if (code !== 0) throw new Error(`log child exited ${code}: ${stderr}`);
    });
    return { readyPath, done };
  });
  try {
    const deadline = Date.now() + 3_000;
    while (
      !children.every((child) => existsSync(child.readyPath)) &&
      Date.now() < deadline
    ) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(children.every((child) => existsSync(child.readyPath)), true);
    await writeFile(startPath, "start");
    await Promise.all(children.map((child) => child.done));
    const segments = await waitForSegments(dir, 2);
    const lines = (await Promise.all(segments.map(readLines))).flat();
    for (const worker of ["a", "b"]) {
      assert.deepEqual(
        lines
          .filter((line) => line.category === worker)
          .map((line) => line.message),
        Array.from({ length: 25 }, (_, index) => String(index)),
      );
    }
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
    await writeFile(`${live}.lease.json`, JSON.stringify({ version: 2, pid: 44, processGeneration: "foreign", instanceId: "old", writerGeneration: "old", createdAt: 0, heartbeatAt: 0 }));
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
    await writeFile(`${dead}.lease.json`, JSON.stringify({ version: 2, pid: 55, processGeneration: "foreign", instanceId: "old", writerGeneration: "old", createdAt: 0, heartbeatAt: 0 }));
    const log = createTelegramRuntimeJsonlLog({ path: base, instanceId: "new", writerGeneration: "new", getNowMs: () => TELEGRAM_RUNTIME_LOG_RETENTION_MS + 10, isProcessAlive: () => false, maxAgeMs: 1, maxSegments: 1 });
    log.record({ at: 1, category: "new", message: "kept" });
    await waitForSegments(dir, 1);
    assert.equal(existsSync(dead), false);
    assert.equal(log.getIncident(), undefined);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Writer retirement and age rotation prevent stale live low-volume segments", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-writer-lifecycle-"));
  let now = 1_000;
  try {
    const base = join(dir, "logs.jsonl");
    const old = createTelegramRuntimeJsonlLog({
      path: base,
      instanceId: "same-process",
      writerGeneration: "old",
      processGeneration: "process-generation",
      getNowMs: () => now,
      isProcessAlive: () => true,
      maxAgeMs: 100,
      maxSegments: 2,
    });
    old.record({ at: now, category: "old", message: "old" });
    await waitForSegments(dir);
    await old.retireGeneration("session-shutdown");

    now += 200;
    const current = createTelegramRuntimeJsonlLog({
      path: base,
      instanceId: "same-process",
      writerGeneration: "new",
      processGeneration: "process-generation",
      getNowMs: () => now,
      isProcessAlive: () => true,
      maxAgeMs: 100,
      maxSegments: 2,
    });
    current.record({ at: now, category: "new", message: "new" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    const segments = await waitForSegments(dir);
    assert.equal(segments.length, 1);
    assert.equal((await readLines(segments[0]!)).at(-1)?.message, "new");
    await current.retireGeneration("session-shutdown");
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Low-volume active writer rotates by immutable segment age", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-age-rotation-"));
  let now = 1_000;
  try {
    const log = createTelegramRuntimeJsonlLog({
      path: join(dir, "logs.jsonl"),
      instanceId: "writer",
      writerGeneration: "generation",
      getNowMs: () => now,
      maxAgeMs: 100,
      maxSegments: 2,
    });
    log.record({ at: now, category: "age", message: "first" });
    await waitForSegments(dir);
    now += 101;
    log.record({ at: now, category: "age", message: "second" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    const segments = await waitForSegments(dir);
    assert.equal(segments.length, 1);
    const lines = await readLines(segments[0]!);
    assert.equal(lines.some((line) => line.boundary === "age-rotation"), true);
    assert.equal(lines.at(-1)?.message, "second");
    await log.retireGeneration("session-shutdown");
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Lease removal failure retains the segment in quota and fails closed", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-lease-remove-"));
  try {
    const base = join(dir, "logs.jsonl");
    const retained = join(dir, "logs.segment-old-old-000000-id.jsonl");
    await writeFile(retained, "{}\n", { mode: 0o600 });
    await writeFile(
      `${retained}.lease.json`,
      JSON.stringify({
        version: 2,
        pid: 55,
        processGeneration: "foreign",
        instanceId: "old",
        writerGeneration: "old",
        createdAt: 0,
        heartbeatAt: 0,
      }),
    );
    const log = createTelegramRuntimeJsonlLog({
      path: base,
      instanceId: "new",
      writerGeneration: "new",
      getNowMs: () => 1_000,
      isProcessAlive: () => false,
      maxAgeMs: 1,
      maxSegments: 1,
      removeFile(path) {
        if (path.endsWith(".lease.json")) throw new Error("lease busy");
        rm(path, { force: true }).catch(() => {});
      },
    });
    log.record({ at: 1_000, category: "new", message: "must-drop" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(existsSync(retained), true);
    assert.equal(log.getIncident()?.kind, "diagnostics-write-failed");
    assert.equal((await waitForSegments(dir)).length, 1);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Live cap overflow drops new diagnostics and retains an in-memory incident", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-tg-overflow-"));
  try {
    const log = createTelegramRuntimeJsonlLog({
      path: join(dir, "logs.jsonl"), instanceId: "a", writerGeneration: "g",
      maxBytes: 500, maxSegments: 1, maxTotalBytes: 900,
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
