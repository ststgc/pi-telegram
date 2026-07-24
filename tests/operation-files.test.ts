/**
 * Regression tests for operation-owned private temporary files
 * Zones: shared utils, filesystem, test infrastructure
 * Guards private creation and capability-scoped cleanup.
 */

import assert from "node:assert/strict";
import { access, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createTelegramOperationOwnedPrivateFile } from "../lib/operation-files.ts";

test("operation-owned private files preserve bytes and clean only their own path", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-telegram-operation-file-"));
  const neighbor = join(directory, "neighbor.txt");
  await writeFile(neighbor, "keep");
  try {
    const owned = await createTelegramOperationOwnedPrivateFile({
      directory,
      fileName: "full-response.md",
      bytes: new TextEncoder().encode("complete **Markdown**"),
      operationKey: "turn-operation-file",
      prefix: "guest-response",
    });
    assert.equal(owned.fileName, "full-response.md");
    assert.equal(await readFile(owned.path, "utf8"), "complete **Markdown**");
    if (process.platform !== "win32") {
      assert.equal((await stat(owned.path)).mode & 0o777, 0o600);
    }

    await owned.cleanup();
    await owned.cleanup();
    await assert.rejects(access(owned.path));
    assert.equal(await readFile(neighbor, "utf8"), "keep");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
