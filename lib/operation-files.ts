/**
 * Operation-owned private temporary file primitive
 * Zones: shared utils, filesystem, privacy
 * Creates one private file and exposes cleanup only through its owning handle.
 */

import { createHash, randomUUID } from "node:crypto";
import { chmodSync, unlinkSync } from "node:fs";
import { chmod, mkdir, readdir, unlink, writeFile } from "node:fs/promises";
import { basename, isAbsolute, join } from "node:path";

export interface TelegramOperationOwnedPrivateFile {
  readonly path: string;
  readonly fileName: string;
  cleanup(): Promise<void>;
  cleanupSync(): void;
}

const operationFilesByOwner = new WeakMap<
  object,
  readonly TelegramOperationOwnedPrivateFile[]
>();

export interface TelegramOperationOwnedPrivateFileOptions {
  directory: string;
  fileName: string;
  bytes: Uint8Array;
  operationKey: string;
  prefix?: string;
}

export interface TelegramOperationOwnedPrivateFileCleanupOptions {
  directory: string;
  operationKey: string;
  prefix?: string;
}

function assertOperationFileName(fileName: string): string {
  if (
    !fileName ||
    fileName === "." ||
    fileName === ".." ||
    isAbsolute(fileName) ||
    /^[A-Za-z]:/.test(fileName) ||
    fileName.includes("/") ||
    fileName.includes("\\") ||
    basename(fileName) !== fileName
  ) {
    throw new Error(`Invalid operation-owned filename: ${fileName}`);
  }
  return fileName;
}

function normalizePrefix(prefix: string | undefined): string {
  const value = prefix ?? "operation";
  if (!/^[a-z0-9-]+$/u.test(value)) {
    throw new Error(`Invalid operation-owned file prefix: ${value}`);
  }
  return value;
}

function getOperationFilePrefix(prefix: string, operationKey: string): string {
  if (!operationKey) throw new Error("Operation-owned file key is required");
  const digest = createHash("sha256").update(operationKey).digest("hex");
  return `.${prefix}-${digest}-`;
}

function isMissingFileError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    Reflect.get(error, "code") === "ENOENT"
  );
}

/** Claims a caller-declared operation output and returns its cleanup capability. */
export function claimTelegramOperationOwnedPrivateFile(
  path: string,
  fileName = basename(path),
): TelegramOperationOwnedPrivateFile {
  assertOperationFileName(fileName);
  try {
    chmodSync(path, 0o600);
  } catch (error) {
    if (!isMissingFileError(error)) throw error;
  }
  let cleaned = false;
  return {
    path,
    fileName,
    async cleanup(): Promise<void> {
      if (cleaned) return;
      try {
        await unlink(path);
        cleaned = true;
      } catch (error) {
        if (!isMissingFileError(error)) throw error;
        cleaned = true;
      }
    },
    cleanupSync(): void {
      if (cleaned) return;
      try {
        unlinkSync(path);
        cleaned = true;
      } catch (error) {
        if (!isMissingFileError(error)) throw error;
        cleaned = true;
      }
    },
  };
}

export function setTelegramOperationOwnedFiles(
  owner: object,
  files: readonly TelegramOperationOwnedPrivateFile[],
): void {
  if (files.length > 0) operationFilesByOwner.set(owner, [...files]);
}

export function getTelegramOperationOwnedFiles(
  owner: object,
): readonly TelegramOperationOwnedPrivateFile[] {
  return operationFilesByOwner.get(owner) ?? [];
}

/** Creates one 0600 file in a 0700 directory and returns its sole cleanup capability. */
export async function createTelegramOperationOwnedPrivateFile(
  options: TelegramOperationOwnedPrivateFileOptions,
): Promise<TelegramOperationOwnedPrivateFile> {
  const fileName = assertOperationFileName(options.fileName);
  const prefix = normalizePrefix(options.prefix);
  const operationPrefix = getOperationFilePrefix(prefix, options.operationKey);
  await mkdir(options.directory, { recursive: true, mode: 0o700 });
  await chmod(options.directory, 0o700);
  const path = join(
    options.directory,
    `${operationPrefix}${process.pid}-${randomUUID()}-${fileName}`,
  );
  await writeFile(path, options.bytes, { mode: 0o600, flag: "wx" });
  return claimTelegramOperationOwnedPrivateFile(path, fileName);
}

/** Removes only files minted for the exact operation key, including after restart. */
export async function cleanupTelegramOperationOwnedPrivateFiles(
  options: TelegramOperationOwnedPrivateFileCleanupOptions,
): Promise<void> {
  const prefix = getOperationFilePrefix(
    normalizePrefix(options.prefix),
    options.operationKey,
  );
  let entries: Array<{ isFile(): boolean; name: string }>;
  try {
    entries = await readdir(options.directory, { withFileTypes: true });
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      Reflect.get(error, "code") === "ENOENT"
    ) return;
    throw error;
  }
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.startsWith(prefix)) continue;
    await unlink(join(options.directory, entry.name)).catch((error) => {
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        Reflect.get(error, "code") === "ENOENT"
      ) return;
      throw error;
    });
  }
}
