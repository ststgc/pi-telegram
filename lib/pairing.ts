/**
 * Secure Telegram owner pairing
 * Zones: telegram pairing, cryptography, private filesystem
 * Owns local proof generation, salted verification, exact unpaired-update parsing, atomic claims, and profile-scoped attempt limiting
 */

import {
  createHash,
  randomBytes as cryptoRandomBytes,
  randomUUID,
  timingSafeEqual,
} from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname } from "node:path";
import {
  isValidTelegramAllowedUserId,
  TELEGRAM_PAIRING_EXPIRY_MS,
  type TelegramBotProfile,
  type TelegramConfigStore,
  type TelegramPairingVerifier,
} from "./config.ts";
import { withTelegramFileTransaction } from "./locks.ts";
import {
  resolveAgentDir,
  resolveTelegramProfileTempFilePath,
} from "./paths.ts";

export { TELEGRAM_PAIRING_EXPIRY_MS } from "./config.ts";
export const TELEGRAM_PAIRING_MAX_FAILED_ATTEMPTS = 5;
export const TELEGRAM_PAIRING_RESPONSE = "Pairing request received.";

const TELEGRAM_PAIRING_CODE_BYTES = 16;
const TELEGRAM_PAIRING_SALT_BYTES = 16;
const TELEGRAM_PAIRING_CODE_PATTERN = /^[0-9a-f]{32}$/u;
const TELEGRAM_PAIRING_MESSAGE_KEYS = new Set([
  "message_id",
  "message_thread_id",
  "is_topic_message",
  "from",
  "date",
  "chat",
  "text",
  "entities",
]);

export interface TelegramPairingClock {
  now: () => number;
}

export interface TelegramPairingRandom {
  bytes: (size: number) => Uint8Array;
}

export interface TelegramPairingHasher {
  hash: (code: string, salt: string) => string;
}

export interface TelegramPairingClaimInput {
  senderId: number;
  code: string;
}

export type TelegramPairingClaimResult =
  | { kind: "claimed" }
  | { kind: "rejected" }
  | { kind: "expired" }
  | { kind: "limited" }
  | { kind: "already-paired" };

export type TelegramPairingBeginResult =
  | { kind: "ready"; code: string; createdAtMs: number; expiresAtMs: number }
  | { kind: "pending"; createdAtMs: number; expiresAtMs: number }
  | { kind: "already-paired" }
  | { kind: "unavailable" };

export interface TelegramPairingRuntime {
  begin: () => Promise<TelegramPairingBeginResult>;
  getLocalInstructions: () => Promise<string | undefined>;
  claim: (input: TelegramPairingClaimInput) => Promise<TelegramPairingClaimResult>;
}

export interface TelegramPairingRuntimeOptions {
  configStore: Pick<
    TelegramConfigStore,
    "getActiveProfileName" | "getAllowedUserId" | "transactActiveProfile"
  >;
  agentDir?: string;
  attemptsPath?: (profileName: string | undefined) => string;
  clock?: TelegramPairingClock;
  random?: TelegramPairingRandom;
  hasher?: TelegramPairingHasher;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
}

interface TelegramPairingAttemptEntry {
  senderId: number;
  timestamps: number[];
  expiresAtMs: number;
}

interface TelegramPairingAttemptState {
  version: 1;
  entries: TelegramPairingAttemptEntry[];
}

interface TelegramLocalPairingProof {
  code: string;
  pairing: TelegramPairingVerifier;
}

export interface TelegramPairingCandidate {
  senderId: number;
  chatId: number;
  messageId: number;
  threadId?: number;
  code: string;
}

interface TelegramPairingMessageLike {
  message_id?: unknown;
  message_thread_id?: unknown;
  from?: { id?: unknown; is_bot?: unknown };
  chat?: { id?: unknown; type?: unknown };
  text?: unknown;
  [key: string]: unknown;
}

function defaultHash(code: string, salt: string): string {
  return createHash("sha256")
    .update(salt, "utf8")
    .update("\0", "utf8")
    .update(code, "utf8")
    .digest("base64url");
}

function verifierMatches(left: string, right: string): boolean {
  const leftBytes = Buffer.from(left, "base64url");
  const rightBytes = Buffer.from(right, "base64url");
  return (
    leftBytes.length > 0 &&
    leftBytes.length === rightBytes.length &&
    timingSafeEqual(leftBytes, rightBytes)
  );
}

function isFiniteTimestamp(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function parseAttemptState(value: unknown): TelegramPairingAttemptState {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Pairing attempt state is invalid");
  }
  const state = value as Partial<TelegramPairingAttemptState>;
  if (state.version !== 1 || !Array.isArray(state.entries)) {
    throw new Error("Pairing attempt state is invalid");
  }
  const entries = state.entries.map((entry) => {
    if (
      !entry ||
      typeof entry !== "object" ||
      Array.isArray(entry) ||
      !isValidTelegramAllowedUserId(entry.senderId) ||
      !Array.isArray(entry.timestamps) ||
      !entry.timestamps.every(isFiniteTimestamp) ||
      !isFiniteTimestamp(entry.expiresAtMs)
    ) {
      throw new Error("Pairing attempt state is invalid");
    }
    return {
      senderId: entry.senderId,
      timestamps: [...entry.timestamps],
      expiresAtMs: entry.expiresAtMs,
    };
  });
  return { version: 1, entries };
}

function readAttemptState(path: string): TelegramPairingAttemptState {
  if (!existsSync(path)) return { version: 1, entries: [] };
  return parseAttemptState(JSON.parse(readFileSync(path, "utf8")));
}

function writeAttemptState(path: string, state: TelegramPairingAttemptState): void {
  const parent = dirname(path);
  mkdirSync(parent, { recursive: true, mode: 0o700 });
  chmodSync(parent, 0o700);
  const tempPath = `${path}.tmp-${process.pid}-${randomUUID()}`;
  writeFileSync(tempPath, `${JSON.stringify(state, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  chmodSync(tempPath, 0o600);
  renameSync(tempPath, path);
}

function consumePairingAttempt(
  path: string,
  senderId: number,
  nowMs: number,
): boolean {
  const parent = dirname(path);
  mkdirSync(parent, { recursive: true, mode: 0o700 });
  chmodSync(parent, 0o700);
  return withTelegramFileTransaction(`${path}.transaction`, () => {
    const state = readAttemptState(path);
    const cutoff = nowMs - TELEGRAM_PAIRING_EXPIRY_MS;
    const entries = state.entries
      .filter((entry) => entry.expiresAtMs > nowMs)
      .map((entry) => ({
        ...entry,
        timestamps: entry.timestamps.filter((timestamp) => timestamp > cutoff),
      }))
      .filter((entry) => entry.timestamps.length > 0);
    const entry = entries.find((candidate) => candidate.senderId === senderId);
    if (
      entry &&
      entry.timestamps.length >= TELEGRAM_PAIRING_MAX_FAILED_ATTEMPTS
    ) {
      if (JSON.stringify(entries) !== JSON.stringify(state.entries)) {
        writeAttemptState(path, { version: 1, entries });
      }
      return false;
    }
    if (entry) {
      entry.timestamps.push(nowMs);
      entry.expiresAtMs = nowMs + TELEGRAM_PAIRING_EXPIRY_MS;
    } else {
      entries.push({
        senderId,
        timestamps: [nowMs],
        expiresAtMs: nowMs + TELEGRAM_PAIRING_EXPIRY_MS,
      });
    }
    writeAttemptState(path, { version: 1, entries });
    return true;
  });
}

function clearPairingAttempts(path: string): void {
  const parent = dirname(path);
  mkdirSync(parent, { recursive: true, mode: 0o700 });
  chmodSync(parent, 0o700);
  withTelegramFileTransaction(`${path}.transaction`, () => {
    if (existsSync(path)) unlinkSync(path);
  });
}

function cloneProfileWithoutPairing(
  profile: TelegramBotProfile,
): TelegramBotProfile {
  const { pairing: _pairing, ...remaining } = profile;
  return remaining;
}

export function isTelegramPairingProofShapedUpdate(update: unknown): boolean {
  if (!update || typeof update !== "object" || Array.isArray(update)) {
    return false;
  }
  const message = Reflect.get(update, "message");
  return (
    Boolean(message) &&
    typeof message === "object" &&
    !Array.isArray(message) &&
    typeof Reflect.get(message, "text") === "string" &&
    /^\/start [0-9a-f]{32}$/u.test(Reflect.get(message, "text"))
  );
}

export function parseTelegramPairingCandidate(
  update: unknown,
): TelegramPairingCandidate | undefined {
  if (!update || typeof update !== "object" || Array.isArray(update)) {
    return undefined;
  }
  const updateRecord = update as Record<string, unknown>;
  if (
    !Object.keys(updateRecord).every(
      (key) => key === "update_id" || key === "message",
    )
  ) {
    return undefined;
  }
  const message = updateRecord.message as TelegramPairingMessageLike | undefined;
  if (
    !message ||
    typeof message !== "object" ||
    Array.isArray(message) ||
    !Object.keys(message).every((key) => TELEGRAM_PAIRING_MESSAGE_KEYS.has(key)) ||
    message.chat?.type !== "private" ||
    !isValidTelegramAllowedUserId(message.chat.id as number | undefined) ||
    message.from?.is_bot !== false ||
    !isValidTelegramAllowedUserId(message.from.id as number | undefined) ||
    !Number.isSafeInteger(message.message_id) ||
    (message.message_id as number) <= 0 ||
    typeof message.text !== "string"
  ) {
    return undefined;
  }
  const match = /^\/start ([0-9a-f]{32})$/u.exec(message.text);
  if (!match || !TELEGRAM_PAIRING_CODE_PATTERN.test(match[1] ?? "")) {
    return undefined;
  }
  const threadId = message.message_thread_id;
  const isThreadedMessage = message.is_topic_message === true;
  if (
    (threadId !== undefined &&
      (!Number.isSafeInteger(threadId) || (threadId as number) <= 0)) ||
    (threadId !== undefined) !== isThreadedMessage ||
    (message.is_topic_message !== undefined && !isThreadedMessage)
  ) {
    return undefined;
  }
  return {
    senderId: message.from.id as number,
    chatId: message.chat.id as number,
    messageId: message.message_id as number,
    ...(typeof threadId === "number" ? { threadId } : {}),
    code: match[1]!,
  };
}

export function createTelegramPairingRuntime(
  options: TelegramPairingRuntimeOptions,
): TelegramPairingRuntime {
  const clock = options.clock ?? { now: () => Date.now() };
  const random = options.random ?? { bytes: (size: number) => cryptoRandomBytes(size) };
  const hasher = options.hasher ?? { hash: defaultHash };
  const agentDir = options.agentDir ?? resolveAgentDir();
  const localProofs = new Map<string, TelegramLocalPairingProof>();
  const getProfileKey = () =>
    options.configStore.getActiveProfileName() ?? "default";
  const getAttemptsPath = () => {
    const profileName = options.configStore.getActiveProfileName();
    return options.attemptsPath
      ? options.attemptsPath(profileName)
      : resolveTelegramProfileTempFilePath(
          "pairing-attempts",
          "json",
          agentDir,
          profileName,
        );
  };
  const recordFailure = (phase: string, error: unknown) => {
    try {
      options.recordRuntimeEvent?.("pairing", error, { phase });
    } catch {
      // Diagnostics must never turn a rejected pairing operation into poll failure.
    }
  };

  const begin = async (): Promise<TelegramPairingBeginResult> => {
    const profileKey = getProfileKey();
    if (isValidTelegramAllowedUserId(options.configStore.getAllowedUserId())) {
      localProofs.delete(profileKey);
      return { kind: "already-paired" };
    }
    return options.configStore.transactActiveProfile<TelegramPairingBeginResult>((profile) => {
      if (!profile?.botToken) {
        return { profile, result: { kind: "unavailable" } as const };
      }
      if (isValidTelegramAllowedUserId(profile.allowedUserId)) {
        localProofs.delete(profileKey);
        return { profile, result: { kind: "already-paired" } as const };
      }
      const nowMs = clock.now();
      const existing = profile.pairing;
      if (existing && existing.expiresAtMs > nowMs) {
        const cached = localProofs.get(profileKey);
        if (
          cached &&
          cached.pairing.verifier === existing.verifier &&
          cached.pairing.salt === existing.salt &&
          cached.pairing.createdAtMs === existing.createdAtMs &&
          cached.pairing.expiresAtMs === existing.expiresAtMs &&
          verifierMatches(hasher.hash(cached.code, existing.salt), existing.verifier)
        ) {
          return {
            profile,
            result: {
              kind: "ready",
              code: cached.code,
              createdAtMs: existing.createdAtMs,
              expiresAtMs: existing.expiresAtMs,
            } as const,
          };
        }
        return {
          profile,
          result: {
            kind: "pending",
            createdAtMs: existing.createdAtMs,
            expiresAtMs: existing.expiresAtMs,
          } as const,
        };
      }
      const codeBytes = random.bytes(TELEGRAM_PAIRING_CODE_BYTES);
      const saltBytes = random.bytes(TELEGRAM_PAIRING_SALT_BYTES);
      if (
        codeBytes.byteLength !== TELEGRAM_PAIRING_CODE_BYTES ||
        saltBytes.byteLength !== TELEGRAM_PAIRING_SALT_BYTES
      ) {
        throw new Error("Pairing randomness source returned an invalid length");
      }
      const code = Buffer.from(codeBytes).toString("hex");
      const salt = Buffer.from(saltBytes).toString("base64url");
      const createdAtMs = nowMs;
      const expiresAtMs = createdAtMs + TELEGRAM_PAIRING_EXPIRY_MS;
      const pairing = {
        verifier: hasher.hash(code, salt),
        salt,
        createdAtMs,
        expiresAtMs,
      };
      localProofs.set(profileKey, { code, pairing });
      return {
        profile: { ...profile, pairing },
        result: { kind: "ready", code, createdAtMs, expiresAtMs } as const,
      };
    });
  };

  return {
    begin,
    async getLocalInstructions() {
      const result = await begin();
      if (result.kind === "pending") {
        return "Pairing is already pending. Use the code shown by the Pi instance that created it, or wait for it to expire.";
      }
      if (result.kind !== "ready") return undefined;
      return `Pairing code: ${result.code}\nSend /start ${result.code} to the bot within 10 minutes.`;
    },
    async claim(input) {
      if (
        !isValidTelegramAllowedUserId(input.senderId) ||
        !TELEGRAM_PAIRING_CODE_PATTERN.test(input.code)
      ) {
        return { kind: "rejected" };
      }
      const attemptsPath = getAttemptsPath();
      const nowMs = clock.now();
      try {
        if (!consumePairingAttempt(attemptsPath, input.senderId, nowMs)) {
          return { kind: "limited" };
        }
      } catch (error) {
        recordFailure("attempt-limit", error);
        return { kind: "limited" };
      }
      let result: TelegramPairingClaimResult;
      try {
        result = await options.configStore.transactActiveProfile<TelegramPairingClaimResult>((profile) => {
          if (!profile?.botToken) {
            return { profile, result: { kind: "rejected" } as const };
          }
          if (isValidTelegramAllowedUserId(profile.allowedUserId)) {
            return { profile, result: { kind: "already-paired" } as const };
          }
          const pairing = profile.pairing;
          if (!pairing) {
            return { profile, result: { kind: "rejected" } as const };
          }
          if (pairing.expiresAtMs <= nowMs) {
            return {
              profile: cloneProfileWithoutPairing(profile),
              result: { kind: "expired" } as const,
            };
          }
          const candidateVerifier = hasher.hash(input.code, pairing.salt);
          if (!verifierMatches(candidateVerifier, pairing.verifier)) {
            return { profile, result: { kind: "rejected" } as const };
          }
          return {
            profile: {
              ...cloneProfileWithoutPairing(profile),
              allowedUserId: input.senderId,
            },
            result: { kind: "claimed" } as const,
          };
        });
      } catch (error) {
        recordFailure("claim-transaction", error);
        return { kind: "rejected" };
      }
      if (
        result.kind === "claimed" ||
        result.kind === "expired" ||
        result.kind === "already-paired"
      ) {
        localProofs.delete(getProfileKey());
        try {
          clearPairingAttempts(attemptsPath);
        } catch (error) {
          recordFailure("attempt-cleanup", error);
        }
      }
      return result;
    },
  };
}
