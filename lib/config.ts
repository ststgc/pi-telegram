/**
 * Telegram bridge configuration helpers
 * Zones: telegram config, filesystem
 * Owns persisted bot/session state, local config storage, live config controls, authorization policy, and serialized profile transactions
 */

import { randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { chmod, mkdir, rename, writeFile } from "node:fs/promises";
import {
  resolveAgentDir,
  resolveTelegramConfigPath,
  TELEGRAM_DEFAULT_PROFILE_NAME,
} from "./paths.ts";
export { TELEGRAM_DEFAULT_PROFILE_NAME } from "./paths.ts";

import type { CommandTemplateObjectConfig } from "./command-templates.ts";
import type { TelegramInboundHandlerConfig } from "./inbound.ts";
import { withTelegramFileTransaction } from "./locks.ts";

const CONFIG_RUNTIME_KEY = "__piTelegramConfigRuntime__";

function getConfigPath(): string {
  return resolveTelegramConfigPath();
}

export type TelegramOutboundCommandTemplateConfig =
  string | CommandTemplateObjectConfig;
export interface TelegramOutboundHandlerConfig extends CommandTemplateObjectConfig {
  type?: string;
  match?: string | string[];
  output?: string;
  timeout?: number | string;
}

export type TelegramTimeMode = "hidden" | "always" | "interval";

export interface TelegramTimeConfig {
  injectionMode?: TelegramTimeMode;
  interval?: number;
}

export interface ResolvedTelegramTimeConfig {
  injectionMode: TelegramTimeMode;
  interval: number;
  timezone: string;
}

export type TelegramAssistantRenderingMode = "rich" | "html";

export interface TelegramConfig {
  /** @deprecated persisted identity belongs in profiles.default; retained for effective/legacy views */
  botToken?: string;
  /** @deprecated persisted identity belongs in profiles.default; retained for effective/legacy views */
  botUsername?: string;
  /** @deprecated persisted identity belongs in profiles.default; retained for effective/legacy views */
  botId?: number;
  /** @deprecated persisted identity belongs in profiles.default; retained for effective/legacy views */
  allowedUserId?: number;
  /** @deprecated persisted identity belongs in profiles.default; retained for effective/legacy views */
  lastUpdateId?: number;
  /** Effective runtime view only; persisted under the active profile. */
  pairing?: TelegramPairingVerifier;
  inboundHandlers?: TelegramInboundHandlerConfig[];
  attachmentHandlers?: TelegramInboundHandlerConfig[];
  outboundHandlers?: TelegramOutboundHandlerConfig[];
  assistant?: {
    draftPreviews?: boolean;
    rendering?: TelegramAssistantRenderingMode;
    proactivePush?: boolean;
  };
  /** @deprecated use assistant.draftPreviews */
  draftPreviews?: boolean;
  /** @deprecated use assistant.draftPreviews */
  richDraftPreviews?: boolean;
  /** @deprecated use assistant.rendering */
  assistantRendering?: TelegramAssistantRenderingMode;
  voice?: {
    replyMode?: "hidden" | "mirror" | "always";
    /** Whether to attach the provider's transcriptText as caption on voice messages */
    sendTranscript?: boolean;
  };
  time?: TelegramTimeConfig;
  /** Canonical bot/session profiles, including profiles.default. */
  profiles?: Record<string, TelegramBotProfile>;
}

/**
 * Per-profile bot/session identity fields.
 * Stored under `profiles.<name>` in telegram.json.
 * Shared bridge settings (inboundHandlers, outboundHandlers, voice, time,
 * assistant) stay at the top level.
 */
export const TELEGRAM_PAIRING_EXPIRY_MS = 10 * 60 * 1000;

export interface TelegramPairingVerifier {
  verifier: string;
  salt: string;
  createdAtMs: number;
  expiresAtMs: number;
}

export interface TelegramBotProfile {
  botToken: string;
  botUsername?: string;
  botId?: number;
  allowedUserId?: number;
  lastUpdateId?: number;
  /** Runtime-managed proof verifier. The raw pairing code is never persisted. */
  pairing?: TelegramPairingVerifier;
}

/** Profile names must contain only lowercase ASCII letters and digits; max 32 chars. */
const TELEGRAM_PROFILE_NAME_PATTERN = /^[a-z0-9]{1,32}$/;
const TELEGRAM_RESERVED_PROFILE_NAMES: ReadonlySet<string> = new Set([
  "main",
  "active",
]);

export function isValidTelegramProfileName(name: string): boolean {
  return (
    TELEGRAM_PROFILE_NAME_PATTERN.test(name) &&
    !TELEGRAM_RESERVED_PROFILE_NAMES.has(name)
  );
}

/** List defined profile names. */
export function getTelegramProfileNames(config: TelegramConfig): string[] {
  return Object.keys(config.profiles ?? {}).sort();
}

export interface TelegramConfigStore {
  get: () => TelegramConfig;
  getStoredConfig: () => TelegramConfig;
  set: (config: TelegramConfig) => void;
  setProfile: (profileName: string, profile: TelegramBotProfile) => void;
  update: (mutate: (config: TelegramConfig) => void) => void;
  activateProfile: (profileName: string | undefined) => boolean;
  getActiveProfileName: () => string | undefined;
  getBotToken: () => string | undefined;
  hasBotToken: () => boolean;
  getAllowedUserId: () => number | undefined;
  getInboundHandlers: () => TelegramInboundHandlerConfig[] | undefined;
  getAttachmentHandlers: () => TelegramInboundHandlerConfig[] | undefined;
  getOutboundHandlers: () => TelegramOutboundHandlerConfig[] | undefined;
  setAllowedUserId: (userId: number) => void;
  transactActiveProfile: <T>(
    mutate: (
      profile: TelegramBotProfile | undefined,
    ) => { profile: TelegramBotProfile | undefined; result: T },
  ) => Promise<T>;
  load: () => Promise<void>;
  persist: (config?: TelegramConfig) => Promise<void>;
}

export interface TelegramConfigStoreOptions {
  initialConfig?: TelegramConfig;
  agentDir?: string;
  configPath?: string;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
}

export interface TelegramInvalidConfigRecovery {
  configPath: string;
  recoveryPath: string;
  error: unknown;
}

export interface TelegramConfigRuntime {
  updateVoiceConfig: (voice: NonNullable<TelegramConfig["voice"]>) => void;
}

export function setGlobalTelegramConfigRuntime(
  runtime: TelegramConfigRuntime | undefined,
): void {
  const globals = globalThis as Record<string, unknown>;
  if (runtime) globals[CONFIG_RUNTIME_KEY] = runtime;
  else delete globals[CONFIG_RUNTIME_KEY];
}

export function updateTelegramVoiceConfig(
  voice: NonNullable<TelegramConfig["voice"]>,
): boolean {
  const runtime = (globalThis as Record<string, unknown>)[
    CONFIG_RUNTIME_KEY
  ] as TelegramConfigRuntime | undefined;
  if (!runtime || typeof runtime.updateVoiceConfig !== "function") return false;
  runtime.updateVoiceConfig(voice);
  return true;
}

type TelegramMutableConfigStore = Pick<
  TelegramConfigStore,
  "get" | "set" | "persist"
> & {
  load?: () => Promise<void>;
};

function isEmptyTelegramConfig(config: TelegramConfig): boolean {
  return Object.keys(config).length === 0;
}

async function loadLatestTelegramConfig(
  configStore: TelegramMutableConfigStore,
): Promise<void> {
  if (!configStore.load) return;
  const before = configStore.get();
  await configStore.load();
  if (
    !isEmptyTelegramConfig(before) &&
    isEmptyTelegramConfig(configStore.get())
  ) {
    configStore.set(before);
  }
}

export function bindGlobalTelegramConfigRuntime(
  configStore: TelegramMutableConfigStore,
): void {
  setGlobalTelegramConfigRuntime({
    updateVoiceConfig(voice) {
      const current = configStore.get();
      const next = {
        ...current,
        voice: { ...(current.voice ?? {}), ...voice },
      };
      configStore.set(next);
      void configStore.persist(next);
    },
  });
}

function getInvalidTelegramConfigRecoveryPath(configPath: string): string {
  return `${configPath}.invalid-${process.pid}-${Date.now()}`;
}

const TELEGRAM_PAIRING_VERIFIER_PATTERN = /^[A-Za-z0-9_-]{43}$/u;
const TELEGRAM_PAIRING_SALT_PATTERN = /^[A-Za-z0-9_-]{22}$/u;
const TELEGRAM_PAIRING_KEYS = new Set([
  "verifier",
  "salt",
  "createdAtMs",
  "expiresAtMs",
]);

function isCanonicalBase64Url(
  value: unknown,
  pattern: RegExp,
  expectedBytes: number,
): value is string {
  if (typeof value !== "string" || !pattern.test(value)) return false;
  const decoded = Buffer.from(value, "base64url");
  return (
    decoded.byteLength === expectedBytes &&
    decoded.toString("base64url") === value
  );
}

function validateTelegramPairingVerifier(
  value: unknown,
  location: string,
): void {
  if (!isPlainConfigRecord(value)) {
    throw new Error(`Invalid Telegram pairing verifier at ${location}`);
  }
  if (
    !Object.keys(value).every((key) => TELEGRAM_PAIRING_KEYS.has(key)) ||
    Object.keys(value).length !== TELEGRAM_PAIRING_KEYS.size ||
    !isCanonicalBase64Url(value.verifier, TELEGRAM_PAIRING_VERIFIER_PATTERN, 32) ||
    !isCanonicalBase64Url(value.salt, TELEGRAM_PAIRING_SALT_PATTERN, 16) ||
    !Number.isSafeInteger(value.createdAtMs) ||
    (value.createdAtMs as number) < 0 ||
    !Number.isSafeInteger(value.expiresAtMs) ||
    (value.expiresAtMs as number) - (value.createdAtMs as number) !==
      TELEGRAM_PAIRING_EXPIRY_MS
  ) {
    throw new Error(`Invalid Telegram pairing verifier at ${location}`);
  }
}

function parseTelegramConfigContent(
  content: string,
  configPath: string,
): TelegramConfig {
  const parsed: unknown = JSON.parse(content);
  if (!isPlainConfigRecord(parsed)) {
    throw new Error(`Invalid Telegram config object: ${configPath}`);
  }
  if (Object.hasOwn(parsed, "pairing")) {
    validateTelegramPairingVerifier(parsed.pairing, "root");
  }
  if (isPlainConfigRecord(parsed.profiles)) {
    for (const [profileName, profile] of Object.entries(parsed.profiles)) {
      if (isPlainConfigRecord(profile) && Object.hasOwn(profile, "pairing")) {
        validateTelegramPairingVerifier(
          profile.pairing,
          `profiles.${profileName}`,
        );
      }
    }
  }
  return parsed as TelegramConfig;
}

export async function readTelegramConfig(
  configPath: string,
  options: {
    onInvalidConfig?: (recovery: TelegramInvalidConfigRecovery) => void;
  } = {},
): Promise<TelegramConfig> {
  if (!existsSync(configPath)) return {};
  const content = readFileSync(configPath, "utf8");
  try {
    return parseTelegramConfigContent(content, configPath);
  } catch {
    // Atomic config publication makes ordinary reads safe without serialization.
    // Acquire the transaction only before destructive invalid-file recovery.
    return withTelegramFileTransaction(`${configPath}.transaction`, () => {
      if (!existsSync(configPath)) return {};
      const identity = statSync(configPath);
      const currentContent = readFileSync(configPath, "utf8");
      try {
        return parseTelegramConfigContent(currentContent, configPath);
      } catch (error) {
        const currentIdentity = statSync(configPath);
        if (
          currentIdentity.dev !== identity.dev ||
          currentIdentity.ino !== identity.ino ||
          currentIdentity.size !== identity.size ||
          currentIdentity.mtimeMs !== identity.mtimeMs
        ) {
          throw new Error(
            `Telegram config changed while validating invalid content: ${configPath}`,
            { cause: error },
          );
        }
        const recoveryPath = getInvalidTelegramConfigRecoveryPath(configPath);
        renameSync(configPath, recoveryPath);
        options.onInvalidConfig?.({ configPath, recoveryPath, error });
        return {};
      }
    });
  }
}

export async function writeTelegramConfig(
  agentDir: string,
  configPath: string,
  config: TelegramConfig,
): Promise<void> {
  await mkdir(agentDir, { recursive: true });
  const tempConfigPath = `${configPath}.tmp-${process.pid}-${Date.now()}`;
  await writeFile(tempConfigPath, JSON.stringify(config, null, "\t") + "\n", {
    encoding: "utf8",
    mode: 0o600,
  });
  await chmod(tempConfigPath, 0o600);
  await rename(tempConfigPath, configPath);
}

function isPlainConfigRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function cloneTelegramConfig<T>(value: T): T {
  return structuredClone(value);
}

function configValuesEqual(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function mergeTelegramConfigDelta(
  base: Record<string, unknown>,
  desired: Record<string, unknown>,
  latest: Record<string, unknown>,
): Record<string, unknown> {
  const merged = cloneTelegramConfig(latest);
  for (const key of new Set([...Object.keys(base), ...Object.keys(desired)])) {
    const baseHas = Object.hasOwn(base, key);
    const desiredHas = Object.hasOwn(desired, key);
    const baseValue = base[key];
    const desiredValue = desired[key];
    if (baseHas === desiredHas && configValuesEqual(baseValue, desiredValue)) {
      continue;
    }
    if (!desiredHas) {
      if (key !== "lastUpdateId") delete merged[key];
      continue;
    }
    if (
      key === "lastUpdateId" &&
      typeof desiredValue === "number" &&
      typeof merged[key] === "number"
    ) {
      merged[key] = Math.max(desiredValue, merged[key] as number);
      continue;
    }
    if (
      isPlainConfigRecord(desiredValue) &&
      (!baseHas || isPlainConfigRecord(baseValue))
    ) {
      merged[key] = mergeTelegramConfigDelta(
        isPlainConfigRecord(baseValue) ? baseValue : {},
        desiredValue,
        isPlainConfigRecord(merged[key]) ? merged[key] : {},
      );
      continue;
    }
    merged[key] = cloneTelegramConfig(desiredValue);
  }
  return merged;
}

function readTelegramConfigForTransaction(configPath: string): TelegramConfig {
  if (!existsSync(configPath)) return {};
  return parseTelegramConfigContent(readFileSync(configPath, "utf8"), configPath);
}

function writeTelegramConfigInTransaction(
  agentDir: string,
  configPath: string,
  config: TelegramConfig,
): void {
  mkdirSync(agentDir, { recursive: true, mode: 0o700 });
  const tempConfigPath = `${configPath}.tmp-${process.pid}-${randomUUID()}`;
  writeFileSync(tempConfigPath, `${JSON.stringify(config, null, "\t")}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  chmodSync(tempConfigPath, 0o600);
  renameSync(tempConfigPath, configPath);
}

export function getTelegramProfileFields(
  config: TelegramConfig,
): TelegramBotProfile | undefined {
  const token = config.botToken?.trim();
  if (!token) return undefined;
  return {
    botToken: token,
    ...(config.botUsername !== undefined
      ? { botUsername: config.botUsername }
      : {}),
    ...(config.botId !== undefined ? { botId: config.botId } : {}),
    ...(config.allowedUserId !== undefined
      ? { allowedUserId: config.allowedUserId }
      : {}),
    ...(config.lastUpdateId !== undefined
      ? { lastUpdateId: config.lastUpdateId }
      : {}),
    ...(config.pairing !== undefined
      ? { pairing: cloneTelegramConfig(config.pairing) }
      : {}),
  };
}

function omitTelegramRootProfileFields(config: TelegramConfig): TelegramConfig {
  const {
    botToken: _botToken,
    botUsername: _botUsername,
    botId: _botId,
    allowedUserId: _allowedUserId,
    lastUpdateId: _lastUpdateId,
    pairing: _pairing,
    ...sharedConfig
  } = config;
  return sharedConfig;
}

export function normalizeTelegramDefaultProfileConfig(config: TelegramConfig): {
  config: TelegramConfig;
  changed: boolean;
} {
  const hasLegacyRootProfile = [
    "botToken",
    "botUsername",
    "botId",
    "allowedUserId",
    "lastUpdateId",
  ].some((field) => Object.hasOwn(config, field));
  if (!hasLegacyRootProfile) return { config, changed: false };
  const canonicalProfile = config.profiles?.[TELEGRAM_DEFAULT_PROFILE_NAME];
  const legacyToken = config.botToken?.trim();
  if (Object.hasOwn(config, "botToken") && !legacyToken) {
    throw new Error("Legacy Telegram default profile has no bot token");
  }
  const legacyProfile: Partial<TelegramBotProfile> = {
    ...(legacyToken ? { botToken: legacyToken } : {}),
    ...(config.botUsername !== undefined
      ? { botUsername: config.botUsername }
      : {}),
    ...(config.botId !== undefined ? { botId: config.botId } : {}),
    ...(config.allowedUserId !== undefined
      ? { allowedUserId: config.allowedUserId }
      : {}),
    ...(config.lastUpdateId !== undefined
      ? { lastUpdateId: config.lastUpdateId }
      : {}),
  };
  if (!canonicalProfile && !legacyToken) {
    throw new Error("Legacy Telegram default profile has no bot token");
  }
  const hasConflict = canonicalProfile
    ? Object.entries(legacyProfile).some(
        ([field, value]) =>
          Object.hasOwn(canonicalProfile, field) &&
          !configValuesEqual(
            canonicalProfile[field as keyof TelegramBotProfile],
            value,
          ),
      )
    : false;
  if (hasConflict) {
    throw new Error(
      "Conflicting Telegram default profile identity at root and profiles.default",
    );
  }
  const normalizedProfile: TelegramBotProfile = canonicalProfile
    ? { ...legacyProfile, ...canonicalProfile }
    : (legacyProfile as TelegramBotProfile);
  return {
    config: {
      ...omitTelegramRootProfileFields(config),
      profiles: {
        ...(config.profiles ?? {}),
        [TELEGRAM_DEFAULT_PROFILE_NAME]: normalizedProfile,
      },
    },
    changed: true,
  };
}

function applyTelegramProfile(
  config: TelegramConfig,
  profileName: string | undefined,
): TelegramConfig {
  const effectiveProfileName = profileName ?? TELEGRAM_DEFAULT_PROFILE_NAME;
  const profile = config.profiles?.[effectiveProfileName];
  if (!profile) return omitTelegramRootProfileFields(config);
  return {
    ...omitTelegramRootProfileFields(config),
    ...profile,
  };
}

function storeTelegramEffectiveConfig(
  baseConfig: TelegramConfig,
  nextConfig: TelegramConfig,
  profileName: string | undefined,
): TelegramConfig {
  const effectiveProfileName = profileName ?? TELEGRAM_DEFAULT_PROFILE_NAME;
  const profile = getTelegramProfileFields(nextConfig);
  const profiles = { ...(baseConfig.profiles ?? {}) };
  if (profile) profiles[effectiveProfileName] = profile;
  else delete profiles[effectiveProfileName];
  return {
    ...omitTelegramRootProfileFields(nextConfig),
    profiles: Object.keys(profiles).length > 0 ? profiles : undefined,
  };
}

export function createTelegramConfigStore(
  options: TelegramConfigStoreOptions = {},
): TelegramConfigStore {
  let config: TelegramConfig = normalizeTelegramDefaultProfileConfig(
    cloneTelegramConfig(options.initialConfig ?? {}),
  ).config;
  let persistedConfig: TelegramConfig = {};
  let mutationVersion = 0;
  let persistQueue: Promise<void> = Promise.resolve();
  let activeProfileName: string | undefined;
  const agentDir = options.agentDir ?? resolveAgentDir();
  const configPath = options.configPath ?? getConfigPath();
  const getEffectiveConfig = () =>
    applyTelegramProfile(config, activeProfileName);
  const setEffectiveConfig = (nextConfig: TelegramConfig) => {
    config = storeTelegramEffectiveConfig(
      config,
      nextConfig,
      activeProfileName,
    );
    mutationVersion += 1;
  };
  return {
    get: getEffectiveConfig,
    getStoredConfig: () => config,
    set: setEffectiveConfig,
    setProfile: (profileName, profile) => {
      config = {
        ...omitTelegramRootProfileFields(config),
        profiles: {
          ...(config.profiles ?? {}),
          [profileName]: cloneTelegramConfig(profile),
        },
      };
      mutationVersion += 1;
    },
    update: (mutate) => {
      const nextConfig = getEffectiveConfig();
      mutate(nextConfig);
      setEffectiveConfig(nextConfig);
    },
    activateProfile: (profileName) => {
      const normalizedProfileName =
        !profileName || profileName === TELEGRAM_DEFAULT_PROFILE_NAME
          ? undefined
          : profileName;
      if (normalizedProfileName && !config.profiles?.[normalizedProfileName]) {
        return false;
      }
      activeProfileName = normalizedProfileName;
      return true;
    },
    getActiveProfileName: () => activeProfileName,
    getBotToken: () => getEffectiveConfig().botToken,
    hasBotToken: () => !!getEffectiveConfig().botToken,
    getAllowedUserId: () => getEffectiveConfig().allowedUserId,
    getInboundHandlers: () => [
      ...(config.inboundHandlers ?? []),
      ...(config.attachmentHandlers ?? []),
    ],
    getAttachmentHandlers: () => config.attachmentHandlers,
    getOutboundHandlers: () => config.outboundHandlers,
    setAllowedUserId: (userId) => {
      const nextConfig = getEffectiveConfig();
      nextConfig.allowedUserId = userId;
      setEffectiveConfig(nextConfig);
    },
    transactActiveProfile: <T>(
      mutate: (
        profile: TelegramBotProfile | undefined,
      ) => { profile: TelegramBotProfile | undefined; result: T },
    ): Promise<T> => {
      const profileName = activeProfileName ?? TELEGRAM_DEFAULT_PROFILE_NAME;
      const baseConfig = cloneTelegramConfig(config);
      const capturedMutationVersion = mutationVersion;
      let transactionResult!: T;
      const transaction = persistQueue.then(() => {
        const committedConfig = withTelegramFileTransaction(
          `${configPath}.transaction`,
          () => {
            const rawConfig = readTelegramConfigForTransaction(configPath);
            const normalized = normalizeTelegramDefaultProfileConfig(rawConfig);
            const latestConfig = normalized.config;
            const currentProfile = latestConfig.profiles?.[profileName];
            const outcome = mutate(
              currentProfile
                ? cloneTelegramConfig(currentProfile)
                : undefined,
            );
            transactionResult = outcome.result;
            const profiles = { ...(latestConfig.profiles ?? {}) };
            if (outcome.profile) {
              profiles[profileName] = cloneTelegramConfig(outcome.profile);
            } else {
              delete profiles[profileName];
            }
            const nextConfig = {
              ...latestConfig,
              profiles:
                Object.keys(profiles).length > 0 ? profiles : undefined,
            };
            if (
              normalized.changed ||
              !configValuesEqual(latestConfig, nextConfig)
            ) {
              writeTelegramConfigInTransaction(agentDir, configPath, nextConfig);
            }
            return nextConfig;
          },
        );
        persistedConfig = cloneTelegramConfig(committedConfig);
        if (mutationVersion === capturedMutationVersion) {
          config = cloneTelegramConfig(committedConfig);
        } else {
          config = mergeTelegramConfigDelta(
            baseConfig as Record<string, unknown>,
            config as Record<string, unknown>,
            committedConfig as Record<string, unknown>,
          ) as TelegramConfig;
        }
        return transactionResult;
      });
      persistQueue = transaction.then(
        () => undefined,
        () => undefined,
      );
      return transaction;
    },
    load: async () => {
      const loadedConfig = await readTelegramConfig(configPath, {
        onInvalidConfig: (recovery) => {
          options.recordRuntimeEvent?.("config", recovery.error, {
            phase: "load",
            configPath: recovery.configPath,
            recoveryPath: recovery.recoveryPath,
          });
        },
      });
      let normalized: ReturnType<typeof normalizeTelegramDefaultProfileConfig>;
      try {
        normalized = normalizeTelegramDefaultProfileConfig(loadedConfig);
      } catch (error) {
        options.recordRuntimeEvent?.("config", error, {
          phase: "default-profile-normalize",
          configPath,
        });
        throw error;
      }
      config = normalized.changed
        ? withTelegramFileTransaction(`${configPath}.transaction`, () => {
            const latestConfig = readTelegramConfigForTransaction(configPath);
            const latestNormalized =
              normalizeTelegramDefaultProfileConfig(latestConfig);
            if (latestNormalized.changed) {
              writeTelegramConfigInTransaction(
                agentDir,
                configPath,
                latestNormalized.config,
              );
            }
            return latestNormalized.config;
          })
        : normalized.config;
      persistedConfig = cloneTelegramConfig(config);
      mutationVersion += 1;
    },
    persist: (nextConfig = getEffectiveConfig()) => {
      const profileName = activeProfileName;
      const desiredConfig = storeTelegramEffectiveConfig(
        config,
        cloneTelegramConfig(nextConfig),
        profileName,
      );
      const baseConfig = cloneTelegramConfig(persistedConfig);
      const capturedMutationVersion = mutationVersion;
      const persist = persistQueue.then(() => {
        const mergedConfig = withTelegramFileTransaction(
          `${configPath}.transaction`,
          () => {
            const latestConfig = readTelegramConfigForTransaction(configPath);
            const merged = mergeTelegramConfigDelta(
              baseConfig as Record<string, unknown>,
              desiredConfig as Record<string, unknown>,
              latestConfig as Record<string, unknown>,
            ) as TelegramConfig;
            if (!configValuesEqual(latestConfig, merged)) {
              writeTelegramConfigInTransaction(agentDir, configPath, merged);
            }
            return merged;
          },
        );
        persistedConfig = cloneTelegramConfig(mergedConfig);
        if (mutationVersion === capturedMutationVersion) {
          config = cloneTelegramConfig(mergedConfig);
        } else {
          config = mergeTelegramConfigDelta(
            baseConfig as Record<string, unknown>,
            config as Record<string, unknown>,
            mergedConfig as Record<string, unknown>,
          ) as TelegramConfig;
        }
      });
      persistQueue = persist.catch(() => undefined);
      return persist;
    },
  };
}

export function createTelegramPollingOffsetPersister(
  configStore: Pick<TelegramConfigStore, "get" | "persist">,
  persist: (config?: TelegramConfig) => Promise<void> = (config) =>
    configStore.persist(config),
): (pollingConfig: { lastUpdateId?: number }) => Promise<void> {
  return async (pollingConfig) => {
    const nextOffset = pollingConfig.lastUpdateId;
    if (typeof nextOffset !== "number") {
      await persist();
      return;
    }
    const current = configStore.get();
    const currentOffset = current.lastUpdateId;
    await persist({
      ...current,
      lastUpdateId:
        typeof currentOffset === "number"
          ? Math.max(currentOffset, nextOffset)
          : nextOffset,
    });
  };
}

export function createTelegramProactivePushChecker(
  configStore: Pick<TelegramConfigStore, "get">,
): () => boolean {
  return () => configStore.get().assistant?.proactivePush ?? true;
}

export function createTelegramProactivePushSetter(
  configStore: TelegramMutableConfigStore,
): (enabled: boolean) => Promise<void> {
  return async (enabled) => {
    await loadLatestTelegramConfig(configStore);
    const current = configStore.get();
    const config = {
      ...current,
      assistant: { ...current.assistant, proactivePush: enabled },
    };
    configStore.set(config);
    await configStore.persist(config);
  };
}

export function createTelegramDraftPreviewsChecker(
  configStore: Pick<TelegramConfigStore, "get">,
): () => boolean {
  return () => {
    const config = configStore.get();
    return (
      config.assistant?.draftPreviews ??
      config.draftPreviews ??
      config.richDraftPreviews ??
      false
    );
  };
}

export function createTelegramDraftPreviewsSetter(
  configStore: TelegramMutableConfigStore,
): (enabled: boolean) => Promise<void> {
  return async (enabled) => {
    await loadLatestTelegramConfig(configStore);
    const {
      draftPreviews: _legacyDraftPreviews,
      richDraftPreviews: _legacyRichDraftPreviews,
      ...current
    } = configStore.get();
    const config = {
      ...current,
      assistant: { ...current.assistant, draftPreviews: enabled },
    };
    configStore.set(config);
    await configStore.persist(config);
  };
}

export function createTelegramAssistantRenderingModeGetter(
  configStore: Pick<TelegramConfigStore, "get">,
): () => TelegramAssistantRenderingMode {
  return () => {
    const config = configStore.get();
    const mode = config.assistant?.rendering ?? config.assistantRendering;
    return mode === "html" ? "html" : "rich";
  };
}

export function createTelegramAssistantRenderingModeSetter(
  configStore: TelegramMutableConfigStore,
): (mode: TelegramAssistantRenderingMode) => Promise<void> {
  return async (mode) => {
    await loadLatestTelegramConfig(configStore);
    const { assistantRendering: _legacyAssistantRendering, ...current } =
      configStore.get();
    const config = {
      ...current,
      assistant: { ...current.assistant, rendering: mode },
    };
    configStore.set(config);
    await configStore.persist(config);
  };
}

export function createTelegramVoiceReplyModeGetter(
  configStore: Pick<TelegramConfigStore, "get">,
): () => "hidden" | "mirror" | "always" {
  return () => {
    const mode = configStore.get().voice?.replyMode;
    return mode === "mirror" || mode === "always" ? mode : "hidden";
  };
}

export function createTelegramVoiceReplyModeConfiguredChecker(
  configStore: Pick<TelegramConfigStore, "get">,
): () => boolean {
  return () => {
    const mode = configStore.get().voice?.replyMode;
    return mode === "mirror" || mode === "always";
  };
}

export function createTelegramVoiceReplyModeSetter(
  configStore: TelegramMutableConfigStore,
): (replyMode: "hidden" | "mirror" | "always" | undefined) => Promise<void> {
  return async (replyMode) => {
    await loadLatestTelegramConfig(configStore);
    const current = configStore.get();
    if (replyMode === undefined || replyMode === "hidden") {
      const { replyMode: _replyMode, ...remainingVoice } = current.voice ?? {};
      const next = { ...current };
      if (Object.keys(remainingVoice).length > 0) next.voice = remainingVoice;
      else delete next.voice;
      configStore.set(next);
      await configStore.persist(next);
      return;
    }
    const next = { ...current, voice: { ...(current.voice ?? {}), replyMode } };
    configStore.set(next);
    await configStore.persist(next);
  };
}

function getSystemTimezone(): string {
  try {
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    return tz && tz.length > 0 ? tz : "UTC";
  } catch {
    return "UTC";
  }
}

export function resolveTelegramTimeConfig(
  raw: TelegramTimeConfig | undefined,
): ResolvedTelegramTimeConfig {
  const injectionMode: TelegramTimeMode =
    raw?.injectionMode === "always" || raw?.injectionMode === "interval"
      ? raw.injectionMode
      : "hidden";
  const interval =
    typeof raw?.interval === "number" && raw.interval > 0
      ? raw.interval
      : 60 * 60 * 1000;
  const timezone = getSystemTimezone();
  return { injectionMode, interval, timezone };
}

export function createTelegramTimeConfigGetter(
  configStore: Pick<TelegramConfigStore, "get">,
): () => ResolvedTelegramTimeConfig {
  return () => resolveTelegramTimeConfig(configStore.get().time);
}

export function createTelegramTimeInjectionModeGetter(
  configStore: Pick<TelegramConfigStore, "get">,
): () => TelegramTimeMode {
  return () => resolveTelegramTimeConfig(configStore.get().time).injectionMode;
}

export function createTelegramTimeInjectionModeSetter(
  configStore: TelegramMutableConfigStore,
): (injectionMode: TelegramTimeMode) => Promise<void> {
  return async (injectionMode) => {
    await loadLatestTelegramConfig(configStore);
    const current = configStore.get();
    if (injectionMode === "hidden") {
      const { injectionMode: _injectionMode, ...remainingTime } =
        current.time ?? {};
      const next = { ...current };
      if (Object.keys(remainingTime).length > 0) next.time = remainingTime;
      else delete next.time;
      configStore.set(next);
      await configStore.persist(next);
      return;
    }
    const next = {
      ...current,
      time: { ...(current.time ?? {}), injectionMode },
    };
    configStore.set(next);
    await configStore.persist(next);
  };
}

export interface TelegramProactivePushTarget {
  chatId: number;
  threadId?: number;
}

export function createTelegramProactivePushChatIdGetter(
  getTarget: () => TelegramProactivePushTarget | undefined,
): () => number | undefined {
  return () => getTarget()?.chatId;
}

export function createTelegramProactivePushTargetGetter(deps: {
  getActiveTurnTarget: () => TelegramProactivePushTarget | undefined;
  getAssignedTarget: () => TelegramProactivePushTarget | undefined;
  getAllowedUserId: () => number | undefined;
}): () => TelegramProactivePushTarget | undefined {
  return () => {
    const activeTarget = deps.getActiveTurnTarget();
    if (activeTarget) return activeTarget;
    const assignedTarget = deps.getAssignedTarget();
    if (assignedTarget) return assignedTarget;
    const chatId = deps.getAllowedUserId();
    return typeof chatId === "number" ? { chatId } : undefined;
  };
}

export function createTelegramConfigControls(
  configStore: TelegramMutableConfigStore,
) {
  return {
    isProactivePushEnabled: createTelegramProactivePushChecker(configStore),
    setProactivePushEnabled: createTelegramProactivePushSetter(configStore),
    areDraftPreviewsEnabled: createTelegramDraftPreviewsChecker(configStore),
    setDraftPreviewsEnabled: createTelegramDraftPreviewsSetter(configStore),
    getAssistantRenderingMode:
      createTelegramAssistantRenderingModeGetter(configStore),
    setAssistantRenderingMode:
      createTelegramAssistantRenderingModeSetter(configStore),
    getVoiceReplyMode: createTelegramVoiceReplyModeGetter(configStore),
    isVoiceReplyModeConfigured:
      createTelegramVoiceReplyModeConfiguredChecker(configStore),
    setVoiceReplyMode: createTelegramVoiceReplyModeSetter(configStore),
    getTimeInjectionMode: createTelegramTimeInjectionModeGetter(configStore),
    setTimeInjectionMode: createTelegramTimeInjectionModeSetter(configStore),
  };
}

export type TelegramAuthorizationState = { kind: "allow" } | { kind: "deny" };

export function isValidTelegramAllowedUserId(
  userId: number | undefined,
): userId is number {
  return Number.isSafeInteger(userId) && (userId ?? 0) > 0;
}

export function getTelegramAuthorizationState(
  userId: number,
  allowedUserId?: number,
): TelegramAuthorizationState {
  return isValidTelegramAllowedUserId(allowedUserId) && userId === allowedUserId
    ? { kind: "allow" }
    : { kind: "deny" };
}
