/**
 * Telegram diagnostics logs
 * Zones: telegram diagnostics, filesystem, session observability
 * Owns append-only profile/instance writer segments, leases, retention, and redacted runtime evidence without becoming routing state
 */

import { randomUUID } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, extname, join } from "node:path";
import { resolveAgentDir, resolveTelegramProfileTempFilePath } from "./paths.ts";
import { withTelegramFileTransaction } from "./locks.ts";
import * as Status from "./status.ts";

export type TelegramLogPathInput = string | (() => string);

export interface TelegramRuntimeJsonlEvent {
  at: number;
  category: string;
  message: string;
  details?: Record<string, unknown>;
}

export interface TelegramRuntimeLogIncident {
  kind: "diagnostics-overflow" | "diagnostics-write-failed";
  at: number;
  dropped: number;
}

export interface TelegramRuntimeJsonlLogOptions {
  path?: TelegramLogPathInput;
  /** Legacy option retained for source compatibility; segments never overwrite a previous log. */
  previousPath?: TelegramLogPathInput;
  maxBytes?: number;
  maxAgeMs?: number;
  maxSegments?: number;
  maxTotalBytes?: number;
  leaseStaleMs?: number;
  getNowMs?: () => number;
  instanceId?: string | (() => string);
  writerGeneration?: string;
  processGeneration?: string;
  pid?: number;
  isProcessAlive?: (pid: number) => boolean;
  removeFile?: (path: string) => void;
  canReset?: () => boolean;
  commitReset?: (commit: () => void) => boolean;
}

export interface TelegramRuntimeJsonlLog {
  getPath: () => string;
  startGeneration: (
    reason: string,
    scope?: Record<string, unknown>,
  ) => Promise<void>;
  retireGeneration: (
    reason: string,
    scope?: Record<string, unknown>,
  ) => Promise<void>;
  reset: (reason: string, scope?: Record<string, unknown>) => void;
  resetIfScopeChanged: (
    scopeKey: string,
    reason: string,
    scope?: Record<string, unknown>,
  ) => void;
  record: (event: TelegramRuntimeJsonlEvent) => void;
  getIncident: () => TelegramRuntimeLogIncident | undefined;
}

export const TELEGRAM_RUNTIME_LOG_RETENTION_MS = 14 * 24 * 60 * 60 * 1000;
export const TELEGRAM_RUNTIME_LOG_MAX_SEGMENTS = 100;
export const TELEGRAM_RUNTIME_LOG_MAX_TOTAL_BYTES = 100 * 1024 * 1024;
const DEFAULT_MAX_SEGMENT_BYTES = 5 * 1024 * 1024;
const DEFAULT_LEASE_STALE_MS = 30_000;

export function getTelegramRuntimeLogPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath("logs", "jsonl", agentDir, profileName);
}

/** Legacy display helper. Append-only segment discovery uses the base log path. */
export function getTelegramPreviousRuntimeLogPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath("logs", "_prev.jsonl", agentDir, profileName);
}

function safeComponent(value: string): string {
  const normalized = value.replace(/[^A-Za-z0-9_-]/gu, "-").slice(0, 80);
  return normalized || "unknown";
}

function redactJsonValue(key: string, value: unknown): unknown {
  if (/token|secret|password|authorization|cookie/iu.test(key)) return "<redacted>";
  if (value instanceof Error) return value.message;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "function" || typeof value === "symbol") return undefined;
  return value;
}

function safeJsonLine(value: unknown): string {
  return JSON.stringify(value, redactJsonValue);
}

interface SegmentLease {
  version: 2;
  pid: number;
  processGeneration: string;
  instanceId: string;
  writerGeneration: string;
  createdAt: number;
  heartbeatAt: number;
}

interface SegmentInfo {
  path: string;
  leasePath: string;
  size: number;
  createdAt: number;
  live: boolean;
}

const TELEGRAM_RUNTIME_LOG_PROCESS_GENERATION = randomUUID();
const liveWriterGenerations = new Set<string>();

function writerGenerationKey(
  processGeneration: string,
  writerGeneration: string,
): string {
  return `${processGeneration}:${writerGeneration}`;
}

function defaultIsProcessAlive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return Boolean(
      typeof error === "object" && error !== null && "code" in error &&
      Reflect.get(error, "code") === "EPERM",
    );
  }
}

function readSegmentCreatedAt(path: string, fallback: number): number {
  try {
    const firstLine = readFileSync(path, "utf8").split("\n", 1)[0];
    if (!firstLine) return fallback;
    const value: unknown = JSON.parse(firstLine);
    const at =
      value && typeof value === "object" && !Array.isArray(value)
        ? Reflect.get(value, "at")
        : undefined;
    return typeof at === "number" && Number.isFinite(at) ? at : fallback;
  } catch {
    return fallback;
  }
}

function readLease(path: string): SegmentLease | undefined {
  try {
    const value: unknown = JSON.parse(readFileSync(path, "utf8"));
    if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
    const lease = value as Partial<SegmentLease>;
    if (
      lease.version !== 2 || !Number.isSafeInteger(lease.pid) ||
      typeof lease.processGeneration !== "string" ||
      typeof lease.instanceId !== "string" ||
      typeof lease.writerGeneration !== "string" ||
      !Number.isFinite(lease.createdAt) ||
      !Number.isFinite(lease.heartbeatAt)
    ) return undefined;
    return lease as SegmentLease;
  } catch {
    return undefined;
  }
}

export function createTelegramRuntimeJsonlLog(
  options: TelegramRuntimeJsonlLogOptions = {},
): TelegramRuntimeJsonlLog {
  const resolveBasePath = () =>
    typeof options.path === "function"
      ? options.path()
      : (options.path ?? getTelegramRuntimeLogPath());
  const resolveInstanceId = () => safeComponent(
    typeof options.instanceId === "function"
      ? options.instanceId()
      : (options.instanceId ?? "instance"),
  );
  let generation = safeComponent(options.writerGeneration ?? randomUUID());
  const processGeneration = safeComponent(
    options.processGeneration ?? TELEGRAM_RUNTIME_LOG_PROCESS_GENERATION,
  );
  const pid = options.pid ?? process.pid;
  const now = options.getNowMs ?? Date.now;
  const maxSegmentBytes = options.maxBytes ?? DEFAULT_MAX_SEGMENT_BYTES;
  const maxAgeMs = options.maxAgeMs ?? TELEGRAM_RUNTIME_LOG_RETENTION_MS;
  const maxSegments = options.maxSegments ?? TELEGRAM_RUNTIME_LOG_MAX_SEGMENTS;
  const maxTotalBytes = options.maxTotalBytes ?? TELEGRAM_RUNTIME_LOG_MAX_TOTAL_BYTES;
  const leaseStaleMs = options.leaseStaleMs ?? DEFAULT_LEASE_STALE_MS;
  const isProcessAlive = options.isProcessAlive ?? defaultIsProcessAlive;
  const removeFile =
    options.removeFile ?? ((path: string) => rmSync(path, { force: true }));
  const scopeKeys = new Map<string, string | undefined>();
  let rotation = 0;
  let activeBasePath: string | undefined;
  let activePath: string | undefined;
  let activeCreatedAt: number | undefined;
  let generationActive = true;
  let incident: TelegramRuntimeLogIncident | undefined;
  let pending: Promise<void> = Promise.resolve();
  let queued: Array<{ basePath: string; line: string }> = [];
  let scheduled = false;
  liveWriterGenerations.add(
    writerGenerationKey(processGeneration, generation),
  );

  const segmentPrefix = (basePath: string) => {
    const extension = extname(basePath);
    const stem = basename(basePath, extension);
    return `${stem}.segment-`;
  };
  const makeSegmentPath = (basePath: string) => {
    const extension = extname(basePath) || ".jsonl";
    return join(
      dirname(basePath),
      `${segmentPrefix(basePath)}${resolveInstanceId()}-${generation}-${String(rotation).padStart(6, "0")}-${randomUUID()}${extension}`,
    );
  };
  const leasePathFor = (path: string) => `${path}.lease.json`;
  const ensureParent = (path: string) => {
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    try { chmodSync(dirname(path), 0o700); } catch { /* unsupported filesystem */ }
  };
  const writeLease = (path: string) => {
    const createdAt = activeCreatedAt;
    if (createdAt === undefined) {
      throw new Error("Telegram diagnostics segment creation time is absent");
    }
    const lease: SegmentLease = {
      version: 2,
      pid,
      processGeneration,
      instanceId: resolveInstanceId(),
      writerGeneration: generation,
      createdAt,
      heartbeatAt: now(),
    };
    writeFileSync(leasePathFor(path), `${safeJsonLine(lease)}\n`, { mode: 0o600 });
    try { chmodSync(leasePathFor(path), 0o600); } catch { /* unsupported filesystem */ }
  };
  const listSegments = (basePath: string): SegmentInfo[] => {
    const directory = dirname(basePath);
    const prefix = segmentPrefix(basePath);
    let names: string[];
    try { names = readdirSync(directory); } catch { return []; }
    return names
      .filter((name) => name.startsWith(prefix) && name.endsWith(".jsonl"))
      .flatMap((name) => {
        const path = join(directory, name);
        try {
          const stat = statSync(path);
          if (!stat.isFile()) return [];
          const leasePath = leasePathFor(path);
          const lease = readLease(leasePath);
          const leaseMatchesSegment = Boolean(
            lease &&
            name.includes(
              `-${safeComponent(lease.instanceId)}-${safeComponent(lease.writerGeneration)}-`,
            ),
          );
          const processAlive = Boolean(
            leaseMatchesSegment && lease && isProcessAlive(lease.pid),
          );
          const exactGenerationLive = Boolean(
            lease &&
              (lease.processGeneration !== processGeneration ||
                liveWriterGenerations.has(
                  writerGenerationKey(
                    lease.processGeneration,
                    lease.writerGeneration,
                  ),
                )),
          );
          const stale = lease ? now() - lease.heartbeatAt > leaseStaleMs : true;
          const alive = processAlive && exactGenerationLive;
          const classification = !leaseMatchesSegment
            ? "dead"
            : stale
              ? alive
                ? "stale-live"
                : "dead"
              : alive
                ? "live"
                : "dead";
          const live = classification === "live" || classification === "stale-live";
          return [{
            path,
            leasePath,
            size: stat.size,
            createdAt:
              lease?.createdAt ?? readSegmentCreatedAt(path, stat.mtimeMs),
            live,
          }];
        } catch { return []; }
      });
  };
  const removeSegment = (entry: SegmentInfo): boolean => {
    try {
      if (existsSync(entry.leasePath)) removeFile(entry.leasePath);
      removeFile(entry.path);
      return true;
    } catch {
      noteDrop("diagnostics-write-failed");
      return false;
    }
  };
  const retain = (basePath: string): SegmentInfo[] => {
    let segments = listSegments(basePath).sort((a, b) => a.createdAt - b.createdAt);
    const removable = () => segments.filter((entry) => !entry.live && entry.path !== activePath);
    for (const entry of removable()) {
      if (now() - entry.createdAt <= maxAgeMs) continue;
      if (!removeSegment(entry)) continue;
      segments = segments.filter((candidate) => candidate.path !== entry.path);
    }
    while (segments.length > maxSegments || segments.reduce((sum, entry) => sum + entry.size, 0) > maxTotalBytes) {
      const candidate = removable()[0];
      if (!candidate) break;
      if (!removeSegment(candidate)) break;
      segments = segments.filter((entry) => entry.path !== candidate.path);
    }
    return segments;
  };
  const makeRoom = (
    basePath: string,
    segments: SegmentInfo[],
    additionalSegments: number,
    additionalBytes: number,
  ): SegmentInfo[] => {
    void basePath;
    let current = [...segments];
    while (
      current.length + additionalSegments > maxSegments ||
      current.reduce((sum, entry) => sum + entry.size, 0) + additionalBytes >
        maxTotalBytes
    ) {
      const candidate = current.find(
        (entry) => !entry.live && entry.path !== activePath,
      );
      if (!candidate) break;
      if (!removeSegment(candidate)) break;
      current = current.filter((entry) => entry.path !== candidate.path);
    }
    return current;
  };
  const noteDrop = (kind: TelegramRuntimeLogIncident["kind"]) => {
    if (
      incident?.kind === "diagnostics-write-failed" &&
      kind === "diagnostics-overflow"
    ) {
      incident = { ...incident, at: now(), dropped: incident.dropped + 1 };
      return;
    }
    incident = incident?.kind === kind
      ? { ...incident, at: now(), dropped: incident.dropped + 1 }
      : { kind, at: now(), dropped: 1 };
  };
  const appendLocked = (basePath: string, line: string) => {
    ensureParent(basePath);
    let segments = retain(basePath);
    if (!activePath || activeBasePath !== basePath || !existsSync(activePath)) {
      if (activePath && activeBasePath !== basePath) {
        try {
          if (existsSync(leasePathFor(activePath))) {
            removeFile(leasePathFor(activePath));
          }
        } catch {
          noteDrop("diagnostics-write-failed");
          return;
        }
      }
      activeBasePath = basePath;
      activePath = makeSegmentPath(basePath);
      activeCreatedAt = now();
      rotation += 1;
      segments = makeRoom(
        basePath,
        segments,
        1,
        Buffer.byteLength(line) + 256,
      );
      if (
        segments.length + 1 > maxSegments ||
        segments.reduce((sum, entry) => sum + entry.size, 0) +
          Buffer.byteLength(line) + 256 > maxTotalBytes
      ) {
        noteDrop("diagnostics-overflow");
        activePath = undefined;
        return;
      }
      writeFileSync(activePath, safeJsonLine({
        at: activeCreatedAt, kind: "boundary", boundary: "writer-start",
        instanceId: resolveInstanceId(), writerGeneration: generation,
        processGeneration, pid,
      }) + "\n", { mode: 0o600, flag: "wx" });
      writeLease(activePath);
      segments = listSegments(basePath);
    }
    const activeSize = statSync(activePath).size;
    const ageExpired =
      activeCreatedAt !== undefined && now() - activeCreatedAt > maxAgeMs;
    if (
      ageExpired ||
      (activeSize > 0 && activeSize + Buffer.byteLength(line) > maxSegmentBytes)
    ) {
      const oldPath = activePath;
      const oldCreatedAt = activeCreatedAt;
      activePath = makeSegmentPath(basePath);
      activeCreatedAt = now();
      rotation += 1;
      segments = makeRoom(
        basePath,
        segments,
        1,
        Buffer.byteLength(line) + 256,
      );
      const total = segments.reduce((sum, entry) => sum + entry.size, 0);
      if (
        segments.length + 1 > maxSegments ||
        total + Buffer.byteLength(line) + 256 > maxTotalBytes
      ) {
        activePath = oldPath;
        activeCreatedAt = oldCreatedAt;
        noteDrop("diagnostics-overflow");
        return;
      }
      try {
        if (existsSync(leasePathFor(oldPath))) {
          removeFile(leasePathFor(oldPath));
        }
      } catch {
        activePath = oldPath;
        activeCreatedAt = oldCreatedAt;
        noteDrop("diagnostics-write-failed");
        return;
      }
      writeFileSync(activePath, safeJsonLine({
        at: activeCreatedAt, kind: "boundary",
        boundary: ageExpired ? "age-rotation" : "rotation",
        instanceId: resolveInstanceId(), writerGeneration: generation,
        processGeneration,
      }) + "\n", { mode: 0o600, flag: "wx" });
      writeLease(activePath);
    }
    segments = retain(basePath);
    const totalBytes = segments.reduce((sum, entry) => sum + entry.size, 0);
    if (totalBytes + Buffer.byteLength(line) > maxTotalBytes) {
      noteDrop("diagnostics-overflow");
      return;
    }
    appendFileSync(activePath, line, { mode: 0o600 });
    try { chmodSync(activePath, 0o600); } catch { /* unsupported filesystem */ }
    writeLease(activePath);
  };
  const enqueue = (basePath: string, line: string) => {
    if (!generationActive) return;
    queued.push({ basePath, line });
    if (scheduled) return;
    scheduled = true;
    pending = pending.then(() => {
      scheduled = false;
      const batch = queued;
      queued = [];
      for (const entry of batch) {
        try {
          withTelegramFileTransaction(`${entry.basePath}.segments.transaction`, () => {
            appendLocked(entry.basePath, entry.line);
          });
        } catch {
          noteDrop("diagnostics-write-failed");
        }
      }
    }).catch(() => noteDrop("diagnostics-write-failed"));
  };
  const boundary = (reason: string, scope?: Record<string, unknown>) => {
    const basePath = resolveBasePath();
    enqueue(basePath, safeJsonLine({
      at: now(), kind: "boundary", boundary: reason,
      instanceId: resolveInstanceId(), writerGeneration: generation,
      processGeneration, scope,
    }) + "\n");
  };
  const retireGeneration = async (
    reason: string,
    scope?: Record<string, unknown>,
  ): Promise<void> => {
    if (!generationActive) return;
    boundary(reason, scope);
    await pending;
    const retiredGeneration = generation;
    const retiredPath = activePath;
    const retiredBasePath = activeBasePath;
    generationActive = false;
    liveWriterGenerations.delete(
      writerGenerationKey(processGeneration, retiredGeneration),
    );
    if (retiredPath && retiredBasePath) {
      try {
        withTelegramFileTransaction(
          `${retiredBasePath}.segments.transaction`,
          () => {
            const leasePath = leasePathFor(retiredPath);
            if (existsSync(leasePath)) removeFile(leasePath);
          },
        );
      } catch {
        noteDrop("diagnostics-write-failed");
      }
    }
    activePath = undefined;
    activeBasePath = undefined;
    activeCreatedAt = undefined;
  };

  return {
    getPath: () => activePath ?? resolveBasePath(),
    async startGeneration(reason, scope) {
      await retireGeneration("writer-replaced");
      generation = safeComponent(randomUUID());
      generationActive = true;
      liveWriterGenerations.add(
        writerGenerationKey(processGeneration, generation),
      );
      boundary(reason, scope);
    },
    retireGeneration,
    reset(reason, scope) {
      boundary(reason, scope);
      scopeKeys.set(resolveBasePath(), scope ? safeJsonLine(scope) : undefined);
    },
    resetIfScopeChanged(scopeKey, reason, scope) {
      const basePath = resolveBasePath();
      if (scopeKeys.get(basePath) === scopeKey) return;
      scopeKeys.set(basePath, scopeKey);
      boundary(reason, scope);
    },
    record(event) {
      try {
        enqueue(resolveBasePath(), safeJsonLine({ kind: "event", ...event }) + "\n");
      } catch {
        noteDrop("diagnostics-write-failed");
      }
    },
    getIncident: () => incident,
  };
}

export interface TelegramRuntimeDiagnosticsRuntime<TContext> {
  events: Status.TelegramRuntimeEventRecorder;
  recordRuntimeEvent(category: string, error: unknown, details?: Record<string, unknown>): void;
  bindStorage(ports: {
    getBotToken(): string | undefined;
    getProfileName(): string | undefined;
    canReset(): boolean;
    commitReset(commit: () => void): boolean;
  }): void;
  bindStatus(ports: {
    instanceId: string;
    updateStatus(ctx: TContext, error?: string): void;
    getStatusState(): Status.TelegramBridgeStatusLineState;
    persistSnapshot(snapshot: ReturnType<typeof Status.createTelegramStatusSnapshot>): Promise<void>;
  }): void;
  onSessionStart(): Promise<void>;
  onSessionShutdown(): Promise<void>;
  updateStatus(ctx: TContext, error?: string): void;
  getStatusLines(options?: Status.TelegramBridgeStatusLineOptions): string[];
  scheduleSnapshotPersist(): void;
}

export function createTelegramRuntimeDiagnosticsRuntime<TContext>(): TelegramRuntimeDiagnosticsRuntime<TContext> {
  let getBotToken = (): string | undefined => undefined;
  let getProfileName = (): string | undefined => undefined;
  let statusPorts: {
    instanceId: string;
    updateStatus(ctx: TContext, error?: string): void;
    getStatusState(): Status.TelegramBridgeStatusLineState;
    persistSnapshot(snapshot: ReturnType<typeof Status.createTelegramStatusSnapshot>): Promise<void>;
  } | undefined;
  let requestSnapshotPersist = (): void => {};
  const events = Status.createTelegramRuntimeEventRecorder({ getBotToken: () => getBotToken() });
  const jsonl = createTelegramRuntimeJsonlLog({
    path: () => getTelegramRuntimeLogPath(undefined, getProfileName()),
    instanceId: () => statusPorts?.instanceId ?? "bootstrap",
  });
  const recordRuntimeEvent = (category: string, error: unknown, details?: Record<string, unknown>): void => {
    events.record(category, error, details);
    const latestEvent = events.getEvents().at(-1);
    if (latestEvent) jsonl.record(latestEvent);
    requestSnapshotPersist();
  };
  const persistCurrentSnapshot = async (): Promise<void> => {
    if (!statusPorts) return;
    await statusPorts.persistSnapshot(Status.createTelegramStatusSnapshot(statusPorts.getStatusState()));
  };
  const updateRuntimeLogScope = (reason: string): void => {
    if (!statusPorts) return;
    const scope = Status.createTelegramRuntimeLogScope({
      state: statusPorts.getStatusState(),
      instanceId: statusPorts.instanceId,
    });
    jsonl.resetIfScopeChanged(JSON.stringify(scope), reason, scope);
  };
  return {
    events,
    recordRuntimeEvent,
    bindStorage(ports) {
      getBotToken = ports.getBotToken;
      getProfileName = ports.getProfileName;
    },
    bindStatus(ports) {
      statusPorts = ports;
      requestSnapshotPersist = Status.createTelegramRuntimeDiagnosticsSnapshotScheduler({
        persistSnapshot: persistCurrentSnapshot,
        recordError(error) {
          events.record("telegram", error, { phase: "runtime-diagnostics-snapshot-persist" });
        },
      });
    },
    async onSessionStart() {
      await jsonl.startGeneration("session-start");
    },
    async onSessionShutdown() {
      await jsonl.retireGeneration("session-shutdown");
    },
    updateStatus(ctx, error) {
      if (!statusPorts) return;
      statusPorts.updateStatus(ctx, error);
      updateRuntimeLogScope("status-scope-change");
    },
    getStatusLines(options) {
      if (!statusPorts) return [];
      void persistCurrentSnapshot().catch((error) => {
        recordRuntimeEvent("telegram", error, { phase: "status-snapshot-persist" });
      });
      return Status.buildTelegramBridgeStatusLines(statusPorts.getStatusState(), options);
    },
    scheduleSnapshotPersist() { requestSnapshotPersist(); },
  };
}
