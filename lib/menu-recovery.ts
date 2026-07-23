/**
 * Telegram durable-recovery operator menu
 * Zones: telegram ui, recovery controls, menu composition
 * Owns the metadata-only recovery projection, confirmation flow, and callback handling
 */

import type {
  RecoveryMetadataStatus,
  RecoveryOrphanReassignmentCandidate,
} from "./recovery.ts";
import type {
  TelegramModelMenuState,
  TelegramReplyMarkup,
} from "./menu-model.ts";

const KNOWN_RECOVERY_INCIDENTS = new Set([
  "orphaned-dispatch-marked-uncertain",
  "orphan-snapshot-tail-removed",
  "orphan-recovery-file-removed",
  "post-commit-cleanup-failed",
  "post-commit-cleanup-fsync-failed",
]);

export interface TelegramRecoveryOperatorItem {
  handle: string;
  requiredAction: "drain" | "retry-or-discard" | "none";
}

export interface TelegramRecoveryOrphanCandidate {
  handle: string;
  requiredAction: "reassign";
}

export interface TelegramRecoveryOperatorStatus {
  profile: string;
  mode: "active" | "downgrade-exclusive";
  familyCounts: { inbound: number; outbound: number; bus: number };
  stateCounts: Record<string, number>;
  quota: { usedBytes: number; limitBytes: number };
  oldestUnresolvedAgeMs: number | null;
  incidents: string[];
  items: TelegramRecoveryOperatorItem[];
  orphanCandidates: TelegramRecoveryOrphanCandidate[];
}

export interface TelegramRecoveryOperatorPort<TContext> {
  getStatus: () => RecoveryMetadataStatus;
  getOrphanCandidates: () => RecoveryOrphanReassignmentCandidate[];
  drainSafe: (ctx: TContext) => Promise<number>;
  retryUncertain: (
    actionId: string,
    ctx: TContext,
  ) => Promise<{ appended: boolean; duplicationWarning: true }>;
  discard: (actionId: string, ctx: TContext) => Promise<void> | void;
  reassign: (actionId: string, ctx: TContext) => Promise<void>;
  downgrade: (
    ctx: TContext,
  ) => Promise<
    | { status: "blocked"; blockerCount: number }
    | { status: "downgraded" }
  >;
}

export interface TelegramRecoveryMenuQuery {
  id: string;
  data?: string;
  message?: {
    message_id?: number;
    chat?: { id?: number };
  };
}

export interface TelegramRecoveryMenuRuntimeDeps<TContext>
  extends TelegramRecoveryOperatorPort<TContext> {
  getStoredModelMenuState: (
    messageId: number | undefined,
    chatId?: number,
  ) => TelegramModelMenuState | undefined;
  editInteractiveMessage: (
    chatId: number,
    messageId: number,
    text: string,
    mode: "html",
    replyMarkup: TelegramReplyMarkup,
  ) => Promise<void>;
  answerCallbackQuery: (callbackQueryId: string, text?: string) => Promise<void>;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
}

export interface TelegramRecoveryMenuRuntime<TContext> {
  getUnresolvedCount: () => number;
  handleCallbackQuery: (
    query: TelegramRecoveryMenuQuery,
    ctx: TContext,
  ) => Promise<boolean>;
}

function safeIncidentLabel(value: string): string {
  return KNOWN_RECOVERY_INCIDENTS.has(value) ? value : "recovery-incident";
}

export function projectTelegramRecoveryOperatorStatus(
  status: RecoveryMetadataStatus,
  orphanCandidates: readonly RecoveryOrphanReassignmentCandidate[],
): TelegramRecoveryOperatorStatus {
  const familyCounts = { inbound: 0, outbound: 0, bus: 0 };
  const stateCounts: Record<string, number> = {};
  for (const item of status.items) {
    familyCounts[item.family] += 1;
    stateCounts[item.state] = (stateCounts[item.state] ?? 0) + 1;
  }
  const items = status.items
    .filter((item) => item.family === "inbound")
    .map((item): TelegramRecoveryOperatorItem => ({
      handle: item.actionId,
      requiredAction: item.requiredAction,
    }));
  return {
    profile: status.profile,
    mode: status.mode,
    familyCounts,
    stateCounts,
    quota: {
      usedBytes: status.quota.totalBytes,
      limitBytes: status.quota.limitBytes,
    },
    oldestUnresolvedAgeMs: status.oldestUnresolvedAgeMs,
    incidents: [...new Set(status.incidents.map(safeIncidentLabel))],
    items,
    orphanCandidates: orphanCandidates.map((candidate) => ({
      handle: candidate.actionId,
      requiredAction: "reassign",
    })),
  };
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}

function formatAge(ageMs: number | null): string {
  if (ageMs === null) return "none";
  const seconds = Math.floor(ageMs / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

function requiredActionLabel(
  action: TelegramRecoveryOperatorItem["requiredAction"],
): string {
  if (action === "drain") return "safe drain";
  if (action === "retry-or-discard") return "retry or discard";
  return "none";
}

export function buildTelegramRecoveryMenuText(
  status: TelegramRecoveryOperatorStatus,
): string {
  const stateCounts = Object.entries(status.stateCounts)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([state, count]) => `${escapeHtml(state)}=${count}`)
    .join(", ") || "none";
  const lines = [
    "<b>🛟 Durable recovery</b>",
    `<code>profile</code>: ${escapeHtml(status.profile)}`,
    `<code>mode</code>: ${escapeHtml(status.mode)}`,
    `<code>families</code>: inbound=${status.familyCounts.inbound}, outbound=${status.familyCounts.outbound}, bus=${status.familyCounts.bus}`,
    `<code>states</code>: ${stateCounts}`,
    `<code>quota</code>: ${formatBytes(status.quota.usedBytes)} / ${formatBytes(status.quota.limitBytes)}`,
    `<code>oldest unresolved</code>: ${formatAge(status.oldestUnresolvedAgeMs)}`,
  ];
  if (status.items.length > 0) {
    lines.push("", "<b>Work handles</b>");
    for (const item of status.items) {
      lines.push(
        `<code>${escapeHtml(item.handle)}</code> · ${requiredActionLabel(item.requiredAction)}`,
      );
    }
  }
  if (status.orphanCandidates.length > 0) {
    lines.push("", "<b>Orphan candidates</b>");
    for (const candidate of status.orphanCandidates) {
      lines.push(`<code>${escapeHtml(candidate.handle)}</code> · reassign`);
    }
  }
  if (status.incidents.length > 0) {
    lines.push("", "<b>Incidents</b>");
    for (const incident of status.incidents) {
      lines.push(`<code>-</code> ${escapeHtml(incident)}`);
    }
  }
  return lines.join("\n");
}

export function buildTelegramRecoveryMenuReplyMarkup(
  status: TelegramRecoveryOperatorStatus,
): TelegramReplyMarkup {
  const rows: TelegramReplyMarkup["inline_keyboard"] = [
    [{ text: "⬆️ Main menu", callback_data: "menu:back" }],
    [{ text: "🌀 Refresh", callback_data: "recovery:refresh" }],
  ];
  if (status.items.some((item) => item.requiredAction === "drain")) {
    rows.push([
      { text: "☑️ Drain safe work", callback_data: "recovery:ask:drain" },
    ]);
  }
  for (const item of status.items) {
    if (item.requiredAction !== "retry-or-discard") {
      continue;
    }
    rows.push([
      {
        text: `⚠️ Retry ${item.handle}`,
        callback_data: `recovery:ask:retry:${item.handle}`,
      },
      {
        text: `🗑 Discard ${item.handle}`,
        callback_data: `recovery:ask:discard:${item.handle}`,
      },
    ]);
  }
  for (const candidate of status.orphanCandidates) {
    rows.push([
      {
        text: `🔁 Reassign ${candidate.handle}`,
        callback_data: `recovery:ask:reassign:${candidate.handle}`,
      },
    ]);
  }
  rows.push([
    {
      text: "📦 Downgrade recovery",
      callback_data: "recovery:ask:downgrade",
    },
  ]);
  return { inline_keyboard: rows };
}

function parseAction(
  data: string,
): { stage: "ask" | "confirm"; action: string; handle?: string } | undefined {
  const match = data.match(
    /^recovery:(ask|confirm):(drain|retry|discard|reassign|downgrade)(?::([A-Za-z0-9_-]{1,32}))?$/,
  );
  if (!match) return undefined;
  const action = match[2]!;
  const handle = match[3];
  const needsHandle =
    action === "retry" || action === "discard" || action === "reassign";
  if (needsHandle !== (handle !== undefined)) return undefined;
  return { stage: match[1] as "ask" | "confirm", action, handle };
}

function confirmationText(action: string): string {
  if (action === "retry") {
    return "<b>Retry this uncertain attempt? It may duplicate work that already ran.</b>";
  }
  if (action === "discard") {
    return "<b>Durably discard this recovery item?</b>";
  }
  if (action === "drain") return "<b>Drain all safe recovery work now?</b>";
  if (action === "reassign") {
    return "<b>Reassign this orphan candidate to the current exact binding?</b>";
  }
  return "<b>Downgrade durable recovery? This fences all profile runtimes and quarantines the recovery store.</b>";
}

function confirmationMarkup(
  action: string,
  handle?: string,
): TelegramReplyMarkup {
  const suffix = handle ? `:${handle}` : "";
  return {
    inline_keyboard: [
      [
        {
          text: "✅ Confirm",
          callback_data: `recovery:confirm:${action}${suffix}`,
        },
        { text: "❌ Cancel", callback_data: "recovery:refresh" },
      ],
    ],
  };
}

function safeRecoveryRuntimeError(action: string): Error {
  return new Error(`Telegram recovery operation failed (${action})`);
}

export function createTelegramRecoveryMenuRuntime<TContext>(
  deps: TelegramRecoveryMenuRuntimeDeps<TContext>,
): TelegramRecoveryMenuRuntime<TContext> {
  const readStatus = (): TelegramRecoveryOperatorStatus =>
    projectTelegramRecoveryOperatorStatus(
      deps.getStatus(),
      deps.getOrphanCandidates(),
    );

  const editStatus = async (state: TelegramModelMenuState): Promise<void> => {
    const status = readStatus();
    state.mode = "recovery";
    await deps.editInteractiveMessage(
      state.chatId,
      state.messageId,
      buildTelegramRecoveryMenuText(status),
      "html",
      buildTelegramRecoveryMenuReplyMarkup(status),
    );
  };

  return {
    getUnresolvedCount() {
      try {
        const status = readStatus();
        return Object.values(status.familyCounts).reduce(
          (total, count) => total + count,
          0,
        );
      } catch {
        deps.recordRuntimeEvent?.(
          "recovery",
          safeRecoveryRuntimeError("status-count"),
          { action: "status-count" },
        );
        return 0;
      }
    },

    async handleCallbackQuery(query, ctx) {
      const data = query.data;
      if (data !== "menu:recovery" && !data?.startsWith("recovery:")) {
        return false;
      }
      try {
      const state = deps.getStoredModelMenuState(
        query.message?.message_id,
        query.message?.chat?.id,
      );
      if (!state) {
        await deps.answerCallbackQuery(query.id, "Interactive message expired.");
        return true;
      }
      if (data === "menu:recovery" || data === "recovery:refresh") {
        try {
          await editStatus(state);
          await deps.answerCallbackQuery(query.id);
        } catch (error) {
          deps.recordRuntimeEvent?.(
            "recovery",
            safeRecoveryRuntimeError("refresh"),
            { action: "refresh" },
          );
          await deps.answerCallbackQuery(
            query.id,
            "Recovery status failed. Check diagnostics.",
          );
        }
        return true;
      }
      const parsed = parseAction(data);
      if (!parsed) {
        await deps.answerCallbackQuery(query.id, "Invalid recovery action.");
        return true;
      }
      if (parsed.stage === "ask") {
        await deps.editInteractiveMessage(
          state.chatId,
          state.messageId,
          confirmationText(parsed.action),
          "html",
          confirmationMarkup(parsed.action, parsed.handle),
        );
        await deps.answerCallbackQuery(query.id);
        return true;
      }
      let callbackText = "Recovery action completed.";
      try {
        if (parsed.action === "drain") {
          const count = await deps.drainSafe(ctx);
          callbackText = `Safe recovery drain completed (${count}).`;
        } else if (parsed.action === "retry") {
          const result = await deps.retryUncertain(parsed.handle!, ctx);
          callbackText = result.appended
            ? "Retry queued. Duplicate execution remains possible."
            : "Linked retry was already queued. Duplicate execution remains possible.";
        } else if (parsed.action === "discard") {
          await deps.discard(parsed.handle!, ctx);
          callbackText = "Recovery item durably discarded.";
        } else if (parsed.action === "reassign") {
          await deps.reassign(parsed.handle!, ctx);
          callbackText = "Recovery work reassigned to the current binding.";
        } else {
          const result = await deps.downgrade(ctx);
          callbackText =
            result.status === "downgraded"
              ? "Recovery store quarantined for downgrade."
              : `Recovery downgrade blocked (${result.blockerCount}).`;
        }
      } catch (error) {
        deps.recordRuntimeEvent?.(
          "recovery",
          safeRecoveryRuntimeError(parsed.action),
          { action: parsed.action },
        );
        callbackText = "Recovery action failed. Check diagnostics.";
      }
      try {
        await editStatus(state);
      } catch (error) {
        deps.recordRuntimeEvent?.(
          "recovery",
          safeRecoveryRuntimeError("post-action-refresh"),
          { action: "post-action-refresh" },
        );
      }
      await deps.answerCallbackQuery(query.id, callbackText);
      return true;
      } catch {
        deps.recordRuntimeEvent?.(
          "recovery",
          safeRecoveryRuntimeError("callback-boundary"),
          { action: "callback-boundary" },
        );
        try {
          await deps.answerCallbackQuery(
            query.id,
            "Recovery action failed. Check diagnostics.",
          );
        } catch {
          // Callback acknowledgement failures are already represented by the
          // fixed diagnostic above; never rethrow provider text or paths.
        }
        return true;
      }
    },
  };
}
