/**
 * Durable Telegram outbound planning and source spooling
 * Zones: telegram outbound, recovery, filesystem
 * Owns pure final-reply unit planning and descriptor-based source capture before
 * recovery publication; it does not own agent lifecycle wiring or Telegram mutations.
 */

import { createHash } from "node:crypto";
import type { Stats } from "node:fs";
import { open } from "node:fs/promises";
import { basename, isAbsolute } from "node:path";

import {
  getTelegramGuestAttachmentTransport,
  getTelegramRichOutboundAttachmentMediaKind,
  isTelegramOutboundPhotoAttachmentPath,
  TELEGRAM_OUTBOUND_ATTACHMENT_MAX_BYTES,
} from "./outbound-attachments.ts";
import {
  planTelegramDurableOutboundReply,
  type TelegramDurableOutboundReplyPlan,
} from "./outbound.ts";
import {
  type RecoveryOutboundDrainItem,
  type RecoveryOutboundMediaKind,
  type RecoveryOutboundPlanInput,
  type RecoveryOutboundRecord,
  type RecoveryOutboundUnit,
  type RecoveryStore,
} from "./recovery.ts";
import { renderTelegramMessage } from "./rendering.ts";
import {
  normalizeTelegramNativeMarkdown,
  splitTelegramNativeMarkdown,
} from "./replies.ts";

export interface TelegramDurableOutboundSourceDescriptor {
  path: string;
  fileName: string;
  caption?: string;
}

export interface TelegramDurableOutboundPlanOptions {
  intentId: string;
  turnId: string;
  sourceInboundRecordIds: readonly string[];
  claim: RecoveryOutboundPlanInput["claim"];
  replyToMessageId: number;
  renderingMode: RecoveryOutboundPlanInput["renderingMode"];
  finalMarkdown: string;
  queuedAttachments: readonly TelegramDurableOutboundSourceDescriptor[];
  generatedVoice?: readonly TelegramDurableOutboundSourceDescriptor[];
  automaticVoice?: boolean;
  guestQueryId?: string;
}

export interface TelegramDurableOutboundPlannedIntent {
  recoveryPlan: Omit<RecoveryOutboundPlanInput, "spool">;
  spoolSources: TelegramDurableOutboundSourceDescriptor[];
}

export interface TelegramDurableOutboundSourceHandle {
  stat: () => Promise<Stats>;
  read: (
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ) => Promise<{ bytesRead: number }>;
  close: () => Promise<void>;
}

export interface TelegramDurableOutboundCommitDeps {
  store: Pick<
    RecoveryStore,
    "planOutbound" | "listClaimableOutboundRecords"
  >;
  transformReply: (text: string) => Promise<string>;
  maxSourceBytes?: number;
  openSource?: (
    path: string,
  ) => Promise<TelegramDurableOutboundSourceHandle>;
  afterSourceRead?: (
    source: Readonly<TelegramDurableOutboundSourceDescriptor>,
    index: number,
  ) => void | Promise<void>;
}

export interface TelegramDurableOutboundSourceBytes {
  bytes: Uint8Array;
  sha256: string;
}

function operationId(index: number): string {
  return `outbound-unit-${String(index).padStart(4, "0")}`;
}

function normalizePersistedFileName(fileName: string): string {
  if (
    typeof fileName !== "string" ||
    fileName.length === 0 ||
    fileName === "." ||
    fileName === ".." ||
    isAbsolute(fileName) ||
    /^[A-Za-z]:/.test(fileName) ||
    fileName.includes("/") ||
    fileName.includes("\\")
  ) {
    throw new Error(`Invalid durable outbound filename: ${fileName}`);
  }
  const normalized = basename(fileName);
  if (!normalized || normalized === "." || normalized === ".." || normalized !== fileName) {
    throw new Error(`Invalid durable outbound filename: ${fileName}`);
  }
  return normalized;
}

function cloneSource(
  source: TelegramDurableOutboundSourceDescriptor,
): TelegramDurableOutboundSourceDescriptor {
  return {
    path: source.path,
    fileName: normalizePersistedFileName(source.fileName),
    ...(source.caption !== undefined ? { caption: source.caption } : {}),
  };
}

function capGuestCaption(caption: string): string {
  return Array.from(caption).slice(0, 1024).join("");
}

function getGuestMediaKind(
  path: string,
): RecoveryOutboundMediaKind {
  const transport = getTelegramGuestAttachmentTransport(path);
  if (transport.fileField === "photo") return "photo";
  if (transport.fileField === "audio") return "audio";
  if (transport.fileField === "voice") return "voice";
  return "document";
}

function getOrdinaryAttachmentUnit(
  source: TelegramDurableOutboundSourceDescriptor,
  spoolRefIndex: number,
  index: number,
): RecoveryOutboundUnit {
  const photo = isTelegramOutboundPhotoAttachmentPath(source.path);
  return {
    kind: "attachment",
    operationId: operationId(index),
    method: photo ? "sendPhoto" : "sendDocument",
    spoolRefIndex,
    fileName: source.fileName,
    mediaKind: photo ? "photo" : "document",
    ...(source.caption !== undefined ? { caption: source.caption } : {}),
  };
}

function assertGeneratedVoiceSource(
  source: TelegramDurableOutboundSourceDescriptor,
): void {
  const normalized = source.fileName.toLowerCase();
  if (!normalized.endsWith(".ogg") && !normalized.endsWith(".opus")) {
    throw new Error(
      `Durable generated voice source must be .ogg or .opus: ${source.fileName}`,
    );
  }
}

function getTextChunks(
  markdown: string,
  renderingMode: RecoveryOutboundPlanInput["renderingMode"],
): string[] {
  if (!markdown) return [];
  if (renderingMode === "rich") return splitTelegramNativeMarkdown(markdown);
  return renderTelegramMessage(markdown, { mode: "markdown" }).map(
    (chunk) => chunk.text,
  );
}

function planTelegramDurableOutboundFromReply(
  options: TelegramDurableOutboundPlanOptions,
  reply: TelegramDurableOutboundReplyPlan,
): TelegramDurableOutboundPlannedIntent {
  const attachments = options.queuedAttachments.map(cloneSource);
  const generatedVoice = (options.generatedVoice ?? []).map(cloneSource);
  const units: RecoveryOutboundUnit[] = [];
  const spoolSources: TelegramDurableOutboundSourceDescriptor[] = [];
  const addSource = (
    source: TelegramDurableOutboundSourceDescriptor,
  ): number => {
    spoolSources.push(source);
    return spoolSources.length - 1;
  };
  const textChunks = getTextChunks(reply.markdown, options.renderingMode);

  if (options.guestQueryId) {
    if (attachments.length > 1) {
      throw new Error("Durable Guest outbound supports one attachment");
    }
    const attachment = attachments[0];
    if (attachment) {
      if (generatedVoice.length > 0) {
        throw new Error("Durable Guest attachment plan cannot include generated voice");
      }
      const spoolRefIndex = addSource(attachment);
      units.push({
        kind: "guest",
        operationId: operationId(0),
        method: "answerGuestQuery",
        spoolRefIndex,
        fileName: attachment.fileName,
        mediaKind: getGuestMediaKind(attachment.path),
        ...(reply.markdown ? { caption: capGuestCaption(reply.markdown) } : {}),
      });
    } else if (reply.voiceReplies.length > 0) {
      if (generatedVoice.length !== 1) {
        throw new Error("Durable Guest voice plan requires one generated voice source");
      }
      const voiceSource = generatedVoice[0];
      if (!voiceSource) {
        throw new Error("Durable Guest voice source is missing");
      }
      assertGeneratedVoiceSource(voiceSource);
      const spoolRefIndex = addSource(voiceSource);
      units.push({
        kind: "guest",
        operationId: operationId(0),
        method: "answerGuestQuery",
        spoolRefIndex,
        fileName: voiceSource.fileName,
        mediaKind: "voice",
        ...(reply.markdown ? { caption: capGuestCaption(reply.markdown) } : {}),
      });
    } else if (textChunks[0]) {
      if (generatedVoice.length > 0) {
        throw new Error("Durable Guest text plan cannot include generated voice");
      }
      units.push({
        kind: "guest",
        operationId: operationId(0),
        method: "answerGuestQuery",
        markdown: textChunks[0],
      });
    }
    if (units.length === 0) {
      throw new Error("Durable outbound plan has no deliverable Guest result");
    }
    const firstVoice = attachment ? undefined : reply.voiceReplies[0];
    return {
      recoveryPlan: {
        intentId: options.intentId,
        turnId: options.turnId,
        sourceInboundRecordIds: [...options.sourceInboundRecordIds],
        claim: options.claim,
        replyToMessageId: options.replyToMessageId,
        renderingMode: options.renderingMode,
        finalMarkdown: reply.markdown,
        renderedChunks: textChunks,
        units,
        buttons: [],
        ...(firstVoice
          ? {
              voice: {
                text: firstVoice.text,
                automatic: reply.automaticVoice,
              },
            }
          : {}),
        guestQueryId: options.guestQueryId,
      },
      spoolSources,
    };
  }

  if (generatedVoice.length !== reply.voiceReplies.length) {
    throw new Error(
      `Durable outbound voice source count mismatch (${generatedVoice.length} != ${reply.voiceReplies.length})`,
    );
  }

  const richMediaKind =
    options.renderingMode === "rich" &&
      reply.markdown.trim() &&
      reply.voiceReplies.length === 0 &&
      attachments.length === 1
      ? getTelegramRichOutboundAttachmentMediaKind(attachments[0]?.path ?? "")
      : undefined;
  if (richMediaKind) {
    const attachment = attachments[0];
    if (!attachment) {
      throw new Error("Durable Rich media source is missing");
    }
    const spoolRefIndex = addSource(attachment);
    units.push({
      kind: "rich-media",
      operationId: operationId(0),
      method: "sendRichMessage",
      spoolRefIndex,
      fileName: attachment.fileName,
      mediaKind: richMediaKind,
      caption: normalizeTelegramNativeMarkdown(reply.markdown),
    });
    for (const chunk of textChunks) {
      units.push({
        kind: "final-text",
        operationId: operationId(units.length),
        method: "sendRichMessage",
        content: chunk,
        contentMode: "rich-markdown",
      });
    }
    units.push(
      getOrdinaryAttachmentUnit(attachment, spoolRefIndex, units.length),
    );
    const richUnit = units[0];
    if (richUnit?.kind !== "rich-media") {
      throw new Error("Durable Rich media fallback root is missing");
    }
    richUnit.fallback = {
      trigger: "known-failure",
      operationIds: units.slice(1).map((unit) => unit.operationId),
    };
  } else {
    for (const chunk of textChunks) {
      units.push({
        kind: "final-text",
        operationId: operationId(units.length),
        method: options.renderingMode === "rich"
          ? "sendRichMessage"
          : "sendMessage",
        content: chunk,
        contentMode: options.renderingMode === "rich"
          ? "rich-markdown"
          : "html",
      });
    }
    for (const voiceSource of generatedVoice) {
      assertGeneratedVoiceSource(voiceSource);
      const spoolRefIndex = addSource(voiceSource);
      units.push({
        kind: "voice",
        operationId: operationId(units.length),
        method: "sendVoice",
        spoolRefIndex,
        fileName: voiceSource.fileName,
        mediaKind: "voice",
        ...(voiceSource.caption !== undefined
          ? { caption: voiceSource.caption }
          : {}),
      });
    }
    if (
      units.length === 0 &&
      attachments.length > 0
    ) {
      units.push({
        kind: "final-text",
        operationId: operationId(0),
        method: "sendMessage",
        content: "Attached requested file(s).",
        contentMode: "plain",
      });
    }
    for (const attachment of attachments) {
      const spoolRefIndex = addSource(attachment);
      units.push(
        getOrdinaryAttachmentUnit(
          attachment,
          spoolRefIndex,
          units.length,
        ),
      );
    }
  }

  if (units.length === 0) {
    throw new Error("Durable outbound plan has no delivery units");
  }
  return {
    recoveryPlan: {
      intentId: options.intentId,
      turnId: options.turnId,
      sourceInboundRecordIds: [...options.sourceInboundRecordIds],
      claim: options.claim,
      replyToMessageId: options.replyToMessageId,
      renderingMode: options.renderingMode,
      finalMarkdown: reply.markdown,
      renderedChunks: textChunks,
      units,
      buttons: reply.buttons,
      ...(reply.voiceText
        ? {
            voice: {
              text: reply.voiceText,
              automatic: reply.automaticVoice,
            },
          }
        : {}),
    },
    spoolSources,
  };
}

/** Purely converts current reply semantics into strict, ordered recovery units. */
export function planTelegramDurableOutbound(
  options: TelegramDurableOutboundPlanOptions,
): TelegramDurableOutboundPlannedIntent {
  return planTelegramDurableOutboundFromReply(
    options,
    planTelegramDurableOutboundReply(options.finalMarkdown, {
      automaticVoice: options.automaticVoice,
    }),
  );
}

async function openNodeSource(
  path: string,
): Promise<TelegramDurableOutboundSourceHandle> {
  const handle = await open(path, "r");
  return {
    stat: () => handle.stat(),
    read: (buffer, offset, length, position) =>
      handle.read(buffer, offset, length, position),
    close: () => handle.close(),
  };
}

function getBoundedSourceSize(stats: Stats, maxSourceBytes: number): number {
  if (!stats.isFile()) throw new Error("Durable outbound source is not a file");
  if (!Number.isSafeInteger(stats.size) || stats.size < 0) {
    throw new Error("Durable outbound source has an invalid size");
  }
  if (stats.size > maxSourceBytes) {
    throw new Error(
      `Durable outbound source exceeds size limit (${stats.size} bytes > ${maxSourceBytes} bytes)`,
    );
  }
  return stats.size;
}

function stableBirthtimeMs(stats: Stats): number | undefined {
  return Number.isFinite(stats.birthtimeMs) && stats.birthtimeMs > 0
    ? stats.birthtimeMs
    : undefined;
}

function sourceIdentityMatches(before: Stats, after: Stats): boolean {
  const beforeBirthtimeMs = stableBirthtimeMs(before);
  const afterBirthtimeMs = stableBirthtimeMs(after);
  return before.dev === after.dev &&
    before.ino === after.ino &&
    before.size === after.size &&
    before.mtimeMs === after.mtimeMs &&
    before.ctimeMs === after.ctimeMs &&
    beforeBirthtimeMs === afterBirthtimeMs;
}

/** Read and hash one opened source while rejecting replacement or mutation. */
export async function readTelegramDurableOutboundSource(
  source: Readonly<TelegramDurableOutboundSourceDescriptor>,
  options: {
    maxSourceBytes?: number;
    openSource?: TelegramDurableOutboundCommitDeps["openSource"];
    afterRead?: () => void | Promise<void>;
  } = {},
): Promise<TelegramDurableOutboundSourceBytes> {
  const maxSourceBytes =
    options.maxSourceBytes ?? TELEGRAM_OUTBOUND_ATTACHMENT_MAX_BYTES;
  if (!Number.isSafeInteger(maxSourceBytes) || maxSourceBytes <= 0) {
    throw new Error("Invalid durable outbound source size limit");
  }
  let handle: TelegramDurableOutboundSourceHandle;
  try {
    handle = await (options.openSource ?? openNodeSource)(source.path);
  } catch (error) {
    throw new Error(
      `Durable outbound source is unavailable: ${source.fileName}`,
      { cause: error },
    );
  }
  try {
    const before = await handle.stat();
    const size = getBoundedSourceSize(before, maxSourceBytes);
    const bytes = new Uint8Array(size);
    const hash = createHash("sha256");
    let offset = 0;
    while (offset < size) {
      const result = await handle.read(bytes, offset, size - offset, offset);
      if (result.bytesRead <= 0) {
        throw new Error("Durable outbound source ended before its declared size");
      }
      hash.update(bytes.subarray(offset, offset + result.bytesRead));
      offset += result.bytesRead;
    }
    await options.afterRead?.();
    const after = await handle.stat();
    if (!sourceIdentityMatches(before, after)) {
      throw new Error("Durable outbound source changed while being read");
    }
    return { bytes, sha256: hash.digest("hex") };
  } finally {
    await handle.close();
  }
}

async function transformTelegramDurableOutboundReply(
  reply: TelegramDurableOutboundReplyPlan,
  transformReply: TelegramDurableOutboundCommitDeps["transformReply"],
): Promise<TelegramDurableOutboundReplyPlan> {
  const markdown = reply.markdown
    ? await transformReply(reply.markdown)
    : reply.markdown;
  const buttons = [];
  for (const button of reply.buttons) {
    buttons.push({
      ...button,
      label: await transformReply(button.label),
    });
  }
  return { ...reply, markdown, buttons };
}

/**
 * Transform the semantic reply, capture every source, publish payload and
 * spools atomically, then re-read the plan through digest verification.
 */
export async function commitTelegramDurableOutbound(
  options: TelegramDurableOutboundPlanOptions,
  deps: TelegramDurableOutboundCommitDeps,
): Promise<RecoveryOutboundDrainItem> {
  const semanticReply = planTelegramDurableOutboundReply(options.finalMarkdown, {
    automaticVoice: options.automaticVoice,
  });
  const transformedReply = await transformTelegramDurableOutboundReply(
    semanticReply,
    deps.transformReply,
  );
  const planned = planTelegramDurableOutboundFromReply(options, transformedReply);
  const captured: TelegramDurableOutboundSourceBytes[] = [];
  for (const [index, source] of planned.spoolSources.entries()) {
    captured.push(
      await readTelegramDurableOutboundSource(source, {
        maxSourceBytes: deps.maxSourceBytes,
        openSource: deps.openSource,
        afterRead: () => deps.afterSourceRead?.(source, index),
      }),
    );
  }
  const record: RecoveryOutboundRecord = deps.store.planOutbound({
    ...planned.recoveryPlan,
    spool: captured.map((entry) => entry.bytes),
  });
  const committed = deps.store
    .listClaimableOutboundRecords(options.claim)
    .find((item) => item.record.recordId === record.recordId);
  if (!committed) {
    throw new Error("Committed durable outbound plan could not be verified");
  }
  return committed;
}
