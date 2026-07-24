/**
 * Durable Telegram outbound planning and source spooling
 * Zones: telegram outbound, recovery, filesystem
 * Owns final-reply unit planning, descriptor-based source capture, and receipt-aware
 * one-unit Telegram execution; it does not own agent lifecycle or entrypoint wiring.
 */

import { createHash, randomUUID } from "node:crypto";
import type { Stats } from "node:fs";
import { mkdir, open, unlink, writeFile } from "node:fs/promises";
import { basename, isAbsolute, join } from "node:path";

import {
  cleanupTelegramOperationOwnedPrivateFiles,
  createTelegramOperationOwnedPrivateFile,
  type TelegramOperationOwnedPrivateFile,
} from "./operation-files.ts";
export type { TelegramOperationOwnedPrivateFile } from "./operation-files.ts";
import {
  getTelegramGuestAttachmentTransport,
  getTelegramMultipartTargetFields,
  getTelegramRichOutboundAttachmentMediaKind,
  isTelegramOutboundPhotoAttachmentPath,
  isTelegramRichAttachmentCommitUnknownError,
  sendTelegramOutboundBinaryReplyUnit,
  TELEGRAM_OUTBOUND_ATTACHMENT_MAX_BYTES,
  type TelegramOutboundBinaryReplyUnitDeps,
} from "./outbound-attachments.ts";
import {
  planTelegramDurableOutboundReply,
  type TelegramDurableOutboundReplyPlan,
} from "./outbound.ts";
import {
  RecoverySnapshotCommitUnknownError,
  type RecoveryOutboundDrainItem,
  type RecoveryOutboundMediaKind,
  type RecoveryOutboundPlanInput,
  type RecoveryIdentity,
  type RecoveryIdentityClaim,
  type RecoveryOutboundButton,
  type RecoveryOutboundReceipt,
  type RecoveryOutboundReceiptResult,
  type RecoveryOutboundRecord,
  type RecoveryOutboundUncertaintyReason,
  type RecoveryOutboundUnit,
  type RecoveryStore,
} from "./recovery.ts";
import { renderTelegramMessage } from "./rendering.ts";
import {
  normalizeTelegramNativeMarkdown,
  sendTelegramGuestMarkdownReplyUnit,
  sendTelegramNativeMarkdownReplyUnit,
  sendTelegramRenderedReplyUnit,
  splitTelegramNativeMarkdown,
  TelegramReplyMalformedSuccessError,
} from "./replies.ts";
import {
  isTelegramApiCommitUnknownError,
  TelegramApiHttpError,
  type TelegramAnswerGuestQueryOptions,
  type TelegramGuestCachedMediaResult,
  type TelegramSendMessageBody,
  type TelegramSendRichMessageBody,
  type TelegramSentMessage,
} from "./telegram-api.ts";
import type { TelegramTarget } from "./target.ts";

export interface TelegramDurableOutboundSourceDescriptor {
  path: string;
  fileName: string;
  caption?: string;
  cleanup?: () => void | Promise<void>;
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
  voiceFallbackToText?: boolean;
  guestQueryId?: string;
  guestStagingTarget?: TelegramTarget;
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
  operationTempDir?: string;
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

export const TELEGRAM_GUEST_FULL_RESPONSE_CAPTION =
  "Full response attached.";
const TELEGRAM_GUEST_FULL_RESPONSE_FILE_NAME = "full-response.md";

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

interface TelegramDurableOutboundTransformedReply
  extends TelegramDurableOutboundReplyPlan {
  fallbackMarkdown?: string;
}

function planTelegramDurableOutboundFromReply(
  options: TelegramDurableOutboundPlanOptions,
  reply: TelegramDurableOutboundTransformedReply,
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
    if (!options.guestStagingTarget || options.guestStagingTarget.chatId < 1) {
      throw new Error("Durable Guest outbound requires a private staging target");
    }
    if (attachments.length > 1) {
      throw new Error("Durable Guest outbound supports one attachment");
    }
    const attachment = attachments[0];
    let guestSource: TelegramDurableOutboundSourceDescriptor | undefined;
    let guestMediaKind: RecoveryOutboundMediaKind | undefined;
    if (attachment) {
      if (generatedVoice.length > 0) {
        throw new Error("Durable Guest attachment plan cannot include generated voice");
      }
      guestSource = attachment;
      guestMediaKind = getGuestMediaKind(attachment.path);
    } else if (reply.voiceReplies.length > 0) {
      if (generatedVoice.length !== 1) {
        throw new Error("Durable Guest voice plan requires one generated voice source");
      }
      guestSource = generatedVoice[0];
      if (!guestSource) throw new Error("Durable Guest voice source is missing");
      assertGeneratedVoiceSource(guestSource);
      guestMediaKind = "voice";
    }
    if (guestSource && guestMediaKind) {
      const spoolRefIndex = addSource(guestSource);
      const stageOperationId = operationId(0);
      const answerOperationId = operationId(1);
      const cleanupOperationId = operationId(2);
      const fallbackText = reply.markdown || reply.fallbackMarkdown;
      const hasFallback = !!fallbackText;
      const extractionCleanupOperationId = operationId(3);
      const fallbackOperationId = operationId(4);
      const stageMethod = guestMediaKind === "photo"
        ? "sendPhoto"
        : guestMediaKind === "audio"
          ? "sendAudio"
          : guestMediaKind === "voice"
            ? "sendVoice"
            : "sendDocument";
      units.push({
        kind: "guest-stage",
        operationId: stageOperationId,
        method: stageMethod,
        spoolRefIndex,
        fileName: guestSource.fileName,
        mediaKind: guestMediaKind,
        ...(hasFallback
          ? {
              branch: {
                knownFailure: {
                  kind: "operation" as const,
                  operationId: fallbackOperationId,
                },
                receiptFailure: {
                  kind: "operation" as const,
                  operationId: extractionCleanupOperationId,
                },
              },
            }
          : {}),
      });
      units.push({
        kind: "guest-answer",
        operationId: answerOperationId,
        method: "answerGuestQuery",
        stageOperationId,
        fileName: guestSource.fileName,
        mediaKind: guestMediaKind,
        ...(guestSource.caption !== undefined
          ? { caption: capGuestCaption(guestSource.caption) }
          : reply.markdown
            ? { caption: capGuestCaption(reply.markdown) }
            : {}),
        branch: {
          success: { kind: "operation", operationId: cleanupOperationId },
        },
      });
      units.push({
        kind: "guest-cleanup",
        operationId: cleanupOperationId,
        method: "deleteMessage",
        stageOperationId,
        branch: { success: { kind: "terminal" } },
      });
      if (hasFallback) {
        units.push({
          kind: "guest-cleanup",
          operationId: extractionCleanupOperationId,
          method: "deleteMessage",
          stageOperationId,
        });
        units.push({
          kind: "guest-text",
          operationId: fallbackOperationId,
          method: "answerGuestQuery",
          markdown: fallbackText,
        });
      }
    } else if (textChunks[0]) {
      if (generatedVoice.length > 0) {
        throw new Error("Durable Guest text plan cannot include generated voice");
      }
      units.push({
        kind: "guest-text",
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
        guestStagingTarget: options.guestStagingTarget,
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
    const firstFallbackUnit = units[1];
    if (!firstFallbackUnit) {
      throw new Error("Durable Rich media fallback is empty");
    }
    richUnit.branch = {
      success: { kind: "terminal" },
      knownFailure: {
        kind: "operation",
        operationId: firstFallbackUnit.operationId,
      },
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
    const voiceUnitIndices: number[] = [];
    for (const voiceSource of generatedVoice) {
      assertGeneratedVoiceSource(voiceSource);
      const spoolRefIndex = addSource(voiceSource);
      voiceUnitIndices.push(units.length);
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
      textChunks.length === 0 &&
      attachments.length === 0 &&
      voiceUnitIndices.length > 0
    ) {
      const fallbackChunks = getTextChunks(
        reply.fallbackMarkdown ?? reply.voiceText ?? "",
        options.renderingMode,
      );
      const firstFallbackIndex = units.length;
      for (const chunk of fallbackChunks) {
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
      const firstFallback = units[firstFallbackIndex];
      if (!firstFallback) {
        throw new Error("Durable voice-only delivery requires text fallback");
      }
      for (const voiceIndex of voiceUnitIndices) {
        const voiceUnit = units[voiceIndex];
        if (voiceUnit?.kind !== "voice") {
          throw new Error("Durable voice fallback root is missing");
        }
        voiceUnit.branch = {
          knownFailure: {
            kind: "operation",
            operationId: firstFallback.operationId,
          },
          ...(voiceIndex === voiceUnitIndices.at(-1)
            ? { success: { kind: "terminal" as const } }
            : {}),
        };
      }
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
  const reply = planTelegramDurableOutboundReply(options.finalMarkdown, {
    automaticVoice: options.automaticVoice,
  });
  return planTelegramDurableOutboundFromReply(options, {
    ...reply,
    ...(!reply.markdown && reply.voiceText
      ? { fallbackMarkdown: reply.voiceText }
      : {}),
  });
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
): Promise<TelegramDurableOutboundTransformedReply> {
  const markdown = reply.markdown
    ? await transformReply(reply.markdown)
    : reply.markdown;
  const fallbackMarkdown = !markdown && reply.voiceText
    ? await transformReply(reply.voiceText)
    : undefined;
  const buttons = [];
  for (const button of reply.buttons) {
    buttons.push({
      ...button,
      label: await transformReply(button.label),
    });
  }
  return {
    ...reply,
    markdown,
    buttons,
    ...(fallbackMarkdown ? { fallbackMarkdown } : {}),
  };
}

export interface TelegramDurableOutboundCommittedIntent
  extends RecoveryOutboundDrainItem {
  operationOwnedFiles: readonly TelegramOperationOwnedPrivateFile[];
}

export interface TelegramDurableOutboundOperationFileRuntime {
  track(
    turnId: string,
    files: readonly TelegramOperationOwnedPrivateFile[],
  ): void;
  resolveTerminal(turnId: string): Promise<void>;
}

/** Retains only this operation's cleanup capabilities until durable terminal state. */
export function createTelegramDurableOutboundOperationFileRuntime(deps: {
  operationTempDir?: string;
  recordCleanupFailure?: (error: unknown, turnId: string) => void;
} = {}): TelegramDurableOutboundOperationFileRuntime {
  const filesByTurnId = new Map<
    string,
    readonly TelegramOperationOwnedPrivateFile[]
  >();
  return {
    track(turnId, files): void {
      if (files.length === 0) return;
      if (filesByTurnId.has(turnId)) {
        throw new Error("Durable outbound turn already owns operation files");
      }
      filesByTurnId.set(turnId, [...files]);
    },
    async resolveTerminal(turnId): Promise<void> {
      const files = filesByTurnId.get(turnId) ?? [];
      filesByTurnId.delete(turnId);
      for (const file of files) {
        try {
          await file.cleanup();
        } catch (error) {
          deps.recordCleanupFailure?.(error, turnId);
        }
      }
      if (deps.operationTempDir) {
        try {
          await cleanupTelegramOperationOwnedPrivateFiles({
            directory: deps.operationTempDir,
            operationKey: turnId,
            prefix: "guest-response",
          });
        } catch (error) {
          deps.recordCleanupFailure?.(error, turnId);
        }
      }
    },
  };
}

async function cleanupTelegramOperationOwnedFiles(
  files: readonly TelegramOperationOwnedPrivateFile[],
): Promise<void> {
  const results = await Promise.allSettled(files.map((file) => file.cleanup()));
  const failure = results.find(
    (result): result is PromiseRejectedResult => result.status === "rejected",
  );
  if (failure) throw failure.reason;
}

/**
 * Transform the semantic reply, capture every source, publish payload and
 * spools atomically, then re-read the plan through digest verification.
 */
export async function commitTelegramDurableOutbound(
  options: TelegramDurableOutboundPlanOptions,
  deps: TelegramDurableOutboundCommitDeps,
): Promise<TelegramDurableOutboundCommittedIntent> {
  const operationOwnedFiles: TelegramOperationOwnedPrivateFile[] = [];
  const transferredSourceCleanups = (options.generatedVoice ?? []).flatMap(
    (source) => source.cleanup ? [source.cleanup] : [],
  );
  let durablePublicationConfirmed = false;
  try {
    let semanticReply = planTelegramDurableOutboundReply(options.finalMarkdown, {
      automaticVoice: options.automaticVoice,
    });
    if (options.voiceFallbackToText && semanticReply.voiceReplies.length > 0) {
      const fallbackMarkdown = [
        semanticReply.markdown,
        ...semanticReply.voiceReplies.map((reply) => reply.text),
      ].filter((part) => part.trim().length > 0).join("\n\n");
      semanticReply = {
        markdown: fallbackMarkdown,
        buttons: semanticReply.buttons,
        voiceReplies: [],
        automaticVoice: false,
      };
    }
    const transformedReply = await transformTelegramDurableOutboundReply(
      semanticReply,
      deps.transformReply,
    );
    let planningOptions = options;
    if (
      options.guestQueryId &&
      options.queuedAttachments.length === 0 &&
      splitTelegramNativeMarkdown(transformedReply.markdown).length > 1
    ) {
      if (!deps.operationTempDir) {
        throw new Error(
          "Oversized durable Guest response requires an operation temp directory",
        );
      }
      const document = await createTelegramOperationOwnedPrivateFile({
        directory: deps.operationTempDir,
        fileName: TELEGRAM_GUEST_FULL_RESPONSE_FILE_NAME,
        bytes: new TextEncoder().encode(transformedReply.markdown),
        operationKey: options.turnId,
        prefix: "guest-response",
      });
      operationOwnedFiles.push(document);
      planningOptions = {
        ...options,
        queuedAttachments: [{
          path: document.path,
          fileName: document.fileName,
          caption: TELEGRAM_GUEST_FULL_RESPONSE_CAPTION,
        }],
        generatedVoice: [],
      };
    }
    const planned = planTelegramDurableOutboundFromReply(
      planningOptions,
      transformedReply,
    );
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
    durablePublicationConfirmed = true;
    const committed = deps.store
      .listClaimableOutboundRecords(options.claim)
      .find((item) => item.record.recordId === record.recordId);
    if (!committed) {
      throw new Error("Committed durable outbound plan could not be verified");
    }
    await Promise.allSettled(
      transferredSourceCleanups.map((cleanup) => cleanup()),
    );
    return { ...committed, operationOwnedFiles };
  } catch (error) {
    if (
      !durablePublicationConfirmed &&
      !(error instanceof RecoverySnapshotCommitUnknownError)
    ) {
      await cleanupTelegramOperationOwnedFiles(operationOwnedFiles).catch(
        () => {},
      );
      await Promise.allSettled(
        transferredSourceCleanups.map((cleanup) => cleanup()),
      );
    }
    throw error;
  }
}

export type TelegramDurableOutboundAdapterOutcome =
  | {
      kind: "committed";
      method: string;
      messageId?: number;
      result?: RecoveryOutboundReceiptResult;
      transition?: "success" | "receipt-failure";
    }
  | { kind: "known-not-committed" }
  | {
      kind: "commit-unknown";
      reason: RecoveryOutboundUncertaintyReason;
    }
  | { kind: "not-started" };

export interface TelegramDurableOutboundAdapterInput {
  identity: RecoveryIdentity;
  unit: RecoveryOutboundUnit;
  spool: readonly Uint8Array[];
  receipts: readonly RecoveryOutboundReceipt[];
  replyToMessageId?: number;
  replyMarkup?: unknown;
  guestQueryId?: string;
  guestStagingTarget?: TelegramTarget;
}

export interface TelegramDurableOutboundUnitAdapter {
  execute: (
    input: TelegramDurableOutboundAdapterInput,
  ) => Promise<TelegramDurableOutboundAdapterOutcome>;
}

export interface TelegramDurableOutboundMutationGate {
  canStart: (
    identity: RecoveryIdentity,
    unit: RecoveryOutboundUnit,
  ) => boolean;
  isActive: (identity: RecoveryIdentity) => boolean;
}

export interface TelegramDurableOutboundUnitAdapterDeps
  extends TelegramOutboundBinaryReplyUnitDeps {
  gate: TelegramDurableOutboundMutationGate;
  sendMessage: (
    body: TelegramSendMessageBody,
  ) => Promise<TelegramSentMessage>;
  sendRichMessage: (
    body: TelegramSendRichMessageBody,
  ) => Promise<TelegramSentMessage>;
  answerGuestQuery: (
    guestQueryId: string,
    text?: string,
    options?: TelegramAnswerGuestQueryOptions,
  ) => Promise<void>;
  deleteMessage: (chatId: number, messageId: number) => Promise<void>;
}

function getSpoolBytes(
  input: TelegramDurableOutboundAdapterInput,
  spoolRefIndex: number,
): Uint8Array {
  const bytes = input.spool[spoolRefIndex];
  if (!bytes) {
    throw new Error("Verified durable outbound spool is missing");
  }
  return bytes;
}

function classifyTelegramDurableOutboundError(
  error: unknown,
  started: boolean,
): TelegramDurableOutboundAdapterOutcome {
  if (!started) return { kind: "not-started" };
  if (isTelegramApiCommitUnknownError(error)) {
    return { kind: "commit-unknown", reason: error.reason };
  }
  if (
    error instanceof TelegramApiHttpError &&
    error.status !== undefined &&
    error.status < 500
  ) {
    return { kind: "known-not-committed" };
  }
  if (error instanceof TelegramReplyMalformedSuccessError) {
    return { kind: "commit-unknown", reason: "malformed-success" };
  }
  if (isTelegramRichAttachmentCommitUnknownError(error)) {
    if (error instanceof Error && isTelegramApiCommitUnknownError(error.cause)) {
      return { kind: "commit-unknown", reason: error.cause.reason };
    }
    return { kind: "commit-unknown", reason: "malformed-success" };
  }
  return { kind: "commit-unknown", reason: "commit-unknown" };
}

interface TelegramGuestStagingMessage {
  message_id?: number;
  document?: { file_id?: string };
  photo?: Array<{ file_id?: string; file_size?: number }>;
  audio?: { file_id?: string };
  voice?: { file_id?: string };
}

function readGuestStagingResult(
  value: unknown,
  mediaKind: RecoveryOutboundMediaKind,
): Extract<RecoveryOutboundReceiptResult, { kind: "guest-staging" }> {
  if (typeof value !== "object" || value === null) {
    throw new TelegramReplyMalformedSuccessError("Guest staging upload");
  }
  const message = value as TelegramGuestStagingMessage;
  const stagingMessageId = message.message_id;
  if (
    typeof stagingMessageId !== "number" ||
    !Number.isSafeInteger(stagingMessageId) ||
    stagingMessageId <= 0
  ) {
    throw new TelegramReplyMalformedSuccessError("Guest staging upload");
  }
  let fileId: string | undefined;
  if (mediaKind === "photo") {
    fileId = [...(message.photo ?? [])]
      .sort((left, right) => (left.file_size ?? 0) - (right.file_size ?? 0))
      .at(-1)?.file_id;
  } else if (mediaKind === "audio") {
    fileId = message.audio?.file_id;
  } else if (mediaKind === "voice") {
    fileId = message.voice?.file_id;
  } else if (mediaKind === "document") {
    fileId = message.document?.file_id;
  }
  return {
    kind: "guest-staging",
    stagingMessageId,
    mediaKind,
    ...(fileId ? { fileId } : {}),
  };
}

function findGuestStagingReceipt(
  input: TelegramDurableOutboundAdapterInput,
  stageOperationId: string,
): Extract<RecoveryOutboundReceiptResult, { kind: "guest-staging" }> {
  const result = input.receipts.find(
    (receipt) => receipt.operationId === stageOperationId,
  )?.result;
  if (result?.kind !== "guest-staging") {
    throw new Error("Durable Guest staging receipt is missing");
  }
  return result;
}

function buildGuestCachedMediaResult(
  unit: Extract<RecoveryOutboundUnit, { kind: "guest-answer" }>,
  fileId: string,
): TelegramGuestCachedMediaResult {
  const caption = unit.caption ? { caption: unit.caption } : {};
  if (unit.mediaKind === "photo") {
    return {
      type: "photo",
      id: "attachment-1",
      photo_file_id: fileId,
      ...caption,
    };
  }
  if (unit.mediaKind === "audio") {
    return {
      type: "audio",
      id: "attachment-1",
      audio_file_id: fileId,
      ...caption,
    };
  }
  if (unit.mediaKind === "voice") {
    return {
      type: "voice",
      id: "attachment-1",
      voice_file_id: fileId,
      title: unit.fileName,
      ...caption,
    };
  }
  return {
    type: "document",
    id: "attachment-1",
    title: unit.fileName,
    document_file_id: fileId,
    ...caption,
  };
}

/**
 * Creates the concrete one-unit adapter. Every branch invokes exactly one
 * non-idempotent Telegram mutation port after the injected gate admits it.
 */
export function createTelegramDurableOutboundUnitAdapter(
  deps: TelegramDurableOutboundUnitAdapterDeps,
): TelegramDurableOutboundUnitAdapter {
  return {
    async execute(input) {
      let started = false;
      const startMutation = (): void => {
        if (started) {
          throw new Error("Durable outbound unit attempted multiple mutations");
        }
        if (!deps.gate.canStart(input.identity, input.unit)) {
          throw new Error("Durable outbound mutation gate denied start");
        }
        started = true;
      };
      const unitDeps = {
        sendMessage: (body: TelegramSendMessageBody) => {
          startMutation();
          return deps.sendMessage(body);
        },
        sendRichMessage: (body: TelegramSendRichMessageBody) => {
          startMutation();
          return deps.sendRichMessage(body);
        },
        sendMultipartBytes: (
          method: string,
          fields: Record<string, string>,
          fileField: string,
          bytes: Uint8Array,
          fileName: string,
        ) => {
          startMutation();
          return deps.sendMultipartBytes(
            method,
            fields,
            fileField,
            bytes,
            fileName,
          );
        },
        answerGuestQuery: (
          guestQueryId: string,
          text?: string,
          options?: TelegramAnswerGuestQueryOptions,
        ) => {
          startMutation();
          return deps.answerGuestQuery(guestQueryId, text, options);
        },
      };
      try {
        let receipt: {
          method: string;
          messageId?: number;
          result?: RecoveryOutboundReceiptResult;
          transition?: "success" | "receipt-failure";
        };
        const unit = input.unit;
        if (unit.kind === "final-text") {
          receipt = unit.method === "sendRichMessage"
            ? await sendTelegramNativeMarkdownReplyUnit(
                input.identity.target.chatId,
                unit.content,
                unitDeps,
                {
                  target: input.identity.target,
                  replyToMessageId: input.replyToMessageId,
                  replyMarkup: input.replyMarkup,
                },
              )
            : await sendTelegramRenderedReplyUnit(
                input.identity.target.chatId,
                unit.content,
                unit.contentMode === "html" ? "html" : "plain",
                unitDeps,
                {
                  target: input.identity.target,
                  replyToMessageId: input.replyToMessageId,
                  replyMarkup: input.replyMarkup,
                },
              );
        } else if (unit.kind === "rich-media") {
          receipt = await sendTelegramOutboundBinaryReplyUnit(
            {
              method: "sendRichMessage",
              chatId: input.identity.target.chatId,
              target: input.identity.target,
              replyToMessageId: input.replyToMessageId,
              replyMarkup: input.replyMarkup,
              bytes: getSpoolBytes(input, unit.spoolRefIndex),
              fileName: unit.fileName,
              mediaKind: unit.mediaKind,
              ...(unit.caption !== undefined ? { caption: unit.caption } : {}),
            },
            unitDeps,
          );
        } else if (unit.kind === "attachment" || unit.kind === "voice") {
          const method = unit.kind === "voice"
            ? "sendVoice"
            : unit.mediaKind === "photo"
              ? "sendPhoto"
              : "sendDocument";
          receipt = await sendTelegramOutboundBinaryReplyUnit(
            {
              method,
              chatId: input.identity.target.chatId,
              target: input.identity.target,
              replyToMessageId: input.replyToMessageId,
              ...(unit.kind === "voice" && input.replyMarkup !== undefined
                ? { replyMarkup: input.replyMarkup }
                : {}),
              bytes: getSpoolBytes(input, unit.spoolRefIndex),
              fileName: unit.fileName,
              mediaKind: unit.mediaKind,
              ...(unit.caption !== undefined ? { caption: unit.caption } : {}),
            },
            unitDeps,
          );
        } else if (unit.kind === "guest-text") {
          if (!input.guestQueryId) {
            throw new Error("Durable Guest query identity is missing");
          }
          receipt = await sendTelegramGuestMarkdownReplyUnit(
            input.guestQueryId,
            unit.markdown,
            unitDeps,
          );
        } else if (unit.kind === "guest-stage") {
          if (!input.guestStagingTarget) {
            throw new Error("Durable Guest staging target is missing");
          }
          const fileField = unit.mediaKind === "photo"
            ? "photo"
            : unit.mediaKind === "audio"
              ? "audio"
              : unit.mediaKind === "voice"
                ? "voice"
                : "document";
          const result = await unitDeps.sendMultipartBytes(
            unit.method,
            {
              chat_id: String(input.guestStagingTarget.chatId),
              ...getTelegramMultipartTargetFields(input.guestStagingTarget),
            },
            fileField,
            getSpoolBytes(input, unit.spoolRefIndex),
            unit.fileName,
          );
          const staging = readGuestStagingResult(result, unit.mediaKind);
          if (!staging.fileId && !unit.branch?.receiptFailure) {
            throw new TelegramReplyMalformedSuccessError(
              "Guest staging upload file id",
            );
          }
          receipt = {
            method: unit.method,
            messageId: staging.stagingMessageId,
            result: staging,
            ...(!staging.fileId
              ? { transition: "receipt-failure" as const }
              : {}),
          };
        } else if (unit.kind === "guest-answer") {
          if (!input.guestQueryId) {
            throw new Error("Durable Guest query identity is missing");
          }
          const staging = findGuestStagingReceipt(
            input,
            unit.stageOperationId,
          );
          if (!staging.fileId) {
            throw new Error("Durable Guest staged file id is missing");
          }
          await unitDeps.answerGuestQuery(input.guestQueryId, undefined, {
            result: buildGuestCachedMediaResult(unit, staging.fileId),
          });
          receipt = {
            method: "answerGuestQuery",
            result: { kind: "guest-answer" },
          };
        } else if (unit.kind === "guest-cleanup") {
          if (!input.guestStagingTarget) {
            throw new Error("Durable Guest staging target is missing");
          }
          const staging = findGuestStagingReceipt(
            input,
            unit.stageOperationId,
          );
          startMutation();
          await deps.deleteMessage(
            input.guestStagingTarget.chatId,
            staging.stagingMessageId,
          );
          receipt = {
            method: "deleteMessage",
            result: {
              kind: "guest-cleanup",
              stagingMessageId: staging.stagingMessageId,
            },
          };
        } else {
          throw new Error("Unsupported durable outbound unit");
        }
        if (!deps.gate.isActive(input.identity)) {
          return {
            kind: "commit-unknown",
            reason: "authority-lost-after-start",
          };
        }
        if (receipt.method !== unit.method) {
          return { kind: "commit-unknown", reason: "malformed-success" };
        }
        return { kind: "committed", ...receipt };
      } catch (error) {
        return classifyTelegramDurableOutboundError(error, started);
      }
    },
  };
}

export interface TelegramDurableOutboundExecutorDeps {
  store: Pick<
    RecoveryStore,
    | "activateOutbound"
    | "claimOutboundUnit"
    | "recordOutboundReceipt"
    | "releaseOutboundUnitNotStarted"
    | "recordOutboundSafeFailure"
    | "markOutboundUncertain"
  >;
  claim: RecoveryIdentityClaim;
  adapter: TelegramDurableOutboundUnitAdapter;
  createReplyMarkup?: (
    buttons: readonly RecoveryOutboundButton[],
  ) => unknown;
  recordOwnership?: (input: {
    identity: RecoveryIdentity;
    messageId: number;
  }) => void;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
}

export interface TelegramDurableOutboundExecutionResult {
  record: RecoveryOutboundRecord;
  outcome: TelegramDurableOutboundAdapterOutcome;
}

function buildTelegramDurableOutboundAdapterInput(
  claimed: RecoveryOutboundDrainItem,
  deps: TelegramDurableOutboundExecutorDeps,
): TelegramDurableOutboundAdapterInput {
  const execution = claimed.execution;
  return {
    identity: deps.claim.identity,
    unit: execution.unit,
    spool: claimed.spool,
    receipts: claimed.record.receipts,
    ...(execution.isFirstExecutedUnit && execution.replyToMessageId > 0
      ? { replyToMessageId: execution.replyToMessageId }
      : {}),
    ...(execution.guestQueryId
      ? { guestQueryId: execution.guestQueryId }
      : {}),
    ...(execution.guestStagingTarget
      ? { guestStagingTarget: execution.guestStagingTarget }
      : {}),
  };
}

/** Claims and executes exactly one durable outbound unit. */
export async function executeNextTelegramDurableOutboundUnit(
  recordId: string,
  deps: TelegramDurableOutboundExecutorDeps,
): Promise<TelegramDurableOutboundExecutionResult> {
  const activated = deps.store.activateOutbound(recordId, deps.claim);
  const claimed = deps.store.claimOutboundUnit({
    recordId: activated.recordId,
    claim: deps.claim,
  });
  const execution = claimed.execution;
  let outcome: TelegramDurableOutboundAdapterOutcome | undefined;
  let replyMarkup: unknown;
  try {
    if (execution.acceptsFinalButtons && execution.buttons.length > 0) {
      replyMarkup = deps.createReplyMarkup?.(execution.buttons);
      if (replyMarkup === undefined) outcome = { kind: "not-started" };
    }
  } catch {
    outcome = { kind: "not-started" };
  }
  if (!outcome) {
    const input = {
      ...buildTelegramDurableOutboundAdapterInput(claimed, deps),
      ...(replyMarkup !== undefined ? { replyMarkup } : {}),
    };
    try {
      outcome = await deps.adapter.execute(input);
    } catch (error) {
      outcome = classifyTelegramDurableOutboundError(error, true);
    }
  }
  const activeUnit = claimed.record.activeUnit;
  if (!activeUnit) {
    throw new Error("Durable outbound claim returned no active attempt");
  }
  if (outcome.kind === "committed") {
    const unit = execution.unit;
    const hasValidMessageId =
      outcome.messageId !== undefined &&
      Number.isSafeInteger(outcome.messageId) &&
      outcome.messageId > 0;
    const resultMatches = unit.kind === "guest-stage"
      ? hasValidMessageId &&
        outcome.result?.kind === "guest-staging" &&
        outcome.result.stagingMessageId === outcome.messageId &&
        outcome.result.mediaKind === unit.mediaKind
      : unit.kind === "guest-answer"
        ? outcome.messageId === undefined &&
          outcome.result?.kind === "guest-answer"
        : unit.kind === "guest-cleanup"
          ? outcome.messageId === undefined &&
            outcome.result?.kind === "guest-cleanup"
          : unit.kind === "guest-text"
            ? outcome.messageId === undefined && outcome.result === undefined
            : hasValidMessageId && outcome.result === undefined;
    if (outcome.method !== unit.method || !resultMatches) {
      outcome = { kind: "commit-unknown", reason: "malformed-success" };
    } else {
      const committedOutcome = outcome;
      let record: RecoveryOutboundRecord | undefined;
      try {
        record = deps.store.recordOutboundReceipt({
          recordId,
          claim: deps.claim,
          attemptId: activeUnit.attemptId,
          operationId: execution.unit.operationId,
          method: committedOutcome.method,
          ...(committedOutcome.messageId !== undefined
            ? { messageId: committedOutcome.messageId }
            : {}),
          ...(committedOutcome.result
            ? { result: committedOutcome.result }
            : {}),
          ...(committedOutcome.transition
            ? { transition: committedOutcome.transition }
            : {}),
        });
      } catch {
        outcome = {
          kind: "commit-unknown",
          reason: "confirmed-before-receipt",
        };
      }
      if (record) {
        if (
          committedOutcome.messageId !== undefined &&
          execution.unit.kind !== "guest-stage"
        ) {
          try {
            deps.recordOwnership?.({
              identity: deps.claim.identity,
              messageId: committedOutcome.messageId,
            });
          } catch (error) {
            try {
              deps.recordRuntimeEvent?.("delivery", error, {
                phase: "durable-outbound-ownership",
                recordId,
                operationId: execution.unit.operationId,
              });
            } catch {
              // Diagnostics must never roll back or block a committed receipt.
            }
          }
        }
        return { record, outcome: committedOutcome };
      }
    }
  }
  const failureInput = {
    recordId,
    claim: deps.claim,
    attemptId: activeUnit.attemptId,
  };
  const record = outcome.kind === "commit-unknown"
    ? deps.store.markOutboundUncertain({
        ...failureInput,
        reason: outcome.reason,
      })
    : outcome.kind === "not-started"
      ? deps.store.releaseOutboundUnitNotStarted(failureInput)
      : deps.store.recordOutboundSafeFailure(failureInput);
  return { record, outcome };
}

export interface TelegramDurableOutboundMultipartBytesSenderDeps {
  tempDir: string;
  callMultipart: (
    method: string,
    fields: Record<string, string>,
    fileField: string,
    filePath: string,
    fileName: string,
  ) => Promise<unknown>;
}

/** Materializes verified spool bytes only for the duration of the live upload. */
export function createTelegramDurableOutboundMultipartBytesSender(
  deps: TelegramDurableOutboundMultipartBytesSenderDeps,
): TelegramOutboundBinaryReplyUnitDeps["sendMultipartBytes"] {
  return async (method, fields, fileField, bytes, fileName) => {
    await mkdir(deps.tempDir, { recursive: true, mode: 0o700 });
    const path = join(
      deps.tempDir,
      `.outbox-${process.pid}-${randomUUID()}-${normalizePersistedFileName(fileName)}`,
    );
    await writeFile(path, bytes, { mode: 0o600, flag: "wx" });
    try {
      return await deps.callMultipart(
        method,
        fields,
        fileField,
        path,
        fileName,
      );
    } finally {
      await unlink(path).catch(() => {});
    }
  };
}

export interface TelegramDurableOutboundWorkerDeps {
  getStore: () => TelegramDurableOutboundExecutorDeps["store"];
  operationGate: {
    enter(profile: string): { release(): void } | undefined;
  };
  adapter: TelegramDurableOutboundUnitAdapter;
  createReplyMarkup?: TelegramDurableOutboundExecutorDeps["createReplyMarkup"];
  recordOwnership?: TelegramDurableOutboundExecutorDeps["recordOwnership"];
  onTerminal: (input: {
    record: RecoveryOutboundRecord;
    claim: RecoveryIdentityClaim;
  }) => void | Promise<void>;
  recordRuntimeEvent?: TelegramDurableOutboundExecutorDeps["recordRuntimeEvent"];
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  retryAfterFailureMs?: number;
  startSuspended?: boolean;
}

export interface TelegramDurableOutboundWorker {
  register(record: RecoveryOutboundRecord, claim: RecoveryIdentityClaim): void;
  schedule(recordId: string): void;
  suspend(): Promise<void>;
  resume(): Promise<void>;
  isIdle(): boolean;
}

interface TelegramDurableOutboundWorkerEntry {
  record: RecoveryOutboundRecord;
  claim: RecoveryIdentityClaim;
  timer?: ReturnType<typeof setTimeout>;
  running?: Promise<void>;
}

function isTelegramDurableOutboundTerminal(
  state: RecoveryOutboundRecord["state"],
): boolean {
  return state === "delivered" ||
    state === "delivery-uncertain" ||
    state === "explicitly-discarded";
}

/**
 * Serializes each record's one-unit claims, retains exact identity authority,
 * and notifies queue ownership only after a durable terminal disposition.
 */
export function createTelegramDurableOutboundWorker(
  deps: TelegramDurableOutboundWorkerDeps,
): TelegramDurableOutboundWorker {
  const entries = new Map<string, TelegramDurableOutboundWorkerEntry>();
  const terminalTurnIds = new Set<string>();
  const setTimer = deps.setTimer ?? ((callback, delayMs) => setTimeout(callback, delayMs));
  const clearTimer = deps.clearTimer ?? clearTimeout;
  const retryAfterFailureMs = deps.retryAfterFailureMs ?? 1000;
  let suspended = deps.startSuspended ?? false;
  let schedulerGeneration = 0;

  const notifyTerminal = async (
    entry: TelegramDurableOutboundWorkerEntry,
  ): Promise<void> => {
    if (!isTelegramDurableOutboundTerminal(entry.record.state)) return;
    if (terminalTurnIds.has(entry.record.turnId)) {
      entries.delete(entry.record.recordId);
      return;
    }
    await deps.onTerminal({ record: entry.record, claim: entry.claim });
    terminalTurnIds.add(entry.record.turnId);
    entries.delete(entry.record.recordId);
  };

  const scheduleEntry = (
    entry: TelegramDurableOutboundWorkerEntry,
    delayMs: number,
  ): void => {
    if (suspended || entry.running || entry.timer) return;
    const generation = schedulerGeneration;
    entry.timer = setTimer(() => {
      entry.timer = undefined;
      if (suspended || generation !== schedulerGeneration) return;
      const lease = deps.operationGate.enter(entry.claim.identity.profile);
      if (!lease) return;
      let nextDelayMs: number | undefined;
      const running = (async () => {
        try {
          if (isTelegramDurableOutboundTerminal(entry.record.state)) {
            await notifyTerminal(entry);
            return;
          }
          const result = await executeNextTelegramDurableOutboundUnit(
            entry.record.recordId,
            {
              store: deps.getStore(),
              claim: entry.claim,
              adapter: deps.adapter,
              createReplyMarkup: deps.createReplyMarkup,
              recordOwnership: deps.recordOwnership,
              recordRuntimeEvent: deps.recordRuntimeEvent,
            },
          );
          entry.record = result.record;
          if (isTelegramDurableOutboundTerminal(entry.record.state)) {
            await notifyTerminal(entry);
            return;
          }
          if (entry.record.state === "pending") {
            nextDelayMs = 0;
            return;
          }
          if (
            entry.record.state === "retryable-pending" &&
            entry.record.retryNotBeforeMs !== undefined
          ) {
            nextDelayMs = Math.max(
              0,
              entry.record.retryNotBeforeMs - Date.now(),
            );
          }
        } catch (error) {
          deps.recordRuntimeEvent?.("delivery", error, {
            phase: "durable-outbox-worker",
            recordId: entry.record.recordId,
          });
          nextDelayMs = retryAfterFailureMs;
        } finally {
          lease.release();
        }
      })();
      entry.running = running.finally(() => {
        entry.running = undefined;
        if (nextDelayMs !== undefined) scheduleEntry(entry, nextDelayMs);
      });
    }, delayMs);
    entry.timer.unref?.();
  };

  const runtime: TelegramDurableOutboundWorker = {
    register(record, claim) {
      const current = entries.get(record.recordId);
      if (current) {
        current.record = structuredClone(record);
        current.claim = structuredClone(claim);
        return;
      }
      entries.set(record.recordId, {
        record: structuredClone(record),
        claim: structuredClone(claim),
      });
    },
    schedule(recordId) {
      const entry = entries.get(recordId);
      if (!entry) throw new Error("Unknown durable outbound worker record");
      if (isTelegramDurableOutboundTerminal(entry.record.state)) {
        scheduleEntry(entry, 0);
        return;
      }
      const delayMs = entry.record.state === "retryable-pending" &&
          entry.record.retryNotBeforeMs !== undefined
        ? Math.max(0, entry.record.retryNotBeforeMs - Date.now())
        : 0;
      scheduleEntry(entry, delayMs);
    },
    async suspend() {
      suspended = true;
      schedulerGeneration += 1;
      for (const entry of entries.values()) {
        if (entry.timer) {
          clearTimer(entry.timer);
          entry.timer = undefined;
        }
      }
      await Promise.all(
        [...entries.values()].map((entry) => entry.running).filter(
          (running): running is Promise<void> => running !== undefined,
        ),
      );
    },
    async resume() {
      if (!suspended) return;
      suspended = false;
      schedulerGeneration += 1;
      for (const entry of entries.values()) runtime.schedule(entry.record.recordId);
    },
    isIdle() {
      return [...entries.values()].every((entry) => !entry.timer && !entry.running);
    },
  };
  return runtime;
}
