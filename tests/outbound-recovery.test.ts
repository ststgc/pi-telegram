/**
 * Regression tests for durable Telegram outbound planning and source spooling
 * Zones: telegram outbound, recovery, filesystem, test infrastructure
 * Guards deterministic unit order, descriptor mutation fencing, atomic store
 * publication, path-free recovery payloads, and verified spool reconstruction.
 */

import assert from "node:assert/strict";
import { fork, type ChildProcess } from "node:child_process";
import type { Stats } from "node:fs";
import { access, mkdtemp, readFile, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  commitTelegramDurableOutbound,
  createTelegramDurableOutboundOperationFileRuntime,
  createTelegramDurableOutboundUnitAdapter,
  createTelegramDurableOutboundWorker,
  executeNextTelegramDurableOutboundUnit,
  planTelegramDurableOutbound,
  readTelegramDurableOutboundSource,
  type TelegramDurableOutboundAdapterInput,
  type TelegramDurableOutboundPlanOptions,
  TELEGRAM_GUEST_FULL_RESPONSE_CAPTION,
  type TelegramDurableOutboundUnitAdapterDeps,
} from "../lib/outbound-recovery.ts";
import {
  openRecoveryStore,
  RecoveryProfileOperationGate,
  RecoveryQuotaExceededError,
  type RecoveryIdentity,
  type RecoveryOutboundUnit,
  type RecoveryStore,
} from "../lib/recovery.ts";
import {
  TelegramApiCommitUnknownError,
  TelegramApiHttpError,
} from "../lib/telegram-api.ts";

const identityTransform = async (text: string): Promise<string> => text;

const IDENTITY: RecoveryIdentity = {
  profile: "default",
  target: { chatId: 100, threadId: 7 },
  owner: {
    kind: "manual-follower",
    ownerId: "manual-owner",
    registrationGeneration: "registration-1",
  },
  sessionGeneration: 2,
};

interface DurableHarness {
  tempDir: string;
  store: RecoveryStore;
  inboundRecordId: string;
  turnId: string;
}

async function createHarness(quotaBytes?: number): Promise<DurableHarness> {
  const tempDir = await mkdtemp(join(tmpdir(), "pi-telegram-outbound-recovery-"));
  let nextId = 0;
  const store = openRecoveryStore({
    profile: "default",
    rootPath: join(tempDir, "recovery-v1"),
    quotaBytes,
    randomId: () => `durable-id-${String(++nextId).padStart(4, "0")}`,
    isIdentityAuthenticated: () => true,
  });
  const inbound = store.observeInbound(900, IDENTITY);
  store.admitInbound({
    recordId: inbound.recordId,
    payload: Buffer.from("durable source turn"),
  });
  store.markPreDispatch(inbound.recordId, { identity: IDENTITY });
  store.markDispatching(inbound.recordId, { identity: IDENTITY });
  return {
    tempDir,
    store,
    inboundRecordId: inbound.recordId,
    turnId: inbound.turnId,
  };
}

function baseOptions(
  harness: Pick<DurableHarness, "inboundRecordId" | "turnId">,
  overrides: Partial<TelegramDurableOutboundPlanOptions> = {},
): TelegramDurableOutboundPlanOptions {
  return {
    intentId: "intent-900",
    turnId: harness.turnId,
    sourceInboundRecordIds: [harness.inboundRecordId],
    claim: { identity: IDENTITY },
    replyToMessageId: 45,
    renderingMode: "rich",
    finalMarkdown: "Final answer.",
    queuedAttachments: [],
    ...overrides,
  };
}

async function removeHarness(harness: DurableHarness): Promise<void> {
  await rm(harness.tempDir, { recursive: true, force: true });
}

function createUnitAdapterHarness(
  overrides: Partial<TelegramDurableOutboundUnitAdapterDeps> = {},
) {
  const calls: string[] = [];
  const base: TelegramDurableOutboundUnitAdapterDeps = {
    gate: {
      canStart: () => true,
      isActive: () => true,
    },
    sendMessage: async () => {
      calls.push("sendMessage");
      return { message_id: 101 };
    },
    sendRichMessage: async () => {
      calls.push("sendRichMessage");
      return { message_id: 102 };
    },
    sendMultipartBytes: async (method) => {
      calls.push(method);
      return { message_id: 103 };
    },
    answerGuestQuery: async () => {
      calls.push("answerGuestQuery");
    },
    deleteMessage: async () => {
      calls.push("deleteMessage");
    },
  };
  return {
    calls,
    adapter: createTelegramDurableOutboundUnitAdapter({
      ...base,
      ...overrides,
      gate: overrides.gate ?? base.gate,
    }),
  };
}

function adapterInput(
  unit: RecoveryOutboundUnit,
  overrides: Partial<TelegramDurableOutboundAdapterInput> = {},
): TelegramDurableOutboundAdapterInput {
  return {
    identity: IDENTITY,
    unit,
    spool: [],
    receipts: [],
    ...overrides,
  };
}

test("durable outbound planner is deterministic and preserves rich versus ordinary unit order", () => {
  const harnessView = { inboundRecordId: "inbound-1", turnId: "turn-1" };
  const richOptions = baseOptions(harnessView, {
    finalMarkdown: [
      "Final **answer**.",
      "",
      '<!-- telegram_button label=Continue prompt="Continue safely." -->',
    ].join("\n"),
    queuedAttachments: [{ path: "/private/source/report.png", fileName: "report.png" }],
  });
  const first = planTelegramDurableOutbound(richOptions);
  const repeated = planTelegramDurableOutbound(richOptions);
  assert.deepEqual(repeated, first);
  assert.deepEqual(first.recoveryPlan.units, [
    {
      kind: "rich-media",
      operationId: "outbound-unit-0000",
      method: "sendRichMessage",
      spoolRefIndex: 0,
      fileName: "report.png",
      mediaKind: "photo",
      caption: "Final **answer**.",
      branch: {
        success: { kind: "terminal" },
        knownFailure: {
          kind: "operation",
          operationId: "outbound-unit-0001",
        },
      },
    },
    {
      kind: "final-text",
      operationId: "outbound-unit-0001",
      method: "sendRichMessage",
      content: "Final **answer**.",
      contentMode: "rich-markdown",
    },
    {
      kind: "attachment",
      operationId: "outbound-unit-0002",
      method: "sendPhoto",
      spoolRefIndex: 0,
      fileName: "report.png",
      mediaKind: "photo",
    },
  ]);
  assert.deepEqual(first.recoveryPlan.buttons, [
    { label: "Continue", prompt: "Continue safely." },
  ]);

  const ordinary = planTelegramDurableOutbound(baseOptions(harnessView, {
    renderingMode: "html",
    queuedAttachments: [
      { path: "/private/source/first.webp", fileName: "first.webp" },
      { path: "/private/source/second.zip", fileName: "second.zip" },
    ],
  }));
  assert.deepEqual(
    ordinary.recoveryPlan.units.map((unit) => [unit.kind, unit.method]),
    [
      ["final-text", "sendMessage"],
      ["attachment", "sendPhoto"],
      ["attachment", "sendDocument"],
    ],
  );
  assert.deepEqual(
    ordinary.recoveryPlan.units.map((unit) => unit.operationId),
    ["outbound-unit-0000", "outbound-unit-0001", "outbound-unit-0002"],
  );

  const attachmentsOnly = planTelegramDurableOutbound(baseOptions(harnessView, {
    finalMarkdown: "",
    queuedAttachments: [
      { path: "/private/source/result.txt", fileName: "result.txt" },
    ],
  }));
  assert.deepEqual(
    attachmentsOnly.recoveryPlan.units.map((unit) => [unit.kind, unit.method]),
    [
      ["final-text", "sendMessage"],
      ["attachment", "sendDocument"],
    ],
  );
});

test("durable outbound planner preserves generated voice and Guest precedence", () => {
  const harnessView = { inboundRecordId: "inbound-2", turnId: "turn-2" };
  const voice = planTelegramDurableOutbound(baseOptions(harnessView, {
    finalMarkdown: [
      "Visible answer.",
      "",
      "<!-- telegram_voice: Spoken summary. -->",
    ].join("\n"),
    generatedVoice: [
      { path: "/private/source/summary.ogg", fileName: "summary.ogg" },
    ],
    queuedAttachments: [
      { path: "/private/source/report.txt", fileName: "report.txt" },
    ],
  }));
  assert.deepEqual(
    voice.recoveryPlan.units.map((unit) => [unit.kind, unit.method]),
    [
      ["final-text", "sendRichMessage"],
      ["voice", "sendVoice"],
      ["attachment", "sendDocument"],
    ],
  );
  assert.deepEqual(voice.recoveryPlan.voice, {
    text: "Spoken summary.",
    automatic: false,
  });

  const guestAttachment = planTelegramDurableOutbound(baseOptions(harnessView, {
    replyToMessageId: 0,
    guestQueryId: "guest-query-1",
    guestStagingTarget: { chatId: 840585 },
    finalMarkdown: "Guest caption.",
    queuedAttachments: [
      { path: "/private/source/guest.mp3", fileName: "guest.mp3" },
    ],
  }));
  assert.deepEqual(guestAttachment.recoveryPlan.guestStagingTarget, {
    chatId: 840585,
  });
  assert.throws(
    () => planTelegramDurableOutbound(baseOptions(harnessView, {
      replyToMessageId: 0,
      guestQueryId: "guest-query-without-private-stage",
      finalMarkdown: "Guest answer.",
    })),
    /private staging target/,
  );
  assert.deepEqual(
    guestAttachment.recoveryPlan.units.map((unit) => [unit.kind, unit.method]),
    [
      ["guest-stage", "sendAudio"],
      ["guest-answer", "answerGuestQuery"],
      ["guest-cleanup", "deleteMessage"],
      ["guest-cleanup", "deleteMessage"],
      ["guest-text", "answerGuestQuery"],
    ],
  );
  assert.deepEqual(guestAttachment.recoveryPlan.units[0]?.branch, {
    knownFailure: {
      kind: "operation",
      operationId: "outbound-unit-0004",
    },
    receiptFailure: {
      kind: "operation",
      operationId: "outbound-unit-0003",
    },
  });
  assert.deepEqual(guestAttachment.recoveryPlan.buttons, []);

  const guestAttachmentWins = planTelegramDurableOutbound(baseOptions(harnessView, {
    replyToMessageId: 0,
    guestQueryId: "guest-query-attachment-wins",
    guestStagingTarget: { chatId: 840585 },
    finalMarkdown: "Guest caption.\n\n<!-- telegram_voice: Ignored voice. -->",
    queuedAttachments: [
      { path: "/private/source/guest.pdf", fileName: "guest.pdf" },
    ],
  }));
  assert.equal(guestAttachmentWins.recoveryPlan.voice, undefined);
  assert.equal(guestAttachmentWins.recoveryPlan.units[0]?.kind, "guest-stage");

  const guestVoice = planTelegramDurableOutbound(baseOptions(harnessView, {
    replyToMessageId: 0,
    guestQueryId: "guest-query-2",
    guestStagingTarget: { chatId: 840585 },
    finalMarkdown: "Speak automatically.",
    automaticVoice: true,
    generatedVoice: [
      { path: "/private/source/guest.opus", fileName: "guest.opus" },
    ],
  }));
  assert.deepEqual(
    guestVoice.recoveryPlan.units.map((unit) => [unit.kind, unit.method]),
    [
      ["guest-stage", "sendVoice"],
      ["guest-answer", "answerGuestQuery"],
      ["guest-cleanup", "deleteMessage"],
      ["guest-cleanup", "deleteMessage"],
      ["guest-text", "answerGuestQuery"],
    ],
  );
  assert.deepEqual(guestVoice.recoveryPlan.voice, {
    text: "Speak automatically.",
    automatic: true,
  });
});

test("durable planner rejects path-like filenames and caps Guest captions by code point", () => {
  const harnessView = { inboundRecordId: "inbound-name", turnId: "turn-name" };
  for (const fileName of ["", ".", "..", "nested/file.txt", "nested\\file.txt", "/tmp/file.txt", "C:\\tmp\\file.txt"]) {
    assert.throws(
      () => planTelegramDurableOutbound(baseOptions(harnessView, {
        queuedAttachments: [{ path: "/private/source/file.txt", fileName }],
      })),
      /Invalid durable outbound filename/,
    );
  }

  const caption = `${"😀".repeat(1024)}tail`;
  const guest = planTelegramDurableOutbound(baseOptions(harnessView, {
    replyToMessageId: 0,
    guestQueryId: "guest-caption-limit",
    guestStagingTarget: { chatId: 840585 },
    finalMarkdown: caption,
    queuedAttachments: [{ path: "/private/source/file.pdf", fileName: "file.pdf" }],
  }));
  const unit = guest.recoveryPlan.units[1];
  assert.equal(unit?.kind, "guest-answer");
  assert.equal(
    Array.from(unit?.kind === "guest-answer" ? unit.caption ?? "" : "").length,
    1024,
  );
  assert.equal(
    unit?.kind === "guest-answer" ? unit.caption : undefined,
    "😀".repeat(1024),
  );
});

test("descriptor reader rejects source mutation, missing files, and over-limit files", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "pi-telegram-outbound-source-"));
  try {
    const stablePath = join(tempDir, "stable.bin");
    await writeFile(stablePath, "stable bytes");
    const stable = await readTelegramDurableOutboundSource({
      path: stablePath,
      fileName: "stable.bin",
    });
    assert.equal(Buffer.from(stable.bytes).toString(), "stable bytes");
    assert.match(stable.sha256, /^[0-9a-f]{64}$/);

    const mutablePath = join(tempDir, "mutable.bin");
    await writeFile(mutablePath, "before");
    await assert.rejects(
      readTelegramDurableOutboundSource(
        { path: mutablePath, fileName: "mutable.bin" },
        { afterRead: () => writeFile(mutablePath, "after-and-longer") },
      ),
      /changed while being read/,
    );
    const seamBytes = Buffer.from("same!!");
    let statCall = 0;
    const beforeStats = {
      dev: 1,
      ino: 2,
      size: seamBytes.length,
      mtimeMs: 10,
      ctimeMs: 20,
      birthtimeMs: 5,
      isFile: () => true,
    } as Stats;
    const afterStats = { ...beforeStats, ctimeMs: 21 } as Stats;
    await assert.rejects(
      readTelegramDurableOutboundSource(
        { path: "injected.bin", fileName: "injected.bin" },
        {
          openSource: async () => ({
            stat: async () => statCall++ === 0 ? beforeStats : afterStats,
            read: async (buffer, offset, length) => {
              const bytesRead = Math.min(length, seamBytes.length - offset);
              buffer.set(seamBytes.subarray(offset, offset + bytesRead), offset);
              return { bytesRead };
            },
            close: async () => {},
          }),
        },
      ),
      /changed while being read/,
    );
    await assert.rejects(
      readTelegramDurableOutboundSource({
        path: join(tempDir, "missing.bin"),
        fileName: "missing.bin",
      }),
      /source is unavailable: missing.bin/,
    );
    const largePath = join(tempDir, "large.bin");
    await writeFile(largePath, Buffer.alloc(32, 1));
    await assert.rejects(
      readTelegramDurableOutboundSource(
        { path: largePath, fileName: "large.bin" },
        { maxSourceBytes: 16 },
      ),
      /exceeds size limit/,
    );
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("commit transforms Markdown and button labels before rendering and publication", async () => {
  const harness = await createHarness();
  try {
    const calls: string[] = [];
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness, {
        finalMarkdown: [
          "Final answer.",
          "",
          '<!-- telegram_button label=Continue prompt="Continue safely." -->',
        ].join("\n"),
      }),
      {
        store: harness.store,
        transformReply: async (text) => {
          calls.push(text);
          return `translated:${text}`;
        },
      },
    );
    assert.deepEqual(calls, ["Final answer.", "Continue"]);
    const payload = JSON.parse(Buffer.from(committed.payload).toString("utf8")) as {
      finalMarkdown: string;
      renderedChunks: string[];
      buttons: Array<{ label: string; prompt: string }>;
    };
    assert.equal(payload.finalMarkdown, "translated:Final answer.");
    assert.deepEqual(payload.renderedChunks, ["translated:Final answer."]);
    assert.deepEqual(payload.buttons, [{
      label: "translated:Continue",
      prompt: "Continue safely.",
    }]);
  } finally {
    await removeHarness(harness);
  }
});

test("oversized Guest response stages one complete private document and cleans it only after terminal", async () => {
  const harness = await createHarness();
  try {
    const markdown = `${"a".repeat(32_000)}\n\n${"b".repeat(2_000)}`;
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness, {
        replyToMessageId: 0,
        guestQueryId: "guest-complete-document",
        guestStagingTarget: { chatId: 840585 },
        finalMarkdown: markdown,
      }),
      {
        store: harness.store,
        operationTempDir: harness.tempDir,
        transformReply: identityTransform,
      },
    );
    assert.equal(committed.operationOwnedFiles.length, 1);
    const owned = committed.operationOwnedFiles[0];
    assert.ok(owned);
    assert.equal(await readFile(owned.path, "utf8"), markdown);
    assert.equal(Buffer.from(committed.spool[0] ?? []).toString("utf8"), markdown);

    const payload = JSON.parse(Buffer.from(committed.payload).toString("utf8")) as {
      units: Array<{ kind: string; caption?: string }>;
    };
    assert.deepEqual(
      payload.units.slice(0, 3).map((unit) => [unit.kind, unit.caption]),
      [
        ["guest-stage", undefined],
        ["guest-answer", TELEGRAM_GUEST_FULL_RESPONSE_CAPTION],
        ["guest-cleanup", undefined],
      ],
    );

    const guestAnswers: unknown[] = [];
    const adapter = createTelegramDurableOutboundUnitAdapter({
      gate: { canStart: () => true, isActive: () => true },
      sendMessage: async () => ({ message_id: 1 }),
      sendRichMessage: async () => ({ message_id: 2 }),
      sendMultipartBytes: async () => ({
        message_id: 710,
        document: { file_id: "complete-document-file-id" },
      }),
      answerGuestQuery: async (_queryId, _text, options) => {
        guestAnswers.push(options?.result);
      },
      deleteMessage: async () => {},
    });
    let result = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: harness.store, claim: { identity: IDENTITY }, adapter },
    );
    while (result.record.state === "pending") {
      result = await executeNextTelegramDurableOutboundUnit(
        committed.record.recordId,
        { store: harness.store, claim: { identity: IDENTITY }, adapter },
      );
    }
    assert.equal(result.record.state, "delivered");
    assert.deepEqual(guestAnswers, [{
      type: "document",
      id: "attachment-1",
      title: "full-response.md",
      document_file_id: "complete-document-file-id",
      caption: TELEGRAM_GUEST_FULL_RESPONSE_CAPTION,
    }]);
    await access(owned.path);

    const operationFiles = createTelegramDurableOutboundOperationFileRuntime({
      operationTempDir: harness.tempDir,
    });
    await operationFiles.resolveTerminal(result.record.turnId);
    await assert.rejects(access(owned.path));
  } finally {
    await removeHarness(harness);
  }
});

test("commit preserves voice metadata while transforming deterministic voice-only fallback", async () => {
  const harness = await createHarness();
  try {
    const voicePath = join(harness.tempDir, "voice.ogg");
    await writeFile(voicePath, "voice bytes");
    const calls: string[] = [];
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness, {
        finalMarkdown: "Speak this answer.",
        automaticVoice: true,
        generatedVoice: [{ path: voicePath, fileName: "voice.ogg" }],
      }),
      {
        store: harness.store,
        transformReply: async (text) => {
          calls.push(text);
          return text;
        },
      },
    );
    assert.deepEqual(calls, ["Speak this answer."]);
    const payload = JSON.parse(Buffer.from(committed.payload).toString("utf8")) as {
      finalMarkdown: string;
      voice?: { text: string; automatic: boolean };
      units: Array<{ kind: string; content?: string }>;
    };
    assert.equal(payload.finalMarkdown, "");
    assert.deepEqual(payload.voice, { text: "Speak this answer.", automatic: true });
    assert.deepEqual(
      payload.units.map((unit) => [unit.kind, unit.content]),
      [["voice", undefined], ["final-text", "Speak this answer."]],
    );
  } finally {
    await removeHarness(harness);
  }
});

test("commit publishes all sources atomically and reconstructs only verified recovery spools", async () => {
  const harness = await createHarness();
  try {
    const sourcePath = join(harness.tempDir, "source-report.png");
    const sourceBytes = Buffer.from("private attachment bytes");
    await writeFile(sourcePath, sourceBytes);
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness, {
        finalMarkdown: "Durable answer body.",
        queuedAttachments: [{ path: sourcePath, fileName: "report.png" }],
      }),
      { store: harness.store, transformReply: identityTransform },
    );
    await unlink(sourcePath);

    assert.equal(Buffer.from(committed.spool[0] ?? []).toString(), sourceBytes.toString());
    const reopenedStore = openRecoveryStore({
      profile: "default",
      rootPath: harness.store.rootPath,
      isIdentityAuthenticated: () => true,
    });
    const reread = reopenedStore.listClaimableOutboundRecords({ identity: IDENTITY });
    assert.equal(reread.length, 1);
    assert.equal(Buffer.from(reread[0]?.spool[0] ?? []).toString(), sourceBytes.toString());

    const serializedPayload = Buffer.from(committed.payload).toString("utf8");
    assert.doesNotMatch(serializedPayload, /source-report\.png/);
    assert.doesNotMatch(serializedPayload, new RegExp(harness.tempDir.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
    assert.match(serializedPayload, /"fileName":"report\.png"/);
    const serializedStatus = JSON.stringify(harness.store.getStatus());
    assert.doesNotMatch(serializedStatus, /Durable answer body|source-report|report\.png/);
  } finally {
    await removeHarness(harness);
  }
});

test("missing and quota-rejected source plans leave dispatching inbound with no partial intent", async () => {
  const missingHarness = await createHarness();
  try {
    const presentPath = join(missingHarness.tempDir, "present.txt");
    await writeFile(presentPath, "captured before later failure");
    await assert.rejects(
      commitTelegramDurableOutbound(
        baseOptions(missingHarness, {
          queuedAttachments: [
            { path: presentPath, fileName: "present.txt" },
            {
              path: join(missingHarness.tempDir, "absent.txt"),
              fileName: "absent.txt",
            },
          ],
        }),
        { store: missingHarness.store, transformReply: identityTransform },
      ),
      /source is unavailable/,
    );
    const status = missingHarness.store.getStatus();
    assert.equal(status.counts.dispatching, 1);
    assert.equal(status.items.filter((item) => item.family === "outbound").length, 0);
  } finally {
    await removeHarness(missingHarness);
  }

  const mutationHarness = await createHarness();
  try {
    const sourcePath = join(mutationHarness.tempDir, "mutating.bin");
    await writeFile(sourcePath, "before");
    await assert.rejects(
      commitTelegramDurableOutbound(
        baseOptions(mutationHarness, {
          queuedAttachments: [{ path: sourcePath, fileName: "mutating.bin" }],
        }),
        {
          store: mutationHarness.store,
          transformReply: identityTransform,
          afterSourceRead: () => writeFile(sourcePath, "after-and-longer"),
        },
      ),
      /changed while being read/,
    );
    const status = mutationHarness.store.getStatus();
    assert.equal(status.counts.dispatching, 1);
    assert.equal(status.items.filter((item) => item.family === "outbound").length, 0);
  } finally {
    await removeHarness(mutationHarness);
  }

  const quotaHarness = await createHarness(8_000);
  try {
    const sourcePath = join(quotaHarness.tempDir, "quota.bin");
    await writeFile(sourcePath, Buffer.alloc(7_000, 9));
    await assert.rejects(
      commitTelegramDurableOutbound(
        baseOptions(quotaHarness, {
          finalMarkdown: "Quota-fenced answer.",
          queuedAttachments: [{ path: sourcePath, fileName: "quota.bin" }],
        }),
        {
          store: quotaHarness.store,
          transformReply: identityTransform,
          maxSourceBytes: 10_000,
        },
      ),
      RecoveryQuotaExceededError,
    );
    const status = quotaHarness.store.getStatus();
    assert.equal(status.counts.dispatching, 1);
    assert.equal(status.items.filter((item) => item.family === "outbound").length, 0);
    assert.deepEqual(
      quotaHarness.store.listClaimableOutboundRecords({ identity: IDENTITY }),
      [],
    );
  } finally {
    await removeHarness(quotaHarness);
  }
});

test("one-unit adapter distinguishes pre-start denial, safe rejection, and exact ambiguity", async () => {
  const finalUnit: RecoveryOutboundUnit = {
    kind: "final-text",
    operationId: "operation-text",
    method: "sendMessage",
    content: "answer",
    contentMode: "plain",
  };

  const denied = createUnitAdapterHarness({
    gate: { canStart: () => false, isActive: () => true },
  });
  assert.deepEqual(
    await denied.adapter.execute(adapterInput(finalUnit)),
    { kind: "not-started" },
  );
  assert.deepEqual(denied.calls, []);

  const invalidMarkup = createUnitAdapterHarness();
  assert.deepEqual(
    await invalidMarkup.adapter.execute(adapterInput(finalUnit, {
      replyMarkup: {
        inline_keyboard: [[{ text: "bad", callback_data: "x".repeat(65) }]],
      },
    })),
    { kind: "not-started" },
  );
  assert.deepEqual(invalidMarkup.calls, []);

  const rejected = createUnitAdapterHarness({
    sendMessage: async () => {
      throw new TelegramApiHttpError("bad request", 400, undefined);
    },
  });
  assert.deepEqual(
    await rejected.adapter.execute(adapterInput(finalUnit)),
    { kind: "known-not-committed" },
  );

  const timedOut = createUnitAdapterHarness({
    sendMessage: async () => {
      throw new TelegramApiCommitUnknownError(
        "sendMessage",
        new Error("deadline"),
        "timeout-after-write",
      );
    },
  });
  assert.deepEqual(
    await timedOut.adapter.execute(adapterInput(finalUnit)),
    { kind: "commit-unknown", reason: "timeout-after-write" },
  );

  const malformed = createUnitAdapterHarness({
    sendMessage: async () => ({ message_id: 0 }),
  });
  assert.deepEqual(
    await malformed.adapter.execute(adapterInput(finalUnit)),
    { kind: "commit-unknown", reason: "malformed-success" },
  );

  const authorityLost = createUnitAdapterHarness({
    gate: { canStart: () => true, isActive: () => false },
  });
  assert.deepEqual(
    await authorityLost.adapter.execute(adapterInput(finalUnit)),
    { kind: "commit-unknown", reason: "authority-lost-after-start" },
  );
  assert.deepEqual(authorityLost.calls, ["sendMessage"]);
});

test("one-unit adapter executes attachment, voice, and Guest branches exactly once", async () => {
  for (const [unit, expectedCall] of [
    [{
      kind: "attachment",
      operationId: "photo",
      method: "sendPhoto",
      spoolRefIndex: 0,
      fileName: "photo.png",
      mediaKind: "photo",
    }, "sendPhoto"],
    [{
      kind: "attachment",
      operationId: "document",
      method: "sendDocument",
      spoolRefIndex: 0,
      fileName: "report.pdf",
      mediaKind: "document",
    }, "sendDocument"],
    [{
      kind: "voice",
      operationId: "voice",
      method: "sendVoice",
      spoolRefIndex: 0,
      fileName: "answer.ogg",
      mediaKind: "voice",
    }, "sendVoice"],
  ] as const) {
    const harness = createUnitAdapterHarness();
    assert.deepEqual(
      await harness.adapter.execute(adapterInput(unit, {
        spool: [Buffer.from("verified bytes")],
      })),
      { kind: "committed", method: expectedCall, messageId: 103 },
    );
    assert.deepEqual(harness.calls, [expectedCall]);
  }

  const guestText = createUnitAdapterHarness();
  assert.deepEqual(
    await guestText.adapter.execute(adapterInput({
      kind: "guest-text",
      operationId: "guest-text",
      method: "answerGuestQuery",
      markdown: "Guest answer",
    }, {
      guestQueryId: "guest-query",
      guestStagingTarget: { chatId: 840585 },
    })),
    { kind: "committed", method: "answerGuestQuery" },
  );
  assert.deepEqual(guestText.calls, ["answerGuestQuery"]);

  const guestStage = createUnitAdapterHarness({
    sendMultipartBytes: async (method, fields) => {
      guestStage.calls.push(`${method}:${fields.chat_id}`);
      return { message_id: 110, voice: { file_id: "voice-file" } };
    },
  });
  assert.deepEqual(
    await guestStage.adapter.execute(adapterInput({
      kind: "guest-stage",
      operationId: "guest-stage",
      method: "sendVoice",
      spoolRefIndex: 0,
      fileName: "guest.ogg",
      mediaKind: "voice",
    }, {
      spool: [Buffer.from("guest voice")],
      guestStagingTarget: { chatId: 840585 },
    })),
    {
      kind: "committed",
      method: "sendVoice",
      messageId: 110,
      result: {
        kind: "guest-staging",
        stagingMessageId: 110,
        mediaKind: "voice",
        fileId: "voice-file",
      },
    },
  );
  assert.deepEqual(guestStage.calls, ["sendVoice:840585"]);

  const stageReceipt = {
    unitIndex: 0,
    operationId: "guest-stage",
    method: "sendVoice",
    messageId: 110,
    result: {
      kind: "guest-staging" as const,
      stagingMessageId: 110,
      mediaKind: "voice" as const,
      fileId: "voice-file",
    },
    committedAtMs: 1,
  };
  const guestAnswer = createUnitAdapterHarness();
  assert.deepEqual(
    await guestAnswer.adapter.execute(adapterInput({
      kind: "guest-answer",
      operationId: "guest-answer",
      method: "answerGuestQuery",
      stageOperationId: "guest-stage",
      fileName: "guest.ogg",
      mediaKind: "voice",
    }, {
      guestQueryId: "guest-query",
      guestStagingTarget: { chatId: 840585 },
      receipts: [stageReceipt],
    })),
    {
      kind: "committed",
      method: "answerGuestQuery",
      result: { kind: "guest-answer" },
    },
  );
  assert.deepEqual(guestAnswer.calls, ["answerGuestQuery"]);

  const guestCleanup = createUnitAdapterHarness();
  assert.deepEqual(
    await guestCleanup.adapter.execute(adapterInput({
      kind: "guest-cleanup",
      operationId: "guest-cleanup",
      method: "deleteMessage",
      stageOperationId: "guest-stage",
    }, {
      guestStagingTarget: { chatId: 840585 },
      receipts: [stageReceipt],
    })),
    {
      kind: "committed",
      method: "deleteMessage",
      result: { kind: "guest-cleanup", stagingMessageId: 110 },
    },
  );
  assert.deepEqual(guestCleanup.calls, ["deleteMessage"]);

  const missingSpool = createUnitAdapterHarness();
  assert.deepEqual(
    await missingSpool.adapter.execute(adapterInput({
      kind: "voice",
      operationId: "missing",
      method: "sendVoice",
      spoolRefIndex: 0,
      fileName: "missing.ogg",
      mediaKind: "voice",
    })),
    { kind: "not-started" },
  );
  assert.deepEqual(missingSpool.calls, []);
});

test("executor persists each text receipt before restart continuation without confirmed resend", async () => {
  const harness = await createHarness();
  try {
    const markdown = [
      "a".repeat(32_000),
      "",
      "b".repeat(2_000),
      "",
      '<!-- telegram_button label=Continue prompt="Continue safely." -->',
    ].join("\n");
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness, { finalMarkdown: markdown }),
      { store: harness.store, transformReply: identityTransform },
    );
    const bodies: Array<Record<string, unknown>> = [];
    const ownership: number[] = [];
    const adapter = createTelegramDurableOutboundUnitAdapter({
      gate: { canStart: () => true, isActive: () => true },
      sendMessage: async () => {
        throw new Error("ordinary transport must not run");
      },
      sendRichMessage: async (body) => {
        bodies.push(body);
        return { message_id: bodies.length + 200 };
      },
      sendMultipartBytes: async () => {
        throw new Error("multipart transport must not run");
      },
      answerGuestQuery: async () => {
        throw new Error("Guest transport must not run");
      },
      deleteMessage: async () => {
        throw new Error("Guest cleanup transport must not run");
      },
    });
    const deps = {
      claim: { identity: IDENTITY },
      adapter,
      createReplyMarkup: () => ({
        inline_keyboard: [[{ text: "Continue", callback_data: "continue" }]],
      }),
      recordOwnership: ({ messageId }: { messageId: number }) => {
        ownership.push(messageId);
      },
    };
    const first = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { ...deps, store: harness.store },
    );
    assert.equal(first.record.state, "pending");
    assert.deepEqual(first.record.receipts.map((receipt) => receipt.unitIndex), [0]);

    const reopened = openRecoveryStore({
      profile: "default",
      rootPath: harness.store.rootPath,
      isIdentityAuthenticated: () => true,
    });
    const second = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { ...deps, store: reopened },
    );
    assert.equal(second.record.state, "delivered");
    assert.deepEqual(
      second.record.receipts.map((receipt) => receipt.unitIndex),
      [0, 1],
    );
    assert.equal(bodies.length, 2);
    assert.deepEqual(bodies[0]?.reply_parameters, {
      message_id: 45,
      allow_sending_without_reply: true,
    });
    assert.equal("reply_markup" in (bodies[0] ?? {}), false);
    assert.equal("reply_parameters" in (bodies[1] ?? {}), false);
    assert.deepEqual(bodies[1]?.reply_markup, {
      inline_keyboard: [[{ text: "Continue", callback_data: "continue" }]],
    });
    assert.deepEqual(ownership, [201, 202]);
    await assert.rejects(
      executeNextTelegramDurableOutboundUnit(
        committed.record.recordId,
        { ...deps, store: reopened },
      ),
      /Invalid outbound transition delivered/,
    );
    assert.equal(bodies.length, 2);
  } finally {
    await removeHarness(harness);
  }
});

test("Rich primary success skips fallback while safe rejection selects it as the first reply", async () => {
  const successHarness = await createHarness();
  try {
    const sourcePath = join(successHarness.tempDir, "result.png");
    await writeFile(sourcePath, "image bytes");
    const committed = await commitTelegramDurableOutbound(
      baseOptions(successHarness, {
        finalMarkdown: "Result\n\n<!-- telegram_button: Open -->",
        queuedAttachments: [{ path: sourcePath, fileName: "result.png" }],
      }),
      { store: successHarness.store, transformReply: identityTransform },
    );
    const multipart: Array<Record<string, string>> = [];
    const adapter = createTelegramDurableOutboundUnitAdapter({
      gate: { canStart: () => true, isActive: () => true },
      sendMessage: async () => ({ message_id: 1 }),
      sendRichMessage: async () => ({ message_id: 2 }),
      sendMultipartBytes: async (_method, fields) => {
        multipart.push(fields);
        return { message_id: 303 };
      },
      answerGuestQuery: async () => {},
      deleteMessage: async () => {},
    });
    const result = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: successHarness.store,
        claim: { identity: IDENTITY },
        adapter,
        createReplyMarkup: () => ({
          inline_keyboard: [[{ text: "Open", callback_data: "open" }]],
        }),
      },
    );
    assert.equal(result.record.state, "delivered");
    assert.deepEqual(result.record.receipts.map((receipt) => receipt.unitIndex), [0]);
    assert.equal(multipart.length, 1);
    assert.equal(multipart[0]?.reply_parameters, JSON.stringify({
      message_id: 45,
      allow_sending_without_reply: true,
    }));
    assert.equal(multipart[0]?.reply_markup, JSON.stringify({
      inline_keyboard: [[{ text: "Open", callback_data: "open" }]],
    }));
  } finally {
    await removeHarness(successHarness);
  }

  const fallbackHarness = await createHarness();
  try {
    const sourcePath = join(fallbackHarness.tempDir, "result.png");
    await writeFile(sourcePath, "image bytes");
    const committed = await commitTelegramDurableOutbound(
      baseOptions(fallbackHarness, {
        finalMarkdown: "Result\n\n<!-- telegram_button: Open -->",
        queuedAttachments: [{ path: sourcePath, fileName: "result.png" }],
      }),
      { store: fallbackHarness.store, transformReply: identityTransform },
    );
    const rejected = createUnitAdapterHarness({
      sendMultipartBytes: async () => {
        throw new TelegramApiHttpError("unsupported", 400, undefined);
      },
    });
    const first = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: fallbackHarness.store,
        claim: { identity: IDENTITY },
        adapter: rejected.adapter,
        createReplyMarkup: () => ({
          inline_keyboard: [[{ text: "Open", callback_data: "open" }]],
        }),
      },
    );
    assert.deepEqual(first.outcome, { kind: "known-not-committed" });
    assert.equal(first.record.nextUnitIndex, 1);
    assert.deepEqual(first.record.receipts, []);

    const richBodies: Array<Record<string, unknown>> = [];
    const fallback = createUnitAdapterHarness({
      sendRichMessage: async (body) => {
        richBodies.push(body);
        return { message_id: 404 };
      },
    });
    const second = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: fallbackHarness.store,
        claim: { identity: IDENTITY },
        adapter: fallback.adapter,
        createReplyMarkup: () => ({
          inline_keyboard: [[{ text: "Open", callback_data: "open" }]],
        }),
      },
    );
    assert.equal(second.record.state, "pending");
    assert.deepEqual(second.record.receipts.map((receipt) => receipt.unitIndex), [1]);
    assert.deepEqual(richBodies[0]?.reply_parameters, {
      message_id: 45,
      allow_sending_without_reply: true,
    });
    assert.deepEqual(richBodies[0]?.reply_markup, {
      inline_keyboard: [[{ text: "Open", callback_data: "open" }]],
    });
  } finally {
    await removeHarness(fallbackHarness);
  }
});

test("Rich ambiguity blocks fallback and receipt commit failure becomes durable uncertainty", async () => {
  const richHarness = await createHarness();
  try {
    const sourcePath = join(richHarness.tempDir, "result.png");
    await writeFile(sourcePath, "image bytes");
    const committed = await commitTelegramDurableOutbound(
      baseOptions(richHarness, {
        queuedAttachments: [{ path: sourcePath, fileName: "result.png" }],
      }),
      { store: richHarness.store, transformReply: identityTransform },
    );
    let mutations = 0;
    const adapter = createUnitAdapterHarness({
      sendMultipartBytes: async () => {
        mutations += 1;
        throw new TelegramApiCommitUnknownError(
          "sendRichMessage",
          new Error("response lost"),
          "response-lost",
        );
      },
    }).adapter;
    const uncertain = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: richHarness.store,
        claim: { identity: IDENTITY },
        adapter,
      },
    );
    assert.equal(uncertain.record.state, "delivery-uncertain");
    assert.equal(uncertain.record.uncertainty?.reason, "response-lost");
    assert.equal(mutations, 1);
    await assert.rejects(
      executeNextTelegramDurableOutboundUnit(
        committed.record.recordId,
        { store: richHarness.store, claim: { identity: IDENTITY }, adapter },
      ),
      /Invalid outbound transition delivery-uncertain/,
    );
    assert.equal(mutations, 1);
  } finally {
    await removeHarness(richHarness);
  }

  const receiptHarness = await createHarness();
  try {
    const committed = await commitTelegramDurableOutbound(
      baseOptions(receiptHarness),
      { store: receiptHarness.store, transformReply: identityTransform },
    );
    const adapter = createUnitAdapterHarness().adapter;
    const uncertain = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: {
          activateOutbound: receiptHarness.store.activateOutbound.bind(receiptHarness.store),
          claimOutboundUnit: receiptHarness.store.claimOutboundUnit.bind(receiptHarness.store),
          recordOutboundReceipt: () => {
            throw new Error("snapshot unavailable");
          },
          releaseOutboundUnitNotStarted:
            receiptHarness.store.releaseOutboundUnitNotStarted.bind(receiptHarness.store),
          recordOutboundSafeFailure:
            receiptHarness.store.recordOutboundSafeFailure.bind(receiptHarness.store),
          markOutboundUncertain:
            receiptHarness.store.markOutboundUncertain.bind(receiptHarness.store),
        },
        claim: { identity: IDENTITY },
        adapter,
      },
    );
    assert.equal(uncertain.record.state, "delivery-uncertain");
    assert.equal(
      uncertain.record.uncertainty?.reason,
      "confirmed-before-receipt",
    );
  } finally {
    await removeHarness(receiptHarness);
  }
});

test("executor releases not-started units without consuming attempts or selecting fallback", async () => {
  const harness = await createHarness();
  try {
    const sourcePath = join(harness.tempDir, "result.png");
    await writeFile(sourcePath, "image bytes");
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness, {
        finalMarkdown: "Result\n\n<!-- telegram_button: Open -->",
        queuedAttachments: [{ path: sourcePath, fileName: "result.png" }],
      }),
      { store: harness.store, transformReply: identityTransform },
    );
    let mutations = 0;
    const adapter = createUnitAdapterHarness({
      sendMultipartBytes: async () => {
        mutations += 1;
        return { message_id: 1 };
      },
    }).adapter;
    const result = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: harness.store,
        claim: { identity: IDENTITY },
        adapter,
      },
    );
    assert.deepEqual(result.outcome, { kind: "not-started" });
    assert.equal(result.record.state, "pending");
    assert.equal(result.record.nextUnitIndex, 0);
    assert.equal(result.record.automaticAttemptCount, 0);
    assert.deepEqual(result.record.receipts, []);
    assert.equal(mutations, 0);
    assert.throws(
      () => harness.store.releaseOutboundUnitNotStarted({
        recordId: committed.record.recordId,
        claim: { identity: IDENTITY },
        attemptId: "stale-attempt",
      }),
      /active attempt claim denied/,
    );
  } finally {
    await removeHarness(harness);
  }
});

test("ownership projection failure cannot roll a confirmed receipt back to uncertainty", async () => {
  const harness = await createHarness();
  try {
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness),
      { store: harness.store, transformReply: identityTransform },
    );
    let sends = 0;
    const adapter = createUnitAdapterHarness({
      sendRichMessage: async () => {
        sends += 1;
        return { message_id: 505 };
      },
    }).adapter;
    const events: Array<{
      category: string;
      message: string;
      details?: Record<string, unknown>;
    }> = [];
    const delivered = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: harness.store,
        claim: { identity: IDENTITY },
        adapter,
        recordOwnership: () => {
          throw new Error("ownership projection failed");
        },
        recordRuntimeEvent: (category, error, details) => {
          events.push({
            category,
            message: (error as Error).message,
            details,
          });
        },
      },
    );
    assert.equal(delivered.record.state, "delivered");
    assert.deepEqual(events, [{
      category: "delivery",
      message: "ownership projection failed",
      details: {
        phase: "durable-outbound-ownership",
        recordId: committed.record.recordId,
        operationId: "outbound-unit-0000",
      },
    }]);
    assert.equal(sends, 1);
    assert.deepEqual(
      harness.store.listClaimableOutboundRecords({ identity: IDENTITY }),
      [],
    );
    await assert.rejects(
      executeNextTelegramDurableOutboundUnit(committed.record.recordId, {
        store: harness.store,
        claim: { identity: IDENTITY },
        adapter,
      }),
      /Invalid outbound transition delivered/,
    );
    assert.equal(sends, 1);
  } finally {
    await removeHarness(harness);
  }
});

test("voice-only branches skip fallback on success, select it once on safe failure, and stop on ambiguity", async () => {
  const makeVoicePlan = async (harness: DurableHarness) => {
    const firstPath = join(harness.tempDir, "first.ogg");
    const secondPath = join(harness.tempDir, "second.ogg");
    await writeFile(firstPath, "first voice");
    await writeFile(secondPath, "second voice");
    return commitTelegramDurableOutbound(
      baseOptions(harness, {
        finalMarkdown: [
          "<!-- telegram_voice: First spoken part. -->",
          "<!-- telegram_voice: Second spoken part. -->",
        ].join("\n"),
        generatedVoice: [
          { path: firstPath, fileName: "first.ogg" },
          { path: secondPath, fileName: "second.ogg" },
        ],
      }),
      { store: harness.store, transformReply: identityTransform },
    );
  };

  const successHarness = await createHarness();
  try {
    const committed = await makeVoicePlan(successHarness);
    const adapter = createUnitAdapterHarness().adapter;
    const first = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: successHarness.store, claim: { identity: IDENTITY }, adapter },
    );
    assert.equal(first.record.state, "pending");
    const reopened = openRecoveryStore({
      profile: "default",
      rootPath: successHarness.store.rootPath,
      isIdentityAuthenticated: () => true,
    });
    const second = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: reopened, claim: { identity: IDENTITY }, adapter },
    );
    assert.equal(second.record.state, "delivered");
    assert.deepEqual(
      second.record.receipts.map((receipt) => receipt.unitIndex),
      [0, 1],
    );
    assert.deepEqual(second.record.unitProgress, [
      {
        unitIndex: 0,
        operationId: "outbound-unit-0000",
        outcome: "committed",
      },
      {
        unitIndex: 1,
        operationId: "outbound-unit-0001",
        outcome: "committed",
      },
      {
        unitIndex: 2,
        operationId: "outbound-unit-0002",
        outcome: "skipped",
        reason: "branch",
      },
    ]);
  } finally {
    await removeHarness(successHarness);
  }

  const partialHarness = await createHarness();
  try {
    const committed = await makeVoicePlan(partialHarness);
    let voiceStarts = 0;
    const adapter = createUnitAdapterHarness({
      sendMultipartBytes: async () => {
        voiceStarts += 1;
        if (voiceStarts === 2) {
          throw new TelegramApiHttpError("voice rejected", 400, undefined);
        }
        return { message_id: 610 };
      },
    }).adapter;
    await executeNextTelegramDurableOutboundUnit(committed.record.recordId, {
      store: partialHarness.store,
      claim: { identity: IDENTITY },
      adapter,
    });
    const reopened = openRecoveryStore({
      profile: "default",
      rootPath: partialHarness.store.rootPath,
      isIdentityAuthenticated: () => true,
    });
    const failed = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: reopened, claim: { identity: IDENTITY }, adapter },
    );
    assert.deepEqual(failed.outcome, { kind: "known-not-committed" });
    assert.equal(failed.record.nextUnitIndex, 2);
    assert.deepEqual(
      failed.record.receipts.map((receipt) => receipt.unitIndex),
      [0],
    );
    assert.deepEqual(failed.record.unitProgress.slice(0, 2), [
      {
        unitIndex: 0,
        operationId: "outbound-unit-0000",
        outcome: "committed",
      },
      {
        unitIndex: 1,
        operationId: "outbound-unit-0001",
        outcome: "skipped",
        reason: "known-failure",
      },
    ]);
    const fallbackCalls: string[] = [];
    const fallback = createUnitAdapterHarness({
      sendRichMessage: async () => {
        fallbackCalls.push("fallback");
        return { message_id: 611 };
      },
    }).adapter;
    const delivered = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: reopened, claim: { identity: IDENTITY }, adapter: fallback },
    );
    assert.equal(delivered.record.state, "delivered");
    assert.deepEqual(fallbackCalls, ["fallback"]);
    assert.equal(voiceStarts, 2);
  } finally {
    await removeHarness(partialHarness);
  }

  const firstFailureHarness = await createHarness();
  try {
    const committed = await makeVoicePlan(firstFailureHarness);
    let voiceStarts = 0;
    const adapter = createUnitAdapterHarness({
      sendMultipartBytes: async () => {
        voiceStarts += 1;
        throw new TelegramApiHttpError("voice rejected", 400, undefined);
      },
    }).adapter;
    const failed = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: firstFailureHarness.store,
        claim: { identity: IDENTITY },
        adapter,
      },
    );
    assert.equal(failed.record.nextUnitIndex, 2);
    assert.deepEqual(failed.record.unitProgress, [
      {
        unitIndex: 0,
        operationId: "outbound-unit-0000",
        outcome: "skipped",
        reason: "known-failure",
      },
      {
        unitIndex: 1,
        operationId: "outbound-unit-0001",
        outcome: "skipped",
        reason: "branch",
      },
    ]);
    assert.equal(voiceStarts, 1);
  } finally {
    await removeHarness(firstFailureHarness);
  }

  const ambiguousHarness = await createHarness();
  try {
    const committed = await makeVoicePlan(ambiguousHarness);
    let starts = 0;
    const adapter = createUnitAdapterHarness({
      sendMultipartBytes: async () => {
        starts += 1;
        throw new TelegramApiCommitUnknownError(
          "sendVoice",
          new Error("lost"),
          "response-lost",
        );
      },
    }).adapter;
    const uncertain = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      {
        store: ambiguousHarness.store,
        claim: { identity: IDENTITY },
        adapter,
      },
    );
    assert.equal(uncertain.record.state, "delivery-uncertain");
    assert.deepEqual(uncertain.record.unitProgress, []);
    assert.equal(starts, 1);
    const reopened = openRecoveryStore({
      profile: "default",
      rootPath: ambiguousHarness.store.rootPath,
      isIdentityAuthenticated: () => true,
    });
    await assert.rejects(
      executeNextTelegramDurableOutboundUnit(committed.record.recordId, {
        store: reopened,
        claim: { identity: IDENTITY },
        adapter,
      }),
      /delivery-uncertain/,
    );
    assert.equal(starts, 1);
  } finally {
    await removeHarness(ambiguousHarness);
  }
});

test("Guest media persists staged file ids across answer and cleanup reopen boundaries", async () => {
  const harness = await createHarness();
  try {
    const sourcePath = join(harness.tempDir, "guest.pdf");
    await writeFile(sourcePath, "guest bytes");
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness, {
        replyToMessageId: 0,
        guestQueryId: "guest-query",
        guestStagingTarget: { chatId: 840585 },
        finalMarkdown: "Guest answer.",
        queuedAttachments: [{ path: sourcePath, fileName: "guest.pdf" }],
      }),
      { store: harness.store, transformReply: identityTransform },
    );
    const calls: Array<Record<string, unknown>> = [];
    const adapter = createTelegramDurableOutboundUnitAdapter({
      gate: { canStart: () => true, isActive: () => true },
      sendMessage: async () => ({ message_id: 1 }),
      sendRichMessage: async () => ({ message_id: 2 }),
      sendMultipartBytes: async (method) => {
        calls.push({ phase: "stage", method });
        return {
          message_id: 701,
          document: { file_id: "durable-file-id" },
        };
      },
      answerGuestQuery: async (_guestQueryId, _text, options) => {
        calls.push({ phase: "answer", result: options?.result });
      },
      deleteMessage: async (chatId, messageId) => {
        calls.push({ phase: "cleanup", chatId, messageId });
      },
    });
    const first = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: harness.store, claim: { identity: IDENTITY }, adapter },
    );
    assert.equal(first.record.nextUnitIndex, 1);
    assert.equal(first.record.receipts[0]?.result?.kind, "guest-staging");

    const afterStage = openRecoveryStore({
      profile: "default",
      rootPath: harness.store.rootPath,
      isIdentityAuthenticated: () => true,
    });
    const second = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: afterStage, claim: { identity: IDENTITY }, adapter },
    );
    assert.equal(second.record.nextUnitIndex, 2);

    const afterAnswer = openRecoveryStore({
      profile: "default",
      rootPath: harness.store.rootPath,
      isIdentityAuthenticated: () => true,
    });
    const third = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: afterAnswer, claim: { identity: IDENTITY }, adapter },
    );
    assert.equal(third.record.state, "delivered");
    assert.deepEqual(calls, [
      { phase: "stage", method: "sendDocument" },
      {
        phase: "answer",
        result: {
          type: "document",
          id: "attachment-1",
          title: "guest.pdf",
          document_file_id: "durable-file-id",
          caption: "Guest answer.",
        },
      },
      { phase: "cleanup", chatId: 840585, messageId: 701 },
    ]);
    assert.deepEqual(third.record.unitProgress.map((progress) => [
      progress.unitIndex,
      progress.outcome,
    ]), [
      [0, "committed"],
      [1, "committed"],
      [2, "committed"],
      [3, "skipped"],
      [4, "skipped"],
    ]);
  } finally {
    await removeHarness(harness);
  }
});

test("Guest media faults are phase-exact across reopen and ambiguity never selects fallback", async () => {
  const makeGuestPlan = async (harness: DurableHarness) => {
    const sourcePath = join(harness.tempDir, "guest.pdf");
    await writeFile(sourcePath, "guest bytes");
    return commitTelegramDurableOutbound(
      baseOptions(harness, {
        replyToMessageId: 0,
        guestQueryId: "guest-query",
        guestStagingTarget: { chatId: 840585 },
        finalMarkdown: "Guest fallback.",
        queuedAttachments: [{ path: sourcePath, fileName: "guest.pdf" }],
      }),
      { store: harness.store, transformReply: identityTransform },
    );
  };
  const reopen = (harness: DurableHarness) => openRecoveryStore({
    profile: "default",
    rootPath: harness.store.rootPath,
    isIdentityAuthenticated: () => true,
  });

  const safeStageHarness = await createHarness();
  try {
    const committed = await makeGuestPlan(safeStageHarness);
    const stage = createUnitAdapterHarness({
      sendMultipartBytes: async () => {
        throw new TelegramApiHttpError("stage rejected", 400, undefined);
      },
    }).adapter;
    const failed = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: safeStageHarness.store, claim: { identity: IDENTITY }, adapter: stage },
    );
    assert.equal(failed.record.nextUnitIndex, 4);
    assert.deepEqual(failed.record.receipts, []);
    let answers = 0;
    const fallback = createUnitAdapterHarness({
      answerGuestQuery: async () => {
        answers += 1;
      },
    }).adapter;
    const delivered = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: reopen(safeStageHarness), claim: { identity: IDENTITY }, adapter: fallback },
    );
    assert.equal(delivered.record.state, "delivered");
    assert.equal(answers, 1);
  } finally {
    await removeHarness(safeStageHarness);
  }

  const extractionHarness = await createHarness();
  try {
    const committed = await makeGuestPlan(extractionHarness);
    const calls: string[] = [];
    const stage = createUnitAdapterHarness({
      sendMultipartBytes: async () => ({ message_id: 710 }),
    }).adapter;
    const extracted = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: extractionHarness.store, claim: { identity: IDENTITY }, adapter: stage },
    );
    assert.equal(extracted.record.nextUnitIndex, 3);
    assert.equal(extracted.record.receipts[0]?.result?.kind, "guest-staging");
    const cleanup = createUnitAdapterHarness({
      deleteMessage: async () => {
        calls.push("cleanup");
      },
    }).adapter;
    const cleaned = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: reopen(extractionHarness), claim: { identity: IDENTITY }, adapter: cleanup },
    );
    assert.equal(cleaned.record.nextUnitIndex, 4);
    const fallback = createUnitAdapterHarness({
      answerGuestQuery: async () => {
        calls.push("fallback");
      },
    }).adapter;
    const delivered = await executeNextTelegramDurableOutboundUnit(
      committed.record.recordId,
      { store: reopen(extractionHarness), claim: { identity: IDENTITY }, adapter: fallback },
    );
    assert.equal(delivered.record.state, "delivered");
    assert.deepEqual(calls, ["cleanup", "fallback"]);
  } finally {
    await removeHarness(extractionHarness);
  }

  for (const phase of ["answer", "cleanup"] as const) {
    const harness = await createHarness();
    try {
      const committed = await makeGuestPlan(harness);
      const adapter = createTelegramDurableOutboundUnitAdapter({
        gate: { canStart: () => true, isActive: () => true },
        sendMessage: async () => ({ message_id: 1 }),
        sendRichMessage: async () => ({ message_id: 2 }),
        sendMultipartBytes: async () => ({
          message_id: 715,
          document: { file_id: "file-715" },
        }),
        answerGuestQuery: async () => {
          if (phase === "answer") {
            throw new TelegramApiHttpError("answer rejected", 400, undefined);
          }
        },
        deleteMessage: async () => {
          if (phase === "cleanup") {
            throw new TelegramApiHttpError("cleanup rejected", 400, undefined);
          }
        },
      });
      let store = harness.store;
      await executeNextTelegramDurableOutboundUnit(
        committed.record.recordId,
        { store, claim: { identity: IDENTITY }, adapter },
      );
      store = reopen(harness);
      let result = await executeNextTelegramDurableOutboundUnit(
        committed.record.recordId,
        { store, claim: { identity: IDENTITY }, adapter },
      );
      if (phase === "cleanup") {
        store = reopen(harness);
        result = await executeNextTelegramDurableOutboundUnit(
          committed.record.recordId,
          { store, claim: { identity: IDENTITY }, adapter },
        );
      }
      assert.deepEqual(result.outcome, { kind: "known-not-committed" });
      assert.equal(result.record.state, "retryable-pending");
      assert.equal(result.record.nextUnitIndex, phase === "answer" ? 1 : 2);
      assert.equal(
        result.record.unitProgress.some((progress) => progress.unitIndex === 4),
        false,
      );
      assert.doesNotThrow(() => reopen(harness));
    } finally {
      await removeHarness(harness);
    }
  }

  for (const phase of ["stage", "answer", "cleanup"] as const) {
    const harness = await createHarness();
    try {
      const committed = await makeGuestPlan(harness);
      let store = harness.store;
      let mutationCount = 0;
      const adapter = createTelegramDurableOutboundUnitAdapter({
        gate: { canStart: () => true, isActive: () => true },
        sendMessage: async () => ({ message_id: 1 }),
        sendRichMessage: async () => ({ message_id: 2 }),
        sendMultipartBytes: async () => {
          mutationCount += 1;
          if (phase === "stage") {
            throw new TelegramApiCommitUnknownError(
              "sendDocument",
              new Error("lost"),
              "response-lost",
            );
          }
          return { message_id: 720, document: { file_id: "file-720" } };
        },
        answerGuestQuery: async () => {
          mutationCount += 1;
          if (phase === "answer") {
            throw new TelegramApiCommitUnknownError(
              "answerGuestQuery",
              new Error("lost"),
              "response-lost",
            );
          }
        },
        deleteMessage: async () => {
          mutationCount += 1;
          if (phase === "cleanup") {
            throw new TelegramApiCommitUnknownError(
              "deleteMessage",
              new Error("lost"),
              "response-lost",
            );
          }
        },
      });
      let result = await executeNextTelegramDurableOutboundUnit(
        committed.record.recordId,
        { store, claim: { identity: IDENTITY }, adapter },
      );
      if (phase !== "stage") {
        store = reopen(harness);
        result = await executeNextTelegramDurableOutboundUnit(
          committed.record.recordId,
          { store, claim: { identity: IDENTITY }, adapter },
        );
      }
      if (phase === "cleanup") {
        store = reopen(harness);
        result = await executeNextTelegramDurableOutboundUnit(
          committed.record.recordId,
          { store, claim: { identity: IDENTITY }, adapter },
        );
      }
      assert.equal(result.record.state, "delivery-uncertain", phase);
      const beforeRetry = mutationCount;
      const reopened = reopen(harness);
      await assert.rejects(
        executeNextTelegramDurableOutboundUnit(committed.record.recordId, {
          store: reopened,
          claim: { identity: IDENTITY },
          adapter,
        }),
        /delivery-uncertain/,
      );
      assert.equal(mutationCount, beforeRetry, phase);
      assert.equal(
        result.record.unitProgress.some((progress) => progress.unitIndex === 4),
        false,
        phase,
      );
    } finally {
      await removeHarness(harness);
    }
  }
});


test("durable worker turns a thrown sender into uncertainty before one queue-terminal callback", async () => {
  const harness = await createHarness();
  try {
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness),
      { store: harness.store, transformReply: identityTransform },
    );
    const terminalStates: string[] = [];
    const worker = createTelegramDurableOutboundWorker({
      getStore: () => harness.store,
      operationGate: new RecoveryProfileOperationGate(),
      adapter: createUnitAdapterHarness({
        sendRichMessage: async () => {
          throw new Error("sender threw after mutation start");
        },
      }).adapter,
      onTerminal: ({ record }) => {
        terminalStates.push(record.state);
      },
    });
    worker.register(committed.record, { identity: IDENTITY });
    worker.schedule(committed.record.recordId);
    for (let attempt = 0; attempt < 100 && terminalStates.length === 0; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    assert.deepEqual(terminalStates, ["delivery-uncertain"]);
    assert.equal(
      harness.store.getStatus().items.filter(
        (item) =>
          item.family === "outbound" && item.state === "delivery-uncertain",
      ).length,
      1,
    );
  } finally {
    await removeHarness(harness);
  }
});

test("durable worker registers and schedules a confirmed linked uncertain retry", async () => {
  const harness = await createHarness();
  try {
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness),
      { store: harness.store, transformReply: identityTransform },
    );
    const pending = harness.store.activateOutbound(
      committed.record.recordId,
      { identity: IDENTITY },
    );
    const sending = harness.store.claimOutboundUnit({
      recordId: pending.recordId,
      claim: { identity: IDENTITY },
    });
    harness.store.markOutboundUncertain({
      recordId: pending.recordId,
      claim: { identity: IDENTITY },
      attemptId: sending.record.activeUnit!.attemptId,
      reason: "response-lost",
    });
    const linked = harness.store.retryUncertainOutbound(
      pending.recordId,
      "operator-retry-v1:test-handle",
      { identity: IDENTITY },
    );
    let sends = 0;
    let terminal = 0;
    const worker = createTelegramDurableOutboundWorker({
      getStore: () => harness.store,
      operationGate: new RecoveryProfileOperationGate(),
      adapter: createUnitAdapterHarness({
        sendRichMessage: async () => {
          sends += 1;
          return { message_id: 900 };
        },
      }).adapter,
      onTerminal: () => {
        terminal += 1;
      },
    });
    worker.register(linked.record, { identity: IDENTITY });
    worker.schedule(linked.record.recordId);
    for (let attempt = 0; attempt < 100 && terminal === 0; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    assert.equal(sends, 1);
    assert.equal(terminal, 1);
    assert.equal(
      harness.store.getStatus().items.filter((item) => item.family === "outbound")
        .length,
      0,
    );
  } finally {
    await removeHarness(harness);
  }
});

test("durable worker advances terminal disposition once and never advances registered pending or sending", async () => {
  const harness = await createHarness();
  try {
    const committed = await commitTelegramDurableOutbound(
      baseOptions(harness),
      { store: harness.store, transformReply: identityTransform },
    );
    const terminalCallbacks: string[] = [];
    const worker = createTelegramDurableOutboundWorker({
      getStore: () => harness.store,
      operationGate: new RecoveryProfileOperationGate(),
      adapter: createUnitAdapterHarness().adapter,
      onTerminal: ({ record }) => {
        terminalCallbacks.push(record.recordId);
      },
      startSuspended: false,
    });
    const pending = harness.store.activateOutbound(
      committed.record.recordId,
      { identity: IDENTITY },
    );
    worker.register(pending, { identity: IDENTITY });
    worker.register({
      ...pending,
      recordId: "synthetic-sending",
      state: "sending",
      activeUnit: {
        unitIndex: pending.nextUnitIndex,
        attemptId: "attempt-sending",
        startedAtMs: pending.updatedAtMs,
      },
      automaticAttemptCount: 1,
    }, { identity: IDENTITY });
    assert.deepEqual(terminalCallbacks, []);

    const delivered = {
      ...pending,
      recordId: "terminal-a",
      state: "delivered" as const,
    };
    worker.register(delivered, { identity: IDENTITY });
    worker.register(
      { ...delivered, recordId: "terminal-b" },
      { identity: IDENTITY },
    );
    worker.schedule("terminal-a");
    worker.schedule("terminal-b");
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    assert.deepEqual(terminalCallbacks, ["terminal-a"]);
  } finally {
    await removeHarness(harness);
  }
});

test("durable worker suspend drains an in-flight unit and resume restarts suspended pending work", async () => {
  const drainingHarness = await createHarness();
  try {
    const committed = await commitTelegramDurableOutbound(
      baseOptions(drainingHarness),
      { store: drainingHarness.store, transformReply: identityTransform },
    );
    const pending = drainingHarness.store.activateOutbound(
      committed.record.recordId,
      { identity: IDENTITY },
    );
    let releaseSend: (() => void) | undefined;
    const sendBlocked = new Promise<void>((resolve) => {
      releaseSend = resolve;
    });
    let sendStarted = false;
    const adapter = createUnitAdapterHarness({
      sendRichMessage: async () => {
        sendStarted = true;
        await sendBlocked;
        return { message_id: 501 };
      },
    }).adapter;
    let terminalCount = 0;
    const worker = createTelegramDurableOutboundWorker({
      getStore: () => drainingHarness.store,
      operationGate: new RecoveryProfileOperationGate(),
      adapter,
      onTerminal: () => {
        terminalCount += 1;
      },
    });
    worker.register(pending, { identity: IDENTITY });
    worker.schedule(pending.recordId);
    for (let attempt = 0; attempt < 100 && !sendStarted; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    assert.equal(sendStarted, true);
    let suspendFinished = false;
    const suspending = worker.suspend().then(() => {
      suspendFinished = true;
    });
    await Promise.resolve();
    assert.equal(suspendFinished, false);
    releaseSend?.();
    await suspending;
    assert.equal(suspendFinished, true);
    assert.equal(terminalCount, 1);
    assert.equal(worker.isIdle(), true);
  } finally {
    await removeHarness(drainingHarness);
  }

  const resumeHarness = await createHarness();
  try {
    const committed = await commitTelegramDurableOutbound(
      baseOptions(resumeHarness),
      { store: resumeHarness.store, transformReply: identityTransform },
    );
    const pending = resumeHarness.store.activateOutbound(
      committed.record.recordId,
      { identity: IDENTITY },
    );
    let sends = 0;
    let terminalCount = 0;
    const worker = createTelegramDurableOutboundWorker({
      getStore: () => resumeHarness.store,
      operationGate: new RecoveryProfileOperationGate(),
      adapter: createUnitAdapterHarness({
        sendRichMessage: async () => {
          sends += 1;
          return { message_id: 502 };
        },
      }).adapter,
      onTerminal: () => {
        terminalCount += 1;
      },
      startSuspended: true,
    });
    worker.register(pending, { identity: IDENTITY });
    worker.schedule(pending.recordId);
    await new Promise((resolve) => setTimeout(resolve, 5));
    assert.equal(sends, 0);
    await worker.resume();
    for (let attempt = 0; attempt < 100 && terminalCount === 0; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    assert.equal(sends, 1);
    assert.equal(terminalCount, 1);
  } finally {
    await removeHarness(resumeHarness);
  }
});

const OUTBOUND_PROCESS_FIXTURE_PATH = join(
  process.cwd(),
  "tests/fixtures/outbound-recovery-process.ts",
);
const OUTBOUND_PROCESS_FAULTS = [
  "OUT-01",
  "OUT-02",
  "OUT-03",
  "OUT-04",
  "OUT-05",
  "OUT-06",
] as const;
type OutboundProcessFault = (typeof OUTBOUND_PROCESS_FAULTS)[number];

interface OutboundProcessCounter {
  calls: number;
  committedEffects: number;
  unexpected: string[];
}

function isOutboundFixtureMessage(
  value: unknown,
): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function startOutboundProcessFixture(
  args: readonly string[],
  faultId: OutboundProcessFault,
  counter: OutboundProcessCounter,
): ChildProcess {
  const child = fork(OUTBOUND_PROCESS_FIXTURE_PATH, [...args], {
    execArgv: ["--experimental-strip-types"],
    env: { ...process.env },
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let stderr = "";
  child.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  child.on("message", (message: unknown) => {
    if (!isOutboundFixtureMessage(message)) return;
    if (message.type === "unexpected-terminal-before-kill") {
      counter.unexpected.push(String(message.type));
      return;
    }
    if (
      message.type !== "remote-call" ||
      typeof message.requestId !== "string"
    ) {
      return;
    }
    counter.calls += 1;
    let outcome: "success" | "known-not-committed" | "commit-unknown" =
      "success";
    if (faultId === "OUT-03" && counter.calls <= 2) {
      outcome = "known-not-committed";
    } else if (faultId === "OUT-04") {
      outcome = "commit-unknown";
      counter.committedEffects += 1;
    } else {
      counter.committedEffects += 1;
    }
    child.send({
      type: "remote-response",
      requestId: message.requestId,
      outcome,
      ...(outcome === "success" ? { messageId: 1_000 + counter.calls } : {}),
    });
  });
  child.once("close", (code, signal) => {
    if (code && stderr) {
      process.stderr.write(
        `outbound recovery fixture exited ${code}/${signal ?? "none"}: ${stderr}\n`,
      );
    }
  });
  return child;
}

function waitForOutboundFixtureMessage(
  child: ChildProcess,
  predicate: (message: Record<string, unknown>) => boolean,
  timeoutMs = 20_000,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for outbound recovery fixture IPC"));
    }, timeoutMs);
    const onMessage = (message: unknown) => {
      if (!isOutboundFixtureMessage(message)) return;
      if (message.type === "fixture-error" && typeof message.error === "string") {
        cleanup();
        reject(new Error(message.error));
        return;
      }
      if (!predicate(message)) return;
      cleanup();
      resolve(message);
    };
    const onClose = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(
        new Error(
          `Outbound recovery fixture closed before IPC (${code}/${signal ?? "none"}) pid=${child.pid ?? "none"} args=${child.spawnargs.join(" ")}`,
        ),
      );
    };
    const cleanup = () => {
      clearTimeout(timeout);
      child.off("message", onMessage);
      child.off("close", onClose);
    };
    child.on("message", onMessage);
    child.on("close", onClose);
  });
}

async function stopOutboundProcessFixture(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise<void>((resolve) => {
    child.once("close", () => resolve());
  });
  child.kill("SIGKILL");
  await closed;
}

function outboundEvidence(
  message: Record<string, unknown>,
): Record<string, unknown> {
  const evidence = message.evidence;
  assert.ok(isOutboundFixtureMessage(evidence), "fixture evidence is absent");
  return evidence;
}

function outboundEvidenceNumber(
  evidence: Record<string, unknown>,
  key: string,
): number {
  const value = evidence[key];
  assert.equal(typeof value, "number", `fixture evidence ${key} is not numeric`);
  return value as number;
}

function outboundEvidenceState(evidence: Record<string, unknown>): string | null {
  const value = evidence.outboundState;
  assert.ok(value === null || typeof value === "string");
  return value as string | null;
}

function outboundEvidenceCount(
  evidence: Record<string, unknown>,
  state: string,
): number {
  const counts = evidence.counts;
  assert.ok(isOutboundFixtureMessage(counts));
  return typeof counts[state] === "number" ? counts[state] : 0;
}

function assertProcessStatusPrivacy(
  evidence: Record<string, unknown>,
  secrets: readonly string[],
): void {
  assert.equal(typeof evidence.statusJson, "string");
  for (const secret of secrets) {
    assert.doesNotMatch(evidence.statusJson as string, new RegExp(secret.replace(
      /[.*+?^${}()|[\]\\]/g,
      "\\$&",
    )));
  }
}

const configuredOutboundProcessRuns = Number.parseInt(
  process.env.PI_OUTBOUND_PROCESS_RUNS ?? "1",
  10,
);
const outboundProcessRuns = Number.isSafeInteger(configuredOutboundProcessRuns) &&
    configuredOutboundProcessRuns >= 1 && configuredOutboundProcessRuns <= 50
  ? configuredOutboundProcessRuns
  : 1;

test(
  "hard-kill/reopen OUT-01..06 preserves durable answer, unit receipt, spool, retry, privacy, and queue contracts",
  { timeout: Math.max(30_000, outboundProcessRuns * 45_000) },
  async () => {
    const tempDir = await mkdtemp(join(tmpdir(), "pi-telegram-outbound-process-"));
    const children = new Set<ChildProcess>();
    try {
      for (let iteration = 0; iteration < outboundProcessRuns; iteration += 1) {
        for (const faultId of OUTBOUND_PROCESS_FAULTS) {
          const seed = `${process.env.PI_RELIABILITY_SEED ?? "2026072205"}:${iteration}:${faultId}`;
          const rootPath = join(tempDir, `${iteration}-${faultId}-recovery-v1`);
          const attachmentPath = join(tempDir, `${iteration}-${faultId}-private.png`);
          const attachmentText = `PRIVATE-ATTACHMENT-${seed}`;
          const answerText = `PRIVATE-ANSWER-${seed}`;
          await writeFile(attachmentPath, attachmentText);
          const counter: OutboundProcessCounter = {
            calls: 0,
            committedEffects: 0,
            unexpected: [],
          };

          const crash = startOutboundProcessFixture(
            ["crash", rootPath, attachmentPath, faultId, seed],
            faultId,
            counter,
          );
          children.add(crash);
          await waitForOutboundFixtureMessage(
            crash,
            (message) => message.type === "ready" && message.faultId === faultId,
          );
          const boundary = await waitForOutboundFixtureMessage(
            crash,
            (message) =>
              message.type === "fault-boundary" && message.faultId === faultId,
          );
          assert.equal(boundary.faultAsserted, true, faultId);
          assert.equal(boundary.seed, seed, faultId);
          await stopOutboundProcessFixture(crash);
          children.delete(crash);

          const reopened = startOutboundProcessFixture(
            ["reopen", rootPath, attachmentPath, faultId, seed],
            faultId,
            counter,
          );
          children.add(reopened);
          const reopenedMessage = await waitForOutboundFixtureMessage(
            reopened,
            (message) => message.type === "reopened" && message.faultId === faultId,
          );
          const initial = outboundEvidence(reopenedMessage);
          assertProcessStatusPrivacy(initial, [
            answerText,
            attachmentText,
            attachmentPath,
            String(700_007),
          ]);

          const expectedInitialState: Record<OutboundProcessFault, string | null> = {
            "OUT-01": null,
            "OUT-02": "pending",
            "OUT-03": "retryable-pending",
            "OUT-04": "delivery-uncertain",
            "OUT-05": "delivery-uncertain",
            "OUT-06": "delivered",
          };
          assert.equal(
            outboundEvidenceState(initial),
            expectedInitialState[faultId],
            faultId,
          );
          assert.equal(
            outboundEvidenceNumber(initial, "receiptCount"),
            faultId === "OUT-06" ? 1 : 0,
            faultId,
          );
          const initialSpoolExpected =
            faultId === "OUT-02" ||
            faultId === "OUT-03" ||
            faultId === "OUT-04" ||
            faultId === "OUT-05"
              ? 1
              : 0;
          assert.equal(
            outboundEvidenceNumber(initial, "outboundSpoolRefCount"),
            initialSpoolExpected,
            faultId,
          );
          assert.equal(
            outboundEvidenceNumber(initial, "spoolFileCount"),
            initialSpoolExpected,
            faultId,
          );
          assert.equal(
            outboundEvidenceNumber(initial, "outboundSpoolBytes"),
            initialSpoolExpected ? Buffer.byteLength(attachmentText) : 0,
            faultId,
          );
          if (faultId === "OUT-01") {
            assert.equal(outboundEvidenceCount(initial, "execution-uncertain"), 1);
            assert.equal(outboundEvidenceCount(initial, "planned"), 0);
            assert.equal(outboundEvidenceCount(initial, "pending"), 0);
          }
          if (faultId === "OUT-06") {
            assert.equal(outboundEvidenceCount(initial, "completed"), 1);
            assert.equal(outboundEvidenceCount(initial, "pre-dispatch"), 1);
          }

          const finalResponse = waitForOutboundFixtureMessage(
            reopened,
            (message) => message.type === "final" && message.faultId === faultId,
          );
          reopened.send({ type: "continue" });
          const finalMessage = await finalResponse;
          const final = outboundEvidence(finalMessage);
          assertProcessStatusPrivacy(final, [
            answerText,
            attachmentText,
            attachmentPath,
            String(700_007),
          ]);
          const delivered = faultId === "OUT-02" ||
            faultId === "OUT-03" || faultId === "OUT-06";
          const uncertain = faultId === "OUT-04" || faultId === "OUT-05";
          assert.equal(
            outboundEvidenceState(final),
            delivered ? "delivered" : uncertain ? "delivery-uncertain" : null,
            faultId,
          );
          assert.equal(
            outboundEvidenceNumber(final, "receiptCount"),
            faultId === "OUT-03" ? 2 : delivered ? 1 : 0,
            faultId,
          );
          assert.equal(
            outboundEvidenceNumber(final, "spoolFileCount"),
            uncertain ? 1 : 0,
            faultId,
          );
          assert.equal(
            outboundEvidenceNumber(final, "outboundSpoolBytes"),
            uncertain ? Buffer.byteLength(attachmentText) : 0,
            faultId,
          );
          assert.equal(
            finalMessage.terminalNotifications,
            faultId === "OUT-02" || faultId === "OUT-03" ? 1 : 0,
            faultId,
          );
          assert.equal(
            finalMessage.nextTurnDispatches,
            faultId === "OUT-06" ? 1 : 0,
            faultId,
          );
          assert.equal(
            finalMessage.remainingQueueCount,
            faultId === "OUT-06" ? 1 : 0,
            faultId,
          );

          const expectedCalls: Record<OutboundProcessFault, number> = {
            "OUT-01": 0,
            "OUT-02": 1,
            "OUT-03": 4,
            "OUT-04": 1,
            "OUT-05": 1,
            "OUT-06": 1,
          };
          assert.equal(counter.calls, expectedCalls[faultId], faultId);
          assert.equal(
            counter.committedEffects,
            faultId === "OUT-01" ? 0 : faultId === "OUT-03" ? 2 : 1,
            faultId,
          );
          assert.deepEqual(counter.unexpected, [], faultId);
          await stopOutboundProcessFixture(reopened);
          children.delete(reopened);
        }
      }
    } finally {
      await Promise.all([...children].map(stopOutboundProcessFixture));
      await rm(tempDir, { recursive: true, force: true });
    }
  },
);
