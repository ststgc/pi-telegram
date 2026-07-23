/**
 * Regression tests for durable Telegram outbound planning and source spooling
 * Zones: telegram outbound, recovery, filesystem, test infrastructure
 * Guards deterministic unit order, descriptor mutation fencing, atomic store
 * publication, path-free recovery payloads, and verified spool reconstruction.
 */

import assert from "node:assert/strict";
import type { Stats } from "node:fs";
import { mkdtemp, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  commitTelegramDurableOutbound,
  planTelegramDurableOutbound,
  readTelegramDurableOutboundSource,
  type TelegramDurableOutboundPlanOptions,
} from "../lib/outbound-recovery.ts";
import {
  openRecoveryStore,
  RecoveryQuotaExceededError,
  type RecoveryIdentity,
  type RecoveryStore,
} from "../lib/recovery.ts";

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
      fallback: {
        trigger: "known-failure",
        operationIds: ["outbound-unit-0001", "outbound-unit-0002"],
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
    finalMarkdown: "Guest caption.",
    queuedAttachments: [
      { path: "/private/source/guest.mp3", fileName: "guest.mp3" },
    ],
  }));
  assert.deepEqual(guestAttachment.recoveryPlan.units, [{
    kind: "guest",
    operationId: "outbound-unit-0000",
    method: "answerGuestQuery",
    spoolRefIndex: 0,
    fileName: "guest.mp3",
    mediaKind: "audio",
    caption: "Guest caption.",
  }]);
  assert.deepEqual(guestAttachment.recoveryPlan.buttons, []);

  const guestAttachmentWins = planTelegramDurableOutbound(baseOptions(harnessView, {
    replyToMessageId: 0,
    guestQueryId: "guest-query-attachment-wins",
    finalMarkdown: "Guest caption.\n\n<!-- telegram_voice: Ignored voice. -->",
    queuedAttachments: [
      { path: "/private/source/guest.pdf", fileName: "guest.pdf" },
    ],
  }));
  assert.equal(guestAttachmentWins.recoveryPlan.voice, undefined);
  assert.equal(guestAttachmentWins.recoveryPlan.units[0]?.kind, "guest");

  const guestVoice = planTelegramDurableOutbound(baseOptions(harnessView, {
    replyToMessageId: 0,
    guestQueryId: "guest-query-2",
    finalMarkdown: "Speak automatically.",
    automaticVoice: true,
    generatedVoice: [
      { path: "/private/source/guest.opus", fileName: "guest.opus" },
    ],
  }));
  assert.deepEqual(guestVoice.recoveryPlan.units, [{
    kind: "guest",
    operationId: "outbound-unit-0000",
    method: "answerGuestQuery",
    spoolRefIndex: 0,
    fileName: "guest.opus",
    mediaKind: "voice",
  }]);
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
    finalMarkdown: caption,
    queuedAttachments: [{ path: "/private/source/file.pdf", fileName: "file.pdf" }],
  }));
  const unit = guest.recoveryPlan.units[0];
  assert.equal(unit?.kind, "guest");
  assert.equal(Array.from(unit?.kind === "guest" ? unit.caption ?? "" : "").length, 1024);
  assert.equal(unit?.kind === "guest" ? unit.caption : undefined, "😀".repeat(1024));
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

test("commit preserves voice semantics without transforming voice-only text", async () => {
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
    assert.deepEqual(calls, []);
    const payload = JSON.parse(Buffer.from(committed.payload).toString("utf8")) as {
      finalMarkdown: string;
      voice?: { text: string; automatic: boolean };
    };
    assert.equal(payload.finalMarkdown, "");
    assert.deepEqual(payload.voice, { text: "Speak this answer.", automatic: true });
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
