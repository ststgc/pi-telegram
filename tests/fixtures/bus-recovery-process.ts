/*
 * Durable follower API bus process fixture
 * Zones: test infrastructure, recovery, multi-instance bus
 * Exercises BUS-01..04 crash/reopen boundaries with the production RecoveryStore
 * and leader envelope handler while the parent counts external mutations.
 */

import { readFile } from "node:fs/promises";

import { createTelegramBusFollowerRegistry } from "../../lib/bus.ts";
import { createTelegramBusLeaderEnvelopeHandler } from "../../lib/bus-leader.ts";
import {
  openRecoveryStore,
  parseRecoverySnapshot,
  type RecoveryBusFaultId,
  type RecoveryIdentity,
} from "../../lib/recovery.ts";
import {
  InjectedReliabilityFaultError,
  ReliabilityFaultController,
  type ReliabilityFaultId,
} from "./reliability-faults.ts";

const PROFILE = "default";
const TARGET = { chatId: 707_707, threadId: 77 };
const INSTANCE_ID = "bus-process-follower";
const OWNER_ID = "bus-process-owner";
const REGISTRATION = "bus-process-registration";
const SESSION_GENERATION = 3;
const LEADER_EPOCH = "bus-process-leader-epoch";
const blockingWord = new Int32Array(new SharedArrayBuffer(4));

const IDENTITY: RecoveryIdentity = {
  profile: PROFILE,
  target: TARGET,
  owner: {
    kind: "manual-follower",
    ownerId: OWNER_ID,
    registrationGeneration: REGISTRATION,
  },
  sessionGeneration: SESSION_GENERATION,
};

function send(message: Record<string, unknown>): void {
  if (!process.send) throw new Error("fixture requires a Node IPC channel");
  process.send(message);
}

function parseFaultId(value: string | undefined): RecoveryBusFaultId {
  if (value === "BUS-01" || value === "BUS-02" || value === "BUS-03" || value === "BUS-04") {
    return value;
  }
  throw new Error(`invalid bus fault id: ${String(value)}`);
}

function identitiesEqual(left: RecoveryIdentity, right: RecoveryIdentity): boolean {
  return left.profile === right.profile &&
    left.target.chatId === right.target.chatId &&
    left.target.threadId === right.target.threadId &&
    left.sessionGeneration === right.sessionGeneration &&
    left.owner.kind === "manual-follower" &&
    right.owner.kind === "manual-follower" &&
    left.owner.ownerId === right.owner.ownerId &&
    left.owner.registrationGeneration === right.owner.registrationGeneration;
}

function createEnvelope() {
  return {
    kind: "follower.callApi" as const,
    requestId: "bus-process-request",
    auth: "bus-process-secret",
    profile: PROFILE,
    target: TARGET,
    instanceId: INSTANCE_ID,
    manualFollowerOwnerId: OWNER_ID,
    registrationGeneration: REGISTRATION,
    followerSessionGeneration: SESSION_GENERATION,
    method: "call",
    args: [
      "sendMessage",
      {
        chat_id: TARGET.chatId,
        message_thread_id: TARGET.threadId,
        text: "PRIVATE-BUS-PAYLOAD",
      },
    ],
    sentAtMs: 1,
  };
}

function createHandler(rootPath: string, fault?: RecoveryBusFaultId) {
  const controller = fault
    ? new ReliabilityFaultController(fault as ReliabilityFaultId, {
        seed: process.argv[5] ?? "bus-process-seed",
      })
    : undefined;
  const store = openRecoveryStore({
    profile: PROFILE,
    rootPath,
    isIdentityAuthenticated: (candidate) => identitiesEqual(candidate, IDENTITY),
    ...(controller
      ? {
          fault(faultId) {
            try {
              controller.hit(faultId as ReliabilityFaultId);
            } catch (error) {
              if (!(error instanceof InjectedReliabilityFaultError)) throw error;
              controller.assertInjected();
              send({
                type: "fault-boundary",
                faultId,
                seed: controller.seed,
                faultAsserted: true,
              });
              Atomics.wait(blockingWord, 0, 0);
            }
          },
        }
      : {}),
  });
  const registry = createTelegramBusFollowerRegistry();
  registry.register({
    instanceId: INSTANCE_ID,
    manualFollowerOwnerId: OWNER_ID,
    registrationGeneration: REGISTRATION,
    sessionGeneration: SESSION_GENERATION,
    target: TARGET,
    connectedAtMs: 1,
  });
  const handler = createTelegramBusLeaderEnvelopeHandler({
    followerRegistry: registry,
    authSecret: "bus-process-secret",
    getCurrentLeaderEpoch: () => LEADER_EPOCH,
    callApi() {
      send({ type: "mutation", faultId: fault ?? null });
      return { message_id: 909 };
    },
    durableBus: {
      getStore: () => store,
      getProfile: () => PROFILE,
      getLeaderSessionGeneration: () => 9,
    },
  });
  return { handler, store };
}

async function main(): Promise<void> {
  const mode = process.argv[2];
  const rootPath = process.argv[3];
  const faultId = parseFaultId(process.argv[4]);
  if (!rootPath) throw new Error("fixture root path is required");
  if (mode === "crash") {
    const { handler } = createHandler(rootPath, faultId);
    send({ type: "ready", faultId });
    await handler(createEnvelope());
    throw new Error(`fault ${faultId} did not block`);
  }
  if (mode === "reopen") {
    const { handler, store } = createHandler(rootPath);
    const response = await handler(createEnvelope());
    const snapshot = parseRecoverySnapshot(await readFile(store.snapshotPath, "utf8"));
    send({
      type: "reopened",
      faultId,
      response,
      busState: snapshot.bus[0]?.state ?? null,
      busCount: snapshot.bus.length,
      statusJson: JSON.stringify(store.getStatus()),
    });
    return;
  }
  throw new Error(`invalid fixture mode: ${String(mode)}`);
}

await main();
