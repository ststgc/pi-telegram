# Telegram Interactions API

## Purpose

The Telegram Interactions API lets trusted extension consumers pause one active Telegram-originated tool flow, ask the paired owner one bounded question in the same Telegram target, and resume the same Promise with a structured answer. `pi-telegram` owns the question view, callbacks, reply routing, authorization, native typing suppression, and terminal cleanup.

This is a narrow active-turn capability. It does not mirror arbitrary `ctx.ui` calls, expose Telegram transport, turn answers into new Pi prompts, or make local-only tools remotely interactive without an explicit consumer adapter.

The stable package membrane is:

```ts
import {
  requestTelegramInteraction,
  type TelegramInteractionAttempt,
  type TelegramInteractionRequest,
} from "@ststgc/pi-telegram/interactions";
```

## Public Contract

```ts
export interface TelegramInteractionOption {
  label: string;
  value?: string;
  description?: string;
}

export type TelegramInteractionMode =
  | { kind: "text" }
  | {
      kind: "single-select";
      options: readonly TelegramInteractionOption[];
    }
  | {
      kind: "multi-select";
      options: readonly TelegramInteractionOption[];
    };

export interface TelegramInteractionRequest {
  question: string;
  details?: string;
  mode: TelegramInteractionMode;
  timeoutMs?: number;
  signal?: AbortSignal;
}

export type TelegramInteractionAnswer =
  | { type: "text"; label: string; value: string }
  | {
      type: "option";
      index: number;
      label: string;
      value: string;
    }
  | { type: "other"; label: string; value: string };

export type TelegramInteractionResult =
  | { status: "answered"; answers: readonly TelegramInteractionAnswer[] }
  | {
      status: "cancelled" | "timed-out" | "unavailable";
      message?: string;
    };

export type TelegramInteractionAttempt =
  | {
      handled: false;
      reason: "no-active-telegram-turn" | "runtime-unavailable";
    }
  | { handled: true; result: TelegramInteractionResult };

export function requestTelegramInteraction(
  request: TelegramInteractionRequest,
): Promise<TelegramInteractionAttempt>;
```

The function resolves the current process-local interaction runtime on every call. It never captures or returns a Pi context.

## Bounds And Mapping

| Field | Contract |
| --- | --- |
| `question` | Trimmed, non-empty, at most 4,000 UTF-16 code units |
| `details` | Optional; trimmed and omitted when empty; at most 8,000 UTF-16 code units |
| select `options` | 1 through 20 entries |
| option `label` | Trimmed, non-empty, at most 1,000 UTF-16 code units |
| option `value` | Optional, at most 1,000 UTF-16 code units; only `undefined` defaults to the trimmed label, so an explicit empty string remains valid |
| option `description` | Optional; trimmed and omitted when empty; at most 2,000 UTF-16 code units |
| `timeoutMs` | Finite integer from 1,000 through 3,600,000 ms; default exactly 900,000 ms (15 minutes) |
| text or Other answer | Trimmed, non-empty, at most 48 KiB UTF-8 |

`text` mode rejects an `options` field. Duplicate labels and values are valid because source position is the selection identity. Public option indexes are one-based. Multi-select answers return in source order and require at least one selection before Submit.

Telegram receives only the question, details, labels, descriptions, and bounded display indexes. Option `value` fields remain process-local: they do not enter Telegram text, callback data, or diagnostics.

## Claim And `handled` Semantics

A runtime claims at most one interaction for the current active Telegram turn. Questionnaires are consumer-owned sequential composition: await one request, map its result, then issue the next request only if the first completed as intended. There is no batch-question or arbitrary form contract.

`handled` is the fallback boundary:

- `handled: false` means no Telegram interaction surface was claimed or sent. A consumer may use its established local TUI/RPC fallback when the originating turn is local.
- Once an active Telegram turn is claimed, every terminal result remains `handled: true`, including invalid request data, cancellation, timeout, authority loss, render failure, or transport failure. A consumer must not open a second local UI after that point.
- A concurrent request against the same active turn returns `handled: true` with `status: "unavailable"`.

An invalid request is therefore `handled: false` only when no active Telegram turn/runtime existed to claim; with an active turn it is `handled: true / unavailable`.

Consumer cancellation uses only `signal`. An already-aborted signal settles as `cancelled` after claim. Pi `/abort` and `/stop` keep command precedence and are never captured as text answers; an adapter should pass its tool abort signal so `/abort` cancels the same pending request. Timeout, abort, shutdown, and answer races settle once. Session shutdown, transport replacement, profile change, follower re-registration, or other authority invalidation settles the claimed interaction as `unavailable` independently of consumer cancellation.

## Answer UI And One-Use Callbacks

- Text mode begins by asking the owner to reply to the question message.
- Single-select resolves after one authorized choice.
- Multi-select toggles choices in the same logical view and resolves on Submit; an empty Submit is rejected without settling.
- Select modes include Other, which edits the same logical view into text-reply mode.
- Cancel is always available.

After settlement, the bridge attempts a terminal label/edit as bounded best-effort cleanup. When its deadline expires, the old mutation releases Delivery's target queue and stops before any later chunk step, so it cannot delay the next question or Pi work; the already-settled result never waits for that UI cleanup.

The bridge owns `interact:<token>:<action>[:<index>]`. Tokens are opaque 16-character base64url values, actions are `pick`, `toggle`, `submit`, `other`, or `cancel`, indexes are unsigned base36, and the complete callback is ASCII at most 40 bytes. Each token is one-use: it is claimed before acknowledgement and rotated after a nonterminal state change. Stale, replayed, malformed, or unauthorized interaction callbacks receive a bounded expiry response and never enter public callback fallback or Pi prompt routing.

A text or Other answer is accepted only when all of these are true:

1. it is paired-owner, non-command text;
2. it replies to the current final message id of the interaction's logical question view;
3. it is newer than that anchor and is not one of the active turn's source messages;
4. its exact target, profile, transport generation, session generation, and direct-owner epoch or follower registration generation still match the claim; and
5. its normalized UTF-8 body is within the documented bound.

An arbitrary next message in the chat/thread is never consumed as an answer. When delivery reconciliation changes the final chunk id, that returned id becomes the new sole reply anchor.

## Ownership And Routing

Interaction input is private bridge control traffic, not a public raw-update event. The inbound order is:

1. first-contact pairing proof handling and exact paired-sender authorization;
2. internal interaction-priority classification;
3. exact owner routing or settlement;
4. public `registerTelegramUpdateHandler()` dispatch for non-interaction updates;
5. ordinary commands, menus, callbacks, queue, edits, and prompt routing.

Only `interact:` callbacks and non-command text replies to a bot message recorded with private `purpose: "interaction"` enter the priority path. Other updates continue unchanged. `/abort` and `/stop` are explicitly returned to ordinary command routing while an interaction is pending.

The claim snapshots the active turn, exact `{ chatId, threadId? }`, active profile and transport generation, session generation, and current direct owner/leader epoch or follower registration generation. Every candidate is checked against that complete identity. A stale record, wrong target, wrong profile, wrong generation, or lost authority cannot settle another interaction.

In Threaded Mode, the leader does not own follower interaction state. It recognizes the private message-purpose record, then forwards the exact complete Telegram update through the existing authenticated bus to the exact live `recipientInstanceId` and `recipientRegistrationGeneration`. The follower runs the same priority classifier before its process-local public handlers and settles its own Promise. The private follower delivery marker is validated and stripped before Bot API transport; it contains no question, answer, target, or option value. No second interaction registry, parallel IPC path, persisted answer, or queued Pi prompt is created.

## Waiting And Native Typing

A claimed interaction acquires a generation-bound waiting lease from the native typing owner before the question is rendered. Acquisition fences new `sendChatAction(typing)` work, clears the repeating loop, and waits for the existing bounded in-flight drain. A request that already began may finish, but no old-generation replacement begins after the fence.

While the lease is current, agent/message hooks cannot re-arm native typing. This prevents Telegram from showing that Pi is still producing work while Pi is actually waiting for the owner. On an authorized answer, the lease may resume the previous typing loop only if the same activity, active turn, target, session, and transport authority are still current. Cancellation, timeout, unavailability, `/abort`, agent completion, and session shutdown release or clear the lease without restarting typing. Releasing a stale lease is a no-op.

The interaction lifecycle is session-bound and memory-only. Shutdown invalidates it before the Delivery generation is torn down, so an old runtime cannot adopt a replacement target or issue follow-up mutations through the new generation.

## Diagnostics And Privacy

Interaction diagnostics are metadata-only runtime events. They may contain the generated correlation id, lifecycle phase, interaction mode, result class, bounded duration, and a redacted failure class. They must not contain question or answer bodies, option labels or values, raw callbacks, Telegram user/chat/thread/message ids, target identities, bot tokens, or transport payloads. Diagnostics are evidence only; they are never routing or settlement authority.

## Consumer Integration And Fallback

A consumer must explicitly adapt its own interactive operation. `pi-telegram` does not intercept or mirror arbitrary `ctx.ui.input()`, `ctx.ui.select()`, editor, or custom widget calls.

For example, an `ask_user_question` adapter can preserve its existing normalization and sequential questionnaire semantics while trying `requestTelegramInteraction()` one question at a time. The safe compatibility pattern is:

1. normalize locally and enter the consumer's existing UI/checkpoint guard;
2. lazily import `@ststgc/pi-telegram/interactions` and verify that `requestTelegramInteraction` is a function;
3. use local TUI/RPC only when the first attempt is `handled: false` and the source turn is not Telegram-originated;
4. fail closed as unavailable when a Telegram-originated turn lacks the API, or when any later sequential question loses the already-claimed runtime; and
5. never switch surfaces after any request reports `handled: true`.

A lazy capability check keeps an older runtime from breaking consumer extension load. It is a compatibility fail-safe, not evidence that the older package supports Telegram interactions.

## Package Migration And Rollout

Interaction support has three separate deliverables:

| Layer | Completion condition |
| --- | --- |
| Source | The canonical `@ststgc/pi-telegram/interactions` export, bridge runtime, tests, and public docs are accepted in this repository. |
| Consumer | The installed tool/extension is separately adapted to call the public API with local fallback and old-runtime fail-safe behavior. Source support alone does not modify consumers. |
| Live rollout | The accepted canonical package and consumer adapter are installed/reloaded, then classic and Threaded Mode behavior is verified against a real Telegram client under explicit operator approval. |

An operator still running `@llblab/pi-telegram@0.20.6` must migrate through Pi's supported package mechanism to the canonical GitHub-distributed `@ststgc/pi-telegram` package before the public interaction import can exist. Companion imports must also use `@ststgc/pi-telegram/interactions`. Do not hand-edit package configuration, package locks, or installed consumer files as part of source-only work.

The source implementation and a reviewed consumer patch can be ready while the installed environment remains unchanged. Report each rollout layer independently rather than turning partial evidence into an all-or-nothing deployment claim: for example, **source ready; consumer activation pending**, **Classic deployed; Threaded live smoke pending**, or **Classic and Threaded deployed**. A real Classic client pass is not evidence for follower routing when the approved bot reports Threaded Mode disabled.

## Non-Goals

This API does not provide:

- arbitrary Pi TUI or `ctx.ui` mirroring;
- a generic form/widget protocol or multiple simultaneous interactions;
- cross-profile or cross-instance question targeting;
- raw Telegram clients, credentials, polling, or Bot API access;
- durable/persisted interaction state or automatic replay after restart;
- a queued prompt generated from the answer;
- installation, configuration mutation, reload, or live Telegram activation.
