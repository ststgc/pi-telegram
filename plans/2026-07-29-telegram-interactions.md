# Telegram-Owned Interactive Question Execution Contract

Status: plan quality gate clean after 3/3 review rounds plus disclosed parent final-correction audit
Baseline date: 2026-07-29
Primary source repository: `https://github.com/ststgc/pi-telegram`
Primary source baseline: `origin/main@fd35b1f752e8cadf481b1b04ef5969cc310d7568` (`0.25.0`)
Integration consumer: `/Users/ststgc/.pi/agent/extensions/ask-user-question.ts` (non-Git, installed extension)
Observed runtime package: `@llblab/pi-telegram@0.20.6`; canonical source package: `@ststgc/pi-telegram@0.25.0`

## 0. Plan quality contract

- During this planning loop, only this file may be edited. Source, tests, public docs, installed extensions, package configuration, and live Telegram state are read-only evidence.
- Quality gate: no unresolved blocker or user decision; no correction worth doing now; the whole-plan execution contract is safe for a downstream worker.
- Review rubric: source grounding, spec conformance, scope control, handoff scope safety, validation quality, risk/rollback, and explicit open-decision handling.
- Review cap: three fresh review rounds. A final writer correction after round three is allowed, but must be disclosed as not independently re-reviewed.
- Keep rule: retain revisions that remove blockers or concrete corrections without widening scope, weakening authorization, bypassing follower routing, or changing read-only evidence to make the plan pass.
- Rollback: while this plan is untracked in the detached planning worktree, restore it from the parent-owned pre-round copy/session artifact; do not claim `git restore` can recover it. Source rollback is defined separately per implementation slice.
- Pause only if evidence requires an unapproved public-contract, security, product, external-write, or live-runtime decision. Ordinary local code, tests, docs, and reversible source changes are not blockers.
- Completion evidence: final plan path, review-round synthesis, final diff inspection, source-doc unchanged check, and parent per-rubric audit.

## 1. Goal

Make a Telegram-originated Pi turn that calls an interactive question tool answerable from the exact Telegram chat/thread that owns the turn, without leaving Telegram in an indefinite native `typing`/Thinking state.

The first consumer is the existing `ask_user_question` tool. The implementation must also establish a narrow, reusable pi-telegram interaction contract rather than hard-coding or monkeypatching that tool's TUI implementation.

## 2. Success criteria

1. A Telegram-owned `ask_user_question` call renders its question in the owning Telegram target and stops native `typing` while it waits.
2. A button choice or accepted text reply resolves the same pending tool execution; the answer does not become a second queued Pi prompt.
3. Single-select, multi-select, free-text, and one-to-four-question questionnaires work. Questionnaires are executed sequentially through the single-question bridge contract.
4. `/abort` and `/stop` remain immediately usable while a question is pending. Timeout, the tool's `AbortSignal`, interaction-runtime unbind/shutdown, session shutdown/reload, and transport-authority loss settle the pending Promise exactly once.
5. A replayed, stale, unauthorized, cross-thread, cross-profile, or wrong-follower-generation answer cannot resolve an interaction.
6. Threaded Mode callbacks and text answers are forwarded to the exact owning follower registration; the leader's process-local public update registry is not treated as a cross-process interaction bus.
7. Native `typing` cannot be re-armed by assistant message start/update while the interaction waiting lease is held. It resumes only if the same live activity remains active after the lease is released.
8. A local TUI/RPC call keeps the existing `ask_user_question` UX. A runtime without the new Telegram capability never enters an unanswerable TUI wait for a current `[telegram...]` turn.
9. `npm run typecheck`, focused domain/integration tests, `npm test`, Domain DAG validation, package smoke/dry-run, and the repository's complete `npm run validate` gate pass on the final source branch.
10. Source implementation, non-Git consumer activation, and live package cutover remain separately reviewable and reversible.

## 3. Non-goals

- Do not mirror arbitrary sibling-extension `ctx.ui.confirm/input/select/custom` calls.
- Do not monkeypatch `ctx.ui`, inject terminal input, use a PTY, run Pi in shadow RPC mode, or launch another Pi process.
- Do not add a second Telegram polling loop, expose bot credentials, or let a companion extension call unrestricted Bot API methods.
- Do not use assistant `telegram_button` callbacks as the final native solution; they enqueue another prompt and cannot resolve the currently running tool Promise.
- Do not create a general workflow/form engine, durable questionnaire history, concurrent interactions per turn, answer editing, media answers, Web Apps, or cross-session recovery of an interrupted tool execution in the first release.
- Do not persist question text, option text, answer text, user/chat/thread ids, or callback payloads in status/log diagnostics.
- Do not publish npm packages, push branches, merge PRs, tag releases, install packages, modify the live bot, or edit the installed consumer during source implementation unless a later operator step explicitly authorizes that exact external effect.
- Do not redesign menus, assistant rendering, queue semantics, or general callback ownership outside the interaction path.

## 4. House rules

1. Preserve the Mobile Companion boundary and use public Pi extension APIs only.
2. Preserve the Flat Domain DAG and keep `index.ts` composition-only; interaction policy/state belongs in `lib/interactions.ts`.
3. One live interaction may claim one active Telegram turn at a time. Questionnaires reuse that claim sequentially; a competing claim returns `handled: true` with `status: "unavailable"` and a bounded busy message rather than blocking, widening the result union, or stealing the turn.
4. `handled: false` is permitted only before a Telegram turn is claimed. Once claimed or rendered, every result is `handled: true`, so a consumer cannot silently fall back to a different surface.
5. Callback tokens are random, opaque, one-use, under Telegram's 64-byte limit, and contain no option value or identity. Generation is a fence, not a secret.
6. Answer authorization requires the paired owner, exact profile, exact target, current session/interaction generation, and—when routed to a follower—the exact live follower registration generation.
7. Bridge-owned safety commands precede text-answer capture. A pending free-text interaction must not consume `/abort`, `/stop`, `/next`, `/compact`, menu commands, or an unrelated message.
8. Free text is accepted only from a non-empty, bounded owner text message sent after the interaction began and replying to the current question message in the exact target. Media, edits, old messages, and unanchored text pass through normal routing in the initial contract.
9. Pending interaction settlement is idempotent. Answer, callback replay, timeout, tool-signal abort, authority invalidation, runtime unbind, and session shutdown may race, but only the first valid terminal transition wins.
10. Session shutdown settles interactions before invalidating the delivery generation. Keyboard/status cleanup is bounded best effort; failure must never leave the Promise pending.
11. A `commit-unknown` question send/edit is never blindly replayed. In-memory tokens are invalidated and the caller receives a handled unavailable/cancelled result.
12. Holding a waiting lease suppresses both the current typing loop and every normal re-arm path. Lease release does not invent activity; it restarts typing only from current, generation-valid agent state.
13. Runtime diagnostics are metadata-only: phase, mode, result class, generation-safe correlation id, and bounded duration. No prompt/answer bodies or transport ids.
14. Keep callback acknowledgement fast. Resolve/acknowledge the tap before bounded message edit or next-question rendering; do not hold the polling loop on unrelated work.
15. Preserve existing local TUI/RPC rendering, checkpoints, answer limits, and result schema in `ask-user-question.ts`.

## 5. Evidence and sources

### 5.1 Reproduced causal chain

- The screenshot shows Pi stopped inside the `ask_user_question` TUI while Telegram has no selectable surface and continues to appear active.
- `/Users/ststgc/.pi/agent/extensions/ask-user-question.ts` routes TUI execution through `askQuestionnaireTui`, `askTextTui`, `askSingleChoiceTui`, and `askMultiChoiceTui`, and RPC through `ctx.ui.input/select`; there is no Telegram response path or general timeout.
- `docs/architecture.md` explicitly states that pi-telegram does not mirror arbitrary sibling `ctx.ui.*` prompts.
- `lib/runtime.ts` owns the 2.5-second native typing loop; `lib/lifecycle.ts:createTelegramMessageActivityTypingHooks` re-arms typing before and after assistant message activity while an active turn exists.
- `lib/updates.ts:createTelegramUpdateHandle` dispatches public update handlers in the polling process before default ownership routing. `executeTelegramUpdatePlan` performs callback/message ownership forwarding later. Therefore `/delivery` plus `/updates` in a follower-local consumer is insufficient for Threaded Mode.
- `docs/delivery.md` and `lib/delivery.ts` provide generation-bound, target-authorized operational send/edit/delete but intentionally do not provide a managed interaction Promise or general callback registry.
- `docs/activity.md` makes Activity non-blocking observation; it must not become the control plane for pausing core typing.
- `lib/bus.ts` already carries exact follower `registrationGeneration` and target ownership. The interaction path must reuse that authority rather than persisted thread hints.

### 5.2 Version and workflow evidence

- The canonical source checkout is detached at `origin/main@fd35b1f` and clean apart from plan-loop artifacts created by this planning session.
- `origin/dev@a0dc2af` has moved beyond `origin/main`; implementation must first inspect the `main..dev` delta and choose the current canonical development base rather than blindly using this plan's observed main SHA.
- The canonical package/import namespace is `@ststgc/pi-telegram`; the observed installed runtime remains `@llblab/pi-telegram@0.20.6`.
- `AGENTS.md` requires user-visible behavior to update README/docs/CHANGELOG in the same source change and states that updating the installed Pi extension is a separate operator step.
- The primary source repository has `validate.yml` and `release.yml`; local source validation remains mandatory, while push/PR/release are not authorized by this plan.
- `/Users/ststgc/.pi/agent/extensions/ask-user-question.ts` is not in a Git worktree. Its observed SHA-256 at planning time is `fc5db121a88f8aa5d0b6e673ad6d2a1cbf23fbd399de6d41786e6087b0f842df`.

### 5.3 Existing quality bar

- `api/delivery.ts` plus `lib/delivery.ts` establish the package membrane, generation-bound global runtime, structured failure, target resolution, and stale-handle conventions to reuse.
- `api/activity.ts` plus `lib/activity.ts` establish stable identity registration and non-blocking lifecycle dispatch conventions.
- `lib/sections.ts` and callback namespace docs establish opaque callback tokens and stale callback behavior.
- `lib/updates.ts:createTelegramUpdateHandle` currently invokes the process-local public registry after pairing/sender authorization but before `defaultHandle` owner routing. Interaction answers therefore require a new internal priority route before that registry; placing the hook only inside `executeTelegramUpdatePlan` would allow public handlers to observe or consume private answers.
- Mirrored tests and `tests/integration.test.ts` are the repository-standard seams; `tests/invariants.test.ts` and the Domain DAG script protect architecture.

## 6. Context gap scan

| Area | Classification | Evidence / plan response |
| --- | --- | --- |
| QA gates | covered | `npm run typecheck`, focused Node test runner commands, `npm test`, `npm run domain-dag`, `npm run pack:smoke`, `npm run pack:check`, `npm run validate`. |
| Tests | gap slice needed | No interaction domain or same-tool Telegram answer harness exists. Slice S0 creates a correct-seam deterministic harness before behavior implementation. |
| Security/privacy | covered | Existing paired-owner, target, delivery-generation, follower-registration, callback-size, and redacted-diagnostic contracts are reusable; explicit negative cases are listed below. |
| Public contracts | covered | New stable `./interactions` membrane is required; `/delivery`, `/updates`, and `/activity` remain unchanged in responsibility. |
| Repository workflow | covered | Primary source is Git Lane; installed consumer is Non-Git Lane; live install is a separately authorized rollout. |
| Operations/rollback | covered | Source branch rollback, consumer file backup/hash, package-coordinate rollback, stale-token invalidation, and metadata diagnostics are specified. |
| External effects | out of autonomous source scope | Telegram live writes, package install/cutover, push/PR/merge/tag/release require a separate exact operator action. Local fixtures incur no external effect. |
| CI/CD | covered | Existing validation and release workflows are evidence; this plan does not add or require new infrastructure. |
| Real Telegram client behavior | missing but non-blocking for source; rollout gate | Deterministic fixtures prove protocol. A later approved classic and Threaded live smoke proves client behavior before declaring operational rollout complete. |
| Consumer source control | gap slice needed | The consumer is a non-Git installed file. Slice S4 requires one-writer backup/hash/diff/validation evidence and does not pretend there is a PR lane. |

## 7. Repository workflow and autonomous scope

### 7.1 Primary source Git Lane

- Before writes, fetch `origin`, inspect `origin/main..origin/dev`, read the current latest plan/release status, and select the canonical development base. Default: branch from current `origin/dev` because the repository's established feature flow targets `dev`; stop only if current repo evidence has replaced that policy.
- This reviewed plan is initially an untracked absolute-path artifact in the detached planning worktree. Before source edits, copy its exact final bytes into the new implementation worktree at `plans/2026-07-29-telegram-interactions.md`, record both SHA-256 values, verify they match, and include the plan in the feature branch diff. Do not assume a new worktree already contains it or that Git can restore its pre-branch state.
- Create one feature branch/worktree such as `fix/telegram-interactions`; keep one source writer. Do not edit the detached baseline worktree if it contains unrelated user changes.
- Do not reset, clean, or remove unrelated files. Planning-loop `.pi-subagents` artifacts are parent-owned temporary evidence and must not be committed.
- Source implementation, tests, and docs are autonomously in scope. Push, PR, merge, tag, release, and live Telegram smoke are not authorized by this planning request.
- Completion evidence for the Git Lane is the path-scoped diff, branch/base/head SHAs, validation output, independent review, and no unrelated tracked changes.

### 7.2 Consumer Non-Git Lane

- `/Users/ststgc/.pi/agent/extensions/ask-user-question.ts` has no branch/PR/clean-main semantics.
- A separate rollout writer must verify the current file hash, save a private timestamped backup with mode preserved, edit only this consumer file, run its available load/type/runtime checks, report the exact changed file and diff, and retain the backup until live smoke passes.
- If the observed hash has changed, re-read and port semantically; do not overwrite from this plan's snapshot.
- This consumer edit is not part of normal in-repo source implementation. It begins only during an explicitly authorized rollout after the source API is accepted.

### 7.3 Explicitly out-of-scope until rollout approval

- Changing `/Users/ststgc/.pi/agent/npm/package.json`, package lock state, `settings.json`, or active installed package coordinates.
- Running `pi install`, `/reload`, `/telegram-connect`, Bot API writes, or a real callback/text-answer smoke.
- Pushing or merging source changes.

Source slices S0-S3 and S5-S6 may complete without these effects. S4 prepares the consumer patch contract; S7 performs activation only after exact approval.

## 8. Target design

### 8.1 Stable public API

Add `@ststgc/pi-telegram/interactions` as a stable package membrane over `lib/interactions.ts`.

Frozen contract:

```ts
export interface TelegramInteractionOption {
  label: string;          // 1..1,000 UTF-16 code units
  value?: string;         // 0..1,000; defaults to label; never sent to Telegram
  description?: string;   // 0..2,000
}

export type TelegramInteractionMode =
  | { kind: "text" }
  | { kind: "single-select"; options: readonly TelegramInteractionOption[] }
  | { kind: "multi-select"; options: readonly TelegramInteractionOption[] };

export interface TelegramInteractionRequest {
  question: string;       // 1..4,000 UTF-16 code units
  details?: string;       // 0..8,000
  mode: TelegramInteractionMode;
  timeoutMs?: number;     // finite integer, 1,000..3,600,000; default 900,000
  signal?: AbortSignal;   // the sole consumer-owned cancellation capability
}

export type TelegramInteractionAnswer =
  | { type: "text"; label: string; value: string }
  | { type: "option"; index: number; label: string; value: string } // one-based public index
  | { type: "other"; label: string; value: string };

export type TelegramInteractionAttempt =
  | { handled: false; reason: "no-active-telegram-turn" | "runtime-unavailable" }
  | { handled: true; result: TelegramInteractionResult };

export type TelegramInteractionResult =
  | { status: "answered"; answers: readonly TelegramInteractionAnswer[] }
  | { status: "cancelled" | "timed-out" | "unavailable"; message?: string };

export function requestTelegramInteraction(
  request: TelegramInteractionRequest,
): Promise<TelegramInteractionAttempt>;
```

Validation and mapping decisions:

- Trim `question` and option `label` for validation/display and reject them when empty after trimming. Optional empty `details`/`description` are omitted from rendering. An explicitly empty `value` remains a valid machine value; only `undefined` defaults to the trimmed label.
- Select modes require 1..20 options. `text` has no options field and rejects a structurally mismatched mode. Duplicate labels/values remain allowed because source position is the stable selection identity.
- Public `TelegramInteractionAnswer.index` is one-based to preserve `ask_user_question`; callback payload indexes are zero-based unsigned base36 and must be range-checked before conversion.
- Free-text and Other answers are trimmed, non-empty, and at most 48 KiB UTF-8, matching `ask_user_question`'s current `MAX_ANSWER_BYTES`. Multi-select returns options in source index order and requires at least one selection before Submit.
- Button labels are a bounded display projection such as `<one-based index>. <truncated label>`. Telegram renders only question/details plus option labels/descriptions. Machine `value` fields never enter Telegram text, callback data, logs, or diagnostics; they remain process-local and appear only in the resolved API result.
- `handled: false` means no Telegram UI was claimed or sent and a local consumer may choose its existing TUI/RPC path.
- After a successful claim, every outcome is `handled: true` even if render/transport later fails.
- Default timeout is exactly 900,000 ms (15 minutes); accepted range is 1,000..3,600,000 ms. Invalid requests return `handled: true / unavailable` only if an active Telegram turn was atomically identified; otherwise they return `handled: false` without claiming a surface.
- Consumer cancellation is represented only by `request.signal`; no second public disposer/handle is added. Pi `/abort` aborts the tool signal, while runtime/session/authority shutdown settles independently inside pi-telegram.
- Consumer mapping is fixed: `answered` preserves the existing answer union; `cancelled` and `timed-out` become the existing consumer `cancelled` result with a bounded reason; `unavailable` becomes the existing `unavailable` result.
- One active turn has one interaction claim. A questionnaire is consumer-owned sequential composition over this API.
- The owned callback grammar is `interact:<token>:<action>[:<index>]`, where `token` is 16 base64url characters (96 random bits), `action` is one of `pick|toggle|submit|other|cancel`, and optional `index` is unsigned base36. Encoded data is ASCII and at most 40 bytes. Add `interact:` to every reserved-prefix list and ownership test.
- Option values stay process-local. Telegram receives only display labels and opaque token/action/index ids.
- The public function resolves the current process-global runtime on every call. Load order is tolerant; it never captures Pi `ExtensionContext`. The typed module exposes only the callable contract above; consumers validate `typeof requestTelegramInteraction === "function"` and rely on package semver rather than an extra public version symbol.

### 8.2 Interaction state machine

`lib/interactions.ts` owns a session-generation-bound runtime with states:

```text
idle
  -> claimed
  -> awaiting-choice | awaiting-text
  -> answered | cancelled | timed-out | unavailable
  -> idle
```

Required properties:

- Claim atomically snapshots active turn identity, exact target, source message id, profile/transport stamp, direct owner/leader epoch or follower registration generation, and session generation.
- It stores one opaque interaction id, question-message delivery handle, the final concrete `messageId` from the latest handle as the sole text-reply anchor, one-use callback tokens, selected option indexes, start/update identity, timeout/abort subscriptions, and the typing lease. After every multi-chunk edit/growth/shrink reconciliation, refresh the anchor from the returned handle's final message id before accepting text.
- Interaction authority is an explicit port over current direct-owner and follower-registration lifecycle state. Every existing transport replacement/takeover, follower deregistration/re-registration, profile switch, and session replacement invalidates the captured stamp before Delivery is rebound; invalidation settles `handled: true / unavailable`, removes tokens/listeners/timers, then releases the waiting lease. A later Delivery operation is not required to discover authority loss.
- Interaction question sends carry a private non-Bot-API ownership purpose (`purpose: "interaction"`) through Delivery/message-ownership recording. For follower sends, a private bus metadata marker is validated and stripped beside existing aggregate metadata before Bot API transport, then records the same purpose with exact recipient registration generation. The marker contains no question, answer, target, or option value. The leader can therefore recognize replies to foreign interaction anchors before public handlers without maintaining a second interaction registry.
- Terminal transition removes tokens/listeners/timers first, resolves once, then performs bounded best-effort message finalization.
- Multi-select callbacks toggle process-local selection and edit the same logical view. `Submit` is disabled semantically until at least one option is selected; `Cancel` is always available.
- Single-select resolves immediately after authorized acknowledgement.
- `Other` transitions to `awaiting-text` and edits the question to require a reply to that exact message.
- Text-only interaction begins in `awaiting-text`; accepted text must reply to the current question message.
- Stale callback tokens receive a short expiry acknowledgement and never enter `[callback]` fallback or a Pi prompt.

### 8.3 Routing order and Threaded Mode

Do not implement interactions through the public pre-routing update registry.

Internal order after the existing pairing-proof and paired-sender authorization gates:

1. Run a private interaction-priority classifier before the process-local public update registry.
2. Treat only `interact:` callbacks or non-command text replies whose replied-to bot message has current `purpose: "interaction"` ownership as interaction candidates. Every slash-prefixed message bypasses interaction capture and continues to existing command routing.
3. Resolve message/target ownership. Forward a foreign candidate to the exact live follower using the existing authenticated full-update path and exact `recipientInstanceId` plus `recipientRegistrationGeneration`; the follower runs the same priority classifier before its local public handlers.
4. In the owning process, resolve the current interaction. Valid answers settle it; stale/replayed/denied/wrong-profile/wrong-generation candidates are consumed with a bounded expired/unavailable acknowledgement and never reach public handlers or Pi fallback.
5. Only non-interaction updates enter `registerTelegramUpdateHandler` dispatch and then the existing default menus, sections, generated buttons, queue, edited-message, command, and prompt routing.

The concrete composition change is a private `priorityHandle(update, ctx)` port on `createTelegramUpdateHandle`, invoked after pairing/sender authorization and before `registry.dispatch`. It returns `pass` or a complete `TelegramInboundHandlingOutcome`. Extract/reuse the existing foreign callback/message ownership and forwarding helpers rather than duplicating authority logic. `executeTelegramUpdatePlan` remains the ordinary default route; it does not become the sole interaction privacy boundary.

For callbacks, `interact:` itself marks the priority candidate and message ownership selects the owner even when Telegram omits `message_thread_id`. For free text/Other, only reply-to ownership with `purpose: "interaction"` marks the candidate; ordinary replies remain public/default updates. Interaction purpose must follow the final message id after Delivery reconciliation.

Do not add a second follower interaction registry at the leader. The leader only classifies ownership purpose and forwards; the follower-local runtime owns Promise settlement. Callback acknowledgement/edit/delete routes back through existing follower API transport. Existing full-update envelopes remain the default; the only planned bus/ownership extension is the private interaction-purpose marker required for pre-public text classification. Do not create parallel IPC or persist interaction answers.

### 8.4 Waiting lease and native activity

Extend the typing owner with a generation-bound suppression lease rather than calling `stop()` once.

Required port behavior:

```ts
acquireWaitingLease(identity): Promise<TelegramTypingWaitingLease>
lease.release({ resumeIfActive: boolean }): void
isWaiting(): boolean
```

- Acquisition synchronously increments a typing-generation fence before clearing the interval. Every queued pre-send microtask rechecks that generation and `isWaiting()` immediately before starting Bot API work, so no old-generation request begins after acquisition.
- `acquireWaitingLease` then awaits the existing bounded `typing.waitForIdle()` drain before resolving; interaction question rendering starts only after that bounded drain. A request that had already begun before acquisition may finish later, but no replacement request may begin and the drain remains bounded by the existing runtime limit.
- Releasing a stale lease is a no-op.
- The runtime supports one current interaction; implementation may use a single generation token rather than a general refcount.
- Resume requires current session identity, current active agent/turn state, current target authority, and no replacement lease.
- `agent_end`, abort, and session shutdown clear the lease without restarting typing.
- Telegram-visible status/diagnostics may report `waiting for answer`; Pi's terminal `telegram` status key remains cleared.

### 8.5 Consumer adaptation and legacy fail-safe

The `ask_user_question` consumer keeps its current normalized request/result types and existing TUI/RPC paths.

Execution order:

1. Normalize and validate current questions exactly as today; return the existing preflight cancellation if `signal.aborted`.
2. Enter the existing `withUILock`, write the existing pending checkpoint, and wrap every following Telegram and TUI/RPC branch in one `try/finally` that executes the existing checkpoint clear/refresh logic for every terminal result, throw, or abort.
3. Inside that guarded region, resolve the current user turn by scanning `ctx.sessionManager.getBranch()` backward to the latest `role === "user"` message and its first text block. Treat only `/^\[telegram(?:\||\])/` as the legacy Telegram fail-safe marker.
4. Lazily `import("@ststgc/pi-telegram/interactions")`; cache only a module where `typeof requestTelegramInteraction === "function"`, never a missing/failed import. There is no top-level static import, so the old package cannot prevent extension load.
5. Attempt the Telegram interaction API one question at a time.
6. If the first attempt returns `handled: false` and the current user turn is not Telegram-originated, use the existing TUI/RPC implementation inside the same checkpoint guard.
7. If the import/capability is unavailable or the first attempt returns `handled: false` while the current turn has the Telegram prefix, return the existing `unavailable` result immediately. Never open TUI for that turn.
8. Once any question is `handled: true`, do not switch the questionnaire to TUI/RPC. If a later question returns `handled: false` because the runtime/session disappeared, normalize it to the existing questionnaire `unavailable` terminal result and exit through the same checkpoint cleanup.
9. Map `answered/cancelled/timed-out/unavailable` exactly as frozen in §8.1 and preserve `AbortSignal`, maximum answer bytes, Other semantics, sequential UI lock, pending checkpoint cleanup, and result rendering.

This optional dynamic-import contract is selected; the rollout worker must not choose a static import or create a partial global registry.

## 9. Implementation slices

Unless a command is explicitly labeled as the intended S0 RED, every validation command in S1-S7 has expected exit `0`. Any other non-zero exit is a failure requiring diagnosis and in-scope repair before continuation. S6 `npm run validate` exit `0` is the final source gate.

### S0 — Correct-seam harness and frozen interaction contract

**Outcome:** Establish deterministic failing characterization and contract tests before production behavior.

**Dependencies:** None.

**Risk:** Low; local contract scaffold and tests only.

**Likely files:**

- `api/interactions.ts` and `lib/interactions.ts` minimal compiling contract scaffold (new; no successful Telegram behavior yet; S0 tests import the relative source membrane, not the absent package subpath)
- `tests/interactions.test.ts` (new)
- `tests/integration.test.ts`
- test helpers colocated with the owning suite unless genuinely reused
- this plan as read-only execution evidence

**Work:**

- Keep baseline typecheck and existing focused regressions GREEN first.
- Add the smallest compiling §8.1 types/runtime seam that returns no successful Telegram interaction.
- Add a bounded desired-behavior test named `authorized answer resolves claimed interaction before fixture deadline`. It calls the relative production seam, injects an authorized answer, and expects `handled: true / answered` before a deterministic fixture deadline. The minimal scaffold must fail this assertion promptly (for example by returning `handled: false`), never by leaving an unbounded Promise. This is test-first scaffolding, not a fake test-local implementation.
- Freeze the exact §8.1 request/result limits, one-current-claim behavior, `interact:` callback grammar, exact target identity, timeout/abort race semantics, and `handled` fallback semantics as intended RED tests.
- Add explicit classic and follower-owned fixture seams; do not use a shallow helper-only test as the only regression.
- Record the current `ask-user-question.ts` hash and the exact consumer behavior seam in the implementation notes; do not edit the installed file.

**Acceptance:** Intended RED fails because interaction behavior is absent, not because imports, fixtures, or infrastructure are broken. Existing focused tests remain green.

**Validation and expected exits:**

```bash
# Baseline remains green before the new behavioral probe.
npm run typecheck                                      # expected exit 0
node --experimental-strip-types --test tests/integration.test.ts  # expected exit 0

# Run only the intended desired-behavior assertion.
node --experimental-strip-types --test --test-name-pattern='authorized answer resolves claimed interaction before fixture deadline' tests/interactions.test.ts  # expected non-zero assertion because scaffold cannot answer
```

Record the failing assertion and confirm there is no import/syntax/fixture failure. Do not leave a committed branch with a globally red suite: S1 must make this test GREEN before the next source checkpoint.

**Continuation:** After intended RED is confirmed, proceed directly to S1.

### S1 — Public interaction runtime and classic single-question execution

**Outcome:** A classic-mode active Telegram turn can claim, render, answer, cancel, and time out one text/single/multi interaction through a generation-bound runtime.

**Dependencies:** S0.

**Risk:** Medium; new public contract and blocking Promise lifecycle, but local and reversible.

**Likely files:**

- `api/interactions.ts` (new)
- `lib/interactions.ts` (new)
- `lib/delivery.ts` only through narrow reused ports; no raw client export
- `lib/keyboard.ts`
- `index.ts` composition wiring
- `package.json`
- `tests/interactions.test.ts`, `tests/public-api.test.ts`, `tests/invariants.test.ts`, `tests/index.test.ts`

**Work:**

- Implement validation, runtime binding, claim/state machine, one-use tokens, selection state, timeout/abort, structured result, and bounded message cleanup.
- Reuse Delivery target authorization, rendering, logical handles, and generation checks. Add only the narrow private delivery/force-reply capability proven necessary by the text-answer UX; do not widen general Bot API access.
- Implement the frozen `interact:` namespace and enforce its 40-byte plan bound plus Telegram's 64-byte hard limit.
- Return `handled: false` only before claim; convert all post-claim failures into handled terminal results and diagnostics.

**Acceptance:** Classic fixtures complete each mode; duplicate claim, replay, timeout-vs-answer, abort-vs-answer, invalid request, stale generation, commit-unknown, runtime unbind, and session shutdown settle once with no leaked timer/listener/token.

**Validation:**

```bash
node --experimental-strip-types --test tests/interactions.test.ts tests/delivery.test.ts tests/public-api.test.ts tests/invariants.test.ts tests/index.test.ts
npm run typecheck
npm run domain-dag
```

**Continuation:** Proceed to S2 after focused GREEN.

### S2 — Owner-aware update routing and follower roundtrip

**Outcome:** Callback/text answers reach the exact interaction owner in classic, leader, and follower modes without being queued or resolved cross-thread.

**Dependencies:** S1.

**Risk:** Medium; touches high-regression routing and authenticated bus boundaries.

**Likely files:**

- `lib/interactions.ts`
- `lib/routing.ts`
- `lib/updates.ts`
- `lib/delivery.ts`, `lib/ownership.ts` for private interaction-purpose recording only
- `lib/bus.ts`, `lib/bus-api.ts`, `lib/bus-leader.ts`, `lib/bus-follower.ts` only for validated/stripped private purpose metadata and existing forwarding evidence
- `index.ts` ports only
- `tests/updates.test.ts`, `tests/routing.test.ts`, `tests/bus*.test.ts`, `tests/integration.test.ts`

**Work:**

- Add the internal interaction-priority path after pairing/sender authorization and before public update handlers. Public handlers must never observe or consume interaction callbacks or reply-anchored text answers.
- Extend private Delivery/bus message-ownership recording with `purpose: "interaction"`, stripped before Bot API transport, so leader-side priority classification can forward text replies to the exact owner without holding follower Promise state.
- Preserve exact paired owner, target, profile, session generation, message ownership purpose, and follower registration generation.
- Give bridge commands precedence over text capture; accept only exact reply-anchored, post-start, bounded non-empty text.
- Acknowledge callbacks promptly; isolate edit/next-question work from polling latency.
- Ensure consumed interaction answers do not enter normal prompt queue, generated-button handling, section fallback, or `[callback]` forwarding.

**Acceptance:** Two simultaneous follower fixtures cannot answer each other's interaction. A consuming public handler registered in leader/classic and follower processes cannot observe or consume valid callback/text answers. Stale, replayed, denied, cross-profile, and wrong-generation interaction candidates also do not reach public handlers. Wrong user/thread, old message, unanchored text, media, edit, and ordinary non-interaction replies are denied or pass normally as specified. `/abort` and `/stop` settle/cancel rather than becoming answers. The owning follower receives and resolves valid answers.

**Validation:**

```bash
node --experimental-strip-types --test tests/interactions.test.ts tests/updates.test.ts tests/routing.test.ts tests/bus.test.ts tests/bus-api.test.ts tests/bus-leader.test.ts tests/bus-follower.test.ts tests/integration.test.ts
npm run typecheck
npm run domain-dag
```

**Continuation:** Proceed to S3 after focused GREEN.

### S3 — Waiting lease, lifecycle cleanup, and observability

**Outcome:** Telegram displays a question instead of persistent Thinking, and every lifecycle boundary clears the wait safely.

**Dependencies:** S1-S2.

**Risk:** Medium; lifecycle/typing is regression-prone but remains local and reversible.

**Likely files:**

- `lib/runtime.ts`
- `lib/lifecycle.ts`
- `lib/interactions.ts`
- `lib/bindings.ts`
- `lib/status.ts` only for metadata-only diagnostics/status projection
- `tests/runtime.test.ts`, `tests/lifecycle.test.ts`, `tests/bindings.test.ts`, `tests/status.test.ts`, `tests/integration.test.ts`

**Work:**

- Add the generation-bound waiting lease and suppress all normal typing start/re-arm paths while held.
- Define answer-time resume from current runtime evidence; prevent stale lease release from restarting a replacement session.
- Wire `invalidateAuthority(capturedStamp)` at the exact transition table below. Every row orders interaction invalidation before Delivery/runtime rebind or teardown. If the named baseline symbol has moved or no equivalent boundary exists on the selected implementation base, stop S3 with evidence rather than omitting that transition.

| Transition | Baseline owner/call site | Required order |
| --- | --- | --- |
| Session shutdown/replacement | `lib/lifecycle.ts:createTelegramBridgeSessionLifecycleAssembly` service shutdown, composed by `index.ts:sessionLifecycleRuntime.onSessionShutdown` | interaction settle/unbind -> Delivery shutdown -> remaining session teardown |
| Session start | same lifecycle assembly service start | Delivery bind -> interaction runtime bind with fresh session/authority stamp -> polling/capability/watchdog start |
| Profile/setup/connect/disconnect transport replacement | `lib/bindings.ts:createTelegramCommandBindings` `onTransportChanged` calls, wired in `index.ts` | interaction authority invalidation -> `deliveryLifecycleRuntime.onSessionStart` rebind -> future claims use new stamp |
| Passive direct ownership loss | `lib/locks.ts:createTelegramLockedPollingRuntime.stopAfterOwnershipLoss` before `stopPolling` | interaction authority invalidation -> polling/bus stop -> Delivery transport becomes inactive |
| Follower deregistration/re-registration/target or generation replacement | central `lib/bus-follower.ts:createTelegramBusFollowerRegistrationState.setRegistered` | add a narrow change subscription; invalidate old interaction before publishing changed registered/target/generation state |
| Promotion/election | existing follower `setRegistered(false)` plus direct lock acquisition paths | follower-state invalidation first; any subsequent direct-owner claim captures a fresh leader epoch/stamp |
- Settle pending interactions before Delivery shutdown invalidates handles; remove keyboard/edit status while the generation is live when possible, but never await cleanup indefinitely before Promise settlement.
- Record metadata-only interaction phases and failures in existing runtime diagnostics.

**Acceptance:** No old-generation typing request begins after lease acquisition, and question rendering follows the bounded prior-send drain. A same-tick `typing.start -> acquireWaitingLease -> microtask flush` regression proves the pre-send fence. Message start/update cannot re-arm typing. Valid answer resumes only the same live activity. Abort, timeout, agent end, interaction-runtime unbind, session shutdown/reload, direct-owner takeover, follower re-registration, and other authority loss clear the lease and Promise; stale release cannot affect replacement runtime.

**Validation:**

```bash
node --experimental-strip-types --test tests/runtime.test.ts tests/lifecycle.test.ts tests/bindings.test.ts tests/status.test.ts tests/interactions.test.ts tests/integration.test.ts
npm run typecheck
```

**Continuation:** Proceed to S4 after focused GREEN.

### S4 — Consumer adapter and old-runtime fail-safe

**Outcome:** Produce and independently review a disposable consumer patch/canary proving that `ask_user_question` can use Telegram interactions when available and fails closed before TUI for a current Telegram-prefixed turn on an old runtime. The installed file remains unchanged until S7.

**Dependencies:** S1-S3 source API accepted. Live consumer activation remains a separate Non-Git rollout action.

**Risk:** Medium; installed non-Git extension, reversible by backup.

**Likely resource:**

- `/Users/ststgc/.pi/agent/extensions/ask-user-question.ts`
- private timestamped backup outside source repo
- optional dedicated fixture copied into a temporary test directory; do not add dead consumer-specific code to pi-telegram core

**Work:**

- Verify the current installed-file hash, copy it into a private non-extension-discovery temporary directory with mode preserved, and apply the reviewed adapter only to that copy.
- Add the exact §8.5 lazy-import, latest-user-turn detection, question-by-question mapping, later-`handled:false` normalization, and old-runtime fail-safe while preserving existing TUI/RPC/checkpoint/result behavior.
- Build a private OS temporary package graph outside every extension-discovery directory: `node_modules/@ststgc/pi-telegram/package.json` exports `./interactions` to a stub module whose `requestTelegramInteraction` reads a wrapper-controlled outcome queue. A temporary Pi wrapper extension imports the copied consumer through Pi's real loader, passes a mock `ExtensionAPI` to capture the tool definition, invokes `execute` across isolated scenarios, and fails its async factory on any assertion. Review verifies that the shipping patch differs from the exercised copy only at the temporary package/injection environment—never at consumer source lines.
- Snapshot the real pending-question legacy path and `.d` directory metadata before the canary. Run every scenario with both `PI_CODING_AGENT_DIR=<private-temp>/agent` and `PI_ASK_USER_QUESTION_PENDING_PATH=<private-temp>/state/pending.json`; no test may resolve to the real agent state. Verify the real pending path/tree is byte-for-byte and metadata unchanged afterward.
- Keep this harness outside the shipping package unless a general reusable consumer-contract fixture earns a repository test location.
- Independently review the patch diff against the installed source and frozen API before any live application.

**Acceptance:** The disposable harness proves: Telegram-prefixed missing runtime returns unavailable before any `ctx.ui` call; first `handled:false` non-Telegram input uses existing TUI/RPC; new runtime maps every answer/result; runtime disappearance between questions becomes unavailable without surface switch; abort and all terminal outcomes clear pending checkpoints. The patched copy loads through Pi's real extension loader. Installed files remain unchanged.

**Validation and expected exits:**

```bash
# The wrapper's async factory executes all adapter assertions through Pi's loader.
PI_CODING_AGENT_DIR=<private-temp>/agent PI_ASK_USER_QUESTION_PENDING_PATH=<private-temp>/state/pending.json \
  pi --no-extensions --list-models -e <private-temp>/adapter-harness.ts >/dev/null  # exit 0; no model call

# Load the patched consumer itself in isolation as a second canary.
PI_CODING_AGENT_DIR=<private-temp>/agent PI_ASK_USER_QUESTION_PENDING_PATH=<private-temp>/state/pending.json \
  pi --no-extensions --list-models -e <private-temp>/ask-user-question.ts >/dev/null  # exit 0; no model call
```

The temporary package graph supplies the controlled interaction export and is removed after evidence is saved. Do not replace this with a paid/model-driven canary. Record exact commands and exits. No `/reload` or live Telegram write occurs in S4.

**Continuation:** Return to the source repo for S5; do not activate the live package yet.

### S5 — Public documentation, architecture invariants, and migration contract

**Outcome:** The source contract and rollout prerequisites are discoverable and consistent.

**Dependencies:** S1-S4 design stable.

**Risk:** Low.

**Likely files:**

- `README.md`
- `AGENTS.md`
- `docs/README.md`, `docs/architecture.md`, `docs/public-api.md`
- `docs/interactions.md` (new)
- `docs/updates.md`, `docs/activity.md`, `docs/delivery.md`, `docs/callback-namespaces.md`, `docs/ui-style.md` as required
- `CHANGELOG.md` current release section
- `BACKLOG.md` only if a real release-relevant item remains open
- `package.json`, package-smoke/public-export tests

**Work:**

- Document the interaction API, ownership/routing order, timeout/abort, waiting status, text-reply rule, follower behavior, diagnostics privacy, and local fallback.
- Update `docs/updates.md` ordering: pairing/sender authorization remains first, internal interaction-priority owner forwarding/settlement precedes the public registry, and public handlers receive only non-interaction updates. Distinguish same-process raw interception from core owner forwarding and document the intentional privacy exception to prior pre-routing visibility.
- Reserve the interaction callback prefix and update the emoji registry only if implementation introduces a new visible emoji.
- Record the durable runtime rule in `AGENTS.md` without naming `ask_user_question` as a generic core domain dependency. A consumer-specific example may name it explicitly as a case study.
- Document canonical package migration from the observed old runtime without modifying installed config.

**Acceptance:** Public docs match exports and implementation; no claim that arbitrary `ctx.ui` is mirrored; no credentials or operator ids/labels leak; BACKLOG/CHANGELOG remain truthful.

**Validation:**

```bash
npm run typecheck
npm run pack:smoke
npm run pack:check
npm run domain-dag
```

**Continuation:** Proceed to S6.

### S6 — Full source validation and independent review

**Outcome:** Source implementation is ready for optional rollout.

**Dependencies:** S0-S5.

**Risk:** Low external risk; high validation breadth.

**Likely files:** No new behavior unless review finds in-scope corrections.

**Work:**

- Run focused suites once more, then full repository validation.
- Run a fresh independent review covering correctness/races, public API/security/privacy, follower routing, lifecycle/typing, test quality, docs, and plan conformance.
- Apply evidence-backed corrections with one writer, rerun affected focused checks and full validation.
- Inspect final diff and AGENTS compliance; ensure no planning/subagent artifacts are committed.

**Acceptance:** No blocking or correction-worth-doing finding remains; the pi-telegram source API and the disposable consumer patch are independently reviewable and every non-live criterion is proven by repository tests or the S4 disposable harness. The report says `source and consumer patch ready; live rollout pending`, not that the installed environment is fixed.

**Validation:**

```bash
npm run typecheck
npm test
npm run audit
npm run domain-dag
npm run pack:check
npm run pack:smoke
npm run validate
```

**Continuation:** Source work stops cleanly. S7 requires separate exact rollout approval.

### S7 — Installed-package cutover and live classic/Threaded smoke

**Outcome:** The user's live Pi environment runs the accepted canonical package and consumer adapter, and the original Telegram reproduction passes.

**Dependencies:** S6 clean, source change available from an approved local path/commit, consumer backup prepared, exact operator approval for package/config changes and benign Telegram writes.

**Risk:** High authority boundary because it mutates the live agent environment and sends real Telegram messages; not authorized by this planning request alone.

**Resources/effects:**

- active Pi package configuration and lock state
- `/Users/ststgc/.pi/agent/extensions/ask-user-question.ts`
- one approved Telegram profile and exact classic/thread targets
- benign test questions/options/answers only

**Work after approval:**

1. Record current package/config/consumer hashes and active version.
2. Install or point Pi at the accepted `@ststgc/pi-telegram` source using the repository-documented GitHub package path; remove/disable the old `@llblab` package only through Pi's supported package mechanism.
3. Apply the reviewed consumer adapter, reload in a controlled session, and verify extension loading before connecting Telegram.
4. Run classic tests: single choice, text reply, multi-select, sequential questionnaire, cancel, timeout (short test override), `/abort`, and local TUI fallback.
5. Run Threaded Mode with leader plus one follower: valid callback/text roundtrip, wrong-thread denial, follower generation replacement/reload, stale callback expiry, and no cross-instance resolution.
6. Inspect `/telegram-status` metadata diagnostics and verify question/answer bodies are absent.

**Acceptance:** The original screenshot scenario is no longer reproducible; Telegram shows answer controls, typing pauses, the same tool continues after answer, and all cancellation/reload boundaries recover. Local TUI remains usable.

**Rollback:** Disconnect controlled smoke sessions; restore the consumer backup; restore the prior supported package coordinate/lock state; reload; confirm old local behavior and record that Telegram interaction support is withdrawn. Do not replay commit-unknown Bot API mutations.

## 10. Cross-slice validation matrix

| Behavior | Primary proof |
| --- | --- |
| No active Telegram turn -> local fallback | `tests/interactions.test.ts` plus consumer disposable-load test |
| Post-claim failure never falls back to TUI | interactions + consumer adapter tests |
| Single/multi/text/Other/questionnaire | interactions unit tests; consumer sequential composition test; live smoke later |
| Same Promise resolves, no queued prompt | `tests/integration.test.ts` queue/active-turn assertion |
| `/abort` and `/stop` precedence | routing/integration tests |
| Timeout/abort/replay race settles once | deterministic timers in interactions/runtime tests |
| Typing suppressed across message activity | runtime/lifecycle tests |
| Typing resumes only for current activity | lifecycle/session-generation tests |
| Classic/leader/follower target ownership | updates/routing/bus/integration tests |
| Wrong user/target/profile/generation denied | negative authorization matrices |
| Callback 64-byte/opaque/no values | keyboard/interactions tests |
| No body leakage in diagnostics | status/log projection assertions |
| Shutdown before delivery invalidation | lifecycle ordering test |
| Package export and no `/lib` dependency | public API/package smoke/invariant tests |
| Old `@llblab` runtime fail-safe and mid-question disappearance | S4 disposable mock-execution and real-loader canary |
| Original mobile behavior | separately approved live smoke |

## 11. Risks, rollback, and privacy/security notes

### 11.1 Principal risks

- **Cross-process misrouting/public interception:** leader-local raw update handlers cannot resolve follower-local Promises and currently run before default owner routing. Mitigation: authenticated internal interaction-priority classification before public handlers, interaction-purpose message ownership, exact registration generation, follower-local settlement, and regressions with consuming public handlers.
- **False text capture:** an ordinary prompt could be swallowed as an answer. Mitigation: exact target/owner/generation, post-start id, reply anchoring, command precedence, bounded non-empty text only.
- **Indefinite wait:** network loss or abandoned mobile UI could hold Pi forever. Mitigation: bounded default timeout, abort/shutdown settlement, metadata status.
- **Typing resurrection:** existing message hooks can re-arm native activity. Mitigation: owner-level waiting lease checked by every start path.
- **Split-surface fallback:** Telegram render failure could open local TUI unexpectedly. Mitigation: irreversible `handled: true` after claim.
- **Stale UI:** reload invalidates Delivery handles before keyboard cleanup. Mitigation: interaction shutdown precedes Delivery invalidation; stale token remains one-use and receives expiry handling when possible.
- **Consumer load break:** statically importing a package absent from the old runtime can prevent extension load. Mitigation: coordinate rollout or use a validated optional versioned capability path plus Telegram-prefix fail-safe.
- **Sensitive diagnostics:** questions and answers may contain private information. Mitigation: never record bodies, labels, ids, raw callbacks, or transport payloads.

### 11.2 Source rollback

- Revert the feature branch/commit as one coherent source change; public `./interactions` is additive and has no persisted state migration.
- Remove the runtime binding/export/docs and return routing/typing to the prior tested behavior.
- Any pending interaction is session-memory-only and must settle unavailable during shutdown; there is no on-disk interaction schema to migrate.

### 11.3 Consumer/rollout rollback

- Restore the exact mode-preserving consumer backup and prior supported package coordinate.
- Reload only after verifying files and package resolution.
- Do not delete Telegram messages or durable recovery state as rollback cleanup.

## 12. Documentation and continuity updates

Implementation must update:

- `README.md` feature/platform/safety descriptions.
- `docs/interactions.md` as the authoritative public contract.
- `docs/public-api.md`, `docs/architecture.md`, `docs/updates.md`, `docs/delivery.md`, `docs/callback-namespaces.md`, and docs index as affected.
- `AGENTS.md` with the durable interaction ownership/waiting invariant and domain map.
- Current `CHANGELOG.md` release section with outcome-focused interaction, routing, lifecycle, and consumer-impact bullets.
- `BACKLOG.md` only for work intentionally left open, such as unperformed live platform smoke; remove it when completed rather than recording completed chronology.

No ADR is required unless implementation evidence forces a different public ownership model than the explicit interaction domain defined here.

## 13. Blockers, assumptions, and recommended defaults

### Assumptions/defaults

- Default timeout: exactly 900,000 ms; accepted range: 1,000..3,600,000 ms.
- Free-text answers require a reply to the question message. This is preferred over consuming the next arbitrary thread message. ForceReply may be added only as a narrow rendering aid, not as broader transport access.
- One interaction per active turn; questionnaires are sequential consumer composition.
- Source features target the current canonical `origin/dev` after a pre-write delta check.
- Public API is additive under `@ststgc/pi-telegram/interactions`.
- Live package cutover and Telegram smoke require separate approval and may be skipped without misreporting source readiness as operational rollout completion.

### Current blockers

None for source planning or implementation. S7 is an explicit later authority boundary, not a blocker for S0-S6.

## 14. Whole-plan execution contract

The completion target is the entire non-blocked plan, not only S0.

- A downstream executor completes S0-S3 and S5-S6 in order in the primary Git Lane, validating and repairing each slice before continuing.
- S4 produces a reviewed disposable consumer patch and mock/loader evidence without changing the installed file. S7 applies that exact reviewed semantic patch and performs live activation as a separate Non-Git/operator rollout action. The source executor must not silently edit or reload the installed environment contrary to repository policy.
- One writer owns each active lane. Do not run concurrent writers against overlapping source or installed-extension files.
- After each slice passes, continue to the next unblocked slice without asking for routine local approval.
- Stop only for a changed authoritative source that invalidates the plan, a dirty/user-owned workspace conflict, an unapproved public/security architecture decision, a required external effect outside current authority, three materially different failed root-cause fixes, or validation evidence that the selected design is wrong.
- Every stop report includes the exact command/error, files/symbols involved, current diff, preserved rollback state, and smallest decision required.
- Fresh independent review—not the slice writer—accepts follower routing, waiting lifecycle, public API/security/privacy, and final whole-plan conformance.
- Completion evidence includes base/head SHAs, changed files, focused/full validation with exits, independent review disposition, skipped live checks, consumer patch/backup evidence when authorized, and residual risks.
- Do not claim the user-visible issue fully deployed until S7 passes. If only S0-S6 complete, report `source ready; live rollout pending`.

## 15. Reviewer synthesis

### Round 1/3

Three fresh read-only reviewers accepted the dedicated interaction-domain direction, explicit non-goals, follower-owner routing requirement, waiting-lease rationale, privacy boundary, and source/live-rollout separation. They found no user decision or product blocker.

Corrections applied:

- Froze the complete public option/answer/result types, input/answer/timeout limits, result mapping, and `handled` semantics.
- Selected `interact:` with an exact opaque token grammar and reserved-prefix update requirement.
- Defined consumer cancellation as the existing tool `AbortSignal`; runtime/session/authority invalidation remains pi-telegram-owned.
- Grounded owner-local dispatch in `executeTelegramUpdatePlan` after foreign-owner forwarding and before local fallback, with existing full-update bus envelopes as the default.
- Added explicit direct-owner/follower-registration authority invalidation before Delivery rebinding.
- Fixed later-question `handled: false` behavior and selected lazy dynamic import plus latest-user-turn prefix detection for old-runtime compatibility.
- Added the package export invariant suite, separated S0 GREEN and intended RED exits, and changed S4 into a disposable patch/mock/real-loader canary so S6 no longer claims the installed environment is fixed.

Deferred by evidence:

- ForceReply is optional because exact reply anchoring is the required safety contract.
- Live classic/Threaded client behavior remains the separately approved S7 gate.
- Bus protocol expansion is not planned unless S0/S2 proves the current authenticated full-update envelope insufficient.

Round 2 reviewed the materially revised API, lifecycle, routing, test, and rollout contracts.

### Round 2/3

Three fresh read-only reviewers again found no user decision or product blocker and accepted the interaction-domain architecture, exact owner/follower authority, rollout separation, and non-goals. Material corrections applied:

- Resolved command-precedence ambiguity: callback interaction dispatch stays before local callback fallback; message capture passes every slash-prefixed input to existing command routing.
- Mapped claim contention to existing `handled: true / unavailable`, froze one-based public option indexes and zero-based base36 callback indexes, and removed the unnecessary version-symbol assumption.
- Added whitespace normalization and corrected privacy language so machine option values never reach Telegram.
- Made the multi-chunk final message id the sole free-text/Other reply anchor and refresh it after every reconciliation.
- Made waiting-lease acquisition async, fenced queued typing microtasks before Bot API start, and required bounded drain before question rendering.
- Replaced the contradictory S0 RED with a bounded desired-behavior assertion through the relative production scaffold.
- Defined a concrete temporary package graph plus Pi-loader wrapper for deterministic consumer adapter injection without editing the live file or invoking a model.
- Added explicit final-plan transfer from the untracked detached planning worktree into the implementation worktree with matching hashes.

Still deferred by evidence: ForceReply, live S7 client behavior, and bus envelope expansion absent a failing S2 proof.

Round 3 was the final fresh review of this contract.

### Round 3/3 and parent final-correction audit

The final three reviewers found no user/product decision and accepted the stable API, source/domain boundaries, owner/follower generation model, typing fence, rollback, and live authority gate. They identified corrections worth doing now, applied in this final writer pass:

- Isolated S4 with `PI_CODING_AGENT_DIR` and `PI_ASK_USER_QUESTION_PENDING_PATH` in a private OS temp tree and required proof that real pending checkpoint state is unchanged.
- Added the exact authority-transition table for session lifecycle, transport changes, passive lock loss, follower registration replacement, and promotion; missing equivalent symbols are an S3 stop condition.
- Declared every GREEN command's expected exit `0` and `npm run validate` as the final source gate.
- Froze and finally reordered checkpoint write before latest-turn detection/import/claim/render, with one `try/finally` cleanup across Telegram and TUI/RPC branches.
- Corrected a material pre-routing privacy flaw: current public handlers run before default owner routing, so the final plan now requires an authenticated internal interaction-priority path before public registry dispatch, private `purpose: "interaction"` message ownership stripped before Bot API transport, exact follower forwarding, and consuming-handler regressions.

The pre-public priority-path correction is material and was applied after the last allowed fresh review round. Per the fixed cap, it was not sent to a fourth reviewer; the parent final audit checked it against `lib/updates.ts:createTelegramUpdateHandle`, existing ownership forwarding, bus registration fencing, Delivery metadata stripping, privacy rules, and the plan rubric. No corrections remain from that audit.

Final stop reason: review cap reached with all evidence-backed corrections applied; remaining items are explicitly deferred rollout evidence, not plan defects.
