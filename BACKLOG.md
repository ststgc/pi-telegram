# Project Backlog

_This backlog tracks only open release-relevant work: hotfixes, bounded maintenance, live runtime verification, evidence-gated Telegram client follow-ups, and upstream Pi API blockers. Completed outcomes and validation evidence belong in `CHANGELOG.md`, not in this queue._

## P0 — Expiring Pi Shrinkwrap Audit Exception

Deadline: 2026-08-21 UTC. The validation gate intentionally fails at `2026-08-22T00:00:00Z` if either exception remains.

Context: `@earendil-works/pi-coding-agent@0.80.6` publishes its own `npm-shrinkwrap.json`, which prevents this consumer package from replacing two installed vulnerable copies. The bounded exception covers only `brace-expansion@5.0.6` / sources `1123898` and `1124334` / `GHSA-3jxr-9vmj-r5cp` and `GHSA-mh99-v99m-4gvg`, and `protobufjs@7.6.4` / source `1123964` / `GHSA-j3f2-48v5-ccww`, plus parent findings whose complete audit graph resolves exclusively to those sources. The second brace-expansion advisory was added when npm began reporting it on 2026-07-29; it shares the existing package, installed path, and deadline rather than widening the package graph. `npm run audit` verifies the exact advisory set, graph, installed paths and versions, and expiry; missing, duplicate, unknown, or changed findings fail closed.

Open work:

- [ ] Upgrade to a Pi release whose published shrinkwrap installs versions outside every affected `brace-expansion` and `protobufjs` range, remove the exception policy, and restore a zero-finding raw `npm audit` before the deadline.

Done when: a clean `npm ci` followed by raw `npm audit` reports zero vulnerabilities and the expiring policy/overrides are removed.

## P1 — Native Windows Runtime Smoke

Context: Deterministic ownership, persistence, recovery, process, and named-pipe coverage passes on native hosted Windows. A live Telegram client remains the only unverified platform boundary and may be exercised in a later release cycle rather than tied to a specific version.

Open work:

- [ ] Run a current build through native Windows classic and Threaded Mode smoke: connect, ownership handoff, leader/follower registration, stale recovery, live downgrade, diagnostics rotation, and shutdown cleanup. Record concrete named-pipe, atomic-file, and Telegram-client evidence.

Done when: native Windows live evidence confirms singleton and leader/follower authority, recovery, diagnostics, downgrade, and shutdown behavior.

## P1 — Durable Outbound Zero-Unit Pending Handoff

Context: An operator-approved Threaded interaction smoke exposed a separate durable-delivery defect. A plain leader-thread turn produced local assistant output but no Telegram final, while pending delivery increased from six to seven. The new outbound intent remained `pending` with zero units, zero receipts, zero automatic attempts, and no uncertain classification. A follower completion also recorded `Recovery outbound sources must be dispatching` followed by settlement without a durable outbound intent. Interaction authority still behaved correctly, but this is release-relevant message loss with accumulating nonterminal state.

Open work:

- [ ] Reproduce the empty-unit handoff deterministically across classic, leader, and follower turns, including plain responses and tool-followed responses. Identify whether semantic-message extraction, source-inbound state, or outbox planning creates the orphan.
- [ ] Make every completed semantic result either commit at least one deliverable unit, reach an explicit terminal no-output disposition, or surface an actionable retry/uncertain state. Never leave zero-unit `pending` work with no scheduled attempt.
- [ ] Add regressions for queue advancement, restart/reload, status counts, payload privacy, and safe operator handling of the already-retained records. Do not replay or delete existing pending work without explicit duplicate/data-loss review and operator confirmation.

Done when: a reproduced affected turn reaches a truthful delivered, terminal no-output, retryable, or uncertain disposition; no zero-unit unscheduled pending intent remains; subsequent turns still advance once; and the existing retained records have an explicit safe operator resolution path.

## Blocked — Same-Thread Telegram `/new`

Blocked: upstream Pi core API remains unavailable. Issue #5952 was auto-closed by intake policy rather than resolved: https://github.com/earendil-works/pi/issues/5952

Context: Threaded Mode manual followers are separate visible Pi processes. Same-thread `/new` is a different feature: replacing the current Pi session inside the same Telegram thread. Extension-only hacks are rejected because they would desynchronize Pi lifecycle/TUI semantics.

Current upstream evidence: Pi 0.83.0 still exposes `ctx.newSession()` only to registered extension commands through `ExtensionCommandContext`; Telegram update and callback handlers receive only `ExtensionContext`, and the 0.83 docs explicitly say session controls are command-only because event-handler invocation can deadlock. The shipped `pi.sendUserMessage()` implementation still calls host prompt handling with `expandPromptTemplates: false`, so extension-origin `/new` or a bridge command becomes an ordinary model prompt rather than acquiring a fresh command context. This also contradicts the 0.83 docs' `reload-runtime` follow-up example and cannot be treated as a supported command bridge. The upstream maintainer previously described an async extension bridge as potentially possible, but no usable API exists yet.

Required upstream shape:

- `pi.newSession(...)` or `pi.requestSessionReplacement(...)` callable from trusted extension runtime code.
- Must use the same session-replacement path as the terminal command, including normal `session_shutdown` / `session_start` lifecycle.

Constraints:

- Do not store stale `ExtensionCommandContext`.
- Do not inject TUI input.
- Do not spawn a shadow `pi` subprocess.
- Do not mutate session files directly.
- Do not route through `pi.exec`; it is shell execution, not a Pi slash-command dispatcher.

Done when: `/new` in the current Telegram thread performs an official same-instance session replacement, preserves the thread binding, rebinds after lifecycle restart, reports success/cancellation in the same thread, and has regressions for active turns, pending Pi messages, queue state, preview cleanup, cancellation, failure, and success.
