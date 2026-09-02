# Updates

`updates` owns Telegram update classification, default-routing plans, and the public update-handler registry. The internal `polling` domain owns the actual `getUpdates` loop, offsets, and abort/controller state.

`pi-telegram` owns a single `getUpdates` long-poll connection per bot. Other pi extensions cannot open a competing polling connection against the same bot — the Telegram Bot API uses a per-bot `offset` cursor, and two loops race each other and lose updates.

This document describes the registry that lets layered pi extensions running in the owning Pi process react to paired inbound Telegram updates before ordinary built-in routing. Two private boundaries run first: first-contact proof plus exact paired-sender authorization, then active-turn interaction-priority routing. Neither pairing/proof traffic nor interaction answers reaches this registry. In Threaded Mode, the leader may forward an exact full update to its owning follower; that follower applies the same private priority boundary before its process-local public registry.

It is the runtime counterpart to [Callback Namespaces](./callback-namespaces.md): callback namespaces define how to share `callback_data` cleanly; update handlers define how to observe and optionally short-circuit the dispatch of those updates.

## When to use it

Use it when a layered extension needs to:

- Resolve extension-owned out-of-band state for a custom callback namespace, rather than waiting for the next agent turn. For bounded questions inside an active Telegram-originated tool flow, prefer the ownership-gated [Telegram Interactions API](./interactions.md).
- Suppress `pi-telegram`'s default routing for callbacks owned by the layered extension, so `pi-telegram` does not also forward them as `[callback] <data>` text.
- Observe arbitrary update types such as messages, edits, channel posts, or reactions without owning the polling connection.

If the layered extension only needs to read assistant-visible callbacks, the existing `[callback] <data>` fallback documented in [Callback Namespaces](./callback-namespaces.md) is enough.

If the extension needs a durable top-level Telegram menu section with managed rendering, callback routing, authorization, and diagnostics, use the higher-level [Telegram Extension Sections](./sections.md) contract instead of a raw update handler.

## Constraints

- One bot profile has one leader-owned `getUpdates` loop. This registry is process-local and does not itself create a multi-instance bus; Threaded Mode uses the bridge's authenticated leader/follower bus and exact target ownership.
- Handlers run in the polling loop. They must return quickly; long awaits delay subsequent updates.
- Handler errors are caught and logged silently so polling never breaks. If you need durable error reporting, do it inside your handler.
- The registry lives on `globalThis`. Module instance identity is not required, so layered extensions can reach it without importing `@ststgc/pi-telegram`.
- Pairing proofs are deliberately outside the public handler contract in every state. Bots, groups/channels, edits, callbacks, reactions, media, service messages, malformed claims, and all other unpaired updates are denied before handler dispatch. An exact `/start <code>` proof is always suppressed before handlers and default routing, including replay after a successful claim; while unpaired it is claimed atomically, and its generic reply/status refresh are best-effort side effects.
- After positive paired-sender authorization, private interaction candidates are also outside this contract. `interact:` callbacks and non-command text replies to a message whose private ownership purpose is `interaction` settle locally or are forwarded to the exact owner before public dispatch. Other paired updates preserve registration order and `consume` behavior.

## Verdicts

Each handler returns one of:

- `"consume"` — `pi-telegram` skips its default routing for this update.
- `"pass"` or `void` / `undefined` — `pi-telegram` routes the update normally. Other handlers registered after this one still run for the same update.

The first handler that returns `"consume"` wins; later handlers are not called for that update.

## Registering a handler

Two equivalent paths.

### Typed import (recommended when you can depend on `@ststgc/pi-telegram`)

```ts
import { registerTelegramUpdateHandler } from "@ststgc/pi-telegram/updates";

const off = registerTelegramUpdateHandler(async (update) => {
  const cb = (update as { callback_query?: { id?: string; data?: string } })
    .callback_query;
  if (!cb?.data?.startsWith("myext:")) return "pass";
  await resolveMyApproval(cb);
  return "consume";
});

// Later, when your extension shuts down:
off();
```

### Zero-coupling globalThis lookup

When the layered extension prefers no `import` from `@ststgc/pi-telegram`, so load order between the two extensions does not matter and either can be installed first, it must implement the **full v1 registry contract**, not just `version` and `add`. pi-telegram's polling runtime calls `dispatch` on whatever object it finds at `globalThis.__piTelegramUpdateHandlerRegistry__`, so a partial object would silently break the first update.

pi-telegram defensively re-creates the registry if the object on `globalThis` is missing `add` or `dispatch`, validated as `version === 1`, `typeof add === "function"`, and `typeof dispatch === "function"`. Handlers registered against a malformed object are dropped — make sure your bootstrap implements all three fields.

```ts
type PiTelegramVerdict =
  | "consume"
  | "pass"
  | void
  | Promise<"consume" | "pass" | void>;
type PiTelegramUpdateHandler = (update: unknown) => PiTelegramVerdict;

interface PiTelegramUpdateHandlerRegistry {
  readonly version: 1;
  add: (handler: PiTelegramUpdateHandler) => () => void;
  // Required: pi-telegram's polling loop calls this on every update.
  dispatch: (update: unknown) => Promise<"consume" | "pass">;
}

const REGISTRY_KEY = "__piTelegramUpdateHandlerRegistry__";

function getOrCreateRegistry(): PiTelegramUpdateHandlerRegistry {
  const g = globalThis as Record<string, unknown>;
  const existing = g[REGISTRY_KEY] as
    | PiTelegramUpdateHandlerRegistry
    | undefined;
  if (
    existing &&
    existing.version === 1 &&
    typeof existing.add === "function" &&
    typeof existing.dispatch === "function"
  ) {
    return existing;
  }
  const handlers = new Set<PiTelegramUpdateHandler>();
  const registry: PiTelegramUpdateHandlerRegistry = {
    version: 1,
    add(handler) {
      handlers.add(handler);
      return () => handlers.delete(handler);
    },
    async dispatch(update) {
      for (const handler of handlers) {
        try {
          const result = await handler(update);
          if (result === "consume") return "consume";
        } catch {
          // Never break polling because of a handler error.
        }
      }
      return "pass";
    },
  };
  g[REGISTRY_KEY] = registry;
  return registry;
}

const off = getOrCreateRegistry().add((update) => {
  /* … */
  return "pass";
});
```

The registry object on `globalThis.__piTelegramUpdateHandlerRegistry__` is versioned (`version: 1`) and stable across pi-telegram releases; future breaking changes will use a new schema version and a new key.

## Interaction with built-in routing

The exact inbound order is:

1. suppress/handle first-contact proof shapes and positively authorize the paired sender;
2. run the private interaction-priority classifier;
3. for a foreign interaction owner, forward the complete original update to the exact live follower `instanceId` and registration generation; the follower repeats step 2 before its local public handlers;
4. dispatch non-interaction updates through this process-local public registry; and
5. if no handler consumes the update, run ordinary commands, app/model/queue/settings menus, sections, generated buttons, edited-message handling, prompt routing, and callback fallback.

`/abort` and `/stop` retain ordinary command precedence while an interaction is pending. Other slash-prefixed messages are not captured as text answers. A stale, replayed, wrong-target, wrong-profile, wrong-generation, or otherwise denied interaction candidate is consumed privately with a bounded expiry/unavailable response; it never leaks to a public handler, `[callback]` fallback, or Pi prompt.

This means:

- Extensions can claim non-reserved callback namespaces that `pi-telegram` would otherwise forward as `[callback] <data>` text.
- Extensions can observe non-interaction updates by always returning `"pass"`.
- Extensions cannot observe or consume pairing proofs or interaction answers.
- Extensions must not consume updates that belong to `pi-telegram`'s own prefixes (`compact:`, `tgbtn:`, `menu:`, `model:`, `thinking:`, `status:`, `queue:`, `recovery:`, `settings:`, `section:`, `interact:`).

## Ownership semantics

The public registry itself is ownership-agnostic, but the bridge is not. Pairing, target ownership, profile/transport generation, session generation, and direct-owner epoch or follower registration generation are checked by private routing before an interaction candidate can reach any public handler. Ordinary non-interaction updates follow the established target-owner forwarding rules.

When a leader forwards a private interaction candidate, it sends the exact full Telegram update over the authenticated bus to the exact current follower registration. The leader does not settle the follower's Promise and does not maintain a second interaction registry. The follower applies its own current session/authority stamp, runs its priority classifier, and acknowledges through the existing bus path. Stale registration generations fail closed.

If the polling runtime loses its exact `owners.json` slot and stops `getUpdates`, its process-local handlers stop receiving new leader-polled updates; registration objects are not thereby an ownership authority. A layered extension that needs lifecycle evidence should use the standard Pi hooks or the Activity API rather than infer authority from handler registration.

## Not a polling multiplexer

This registry never opens or shares another `getUpdates` loop. Classic mode has one polling owner. Threaded Mode has one leader polling owner and explicit operator-started follower Pi processes connected through the authenticated local bus; each process has its own public handler registry. The bus may route owned updates to a follower, but the registry itself cannot spawn instances, select arbitrary followers, bypass registration generations, or contact Telegram directly.

## Relationship to extension sections

Update handlers are the raw update primitive. Extension sections are the structured Telegram UI layer above that primitive.

Use update handlers for immediate update interception, custom callback namespaces, out-of-band Promise resolution, and update types that should not become a Telegram menu surface.

Use extension sections when the desired behavior is a menu-integrated UI: `render(ctx)`, managed callback dispatch, safe runtime ports, stale-callback handling, and diagnostics owned by `pi-telegram`.
