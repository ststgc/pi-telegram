/**
 * Public Telegram interaction API
 * Zones: package boundary, telegram interaction, extension interop
 * Exposes the frozen active-turn interaction contract while keeping runtime and routing internals package-private
 */

export {
  requestTelegramInteraction,
  type TelegramInteractionAnswer,
  type TelegramInteractionAttempt,
  type TelegramInteractionMode,
  type TelegramInteractionOption,
  type TelegramInteractionRequest,
  type TelegramInteractionResult,
} from "../lib/interactions.ts";
