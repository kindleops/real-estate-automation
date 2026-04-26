/**
 * Discord SMS Reply Components & Buttons
 * Action buttons and component builders for inbound SMS reply feature
 */

import { clean } from "@/lib/utils/strings.js";

// Button styles (from Discord API)
const STYLE = {
  PRIMARY: 1,
  SECONDARY: 2,
  SUCCESS: 3,
  DANGER: 4,
};

// Local button and actionRow helpers
function button({ label, custom_id, style = STYLE.PRIMARY, disabled = false }) {
  return {
    type: 2, // BUTTON
    style,
    label: String(label).slice(0, 80),
    custom_id: String(custom_id).slice(0, 100),
    disabled: Boolean(disabled),
  };
}

function actionRow(buttons) {
  return { type: 1, components: buttons.slice(0, 5) };
}

/**
 * Build action buttons for inbound SMS reply card
 * Includes: Send Suggested Reply, Edit Reply, Manual Reply, Mark Hot, Suppress
 *
 * Encoded in custom_id as: sms_action:{action_type}:{message_event_id}
 * (message_event_id limited to ~60 chars to fit in 100 max)
 */
export function buildSmsReplyActionButtons({
  message_event_id = "",
  suggested_reply = "",
  from_phone_number = "",
} = {}) {
  if (!message_event_id) {
    return []; // No buttons if no event ID
  }

  const safe_event_id = String(message_event_id).slice(0, 35);
  const has_suggestion = clean(suggested_reply).length > 0;

  const buttons = [];

  if (has_suggestion) {
    buttons.push(
      button({
        label: "✅ Send Suggested",
        custom_id: `sms_action:send_suggested:${safe_event_id}`,
        style: STYLE.SUCCESS,
      })
    );
  }

  buttons.push(
    button({
      label: has_suggestion ? "✏️ Edit" : "✍️ Manual Reply",
      custom_id: `sms_action:open_modal:${safe_event_id}`,
      style: STYLE.PRIMARY,
    })
  );

  buttons.push(
    button({
      label: "🔥 Hot Lead",
      custom_id: `sms_action:mark_hot:${safe_event_id}`,
      style: STYLE.SECONDARY,
    })
  );

  buttons.push(
    button({
      label: "🚫 Suppress",
      custom_id: `sms_action:suppress:${safe_event_id}`,
      style: STYLE.DANGER,
    })
  );

  return [actionRow(buttons)];
}

/**
 * Build context buttons for inbound alert card
 * Includes: Open Podio, Open Context (if available)
 */
export function buildInboundContextButtons({
  podio_item_id = "",
  master_owner_id = "",
} = {}) {
  const buttons = [];

  if (clean(podio_item_id)) {
    buttons.push(
      button({
        label: "📋 Open Podio",
        custom_id: `context:open_podio:${podio_item_id}`,
        style: STYLE.SECONDARY,
      })
    );
  }

  if (clean(master_owner_id)) {
    buttons.push(
      button({
        label: "🔍 Context",
        custom_id: `context:load:${master_owner_id}`,
        style: STYLE.SECONDARY,
      })
    );
  }

  return buttons.length > 0 ? [actionRow(buttons)] : [];
}

/**
 * Combine reply actions + context buttons
 */
export function buildInboundSmsActionComponents({
  message_event_id = "",
  suggested_reply = "",
  from_phone_number = "",
  podio_item_id = "",
  master_owner_id = "",
} = {}) {
  const components = [];

  const reply_buttons = buildSmsReplyActionButtons({
    message_event_id,
    suggested_reply,
    from_phone_number,
  });

  const context_buttons = buildInboundContextButtons({
    podio_item_id,
    master_owner_id,
  });

  return [...reply_buttons, ...context_buttons];
}

/**
 * Build embedding payload highlighting suggested reply
 * Used when showing inbound alert with reply suggestion
 */
export function buildSuggestedReplyPreview(suggested_reply = "") {
  const trimmed = clean(suggested_reply).slice(0, 200);
  if (!trimmed) return null;

  return {
    name: "💬 Suggested Reply",
    value: `\`\`\`\n${trimmed}\n\`\`\``,
    inline: false,
  };
}
