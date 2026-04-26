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
 * Uses compact deterministic sr:* button IDs.
 */
export function buildSmsReplyActionButtons({
  message_event_id = "",
  suggested_reply = "",
  review_mode = "full",
} = {}) {
  if (!message_event_id) {
    return []; // No buttons if no event ID
  }

  const safe_event_id = String(message_event_id).slice(0, 35);

  if (clean(review_mode) === "manual_only") {
    return [
      actionRow([
        button({
          label: "Manual Reply",
          custom_id: `sr:m:${safe_event_id}`,
          style: STYLE.PRIMARY,
        }),
        button({
          label: "Approve / Send Now",
          custom_id: `sr:a:${safe_event_id}`,
          style: STYLE.SUCCESS,
          disabled: !clean(suggested_reply),
        }),
        button({
          label: "Wrong Number",
          custom_id: `sr:wn:${safe_event_id}`,
          style: STYLE.DANGER,
        }),
        button({
          label: "Not Interested",
          custom_id: `sr:ni:${safe_event_id}`,
          style: STYLE.SECONDARY,
        }),
        button({
          label: "Opt Out",
          custom_id: `sr:oo:${safe_event_id}`,
          style: STYLE.DANGER,
        }),
      ]),
    ];
  }

  const primary_buttons = [
    button({
      label: "Approve / Send Now",
      custom_id: `sr:a:${safe_event_id}`,
      style: STYLE.SUCCESS,
      disabled: !clean(suggested_reply),
    }),
    button({
      label: "Manual Reply",
      custom_id: `sr:m:${safe_event_id}`,
      style: STYLE.PRIMARY,
    }),
    button({
      label: "Cancel Autopilot",
      custom_id: `sr:c:${safe_event_id}`,
      style: STYLE.SECONDARY,
    }),
    button({
      label: "Not Interested",
      custom_id: `sr:ni:${safe_event_id}`,
      style: STYLE.SECONDARY,
    }),
    button({
      label: "Wrong Number",
      custom_id: `sr:wn:${safe_event_id}`,
      style: STYLE.DANGER,
    }),
  ];

  return [
    actionRow(primary_buttons),
    actionRow([
      button({
        label: "Opt Out",
        custom_id: `sr:oo:${safe_event_id}`,
        style: STYLE.DANGER,
      }),
    ]),
  ];
}

/**
 * Build context buttons for inbound alert card
 * Includes: Open Podio, Open Context (if available)
 */
export function buildInboundContextButtons({
  message_event_id = "",
} = {}) {
  const buttons = [];

  if (clean(message_event_id)) {
    buttons.push(
      button({
        label: "Open Record",
        custom_id: `context:open_record:${String(message_event_id).slice(0, 35)}`,
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
  review_mode = "full",
} = {}) {
  const reply_buttons = buildSmsReplyActionButtons({
    message_event_id,
    suggested_reply,
    review_mode,
  });

  const context_buttons = buildInboundContextButtons({
    message_event_id,
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
