/**
 * discord-response-helpers.js
 *
 * Builders for Discord interaction response payloads (JSON objects, not
 * NextResponse instances — the route handler wraps them).
 *
 * Discord interaction response types used here:
 *   1  PONG                           – acknowledge a ping
 *   4  CHANNEL_MESSAGE_WITH_SOURCE    – immediate visible reply
 *   7  UPDATE_MESSAGE                 – edit the original message (components)
 *
 * Message flags:
 *   64  EPHEMERAL  – only the invoking user can see the message
 *
 * Component types:
 *   1  ACTION_ROW
 *   2  BUTTON
 *
 * Button styles:
 *   1  PRIMARY   (blurple)
 *   2  SECONDARY (grey)
 *   3  SUCCESS   (green)
 *   4  DANGER    (red)
 */

export const INTERACTION_TYPE = {
  PING:              1,
  APPLICATION_COMMAND: 2,
  MESSAGE_COMPONENT: 3,
};

export const INTERACTION_RESPONSE_TYPE = {
  PONG:            1,
  CHANNEL_MESSAGE: 4,
  UPDATE_MESSAGE:  7,
};

export const MESSAGE_FLAGS = {
  EPHEMERAL: 64,
};

// ---------------------------------------------------------------------------
// Base builders
// ---------------------------------------------------------------------------

/** Acknowledge Discord PING. */
export function pong() {
  return { type: INTERACTION_RESPONSE_TYPE.PONG };
}

/**
 * Send an ephemeral reply visible only to the invoking user.
 *
 * @param {string}  content
 * @param {object}  [extra]  - optional additions: embeds, components, allowed_mentions
 * @returns {object}
 */
export function ephemeralMessage(content, extra = {}) {
  return {
    type: INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE,
    data: {
      content: String(content ?? "").slice(0, 2000),
      flags:   MESSAGE_FLAGS.EPHEMERAL,
      ...extra,
    },
  };
}

/**
 * Send a public channel reply visible to everyone.
 *
 * @param {string}  content
 * @param {object}  [extra]
 * @returns {object}
 */
export function channelMessage(content, extra = {}) {
  return {
    type: INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE,
    data: {
      content: String(content ?? "").slice(0, 2000),
      ...extra,
    },
  };
}

/**
 * Update an existing message (used to respond to button clicks).
 * Replaces content and removes all components by default.
 *
 * @param {string}  content
 * @param {object}  [extra]
 * @returns {object}
 */
export function updateMessage(content, extra = {}) {
  return {
    type: INTERACTION_RESPONSE_TYPE.UPDATE_MESSAGE,
    data: {
      content:    String(content ?? "").slice(0, 2000),
      components: [],   // remove buttons unless overridden
      ...extra,
    },
  };
}

// ---------------------------------------------------------------------------
// Convenience wrappers
// ---------------------------------------------------------------------------

/** Standardised permission-denied ephemeral reply. */
export function deniedResponse(reason = "You do not have permission to run this command.") {
  return ephemeralMessage(`🚫 **Access denied**: ${reason}`);
}

/** Standardised error ephemeral reply.  Never exposes raw error details. */
export function errorResponse(message = "An unexpected error occurred.") {
  return ephemeralMessage(`❌ **Error**: ${message}`);
}

// ---------------------------------------------------------------------------
// Approval buttons
// ---------------------------------------------------------------------------

/**
 * Build an ACTION_ROW containing Approve and Reject buttons.
 *
 * @param {string} approve_id  - custom_id for the approve button
 * @param {string} reject_id   - custom_id for the reject button
 * @param {string} label       - Short action label shown on the approve button
 * @returns {object[]}  Array of component objects (one ACTION_ROW)
 */
export function approvalComponents(approve_id, reject_id, label) {
  return [
    {
      type: 1,  // ACTION_ROW
      components: [
        {
          type:      2,   // BUTTON
          style:     3,   // SUCCESS (green)
          label:     `✅ Approve: ${label}`.slice(0, 80),
          custom_id: String(approve_id),
        },
        {
          type:      2,   // BUTTON
          style:     4,   // DANGER (red)
          label:     "❌ Reject",
          custom_id: String(reject_id),
        },
      ],
    },
  ];
}

// ---------------------------------------------------------------------------
// Role mention helper
// ---------------------------------------------------------------------------

/**
 * Build the `allowed_mentions` block that safely pings a set of roles.
 * Discord ignores role pings in content unless the allow-list explicitly
 * includes the role ID — so this is the only safe way to mention roles.
 *
 * @param {string[]} role_ids  - Discord role ID strings to ping
 * @returns {{ parse: string[], roles: string[] }}
 */
export function allowedRoleMentions(role_ids) {
  return {
    parse: [],                             // no wildcard @everyone/@here/@role
    roles: Array.isArray(role_ids)
      ? role_ids.filter((id) => id && typeof id === "string")
      : [],
  };
}

// ---------------------------------------------------------------------------
// Cinematic command-center helpers
// ---------------------------------------------------------------------------

/**
 * Build a rich embed-based interaction response (type 4).
 *
 * All responses produced by this helper include `allowed_mentions: { parse: [] }`
 * to prevent accidental @everyone/@here/@role pings.
 *
 * @param {object}    opts
 * @param {string}    [opts.content]    - Optional text above embeds (max 2000 chars)
 * @param {object[]}  [opts.embeds]     - Discord embed objects (max 10)
 * @param {object[]}  [opts.components] - Action rows / buttons
 * @param {boolean}   [opts.ephemeral]  - If true, only the invoker sees the reply
 * @returns {object}  Discord interaction response
 */
export function cinematicMessage({ content = null, embeds = [], components = [], ephemeral = false } = {}) {
  const data = {
    ...(content          ? { content: String(content).slice(0, 2000) } : {}),
    ...(embeds.length    ? { embeds:  embeds.slice(0, 10) }            : {}),
    ...(components.length ? { components }                             : {}),
    ...(ephemeral        ? { flags: MESSAGE_FLAGS.EPHEMERAL }          : {}),
    allowed_mentions: { parse: [] },
  };
  return { type: INTERACTION_RESPONSE_TYPE.CHANNEL_MESSAGE, data };
}

/**
 * Return a deferred acknowledgement that shows "thinking…" to the user.
 *
 * Use when the actual work will take longer than ~3 seconds.  After the work
 * finishes, call editOriginalInteractionResponse from discord-followups.js to
 * deliver the result.
 *
 * @param {object}  [opts]
 * @param {boolean} [opts.ephemeral]  - If true, only the invoker sees the reply
 * @returns {{ type: 5 }}
 */
export function deferMessage({ ephemeral = false } = {}) {
  return ephemeral
    ? { type: 5, data: { flags: MESSAGE_FLAGS.EPHEMERAL } }
    : { type: 5 };
}

/**
 * Return an `allowed_mentions` block that suppresses all automatic pings.
 * Use in any message where @mentions appear in content but should NOT notify.
 *
 * @returns {{ parse: [] }}
 */
export function safeAllowedMentions() {
  return { parse: [] };
}

/**
 * Produce a safe, user-facing error string from an Error or unknown value.
 * Never exposes raw error messages, stack traces, or credential-like strings.
 *
 * @param {unknown} error
 * @returns {string}  Safe display string (max 200 chars)
 */
export function formatCommandError(error) {
  if (!error) return "An unexpected error occurred.";
  // Never pass through raw error messages — they may contain secrets, stack
  // traces, or internal details.  Only relay a message when the error has an
  // explicit, known-safe code property.
  const SAFE_CODES = ["not_found", "not_configured", "permission_denied", "timeout", "validation_error"];
  const code = error?.code ?? null;
  if (code && SAFE_CODES.includes(code)) {
    return String(error.message ?? code).slice(0, 200) || "An unexpected error occurred.";
  }
  return "An unexpected error occurred. Check server logs for details.";
}
