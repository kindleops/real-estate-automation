/**
 * discord-followups.js
 *
 * Helpers for deferred Discord interaction responses.
 *
 * When a command takes longer than ~3 seconds Discord stops waiting for an
 * initial response.  Returning a deferred type immediately (type 5) shows
 * "AppName is thinking..." to the user.  Once the work completes, call
 * editOriginalInteractionResponse or createFollowupMessage to deliver the
 * result.
 *
 * Security:
 *   - The interaction token is NEVER logged.
 *   - applicationId defaults to the DISCORD_APPLICATION_ID env var.
 */

const DISCORD_API = "https://discord.com/api/v10";

// ---------------------------------------------------------------------------
// Synchronous response stubs (returned to Discord immediately)
// ---------------------------------------------------------------------------

/**
 * Immediate deferred response — shows "thinking" to all channel members.
 * Return this object as the interaction response from the route handler.
 *
 * @returns {{ type: 5 }}
 */
export function deferredPublicResponse() {
  return { type: 5 };
}

/**
 * Immediate deferred ephemeral response — shows "thinking" only to invoker.
 *
 * @returns {{ type: 5, data: { flags: 64 } }}
 */
export function deferredEphemeralResponse() {
  return { type: 5, data: { flags: 64 } };
}

// ---------------------------------------------------------------------------
// REST follow-up helpers
// ---------------------------------------------------------------------------

/**
 * Edit the original deferred interaction message.
 *
 * Call this after performing the deferred work to replace the
 * "thinking…" placeholder with the actual result.
 *
 * @param {object}    opts
 * @param {string}    opts.token            - Discord interaction token
 * @param {string}    [opts.applicationId]  - Defaults to DISCORD_APPLICATION_ID env
 * @param {string}    [opts.content]        - Text content (max 2000 chars)
 * @param {object[]}  [opts.embeds]         - Embed array (max 10)
 * @param {object[]}  [opts.components]     - Component rows
 * @param {number}    [opts.flags]          - e.g. 64 for ephemeral
 * @returns {Promise<{ ok: boolean, error?: string }>}
 */
export async function editOriginalInteractionResponse({
  token,
  applicationId,
  content,
  embeds,
  components,
  flags,
} = {}) {
  const app_id = String(applicationId ?? process.env.DISCORD_APPLICATION_ID ?? "");

  if (!app_id || !token) {
    return { ok: false, error: "missing_app_id_or_token" };
  }

  const url  = `${DISCORD_API}/webhooks/${app_id}/${token}/messages/@original`;
  const body = {};

  if (content    != null) body.content    = String(content).slice(0, 2000);
  if (embeds     != null) body.embeds     = Array.isArray(embeds)     ? embeds.slice(0, 10)  : [];
  if (components != null) body.components = Array.isArray(components) ? components           : [];
  if (flags      != null) body.flags      = Number(flags);

  try {
    const res = await fetch(url, {
      method:  "PATCH",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(body),
    });

    if (res.ok) return { ok: true };

    const text = await res.text().catch(() => "");
    return { ok: false, error: `http_${res.status}`, detail: text.slice(0, 200) };
  } catch {
    return { ok: false, error: "fetch_failed" };
  }
}

/**
 * Post a follow-up message after an interaction (appends, does not replace).
 *
 * @param {object}    opts
 * @param {string}    opts.token
 * @param {string}    [opts.applicationId]
 * @param {string}    [opts.content]
 * @param {object[]}  [opts.embeds]
 * @param {object[]}  [opts.components]
 * @param {number}    [opts.flags]          - 64 for ephemeral follow-up
 * @returns {Promise<{ ok: boolean, error?: string }>}
 */
export async function createFollowupMessage({
  token,
  applicationId,
  content,
  embeds,
  components,
  flags,
} = {}) {
  const app_id = String(applicationId ?? process.env.DISCORD_APPLICATION_ID ?? "");

  if (!app_id || !token) {
    return { ok: false, error: "missing_app_id_or_token" };
  }

  const url  = `${DISCORD_API}/webhooks/${app_id}/${token}`;
  const body = {};

  if (content    != null) body.content    = String(content).slice(0, 2000);
  if (embeds     != null) body.embeds     = Array.isArray(embeds)     ? embeds.slice(0, 10)  : [];
  if (components != null) body.components = Array.isArray(components) ? components           : [];
  if (flags      != null) body.flags      = Number(flags);

  try {
    const res = await fetch(url, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(body),
    });

    if (res.ok) return { ok: true };

    const text = await res.text().catch(() => "");
    return { ok: false, error: `http_${res.status}`, detail: text.slice(0, 200) };
  } catch {
    return { ok: false, error: "fetch_failed" };
  }
}
