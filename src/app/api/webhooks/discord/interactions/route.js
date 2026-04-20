/**
 * POST /api/webhooks/discord/interactions
 *
 * Entry point for all Discord slash commands and button interactions.
 *
 * Security:
 *  1. Ed25519 signature verification (DISCORD_PUBLIC_KEY) — required before
 *     any processing.  Invalid signature → 401.  This prevents spoofed payloads
 *     from reaching the action router.
 *  2. Guild ID check — only requests from DISCORD_GUILD_ID are processed.
 *  3. Role-based permission checks are enforced inside the action router.
 *  4. Secrets are never included in Discord response content.
 *
 * Flow:
 *   PING (type=1)             → PONG immediately (Discord health check)
 *   Slash command (type=2)    → routeDiscordInteraction()
 *   Button click (type=3)     → routeDiscordInteraction()
 *   Anything else             → 400
 */

import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { verifyDiscordRequest } from "@/lib/discord/verify-discord-request.js";
import { routeDiscordInteraction } from "@/lib/discord/discord-action-router.js";
import { pong, errorResponse } from "@/lib/discord/discord-response-helpers.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

const logger = child({ module: "api.webhooks.discord.interactions" });

// ---------------------------------------------------------------------------
// Signature verification
// ---------------------------------------------------------------------------

/**
 * Verify the Discord Ed25519 signature on the raw body.
 *
 * Reading the raw body here (before any JSON.parse) is required because the
 * signature covers the literal byte stream, not the parsed object.
 *
 * @param {Request} request
 * @returns {Promise<{ verified: boolean, rawBody: string, body: object|null }>}
 */
async function verifyAndParse(request) {
  const signature = String(request.headers.get("x-signature-ed25519") ?? "");
  const timestamp = String(request.headers.get("x-signature-timestamp") ?? "");
  const publicKey = String(process.env.DISCORD_PUBLIC_KEY ?? "");

  // Buffer the raw bytes before any parsing.
  const rawBody = await request.text();

  const verified = verifyDiscordRequest({ publicKey, signature, timestamp, rawBody });

  let body = null;
  if (verified) {
    try {
      body = JSON.parse(rawBody);
    } catch {
      return { verified: false, rawBody, body: null };
    }
  }

  return { verified, rawBody, body };
}

// ---------------------------------------------------------------------------
// Guild guard
// ---------------------------------------------------------------------------

function isAllowedGuild(interaction) {
  const allowed_guild = String(process.env.DISCORD_GUILD_ID ?? "").trim();
  if (!allowed_guild) return true; // unconfigured → allow (dev/test)
  return String(interaction?.guild_id ?? "") === allowed_guild;
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------


function last4(value) {
  const text = String(value ?? "");
  return text ? text.slice(-4) : null;
}

function getInteractionLabel(body) {
  if (body?.type === 1) return "PING";
  if (body?.type === 2) return `COMMAND:${body?.data?.name ?? "unknown"}`;
  if (body?.type === 3) return `COMPONENT:${body?.data?.custom_id ?? "unknown"}`;
  return `TYPE:${body?.type ?? "unknown"}`;
}

export async function POST(request) {
  const signature = String(request.headers.get("x-signature-ed25519") ?? "");
  const timestamp = String(request.headers.get("x-signature-timestamp") ?? "");

  logger.info("discord.interaction.route_hit", {
    method: request.method,
    signature_present: Boolean(signature),
    signature_length: signature.length,
    timestamp_present: Boolean(timestamp),
    timestamp_length: timestamp.length,
    public_key_configured: Boolean(String(process.env.DISCORD_PUBLIC_KEY ?? "").trim()),
    guild_guard_configured: Boolean(String(process.env.DISCORD_GUILD_ID ?? "").trim()),
  });

  let verified = false;
  let body     = null;

  try {
    const result = await verifyAndParse(request);
    verified = result.verified;
    body     = result.body;

    logger.info("discord.interaction.signature_check", {
      verified,
      raw_body_length: result.rawBody?.length ?? 0,
      parsed_body_present: Boolean(body),
    });
  } catch (err) {
    logger.error("discord.interaction.error", {
      phase: "parse_or_verify",
      error: err?.message,
    });
    return NextResponse.json({ error: "Bad request" }, { status: 400 });
  }

  if (!verified) {
    logger.warn("discord.interactions.invalid_signature");
    return NextResponse.json({ error: "Invalid request signature" }, { status: 401 });
  }

  logger.info("discord.interaction.parsed", {
    type: body?.type ?? null,
    label: getInteractionLabel(body),
    command_name: body?.data?.name ?? null,
    custom_id_present: Boolean(body?.data?.custom_id),
    guild_id_last4: last4(body?.guild_id),
    channel_id_last4: last4(body?.channel_id),
    user_id_last4: last4(body?.member?.user?.id || body?.user?.id),
  });

  // Discord PING — respond immediately.
  if (body?.type === 1) {
    logger.info("discord.interaction.response_sent", {
      label: "PING",
      response_type: "PONG",
    });
    return NextResponse.json(pong());
  }

  // Guild guard — reject requests from unexpected guilds.
  if (!isAllowedGuild(body)) {
    logger.warn("discord.interactions.wrong_guild", {
      guild_id_last4: last4(body?.guild_id),
      expected_guild_id_last4: last4(process.env.DISCORD_GUILD_ID),
    });
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  }

  // APPLICATION_COMMAND (2) and MESSAGE_COMPONENT (3) — route to action router.
  if (body?.type === 2 || body?.type === 3) {
    try {
      const response = await routeDiscordInteraction(body);

      logger.info("discord.interaction.response_sent", {
        label: getInteractionLabel(body),
        response_type: response?.type ?? null,
        content_present: Boolean(response?.data?.content),
        ephemeral: Boolean(response?.data?.flags),
      });

      return NextResponse.json(response);
    } catch (err) {
      logger.error("discord.interaction.error", {
        phase: "router",
        label: getInteractionLabel(body),
        error: err?.message,
      });
      return NextResponse.json(errorResponse("Unexpected server error."));
    }
  }

  // Any other interaction type is unsupported.
  logger.warn("discord.interactions.unsupported_type", { type: body?.type });
  return NextResponse.json({ error: "Unsupported interaction type" }, { status: 400 });
}

// GET: health check so Discord can verify the endpoint is listening.
export async function GET() {
  return NextResponse.json({
    ok:     true,
    route:  "webhooks/discord/interactions",
    status: "listening",
  });
}
