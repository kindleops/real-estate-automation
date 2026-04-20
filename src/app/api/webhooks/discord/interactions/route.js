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

export async function POST(request) {
  let verified = false;
  let body     = null;

  try {
    const result = await verifyAndParse(request);
    verified = result.verified;
    body     = result.body;
  } catch (err) {
    logger.error("discord.interactions.parse_error", { error: err?.message });
    return NextResponse.json({ error: "Bad request" }, { status: 400 });
  }

  if (!verified) {
    logger.warn("discord.interactions.invalid_signature");
    return NextResponse.json({ error: "Invalid request signature" }, { status: 401 });
  }

  // Discord PING — respond immediately.
  if (body?.type === 1) {
    return NextResponse.json(pong());
  }

  // Guild guard — reject requests from unexpected guilds.
  if (!isAllowedGuild(body)) {
    logger.warn("discord.interactions.wrong_guild", {
      guild_id: body?.guild_id,
    });
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  }

  // APPLICATION_COMMAND (2) and MESSAGE_COMPONENT (3) — route to action router.
  if (body?.type === 2 || body?.type === 3) {
    try {
      const response = await routeDiscordInteraction(body);
      return NextResponse.json(response);
    } catch (err) {
      logger.error("discord.interactions.router_error", { error: err?.message });
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
