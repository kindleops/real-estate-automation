import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import {
  acknowledgeSystemAlert,
  silenceSystemAlert,
  unsilenceSystemAlert,
} from "@/lib/domain/alerts/system-alerts.js";
import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.alerts.control",
});

function clean(value) {
  return String(value ?? "").trim();
}

function normalizeAction(value) {
  const normalized = clean(value).toLowerCase();
  if (["ack", "acknowledge"].includes(normalized)) return "acknowledge";
  if (["silence", "mute"].includes(normalized)) return "silence";
  if (["unsilence", "unmute", "open"].includes(normalized)) return "unsilence";
  return normalized;
}

function addMinutesIso(minutes = 0) {
  const next = new Date();
  next.setUTCMinutes(next.getUTCMinutes() + Number(minutes || 0));
  return next.toISOString();
}

function statusForResult(result) {
  if (result?.ok === false && result?.reason === "alert_not_found") return 404;
  return result?.ok === false ? 400 : 200;
}

function parseControlPayload(input = {}) {
  const silence_for_minutes = Number(input?.silence_for_minutes || input?.silenceForMinutes || 0);
  const silenced_until =
    clean(input?.silenced_until || input?.silencedUntil) ||
    (Number.isFinite(silence_for_minutes) && silence_for_minutes > 0
      ? addMinutesIso(silence_for_minutes)
      : null);

  return {
    action: normalizeAction(input?.action),
    alert_item_id: Number(input?.alert_item_id || input?.alertItemId || 0) || null,
    subsystem: clean(input?.subsystem) || null,
    code: clean(input?.code) || null,
    dedupe_key: clean(input?.dedupe_key || input?.dedupeKey) || null,
    signature: clean(input?.signature) || null,
    actor: clean(input?.actor) || "operator",
    note: clean(input?.note || input?.reason) || null,
    silenced_until,
  };
}

async function executeControl(payload = {}) {
  if (payload.action === "acknowledge") {
    return acknowledgeSystemAlert(payload);
  }

  if (payload.action === "silence") {
    if (!clean(payload.silenced_until)) {
      return {
        ok: false,
        reason: "missing_silenced_until",
      };
    }
    return silenceSystemAlert(payload);
  }

  if (payload.action === "unsilence") {
    return unsilenceSystemAlert(payload);
  }

  return {
    ok: false,
    reason: "unsupported_alert_control_action",
  };
}

function authorizeAlertControl(request) {
  const internal_auth = requireSharedSecretAuth(request, logger, {
    env_name: "INTERNAL_API_SECRET",
    header_names: ["x-internal-api-secret"],
  });
  if (internal_auth.authorized) return internal_auth;

  return requireOpsDashboardAuth(request, logger);
}

export async function POST(request) {
  try {
    const auth = authorizeAlertControl(request);
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const payload = parseControlPayload(body);
    const result = await executeControl(payload);

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/alerts/control",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("alerts.control_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "alerts_control_failed",
      },
      { status: 500 }
    );
  }
}
