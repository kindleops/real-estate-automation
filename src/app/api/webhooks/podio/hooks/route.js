import { NextResponse } from "next/server";

import APP_IDS from "@/lib/config/app-ids.js";
import { child } from "@/lib/logging/logger.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.podio.hooks",
});

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

async function parseIncomingRequest(request) {
  const contentType = clean(request.headers.get("content-type")).toLowerCase();

  if (contentType.includes("application/json")) {
    const body = await request.json().catch(() => ({}));
    return {
      raw: body,
      type: clean(body?.type),
      hook_id: asNumber(body?.hook_id),
      app_id: asNumber(body?.app_id),
      item_id: asNumber(body?.item_id),
      user_id: asNumber(body?.user_id),
      revision_id: asNumber(body?.revision_id),
    };
  }

  if (
    contentType.includes("application/x-www-form-urlencoded") ||
    contentType.includes("multipart/form-data")
  ) {
    const form = await request.formData().catch(() => null);
    const body = form
      ? Object.fromEntries(Array.from(form.entries()).map(([k, v]) => [k, String(v)]))
      : {};

    return {
      raw: body,
      type: clean(body?.type),
      hook_id: asNumber(body?.hook_id),
      app_id: asNumber(body?.app_id),
      item_id: asNumber(body?.item_id),
      user_id: asNumber(body?.user_id),
      revision_id: asNumber(body?.revision_id),
    };
  }

  const text = await request.text().catch(() => "");
  return {
    raw: { text },
    type: "",
    hook_id: null,
    app_id: null,
    item_id: null,
    user_id: null,
    revision_id: null,
  };
}

function resolveAppName(app_id) {
  const entries = Object.entries(APP_IDS || {});
  const match = entries.find(([, value]) => Number(value) === Number(app_id));
  return match?.[0] || "unknown";
}

async function dispatchPodioHook(payload) {
  const app_name = resolveAppName(payload.app_id);

  logger.info("podio_hook.received", {
    type: payload.type || null,
    hook_id: payload.hook_id,
    app_id: payload.app_id,
    app_name,
    item_id: payload.item_id,
    user_id: payload.user_id,
    revision_id: payload.revision_id,
  });

  // ───────────────────────────────────────────────────────────
  // Future routing surface
  // Keep this fast. Podio expects a quick 200.
  // Put any heavy follow-up into async queues/jobs later.
  // ───────────────────────────────────────────────────────────

  switch (payload.app_id) {
    case APP_IDS.send_queue:
      logger.debug("podio_hook.dispatch.send_queue", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.message_events:
      logger.debug("podio_hook.dispatch.message_events", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.ai_conversation_brain:
      logger.debug("podio_hook.dispatch.ai_conversation_brain", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.offers:
      logger.debug("podio_hook.dispatch.offers", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.contracts:
      logger.debug("podio_hook.dispatch.contracts", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.title_routing:
      logger.debug("podio_hook.dispatch.title_routing", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.closings:
      logger.debug("podio_hook.dispatch.closings", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.buyer_match:
      logger.debug("podio_hook.dispatch.buyer_match", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    case APP_IDS.deal_revenue:
      logger.debug("podio_hook.dispatch.deal_revenue", {
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;

    default:
      logger.debug("podio_hook.dispatch.unhandled_app", {
        app_id: payload.app_id,
        app_name,
        item_id: payload.item_id,
        type: payload.type || null,
      });
      break;
  }
}

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "podio/hooks",
    status: "listening",
  });
}

export async function POST(request) {
  try {
    const payload = await parseIncomingRequest(request);

    if (!payload.app_id && !payload.item_id && !payload.type) {
      logger.warn("podio_hook.invalid_payload", {
        raw: payload.raw,
      });

      return NextResponse.json(
        {
          ok: false,
          error: "invalid_podio_hook_payload",
        },
        { status: 400 }
      );
    }

    await dispatchPodioHook(payload);

    return NextResponse.json({
      ok: true,
      received: true,
    });
  } catch (err) {
    logger.error("podio_hook.unhandled_error", {
      error: err,
    });

    return NextResponse.json(
      {
        ok: false,
        error: "podio_hook_route_failed",
      },
      { status: 500 }
    );
  }
}