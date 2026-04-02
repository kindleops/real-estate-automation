import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { processSendQueue } from "@/lib/domain/queue/process-send-queue.js";
import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.outbound.send_now",
});

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function statusForResult(result) {
  return result?.queued?.ok === false || result?.processed?.ok === false ? 400 : 200;
}

async function buildAndSendNow({ phone, use_case = null, language = null, touch_number = null }) {
  const queued = await queueOutboundMessage({
    phone,
    use_case,
    language,
    touch_number,
  });

  const queue_item_id =
    queued?.queue_item_id ||
    queued?.item_id ||
    queued?.result?.queue_item_id ||
    null;

  if (!queued?.ok) {
    return {
      queued,
      processed: null,
    };
  }

  if (!queue_item_id) {
    return {
      queued,
      processed: {
        ok: false,
        sent: false,
        reason: "missing_queue_item_id_after_queue",
      },
    };
  }

  const processed = await processSendQueue({
    queue_item_id,
  });

  return {
    queued,
    processed,
  };
}

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);

    const phone = clean(searchParams.get("phone"));
    const use_case = clean(searchParams.get("use_case"));
    const language = clean(searchParams.get("language"));
    const touch_number = asNumber(searchParams.get("touch_number"), null);

    logger.info("outbound_send_now.requested", {
      method: "GET",
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    const result = await buildAndSendNow({
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    return NextResponse.json(
      {
        ok: result?.queued?.ok === true && result?.processed?.ok === true,
        route: "internal/outbound/send-now",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("outbound_send_now.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "outbound_send_now_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const phone = clean(body?.phone);
    const use_case = clean(body?.use_case);
    const language = clean(body?.language);
    const touch_number = asNumber(body?.touch_number, null);

    logger.info("outbound_send_now.requested", {
      method: "POST",
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    const result = await buildAndSendNow({
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    return NextResponse.json(
      {
        ok: result?.queued?.ok === true && result?.processed?.ok === true,
        route: "internal/outbound/send-now",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("outbound_send_now.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "outbound_send_now_failed",
      },
      { status: 500 }
    );
  }
}
