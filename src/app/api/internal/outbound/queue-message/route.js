import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.outbound.queue_message",
});

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);

    const phone = clean(searchParams.get("phone"));
    const use_case = clean(searchParams.get("use_case"));
    const language = clean(searchParams.get("language"));
    const touch_number = asNumber(searchParams.get("touch_number"), null);

    logger.info("outbound_queue_message.requested", {
      method: "GET",
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    const result = await queueOutboundMessage({
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/queue-message",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("outbound_queue_message.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "outbound_queue_message_failed",
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

    logger.info("outbound_queue_message.requested", {
      method: "POST",
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    const result = await queueOutboundMessage({
      phone,
      use_case: use_case || null,
      language: language || null,
      touch_number,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/outbound/queue-message",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("outbound_queue_message.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "outbound_queue_message_failed",
      },
      { status: 500 }
    );
  }
}
