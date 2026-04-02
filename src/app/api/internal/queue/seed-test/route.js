import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { seedTestQueueItem } from "@/lib/testing/seed-test-queue-item.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.queue.seed_test",
});

function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);

    const phone = String(searchParams.get("phone") || "").trim();
    const use_case = String(searchParams.get("use_case") || "").trim();
    const language = String(searchParams.get("language") || "").trim();
    const send_now = String(searchParams.get("send_now") || "").trim().toLowerCase() === "true";

    logger.info("queue_seed_test.requested", {
      method: "GET",
      phone,
      use_case: use_case || null,
      language: language || null,
      send_now,
    });

    const result = await seedTestQueueItem({
      phone,
      use_case: use_case || null,
      language: language || null,
      send_now,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/seed-test",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("queue_seed_test.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "queue_seed_test_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const phone = String(body?.phone || "").trim();
    const use_case = String(body?.use_case || "").trim();
    const language = String(body?.language || "").trim();
    const send_now = Boolean(body?.send_now);

    logger.info("queue_seed_test.requested", {
      method: "POST",
      phone,
      use_case: use_case || null,
      language: language || null,
      send_now,
    });

    const result = await seedTestQueueItem({
      phone,
      use_case: use_case || null,
      language: language || null,
      send_now,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/seed-test",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("queue_seed_test.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "queue_seed_test_failed",
      },
      { status: 500 }
    );
  }
}
