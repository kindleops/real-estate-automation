import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { dryRunOutbound } from "@/lib/testing/dry-run-outbound.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.queue.dry_run",
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

    logger.info("queue_dry_run.requested", {
      method: "GET",
      phone,
      use_case: use_case || null,
      language: language || null,
    });

    const result = await dryRunOutbound({
      phone,
      use_case: use_case || null,
      language: language || null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/dry-run",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("queue_dry_run.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "queue_dry_run_failed",
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

    logger.info("queue_dry_run.requested", {
      method: "POST",
      phone,
      use_case: use_case || null,
      language: language || null,
    });

    const result = await dryRunOutbound({
      phone,
      use_case: use_case || null,
      language: language || null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/queue/dry-run",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("queue_dry_run.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "queue_dry_run_failed",
      },
      { status: 500 }
    );
  }
}
