import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { syncClosingMilestones } from "@/lib/domain/closings/sync-closing-milestones.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.closings.sync",
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

    const closing_id = asNumber(searchParams.get("closing_id"));
    const contract_id = asNumber(searchParams.get("contract_id"));
    const status = clean(searchParams.get("status"));
    const notes = clean(searchParams.get("notes"));

    logger.info("closings_sync.requested", {
      method: "GET",
      closing_id,
      contract_id,
      status: status || null,
    });

    const result = await syncClosingMilestones({
      closing_id,
      contract_id,
      status: status || null,
      notes,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/closings/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("closings_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "closings_sync_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const closing_id = asNumber(body?.closing_id);
    const contract_id = asNumber(body?.contract_id);
    const status = clean(body?.status);
    const notes = clean(body?.notes);

    logger.info("closings_sync.requested", {
      method: "POST",
      closing_id,
      contract_id,
      status: status || null,
    });

    const result = await syncClosingMilestones({
      closing_id,
      contract_id,
      status: status || null,
      notes,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/closings/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("closings_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "closings_sync_failed",
      },
      { status: 500 }
    );
  }
}
