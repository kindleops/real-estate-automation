import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { syncTitleMilestones } from "@/lib/domain/title/sync-title-milestones.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.title.sync",
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

    const title_routing_id = asNumber(searchParams.get("title_routing_id"));
    const contract_id = asNumber(searchParams.get("contract_id"));
    const status = clean(searchParams.get("status"));
    const notes = clean(searchParams.get("notes"));

    logger.info("title_sync.requested", {
      method: "GET",
      title_routing_id,
      contract_id,
      status: status || null,
    });

    const result = await syncTitleMilestones({
      title_routing_id,
      contract_id,
      status: status || null,
      notes,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/title/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("title_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "title_sync_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const title_routing_id = asNumber(body?.title_routing_id);
    const contract_id = asNumber(body?.contract_id);
    const status = clean(body?.status);
    const notes = clean(body?.notes);

    logger.info("title_sync.requested", {
      method: "POST",
      title_routing_id,
      contract_id,
      status: status || null,
    });

    const result = await syncTitleMilestones({
      title_routing_id,
      contract_id,
      status: status || null,
      notes,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/title/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("title_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "title_sync_failed",
      },
      { status: 500 }
    );
  }
}
