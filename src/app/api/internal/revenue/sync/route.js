import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { updateDealRevenue } from "@/lib/domain/revenue/update-deal-revenue.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.revenue.sync",
});

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

    const deal_revenue_id = asNumber(searchParams.get("deal_revenue_id"));
    const closing_id = asNumber(searchParams.get("closing_id"));

    logger.info("revenue_sync.requested", {
      method: "GET",
      deal_revenue_id,
      closing_id,
    });

    const result = await updateDealRevenue({
      deal_revenue_id,
      closing_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/revenue/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("revenue_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "revenue_sync_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const deal_revenue_id = asNumber(body?.deal_revenue_id);
    const closing_id = asNumber(body?.closing_id);

    logger.info("revenue_sync.requested", {
      method: "POST",
      deal_revenue_id,
      closing_id,
    });

    const result = await updateDealRevenue({
      deal_revenue_id,
      closing_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/revenue/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("revenue_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "revenue_sync_failed",
      },
      { status: 500 }
    );
  }
}
