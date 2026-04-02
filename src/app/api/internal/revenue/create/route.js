import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { createDealRevenueFlow } from "@/lib/flows/create-deal-revenue-flow.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.revenue.create",
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

    const closing_id = asNumber(searchParams.get("closing_id"));
    const contract_id = asNumber(searchParams.get("contract_id"));
    const buyer_match_id = asNumber(searchParams.get("buyer_match_id"));
    const assignment_fee = asNumber(searchParams.get("assignment_fee"));
    const purchase_price = asNumber(searchParams.get("purchase_price"));
    const resale_price = asNumber(searchParams.get("resale_price"));

    logger.info("revenue_create.requested", {
      method: "GET",
      closing_id,
      contract_id,
      buyer_match_id,
      assignment_fee,
      purchase_price,
      resale_price,
    });

    const result = await createDealRevenueFlow({
      closing_id,
      contract_id,
      buyer_match_id,
      assignment_fee,
      purchase_price,
      resale_price,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/revenue/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("revenue_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "revenue_create_failed",
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
    const buyer_match_id = asNumber(body?.buyer_match_id);
    const assignment_fee = asNumber(body?.assignment_fee);
    const purchase_price = asNumber(body?.purchase_price);
    const resale_price = asNumber(body?.resale_price);

    logger.info("revenue_create.requested", {
      method: "POST",
      closing_id,
      contract_id,
      buyer_match_id,
      assignment_fee,
      purchase_price,
      resale_price,
    });

    const result = await createDealRevenueFlow({
      closing_id,
      contract_id,
      buyer_match_id,
      assignment_fee,
      purchase_price,
      resale_price,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/revenue/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("revenue_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "revenue_create_failed",
      },
      { status: 500 }
    );
  }
}
