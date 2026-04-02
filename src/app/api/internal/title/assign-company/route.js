import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { selectTitleCompany } from "@/lib/domain/title/select-title-company.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.title.assign_company",
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

    const property_id = asNumber(searchParams.get("property_id"));
    const market_id = asNumber(searchParams.get("market_id"));
    const title_routing_id = asNumber(searchParams.get("title_routing_id"));

    logger.info("title_assign_company.requested", {
      method: "GET",
      property_id,
      market_id,
      title_routing_id,
    });

    const result = await selectTitleCompany({
      property_id,
      market_id,
      title_routing_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/title/assign-company",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("title_assign_company.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "title_assign_company_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const property_id = asNumber(body?.property_id);
    const market_id = asNumber(body?.market_id);
    const title_routing_id = asNumber(body?.title_routing_id);

    logger.info("title_assign_company.requested", {
      method: "POST",
      property_id,
      market_id,
      title_routing_id,
    });

    const result = await selectTitleCompany({
      property_id,
      market_id,
      title_routing_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/title/assign-company",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("title_assign_company.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "title_assign_company_failed",
      },
      { status: 500 }
    );
  }
}
