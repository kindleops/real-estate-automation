import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { routeToTitleFlow } from "@/lib/flows/route-to-title-flow.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.title.route_file",
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

    const contract_id = asNumber(searchParams.get("contract_id"));
    const property_id = asNumber(searchParams.get("property_id"));
    const market_id = asNumber(searchParams.get("market_id"));

    logger.info("title_route_file.requested", {
      method: "GET",
      contract_id,
      property_id,
      market_id,
    });

    const result = await routeToTitleFlow({
      contract_id,
      property_id,
      market_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/title/route-file",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("title_route_file.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "title_route_file_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const contract_id = asNumber(body?.contract_id);
    const property_id = asNumber(body?.property_id);
    const market_id = asNumber(body?.market_id);

    logger.info("title_route_file.requested", {
      method: "POST",
      contract_id,
      property_id,
      market_id,
    });

    const result = await routeToTitleFlow({
      contract_id,
      property_id,
      market_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/title/route-file",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("title_route_file.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "title_route_file_failed",
      },
      { status: 500 }
    );
  }
}
