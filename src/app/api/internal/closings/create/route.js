import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { createClosingFlow } from "@/lib/flows/create-closing-flow.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.closings.create",
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
    const title_routing_id = asNumber(searchParams.get("title_routing_id"));
    const property_id = asNumber(searchParams.get("property_id"));

    logger.info("closings_create.requested", {
      method: "GET",
      contract_id,
      title_routing_id,
      property_id,
    });

    const result = await createClosingFlow({
      contract_id,
      title_routing_id,
      property_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/closings/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("closings_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "closings_create_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const contract_id = asNumber(body?.contract_id);
    const title_routing_id = asNumber(body?.title_routing_id);
    const property_id = asNumber(body?.property_id);

    logger.info("closings_create.requested", {
      method: "POST",
      contract_id,
      title_routing_id,
      property_id,
    });

    const result = await createClosingFlow({
      contract_id,
      title_routing_id,
      property_id,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/closings/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("closings_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "closings_create_failed",
      },
      { status: 500 }
    );
  }
}
