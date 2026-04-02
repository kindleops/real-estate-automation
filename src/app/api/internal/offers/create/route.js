import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { createOfferFlow } from "@/lib/flows/create-offer-flow.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.offers.create",
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

    const property_id = asNumber(searchParams.get("property_id"), null);
    const master_owner_id = asNumber(searchParams.get("master_owner_id"), null);
    const prospect_id = asNumber(searchParams.get("prospect_id"), null);
    const strategy = clean(searchParams.get("strategy"));

    logger.info("offer_create.requested", {
      method: "GET",
      property_id,
      master_owner_id,
      prospect_id,
      strategy: strategy || null,
    });

    const result = await createOfferFlow({
      property_id,
      master_owner_id,
      prospect_id,
      strategy: strategy || null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/offers/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("offer_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "offer_create_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const property_id = asNumber(body?.property_id, null);
    const master_owner_id = asNumber(body?.master_owner_id, null);
    const prospect_id = asNumber(body?.prospect_id, null);
    const strategy = clean(body?.strategy);

    logger.info("offer_create.requested", {
      method: "POST",
      property_id,
      master_owner_id,
      prospect_id,
      strategy: strategy || null,
    });

    const result = await createOfferFlow({
      property_id,
      master_owner_id,
      prospect_id,
      strategy: strategy || null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/offers/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("offer_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "offer_create_failed",
      },
      { status: 500 }
    );
  }
}
