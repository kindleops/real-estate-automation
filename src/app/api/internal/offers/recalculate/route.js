import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { calculateOffer } from "@/lib/domain/offers/calculate-offer.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.offers.recalculate",
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
    const strategy = clean(searchParams.get("strategy"));
    const arv = asNumber(searchParams.get("arv"), null);
    const repairs = asNumber(searchParams.get("repairs"), null);

    logger.info("offer_recalculate.requested", {
      method: "GET",
      property_id,
      strategy: strategy || null,
      arv,
      repairs,
    });

    const result = await calculateOffer({
      property_id,
      strategy: strategy || null,
      arv,
      repairs,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/offers/recalculate",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("offer_recalculate.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "offer_recalculate_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const property_id = asNumber(body?.property_id, null);
    const strategy = clean(body?.strategy);
    const arv = asNumber(body?.arv, null);
    const repairs = asNumber(body?.repairs, null);

    logger.info("offer_recalculate.requested", {
      method: "POST",
      property_id,
      strategy: strategy || null,
      arv,
      repairs,
    });

    const result = await calculateOffer({
      property_id,
      strategy: strategy || null,
      arv,
      repairs,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/offers/recalculate",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("offer_recalculate.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "offer_recalculate_failed",
      },
      { status: 500 }
    );
  }
}
