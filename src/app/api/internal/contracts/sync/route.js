import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { syncContractStatus } from "@/lib/domain/contracts/sync-contract-status.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.contracts.sync",
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

    const contract_id = asNumber(searchParams.get("contract_id"));
    const status = clean(searchParams.get("status"));

    logger.info("contracts_sync.requested", {
      method: "GET",
      contract_id,
      status: status || null,
    });

    const result = await syncContractStatus({
      contract_id,
      status: status || null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/contracts/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("contracts_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "contracts_sync_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const contract_id = asNumber(body?.contract_id);
    const status = clean(body?.status);

    logger.info("contracts_sync.requested", {
      method: "POST",
      contract_id,
      status: status || null,
    });

    const result = await syncContractStatus({
      contract_id,
      status: status || null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/contracts/sync",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("contracts_sync.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "contracts_sync_failed",
      },
      { status: 500 }
    );
  }
}
