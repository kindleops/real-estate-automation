import { NextResponse } from "next/server";

import { getRolloutControls, resolveMutationDryRun, resolveScopedId } from "@/lib/config/rollout-controls.js";
import { child } from "@/lib/logging/logger.js";
import { createBuyerMatchFlow } from "@/lib/flows/create-buyer-match-flow.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.buyers.match",
});

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = String(value ?? "").trim().toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

function statusForResult(result) {
  const reason = String(result?.reason || "").toLowerCase();
  if (reason.includes("not_implemented")) return 501;
  return result?.ok === false ? 400 : 200;
}

export async function GET(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const rollout = getRolloutControls();
    const property_id = asNumber(searchParams.get("property_id"));
    const contract_scope = resolveScopedId({
      requested_id: asNumber(searchParams.get("contract_id")),
      safe_id: rollout.single_contract_id,
      resource: "contract",
    });
    if (!contract_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: contract_scope.reason,
        },
        { status: 400 }
      );
    }
    const closing_id = asNumber(searchParams.get("closing_id"));
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(searchParams.get("dry_run"), false),
    });
    const candidate_limit = asNumber(searchParams.get("candidate_limit"), 10);

    logger.info("buyers_match.requested", {
      method: "GET",
      property_id,
      contract_id: contract_scope.effective_id,
      closing_id,
      dry_run: dry_run_resolution.effective_dry_run,
      candidate_limit,
    });

    const result = await createBuyerMatchFlow({
      property_id,
      contract_id: contract_scope.effective_id,
      closing_id,
      dry_run: dry_run_resolution.effective_dry_run,
      candidate_limit,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/buyers/match",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("buyers_match.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "buyers_match_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const rollout = getRolloutControls();
    const property_id = asNumber(body?.property_id);
    const contract_scope = resolveScopedId({
      requested_id: asNumber(body?.contract_id),
      safe_id: rollout.single_contract_id,
      resource: "contract",
    });
    if (!contract_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: contract_scope.reason,
        },
        { status: 400 }
      );
    }
    const closing_id = asNumber(body?.closing_id);
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(body?.dry_run, false),
    });
    const candidate_limit = asNumber(body?.candidate_limit, 10);

    logger.info("buyers_match.requested", {
      method: "POST",
      property_id,
      contract_id: contract_scope.effective_id,
      closing_id,
      dry_run: dry_run_resolution.effective_dry_run,
      candidate_limit,
    });

    const result = await createBuyerMatchFlow({
      property_id,
      contract_id: contract_scope.effective_id,
      closing_id,
      dry_run: dry_run_resolution.effective_dry_run,
      candidate_limit,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/buyers/match",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("buyers_match.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "buyers_match_failed",
      },
      { status: 500 }
    );
  }
}
