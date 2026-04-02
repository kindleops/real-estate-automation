import { NextResponse } from "next/server";

import {
  capBuyerBlastRecipients,
  getRolloutControls,
  resolveMutationDryRun,
  resolveScopedId,
} from "@/lib/config/rollout-controls.js";
import { child } from "@/lib/logging/logger.js";
import { sendBuyerBlast } from "@/lib/domain/buyers/send-buyer-blast.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.buyers.blast",
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
    const buyer_match_scope = resolveScopedId({
      requested_id: asNumber(searchParams.get("buyer_match_id")),
      safe_id: rollout.single_buyer_match_id,
      resource: "buyer_match",
    });
    if (!buyer_match_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: buyer_match_scope.reason,
        },
        { status: 400 }
      );
    }

    const property_id = asNumber(searchParams.get("property_id"));
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(searchParams.get("dry_run"), true),
    });
    const max_buyers = capBuyerBlastRecipients(
      asNumber(searchParams.get("max_buyers"), 5),
      5
    );
    const force = asBoolean(searchParams.get("force"), false);

    logger.info("buyers_blast.requested", {
      method: "GET",
      buyer_match_id: buyer_match_scope.effective_id,
      property_id,
      dry_run: dry_run_resolution.effective_dry_run,
      max_buyers,
      force,
    });

    const result = await sendBuyerBlast({
      buyer_match_id: buyer_match_scope.effective_id,
      property_id,
      dry_run: dry_run_resolution.effective_dry_run,
      max_buyers,
      force,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/buyers/blast",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("buyers_blast.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "buyers_blast_failed",
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
    const buyer_match_scope = resolveScopedId({
      requested_id: asNumber(body?.buyer_match_id),
      safe_id: rollout.single_buyer_match_id,
      resource: "buyer_match",
    });
    if (!buyer_match_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: buyer_match_scope.reason,
        },
        { status: 400 }
      );
    }

    const property_id = asNumber(body?.property_id);
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(body?.dry_run, true),
    });
    const max_buyers = capBuyerBlastRecipients(asNumber(body?.max_buyers, 5), 5);
    const force = asBoolean(body?.force, false);

    logger.info("buyers_blast.requested", {
      method: "POST",
      buyer_match_id: buyer_match_scope.effective_id,
      property_id,
      dry_run: dry_run_resolution.effective_dry_run,
      max_buyers,
      force,
    });

    const result = await sendBuyerBlast({
      buyer_match_id: buyer_match_scope.effective_id,
      property_id,
      dry_run: dry_run_resolution.effective_dry_run,
      max_buyers,
      force,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/buyers/blast",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("buyers_blast.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "buyers_blast_failed",
      },
      { status: 500 }
    );
  }
}
