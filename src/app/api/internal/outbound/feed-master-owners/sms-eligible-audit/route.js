import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import { auditMasterOwnerSmsEligibleValues } from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.outbound.feed_master_owners.sms_eligible_audit",
});

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export async function GET(request) {
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const raw_scan_limit = asNumber(searchParams.get("raw_scan_limit"), 1000);

    logger.info("master_owner_feeder.sms_eligible_audit_requested", {
      method: "GET",
      raw_scan_limit,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await auditMasterOwnerSmsEligibleValues({
      raw_scan_limit,
    });

    return NextResponse.json(
      {
        ok: true,
        route: "internal/outbound/feed-master-owners/sms-eligible-audit",
        result,
      },
      { status: 200 }
    );
  } catch (error) {
    logger.error("master_owner_feeder.sms_eligible_audit_failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_sms_eligible_audit_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const raw_scan_limit = asNumber(body?.raw_scan_limit, 1000);

    logger.info("master_owner_feeder.sms_eligible_audit_requested", {
      method: "POST",
      raw_scan_limit,
      authenticated: auth.auth.authenticated,
      is_vercel_cron: auth.auth.is_vercel_cron,
    });

    const result = await auditMasterOwnerSmsEligibleValues({
      raw_scan_limit,
    });

    return NextResponse.json(
      {
        ok: true,
        route: "internal/outbound/feed-master-owners/sms-eligible-audit",
        result,
      },
      { status: 200 }
    );
  } catch (error) {
    logger.error("master_owner_feeder.sms_eligible_audit_failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "master_owner_feeder_sms_eligible_audit_failed",
      },
      { status: 500 }
    );
  }
}
