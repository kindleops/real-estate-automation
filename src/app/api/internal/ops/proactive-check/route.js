import { NextResponse } from "next/server";

import { child }           from "@/lib/logging/logger.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import {
  runProactiveOpsCheck,
} from "@/lib/domain/ops/proactive-notifications.js";
import {
  buildCampaignScaleApprovalEmbed,
  buildCampaignPauseAlertEmbed,
} from "@/lib/discord/discord-embed-factory.js";
import { opsApprovalActionRow } from "@/lib/discord/discord-components.js";

export const runtime    = "nodejs";
export const dynamic    = "force-dynamic";
export const maxDuration = 60;

const logger = child({ module: "api.internal.ops.proactive-check" });

function parseBoolParam(value, fallback = true) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "boolean") return value;
  const s = String(value).toLowerCase().trim();
  if (s === "false" || s === "0" || s === "no") return false;
  if (s === "true"  || s === "1" || s === "yes") return true;
  return fallback;
}

export async function POST(request) {
  try {
    const auth = requireCronAuth(request, logger);
    if (!auth.authorized) return auth.response;

    let body = {};
    try {
      body = await request.json();
    } catch { /* no body — use defaults */ }

    const dry_run          = parseBoolParam(body?.dry_run, true);
    const discord_dispatch = parseBoolParam(body?.discord_dispatch, false);
    const enabled          = parseBoolParam(
      process.env.OPS_NOTIFICATIONS_ENABLED,
      true,
    );

    if (!enabled) {
      logger.info("ops.proactive_check.disabled");
      return NextResponse.json({ ok: true, skipped: true, reason: "disabled_by_env" });
    }

    const webhook_url = process.env.OPS_DISCORD_WEBHOOK_URL ?? "";

    logger.info("ops.proactive_check.started", { dry_run, discord_dispatch });

    const result = await runProactiveOpsCheck({
      dry_run,
      discord_dispatch: discord_dispatch && !dry_run,
      webhook_url,
      embed_builder: (rec, analysis) => {
        if (rec.type === "scale") {
          return buildCampaignScaleApprovalEmbed({
            campaign_key: rec.campaign_key,
            market:       rec.market,
            asset:        rec.asset,
            strategy:     rec.strategy,
            current_cap:  rec.current_cap,
            proposed_cap: rec.proposed_cap,
            metrics:      rec.metrics,
            request_key:  rec.request_key,
            reason:       rec.reason,
          });
        }
        return buildCampaignPauseAlertEmbed({
          campaign_key: rec.campaign_key,
          reason:       rec.reason,
          opt_out_rate: rec.opt_out_rate,
          failed_rate:  rec.failed_rate,
          request_key:  rec.request_key,
        });
      },
      component_builder: (request_key, type) =>
        opsApprovalActionRow({ requestKey: request_key, type }),
    });

    logger.info("ops.proactive_check.completed", {
      campaigns_checked:     result.campaigns_checked,
      scale_recommendations: result.scale_recommendations,
      pause_recommendations: result.pause_recommendations,
      notifications_created: result.notifications_created,
      dry_run,
    });

    return NextResponse.json({ ok: true, ...result });
  } catch (err) {
    logger.error("ops.proactive_check.error", { error: String(err?.message ?? err) });
    return NextResponse.json({ ok: false, error: "internal_error" }, { status: 500 });
  }
}
