import { NextResponse } from "next/server";

import { requireOpsDashboardAuth } from "@/lib/security/dashboard-auth.js";
import { supabase } from "@/lib/supabase/client.js";
import { getSystemFlag } from "@/lib/system-control.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function clean(value) {
  return String(value ?? "").trim();
}

export async function GET(request) {
  try {
    const auth = requireOpsDashboardAuth(request);
    if (!auth.authorized) return auth.response;

    const dashboard_live_enabled = await getSystemFlag("dashboard_live_enabled");
    if (!dashboard_live_enabled) {
      return NextResponse.json(
        {
          ok: false,
          error: "system_control_disabled",
          flag_key: "dashboard_live_enabled",
          context: "dashboard-offer-stage-ai",
        },
        { status: 423 }
      );
    }

    const { searchParams } = new URL(request.url);
    const thread_key = clean(searchParams.get("thread_key"));

    if (!thread_key) {
      return NextResponse.json(
        { ok: false, error: "missing_thread_key" },
        { status: 400 }
      );
    }

    console.log("[NexusOfferStageAI]", {
      selectedThreadId: thread_key,
      metadataFound: "querying",
    });

    const { data: events, error } = await supabase
      .from("message_events")
      .select("id, message_body, metadata, direction, created_at")
      .contains("metadata", { offer_stage_ai_triggered: true })
      .or(`metadata->>offer_stage_ai_thread_key.eq.${thread_key},thread_key.eq.${thread_key}`)
      .order("created_at", { ascending: false })
      .limit(5);

    if (error) throw error;

    const latestWithMetadata = events?.find((e) => e.metadata?.offer_stage_ai_result) || null;

    if (!latestWithMetadata) {
      console.log("[NexusOfferStageAI]", {
        selectedThreadId: thread_key,
        metadataFound: false,
      });

      return NextResponse.json({
        ok: true,
        route: "internal/dashboard/inbox/offer-stage-ai",
        thread_key,
        has_offer_stage_ai: false,
        data: null,
      });
    }

    const meta = latestWithMetadata.metadata.offer_stage_ai_result;

    console.log("[NexusOfferStageAI]", {
      selectedThreadId: thread_key,
      metadataFound: true,
      triggerReason: meta?.trigger_reason,
      sendMode: meta?.send_mode,
      safeToReveal: meta?.safe_to_reveal_offer,
      hasDraftMessage: Boolean(meta?.draft_message),
    });

    return NextResponse.json({
      ok: true,
      route: "internal/dashboard/inbox/offer-stage-ai",
      thread_key,
      has_offer_stage_ai: true,
      data: {
        triggered: meta?.triggered || false,
        trigger_reason: meta?.trigger_reason || "unknown",
        asset_type: meta?.asset_type || "unknown",
        recommended_opening_offer: meta?.recommended_opening_offer || null,
        target_contract: meta?.target_contract || null,
        walkaway_internal: meta?.walkaway_internal || null,
        offer_confidence_score: meta?.offer_confidence_score || 0,
        safe_to_reveal_offer: meta?.safe_to_reveal_offer || false,
        missing_required_info: meta?.missing_required_info || [],
        blocked_reason: meta?.blocked_reason || null,
        send_mode: meta?.send_mode || "dry_run_offer_ai",
        would_queue: meta?.would_queue || false,
        would_auto_send: meta?.would_auto_send || false,
        draft_message: meta?.draft_message || null,
        action: meta?.action || null,
        route: meta?.route || null,
        timestamp: meta?.timestamp || latestWithMetadata.created_at,
      },
    });
  } catch (error) {
    console.error("[NexusOfferStageAI] Error:", error);
    return NextResponse.json(
      {
        ok: false,
        route: "internal/dashboard/inbox/offer-stage-ai",
        error: "offer_stage_ai_fetch_failed",
        message: error?.message || "Unknown error",
      },
      { status: 500 }
    );
  }
}
