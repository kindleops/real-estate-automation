import { supabase as defaultSupabase } from "@/lib/supabase/client.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function plusHours(hours) {
  const dt = new Date();
  dt.setHours(dt.getHours() + Number(hours || 0));
  return dt.toISOString();
}

const defaultDeps = {
  supabase: defaultSupabase,
};

let runtimeDeps = { ...defaultDeps };

export function __setOpsRecommendationsDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetOpsRecommendationsDeps() {
  runtimeDeps = { ...defaultDeps };
}

async function safeCount(dbQuery) {
  try {
    const { count } = await dbQuery;
    return Number(count || 0);
  } catch {
    return 0;
  }
}

function buildRecommendation({
  recommendation_type,
  priority,
  title,
  reason,
  risk_level,
  expected_impact,
  suggested_action,
  approval_required = false,
  status = "pending",
  expires_hours = 24,
  metadata = {},
}) {
  return {
    recommendation_type,
    priority,
    title,
    reason,
    risk_level,
    expected_impact,
    suggested_action,
    approval_required,
    status,
    expires_at: plusHours(expires_hours),
    metadata,
  };
}

export async function generateOpsRecommendations() {
  const db = runtimeDeps.supabase;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const since = today.toISOString();

  const queued = await safeCount(
    db.from("send_queue").select("id", { count: "exact", head: true }).eq("queue_status", "queued")
  );
  const stuck = await safeCount(
    db.from("send_queue").select("id", { count: "exact", head: true }).eq("queue_status", "sending")
  );
  const failed = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "outbound").in("delivery_status", ["failed", "undelivered"]).gte("created_at", since)
  );
  const unknownInbound = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "inbound").eq("event_type", "inbound_unknown").gte("created_at", since)
  );
  const hotLeadCount = await safeCount(
    db.from("message_events").select("id", { count: "exact", head: true }).eq("direction", "inbound").eq("event_type", "inbound_hot_lead").gte("created_at", since)
  );

  const recommendations = [];

  if (stuck > 0) {
    recommendations.push(
      buildRecommendation({
        recommendation_type: "run_reconcile",
        priority: 9,
        title: "Queue has stuck rows",
        reason: `${stuck} rows are in sending state`,
        risk_level: "high",
        expected_impact: "Restore queue throughput",
        suggested_action: "Run queue reconcile and inspect lock contention",
        approval_required: false,
        metadata: { stuck_rows: stuck, generated_at: nowIso() },
      })
    );
  }

  if (failed >= 25) {
    recommendations.push(
      buildRecommendation({
        recommendation_type: "cool_down_number",
        priority: 8,
        title: "Delivery failures elevated",
        reason: `${failed} outbound failures today`,
        risk_level: "high",
        expected_impact: "Reduce carrier risk and preserve number health",
        suggested_action: "Cool down highest-failure TextGrid number and inspect failure buckets",
        approval_required: true,
        metadata: { failed_count: failed, generated_at: nowIso() },
      })
    );
  }

  if (unknownInbound >= 10) {
    recommendations.push(
      buildRecommendation({
        recommendation_type: "inspect_unknown_inbound",
        priority: 7,
        title: "Unknown inbound volume elevated",
        reason: `${unknownInbound} unknown inbound messages detected`,
        risk_level: "medium",
        expected_impact: "Recover missed opportunities and reduce confusion",
        suggested_action: "Review unknown inbound queue and map contacts",
        approval_required: false,
        metadata: { unknown_inbound: unknownInbound, generated_at: nowIso() },
      })
    );
  }

  if (queued < 20 && failed < 10) {
    recommendations.push(
      buildRecommendation({
        recommendation_type: "scale_campaign",
        priority: 6,
        title: "Queue running lean with manageable risk",
        reason: `Queued rows (${queued}) are below healthy buffer and failures are controlled`,
        risk_level: "low",
        expected_impact: "Increase send capacity and lead flow",
        suggested_action: "Run feed dry-run and approve small live expansion",
        approval_required: true,
        metadata: { queued_count: queued, failed_count: failed, generated_at: nowIso() },
      })
    );
  }

  if (hotLeadCount > 0) {
    recommendations.push(
      buildRecommendation({
        recommendation_type: "follow_up_hot_lead",
        priority: 10,
        title: "Hot lead follow-up available",
        reason: `${hotLeadCount} hot lead signals detected today`,
        risk_level: "low",
        expected_impact: "Accelerate offer creation and conversion",
        suggested_action: "Create offer tasks for top hot lead replies",
        approval_required: false,
        metadata: { hot_lead_count: hotLeadCount, generated_at: nowIso() },
      })
    );
  }

  if (!recommendations.length) {
    recommendations.push(
      buildRecommendation({
        recommendation_type: "approve_small_test",
        priority: 4,
        title: "System stable",
        reason: "No major risk signals detected",
        risk_level: "low",
        expected_impact: "Maintain steady learning loop",
        suggested_action: "Approve a small test campaign and monitor outcomes",
        approval_required: true,
        metadata: { generated_at: nowIso() },
      })
    );
  }

  try {
    if (recommendations.length) {
      await db.from("ops_recommendations").insert(
        recommendations.map((row) => ({
          ...row,
          created_at: nowIso(),
          updated_at: nowIso(),
        }))
      );
    }
  } catch {
    // table might not exist in all environments; fail open
  }

  return recommendations;
}
