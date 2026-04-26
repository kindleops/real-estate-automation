import { nowIso } from "@/lib/utils/dates.js";

function clean(value) {
  return String(value ?? "").trim();
}

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeQueueStatus(value = "") {
  return clean(value).toLowerCase();
}

function addSecondsIso(value = null, seconds = 0) {
  const date = new Date(value || Date.now());
  date.setSeconds(date.getSeconds() + Number(seconds || 0));
  return date.toISOString();
}

const DEFAULT_SELECT = [
  "id",
  "queue_status",
  "scheduled_for",
  "scheduled_for_utc",
  "scheduled_for_local",
  "created_at",
  "updated_at",
  "metadata",
  "message_body",
  "message_text",
].join(",");

export async function findInboundAutopilotQueue(
  {
    message_event_id = "",
    supabase = null,
    includeStatuses = ["queued", "sending"],
  } = {}
) {
  const event_id = clean(message_event_id);
  if (!event_id || !supabase) return null;

  const { data, error } = await supabase
    .from("send_queue")
    .select(DEFAULT_SELECT)
    .eq("metadata->>inbound_message_event_id", event_id)
    .eq("metadata->>autopilot_reply", "true")
    .order("created_at", { ascending: false })
    .limit(10);

  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const wanted = new Set((includeStatuses || []).map((value) => normalizeQueueStatus(value)));

  return (
    rows.find((row) => {
      const status = normalizeQueueStatus(row?.queue_status);
      return !wanted.size || wanted.has(status);
    }) || null
  );
}

export async function updateInboundAutopilotQueue(
  {
    queue_row = null,
    queue_id = "",
    message_event_id = "",
    supabase = null,
    updates = {},
    metadata_updates = {},
  } = {}
) {
  if (!supabase) {
    return { ok: false, reason: "missing_supabase" };
  }

  const existing_row =
    queue_row ||
    (queue_id
      ? { id: queue_id, metadata: {} }
      : await findInboundAutopilotQueue({ message_event_id, supabase, includeStatuses: [] }));

  if (!existing_row?.id) {
    return { ok: true, updated: false, reason: "queue_row_not_found" };
  }

  const next_metadata = {
    ...ensureObject(existing_row.metadata),
    ...ensureObject(metadata_updates),
  };

  const payload = {
    ...ensureObject(updates),
    metadata: next_metadata,
    updated_at: nowIso(),
  };

  const { data, error } = await supabase
    .from("send_queue")
    .update(payload)
    .eq("id", existing_row.id)
    .select(DEFAULT_SELECT)
    .maybeSingle();

  if (error) throw error;

  return {
    ok: true,
    updated: true,
    queue_row: data || { ...existing_row, ...payload },
  };
}

export async function cancelInboundAutopilotQueue(
  {
    message_event_id = "",
    queue_row = null,
    supabase = null,
    discord_user_id = "",
    review_status = "autopilot_cancelled",
    cancellation_reason = "discord_override",
  } = {}
) {
  const existing_row =
    queue_row ||
    (await findInboundAutopilotQueue({
      message_event_id,
      supabase,
      includeStatuses: ["queued"],
    }));

  if (!existing_row?.id) {
    return { ok: true, cancelled: false, reason: "no_pending_autopilot_queue" };
  }

  const result = await updateInboundAutopilotQueue({
    queue_row: existing_row,
    supabase,
    updates: {
      queue_status: "cancelled",
    },
    metadata_updates: {
      discord_review_status: clean(review_status) || "autopilot_cancelled",
      cancelled_by_discord_user_id: clean(discord_user_id) || null,
      autopilot_cancelled_at: nowIso(),
      autopilot_cancellation_reason: clean(cancellation_reason) || null,
    },
  });

  return {
    ok: true,
    cancelled: true,
    queue_row: result.queue_row,
  };
}

export async function expediteInboundAutopilotQueue(
  {
    message_event_id = "",
    queue_row = null,
    supabase = null,
    discord_user_id = "",
    review_status = "approved_send_now",
  } = {}
) {
  const existing_row =
    queue_row ||
    (await findInboundAutopilotQueue({
      message_event_id,
      supabase,
      includeStatuses: ["queued"],
    }));

  if (!existing_row?.id) {
    return { ok: true, expedited: false, reason: "no_pending_autopilot_queue" };
  }

  const now = nowIso();
  const result = await updateInboundAutopilotQueue({
    queue_row: existing_row,
    supabase,
    updates: {
      scheduled_for: now,
      scheduled_for_utc: now,
      scheduled_for_local: now,
    },
    metadata_updates: {
      discord_review_status: clean(review_status) || "approved_send_now",
      approved_send_now_by_discord_user_id: clean(discord_user_id) || null,
      approved_send_now_at: now,
    },
  });

  return {
    ok: true,
    expedited: true,
    queue_row: result.queue_row,
  };
}

export function buildInboundAutopilotSchedule(delay_seconds = 60, base_time = null) {
  const scheduled_for = addSecondsIso(base_time, delay_seconds);
  return {
    scheduled_for,
    scheduled_for_utc: scheduled_for,
    scheduled_for_local: scheduled_for,
  };
}