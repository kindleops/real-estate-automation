// ─── update-title-routing-status.js ──────────────────────────────────────
import {
  TITLE_ROUTING_FIELDS,
  getTitleRoutingItem,
  updateTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);

  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.value?.item_id) return first.value.item_id;
  if (first?.start) return first.start;

  return null;
}

function appendNote(existing_notes, new_note) {
  const prior = clean(existing_notes);
  const next = clean(new_note);

  if (!next) return prior || undefined;
  if (!prior) return next;

  return `${prior}\n${next}`;
}

function normalizeRoutingStatus(value = "") {
  const raw = clean(value).toLowerCase();

  if (raw.includes("file opened") || raw === "opened") return "Opened";
  if (raw.includes("sent to title") || raw.includes("routed")) return "Routed";
  if (raw.includes("title review")) return "Title Reviewing";
  if (raw.includes("awaiting docs") || raw.includes("pending docs")) {
    return "Waiting on Docs";
  }
  if (raw.includes("payoff")) return "Waiting on Payoff";
  if (raw.includes("probate")) return "Waiting on Probate";
  if (raw.includes("seller")) return "Waiting on Seller";
  if (raw.includes("buyer")) return "Waiting on Buyer";
  if (raw.includes("clear")) return "Clear to Close";
  if (raw.includes("closed")) return "Closed";
  if (raw.includes("cancel")) return "Cancelled";
  if (raw.includes("not routed")) return "Not Routed";

  return clean(value) || "";
}

function isTerminalRoutingStatus(status = "") {
  const normalized = clean(status).toLowerCase();
  return normalized === "closed" || normalized === "cancelled";
}

export async function updateTitleRoutingStatus({
  title_routing_item_id = null,
  title_routing_item = null,
  status = null,
  notes = "",
} = {}) {
  let resolved_title_routing_item = title_routing_item || null;

  if (!resolved_title_routing_item && title_routing_item_id) {
    resolved_title_routing_item = await getTitleRoutingItem(title_routing_item_id);
  }

  const resolved_title_routing_item_id =
    resolved_title_routing_item?.item_id ||
    title_routing_item_id ||
    null;

  if (!resolved_title_routing_item_id) {
    return {
      ok: false,
      updated: false,
      reason: "missing_title_routing_item_id",
    };
  }

  const existing_status = clean(
    getFieldValue(resolved_title_routing_item, TITLE_ROUTING_FIELDS.routing_status)
  );

  if (isTerminalRoutingStatus(existing_status) && !clean(status)) {
    return {
      ok: true,
      updated: false,
      reason: "title_routing_already_terminal",
      title_routing_item_id: resolved_title_routing_item_id,
      routing_status: existing_status,
    };
  }

  const normalized_status = normalizeRoutingStatus(status);
  const payload = {
    ...(normalized_status
      ? { [TITLE_ROUTING_FIELDS.routing_status]: normalized_status }
      : {}),
    ...(clean(notes) || existing_status
      ? {
          [TITLE_ROUTING_FIELDS.internal_notes]: appendNote(
            clean(
              getFieldValue(
                resolved_title_routing_item,
                TITLE_ROUTING_FIELDS.internal_notes
              )
            ),
            clean(notes)
              ? `[${nowIso()}] ${clean(notes)}`
              : normalized_status
                ? `[${nowIso()}] Title routing status updated to ${normalized_status}.`
                : ""
          ),
        }
      : {}),
  };

  if (normalized_status === "Routed") {
    payload[TITLE_ROUTING_FIELDS.file_routed_date] = { start: nowIso() };
  }

  if (normalized_status === "Opened") {
    payload[TITLE_ROUTING_FIELDS.title_opened_date] = { start: nowIso() };
  }

  if (normalized_status === "Clear to Close") {
    payload[TITLE_ROUTING_FIELDS.clear_to_close_date] = { start: nowIso() };
  }

  if (normalized_status === "Closed") {
    payload[TITLE_ROUTING_FIELDS.resolved] = "Yes";
  }

  if (normalized_status === "Cancelled") {
    payload[TITLE_ROUTING_FIELDS.resolved] = "No";
  }

  await updateTitleRoutingItem(resolved_title_routing_item_id, payload);
  const pipeline = await syncPipelineState({
    title_routing_item_id: resolved_title_routing_item_id,
    notes:
      clean(notes) ||
      `Title routing status updated to ${normalized_status || existing_status}.`,
  });

  return {
    ok: true,
    updated: true,
    reason: "title_routing_status_updated",
    title_routing_item_id: resolved_title_routing_item_id,
    routing_status: normalized_status || existing_status || null,
    payload,
    pipeline,
  };
}

export default updateTitleRoutingStatus;
