// ─── update-closing-status.js ────────────────────────────────────────────
import {
  CLOSING_FIELDS,
  getClosingItem,
  updateClosingItem,
} from "@/lib/podio/apps/closings.js";
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

function normalizeClosingStatus(value = "") {
  const raw = clean(value).toLowerCase();

  if (!raw) return "";
  if (raw.includes("closed") || raw.includes("completed")) return "Completed";
  if (raw.includes("cancel")) return "Cancelled";
  if (raw.includes("resched")) return "Rescheduled";
  if (raw.includes("confirm") || raw.includes("clear to close")) return "Confirmed";
  if (raw.includes("schedule")) return "Scheduled";
  return "Not Scheduled";
}

function isTerminalClosingStatus(status = "") {
  const normalized = clean(status).toLowerCase();
  return normalized === "completed" || normalized === "cancelled";
}

export async function updateClosingStatus({
  closing_item_id = null,
  closing_item = null,
  status = null,
  notes = "",
} = {}) {
  let resolved_closing_item = closing_item || null;

  if (!resolved_closing_item && closing_item_id) {
    resolved_closing_item = await getClosingItem(closing_item_id);
  }

  const resolved_closing_item_id =
    resolved_closing_item?.item_id ||
    closing_item_id ||
    null;

  if (!resolved_closing_item_id) {
    return {
      ok: false,
      updated: false,
      reason: "missing_closing_item_id",
    };
  }

  const existing_status = clean(
    getFieldValue(resolved_closing_item, CLOSING_FIELDS.closing_status)
  );

  if (isTerminalClosingStatus(existing_status) && !clean(status)) {
    return {
      ok: true,
      updated: false,
      reason: "closing_already_terminal",
      closing_item_id: resolved_closing_item_id,
      closing_status: existing_status,
    };
  }

  const normalized_status = normalizeClosingStatus(status);
  const raw_status = clean(status).toLowerCase();
  const payload = {
    ...(normalized_status
      ? { [CLOSING_FIELDS.closing_status]: normalized_status }
      : {}),
  };

  const noteField =
    normalized_status === "Completed" || normalized_status === "Cancelled"
      ? CLOSING_FIELDS.post_close_notes
      : CLOSING_FIELDS.pre_close_notes;

  payload[noteField] = appendNote(
    clean(getFieldValue(resolved_closing_item, noteField)),
    clean(notes)
      ? `[${nowIso()}] ${clean(notes)}`
      : normalized_status
        ? `[${nowIso()}] Closing status updated to ${normalized_status}.`
        : ""
  );

  if (raw_status.includes("clear to close")) {
    payload[CLOSING_FIELDS.ready_to_close] = "Yes";
    payload[CLOSING_FIELDS.docs_complete] = "Yes";
    payload[CLOSING_FIELDS.confirmed_date] = { start: nowIso() };
  }

  if (raw_status.includes("pending docs")) {
    payload[CLOSING_FIELDS.docs_complete] = "No";
  }

  if (normalized_status === "Scheduled") {
    payload[CLOSING_FIELDS.ready_to_close] = "No";
  }

  if (normalized_status === "Confirmed") {
    payload[CLOSING_FIELDS.confirmed_date] = { start: nowIso() };
  }

  if (normalized_status === "Rescheduled") {
    payload[CLOSING_FIELDS.rescheduled_date] = { start: nowIso() };
  }

  if (normalized_status === "Completed") {
    payload[CLOSING_FIELDS.actual_closing_date] = { start: nowIso() };
    payload[CLOSING_FIELDS.closed_successfully] = "Yes";
  }

  if (normalized_status === "Cancelled") {
    payload[CLOSING_FIELDS.closed_successfully] = "No";
  }

  await updateClosingItem(resolved_closing_item_id, payload);
  const pipeline = await syncPipelineState({
    closing_item_id: resolved_closing_item_id,
    notes:
      clean(notes) ||
      `Closing status updated to ${normalized_status || existing_status}.`,
  });

  return {
    ok: true,
    updated: true,
    reason: "closing_status_updated",
    closing_item_id: resolved_closing_item_id,
    closing_status: normalized_status || existing_status || null,
    payload,
    pipeline,
  };
}

export default updateClosingStatus;
