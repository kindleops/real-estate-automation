// ─── update-closing-status.js ────────────────────────────────────────────
import {
  CLOSING_FIELDS,
  getClosingItem,
  updateClosingItem,
} from "@/lib/podio/apps/closings.js";
import { getFirstAppReferenceId } from "@/lib/providers/podio.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { updateBrainFromExecution } from "@/lib/domain/brain/update-brain-from-execution.js";
import { syncContractStatus } from "@/lib/domain/contracts/sync-contract-status.js";
import { updateTitleRoutingStatus } from "@/lib/domain/title/update-title-routing-status.js";
import { createDealRevenueFromClosedClosing } from "@/lib/domain/revenue/create-deal-revenue-from-closed-closing.js";

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

const defaultDeps = {
  getClosingItem,
  updateClosingItem,
  syncPipelineState,
  updateBrainFromExecution,
  syncContractStatus,
  updateTitleRoutingStatus,
  createDealRevenueFromClosedClosing,
};

let runtimeDeps = { ...defaultDeps };

export function __setClosingStatusTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetClosingStatusTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function updateClosingStatus({
  closing_item_id = null,
  closing_item = null,
  status = null,
  notes = "",
} = {}) {
  let resolved_closing_item = closing_item || null;

  if (!resolved_closing_item && closing_item_id) {
    resolved_closing_item = await runtimeDeps.getClosingItem(closing_item_id);
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

  await runtimeDeps.updateClosingItem(resolved_closing_item_id, payload);
  const contract_item_id = getFirstAppReferenceId(
    resolved_closing_item,
    CLOSING_FIELDS.contract,
    null
  );
  const title_routing_item_id = getFirstAppReferenceId(
    resolved_closing_item,
    CLOSING_FIELDS.title_routing,
    null
  );
  const contract_sync =
    normalized_status === "Completed" && contract_item_id
      ? await runtimeDeps.syncContractStatus({
          contract_item_id,
          status: "Closed",
        })
      : normalized_status === "Cancelled" && contract_item_id
        ? await runtimeDeps.syncContractStatus({
            contract_item_id,
            status: "Cancelled",
          })
        : null;
  const title_sync =
    title_routing_item_id &&
    (normalized_status === "Completed" || normalized_status === "Cancelled")
      ? await runtimeDeps.updateTitleRoutingStatus({
          title_routing_item_id,
          status: normalized_status === "Completed" ? "Closed" : "Cancelled",
          notes:
            clean(notes) ||
            (normalized_status === "Completed"
              ? "Closing completed successfully."
              : "Closing was cancelled."),
        })
      : null;
  const revenue_sync =
    normalized_status === "Completed"
      ? await runtimeDeps.createDealRevenueFromClosedClosing({
          closing_item_id: resolved_closing_item_id,
        })
      : null;
  const pipeline = await runtimeDeps.syncPipelineState({
    closing_item_id: resolved_closing_item_id,
    contract_item_id,
    title_routing_item_id,
    deal_revenue_item_id:
      revenue_sync?.deal_revenue_item_id || revenue_sync?.result?.deal_revenue_item_id || null,
    notes:
      clean(notes) ||
      `Closing status updated to ${normalized_status || existing_status}.`,
  });
  const brain_update = await runtimeDeps.updateBrainFromExecution({
    source: "closing",
    closing_item: resolved_closing_item,
    closing_status: normalized_status || existing_status,
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
    contract_sync,
    title_sync,
    revenue_sync,
    pipeline,
    brain_update,
  };
}

export default updateClosingStatus;
