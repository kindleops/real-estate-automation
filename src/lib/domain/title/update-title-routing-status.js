// ─── update-title-routing-status.js ──────────────────────────────────────
import {
  TITLE_ROUTING_FIELDS,
  getTitleRoutingItem,
  updateTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import { getDateValue, getFirstAppReferenceId } from "@/lib/providers/podio.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import { updateBrainFromExecution } from "@/lib/domain/brain/update-brain-from-execution.js";
import { syncContractStatus } from "@/lib/domain/contracts/sync-contract-status.js";
import { maybeCreateClosingFromTitleRouting } from "@/lib/domain/closings/maybe-create-closing-from-title-routing.js";

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

function mapContractStatusFromRoutingStatus(status = "") {
  switch (clean(status)) {
    case "Routed":
      return "Sent To Title";
    case "Opened":
      return "Opened";
    case "Clear to Close":
      return "Clear To Close";
    case "Closed":
      return "Closed";
    case "Cancelled":
      return "Cancelled";
    default:
      return null;
  }
}

function shouldEnsureClosing({
  normalized_status = "",
  title_routing_item = null,
} = {}) {
  const status = clean(normalized_status);

  if (["Closed", "Cancelled", "Not Routed"].includes(status)) return false;
  if (status === "Clear to Close") return true;

  return Boolean(
    getDateValue(title_routing_item, TITLE_ROUTING_FIELDS.expected_closing_date, null)
  );
}

const defaultDeps = {
  getTitleRoutingItem,
  updateTitleRoutingItem,
  syncPipelineState,
  updateBrainFromExecution,
  syncContractStatus,
  maybeCreateClosingFromTitleRouting,
};

let runtimeDeps = { ...defaultDeps };

export function __setTitleRoutingStatusTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetTitleRoutingStatusTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function updateTitleRoutingStatus({
  title_routing_item_id = null,
  title_routing_item = null,
  status = null,
  notes = "",
} = {}) {
  let resolved_title_routing_item = title_routing_item || null;

  if (!resolved_title_routing_item && title_routing_item_id) {
    resolved_title_routing_item = await runtimeDeps.getTitleRoutingItem(
      title_routing_item_id
    );
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

  await runtimeDeps.updateTitleRoutingItem(resolved_title_routing_item_id, payload);
  const contract_item_id = getFirstAppReferenceId(
    resolved_title_routing_item,
    TITLE_ROUTING_FIELDS.contract,
    null
  );
  const contract_sync = contract_item_id
    ? await runtimeDeps.syncContractStatus({
        contract_item_id,
        status: mapContractStatusFromRoutingStatus(normalized_status || existing_status),
      })
    : null;
  const closing_sync = shouldEnsureClosing({
    normalized_status,
    title_routing_item: resolved_title_routing_item,
  })
    ? await runtimeDeps.maybeCreateClosingFromTitleRouting({
        title_routing_item_id: resolved_title_routing_item_id,
        title_routing_item: resolved_title_routing_item,
        routing_status: normalized_status || existing_status,
        closing_status:
          normalized_status === "Clear to Close" ? "Scheduled" : "Not Scheduled",
        source: "Title Routing Engine",
        notes:
          clean(notes) ||
          `Title routing status updated to ${normalized_status || existing_status}.`,
      })
    : null;
  const pipeline = await runtimeDeps.syncPipelineState({
    title_routing_item_id: resolved_title_routing_item_id,
    closing_item_id:
      closing_sync?.closing_item_id || closing_sync?.result?.closing_item_id || null,
    notes:
      clean(notes) ||
      `Title routing status updated to ${normalized_status || existing_status}.`,
  });
  const brain_update = await runtimeDeps.updateBrainFromExecution({
    source: "title",
    title_routing_item: resolved_title_routing_item,
    routing_status: normalized_status || existing_status,
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
    contract_sync,
    closing_sync,
    pipeline,
    brain_update,
  };
}

export default updateTitleRoutingStatus;
