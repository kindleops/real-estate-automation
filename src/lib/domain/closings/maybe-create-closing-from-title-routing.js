// ─── maybe-create-closing-from-title-routing.js ──────────────────────────
import {
  CLOSING_FIELDS,
  findClosingItems,
} from "@/lib/podio/apps/closings.js";
import {
  TITLE_ROUTING_FIELDS,
} from "@/lib/podio/apps/title-routing.js";
import { createClosingFromTitleRouting } from "@/lib/domain/closings/create-closing-from-title-routing.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);

  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (first?.value?.item_id) return first.value.item_id;
  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.start) return first.start;

  return null;
}

function isOpenableTitleRouting({
  title_routing_item = null,
  routing_status = null,
  result = null,
} = {}) {
  const normalized_status =
    clean(routing_status) ||
    clean(getFieldValue(title_routing_item, TITLE_ROUTING_FIELDS.routing_status)) ||
    clean(result?.reason);

  const status_text = normalized_status.toLowerCase();

  return [
    "not routed",
    "routed",
    "opened",
    "title reviewing",
    "waiting on docs",
    "waiting on payoff",
    "waiting on probate",
    "waiting on seller",
    "waiting on buyer",
    "clear to close",
  ].includes(status_text);
}

async function findLatestClosingByTitleRoutingId(title_routing_item_id) {
  if (!title_routing_item_id) return null;

  const matches = await findClosingItems(
    { [CLOSING_FIELDS.title_routing]: title_routing_item_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

function isTerminalClosingStatus(status = "") {
  const normalized = clean(status).toLowerCase();
  return ["completed", "cancelled"].includes(normalized);
}

export async function maybeCreateClosingFromTitleRouting({
  title_routing_item = null,
  title_routing_item_id = null,
  routing_status = null,
  title_routing_result = null,
  closing_id = null,
  closing_status = "Not Scheduled",
  source = "Title Routing Engine",
  notes = "",
} = {}) {
  const resolved_title_routing_item_id =
    title_routing_item?.item_id ||
    title_routing_item_id ||
    null;

  if (!resolved_title_routing_item_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_title_routing_item_id",
    };
  }

  const openable = isOpenableTitleRouting({
    title_routing_item,
    routing_status,
    result: title_routing_result,
  });

  if (!openable) {
    return {
      ok: true,
      created: false,
      reason: "title_routing_not_openable",
      title_routing_item_id: resolved_title_routing_item_id,
    };
  }

  const existing_closing = await findLatestClosingByTitleRoutingId(
    resolved_title_routing_item_id
  );

  if (existing_closing?.item_id) {
    const existing_status = clean(
      getFieldValue(existing_closing, CLOSING_FIELDS.closing_status)
    );

    if (!isTerminalClosingStatus(existing_status)) {
      const pipeline = await syncPipelineState({
        title_routing_item_id: resolved_title_routing_item_id,
        closing_item_id: existing_closing.item_id,
        notes: "Existing closing found for title routing.",
      });

      return {
        ok: true,
        created: false,
        reason: "existing_closing_found",
        title_routing_item_id: resolved_title_routing_item_id,
        closing_item_id: existing_closing.item_id,
        existing_closing,
        pipeline,
      };
    }
  }

  const created = await createClosingFromTitleRouting({
    title_routing_item_id: resolved_title_routing_item_id,
    title_routing_item,
    closing_id,
    closing_status,
    source,
    notes,
  });

  return {
    ok: Boolean(created?.ok),
    created: Boolean(created?.created),
    reason: created?.reason || "closing_create_attempted",
    title_routing_item_id: resolved_title_routing_item_id,
    closing_item_id: created?.closing_item_id || null,
    result: created,
  };
}

export default maybeCreateClosingFromTitleRouting;
