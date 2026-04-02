// ─── upsert-underwriting-from-context.js ──────────────────────────────────
import {
  createUnderwritingItem,
  findUnderwritingItems,
  UNDERWRITING_FIELDS,
  updateUnderwritingItem,
} from "@/lib/podio/apps/underwriting.js";

import { buildUnderwritingPayload } from "@/lib/domain/underwriting/build-underwriting-payload.js";

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

async function findLatestUnderwritingMatch({
  property_id = null,
  prospect_id = null,
  master_owner_id = null,
} = {}) {
  if (property_id) {
    const property_matches = await findUnderwritingItems(
      { [UNDERWRITING_FIELDS.property]: property_id },
      50,
      0
    );

    const latest_property_match = sortNewestFirst(property_matches)[0] || null;
    if (latest_property_match) return latest_property_match;
  }

  if (prospect_id) {
    const prospect_matches = await findUnderwritingItems(
      { [UNDERWRITING_FIELDS.prospect]: prospect_id },
      50,
      0
    );

    const latest_prospect_match = sortNewestFirst(prospect_matches)[0] || null;
    if (latest_prospect_match) return latest_prospect_match;
  }

  if (master_owner_id) {
    const owner_matches = await findUnderwritingItems(
      { [UNDERWRITING_FIELDS.master_owner]: master_owner_id },
      50,
      0
    );

    const latest_owner_match = sortNewestFirst(owner_matches)[0] || null;
    if (latest_owner_match) return latest_owner_match;
  }

  return null;
}

export async function upsertUnderwritingFromContext({
  context = null,
  signals = {},
  offer_item_id = null,
  pipeline_item_id = null,
  underwriting_id = null,
  source_channel = "SMS",
  notes = "",
} = {}) {
  if (!context?.found) {
    return {
      ok: false,
      created: false,
      updated: false,
      reason: "context_not_found",
    };
  }

  const ids = context?.ids || {};
  const property_id = ids.property_id || null;
  const prospect_id = ids.prospect_id || null;
  const master_owner_id = ids.master_owner_id || null;

  if (!property_id && !prospect_id && !master_owner_id) {
    return {
      ok: false,
      created: false,
      updated: false,
      reason: "missing_underwriting_anchor",
    };
  }

  const payload_result = buildUnderwritingPayload({
    context,
    signals,
    offer_item_id,
    pipeline_item_id,
    underwriting_id,
    source_channel,
    notes,
  });

  if (!payload_result?.ok) {
    return {
      ok: false,
      created: false,
      updated: false,
      reason: "payload_build_failed",
    };
  }

  const payload = payload_result.payload || {};
  const explicit_underwriting_id = clean(underwriting_id);

  let existing_item = null;

  if (explicit_underwriting_id) {
    const direct_matches = await findUnderwritingItems(
      { [UNDERWRITING_FIELDS.underwriting_id]: explicit_underwriting_id },
      10,
      0
    );

    existing_item = sortNewestFirst(direct_matches)[0] || null;
  }

  if (!existing_item) {
    existing_item = await findLatestUnderwritingMatch({
      property_id,
      prospect_id,
      master_owner_id,
    });
  }

  if (existing_item?.item_id) {
    await updateUnderwritingItem(existing_item.item_id, payload);

    return {
      ok: true,
      created: false,
      updated: true,
      reason: "underwriting_updated",
      underwriting_item_id: existing_item.item_id,
      payload,
    };
  }

  const created = await createUnderwritingItem(payload);

  return {
    ok: true,
    created: true,
    updated: false,
    reason: "underwriting_created",
    underwriting_item_id: created?.item_id || null,
    payload,
    raw: created,
  };
}

export default upsertUnderwritingFromContext;
