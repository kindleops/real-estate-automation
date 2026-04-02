// ─── update-offer-status.js ──────────────────────────────────────────────
import { getTextValue } from "@/lib/providers/podio.js";
import {
  getOfferItem,
  OFFER_FIELDS,
  updateOfferItem,
  normalizeOfferStatus,
} from "@/lib/podio/apps/offers.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function nowIso() {
  return new Date().toISOString();
}

function clean(value) {
  return String(value ?? "").trim();
}

function buildTimestampFields(status, timestamp) {
  const start = timestamp || nowIso();

  if (status === "Offer Sent" || status === "Revised Offer Sent") {
    return {
      [OFFER_FIELDS.offer_date]: { start },
    };
  }

  if (status === "Accepted (Ready for Contract)") {
    return {
      [OFFER_FIELDS.accepted_date]: { start },
    };
  }

  if (status === "Rejected") {
    return {
      [OFFER_FIELDS.rejected_date]: { start },
    };
  }

  if (status === "Expired") {
    return {
      [OFFER_FIELDS.offer_expiration_date]: { start },
    };
  }

  return {};
}

export async function updateOfferStatus({
  offer_item_id = null,
  status = null,
  notes = "",
  timestamp = null,
} = {}) {
  if (!offer_item_id) {
    return {
      ok: false,
      reason: "missing_offer_item_id",
    };
  }

  if (!status) {
    return {
      ok: false,
      reason: "missing_status",
      offer_item_id,
    };
  }

  const normalized_status = normalizeOfferStatus(status);
  const existing_offer = await getOfferItem(offer_item_id);
  const existing_notes = getTextValue(existing_offer, OFFER_FIELDS.notes, "");
  const status_note = `[${timestamp || nowIso()}] Offer status updated to ${normalized_status}.`;
  const combined_notes = [existing_notes, status_note, clean(notes)]
    .filter(Boolean)
    .join("\n");

  const payload = {
    [OFFER_FIELDS.offer_status]: normalized_status,
    ...buildTimestampFields(normalized_status, timestamp),
    ...(combined_notes ? { [OFFER_FIELDS.notes]: combined_notes } : {}),
  };

  await updateOfferItem(offer_item_id, payload);
  const pipeline = await syncPipelineState({
    offer_item_id,
    notes: status_note,
  });

  return {
    ok: true,
    offer_item_id,
    status: normalized_status,
    payload,
    pipeline,
  };
}

export default updateOfferStatus;
