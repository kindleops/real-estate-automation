// ─── maybe-upsert-underwriting-from-inbound.js ───────────────────────────
import { extractUnderwritingSignals } from "@/lib/domain/underwriting/extract-underwriting-signals.js";
import { selectUnderwritingStrategy } from "@/lib/domain/underwriting/select-underwriting-strategy.js";
import { upsertUnderwritingFromContext } from "@/lib/domain/underwriting/upsert-underwriting-from-context.js";

export async function maybeUpsertUnderwritingFromInbound({
  context = null,
  classification = null,
  route = null,
  message = "",
  offer_item_id = null,
  pipeline_item_id = null,
  source_channel = "SMS",
  notes = "",
} = {}) {
  if (!context?.found) {
    return {
      ok: false,
      created: false,
      updated: false,
      extracted: false,
      reason: "context_not_found",
    };
  }

  const extraction = extractUnderwritingSignals({
    message,
    classification,
    route,
    context,
  });

  if (!extraction?.ok) {
    return {
      ok: false,
      created: false,
      updated: false,
      extracted: false,
      reason: extraction?.reason || "underwriting_extraction_failed",
    };
  }

  if (!extraction?.extracted) {
    return {
      ok: true,
      created: false,
      updated: false,
      extracted: false,
      reason: extraction?.reason || "no_meaningful_signals",
      signals: extraction?.signals || {},
      strategy: null,
    };
  }

  const strategy = selectUnderwritingStrategy({
    context,
    signals: extraction.signals,
    classification,
    route,
    property_item: context?.items?.property_item || null,
  });

  const enriched_signals = {
    ...extraction.signals,
    underwriting_strategy: strategy?.strategy || null,
    underwriting_reason: strategy?.reason || null,
    underwriting_auto_offer_ready: strategy?.auto_offer_ready ?? null,
    underwriting_needs_manual_review: strategy?.needs_manual_review ?? null,
    property_type: strategy?.property_type || null,
  };

  const upsert_result = await upsertUnderwritingFromContext({
    context,
    signals: enriched_signals,
    offer_item_id,
    pipeline_item_id,
    source_channel,
    notes: notes || message,
  });

  return {
    ok: Boolean(upsert_result?.ok),
    created: Boolean(upsert_result?.created),
    updated: Boolean(upsert_result?.updated),
    extracted: true,
    reason: upsert_result?.reason || "underwriting_upsert_completed",
    underwriting_item_id: upsert_result?.underwriting_item_id || null,
    signals: enriched_signals,
    strategy,
    upsert_result,
  };
}

export default maybeUpsertUnderwritingFromInbound;