// ─── maybe-create-offer-from-context.js ──────────────────────────────────
import { createOffer } from "@/lib/domain/offers/create-offer.js";
import { selectOfferStrategy } from "@/lib/domain/offers/select-offer-strategy.js";
import { getCategoryValue, getNumberValue } from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function includesAny(text, needles = []) {
  const normalized = lower(text);
  return needles.some((needle) => normalized.includes(lower(needle)));
}

function shouldCreateOffer({
  classification = null,
  route = null,
  context = null,
  message = "",
} = {}) {
  const objection = classification?.objection || null;
  const emotion = classification?.emotion || null;
  const stage = route?.stage || context?.summary?.conversation_stage || "Ownership";
  const use_case = route?.use_case || null;
  const msg = clean(message);

  if (classification?.compliance_flag === "stop_texting") {
    return { should_create: false, reason: "compliance_stop" };
  }

  if (stage === "Contract") {
    return { should_create: false, reason: "already_past_offer_stage" };
  }

  if (use_case === "offer_reveal") {
    return { should_create: true, reason: "route_offer_reveal" };
  }

  if (
    objection === "send_offer_first" ||
    objection === "need_more_money" ||
    objection === "has_other_buyer" ||
    objection === "wants_retail" ||
    objection === "wants_written_offer" ||
    objection === "wants_proof_of_funds"
  ) {
    return { should_create: true, reason: "offer_objection_signal" };
  }

  if (emotion === "motivated") {
    return { should_create: true, reason: "motivated_signal" };
  }

  if (
    includesAny(msg, [
      "send me an offer",
      "what's your offer",
      "what is your offer",
      "how much can you pay",
      "what can you pay",
      "cash offer",
      "give me a number",
      "send it in writing",
      "written offer",
    ])
  ) {
    return { should_create: true, reason: "message_offer_signal" };
  }

  return { should_create: false, reason: "not_offer_ready" };
}

export async function maybeCreateOfferFromContext({
  context = null,
  classification = null,
  route = null,
  message = "",
  tags = [],
  notes = "",
  unit_count = null,
  requested_strategy = null,
  allow_creative = false,
  allow_novation = false,
  explicit_offer_amount = null,
  offer_status = "Offer Sent",
  offer_id = null,
  offer_label = null,
  created_by = "AI Offer Engine",
  respect_underwriting_gate = true,
} = {}) {
  if (!context?.found) {
    return {
      ok: false,
      created: false,
      reason: "context_not_found",
    };
  }

  const property_item = context?.items?.property_item || null;
  const property_id = context?.ids?.property_id || null;

  if (!property_id && !property_item) {
    return {
      ok: false,
      created: false,
      reason: "missing_property_context",
    };
  }

  const decision = shouldCreateOffer({
    classification,
    route,
    context,
    message,
  });

  if (!decision.should_create) {
    return {
      ok: true,
      created: false,
      reason: decision.reason,
    };
  }

  const summary = context?.summary || {};

  const property_type =
    getCategoryValue(property_item, "property-type", null) ||
    summary.primary_category ||
    "Residential";

  const motivation_score =
    classification?.motivation_score ??
    summary.motivation_score ??
    null;

  const seller_profile =
    summary.seller_profile ||
    null;

  const strategy_preview = selectOfferStrategy({
    property_type,
    seller_profile,
    motivation_score,
    tags: safeArray(tags),
    notes: notes || message || "",
    unit_count,
    requested_strategy,
    has_sfr_cash_offer: getNumberValue(property_item, "smart-cash-offer-2", null) !== null,
    has_multifamily_cash_offer:
      getNumberValue(property_item, "smart-cash-offer-2", null) !== null,
    allow_creative,
    allow_novation,
  });

  if (respect_underwriting_gate && strategy_preview?.flags?.needs_underwriting_flow) {
    return {
      ok: true,
      created: false,
      reason: "offer_requires_underwriting_first",
      strategy_result: strategy_preview,
    };
  }

  const result = await createOffer({
    context,
    property_item,
    property_type,
    seller_profile,
    motivation_score,
    tags: safeArray(tags),
    notes: notes || message || "",
    unit_count,
    requested_strategy,
    allow_creative,
    allow_novation,
    explicit_offer_amount,
    offer_status,
    offer_id,
    offer_label,
    created_by,
  });

  return {
    ok: true,
    created: Boolean(result?.offer_item_id),
    reason: result?.offer_item_id ? decision.reason : result?.reason || "offer_not_created",
    offer: result,
    strategy_result: strategy_preview,
  };
}

export default maybeCreateOfferFromContext;
