// ─── select-underwriting-strategy.js ─────────────────────────────────────
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

function asNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizePropertyType(value = "") {
  const text = lower(value);

  if (
    includesAny(text, [
      "multifamily",
      "multi family",
      "apartment",
      "apartments",
      "5+ unit",
      "commercial multifamily",
    ])
  ) {
    return "Multifamily";
  }

  if (
    includesAny(text, [
      "condo",
      "townhome",
      "townhouse",
      "single family",
      "sfr",
      "residential",
      "mobile home",
      "manufactured",
    ])
  ) {
    return "Residential";
  }

  if (
    includesAny(text, [
      "commercial",
      "retail",
      "office",
      "industrial",
      "mixed use",
      "warehouse",
      "self storage",
    ])
  ) {
    return "Commercial";
  }

  if (
    includesAny(text, [
      "land",
      "lot",
      "vacant land",
      "acreage",
    ])
  ) {
    return "Land";
  }

  return clean(value) || "Residential";
}

function derivePropertyType({ property_item = null, context = null, route = null } = {}) {
  return normalizePropertyType(
    getCategoryValue(property_item, "property-type", null) ||
      context?.summary?.property_type ||
      route?.primary_category ||
      "Residential"
  );
}

function deriveUnitCount({ signals = {}, property_item = null } = {}) {
  return (
    asNumber(signals.unit_count) ??
    getNumberValue(property_item, "number-of-units", null) ??
    null
  );
}

function deriveOccupancyStatus({ signals = {}, property_item = null } = {}) {
  return (
    clean(signals.occupancy_status) ||
    clean(getCategoryValue(property_item, "occupancy-status", null)) ||
    null
  );
}

function deriveConditionLevel({ signals = {}, property_item = null } = {}) {
  const rehab_level = clean(getCategoryValue(property_item, "rehab-level", null)).toLowerCase();
  const building_condition = clean(
    getCategoryValue(property_item, "building-condition", null)
  ).toLowerCase();

  if (rehab_level === "full rehab" || rehab_level === "structural") return "Heavy";
  if (rehab_level === "moderate") return "Moderate";
  if (rehab_level === "cosmetic") return "Light";

  if (["unsound", "poor", "fair"].includes(building_condition)) return "Heavy";
  if (building_condition === "average") return "Moderate";
  if (["good", "very good", "excellent"].includes(building_condition)) return "Light";

  return (
    clean(signals.condition_level) ||
    null
  );
}

function deriveMotivationScore({ signals = {}, classification = null, context = null } = {}) {
  return (
    asNumber(signals.motivation_score) ??
    asNumber(classification?.motivation_score) ??
    asNumber(context?.summary?.motivation_score) ??
    null
  );
}

function computeFlags({
  property_type,
  unit_count,
  signals = {},
  route = null,
  classification = null,
  context = null,
  occupancy_status = null,
  distress_tags = [],
}) {
  const objection = classification?.objection || null;
  const emotion = classification?.emotion || null;

  const is_multifamily =
    property_type === "Multifamily" ||
    (unit_count !== null && unit_count >= 2) ||
    route?.is_multifamily_like === true;

  const has_creative_signal =
    signals.creative_terms_interest === true ||
    route?.needs_creative_review === true ||
    objection === "need_more_money" ||
    objection === "wants_retail";

  const has_novation_signal =
    signals.novation_interest === true ||
    includesAny(route?.next_move || "", ["creative"]) === false &&
      (objection === "wants_retail" || objection === "has_other_buyer");

  const has_probate_signal =
    distress_tags.includes("Probate") ||
    objection === "probate" ||
    clean(context?.summary?.seller_profile) === "Probate";

  const has_divorce_signal =
    distress_tags.includes("Divorce") ||
    objection === "divorce";

  const has_tenant_signal =
    signals.tenant_present === true ||
    occupancy_status === "Tenant Occupied" ||
    distress_tags.includes("Tenant Distress") ||
    objection === "tenant_issue" ||
    emotion === "tired_landlord";

  const has_distress_signal =
    distress_tags.includes("Foreclosure") ||
    distress_tags.includes("Payment Distress") ||
    distress_tags.includes("Tax Distress") ||
    objection === "financial_distress";

  return {
    is_multifamily,
    has_creative_signal,
    has_novation_signal,
    has_probate_signal,
    has_divorce_signal,
    has_tenant_signal,
    has_distress_signal,
  };
}

function pickStrategy({
  property_type,
  unit_count,
  occupancy_status,
  condition_level,
  motivation_score,
  flags,
}) {
  if (flags.has_probate_signal) {
    return {
      strategy: "probate_sensitive",
      reason: "probate_signal_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (flags.has_divorce_signal) {
    return {
      strategy: "divorce_sensitive",
      reason: "divorce_signal_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (flags.is_multifamily) {
    if (flags.has_creative_signal || flags.has_novation_signal) {
      return {
        strategy: "mf_creative_review",
        reason: "multifamily_creative_or_novation_signal",
        auto_offer_ready: false,
        needs_manual_review: true,
      };
    }

    return {
      strategy: "mf_auto_underwrite",
      reason: "multifamily_detected",
      auto_offer_ready: false,
      needs_manual_review: false,
    };
  }

  if (property_type === "Commercial") {
    return {
      strategy: "commercial_review",
      reason: "commercial_property_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (property_type === "Land") {
    return {
      strategy: "land_review",
      reason: "land_property_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (flags.has_creative_signal && flags.has_novation_signal) {
    return {
      strategy: "creative_or_novation_review",
      reason: "multiple_non_cash_signals_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (flags.has_creative_signal) {
    return {
      strategy: "creative_review",
      reason: "creative_signal_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (flags.has_novation_signal) {
    return {
      strategy: "novation_review",
      reason: "novation_signal_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (flags.has_tenant_signal) {
    return {
      strategy: "tenant_sensitive_cash_review",
      reason: "tenant_signal_detected",
      auto_offer_ready: false,
      needs_manual_review: true,
    };
  }

  if (flags.has_distress_signal && motivation_score !== null && motivation_score >= 70) {
    return {
      strategy: "cash_fast_close",
      reason: "high_distress_high_motivation",
      auto_offer_ready: true,
      needs_manual_review: false,
    };
  }

  if (condition_level === "Heavy") {
    return {
      strategy: "cash_heavy_rehab",
      reason: "heavy_condition_detected",
      auto_offer_ready: true,
      needs_manual_review: false,
    };
  }

  if (condition_level === "Moderate") {
    return {
      strategy: "cash_light_rehab",
      reason: "moderate_condition_detected",
      auto_offer_ready: true,
      needs_manual_review: false,
    };
  }

  if (
    occupancy_status === "Vacant" &&
    motivation_score !== null &&
    motivation_score >= 60
  ) {
    return {
      strategy: "cash_vacant_fast",
      reason: "vacant_and_motivated",
      auto_offer_ready: true,
      needs_manual_review: false,
    };
  }

  return {
    strategy: "cash_standard_sfr",
    reason: "default_residential_cash_path",
    auto_offer_ready: true,
    needs_manual_review: false,
  };
}

export function selectUnderwritingStrategy({
  context = null,
  signals = {},
  classification = null,
  route = null,
  property_item = null,
} = {}) {
  const property_type = derivePropertyType({
    property_item,
    context,
    route,
  });

  const unit_count = deriveUnitCount({
    signals,
    property_item,
  });

  const occupancy_status = deriveOccupancyStatus({
    signals,
    property_item,
  });

  const condition_level = deriveConditionLevel({
    signals,
    property_item,
  });

  const motivation_score = deriveMotivationScore({
    signals,
    classification,
    context,
  });

  const distress_tags = safeArray(signals.distress_tags);

  const flags = computeFlags({
    property_type,
    unit_count,
    signals,
    route,
    classification,
    context,
    occupancy_status,
    distress_tags,
  });

  const decision = pickStrategy({
    property_type,
    unit_count,
    occupancy_status,
    condition_level,
    motivation_score,
    flags,
  });

  return {
    ok: true,
    property_type,
    unit_count,
    occupancy_status,
    condition_level,
    motivation_score,
    distress_tags,
    flags,
    ...decision,
  };
}

export default selectUnderwritingStrategy;
