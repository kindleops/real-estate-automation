// ─── select-offer-strategy.js ────────────────────────────────────────────

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function toNumber(value, fallback = null) {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  return fallback;
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function includesAny(text, needles = []) {
  const normalized = lower(text);
  return needles.some((needle) => normalized.includes(lower(needle)));
}

function hasTag(tags = [], needles = []) {
  const normalized_tags = safeArray(tags).map((tag) => lower(tag));
  return needles.some((needle) => normalized_tags.includes(lower(needle)));
}

function deriveMotivationBand(score) {
  const value = toNumber(score, null);

  if (value === null) return "unknown";
  if (value >= 80) return "very_high";
  if (value >= 65) return "high";
  if (value >= 45) return "medium";
  if (value >= 25) return "low";
  return "very_low";
}

function deriveSignals({
  property_type = null,
  seller_profile = null,
  tags = [],
  notes = "",
  unit_count = null,
  has_sfr_cash_offer = false,
  has_multifamily_cash_offer = false,
  allow_creative = false,
  allow_novation = false,
} = {}) {
  const normalized_property_type = lower(property_type);
  const normalized_seller_profile = lower(seller_profile);
  const normalized_notes = lower(notes);
  const normalized_unit_count = toNumber(unit_count, 0) || 0;

  const is_probate =
    normalized_seller_profile === "probate" ||
    hasTag(tags, ["probate", "estate", "inherited", "trust"]) ||
    includesAny(normalized_notes, ["probate", "estate", "inherited", "trust"]);

  const is_tired_landlord =
    normalized_seller_profile === "tired landlord" ||
    hasTag(tags, ["tired landlord", "landlord", "tenant issue"]) ||
    includesAny(normalized_notes, ["tenant", "landlord", "occupied", "renter"]);

  const is_multifamily_like =
    normalized_property_type.includes("multifamily") ||
    normalized_property_type.includes("multi family") ||
    normalized_property_type.includes("apartment") ||
    normalized_property_type.includes("5+") ||
    normalized_property_type.includes("commercial multifamily") ||
    normalized_unit_count >= 5 ||
    hasTag(tags, ["multifamily", "multi-family", "apartments", "5+ units"]) ||
    includesAny(normalized_notes, ["5 unit", "5+ unit", "apartment building", "apartments"]);

  const is_distressed =
    hasTag(tags, [
      "vacant",
      "fire damage",
      "water damage",
      "mold",
      "foundation issues",
      "tax delinquent",
      "foreclosure",
      "code violations",
      "hoarder",
      "tear down",
      "condemned",
      "heavy repairs",
    ]) ||
    includesAny(normalized_notes, [
      "vacant",
      "fire damage",
      "water damage",
      "mold",
      "foundation",
      "foreclosure",
      "code violation",
      "hoarder",
      "tear down",
      "condemned",
      "as-is",
      "as is",
      "needs work",
      "major repairs",
      "heavy repairs",
    ]);

  const has_creative_signals =
    hasTag(tags, [
      "seller finance",
      "owner finance",
      "subject to",
      "subto",
      "terms",
      "lease option",
      "carry the note",
    ]) ||
    includesAny(normalized_notes, [
      "seller finance",
      "owner finance",
      "subject to",
      "subto",
      "terms",
      "lease option",
      "carry the note",
      "monthly payments",
      "take over payments",
    ]);

  const has_novation_signals =
    hasTag(tags, ["novation", "retail ready", "light rehab retail"]) ||
    includesAny(normalized_notes, [
      "novation",
      "list it",
      "on market",
      "retail buyer",
      "retail exit",
      "hotel price",
      "full market value",
    ]);

  return {
    is_probate,
    is_tired_landlord,
    is_multifamily_like,
    is_distressed,
    has_creative_signals,
    has_novation_signals,
    has_sfr_cash_offer: Boolean(has_sfr_cash_offer),
    has_multifamily_cash_offer: Boolean(has_multifamily_cash_offer),
    allow_creative: Boolean(allow_creative),
    allow_novation: Boolean(allow_novation),
  };
}

export function selectOfferStrategy({
  property_type = "Residential",
  seller_profile = null,
  motivation_score = null,
  tags = [],
  notes = "",
  unit_count = null,
  requested_strategy = null,
  has_sfr_cash_offer = false,
  has_multifamily_cash_offer = false,
  allow_creative = false,
  allow_novation = false,
} = {}) {
  const motivation_band = deriveMotivationBand(motivation_score);

  const signals = deriveSignals({
    property_type,
    seller_profile,
    tags,
    notes,
    unit_count,
    has_sfr_cash_offer,
    has_multifamily_cash_offer,
    allow_creative,
    allow_novation,
  });

  const normalized_requested_strategy = lower(requested_strategy);

  if (normalized_requested_strategy) {
    return {
      strategy: normalized_requested_strategy,
      strategy_source: "manual_override",
      motivation_band,
      signals,
      flags: {
        use_existing_property_offer: normalized_requested_strategy.includes("cash"),
        needs_underwriting_flow:
          normalized_requested_strategy.includes("underwrite") ||
          normalized_requested_strategy.includes("creative") ||
          normalized_requested_strategy.includes("novation"),
        needs_human_review:
          normalized_requested_strategy.includes("creative") ||
          normalized_requested_strategy.includes("novation"),
      },
      rationale: [`Manual strategy override: ${requested_strategy}`],
    };
  }

  const rationale = [];
  const flags = {
    use_existing_property_offer: false,
    needs_underwriting_flow: false,
    needs_human_review: false,
  };

  if (signals.has_creative_signals || signals.allow_creative) {
    rationale.push("Creative finance signals detected.");
    flags.needs_underwriting_flow = true;
    flags.needs_human_review = true;

    return {
      strategy: "creative_underwrite",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.has_novation_signals || signals.allow_novation) {
    rationale.push("Novation / retail-exit signals detected.");
    flags.needs_underwriting_flow = true;
    flags.needs_human_review = true;

    return {
      strategy: "novation_underwrite",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.is_multifamily_like) {
    if (signals.has_multifamily_cash_offer) {
      rationale.push("Multifamily-like asset with existing MF cash offer available.");
      flags.use_existing_property_offer = true;
      flags.needs_underwriting_flow = true;

      return {
        strategy: "multifamily_underwrite",
        strategy_source: "rule_engine",
        motivation_band,
        signals,
        flags,
        rationale,
      };
    }

    rationale.push("Multifamily-like asset detected without trusted MF baseline.");
    flags.needs_underwriting_flow = true;
    flags.needs_human_review = true;

    return {
      strategy: "multifamily_underwrite",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.is_probate && signals.is_distressed) {
    rationale.push("Probate + distress signals detected.");
    flags.use_existing_property_offer = signals.has_sfr_cash_offer;

    return {
      strategy: "probate_distressed_cash",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.is_probate) {
    rationale.push("Probate signals detected.");
    flags.use_existing_property_offer = signals.has_sfr_cash_offer;

    return {
      strategy: "probate_cash",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.is_tired_landlord && signals.is_distressed) {
    rationale.push("Tired landlord + distress signals detected.");
    flags.use_existing_property_offer = signals.has_sfr_cash_offer;

    return {
      strategy: "landlord_distressed_cash",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.is_tired_landlord) {
    rationale.push("Tired landlord signals detected.");
    flags.use_existing_property_offer = signals.has_sfr_cash_offer;

    return {
      strategy: "landlord_cash",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.is_distressed && ["very_high", "high"].includes(motivation_band)) {
    rationale.push("Distress + high motivation detected.");
    flags.use_existing_property_offer = signals.has_sfr_cash_offer;

    return {
      strategy: "distressed_cash",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (signals.is_distressed) {
    rationale.push("General distress signals detected.");
    flags.use_existing_property_offer = signals.has_sfr_cash_offer;

    return {
      strategy: "distressed_cash",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  if (["very_high", "high"].includes(motivation_band)) {
    rationale.push("High seller motivation detected.");
    flags.use_existing_property_offer = signals.has_sfr_cash_offer;

    return {
      strategy: "motivated_cash",
      strategy_source: "rule_engine",
      motivation_band,
      signals,
      flags,
      rationale,
    };
  }

  rationale.push("Defaulting to standard residential cash path using existing property offer.");
  flags.use_existing_property_offer = signals.has_sfr_cash_offer;

  return {
    strategy: "standard_cash",
    strategy_source: "default",
    motivation_band,
    signals,
    flags,
    rationale,
  };
}

export default selectOfferStrategy;
