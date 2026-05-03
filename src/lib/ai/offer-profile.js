import { log } from "@/lib/ai/opencode-zen-client.js";

const CONFIDENCE_THRESHOLD = 0.7;

export function resolveOfferProfile(property, conversationContext = {}) {
  log("OfferProfile", "Resolving offer profile", { assetType: property.asset_type });

  const assetType = normalizeAssetType(property.asset_type);
  const legacyCashOffer = property.legacy_cash_offer || null;

  let profile = {
    asset_type: assetType,
    valuation_method: null,
    legacy_cash_offer: legacyCashOffer,
    estimated_arv_low: null,
    estimated_arv_mid: null,
    estimated_arv_high: null,
    estimated_repairs_low: null,
    estimated_repairs_mid: null,
    estimated_repairs_high: null,
    current_gross_rent: null,
    current_noi: null,
    pro_forma_noi: null,
    market_cap_rate: null,
    price_per_unit_range: null,
    recommended_opening_offer: null,
    recommended_target_offer: null,
    walkaway_internal: null,
    offer_confidence_score: 0,
    missing_required_info: [],
    next_seller_question: null,
    safe_to_reveal_offer: false,
    offer_reason: null,
  };

  switch (assetType) {
    case "single_family":
      Object.assign(profile, resolveSingleFamilyProfile(property, legacyCashOffer));
      break;
    case "2_to_4_unit":
      Object.assign(profile, resolveTwoToFourUnitProfile(property, legacyCashOffer));
      break;
    case "multifamily_5_plus":
      Object.assign(profile, resolveMultifamilyProfile(property, legacyCashOffer));
      break;
    case "commercial":
    case "self_storage":
    case "retail":
      Object.assign(profile, resolveCommercialProfile(property, legacyCashOffer));
      break;
    default:
      log("OfferProfile", `Unknown asset type: ${assetType}, defaulting to single_family`);
      Object.assign(profile, resolveSingleFamilyProfile(property, legacyCashOffer));
  }

  profile.offer_confidence_score = calculateConfidence(profile, assetType);
  profile.safe_to_reveal_offer = profile.offer_confidence_score >= CONFIDENCE_THRESHOLD &&
    profile.missing_required_info.length === 0 &&
    profile.recommended_opening_offer !== null;

  if (!profile.safe_to_reveal_offer && profile.missing_required_info.length > 0) {
    profile.next_seller_question = buildMissingInfoQuestion(profile.missing_required_info, assetType);
  }

  log("OfferProfile", "Offer profile resolved", {
    asset_type: profile.asset_type,
    safe_to_reveal: profile.safe_to_reveal_offer,
    confidence: profile.offer_confidence_score,
  });

  return profile;
}

function normalizeAssetType(assetType) {
  if (!assetType) return "single_family";
  const normalized = assetType.toLowerCase().replace(/[_\s]/g, "_");
  const validTypes = ["single_family", "2_to_4_unit", "multifamily_5_plus", "commercial", "self_storage", "retail"];
  return validTypes.includes(normalized) ? normalized : "single_family";
}

function resolveSingleFamilyProfile(property, legacyCashOffer) {
  const arvLow = property.arv_low || (legacyCashOffer ? legacyCashOffer * 0.85 : null);
  const arvMid = property.arv_mid || legacyCashOffer;
  const arvHigh = property.arv_high || (legacyCashOffer ? legacyCashOffer * 1.15 : null);

  const repairsLow = property.repairs_low || 0;
  const repairsMid = property.repairs_mid || 0;
  const repairsHigh = property.repairs_high || 0;

  const walkaway = arvMid ? (arvMid * 0.7) - repairsMid : null;
  const target = walkaway ? walkaway * 0.95 : null;
  const opening = walkaway ? walkaway * 0.9 : null;

  const missing = [];
  if (!arvMid) missing.push("arv");
  if (!repairsMid && repairsMid !== 0) missing.push("repair_estimate");

  return {
    valuation_method: "arv_wholesale_formula",
    estimated_arv_low: arvLow,
    estimated_arv_mid: arvMid,
    estimated_arv_high: arvHigh,
    estimated_repairs_low: repairsLow,
    estimated_repairs_mid: repairsMid,
    estimated_repairs_high: repairsHigh,
    walkaway_internal: walkaway,
    recommended_target_offer: target,
    recommended_opening_offer: opening,
    missing_required_info: missing,
    offer_reason: `SFH: ARV $${arvMid?.toLocaleString()} * 0.7 - $${repairsMid?.toLocaleString()} repairs = MAO $${walkaway?.toLocaleString()}`,
  };
}

function resolveTwoToFourUnitProfile(property, legacyCashOffer) {
  const arvLow = property.arv_low || (legacyCashOffer ? legacyCashOffer * 0.85 : null);
  const arvMid = property.arv_mid || legacyCashOffer;
  const arvHigh = property.arv_high || (legacyCashOffer ? legacyCashOffer * 1.15 : null);
  const repairsMid = property.repairs_mid || 0;

  const grossRent = property.current_gross_rent || null;
  const capRate = property.market_cap_rate || 0.08;
  const rentBasedValue = grossRent && capRate ? (grossRent * 12 * 0.6) / capRate : null;

  let walkaway = null;
  if (arvMid && rentBasedValue) {
    walkaway = ((arvMid * 0.7 - repairsMid) + rentBasedValue) / 2;
  } else if (arvMid) {
    walkaway = arvMid * 0.7 - repairsMid;
  } else if (rentBasedValue) {
    walkaway = rentBasedValue;
  }

  const target = walkaway ? walkaway * 0.95 : null;
  const opening = walkaway ? walkaway * 0.9 : null;

  const missing = [];
  if (!arvMid && !rentBasedValue) missing.push("arv_or_rent_roll");
  if (!grossRent) missing.push("rent_roll");

  return {
    valuation_method: "hybrid_arv_rent",
    estimated_arv_low: arvLow,
    estimated_arv_mid: arvMid,
    estimated_arv_high: arvHigh,
    estimated_repairs_low: property.repairs_low || 0,
    estimated_repairs_mid: repairsMid,
    estimated_repairs_high: property.repairs_high || 0,
    current_gross_rent: grossRent,
    market_cap_rate: capRate,
    walkaway_internal: walkaway,
    recommended_target_offer: target,
    recommended_opening_offer: opening,
    missing_required_info: missing,
    offer_reason: `2-4 Unit: Hybrid ARV/Rent. ${grossRent ? `Rent-based: $${rentBasedValue?.toLocaleString()}` : "No rent data"}`,
  };
}

function resolveMultifamilyProfile(property, legacyCashOffer) {
  const units = property.number_of_units || null;
  const grossRent = property.current_gross_rent || null;
  const noi = property.current_noi || (grossRent ? grossRent * 12 * 0.5 : null);
  const proFormaNoi = property.pro_forma_noi || (noi ? noi * 1.1 : null);
  const capRate = property.market_cap_rate || 0.08;

  const price = noi && capRate ? noi / capRate : null;
  const pricePerUnit = price && units ? price / units : null;

  const walkaway = price ? price * 0.9 : null;
  const target = walkaway ? walkaway * 0.95 : null;
  const opening = walkaway ? walkaway * 0.9 : null;

  const missing = [];
  if (!grossRent) missing.push("rent_roll");
  if (!property.occupancy_rate) missing.push("occupancy_rate");
  if (!units) missing.push("number_of_units");

  return {
    valuation_method: "noi_cap_rate_price_per_unit",
    current_gross_rent: grossRent,
    current_noi: noi,
    pro_forma_noi: proFormaNoi,
    market_cap_rate: capRate,
    price_per_unit_range: pricePerUnit ? `$${Math.floor(pricePerUnit * 0.9).toLocaleString()}-$${Math.floor(pricePerUnit * 1.1).toLocaleString()}/unit` : null,
    walkaway_internal: walkaway,
    recommended_target_offer: target,
    recommended_opening_offer: opening,
    missing_required_info: missing,
    offer_reason: `Multifamily ${units} units: NOI $${noi?.toLocaleString()}/yr / ${capRate} cap = $${price?.toLocaleString()}`,
  };
}

function resolveCommercialProfile(property, legacyCashOffer) {
  return resolveMultifamilyProfile(property, legacyCashOffer);
}

function calculateConfidence(profile, assetType) {
  let score = 0;
  const hasOffer = profile.walkaway_internal !== null;
  const hasRequiredData = profile.missing_required_info.length === 0;

  if (hasOffer) score += 0.5;
  if (hasRequiredData) score += 0.3;

  if (assetType === "single_family" && profile.estimated_arv_mid) score += 0.2;
  if (["2_to_4_unit", "multifamily_5_plus"].includes(assetType) && profile.current_gross_rent) score += 0.2;

  return Math.min(score, 1.0);
}

function buildMissingInfoQuestion(missingInfo, assetType) {
  if (assetType === "multifamily_5_plus" || assetType === "commercial") {
    if (missingInfo.includes("rent_roll")) {
      return "To give you an accurate offer, I need the rent roll and occupancy rate. What are the average monthly rents and is the property fully occupied?";
    }
  }
  return "I need a bit more info to give you a solid number. Can you provide the missing details?";
}
