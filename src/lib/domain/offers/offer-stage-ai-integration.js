import { log } from "@/lib/ai/opencode-zen-client.js";
import { processOfferStage } from "@/lib/ai/offer-stage-ai.js";
import { checkOfferGate } from "@/lib/ai/offer-gate.js";

const DRY_RUN_MODE = "dry_run_offer_ai";

export function isOfferStageTrigger({ message, classification, sellerStage, route }) {
  if (!message) return { triggered: false, reason: "no_message" };

  const msg = message.toLowerCase().trim();

  const pricePatterns = [
    /how much/i,
    /what.*offer/i,
    /send.*offer/i,
    /give me.*offer/i,
    /best price/i,
    /your offer/i,
    /counter.*offer/i,
    /\$[\d,]+(?:\.\d{2})?/,  // dollar amount
  ];

  if (pricePatterns.some(p => p.test(msg))) {
    return { triggered: true, reason: "price_intent_detected" };
  }

  if (sellerStage === "offer_reveal" || sellerStage === "negotiation") {
    return { triggered: true, reason: `seller_stage_${sellerStage}` };
  }

  if (route?.use_case?.includes("offer") || route?.use_case?.includes("price")) {
    return { triggered: true, reason: `route_use_case_${route.use_case}` };
  }

  if (classification?.intent === "price_inquiry" || classification?.intent === "offer_request") {
    return { triggered: true, reason: "classification_price_intent" };
  }

  return { triggered: false, reason: "no_offer_trigger" };
}

export async function runOfferStageAI({
  message,
  property,
  conversationHistory = [],
  sellerName,
  phone,
  sellerStage,
  suppressionStatus = "allowed",
  contactWindowStatus = "allowed",
  existingProfile = null,
}) {
  log("OfferStageAI", "Running offer stage AI in dry-run mode", {
    sellerName,
    triggered: true,
    sellerStage,
  });

  try {
    const result = await processOfferStage({
      property,
      conversationHistory,
      sellerMessage: message,
      sellerName,
      phone,
      suppressionStatus,
      contactWindowStatus,
    });

    const offerResult = {
      triggered: true,
      trigger_reason: "offer_stage_ai_dry_run",
      asset_type: result.profile?.asset_type || null,
      recommended_opening_offer: result.profile?.recommended_opening_offer || null,
      target_contract: result.profile?.recommended_target_offer || null,
      walkaway_internal: result.profile?.walkaway_internal || null,
      offer_confidence_score: result.profile?.offer_confidence_score || 0,
      safe_to_reveal_offer: result.profile?.safe_to_reveal_offer || false,
      missing_required_info: result.profile?.missing_required_info || [],
      draft_message: result.message || null,
      send_mode: result.sendMode || DRY_RUN_MODE,
      would_queue: result.sendMode !== "dry_run_offer_ai" && !result.dry_run,
      would_auto_send: result.auto_send || false,
      blocked_reason: result.gateResult?.blockedReasons?.join(",") || null,
      action: result.action || "unknown",
      route: result.sellerAskEval?.route || null,
      timestamp: new Date().toISOString(),
    };

    log("OfferStageAI", "Offer stage AI result", offerResult);

    return {
      ok: true,
      dry_run: true,
      offer_ai_result: offerResult,
      message_preview: result.message,
      blocked: result.gateResult ? !result.gateResult.passed : false,
      blocked_reasons: result.gateResult?.blockedReasons || [],
    };
  } catch (err) {
    log("OfferStageAI", "Offer stage AI failed", { error: err.message });

    return {
      ok: false,
      dry_run: true,
      error: err.message,
      offer_ai_result: null,
    };
  }
}

export function buildOfferStageMetadata(offerResult) {
  if (!offerResult?.offer_ai_result) return {};

  const r = offerResult.offer_ai_result;

  return {
    offer_stage_ai_triggered: r.triggered,
    offer_stage_ai_reason: r.trigger_reason,
    offer_stage_ai_asset_type: r.asset_type,
    offer_stage_ai_opening_offer: r.recommended_opening_offer,
    offer_stage_ai_target_offer: r.target_contract,
    offer_stage_ai_confidence: r.offer_confidence_score,
    offer_stage_ai_safe_to_reveal: r.safe_to_reveal_offer,
    offer_stage_ai_missing_info: JSON.stringify(r.missing_required_info),
    offer_stage_ai_draft: r.draft_message,
    offer_stage_ai_send_mode: r.send_mode,
    offer_stage_ai_would_queue: r.would_queue,
    offer_stage_ai_blocked_reason: r.blocked_reason,
    offer_stage_ai_action: r.action,
    offer_stage_ai_route: r.route,
    offer_stage_ai_timestamp: r.timestamp,
  };
}

export function shouldSkipOfferStageAI({ suppressionStatus, contactWindowStatus }) {
  if (suppressionStatus && suppressionStatus !== "allowed") {
    return { skip: true, reason: `suppression_${suppressionStatus}` };
  }
  if (contactWindowStatus && contactWindowStatus !== "allowed") {
    return { skip: true, reason: `contact_window_${contactWindowStatus}` };
  }
  return { skip: false, reason: null };
}
