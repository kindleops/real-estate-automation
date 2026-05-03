import { log } from "@/lib/ai/opencode-zen-client.js";

const CONFIDENCE_THRESHOLD = 0.7;

export function checkOfferGate({ profile, sellerAsk, hasPriceIntent, sendMode, suppressionStatus, contactWindowStatus }) {
  const checks = {
    suppression: { passed: !suppressionStatus || suppressionStatus === "allowed", reason: suppressionStatus },
    contact_window: { passed: !contactWindowStatus || contactWindowStatus === "allowed", reason: contactWindowStatus },
    seller_asked_price: { passed: hasPriceIntent || (profile && profile.safe_to_reveal_offer), reason: hasPriceIntent ? "seller_asked" : "no_price_intent" },
    confidence: { passed: profile ? profile.offer_confidence_score >= CONFIDENCE_THRESHOLD : false, reason: profile ? `confidence_${profile.offer_confidence_score}` : "no_profile" },
    missing_info: { passed: profile ? profile.missing_required_info.length === 0 : false, reason: profile ? `missing_${profile.missing_required_info.join(",")}` : "no_profile" },
    asset_type_valid: { passed: profile && profile.asset_type !== null, reason: profile ? profile.asset_type : "no_asset_type" },
    mao_exposure: { passed: true, reason: "checked_in_draft" },
  };

  const allPassed = Object.values(checks).every(c => c.passed);
  const blockedReasons = Object.entries(checks).filter(([, v]) => !v.passed).map(([k]) => k);

  log("OfferGate", "Offer gate check", { allPassed, blockedReasons });

  return {
    passed: allPassed,
    checks,
    blockedReasons,
    can_reveal_offer: allPassed && profile?.safe_to_reveal_offer,
    recommended_action: allPassed ? "proceed_to_offer" : "block_or_ask",
  };
}

export function evaluateSellerAsk({ sellerAsk, profile, sellerMessage }) {
  if (!sellerAsk || !profile) {
    return { route: "no_ask", action: "continue_conversation" };
  }

  const askAmount = parseDollarAmount(sellerAsk);

  if (askAmount === null) {
    return { route: "unclear_ask", action: "request_clarification" };
  }

  if (askAmount <= profile.recommended_target_offer) {
    return {
      route: "contract_path",
      action: "draft_acceptance",
      reason: `Seller ask $${askAmount} <= target $${profile.recommended_target_offer?.toLocaleString()}`,
    };
  }

  if (askAmount <= profile.walkaway_internal) {
    return {
      route: "negotiation",
      action: "ask_flexibility",
      reason: `Seller ask $${askAmount} <= walkaway $${profile.walkaway_internal?.toLocaleString()}, negotiate terms`,
    };
  }

  return {
    route: "soft_exit",
    action: "nurture_no_argue",
    reason: `Seller ask $${askAmount} > walkaway $${profile.walkaway_internal?.toLocaleString()}, too high`,
  };
}

function parseDollarAmount(text) {
  if (!text) return null;
  const match = text.match(/\$?([\d,]+(?:\.\d{2})?)/);
  if (!match) return null;
  return parseFloat(match[1].replace(/,/g, ""));
}

export function getSendMode(defaultMode = "dry_run_offer_ai") {
  const mode = process.env.OFFER_SEND_MODE || defaultMode;
  const validModes = ["dry_run_offer_ai", "auto_queue_offer", "auto_send_offer_high_confidence"];
  return validModes.includes(mode) ? mode : "dry_run_offer_ai";
}

export function canAutoSend(sendMode, confidence) {
  if (sendMode === "auto_send_offer_high_confidence" && confidence >= 0.85) {
    return true;
  }
  if (sendMode === "auto_queue_offer") {
    return true;
  }
  return false;
}
