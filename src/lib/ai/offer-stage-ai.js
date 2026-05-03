import { log } from "@/lib/ai/opencode-zen-client.js";
import { callBigPickle } from "@/lib/ai/opencode-zen-client.js";
import { resolveOfferProfile } from "@/lib/ai/offer-profile.js";
import { checkOfferGate, evaluateSellerAsk, getSendMode, canAutoSend } from "@/lib/ai/offer-gate.js";
import { draftSellerReplyWithBigPickle } from "@/lib/ai/big-pickle-helpers.js";

const SEND_MODE = getSendMode();

export async function processOfferStage({ property, conversationHistory, sellerMessage, sellerName, phone, suppressionStatus, contactWindowStatus }) {
  log("OfferStageAI", "Processing offer stage", { sellerName, sendMode: SEND_MODE });

  const profile = resolveOfferProfile(property, { conversationHistory });

  const hasPriceIntent = await classifySellerPriceIntent(sellerMessage, conversationHistory);

  const gateResult = checkOfferGate({
    profile,
    sellerAsk: sellerMessage,
    hasPriceIntent,
    sendMode: SEND_MODE,
    suppressionStatus,
    contactWindowStatus,
  });

  if (!gateResult.passed) {
    log("OfferStageAI", "Offer gate blocked", { reasons: gateResult.blockedReasons });
    return handleBlockedOffer({ profile, gateResult, sellerMessage, sellerName, phone });
  }

  const sellerAskEval = evaluateSellerAsk({
    sellerAsk: sellerMessage,
    profile,
    sellerMessage,
  });

  if (sellerAskEval.route === "contract_path") {
    return handleContractPath({ profile, sellerAskEval, sellerName, phone });
  }

  if (sellerAskEval.route === "negotiation") {
    return handleNegotiation({ profile, sellerAskEval, sellerName, phone, sellerMessage });
  }

  if (sellerAskEval.route === "soft_exit") {
    return handleSoftExit({ profile, sellerAskEval, sellerName, phone });
  }

  if (profile.safe_to_reveal_offer && gateResult.can_reveal_offer) {
    return await draftOfferReveal({ profile, sellerName, phone, sellerMessage });
  }

  return await draftInfoRequest({ profile, sellerName, phone });
}

async function classifySellerPriceIntent(message, history) {
  if (!message) return false;

  const pricePatterns = [
    /\$?[\d,]+(?:\.\d{2})?/,
    /how much|what.*offer|give me|pay.*you|price.*you|your offer|best price/i,
  ];

  if (pricePatterns.some(p => p.test(message))) return true;

  const summary = await summarizeConversation(history);
  return summary?.price_intent === true;
}

async function summarizeConversation(history) {
  try {
    const messages = [
      { role: "system", content: `Summarize this conversation for offer intent. Return JSON: {"price_intent": boolean, "summary": "brief"}` },
      { role: "user", content: `Conversation: ${JSON.stringify(history || [])}` },
    ];

    const result = await callBigPickle(messages, { expectJson: true, temperature: 0 });
    return result;
  } catch {
    return null;
  }
}

function handleBlockedOffer({ profile, gateResult, sellerMessage, sellerName, phone }) {
  log("OfferGate", "Offer blocked, using fallback", { reasons: gateResult.blockedReasons });

  if (profile.next_seller_question) {
    return {
      action: "ask_question",
      message: profile.next_seller_question,
      profile,
      gateResult,
      sendMode: SEND_MODE,
      dry_run: true,
    };
  }

  return {
    action: "continue_conversation",
    message: "Thanks for the info. Let me review and get back to you shortly.",
    profile,
    gateResult,
    sendMode: SEND_MODE,
    dry_run: true,
  };
}

function handleContractPath({ profile, sellerAskEval, sellerName, phone }) {
  log("OfferStageAI", "Routing to contract path", { reason: sellerAskEval.reason });

  return {
    action: "contract_path",
    message: `Great! At $${profile.recommended_target_offer?.toLocaleString()}, we can move forward. I'll send over the paperwork.`,
    profile,
    sellerAskEval,
    sendMode: SEND_MODE,
    dry_run: SEND_MODE !== "auto_send_offer_high_confidence",
  };
}

function handleNegotiation({ profile, sellerAskEval, sellerName, phone, sellerMessage }) {
  log("OfferStageAI", "Routing to negotiation", { reason: sellerAskEval.reason });

  return {
    action: "negotiation",
    message: `That's a bit higher than I can go, but I might have some flexibility on terms or closing timeline. What matters most to you - price, speed, or terms?`,
    profile,
    sellerAskEval,
    sendMode: SEND_MODE,
    dry_run: true,
  };
}

function handleSoftExit({ profile, sellerAskEval, sellerName, phone }) {
  log("OfferStageAI", "Routing to soft exit/nurture", { reason: sellerAskEval.reason });

  return {
    action: "soft_exit",
    message: "I appreciate you sharing that. It's a bit outside what I can do on this one, but I'll keep you in mind for future deals.",
    profile,
    sellerAskEval,
    sendMode: SEND_MODE,
    dry_run: true,
  };
}

async function draftOfferReveal({ profile, sellerName, phone, sellerMessage }) {
  log("OfferDraft", "Drafting offer reveal", { assetType: profile.asset_type });

  const opening = profile.recommended_opening_offer;
  const target = profile.recommended_target_offer;

  let draftMessage = "";

  if (profile.asset_type === "single_family") {
    draftMessage = `Based on what I'm seeing, I'd probably be around $${opening?.toLocaleString()}-$${target?.toLocaleString()} cash as-is depending on condition/title. Is that close enough to keep talking?`;
  } else if (profile.asset_type === "2_to_4_unit") {
    draftMessage = `For this ${profile.asset_type === "2_to_4_unit" ? "2-4 unit" : ""} property, I'm looking at an offer around $${opening?.toLocaleString()}-$${target?.toLocaleString()}. Does that work for you?`;
  } else {
    if (profile.price_per_unit_range) {
      draftMessage = `Based on the NOI and ${profile.asset_type} metrics, I'm in the range of ${profile.price_per_unit_range} per unit. That puts me around $${opening?.toLocaleString()}-$${target?.toLocaleString()} total.`;
    } else {
      draftMessage = `I'm looking at an offer in the $${opening?.toLocaleString()}-$${target?.toLocaleString()} range for this property.`;
    }
  }

  const autoSend = canAutoSend(SEND_MODE, profile.offer_confidence_score);

  return {
    action: "offer_reveal",
    message: draftMessage,
    profile,
    sendMode: SEND_MODE,
    dry_run: !autoSend,
    auto_send: autoSend,
  };
}

async function draftInfoRequest({ profile, sellerName, phone }) {
  log("OfferDraft", "Drafting info request", { missing: profile.missing_required_info });

  const message = profile.next_seller_question || "I need a bit more info to give you a solid number. Can you help with that?";

  return {
    action: "request_info",
    message,
    profile,
    sendMode: SEND_MODE,
    dry_run: true,
  };
}

export { classifySellerPriceIntent, summarizeConversation };
