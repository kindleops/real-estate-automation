import { callBigPickle, buildSafeContext, isStale } from "@/lib/ai/opencode-zen-client.js";

function log(prefix, message, data = null) {
  const ts = new Date().toISOString();
  if (data) {
    console.log(`[${ts}] [${prefix}] ${message}`, data);
  } else {
    console.log(`[${ts}] [${prefix}] ${message}`);
  }
}

const CLASSIFY_SYSTEM = `You are a real estate SMS classification assistant. Analyze inbound SMS messages and classify intent.

Return ONLY valid JSON in this format:
{
  "intent": "interested|not_interested|maybe|info_request|opt_out|wrong_number|appointment_request|price_inquiry|other",
  "confidence": 0.0 to 1.0,
  "language": "English|Spanish|French|Other",
  "sentiment": "positive|neutral|negative|hostile",
  "requires_manual_review": true|false,
  "reasoning": "brief explanation"
}

Rules:
- Mark requires_manual_review=true for hostile, legal threats, or unclear intent
- Detect opt-out keywords: stop, unsubscribe, no more texts, etc.
- Identify language for routing to appropriate templates
- Be conservative with "interested" classification`;

export async function classifyInboundWithBigPickle({ message, sellerName, phone, propertyContext }) {
  log("BigPickleClassify", "Classifying inbound message", { sellerName, phone: phone ? "***" : "none" });

  const safeContext = buildSafeContext({
    message,
    propertyDetails: propertyContext,
    additionalContext: sellerName ? `Seller context: ${sellerName}` : undefined,
  });

  const messages = [
    { role: "system", content: CLASSIFY_SYSTEM },
    { role: "user", content: `Classify this inbound SMS:\n\n${safeContext}` },
  ];

  const result = await callBigPickle(messages, { expectJson: true, temperature: 0 });

  if (!result) {
    log("BigPickleClassify", "Classification failed, using deterministic fallback");
    return {
      intent: "other",
      confidence: 0,
      language: "English",
      sentiment: "neutral",
      requires_manual_review: true,
      reasoning: "Big Pickle classification failed, manual review required",
    };
  }

  const validIntents = ["interested", "not_interested", "maybe", "info_request", "opt_out", "wrong_number", "appointment_request", "price_inquiry", "other"];
  if (!validIntents.includes(result.intent)) {
    result.intent = "other";
  }

  if (typeof result.confidence !== "number" || result.confidence < 0 || result.confidence > 1) {
    result.confidence = 0.5;
  }

  log("BigPickleClassify", "Classification complete", {
    intent: result.intent,
    confidence: result.confidence,
    language: result.language,
  });

  return result;
}

const DRAFT_REPLY_SYSTEM = `You are a professional real estate acquisition assistant. Draft replies to seller SMS messages.

Return ONLY valid JSON in this format:
{
  "reply": "the draft reply message",
  "tone": "friendly|professional|empathetic|direct",
  "next_action": "send|wait|manual_review",
  "reasoning": "brief explanation of approach"
}

Rules:
- NEVER mention MAO, walkaway price, max offer, or internal calculations
- Keep replies under 160 characters when possible
- Be professional and courteous
- For opt-outs: acknowledge and confirm removal (no persuasion)
- For price inquiries: be vague, suggest a call to discuss
- For info requests: provide general info, avoid specifics
- Do not create legally binding offers
- Use proper grammar and spelling
- Include call-to-action when appropriate (call, text back, etc.)`;

export async function draftSellerReplyWithBigPickle({ message, sellerName, phone, propertyDetails, intent, language = "English" }) {
  log("BigPickleDraft", "Drafting reply", { intent, language, sellerName });

  const safeContext = buildSafeContext({
    sellerName,
    phone,
    message,
    propertyDetails,
    additionalContext: `Intent: ${intent}\nLanguage: ${language}`,
  });

  const messages = [
    { role: "system", content: DRAFT_REPLY_SYSTEM },
    { role: "user", content: `Draft a reply to this seller SMS:\n\n${safeContext}` },
  ];

  const result = await callBigPickle(messages, { expectJson: true, temperature: 0.3 });

  if (!result || !result.reply) {
    log("BigPickleDraft", "Draft failed, using fallback");
    return {
      reply: "Thanks for your message. We'll get back to you soon.",
      tone: "professional",
      next_action: "manual_review",
      reasoning: "Big Pickle draft failed, manual review required",
    };
  }

  if (result.reply.match(/MAO|walkaway|max offer|\$[\d,]+ for.*you/i)) {
    log("BigPickleDraft", "WARNING: Detected sensitive pricing in draft, redacting");
    result.reply = "Thanks for your message. Let's discuss the details over a call.";
    result.reasoning = "Redacted sensitive pricing information";
  }

  log("BigPickleDraft", "Draft complete", {
    replyLength: result.reply?.length,
    tone: result.tone,
    next_action: result.next_action,
  });

  return result;
}

const REENGAGEMENT_SYSTEM = `You are a real estate re-engagement specialist. Create re-engagement plans for stale leads.

Return ONLY valid JSON in this format:
{
  "should_reengage": true|false,
  "urgency": "high|medium|low|none",
  "recommended_template": "template_name",
  "timing": "immediate|3_days|1_week|2_weeks|1_month",
  "reasoning": "why this lead should/should not be re-engaged",
  "customization_notes": "any special considerations for this lead"
}

Rules:
- Only recommend re-engagement for leads with positive signals (previously interested, responded positively, etc.)
- Skip re-engagement for: opted-out, hostile, wrong numbers, explicitly not interested
- Consider time since last contact (stale = 30+ days)
- Suggest appropriate templates based on lead status
- Be conservative - when in doubt, don't re-engage`;

export async function resolveReengagementPlanWithBigPickle({ sellerName, phone, lastContactDate, leadStatus, previousInteractions, propertyDetails }) {
  log("BigPickleReengagement", "Resolving re-engagement plan", {
    sellerName,
    leadStatus,
    lastContactDate,
    isStale: isStale(lastContactDate),
  });

  const stale = isStale(lastContactDate);
  if (!stale) {
    return {
      should_reengage: false,
      urgency: "none",
      recommended_template: null,
      timing: null,
      reasoning: "Lead is not stale yet",
      customization_notes: null,
    };
  }

  const safeContext = buildSafeContext({
    sellerName,
    phone,
    propertyDetails,
    additionalContext: `Lead Status: ${leadStatus}\nLast Contact: ${lastContactDate}\nPrevious Interactions: ${previousInteractions}`,
  });

  const messages = [
    { role: "system", content: REENGAGEMENT_SYSTEM },
    { role: "user", content: `Create re-engagement plan for this stale lead:\n\n${safeContext}` },
  ];

  const result = await callBigPickle(messages, { expectJson: true, temperature: 0.2 });

  if (!result) {
    log("BigPickleReengagement", "Re-engagement plan failed, using fallback");
    return {
      should_reengage: false,
      urgency: "none",
      recommended_template: null,
      timing: null,
      reasoning: "Big Pickle re-engagement failed, manual review required",
      customization_notes: null,
    };
  }

  log("BigPickleReengagement", "Re-engagement plan complete", {
    should_reengage: result.should_reengage,
    urgency: result.urgency,
    timing: result.timing,
  });

  return result;
}

const UNDERWRITING_SYSTEM = `You are a real estate underwriting assistant. Summarize property details for internal review.

Return ONLY valid JSON in this format:
{
  "property_type": "single_family|multi_family|condo|townhouse|commercial|land|other",
  "estimated_value_range": "low|medium|high|unknown",
  "repair_needed": "light|moderate|heavy|unknown",
  "investment_potential": "excellent|good|fair|poor|unknown",
  "key_concerns": ["list", "of", "concerns"],
  "summary": "brief 2-3 sentence summary for internal team",
  "missing_info": ["list", "of", "missing", "data"]
}

Rules:
- Be objective and factual
- Identify missing critical information (rent rolls for multifamily, etc.)
- For multifamily without rent roll, always note missing rent/occupancy data
- Do not provide specific offer amounts or MAO calculations
- Flag properties needing significant repairs
- Note title issues, liens, or legal concerns if mentioned`;

export async function summarizeUnderwritingWithBigPickle({ propertyDetails, sellerName, phone, additionalData }) {
  log("BigPickleUnderwriting", "Summarizing underwriting", { sellerName });

  const safeContext = buildSafeContext({
    sellerName,
    phone,
    propertyDetails,
    additionalContext: additionalData ? `Additional Data: ${additionalData}` : undefined,
  });

  const messages = [
    { role: "system", content: UNDERWRITING_SYSTEM },
    { role: "user", content: `Summarize this property for underwriting:\n\n${safeContext}` },
  ];

  const result = await callBigPickle(messages, { expectJson: true, temperature: 0 });

  if (!result) {
    log("BigPickleUnderwriting", "Underwriting summary failed");
    return {
      property_type: "unknown",
      estimated_value_range: "unknown",
      repair_needed: "unknown",
      investment_potential: "unknown",
      key_concerns: [],
      summary: "Underwriting summary unavailable",
      missing_info: [],
    };
  }

  if (result.property_type === "multi_family" && !safeContext.includes("rent roll") && !safeContext.includes("rent/occupancy")) {
    result.missing_info = [...(result.missing_info || []), "rent roll", "occupancy rate"];
    result.key_concerns = [...(result.key_concerns || []), "Missing rent/occupancy data for multifamily"];
  }

  log("BigPickleUnderwriting", "Underwriting summary complete", {
    property_type: result.property_type,
    investment_potential: result.investment_potential,
  });

  return result;
}

export { buildSafeContext, isStale };
