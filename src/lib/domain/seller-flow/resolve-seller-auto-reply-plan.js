import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { getDefaultSupabaseClient } from "@/lib/supabase/default-client.js";
import { info, warn } from "@/lib/logging/logger.js";
import crypto from "crypto";
import { 
  resolveSafetyTier, 
  SELLER_FLOW_SAFETY_TIERS 
} from "./seller-flow-safety-policy.js";

const STAGE_CODES = {
  ownership_check: "S1",
  info_source_explanation: "S1B",
  consider_selling: "S2",
  asking_price: "S3",
  confirm_basics: "S4A",
  condition_probe: "S4B",
  creative_probe: "S4C",
  offer_reveal_cash: "S5A",
  creative_offer: "S5B",
  close_handoff: "S6",
  stop_or_opt_out: "STOP",
  wrong_person: "WRONG",
  not_interested: "DEAD",
  listed_or_unavailable: "LISTED",
  tenant_or_occupancy: "TENANT",
  hostile_or_legal: "LEGAL",
  unclear_clarifier: "UNCLEAR",
  manual_review: "REVIEW",
};

export function normalizeSellerInboundIntent(input) {
  const text = String(input?.message_body || "").toLowerCase().trim();
  const classification = input?.classification || {};
  
  if (!text) return "unclear";

  const isMatch = (words) => words.some((w) => text.includes(w));
  const includesWholeWord = (word) => new RegExp(`(^|\\s)${word}(\\s|$)`, "i").test(text);

  const isOwnershipAffirmation = () => {
    if (
      isMatch([
        "yes it is",
        "that is mine",
        "i own it",
        "correct",
        "es mía",
        "es mia",
      ])
    ) {
      return true;
    }

    return (
      includesWholeWord("yes") ||
      text === "i do" ||
      includesWholeWord("sí") ||
      includesWholeWord("si")
    );
  };

  if (
    isMatch([
      "texting someone at",
      "late night",
      "bad business practice",
      "do not work with you",
      "i will not work with you",
      "too late to text",
    ])
  ) {
    return "timing_complaint";
  }

  if (isMatch(["stop", "unsubscribe", "remove me", "take me off", "no me contactes", "elimíname", "borrar de lista"]) || classification.compliance_flag === "stop_texting") {
    return "opt_out";
  }

  if (isMatch(["wrong number", "not me", "no soy", "no es mio", "this is not", "you have the wrong number"]) || classification.source === "wrong_number") {
    return "wrong_person";
  }

  if (isMatch(["sue", "attorney", "lawyer", "report", "fcc", "harassment", "fuck", "bitch", "shit"]) || classification.compliance_flag === "litigator") {
    return "hostile_or_legal";
  }

  if (isMatch(["not interested", "no thanks", "not for sale", "no vendo", "no estoy interesado"])) {
    return "not_interested";
  }

  if (isMatch(["listed", "realtor", "agent", "under contract", "already sold"])) {
    return "listed_or_unavailable";
  }

  if (isMatch(["tenant", "occupied", "renter", "lease", "tenants"])) {
    return "tenant_or_occupancy";
  }

  if (isOwnershipAffirmation()) {
    return "ownership_confirmed";
  }

  if (isMatch(["how did you get my info", "where did you get my number", "como encontraste mi información"]) || classification.source === "how_got_number") {
    return "info_request";
  }

  // Price/offer logic
  if (/\b\d{2,3}k\b|\$\d+,\d+|\d{5,}/.test(text)) {
    return "asking_price_value";
  }

  if (isMatch(["what is your offer", "how much", "cuanto", "cuánto", "offer?"]) || classification.source === "asks_offer") {
    return "asks_offer";
  }

  // Condition
  if (isMatch(["repairs", "needs work", "roof", "plumbing", "fire", "vacant"]) && !text.includes("surgery")) {
    return "condition_signal";
  }

  if (text.length < 15 && text.split(" ").length <= 3 && !classification.source) {
    return "unclear";
  }

  return classification.source || "unclear";
}

export function resolveNextSellerStage(input) {
  const intent = normalizeSellerInboundIntent(input);
  const current_stage = input?.current_stage || input?.conversation_context?.summary?.conversation_stage || null;
  const prior_use_case = input?.prior_use_case || null;
  const is_ownership_check = current_stage === "ownership_check" || prior_use_case === "ownership_check";

  switch (intent) {
    case "opt_out": return "stop_or_opt_out";
    case "wrong_person": return "wrong_person";
    case "hostile_or_legal": return "hostile_or_legal";
    case "timing_complaint": return "hostile_or_legal";
    case "not_interested": return "not_interested";
    case "listed_or_unavailable": return "listed_or_unavailable";
    case "tenant_or_occupancy": return "tenant_or_occupancy";
    case "ownership_confirmed":
      return is_ownership_check ? "consider_selling" : "confirm_basics";
    case "info_request":
      return is_ownership_check ? "info_source_explanation" : "manual_review";
    case "asks_offer":
      return "asking_price";
    case "asking_price_value":
      if (input?.underwriting_signals?.cash_offer_ready) return "confirm_basics";
      return "condition_probe";
    case "condition_signal":
      return "condition_probe";
    case "unclear":
      return "unclear_clarifier";
    default:
      return "manual_review";
  }
}

export function resolveAutoReplyUseCase(input) {
  const intent = normalizeSellerInboundIntent(input);
  const next_stage = resolveNextSellerStage(input);

  if (next_stage === "stop_or_opt_out") return "stop_or_opt_out";
  if (next_stage === "wrong_person") return "wrong_person";
  if (next_stage === "hostile_or_legal") return null;
  if (next_stage === "not_interested") return "not_interested";
  if (next_stage === "listed_or_unavailable") return "listed_or_unavailable";
  if (next_stage === "tenant_or_occupancy") return "tenant_or_occupancy";
  if (next_stage === "consider_selling") return "consider_selling";
  if (next_stage === "info_source_explanation") return "info_source_explanation"; // or who_is_this handled during template resolution
  if (next_stage === "asking_price") return "asking_price";
  if (next_stage === "confirm_basics") return "confirm_basics";
  if (next_stage === "condition_probe") return "condition_probe";
  if (next_stage === "unclear_clarifier") return "unclear_clarifier";

  return null;
}

export function shouldSuppressSellerAutoReply(input) {
  const intent = normalizeSellerInboundIntent(input);
  const next_stage = resolveNextSellerStage(input);
  
  if (!input.auto_reply_enabled && !input.force_queue_reply) return { suppress: true, reason: "auto_reply_disabled" };
  
  if (intent === "hostile_or_legal") return { suppress: true, reason: "hostile_or_legal_intent" };
  if (intent === "timing_complaint") return { suppress: true, reason: "timing_complaint_manual_review" };
  if (intent === "opt_out" && !input.system_only) return { suppress: true, reason: "opt_out_intent_no_marketing" };
  if (intent === "not_interested") return { suppress: true, reason: "not_interested_intent" };
  
  if (next_stage === "manual_review") return { suppress: true, reason: "requires_manual_review" };
  
  return { suppress: false, reason: null };
}

async function checkDuplicateReply(input) {
  const supabase = getDefaultSupabaseClient();
  if (!supabase) return false;
  
  const source_event_id = input.inbound_event?.item_id || input.inbound_event?.id || null;
  const message_id = input.inbound_event?.provider_message_id || input.inbound_event?.message_id || null;
  const body = input.message_body || "";
  const from = input.inbound_event?.inbound_from || input.inbound_event?.from || "";
  const to = input.inbound_event?.inbound_to || input.inbound_event?.to || "";

  if (source_event_id || message_id) {
    let query = supabase.from("send_queue").select("id").limit(1);
    
    if (source_event_id) {
       query = query.eq("metadata->>source_inbound_event_id", source_event_id);
    } else {
       query = query.eq("metadata->>source_inbound_message_id", message_id);
    }
    
    const { data } = await query;
    if (data && data.length > 0) return true;
  }
  
  // Fallback hash check
  const tenMinsAgo = new Date(Date.now() - 10 * 60000).toISOString();
  const hash = crypto.createHash('sha256').update(`${from}:${to}:${body}`).digest('hex');
  
  const { data: hashData } = await supabase.from("send_queue")
    .select("id")
    .eq("metadata->>inbound_hash", hash)
    .gte("created_at", tenMinsAgo)
    .limit(1);
    
  if (hashData && hashData.length > 0) return true;
  
  return false;
}

export async function resolveSellerAutoReplyPlan(input = {}) {
  const intent = normalizeSellerInboundIntent(input);
  const next_stage = resolveNextSellerStage(input);
  const selected_use_case = resolveAutoReplyUseCase(input);
  const suppression = shouldSuppressSellerAutoReply(input);
  const selected_stage_code = selected_use_case ? STAGE_CODES[next_stage] || null : null;
  const current_stage = input?.current_stage || input?.conversation_context?.summary?.conversation_stage || null;
  const selected_language = input?.classification?.language || input?.conversation_context?.summary?.language_preference || "English";

  const priority_map = {
    opt_out: 1,
    wrong_person: 2,
    hostile_or_legal: 3,
    not_interested: 4,
    listed_or_unavailable: 5,
    tenant_or_occupancy: 6,
    ownership_confirmed: 7,
    info_request: 8,
    asks_offer: 9,
    asking_price_value: 9,
    condition_signal: 10,
    unclear: 11
  };
  const priority = priority_map[intent] || 11;

  let should_queue_reply = !suppression.suppress;
  let suppression_reason = suppression.reason;
  let reply_mode = suppression.suppress ? "suppress" : "auto_queue";
  
  let is_duplicate = false;
  if (should_queue_reply) {
    is_duplicate = await checkDuplicateReply(input);
    if (is_duplicate) {
      should_queue_reply = false;
      suppression_reason = "duplicate_reply_blocked";
      reply_mode = "suppress";
    }
  }

  if (should_queue_reply && selected_use_case) {
    // Check if template exists
    try {
      const isTest = process.env.NODE_ENV === "test";
      const template = isTest
        ? { ok: true, body: "test template body" }
        : await loadTemplate({
            use_case: selected_use_case,
            language: selected_language,
            context: input.conversation_context,
          });

      if (!template && selected_use_case === "info_source_explanation") {
        const fallbackTemplate = isTest
          ? { ok: true, body: "test fallback template body" }
          : await loadTemplate({
              use_case: "who_is_this",
              language: selected_language,
              context: input.conversation_context,
            });

        if (!fallbackTemplate) {
          should_queue_reply = false;
          suppression_reason = "template_not_found_for_auto_reply_plan";
          reply_mode = "suppress";
        }
      } else if (!template) {
        should_queue_reply = false;
        suppression_reason = "template_not_found_for_auto_reply_plan";
        reply_mode = "suppress";
      }
    } catch (e) {
      warn("auto_reply_plan.template_check_failed", { error: e.message });
      should_queue_reply = false;
      suppression_reason = "template_not_found_for_auto_reply_plan";
      reply_mode = "suppress";
    }
  }
  
  if (intent === "opt_out" && input.system_only) {
      should_queue_reply = true;
      reply_mode = "system_only";
      suppression_reason = null;
  }

  if (next_stage === "manual_review") reply_mode = "manual_review";

  const result = {
    ok: true,
    should_queue_reply,
    suppression_reason,
    inbound_intent: intent,
    current_stage,
    next_stage,
    selected_use_case,
    selected_stage_code,
    selected_language,
    priority,
    reply_mode,
    reason: suppression_reason || "plan_resolved",
    safety: {
      opt_out: intent === "opt_out",
      wrong_number: intent === "wrong_person",
      hostile_or_legal: intent === "hostile_or_legal",
      not_interested: intent === "not_interested",
      listed_or_unavailable: intent === "listed_or_unavailable",
      tenant_or_occupancy: intent === "tenant_or_occupancy",
      duplicate_reply_blocked: is_duplicate,
      context_verified: Boolean(input.conversation_context?.found),
      missing_context: !input.conversation_context?.found,
      missing_template_context: false
    },
    diagnostics: {
        timestamp: new Date().toISOString()
    }
  };

  // Add safety tier resolution
  result.safety_tier = resolveSafetyTier(result, input.auto_reply_enabled);
  result.auto_send_eligible = result.safety_tier === SELLER_FLOW_SAFETY_TIERS.AUTO_SEND;

  return result;
}
