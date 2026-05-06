import { normalizeSellerInboundIntent } from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";

function clean(value) { return String(value ?? "").trim(); }
function configured(name) { return clean(process.env[name]); }

export function deterministicAiRoute({ use_case = "seller_intent_classification", input = {} } = {}) {
  const message = input.message || input.message_body || input.text || "";
  if (use_case === "seller_intent_classification") {
    const intent = normalizeSellerInboundIntent({ message_body: message, classification: input.classification || {} });
    return { intent, confidence: 0.72, source: "deterministic", safe_fallback_reply: "Got it — are you open to selling it if the numbers made sense?" };
  }
  if (use_case === "reply_quality_check") return { approved: true, risk: "low", source: "deterministic" };
  if (use_case === "number_verification_scoring") return { score: clean(input.phone).length >= 10 ? 0.7 : 0.2, source: "deterministic" };
  if (use_case === "offer_generation") return { ok: false, reason: "no_ai_provider_configured", source: "deterministic" };
  return { ok: true, source: "deterministic" };
}

export function getAiProviderPriority() {
  return [
    configured("OPENCODE_ZEN_API_KEY") || configured("BIG_PICKLE_API_KEY") ? "opencode_zen_big_pickle" : null,
    configured("GROQ_API_KEY") ? "groq" : null,
    configured("GEMINI_API_KEY") || configured("GOOGLE_GENERATIVE_AI_API_KEY") ? "gemini" : null,
    configured("OPENROUTER_API_KEY") ? "openrouter" : null,
    configured("OLLAMA_BASE_URL") ? "ollama" : null,
  ].filter(Boolean);
}

export async function routeAiRequest(payload = {}) {
  const providers = getAiProviderPriority();
  if (!providers.length) return { ok: true, provider: "deterministic", result: deterministicAiRoute(payload) };
  // Provider adapters intentionally stay server-only; until model-specific prompts are configured,
  // deterministic routing remains the safe non-blocking behavior.
  return { ok: true, provider: providers[0], provider_configured: true, result: deterministicAiRoute(payload), fallback_used: true };
}
