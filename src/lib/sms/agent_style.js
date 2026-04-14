// ─── agent_style.js ───────────────────────────────────────────────────────
// Normalize any agent persona / archetype into one of the four CSV-facing
// Agent Style Fit values:
//   - Investor Direct
//   - Warm Professional
//   - Neutral
//   - Buyer / Local Buyer

const VALID_STYLES = Object.freeze([
  "Investor Direct",
  "Warm Professional",
  "Neutral",
  "Buyer / Local Buyer",
]);

const VALID_STYLE_SET = new Set(VALID_STYLES.map((s) => s.toLowerCase()));

// Persona family → CSV Agent Style Fit mapping
const PERSONA_FAMILY_MAP = Object.freeze({
  "no-nonsense closer": "Investor Direct",
  "hard closer": "Investor Direct",
  "soft closer": "Warm Professional",
  "empathetic": "Warm Professional",
  "neighborly": "Warm Professional",
  "market-local": "Buyer / Local Buyer",
  "fallback": "Buyer / Local Buyer",
  "specialist-close": "Investor Direct",
  "specialist-landlord": "Warm Professional",
});

const DEFAULT_STYLE = "Warm Professional";

function lc(val) {
  return String(val ?? "").toLowerCase().trim();
}

/**
 * Resolve the CSV-facing Agent Style Fit using priority order:
 *   1. Explicit mapped style on the assigned agent
 *   2. Agent archetype / profile family mapping
 *   3. Conversation tone override
 *   4. default = Warm Professional
 *
 * @param {object} context
 * @param {string} [context.agent_style] - Direct agent style value
 * @param {string} [context.agent_archetype] - Agent persona family name
 * @param {string} [context.agent_family] - Agent family/archetype
 * @param {string} [context.conversation_tone] - Tone override from brain
 * @returns {string} One of the 4 valid Agent Style Fit values
 */
export function normalizeAgentStyleFit(context = {}) {
  // 1. Explicit mapped style
  const explicit = lc(context.agent_style);
  if (explicit && VALID_STYLE_SET.has(explicit)) {
    return VALID_STYLES.find((s) => s.toLowerCase() === explicit);
  }

  // Handle CSV raw values that match directly
  if (explicit === "investor direct") return "Investor Direct";
  if (explicit === "warm professional") return "Warm Professional";
  if (explicit === "neutral") return "Neutral";
  if (explicit === "buyer / local buyer" || explicit === "buyer/local buyer" || explicit === "local buyer") return "Buyer / Local Buyer";

  // 2. Agent archetype / profile family
  const archetype = lc(context.agent_archetype || context.agent_family);
  if (archetype) {
    // Try direct match first
    const mapped = PERSONA_FAMILY_MAP[archetype];
    if (mapped) return mapped;

    // Try partial match — check if any key is a substring
    for (const [key, value] of Object.entries(PERSONA_FAMILY_MAP)) {
      if (archetype.includes(key)) return value;
    }
  }

  // 3. Conversation tone override
  const tone = lc(context.conversation_tone);
  if (tone) {
    if (tone === "aggressive" || tone === "direct" || tone === "hard") return "Investor Direct";
    if (tone === "warm" || tone === "professional" || tone === "empathetic" || tone === "soft") return "Warm Professional";
    if (tone === "neutral" || tone === "balanced") return "Neutral";
    if (tone === "local" || tone === "buyer" || tone === "neighborly") return "Buyer / Local Buyer";
  }

  // 4. Default
  return DEFAULT_STYLE;
}

export { VALID_STYLES, PERSONA_FAMILY_MAP, DEFAULT_STYLE };

export default { normalizeAgentStyleFit, VALID_STYLES };
