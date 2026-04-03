// ─── render-template.js ───────────────────────────────────────────────────

function normalizeWhitespace(value) {
  return String(value ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\t/g, " ")
    .replace(/[ ]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function countCharacters(value) {
  return String(value || "").length;
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && String(value).trim() !== "") {
      return String(value).trim();
    }
  }
  return "";
}

function buildVariableMap(context = {}, overrides = {}) {
  const summary = context?.summary || {};

  const owner_name = firstNonEmpty(
    overrides.owner_name,
    summary.owner_name
  );

  const property_address = firstNonEmpty(
    overrides.property_address,
    summary.property_address
  );

  const agent_name = firstNonEmpty(
    overrides.agent_name,
    summary.agent_name
  );

  const agent_first_name = firstNonEmpty(
    overrides.agent_first_name,
    summary.agent_first_name,
    agent_name ? agent_name.split(" ")[0] : ""
  );

  const market = firstNonEmpty(
    overrides.market,
    summary.market_name
  );

  const market_state = firstNonEmpty(
    overrides.market_state,
    summary.market_state
  );

  const property_city = firstNonEmpty(
    overrides.property_city,
    summary.property_city
  );

  const property_state = firstNonEmpty(
    overrides.property_state,
    summary.property_state
  );

  const seller_profile = firstNonEmpty(
    overrides.seller_profile,
    summary.seller_profile
  );

  const language = firstNonEmpty(
    overrides.language,
    summary.language_preference,
    "English"
  );

  const phone = firstNonEmpty(
    overrides.phone,
    summary.phone_hidden
  );

  const units = firstNonEmpty(
    overrides.units,
    summary.units
  );

  const occupancy = firstNonEmpty(
    overrides.occupancy,
    summary.occupancy_status
  );

  const avg_rent = firstNonEmpty(
    overrides.avg_rent,
    summary.avg_rent
  );

  const estimated_expenses = firstNonEmpty(
    overrides.estimated_expenses,
    summary.estimated_expenses
  );

  const target_net_to_seller = firstNonEmpty(
    overrides.target_net_to_seller,
    summary.target_net_to_seller
  );

  const conversation_stage = firstNonEmpty(
    overrides.conversation_stage,
    summary.conversation_stage
  );

  const ai_route = firstNonEmpty(
    overrides.ai_route,
    summary.brain_ai_route
  );

  return {
    owner_name,
    property_address,
    agent_name,
    agent_first_name,
    market,
    market_state,
    property_city,
    property_state,
    seller_profile,
    language,
    phone,
    units,
    occupancy,
    avg_rent,
    estimated_expenses,
    target_net_to_seller,
    conversation_stage,
    ai_route,

    // alias support
    seller_name: owner_name,
    first_name: firstNonEmpty(overrides.first_name, agent_first_name),
    city: property_city,
    state: property_state,
  };
}

function extractPlaceholders(template_text) {
  const text = String(template_text || "");
  const matches = [
    ...text.matchAll(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g),
    ...text.matchAll(/\{(?!\{)\s*([a-zA-Z0-9_]+)\s*\}(?!\})/g),
  ];
  return [...new Set(matches.map((m) => m[1]))];
}

export function renderTemplate({
  template_text,
  context = {},
  overrides = {},
  remove_unknown_placeholders = true,
} = {}) {
  const raw_template = String(template_text ?? "");
  if (!raw_template.trim()) {
    throw new Error("renderTemplate: template_text is empty");
  }

  const variables = buildVariableMap(context, overrides);
  const placeholders = extractPlaceholders(raw_template);

  let rendered = raw_template;
  const used_placeholders = [];
  const missing_placeholders = [];

  for (const key of placeholders) {
    const replacement = variables[key];
    const regexes = [
      new RegExp(`\\{\\{\\s*${escapeRegExp(key)}\\s*\\}\\}`, "g"),
      new RegExp(`\\{(?!\\{)\\s*${escapeRegExp(key)}\\s*\\}(?!\\})`, "g"),
    ];

    if (replacement && String(replacement).trim() !== "") {
      for (const regex of regexes) {
        rendered = rendered.replace(regex, String(replacement).trim());
      }
      used_placeholders.push(`{{${key}}}`);
    } else {
      missing_placeholders.push(`{{${key}}}`);

      if (remove_unknown_placeholders) {
        for (const regex of regexes) {
          rendered = rendered.replace(regex, "");
        }
      }
    }
  }

  rendered = normalizeWhitespace(rendered);

  return {
    ok: true,
    template_text: raw_template,
    rendered_text: rendered,
    character_count: countCharacters(rendered),
    used_placeholders,
    missing_placeholders,
    variables,
  };
}

export default renderTemplate;
