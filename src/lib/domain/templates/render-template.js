import { normalizeSellerFlowUseCase } from "@/lib/domain/seller-flow/canonical-seller-flow.js";

export const ALLOWED_TEMPLATE_PLACEHOLDERS = Object.freeze(
  new Set([
    "seller_first_name",
    "agent_first_name",
    "property_address",
    "property_city",
    "offer_price",
    "repair_cost",
    "unit_count",
    "occupied_units",
    "monthly_rents",
    "monthly_expenses",
  ])
);

const LEGACY_PLACEHOLDER_ALIASES = Object.freeze({
  owner_name: "seller_first_name",
  seller_name: "seller_first_name",
  agent_name: "agent_first_name",
  market: "property_city",
  city: "property_city",
  street_address: "property_address",
  units: "unit_count",
  occupancy: "occupied_units",
  avg_rent: "monthly_rents",
  estimated_expenses: "monthly_expenses",
  target_net_to_seller: "offer_price",
  smart_cash_offer_display: "offer_price",
  first_name: "seller_first_name",
});

function normalizeWhitespace(value) {
  return String(value ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\t/g, " ")
    .replace(/[ ]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

// Fixes punctuation/spacing artifacts that survive placeholder substitution:
//  - removes spaces immediately before sentence-ending punctuation ("Helen ." → "Helen.")
//  - normalizes em-dash and en-dash spacing to exactly one space on each side
//  - collapses any double spaces introduced by the above passes
// Exported so it can be unit-tested independently.
export function cleanupPunctuation(value) {
  return String(value ?? "")
    .replace(/\s+([.,!?:;])/g, "$1")
    .replace(/\s*—\s*/g, " — ")
    .replace(/\s*–\s*/g, " – ")
    .replace(/  +/g, " ")
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

export function buildVariableMap(context = {}, overrides = {}) {
  const summary = context?.summary || {};

  const seller_first_name = firstNonEmpty(
    overrides.seller_first_name,
    summary.seller_first_name,
    summary.owner_name ? String(summary.owner_name).trim().split(" ")[0] : ""
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

  const property_city = firstNonEmpty(
    overrides.property_city,
    summary.property_city
  );

  const offer_price = firstNonEmpty(
    overrides.offer_price,
    overrides.smart_cash_offer_display,
    summary.offer_price,
    summary.smart_cash_offer_display
  );

  const repair_cost = firstNonEmpty(
    overrides.repair_cost,
    overrides.estimated_repair_cost,
    summary.repair_cost,
    summary.estimated_repair_cost
  );

  const unit_count = firstNonEmpty(
    overrides.unit_count,
    overrides.units,
    summary.unit_count,
    summary.units
  );

  const occupied_units = firstNonEmpty(
    overrides.occupied_units,
    summary.occupied_units
  );

  const monthly_rents = firstNonEmpty(
    overrides.monthly_rents,
    overrides.current_gross_rents,
    summary.monthly_rents,
    summary.current_gross_rents,
    summary.avg_rent
  );

  const monthly_expenses = firstNonEmpty(
    overrides.monthly_expenses,
    overrides.estimated_expenses,
    summary.monthly_expenses,
    summary.estimated_expenses
  );

  return {
    seller_first_name,
    agent_first_name,
    property_address,
    property_city,
    offer_price,
    repair_cost,
    unit_count,
    occupied_units,
    monthly_rents,
    monthly_expenses,

    // Legacy aliases stay render-compatible but are not valid for new inventory selection.
    owner_name: seller_first_name,
    seller_name: seller_first_name,
    agent_name: agent_first_name,
    street_address: property_address,
    city: property_city,
    market: property_city,
    units: unit_count,
    occupancy: occupied_units,
    avg_rent: monthly_rents,
    estimated_expenses: monthly_expenses,
    smart_cash_offer_display: offer_price,
    first_name: seller_first_name,
  };
}

export function extractPlaceholders(template_text) {
  const text = String(template_text || "");
  const matches = [
    ...text.matchAll(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g),
    ...text.matchAll(/\{(?!\{)\s*([a-zA-Z0-9_]+)\s*\}(?!\})/g),
  ];
  return [...new Set(matches.map((match) => match[1]))];
}

function placeholderAllowedForUseCase(placeholder = "", use_case = null) {
  const normalized_placeholder = LEGACY_PLACEHOLDER_ALIASES[placeholder] || placeholder;
  const normalized_use_case = normalizeSellerFlowUseCase(use_case);

  if (!ALLOWED_TEMPLATE_PLACEHOLDERS.has(normalized_placeholder)) {
    return false;
  }

  if (
    normalized_placeholder === "agent_first_name" &&
    ![
      "ownership_check",
      "ownership_check_follow_up",
      "consider_selling_follow_up",
      "asking_price_follow_up",
      "who_is_this",
      "how_got_number",
      "wrong_person",
      "reengagement",
    ].includes(normalized_use_case)
  ) {
    return false;
  }

  if (
    normalized_placeholder === "offer_price" &&
    ![
      "offer_reveal_cash",
      "offer_reveal_lease_option",
      "offer_reveal_subject_to",
      "offer_reveal_novation",
      "mf_offer_reveal",
      "justify_price",
      "ask_timeline",
      "ask_condition_clarifier",
      "narrow_range",
      "close_handoff",
    ].includes(normalized_use_case)
  ) {
    return false;
  }

  if (
    normalized_placeholder === "repair_cost" &&
    !["justify_price", "ask_condition_clarifier", "narrow_range"].includes(
      normalized_use_case
    )
  ) {
    return false;
  }

  if (
    ["unit_count", "occupied_units", "monthly_rents", "monthly_expenses"].includes(
      normalized_placeholder
    ) &&
    !String(normalized_use_case || "").startsWith("mf_")
  ) {
    return false;
  }

  return true;
}

export function evaluateTemplatePlaceholders({
  template_text,
  use_case = null,
  variant_group = null,
  context = {},
  overrides = {},
} = {}) {
  const placeholders = extractPlaceholders(template_text);
  const variables = buildVariableMap(context, overrides);
  const normalized_use_case = normalizeSellerFlowUseCase(use_case, variant_group);

  const invalid_placeholders = [];
  const missing_required_placeholders = [];

  for (const placeholder of placeholders) {
    if (!placeholderAllowedForUseCase(placeholder, normalized_use_case)) {
      invalid_placeholders.push(`{{${placeholder}}}`);
      continue;
    }

    const canonical_placeholder = LEGACY_PLACEHOLDER_ALIASES[placeholder] || placeholder;
    const value = variables[canonical_placeholder];
    if (!value || String(value).trim() === "") {
      missing_required_placeholders.push(`{{${placeholder}}}`);
    }
  }

  return {
    ok: invalid_placeholders.length === 0 && missing_required_placeholders.length === 0,
    placeholders,
    variables,
    invalid_placeholders,
    missing_required_placeholders,
  };
}

export function renderTemplate({
  template_text,
  context = {},
  overrides = {},
  remove_unknown_placeholders = true,
  use_case = null,
  variant_group = null,
} = {}) {
  const raw_template = String(template_text ?? "");
  if (!raw_template.trim()) {
    throw new Error("renderTemplate: template_text is empty");
  }

  const validation = evaluateTemplatePlaceholders({
    template_text: raw_template,
    use_case,
    variant_group,
    context,
    overrides,
  });

  const variables = validation.variables;
  const placeholders = validation.placeholders;

  let rendered = raw_template;
  const used_placeholders = [];
  const missing_placeholders = [];

  for (const key of placeholders) {
    const canonical_key = LEGACY_PLACEHOLDER_ALIASES[key] || key;
    const replacement = variables[canonical_key];
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
  rendered = cleanupPunctuation(rendered);

  return {
    ok: validation.ok,
    template_text: raw_template,
    rendered_text: rendered,
    character_count: countCharacters(rendered),
    used_placeholders,
    missing_placeholders,
    invalid_placeholders: validation.invalid_placeholders,
    missing_required_placeholders: validation.missing_required_placeholders,
    variables,
  };
}

export default renderTemplate;
