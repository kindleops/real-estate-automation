function clean(value) {
  return String(value ?? "").trim();
}

function sanitizeTemplateHtml(input = "") {
  // Remove script blocks and inline javascript URLs.
  return String(input)
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "")
    .replace(/javascript:/gi, "")
    .trim();
}

function extractTemplateVariables(value = "") {
  const matches = String(value).match(/{{\s*([a-zA-Z0-9_]+)\s*}}/g) || [];
  const out = [];
  for (const raw of matches) {
    const variable = raw.replace(/[{}\s]/g, "");
    if (variable && !out.includes(variable)) out.push(variable);
  }
  return out;
}

function valueForVariable(context = {}, variable = "") {
  const direct = context?.[variable];
  if (direct !== undefined && direct !== null) return String(direct);

  // Friendly aliases for common seller/property variables.
  const aliases = {
    seller_first_name: ["first_name", "seller_first_name", "owner_first_name"],
    property_address: ["property_address", "address"],
    property_city: ["property_city", "city"],
    cash_offer: ["cash_offer", "offer_amount", "price_offer"],
  };

  const keys = aliases[variable] || [variable];
  for (const key of keys) {
    const value = context?.[key];
    if (value !== undefined && value !== null) {
      return String(value);
    }
  }

  return null;
}

function renderOne(value = "", context = {}, missing = new Set()) {
  return String(value).replace(/{{\s*([a-zA-Z0-9_]+)\s*}}/g, (_match, variable) => {
    const resolved = valueForVariable(context, variable);
    if (resolved === null || clean(resolved) === "") {
      missing.add(variable);
      return "";
    }
    return String(resolved);
  });
}

export function renderEmailTemplate(template = {}, context = {}) {
  const subject_raw = clean(template?.subject);
  const html_raw = sanitizeTemplateHtml(template?.html_body || template?.htmlBody || "");
  const text_raw = clean(template?.text_body || template?.textBody || "");

  const explicit_variables = Array.isArray(template?.variables)
    ? template.variables.map((v) => (typeof v === "string" ? v : clean(v?.name || v?.key))).filter(Boolean)
    : [];

  const referenced_variables = [
    ...extractTemplateVariables(subject_raw),
    ...extractTemplateVariables(html_raw),
    ...extractTemplateVariables(text_raw),
  ];

  const critical_variables = [...new Set([...explicit_variables, ...referenced_variables])];

  const missing = new Set();
  const rendered_subject = renderOne(subject_raw, context, missing).trim();
  const rendered_html = renderOne(html_raw, context, missing).trim();
  const rendered_text = renderOne(text_raw, context, missing).trim();

  // Also enforce explicit variables that may not be embedded in text.
  for (const variable of explicit_variables) {
    const resolved = valueForVariable(context, variable);
    if (resolved === null || clean(resolved) === "") {
      missing.add(variable);
    }
  }

  const missing_variables = critical_variables.filter((v) => missing.has(v));

  return {
    subject: rendered_subject,
    html_body: rendered_html,
    text_body: rendered_text,
    missing_variables,
  };
}

export default renderEmailTemplate;
