function clean(value) {
  return String(value ?? "").trim();
}

const SEVERITY_COLOR = Object.freeze({
  debug: 0x95a5a6,
  info: 0x3498db,
  success: 0x2ecc71,
  warning: 0xf39c12,
  error: 0xe67e22,
  critical: 0xe74c3c,
  hot: 0xff5e57,
  approval: 0x8e44ad,
});

const SEVERITY_BADGE = Object.freeze({
  debug: "DEBUG",
  info: "INFO",
  success: "SUCCESS",
  warning: "WARNING",
  error: "ERROR",
  critical: "CRITICAL",
  hot: "HOT LEAD",
  approval: "APPROVAL",
});

function sanitizeField(field = {}) {
  return {
    name: clean(field.name).slice(0, 256) || "Field",
    value: clean(field.value).slice(0, 1024) || "-",
    inline: Boolean(field.inline),
  };
}

function sanitizeFields(fields = []) {
  if (!Array.isArray(fields)) return [];
  return fields.map(sanitizeField).slice(0, 25);
}

export function buildOpsEmbed({
  event_type,
  severity = "info",
  domain = "command",
  title,
  summary,
  fields = [],
  metadata = {},
  dedupe_key = null,
  suggested_action = null,
} = {}) {
  const normalizedSeverity = clean(severity).toLowerCase() || "info";
  const badge = SEVERITY_BADGE[normalizedSeverity] || "INFO";
  const color = SEVERITY_COLOR[normalizedSeverity] || SEVERITY_COLOR.info;
  const environment = clean(process.env.VERCEL_ENV || process.env.NODE_ENV || "unknown");

  const embedFields = sanitizeFields(fields);
  if (suggested_action) {
    embedFields.push({
      name: "Suggested Action",
      value: clean(suggested_action).slice(0, 1024),
      inline: false,
    });
  }

  if (metadata?.risk_level) {
    embedFields.push({
      name: "Risk",
      value: clean(metadata.risk_level).slice(0, 128),
      inline: true,
    });
  }

  return {
    title: `${badge} | ${clean(title).slice(0, 220) || "Ops Notification"}`,
    description: clean(summary).slice(0, 3800) || "No summary provided.",
    color,
    timestamp: new Date().toISOString(),
    fields: embedFields,
    footer: {
      text: [
        `env:${environment}`,
        `event:${clean(event_type) || "unknown"}`,
        `domain:${clean(domain) || "unknown"}`,
        dedupe_key ? `dedupe:${clean(dedupe_key).slice(0, 80)}` : null,
      ].filter(Boolean).join(" | "),
    },
  };
}

export function buildOpsDebugEmbed({ title, summary, metadata = {} } = {}) {
  const lines = Object.entries(metadata || {})
    .map(([key, value]) => `${clean(key)}: ${clean(typeof value === "string" ? value : JSON.stringify(value))}`)
    .slice(0, 25);

  return {
    title: clean(title).slice(0, 256) || "DEBUG | Ops Diagnostics",
    description: [clean(summary), ...lines].filter(Boolean).join("\n").slice(0, 4096),
    color: SEVERITY_COLOR.debug,
    timestamp: new Date().toISOString(),
    footer: { text: "debug-log" },
  };
}

export { SEVERITY_COLOR, SEVERITY_BADGE };
