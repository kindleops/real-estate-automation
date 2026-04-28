import { resolveChannelForEvent } from "@/lib/discord/discord-channel-router.js";

const SECRET_ENV_NAMES = Object.freeze([
  "INTERNAL_API_SECRET",
  "CRON_SECRET",
  "SUPABASE_SERVICE_ROLE_KEY",
  "SUPABASE_ANON_KEY",
  "DISCORD_BOT_TOKEN",
  "TEXTGRID_AUTH_TOKEN",
  "TEXTGRID_WEBHOOK_SECRET",
  "PODIO_CLIENT_SECRET",
  "PODIO_PASSWORD",
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "POSTHOG_KEY",
  "SENTRY_AUTH_TOKEN",
]);

const sentProviderMessageIds = new Set();

const defaultDeps = {
  fetch: globalThis.fetch,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function asObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function truncate(value, max = 1024) {
  const text = clean(value).replace(/\s+/g, " ");
  return text.length > max ? `${text.slice(0, Math.max(0, max - 3))}...` : text;
}

function escapeRegExp(value = "") {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function secretValues(env = process.env) {
  return SECRET_ENV_NAMES
    .map((key) => clean(env?.[key]))
    .filter((value) => value.length >= 8);
}

function redactSecrets(value, env = process.env) {
  let text = String(value ?? "");
  for (const name of SECRET_ENV_NAMES) {
    text = text.replace(new RegExp(`\\b${escapeRegExp(name)}\\b`, "gi"), "[redacted_key]");
  }
  for (const secret of secretValues(env)) {
    text = text.replace(new RegExp(escapeRegExp(secret), "g"), "[redacted]");
  }
  return text;
}

function field(name, value, inline = true, env = process.env) {
  return {
    name: truncate(redactSecrets(name, env), 256) || "Field",
    value: truncate(redactSecrets(value, env), 1024) || "n/a",
    inline,
  };
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const normalized = clean(value);
    if (normalized) return normalized;
  }
  return "";
}

function getResultContext(result = {}) {
  return asObject(result?.context);
}

function getClassificationSummary(result = {}) {
  const classification = asObject(result?.classification);
  const sellerPlan = asObject(result?.seller_stage_reply?.plan);
  return firstNonEmpty(
    sellerPlan.selected_use_case,
    sellerPlan.detected_intent,
    classification.objection,
    classification.intent,
    classification.source,
    result?.unknown_router?.bucket,
    result?.reason,
    result?.error
  );
}

function getRouteSummary(result = {}, buyerResult = {}) {
  const route = asObject(result?.route);
  if (buyerResult?.matched) return "buyer_disposition";
  return firstNonEmpty(
    route.stage && route.use_case ? `${route.stage} / ${route.use_case}` : "",
    route.stage,
    route.use_case,
    result?.unknown_router?.bucket,
    result?.stage,
    result?.reason
  );
}

function getSeverity({ result = {}, buyerResult = {}, failure = null, severity = null } = {}) {
  const explicit = clean(severity).toLowerCase();
  if (explicit) return explicit;
  if (failure) return "error";
  if (buyerResult?.ok === false || result?.ok === false) return result?.retryable ? "warning" : "error";
  const useCase = clean(result?.seller_stage_reply?.plan?.selected_use_case).toLowerCase();
  const unknownBucket = clean(result?.unknown_router?.bucket).toLowerCase();
  if (useCase.includes("stop") || useCase.includes("opt_out") || unknownBucket.includes("opt_out")) return "warning";
  if (useCase.includes("wrong") || unknownBucket.includes("unknown")) return "warning";
  return "info";
}

function severityColor(severity = "info") {
  const normalized = clean(severity).toLowerCase();
  if (normalized === "error" || normalized === "critical") return 0xe74c3c;
  if (normalized === "warning") return 0xf39c12;
  if (normalized === "success") return 0x2ecc71;
  return 0x3498db;
}

export function buildInboundSmsAlertMessage({
  payload = {},
  result = {},
  buyer_result = {},
  provider_message_id = "",
  from = "",
  to = "",
  message_body = "",
  handler_name = "",
  failure = null,
  severity = null,
  final_response_status = null,
  buyer_handler_failed = false,
  buyer_handler_error_message = "",
  env = process.env,
} = {}) {
  const normalizedPayload = asObject(payload);
  const normalizedResult = asObject(result);
  const normalizedBuyerResult = asObject(buyer_result);
  const context = getResultContext(normalizedResult);
  const summary = asObject(context?.summary);
  const ids = asObject(context?.ids);
  const normalizedFailure = asObject(failure);
  const resolvedSeverity = getSeverity({
    result: normalizedResult,
    buyerResult: normalizedBuyerResult,
    failure: normalizedFailure?.error_message ? normalizedFailure : null,
    severity,
  });
  const providerMessageId = firstNonEmpty(
    provider_message_id,
    normalizedPayload.message_id,
    normalizedResult.message_id,
    normalizedBuyerResult.message_id
  );
  const routeSummary = getRouteSummary(normalizedResult, normalizedBuyerResult);
  const classificationSummary = getClassificationSummary(normalizedResult);
  const sourceBody = firstNonEmpty(message_body, normalizedPayload.message_body, normalizedPayload.message, normalizedResult.body);

  const fields = [
    field("From", firstNonEmpty(from, normalizedPayload.from, normalizedResult.inbound_from), true, env),
    field("To", firstNonEmpty(to, normalizedPayload.to, normalizedResult.inbound_to), true, env),
    field("Provider Message ID", providerMessageId || "unknown", false, env),
    field("Body", sourceBody || "n/a", false, env),
    field("Handler", handler_name || (normalizedBuyerResult?.matched ? "maybeHandleBuyerTextgridInbound" : "handleTextgridInbound"), true, env),
    field("Route", routeSummary || "unknown", true, env),
    field("Classification", classificationSummary || "unknown", true, env),
    field("Seller", firstNonEmpty(summary.seller_name, summary.owner_name, ids.master_owner_id), true, env),
    field("Property", firstNonEmpty(summary.property_address, ids.property_id), false, env),
    field("Market", firstNonEmpty(summary.market, summary.market_name, ids.market_id), true, env),
  ];

  if (normalizedBuyerResult?.matched) {
    fields.push(
      field("Buyer Match", firstNonEmpty(normalizedBuyerResult?.result?.buyer_match_item_id, normalizedBuyerResult?.result?.company_item_id), true, env)
    );
  }

  if (buyer_handler_failed) {
    fields.push(field("Buyer Handler Failure", buyer_handler_error_message || "buyer handler failed", false, env));
  }

  if (normalizedFailure?.error_message) {
    fields.push(field("Failure", normalizedFailure.error_message, false, env));
  }

  if (final_response_status !== null && final_response_status !== undefined) {
    fields.push(field("Webhook Status", String(final_response_status), true, env));
  }

  const title =
    resolvedSeverity === "error" || resolvedSeverity === "critical"
      ? "Inbound SMS Routing Failure"
      : "Inbound SMS Received";

  return {
    embeds: [
      {
        title: redactSecrets(title, env),
        description: redactSecrets(
          `Accepted TextGrid inbound SMS${routeSummary ? ` routed as ${routeSummary}` : ""}.`,
          env
        ).slice(0, 4096),
        color: severityColor(resolvedSeverity),
        timestamp: new Date().toISOString(),
        fields: fields.slice(0, 25),
        footer: {
          text: redactSecrets(`event:inbound_sms_alert | severity:${resolvedSeverity}`, env),
        },
      },
    ],
    allowed_mentions: { parse: [] },
  };
}

export function __setInboundSmsAlertDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetInboundSmsAlertDeps() {
  runtimeDeps = { ...defaultDeps };
  sentProviderMessageIds.clear();
}

export function __resetInboundSmsAlertDedupeForTest() {
  sentProviderMessageIds.clear();
}

export async function sendInboundSmsDiscordAlert(input = {}, opts = {}) {
  try {
    const env = opts.env || process.env;
    const providerMessageId = clean(
      input.provider_message_id ||
      input.payload?.message_id ||
      input.result?.message_id ||
      input.buyer_result?.message_id
    );

    if (providerMessageId && sentProviderMessageIds.has(providerMessageId)) {
      return { ok: true, skipped: true, reason: "duplicate_provider_message_id" };
    }
    if (providerMessageId) sentProviderMessageIds.add(providerMessageId);

    const fetchWasInjected = Boolean(opts.fetch) || runtimeDeps.fetch !== defaultDeps.fetch;
    const fetchImpl = opts.fetch || runtimeDeps.fetch;
    const token = clean(env.DISCORD_BOT_TOKEN);
    const resolution = resolveChannelForEvent("inbound_sms_alert", { env });
    const message = buildInboundSmsAlertMessage({ ...input, env });

    if (clean(env.NODE_ENV).toLowerCase() === "test" && !fetchWasInjected) {
      return {
        ok: false,
        skipped: true,
        reason: "discord_fetch_not_injected_for_test",
        channel_id: resolution.channel_id,
        channel_key: resolution.channel_key,
        message,
      };
    }

    if (!token || !resolution.channel_id || typeof fetchImpl !== "function") {
      return {
        ok: false,
        skipped: true,
        reason: "discord_not_configured",
        channel_id: resolution.channel_id,
        channel_key: resolution.channel_key,
        message,
      };
    }

    const response = await fetchImpl(`https://discord.com/api/v10/channels/${resolution.channel_id}/messages`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bot ${token}`,
      },
      body: JSON.stringify(message),
    }).catch(() => null);

    if (!response?.ok) {
      return {
        ok: false,
        reason: "discord_post_failed",
        status: response?.status || null,
        channel_id: resolution.channel_id,
        channel_key: resolution.channel_key,
        message,
      };
    }

    const data = await response.json().catch(() => ({}));
    return {
      ok: true,
      channel_id: resolution.channel_id,
      channel_key: resolution.channel_key,
      discord_message_id: clean(data?.id) || null,
      message,
    };
  } catch {
    return { ok: false, reason: "inbound_sms_alert_failed" };
  }
}

export default sendInboundSmsDiscordAlert;
