import ENV from "@/lib/config/env.js";
import { sendEmail } from "@/lib/providers/email.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function parseList(value) {
  return clean(value)
    .split(",")
    .map((entry) => clean(entry))
    .filter(Boolean);
}

function severityRank(severity = "") {
  const normalized = lower(severity);
  if (normalized === "critical") return 4;
  if (normalized === "high") return 3;
  if (normalized === "warning") return 2;
  if (normalized === "info") return 1;
  return 0;
}

function isAlertSilenced(alert_meta = {}, now = nowIso()) {
  if (lower(alert_meta?.operator_state) !== "silenced") return false;

  const silenced_until_ts = toTimestamp(alert_meta?.silenced_until);
  const now_ts = toTimestamp(now) ?? Date.now();

  return silenced_until_ts !== null && silenced_until_ts > now_ts;
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

export function getAlertDeliveryConfig(env = ENV) {
  const webhook_url = clean(env.ALERT_WEBHOOK_URL);
  const slack_webhook_url = clean(env.ALERT_SLACK_WEBHOOK_URL);
  const email_to = parseList(env.ALERT_EMAIL_TO);

  const cooldown_minutes = normalizePositiveInteger(
    env.ALERT_DELIVERY_COOLDOWN_MINUTES,
    60
  );
  const renotify_every_occurrences = normalizePositiveInteger(
    env.ALERT_RENOTIFY_EVERY_OCCURRENCES,
    10
  );

  const destinations = {
    webhook: {
      id: "webhook",
      enabled: Boolean(webhook_url),
      configured: Boolean(webhook_url),
      url: webhook_url || null,
      bearer_token: clean(env.ALERT_WEBHOOK_BEARER_TOKEN) || null,
      min_severity: lower(env.ALERT_MIN_SEVERITY_WEBHOOK || "high") || "high",
      cooldown_minutes,
      renotify_every_occurrences,
    },
    slack: {
      id: "slack",
      enabled: Boolean(slack_webhook_url),
      configured: Boolean(slack_webhook_url),
      url: slack_webhook_url || null,
      min_severity: lower(env.ALERT_MIN_SEVERITY_SLACK || "high") || "high",
      cooldown_minutes,
      renotify_every_occurrences,
    },
    email: {
      id: "email",
      enabled: email_to.length > 0,
      configured: email_to.length > 0,
      recipients: email_to,
      min_severity: lower(env.ALERT_MIN_SEVERITY_EMAIL || "critical") || "critical",
      cooldown_minutes,
      renotify_every_occurrences,
      subject_prefix: clean(env.ALERT_EMAIL_SUBJECT_PREFIX) || "[REA Alert]",
    },
  };

  return {
    enabled: Object.values(destinations).some((destination) => destination.enabled),
    cooldown_minutes,
    renotify_every_occurrences,
    http_timeout_ms: normalizePositiveInteger(env.ALERT_HTTP_TIMEOUT_MS, 15000),
    destinations,
  };
}

export function shouldDeliverAlertToDestination({
  destination = {},
  alert_meta = {},
  previous_meta = {},
  now = nowIso(),
} = {}) {
  const deliveries = previous_meta?.deliveries || {};
  const previous_delivery = deliveries[destination.id] || {};
  const current_status = lower(alert_meta?.status || "open");
  const previous_status = lower(previous_meta?.status || "");

  if (!destination?.enabled || !destination?.configured) {
    return {
      deliver: false,
      reason: "destination_not_configured",
      previous_delivery,
    };
  }

  if (current_status !== "open") {
    return {
      deliver: false,
      reason: "alert_not_open",
      previous_delivery,
    };
  }

  if (isAlertSilenced(alert_meta, now)) {
    return {
      deliver: false,
      reason: "alert_silenced",
      previous_delivery,
    };
  }

  if (severityRank(alert_meta?.severity) < severityRank(destination?.min_severity)) {
    return {
      deliver: false,
      reason: "below_destination_min_severity",
      previous_delivery,
    };
  }

  const now_ts = toTimestamp(now) ?? Date.now();
  const last_attempt_ts = toTimestamp(previous_delivery?.last_attempt_at);
  const cooldown_ms =
    normalizePositiveInteger(destination?.cooldown_minutes, 60) * 60_000;
  const cooldown_expired =
    last_attempt_ts === null || now_ts - last_attempt_ts >= cooldown_ms;
  const previous_delivered_occurrence = Number(
    previous_delivery?.last_delivered_occurrence || 0
  );
  const current_occurrence = Number(alert_meta?.occurrence_count || 0);
  const renotify_every_occurrences = normalizePositiveInteger(
    destination?.renotify_every_occurrences,
    10
  );
  const severity_escalated =
    severityRank(alert_meta?.severity) >
    severityRank(previous_delivery?.last_delivered_severity || "");
  const never_delivered = !clean(previous_delivery?.last_success_at);
  const previous_failed = lower(previous_delivery?.last_status) === "failed";
  const reopened = previous_status === "resolved";
  const renotify_due =
    current_occurrence >=
    previous_delivered_occurrence + renotify_every_occurrences;

  if (never_delivered) {
    return {
      deliver: true,
      reason: "initial_delivery",
      previous_delivery,
    };
  }

  if (reopened) {
    return {
      deliver: true,
      reason: "reopened_alert",
      previous_delivery,
    };
  }

  if (severity_escalated) {
    return {
      deliver: true,
      reason: "severity_escalated",
      previous_delivery,
    };
  }

  if (previous_failed && cooldown_expired) {
    return {
      deliver: true,
      reason: "retry_failed_delivery_after_cooldown",
      previous_delivery,
    };
  }

  if (cooldown_expired && renotify_due) {
    return {
      deliver: true,
      reason: "renotify_threshold_reached",
      previous_delivery,
    };
  }

  return {
    deliver: false,
    reason: cooldown_expired ? "renotify_threshold_not_reached" : "delivery_cooldown_active",
    previous_delivery,
  };
}

function buildAlertDashboardUrl(alert_meta = {}) {
  const base_url = clean(ENV.APP_BASE_URL).replace(/\/+$/, "");
  if (!base_url) return null;

  const params = new URLSearchParams({
    event_type: "system_alert",
    time_range: "24h",
  });

  if (clean(alert_meta?.subsystem)) params.set("market", clean(alert_meta.subsystem));
  return `${base_url}/dashboard/ops?${params.toString()}`;
}

function buildAlertText(alert_meta = {}) {
  const lines = [
    `Severity: ${clean(alert_meta?.severity) || "warning"}`,
    `Subsystem: ${clean(alert_meta?.subsystem) || "unknown"}`,
    `Code: ${clean(alert_meta?.code) || "unknown"}`,
    `Retryable: ${alert_meta?.retryable ? "yes" : "no"}`,
    `Occurrences: ${Number(alert_meta?.occurrence_count || 0) || 0}`,
    `Summary: ${clean(alert_meta?.summary) || "System alert"}`,
  ];

  if (Array.isArray(alert_meta?.affected_ids) && alert_meta.affected_ids.length) {
    lines.push(`Affected IDs: ${alert_meta.affected_ids.slice(0, 12).join(", ")}`);
  }

  const dashboard_url = buildAlertDashboardUrl(alert_meta);
  if (dashboard_url) lines.push(`Dashboard: ${dashboard_url}`);

  return lines.join("\n");
}

function buildGenericWebhookPayload(alert_meta = {}) {
  return {
    source: "real-estate-automation",
    type: "system_alert",
    status: clean(alert_meta?.status) || "open",
    severity: clean(alert_meta?.severity) || "warning",
    subsystem: clean(alert_meta?.subsystem) || null,
    code: clean(alert_meta?.code) || null,
    summary: clean(alert_meta?.summary) || "System alert",
    retryable: Boolean(alert_meta?.retryable),
    occurrence_count: Number(alert_meta?.occurrence_count || 0) || 0,
    affected_ids: Array.isArray(alert_meta?.affected_ids)
      ? alert_meta.affected_ids.filter(Boolean)
      : [],
    first_seen_at: clean(alert_meta?.first_seen_at) || null,
    last_seen_at: clean(alert_meta?.last_seen_at) || null,
    dashboard_url: buildAlertDashboardUrl(alert_meta),
    metadata:
      alert_meta?.metadata && typeof alert_meta.metadata === "object"
        ? alert_meta.metadata
        : {},
  };
}

function buildSlackPayload(alert_meta = {}) {
  const summary = clean(alert_meta?.summary) || "System alert";
  const severity = clean(alert_meta?.severity).toUpperCase() || "WARNING";
  const dashboard_url = buildAlertDashboardUrl(alert_meta);
  const text = `[${severity}] ${summary}`;

  const fields = [
    {
      type: "mrkdwn",
      text: `*Subsystem*\n${clean(alert_meta?.subsystem) || "unknown"}`,
    },
    {
      type: "mrkdwn",
      text: `*Code*\n${clean(alert_meta?.code) || "unknown"}`,
    },
    {
      type: "mrkdwn",
      text: `*Retryable*\n${alert_meta?.retryable ? "yes" : "no"}`,
    },
    {
      type: "mrkdwn",
      text: `*Occurrences*\n${Number(alert_meta?.occurrence_count || 0) || 0}`,
    },
  ];

  if (Array.isArray(alert_meta?.affected_ids) && alert_meta.affected_ids.length) {
    fields.push({
      type: "mrkdwn",
      text: `*Affected IDs*\n${alert_meta.affected_ids.slice(0, 8).join(", ")}`,
    });
  }

  return {
    text,
    blocks: [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*${text}*`,
        },
      },
      {
        type: "section",
        fields,
      },
      ...(dashboard_url
        ? [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: `<${dashboard_url}|Open Ops Dashboard>`,
              },
            },
          ]
        : []),
    ],
  };
}

async function postJson({
  url,
  payload,
  bearer_token = null,
  timeout_ms = 15000,
  fetch_impl = globalThis.fetch,
} = {}) {
  if (typeof fetch_impl !== "function") {
    return {
      ok: false,
      reason: "fetch_not_available",
      status_code: null,
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeout_ms);

  try {
    const response = await fetch_impl(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(clean(bearer_token)
          ? { Authorization: `Bearer ${clean(bearer_token)}` }
          : {}),
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    const text = await response.text().catch(() => "");

    return {
      ok: response.ok,
      reason: response.ok ? "delivered" : "webhook_delivery_failed",
      status_code: response.status,
      response_body: text || null,
    };
  } catch (error) {
    return {
      ok: false,
      reason: clean(error?.name) === "AbortError" ? "delivery_timeout" : "delivery_request_failed",
      status_code: null,
      error_message: clean(error?.message) || "delivery_request_failed",
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function deliverToWebhook({
  alert_meta,
  destination,
  config,
  fetch_impl,
}) {
  return postJson({
    url: destination.url,
    payload: buildGenericWebhookPayload(alert_meta),
    bearer_token: destination.bearer_token,
    timeout_ms: config.http_timeout_ms,
    fetch_impl,
  });
}

async function deliverToSlack({
  alert_meta,
  destination,
  config,
  fetch_impl,
}) {
  return postJson({
    url: destination.url,
    payload: buildSlackPayload(alert_meta),
    timeout_ms: config.http_timeout_ms,
    fetch_impl,
  });
}

async function deliverToEmail({
  alert_meta,
  destination,
  send_email = sendEmail,
}) {
  const result = await send_email({
    to: destination.recipients,
    subject: `${clean(destination.subject_prefix)} ${clean(alert_meta?.severity).toUpperCase()} ${clean(alert_meta?.subsystem)}:${clean(alert_meta?.code)}`,
    text: buildAlertText(alert_meta),
    dry_run: false,
  });

  return {
    ok: result?.ok !== false,
    reason: result?.ok !== false ? "delivered" : clean(result?.error_message) || "email_delivery_failed",
    provider_message_id: result?.provider_message_id || null,
  };
}

export async function deliverSystemAlert({
  alert_meta = {},
  previous_meta = {},
  config = getAlertDeliveryConfig(),
  fetch_impl = globalThis.fetch,
  send_email = sendEmail,
  now = nowIso(),
} = {}) {
  const deliveries = {
    ...(previous_meta?.deliveries && typeof previous_meta.deliveries === "object"
      ? previous_meta.deliveries
      : {}),
  };
  const attempts = [];

  for (const destination of Object.values(config.destinations || {})) {
    const decision = shouldDeliverAlertToDestination({
      destination,
      alert_meta,
      previous_meta,
      now,
    });
    const previous_delivery = decision.previous_delivery || {};
    const base_delivery = {
      enabled: Boolean(destination?.enabled),
      configured: Boolean(destination?.configured),
      min_severity: destination?.min_severity || "high",
      last_evaluated_at: now,
      last_decision: decision.reason,
      last_attempt_at: previous_delivery.last_attempt_at || null,
      last_success_at: previous_delivery.last_success_at || null,
      last_status: previous_delivery.last_status || null,
      last_error: previous_delivery.last_error || null,
      last_status_code: previous_delivery.last_status_code ?? null,
      last_delivered_occurrence: previous_delivery.last_delivered_occurrence ?? null,
      last_delivered_severity: previous_delivery.last_delivered_severity || null,
      delivery_count: Number(previous_delivery.delivery_count || 0) || 0,
      attempt_count: Number(previous_delivery.attempt_count || 0) || 0,
    };

    if (!decision.deliver) {
      deliveries[destination.id] = base_delivery;
      attempts.push({
        destination: destination.id,
        attempted: false,
        delivered: false,
        reason: decision.reason,
      });
      continue;
    }

    let result = null;
    if (destination.id === "webhook") {
      result = await deliverToWebhook({
        alert_meta,
        destination,
        config,
        fetch_impl,
      });
    } else if (destination.id === "slack") {
      result = await deliverToSlack({
        alert_meta,
        destination,
        config,
        fetch_impl,
      });
    } else if (destination.id === "email") {
      result = await deliverToEmail({
        alert_meta,
        destination,
        send_email,
      });
    } else {
      result = {
        ok: false,
        reason: "unsupported_alert_destination",
      };
    }

    deliveries[destination.id] = {
      ...base_delivery,
      last_attempt_at: now,
      attempt_count: base_delivery.attempt_count + 1,
      last_status: result?.ok ? "delivered" : "failed",
      last_error: result?.ok ? null : clean(result?.error_message || result?.reason),
      last_status_code: Number(result?.status_code || 0) || null,
      last_success_at: result?.ok ? now : base_delivery.last_success_at,
      last_delivered_occurrence: result?.ok
        ? Number(alert_meta?.occurrence_count || 0) || 0
        : base_delivery.last_delivered_occurrence,
      last_delivered_severity: result?.ok
        ? clean(alert_meta?.severity) || "warning"
        : base_delivery.last_delivered_severity,
      delivery_count: result?.ok
        ? base_delivery.delivery_count + 1
        : base_delivery.delivery_count,
      last_provider_message_id: clean(result?.provider_message_id) || null,
      last_response_body: clean(result?.response_body) || null,
    };

    attempts.push({
      destination: destination.id,
      attempted: true,
      delivered: Boolean(result?.ok),
      reason: result?.reason || "delivery_completed",
      status_code: Number(result?.status_code || 0) || null,
      provider_message_id: clean(result?.provider_message_id) || null,
    });
  }

  return {
    deliveries,
    attempts,
  };
}

export default {
  getAlertDeliveryConfig,
  shouldDeliverAlertToDestination,
  deliverSystemAlert,
};
