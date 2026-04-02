import ENV from "@/lib/config/env.js";
import { getAlertDeliveryConfig } from "@/lib/domain/alerts/alert-delivery.js";
import {
  getTextgridProviderCapabilities,
  getTextgridSendCredentialStatus,
  hasTextgridWebhookSecret,
} from "@/lib/providers/textgrid.js";
import {
  getDocusignConfigSummary,
  verifyDocusignAuth,
} from "@/lib/providers/docusign.js";
import {
  getSmtpConfigSummary,
  verifySmtpConnection,
} from "@/lib/providers/email.js";
import { getStorageConfigSummary } from "@/lib/providers/storage.js";

function clean(value) {
  return String(value ?? "").trim();
}

function configuredStatus(configured, extra = {}) {
  return {
    configured: Boolean(configured),
    ...extra,
  };
}

function getPodioConfigSummary(env = process.env) {
  const required = [
    "PODIO_CLIENT_ID",
    "PODIO_CLIENT_SECRET",
    "PODIO_USERNAME",
    "PODIO_PASSWORD",
  ];
  const missing = required.filter((key) => !clean(env[key]));

  return {
    configured: missing.length === 0,
    missing,
    client_id_present: Boolean(clean(env.PODIO_CLIENT_ID)),
    client_secret_present: Boolean(clean(env.PODIO_CLIENT_SECRET)),
    username_present: Boolean(clean(env.PODIO_USERNAME)),
    password_present: Boolean(clean(env.PODIO_PASSWORD)),
  };
}

function getSecretsSummary(env = process.env) {
  return {
    internal_api_secret: configuredStatus(clean(env.INTERNAL_API_SECRET)),
    ops_dashboard_secret: configuredStatus(clean(env.OPS_DASHBOARD_SECRET)),
    cron_secret: configuredStatus(clean(env.CRON_SECRET)),
    textgrid_webhook_secret: configuredStatus(clean(env.TEXTGRID_WEBHOOK_SECRET)),
    docusign_webhook_secret: configuredStatus(clean(env.DOCUSIGN_WEBHOOK_SECRET)),
    title_webhook_secret: configuredStatus(clean(env.TITLE_WEBHOOK_SECRET)),
    closings_webhook_secret: configuredStatus(clean(env.CLOSINGS_WEBHOOK_SECRET)),
  };
}

export function buildVerificationReadinessSnapshot(env = process.env) {
  const podio = getPodioConfigSummary(env);
  const textgrid = getTextgridSendCredentialStatus();
  const textgrid_capabilities = getTextgridProviderCapabilities();
  const docusign = getDocusignConfigSummary();
  const email = getSmtpConfigSummary();
  const storage = getStorageConfigSummary();
  const alerting = getAlertDeliveryConfig(env);
  const secrets = getSecretsSummary(env);

  return {
    ok:
      podio.configured &&
      textgrid.configured &&
      docusign.configured &&
      email.configured &&
      Object.values(secrets).every((entry) => entry.configured),
    podio,
    textgrid: {
      ...textgrid,
      webhook_secret_present: hasTextgridWebhookSecret(),
      provider_capabilities: textgrid_capabilities,
    },
    docusign: {
      ...docusign,
      webhook_secret_present: Boolean(clean(ENV.DOCUSIGN_WEBHOOK_SECRET)),
    },
    email,
    storage,
    alerting: {
      enabled: alerting.enabled,
      cooldown_minutes: alerting.cooldown_minutes,
      renotify_every_occurrences: alerting.renotify_every_occurrences,
      destinations: Object.fromEntries(
        Object.entries(alerting.destinations || {}).map(([name, destination]) => [
          name,
          {
            enabled: Boolean(destination?.enabled),
            configured: Boolean(destination?.configured),
            min_severity: destination?.min_severity || null,
          },
        ])
      ),
    },
    secrets,
  };
}

export async function getVerificationReadiness({
  perform_live = false,
  env = process.env,
  deps = {},
} = {}) {
  const snapshot = buildVerificationReadinessSnapshot(env);

  if (!perform_live) {
    return {
      ok: snapshot.ok,
      perform_live: false,
      ...snapshot,
    };
  }

  const podio_summary = getPodioConfigSummary(env);
  let podio_live = {
    ok: false,
    reason: "podio_not_configured",
  };

  if (podio_summary.configured) {
    try {
      if (deps.verifyPodioAuth) {
        podio_live = await deps.verifyPodioAuth();
      } else {
        const podioProvider = await import("@/lib/providers/podio.js");
        podio_live = await podioProvider.verifyPodioAuth();
      }
    } catch (error) {
      podio_live = {
        ok: false,
        reason: clean(error?.message) || "podio_live_check_failed",
      };
    }
  }

  const docusign_live = snapshot.docusign.configured
    ? await (deps.verifyDocusignAuth || verifyDocusignAuth)({ dry_run: false })
    : {
        ok: false,
        reason: "docusign_not_configured",
      };

  const email_live = snapshot.email.configured
    ? await (deps.verifySmtpConnection || verifySmtpConnection)()
    : {
        ok: false,
        reason: "smtp_not_configured",
      };

  const textgrid_live = {
    ok: false,
    reason: snapshot.textgrid.configured
      ? "no_safe_non_send_textgrid_probe_available"
      : "textgrid_not_configured",
    provider_status_lookup_supported:
      snapshot.textgrid.provider_capabilities.message_status_lookup.supported,
  };

  return {
    ok: snapshot.ok && podio_live.ok && docusign_live.ok && email_live.ok,
    perform_live: true,
    ...snapshot,
    live: {
      podio: podio_live,
      textgrid: textgrid_live,
      docusign: docusign_live,
      email: email_live,
    },
  };
}

export default getVerificationReadiness;
