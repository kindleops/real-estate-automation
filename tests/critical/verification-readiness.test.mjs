import test from "node:test";
import assert from "node:assert/strict";

import { buildVerificationReadinessSnapshot } from "@/lib/verification/readiness.js";

test("verification readiness snapshot reports configured secrets and unsupported TextGrid provider lookup honestly", () => {
  const env = {
    INTERNAL_API_SECRET: "secret",
    OPS_DASHBOARD_SECRET: "ops-secret",
    CRON_SECRET: "cron-secret",
    TEXTGRID_WEBHOOK_SECRET: "tg-webhook",
    DOCUSIGN_WEBHOOK_SECRET: "docu-webhook",
    TITLE_WEBHOOK_SECRET: "title-webhook",
    CLOSINGS_WEBHOOK_SECRET: "closings-webhook",
    PODIO_CLIENT_ID: "podio-id",
    PODIO_CLIENT_SECRET: "podio-secret",
    PODIO_USERNAME: "podio-user",
    PODIO_PASSWORD: "podio-pass",
  };

  const snapshot = buildVerificationReadinessSnapshot(env);

  assert.equal(snapshot.podio.configured, true);
  assert.equal(snapshot.secrets.internal_api_secret.configured, true);
  assert.equal(
    snapshot.textgrid.provider_capabilities.message_status_lookup.supported,
    false
  );
  assert.equal(
    snapshot.textgrid.provider_capabilities.message_status_lookup.reason,
    "no_verified_public_textgrid_message_status_lookup_endpoint"
  );
});
