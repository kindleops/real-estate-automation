import test from "node:test";
import assert from "node:assert/strict";

import {
  deliverSystemAlert,
  shouldDeliverAlertToDestination,
} from "@/lib/domain/alerts/alert-delivery.js";

function createDestination(overrides = {}) {
  return {
    id: "webhook",
    enabled: true,
    configured: true,
    min_severity: "high",
    cooldown_minutes: 60,
    renotify_every_occurrences: 5,
    ...overrides,
  };
}

test("alert delivery decisions trigger initial delivery and enforce severity floors", () => {
  const initial = shouldDeliverAlertToDestination({
    destination: createDestination(),
    alert_meta: {
      status: "open",
      severity: "high",
      occurrence_count: 1,
    },
    previous_meta: {},
    now: "2026-04-01T12:00:00.000Z",
  });

  assert.equal(initial.deliver, true);
  assert.equal(initial.reason, "initial_delivery");

  const belowSeverity = shouldDeliverAlertToDestination({
    destination: createDestination({ min_severity: "critical" }),
    alert_meta: {
      status: "open",
      severity: "warning",
      occurrence_count: 1,
    },
    previous_meta: {},
    now: "2026-04-01T12:00:00.000Z",
  });

  assert.equal(belowSeverity.deliver, false);
  assert.equal(belowSeverity.reason, "below_destination_min_severity");
});

test("alert delivery decisions escalate severity and renotify only after cooldown thresholds", () => {
  const previous_meta = {
    status: "open",
    deliveries: {
      webhook: {
        last_attempt_at: "2026-04-01T12:00:00.000Z",
        last_success_at: "2026-04-01T12:00:00.000Z",
        last_status: "delivered",
        last_delivered_occurrence: 3,
        last_delivered_severity: "high",
      },
    },
  };

  const cooldownActive = shouldDeliverAlertToDestination({
    destination: createDestination(),
    alert_meta: {
      status: "open",
      severity: "high",
      occurrence_count: 4,
    },
    previous_meta,
    now: "2026-04-01T12:30:00.000Z",
  });

  assert.equal(cooldownActive.deliver, false);
  assert.equal(cooldownActive.reason, "delivery_cooldown_active");

  const severityEscalated = shouldDeliverAlertToDestination({
    destination: createDestination(),
    alert_meta: {
      status: "open",
      severity: "critical",
      occurrence_count: 4,
    },
    previous_meta,
    now: "2026-04-01T13:30:00.000Z",
  });

  assert.equal(severityEscalated.deliver, true);
  assert.equal(severityEscalated.reason, "severity_escalated");

  const renotify = shouldDeliverAlertToDestination({
    destination: createDestination(),
    alert_meta: {
      status: "open",
      severity: "high",
      occurrence_count: 8,
    },
    previous_meta,
    now: "2026-04-01T13:30:00.000Z",
  });

  assert.equal(renotify.deliver, true);
  assert.equal(renotify.reason, "renotify_threshold_reached");
});

test("alert delivery decisions suppress silenced alerts until the mute window expires", () => {
  const silenced = shouldDeliverAlertToDestination({
    destination: createDestination(),
    alert_meta: {
      status: "open",
      severity: "critical",
      operator_state: "silenced",
      silenced_until: "2026-04-01T14:00:00.000Z",
      occurrence_count: 3,
    },
    previous_meta: {},
    now: "2026-04-01T13:00:00.000Z",
  });

  assert.equal(silenced.deliver, false);
  assert.equal(silenced.reason, "alert_silenced");
});

test("deliverSystemAlert records webhook and email delivery state without spamming skipped destinations", async () => {
  const alert_meta = {
    status: "open",
    severity: "critical",
    subsystem: "queue",
    code: "run_failed",
    summary: "Queue runner failed.",
    retryable: true,
    occurrence_count: 1,
    affected_ids: ["queue-item-1"],
    first_seen_at: "2026-04-01T12:00:00.000Z",
    last_seen_at: "2026-04-01T12:00:00.000Z",
    metadata: {
      run_id: "run-1",
    },
  };

  const delivery = await deliverSystemAlert({
    alert_meta,
    previous_meta: {},
    now: "2026-04-01T12:00:00.000Z",
    config: {
      enabled: true,
      http_timeout_ms: 1000,
      destinations: {
        webhook: {
          id: "webhook",
          enabled: true,
          configured: true,
          url: "https://alerts.example.test/hook",
          bearer_token: "token-1",
          min_severity: "high",
          cooldown_minutes: 60,
          renotify_every_occurrences: 5,
        },
        email: {
          id: "email",
          enabled: true,
          configured: true,
          recipients: ["ops@example.test"],
          min_severity: "critical",
          cooldown_minutes: 60,
          renotify_every_occurrences: 5,
          subject_prefix: "[REA Alert]",
        },
      },
    },
    fetch_impl: async (url, options) => {
      assert.equal(url, "https://alerts.example.test/hook");
      assert.equal(options.method, "POST");
      assert.equal(options.headers.Authorization, "Bearer token-1");

      return {
        ok: true,
        status: 202,
        text: async () => "accepted",
      };
    },
    send_email: async ({ to, subject, text }) => {
      assert.deepEqual(to, ["ops@example.test"]);
      assert.match(subject, /\[REA Alert\] CRITICAL queue:run_failed/);
      assert.match(text, /Queue runner failed/);
      return {
        ok: true,
        provider_message_id: "smtp-msg-1",
      };
    },
  });

  assert.equal(delivery.attempts.length, 2);
  assert.deepEqual(
    delivery.attempts.map((attempt) => [attempt.destination, attempt.delivered]),
    [
      ["webhook", true],
      ["email", true],
    ]
  );
  assert.equal(delivery.deliveries.webhook.last_status, "delivered");
  assert.equal(delivery.deliveries.webhook.last_status_code, 202);
  assert.equal(delivery.deliveries.email.last_provider_message_id, "smtp-msg-1");
  assert.equal(delivery.deliveries.email.delivery_count, 1);
});
