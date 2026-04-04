import test from "node:test";
import assert from "node:assert/strict";

import {
  resolveLatencyAwareQueueSchedule,
  resolveQueueSchedule,
} from "@/lib/domain/queue/queue-schedule.js";

function parseLocalMinute(value) {
  const match = String(value || "").match(/(\d{2}):(\d{2}):\d{2}$/);
  if (!match) return null;
  return Number(match[1]) * 60 + Number(match[2]);
}

test("queue schedule spreads a Central morning window across the next local day instead of pinning to window start", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "7AM-9AM CT",
    distribution_key: "owner:3274320218",
  });

  assert.match(result.scheduled_for_local, /^2026-04-03 /);
  assert.equal(result.reason, "outside_contact_window_schedule_within_window");
  assert.equal(result.within_contact_window, false);
  assert.ok(parseLocalMinute(result.scheduled_for_local) >= 7 * 60 + 5);
  assert.ok(parseLocalMinute(result.scheduled_for_local) <= 8 * 60 + 54);
});

test("queue schedule without a distribution key still pins to window start", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "7AM-9AM CT",
  });

  assert.equal(result.scheduled_for_local, "2026-04-03 07:00:00");
  assert.equal(result.reason, "outside_contact_window_schedule_at_window_start");
  assert.equal(result.within_contact_window, false);
});

test("queue schedule keeps an in-window Local schedule at the current time", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "9AM-8PM Local",
  });

  assert.equal(result.scheduled_for_local, "2026-04-02 17:14:00");
  assert.equal(result.scheduled_for_utc, "2026-04-02 22:14:00");
  assert.equal(result.within_contact_window, true);
});

test("queue schedule respects target timezone when the window starts later today", () => {
  const mountain = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Mountain",
    contact_window: "5PM-8PM MT",
    distribution_key: "owner:1",
  });
  const eastern = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Eastern",
    contact_window: "12PM-1PM ET",
    distribution_key: "owner:2",
  });

  assert.match(mountain.scheduled_for_local, /^2026-04-02 /);
  assert.ok(parseLocalMinute(mountain.scheduled_for_local) >= 17 * 60 + 5);
  assert.ok(parseLocalMinute(mountain.scheduled_for_local) <= 19 * 60 + 54);
  assert.match(eastern.scheduled_for_local, /^2026-04-03 /);
  assert.ok(parseLocalMinute(eastern.scheduled_for_local) >= 12 * 60 + 5);
  assert.ok(parseLocalMinute(eastern.scheduled_for_local) <= 12 * 60 + 54);
});

test("latency-aware queue schedule delays an in-window reply instead of sending immediately", () => {
  const result = resolveLatencyAwareQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "9AM-8PM CT",
    distribution_key: "owner:1",
    delay_min_minutes: 22,
    delay_max_minutes: 22,
  });

  assert.equal(result.agent_delay_minutes, 22);
  assert.equal(result.scheduled_for_local, "2026-04-02 17:36:00");
  assert.equal(result.scheduled_for_utc, "2026-04-02 22:36:00");
  assert.equal(result.within_contact_window, true);
});

test("latency-aware queue schedule rolls a delayed reply into the next contact window when needed", () => {
  const result = resolveLatencyAwareQueueSchedule({
    now: "2026-04-02T23:30:00Z",
    timezone_label: "Central",
    contact_window: "5PM-6PM CT",
    distribution_key: "owner:late-reply",
    delay_min_minutes: 90,
    delay_max_minutes: 90,
  });

  assert.equal(result.agent_delay_minutes, 90);
  assert.match(result.scheduled_for_local, /^2026-04-03 /);
  assert.equal(result.within_contact_window, false);
  assert.ok(parseLocalMinute(result.scheduled_for_local) >= 17 * 60 + 5);
  assert.ok(parseLocalMinute(result.scheduled_for_local) <= 17 * 60 + 54);
});
