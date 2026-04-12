import test from "node:test";
import assert from "node:assert/strict";

import {
  buildAlwaysOnContactWindow,
  buildFirstContactWindow,
  resolveLatencyAwareQueueSchedule,
  resolveQueueSchedule,
  resolveSchedulingContactWindow,
} from "@/lib/domain/queue/queue-schedule.js";

function parseLocalMinute(value) {
  const match = String(value || "").match(/(\d{2}):(\d{2}):\d{2}$/);
  if (!match) return null;
  return Number(match[1]) * 60 + Number(match[2]);
}

function parseLocalSecond(value) {
  const match = String(value || "").match(/(\d{2}):(\d{2}):(\d{2})$/);
  if (!match) return null;
  return Number(match[3]);
}

test("queue schedule spreads a Central morning window across the next local day instead of pinning to window start", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "7AM-9AM CT",
    distribution_key: "owner:3274320218",
  });

  assert.match(result.scheduled_for_local, /^2026-04-03 /);
  assert.equal(result.reason, "after_contact_window_schedule_next_window");
  assert.equal(result.within_contact_window, false);
  assert.equal(result.scheduled_for_local, "2026-04-03 07:14:43");
  assert.equal(result.scheduled_for_utc, "2026-04-03 12:14:43");
  assert.equal(parseLocalSecond(result.scheduled_for_local), 43);
});

test("queue schedule without a distribution key still pins to window start", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "7AM-9AM CT",
  });

  assert.equal(result.scheduled_for_local, "2026-04-03 07:00:00");
  assert.equal(result.reason, "after_contact_window_schedule_next_window");
  assert.equal(result.within_contact_window, false);
});

test("queue schedule adds a two-minute buffer inside the active contact window", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "9AM-8PM Local",
  });

  assert.equal(result.scheduled_for_local, "2026-04-02 17:16:00");
  assert.equal(result.scheduled_for_utc, "2026-04-02 22:16:00");
  assert.equal(result.reason, "inside_contact_window_schedule_with_spread");
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
  assert.equal(mountain.reason, "before_contact_window_schedule_at_window_start");
  assert.equal(mountain.scheduled_for_local, "2026-04-02 17:02:20");
  assert.equal(mountain.scheduled_for_utc, "2026-04-02 23:02:20");
  assert.match(eastern.scheduled_for_local, /^2026-04-03 /);
  assert.equal(eastern.reason, "after_contact_window_schedule_next_window");
  assert.equal(eastern.scheduled_for_local, "2026-04-03 12:07:35");
  assert.equal(eastern.scheduled_for_utc, "2026-04-03 16:07:35");
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
  assert.equal(result.within_contact_window, true);
  assert.equal(result.reason, "inside_contact_window_schedule_with_spread");
  assert.equal(result.scheduled_for_local, "2026-04-02 17:38:20");
  assert.equal(result.scheduled_for_utc, "2026-04-02 22:38:20");
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
  assert.equal(result.reason, "after_contact_window_schedule_next_window");
  assert.equal(result.scheduled_for_local, "2026-04-03 17:01:44");
  assert.equal(result.scheduled_for_utc, "2026-04-03 22:01:44");
});

test("first-contact scheduling preserves the seller-provided window", () => {
  assert.equal(
    buildFirstContactWindow({
      contact_window: "7AM-9AM CT",
      timezone_label: "Central",
    }),
    "7AM-9AM CT"
  );
});

test("first-contact scheduling preserves seller-specific windows that already fit within quiet hours", () => {
  assert.equal(
    buildFirstContactWindow({
      contact_window: "12PM-2PM CT",
      timezone_label: "Central",
    }),
    "12PM-2PM CT"
  );
});

test("non-first-contact scheduling keeps the seller contact window instead of forcing all-day sending", () => {
  assert.equal(buildAlwaysOnContactWindow("Central"), "12AM-11:59PM CT");
  assert.equal(
    resolveSchedulingContactWindow({
      contact_window: "7AM-9AM CT",
      timezone_label: "Central",
      is_first_contact: false,
    }),
    "7AM-9AM CT"
  );
});

// ── Inside-window distribution tests ─────────────────────────────────────────
// When a batch of rows is processed while already inside the contact window,
// all rows used to get the exact same "now" timestamp.  With a distribution_key
// they are now spread across the remaining window.

test("inside-window rows with different distribution keys get different scheduled times", () => {
  const args = {
    now: "2026-04-04T14:00:00Z", // 9:00 AM Central
    timezone_label: "Central",
    contact_window: "8AM-9PM CT",
  };

  const row_a = resolveQueueSchedule({ ...args, distribution_key: "owner:1001:prop:2001" });
  const row_b = resolveQueueSchedule({ ...args, distribution_key: "owner:1002:prop:2002" });
  const row_c = resolveQueueSchedule({ ...args, distribution_key: "owner:1003:prop:2003" });

  assert.equal(row_a.within_contact_window, true);
  assert.equal(row_b.within_contact_window, true);
  assert.equal(row_c.within_contact_window, true);

  // All rows must be scheduled within the remaining contact window bounds
  const toMinute = (local) => parseLocalMinute(local);
  assert.ok(toMinute(row_a.scheduled_for_local) >= 8 * 60);
  assert.ok(toMinute(row_b.scheduled_for_local) >= 8 * 60);
  assert.ok(toMinute(row_c.scheduled_for_local) >= 8 * 60);
  assert.ok(toMinute(row_a.scheduled_for_local) <= 21 * 60);
  assert.ok(toMinute(row_b.scheduled_for_local) <= 21 * 60);
  assert.ok(toMinute(row_c.scheduled_for_local) <= 21 * 60);

  // At least two of the three rows must have different scheduled times
  const times = new Set([
    row_a.scheduled_for_local,
    row_b.scheduled_for_local,
    row_c.scheduled_for_local,
  ]);
  assert.ok(times.size >= 2, "rows must not all collapse to the same timestamp");
});

test("inside-window schedule reason reflects buffered in-window scheduling when key provided", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-04T14:00:00Z", // 9:00 AM Central — inside 8AM-9PM window
    timezone_label: "Central",
    contact_window: "8AM-9PM CT",
    distribution_key: "owner:9999",
  });

  assert.equal(result.within_contact_window, true);
  assert.equal(
    result.reason,
    "inside_contact_window_schedule_with_spread"
  );
  assert.equal(result.scheduled_for_local, "2026-04-04 16:08:29");
  assert.equal(result.scheduled_for_utc, "2026-04-04 21:08:29");
});

test("inside-window without distribution_key still applies the two-minute minimum buffer", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "9AM-8PM Local",
  });

  assert.equal(result.scheduled_for_local, "2026-04-02 17:16:00");
  assert.equal(result.reason, "inside_contact_window_schedule_with_spread");
  assert.equal(result.within_contact_window, true);
});

test("inside-window with < 5 minutes remaining still respects the minimum buffer inside the window", () => {
  // 8:57 PM Central — only 3 minutes until window end at 9PM
  const result = resolveQueueSchedule({
    now: "2026-04-04T01:57:00Z", // 8:57 PM Central
    timezone_label: "Central",
    contact_window: "8AM-9PM CT",
    distribution_key: "owner:5555",
  });

  assert.equal(result.within_contact_window, true);
  assert.equal(result.reason, "inside_contact_window_schedule_with_spread");
  assert.equal(result.scheduled_for_local, "2026-04-03 20:59:53");
});

test("inside-window distribution is deterministic — same key always gives same time", () => {
  const args = {
    now: "2026-04-04T14:00:00Z",
    timezone_label: "Central",
    contact_window: "8AM-9PM CT",
    distribution_key: "owner:42:prop:99",
  };

  const first = resolveQueueSchedule(args);
  const second = resolveQueueSchedule(args);

  assert.equal(first.scheduled_for_local, second.scheduled_for_local);
  assert.equal(first.scheduled_for_utc, second.scheduled_for_utc);
});
