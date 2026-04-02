import test from "node:test";
import assert from "node:assert/strict";

import { resolveQueueSchedule } from "@/lib/domain/queue/queue-schedule.js";

test("queue schedule moves a Central morning window to the next local day when the window has passed", () => {
  const result = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Central",
    contact_window: "7AM-9AM CT",
  });

  assert.equal(result.scheduled_for_local, "2026-04-03 07:00:00");
  assert.equal(result.scheduled_for_utc, "2026-04-03 12:00:00");
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
  });
  const eastern = resolveQueueSchedule({
    now: "2026-04-02T22:14:00Z",
    timezone_label: "Eastern",
    contact_window: "12PM-1PM ET",
  });

  assert.equal(mountain.scheduled_for_local, "2026-04-02 17:00:00");
  assert.equal(mountain.scheduled_for_utc, "2026-04-02 23:00:00");
  assert.equal(eastern.scheduled_for_local, "2026-04-03 12:00:00");
  assert.equal(eastern.scheduled_for_utc, "2026-04-03 16:00:00");
});
