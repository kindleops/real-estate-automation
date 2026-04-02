import test from "node:test";
import assert from "node:assert/strict";

import APP_IDS from "@/lib/config/app-ids.js";
import { normalizePodioFieldMap } from "@/lib/podio/schema.js";

test("message events source-app preserves attached-schema ids for known base options", () => {
  const fields = normalizePodioFieldMap(APP_IDS.message_events, {
    "source-app": "Send Queue",
  });

  assert.equal(fields["source-app"], 1);
});

test("message events source-app allows known compatibility labels to pass through", () => {
  const runtime_lock_fields = normalizePodioFieldMap(APP_IDS.message_events, {
    "source-app": "Runtime Lock",
  });
  const system_alert_fields = normalizePodioFieldMap(APP_IDS.message_events, {
    "source-app": "System Alert",
  });
  const buyer_thread_fields = normalizePodioFieldMap(APP_IDS.message_events, {
    "source-app": "Buyer Thread",
  });

  assert.equal(runtime_lock_fields["source-app"], "Runtime Lock");
  assert.equal(system_alert_fields["source-app"], "System Alert");
  assert.equal(buyer_thread_fields["source-app"], "Buyer Thread");
});

test("message events source-app still rejects unknown labels", () => {
  assert.throws(
    () =>
      normalizePodioFieldMap(APP_IDS.message_events, {
        "source-app": "Definitely Not Real",
      }),
    /Invalid category value/
  );
});

test("send queue contact-window accepts the live Master Owners schedule values", () => {
  const representative_values = [
    "12PM-2PM CT",
    "7AM-9AM CT",
    "12PM-1PM ET",
    "5PM-8PM MT",
    "9AM-8PM Local",
  ];

  for (const value of representative_values) {
    const fields = normalizePodioFieldMap(APP_IDS.send_queue, {
      "contact-window": value,
    });

    assert.equal(fields["contact-window"], value);
  }
});

test("send queue contact-window still preserves attached-schema ids for known base options", () => {
  const fields = normalizePodioFieldMap(APP_IDS.send_queue, {
    "contact-window": "9AM-8PM CT",
  });

  assert.equal(fields["contact-window"], 1);
});

test("send queue contact-window still rejects unknown labels", () => {
  assert.throws(
    () =>
      normalizePodioFieldMap(APP_IDS.send_queue, {
        "contact-window": "2AM-3AM Mars",
      }),
    /Invalid category value/
  );
});
