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
