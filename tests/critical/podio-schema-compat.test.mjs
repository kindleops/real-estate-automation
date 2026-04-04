import test from "node:test";
import assert from "node:assert/strict";

import APP_IDS from "@/lib/config/app-ids.js";
import { normalizePodioFieldMap } from "@/lib/podio/schema.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";

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

test("send queue contact-window allows derived first-contact quiet-hour windows", () => {
  const fields = normalizePodioFieldMap(APP_IDS.send_queue, {
    "contact-window": "8AM-9AM CT",
  });

  assert.equal(fields["contact-window"], "8AM-9AM CT");
});

test("ai conversation brain linked-message-events accepts message event app refs", () => {
  const fields = normalizePodioFieldMap(APP_IDS.ai_conversation_brain, {
    "linked-message-events": [30541681, "30541682"],
  });

  assert.deepEqual(fields["linked-message-events"], [30541681, 30541682]);
});

test("queue builder preserves live Master Owners contact-window values on create", async () => {
  let created_fields = null;

  const result = await buildSendQueueItem({
    context: {
      found: true,
      items: {
        phone_item: {
          item_id: 1,
          fields: [
            { external_id: "phone-activity-status", values: [{ value: { text: "Active for 12 months or longer" } }] },
            { external_id: "phone-hidden", values: [{ value: "<p>9188102617</p>" }] },
          ],
        },
        master_owner_item: null,
        property_item: null,
        agent_item: null,
        market_item: null,
      },
      ids: {
        phone_item_id: 1,
        master_owner_id: null,
        prospect_id: null,
        property_id: null,
        market_id: null,
        assigned_agent_id: null,
      },
      recent: {
        touch_count: 0,
      },
      summary: {
        total_messages_sent: 0,
      },
    },
    queue_id: "compat-test",
    scheduled_for_local: "2026-04-04 12:00:00",
    scheduled_for_utc: "2026-04-04T17:00:00.000Z",
    timezone: "Central",
    contact_window: "12PM-2PM CT",
    send_priority: "_ Urgent",
    queue_status: "Queued",
    phone_item_id: 1,
    textgrid_number_item_id: 2,
    message_type: "Cold Outbound",
    rendered_message_text: "Hi there",
    dnc_check: "FALSE",
    create_item: async (_app_id, fields) => {
      created_fields = fields;
      return { item_id: 123 };
    },
  });

  assert.equal(result.queue_item_id, 123);
  assert.equal(created_fields["contact-window"], "12PM-2PM CT");
});
