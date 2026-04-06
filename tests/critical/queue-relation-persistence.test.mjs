import test from "node:test";
import assert from "node:assert/strict";

import APP_IDS from "@/lib/config/app-ids.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";
import { buildOutboundMessageEventFields } from "@/lib/domain/events/log-outbound-message-event.js";
import {
  buildFailedOutboundMessageEventFields,
  validateQueuedOutboundNumberItem,
} from "@/lib/domain/queue/process-send-queue.js";
import { mapTextgridFailureBucket } from "@/lib/providers/textgrid.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  dateField,
  textField,
} from "../helpers/test-helpers.js";

function createActivePhoneItem(item_id = 401) {
  return createPodioItem(item_id, {
    "phone-activity-status": categoryField("Active for 12 months or longer"),
    "phone-hidden": textField("<p>9188102617</p>"),
    "canonical-e164": textField("+19188102617"),
    "linked-master-owner": appRefField(201),
    "linked-contact": appRefField(301),
  });
}

test("send queue row persists property, template, phone, and master owner relations", async () => {
  let created_fields = null;

  const result = await buildSendQueueItem({
    context: {
      found: true,
      items: {
        phone_item: createActivePhoneItem(),
        brain_item: createPodioItem(701, {
          properties: appRefField(601),
        }),
        master_owner_item: createPodioItem(201),
        property_item: null,
        agent_item: null,
        market_item: null,
      },
      ids: {
        phone_item_id: 401,
        master_owner_id: 201,
        prospect_id: 301,
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
    queue_id: "relation-test",
    rendered_message_text: "Hi there",
    template_id: 901,
    template_item: {
      item_id: 901,
      raw: {
        app: {
          app_id: APP_IDS.templates,
        },
      },
    },
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-04-04 12:43:17",
    scheduled_for_utc: "2026-04-04 17:43:17",
    create_item: async (_app_id, fields) => {
      created_fields = fields;
      return { item_id: 123 };
    },
    update_item: async () => {},
  });

  assert.equal(result.ok, true);
  assert.deepEqual(created_fields["phone-number"], [401]);
  assert.deepEqual(created_fields["master-owner"], [201]);
  assert.deepEqual(created_fields.prospects, [301]);
  assert.deepEqual(created_fields.properties, [601]);
  assert.deepEqual(created_fields.template, [901]);
});

test("outbound send event payload preserves phone, property, template, and conversation relations", () => {
  const fields = buildOutboundMessageEventFields({
    brain_item: createPodioItem(701, {
      "ai-route": categoryField("Soft"),
    }),
    conversation_item_id: 701,
    master_owner_id: 201,
    prospect_id: 301,
    property_id: 601,
    market_id: 801,
    phone_item_id: 401,
    outbound_number_item_id: 501,
    message_body: "Test message",
    provider_message_id: "provider-1",
    queue_item_id: 123,
    client_reference_id: "queue-123",
    template_id: 901,
    message_variant: 2,
    send_result: {
      ok: true,
      status: "sent",
    },
  });

  assert.deepEqual(fields["phone-number"], [401]);
  assert.deepEqual(fields.property, [601]);
  assert.deepEqual(fields["template-selected"], [901]);
  assert.deepEqual(fields.conversation, [701]);
});

test("failed-send event payload preserves phone, property, template, and conversation relations", () => {
  const fields = buildFailedOutboundMessageEventFields({
    brain_item: createPodioItem(701, {
      "ai-route": categoryField("Soft"),
    }),
    conversation_item_id: 701,
    queue_item_id: 123,
    master_owner_id: 201,
    prospect_id: 301,
    property_id: 601,
    market_id: 801,
    phone_item_id: 401,
    outbound_number_item_id: 501,
    template_id: 901,
    message_body: "Test message",
    message_variant: 2,
    send_result: {
      ok: false,
      error_status: 404,
      error_message: "Invalid Number",
      message_id: null,
    },
    retry_count: 0,
    max_retries: 3,
    client_reference_id: "queue-123",
  });

  assert.deepEqual(fields["phone-number"], [401]);
  assert.deepEqual(fields.property, [601]);
  assert.deepEqual(fields["template-selected"], [901]);
  assert.deepEqual(fields.conversation, [701]);
  assert.equal(fields["failure-bucket"], "Hard Bounce");
});

test("outbound number preflight rejects paused or stale sending numbers deterministically", () => {
  const invalid_sender = createPodioItem(501, {
    title: textField("not-a-phone"),
    status: categoryField("_ Paused"),
    "hard-pause": categoryField("Yes"),
    "pause-until": dateField("2026-04-05T12:00:00.000Z"),
  });

  const validation = validateQueuedOutboundNumberItem(
    invalid_sender,
    new Date("2026-04-04T12:00:00.000Z")
  );

  assert.equal(validation.ok, false);
  assert.equal(validation.reason, "outbound_number_phone_invalid");
});

test("invalid-number preflight messaging still buckets as a hard bounce", () => {
  const bucket = mapTextgridFailureBucket({
    ok: false,
    error_status: "preflight_invalid_number",
    error_message: "Invalid sending number for TextGrid item 501: outbound_number_phone_invalid",
  });

  assert.equal(bucket, "Hard Bounce");
});
