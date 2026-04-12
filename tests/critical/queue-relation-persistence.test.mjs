import test from "node:test";
import assert from "node:assert/strict";

import APP_IDS from "@/lib/config/app-ids.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";
import { buildOutboundMessageEventFields } from "@/lib/domain/events/log-outbound-message-event.js";
import {
  buildFailedOutboundMessageEventFields,
  validateQueuedOutboundNumberItem,
} from "@/lib/domain/queue/process-send-queue.js";
import { PodioError } from "@/lib/providers/podio.js";
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

test("send queue row persists property, template, phone, and master owner relations for Podio templates", async () => {
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
    template_id: 9901,
    template_item: {
      item_id: 9901,
      template_id: 901,
      source: "podio",
      title: "Ownership Check V1",
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
  assert.deepEqual(created_fields["template-2"], [9901]);
  assert.equal(created_fields["current-stage"], "Ownership Confirmation");
  assert.equal(result.selected_template_id, 9901);
  assert.equal(result.selected_template_source, "podio");
  assert.equal(result.selected_template_title, "Ownership Check V1");
  assert.equal(result.template_relation_id, 9901);
  assert.equal(result.template_app_field_written, true);
  assert.equal(result.template_attached, true);
  assert.equal(result.selected_template_resolution_source, "podio_template");
  assert.equal(result.template_attachment_strategy, "selected_template_item_id");
});

test("send queue row uses the direct Podio template item on the live template field", async () => {
  const create_attempts = [];

  const result = await buildSendQueueItem({
    context: {
      found: true,
      items: {
        phone_item: createActivePhoneItem(),
        brain_item: null,
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
    queue_id: "template-direct-fallback",
    rendered_message_text: "Hi there",
    template_id: 9901,
    template_item: {
      item_id: 9901,
      template_id: 901,
      source: "podio",
      template_resolution_source: "podio_template",
      title: "Ownership Check V1",
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
      create_attempts.push(fields);
      return { item_id: 125 };
    },
    update_item: async () => {},
  });

  assert.equal(create_attempts.length, 1);
  assert.deepEqual(create_attempts[0]["template-2"], [9901]);
  assert.equal(result.template_relation_id, 9901);
  assert.equal(result.template_app_field_written, true);
  assert.equal(result.template_attached, true);
  assert.equal(result.template_attachment_strategy, "selected_template_item_id");
});

test("strict cold outbound row forces Touch 1 stage fields even when conflicting inputs are passed", async () => {
  let created_fields = null;

  const result = await buildSendQueueItem({
    context: {
      found: true,
      items: {
        phone_item: createActivePhoneItem(),
        brain_item: null,
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
    queue_id: "strict-touch-one",
    rendered_message_text: "Hi Jose, checking on 3124 Rodeo St. Do you still own it?",
    template_id: 9901,
    template_item: {
      item_id: 9901,
      source: "podio",
      title: "Ownership Check V1",
      raw: {
        app: {
          app_id: APP_IDS.templates,
        },
      },
    },
    message_type: "Follow-Up",
    current_stage: "Offer",
    use_case_template: "offer_reveal_cash",
    strict_cold_outbound: true,
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-04-04 12:43:17",
    scheduled_for_utc: "2026-04-04 17:43:17",
    create_item: async (_app_id, fields) => {
      created_fields = fields;
      return { item_id: 126 };
    },
    update_item: async () => {},
  });

  assert.equal(result.ok, true);
  assert.equal(created_fields["message-type"], "Cold Outbound");
  assert.deepEqual(created_fields["template-2"], [9901]);
  assert.equal(result.message_type_value, "Cold Outbound");
  assert.equal(result.current_stage_value, "Cold Outbound");
  assert.equal(result.use_case_template_value, "ownership_check");
  assert.equal(result.template_relation_id, 9901);
  if ("current-stage" in (created_fields || {})) {
    assert.equal(created_fields["current-stage"], "Cold Outbound");
  }
});

test("strict cold outbound rejects non-numeric Podio template ids", async () => {
  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: {
          found: true,
          items: {
            phone_item: createActivePhoneItem(),
            brain_item: null,
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
        queue_id: "strict-touch-one-invalid-template-id",
        rendered_message_text: "Hi Jose, checking on 3124 Rodeo St. Do you still own it?",
        template_id: "9901",
        template_item: {
          item_id: 9901,
          source: "podio",
          title: "Ownership Check V1",
          raw: {
            app: {
              app_id: APP_IDS.templates,
            },
          },
        },
        current_stage: "Cold Outbound",
        use_case_template: "ownership_check",
        strict_cold_outbound: true,
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-04-04 12:43:17",
        scheduled_for_utc: "2026-04-04 17:43:17",
        create_item: async () => ({ item_id: 127 }),
        update_item: async () => {},
      }),
    (error) => error?.code === "INVALID_TEMPLATE_ID"
  );
});

test("strict cold outbound rejects Touch 1 message text with offer language", async () => {
  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: {
          found: true,
          items: {
            phone_item: createActivePhoneItem(),
            brain_item: null,
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
        queue_id: "strict-touch-one-invalid-message",
        rendered_message_text: "Hi Jose, would you take an offer on 3124 Rodeo St?",
        template_id: 9901,
        template_item: {
          item_id: 9901,
          source: "podio",
          title: "Ownership Check V1",
          raw: {
            app: {
              app_id: APP_IDS.templates,
            },
          },
        },
        current_stage: "Cold Outbound",
        use_case_template: "ownership_check",
        strict_cold_outbound: true,
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-04-04 12:43:17",
        scheduled_for_utc: "2026-04-04 17:43:17",
        create_item: async () => ({ item_id: 128 }),
        update_item: async () => {},
      }),
    (error) => error?.code === "INVALID_STAGE_1_MESSAGE" && error?.phrase === "offer"
  );
});

test("strict cold outbound rejects Podio templates without a resolved relation id", async () => {
  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: {
          found: true,
          items: {
            phone_item: createActivePhoneItem(),
            brain_item: null,
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
        queue_id: "strict-touch-one-missing-template-relation",
        rendered_message_text: "Hi Jose, checking on 3124 Rodeo St. Do you still own it?",
        template_item: {
          source: "podio",
          title: "Ownership Check V1",
          raw: {
            app: {
              app_id: APP_IDS.templates,
            },
          },
        },
        current_stage: "Cold Outbound",
        use_case_template: "ownership_check",
        strict_cold_outbound: true,
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-04-04 12:43:17",
        scheduled_for_utc: "2026-04-04 17:43:17",
        create_item: async () => ({ item_id: 129 }),
        update_item: async () => {},
      }),
    (error) => error?.code === "MISSING_TEMPLATE_RELATION"
  );
});

test("strict cold outbound does not silently create a queue row when Podio rejects the template relation", async () => {
  let create_attempts = 0;

  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: {
          found: true,
          items: {
            phone_item: createActivePhoneItem(),
            brain_item: null,
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
        queue_id: "strict-touch-one-relation-rejected",
        rendered_message_text: "Hi Jose, checking on 3124 Rodeo St. Do you still own it?",
        template_id: 9901,
        template_item: {
          item_id: 9901,
          source: "podio",
          title: "Ownership Check V1",
          raw: {
            app: {
              app_id: APP_IDS.templates,
            },
          },
        },
        current_stage: "Cold Outbound",
        use_case_template: "ownership_check",
        strict_cold_outbound: true,
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-04-04 12:43:17",
        scheduled_for_utc: "2026-04-04 17:43:17",
        create_item: async () => {
          create_attempts += 1;
          throw new PodioError("Invalid value for template relation", {
            status: 400,
            path: "/item/app/30680653/",
            data: {
              error: "invalid_value",
            },
          });
        },
        update_item: async () => {},
      }),
    (error) => error?.code === "MISSING_TEMPLATE_RELATION"
  );

  assert.equal(
    create_attempts,
    1,
    "Podio template relation rejection must not retry by creating a template-less queue row"
  );
});

test("send queue row still queues successfully when local template fallback is selected", async () => {
  let created_fields = null;

  const result = await buildSendQueueItem({
    context: {
      found: true,
      items: {
        phone_item: createActivePhoneItem(),
        brain_item: null,
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
    queue_id: "local-template-test",
    rendered_message_text: "Hi there",
    template_id: "local-template:ownership_check:no-agent:v1",
    template_item: {
      item_id: "local-template:ownership_check:no-agent:v1",
      source: "local_registry",
      title: null,
    },
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-04-04 12:43:17",
    scheduled_for_utc: "2026-04-04 17:43:17",
    create_item: async (_app_id, fields) => {
      created_fields = fields;
      return { item_id: 124 };
    },
    update_item: async () => {},
  });

  assert.equal(result.ok, true);
  assert.equal("template-2" in (created_fields || {}), false);
  assert.equal(result.selected_template_id, "local-template:ownership_check:no-agent:v1");
  assert.equal(result.selected_template_source, "local_registry");
  assert.equal(result.template_relation_id, null);
  assert.equal(result.template_app_field_written, false);
  assert.equal(result.template_attached, false);
  assert.equal(result.selected_template_resolution_source, "local_template_fallback");
  assert.equal(result.template_attachment_reason, "local_template_not_attachable");
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
  assert.deepEqual(fields.template, [901]);
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
  assert.deepEqual(fields.template, [901]);
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
