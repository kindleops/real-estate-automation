import test from "node:test";
import assert from "node:assert/strict";

import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

function buildContext() {
  return {
    found: true,
    items: {
      brain_item: null,
      phone_item: createPodioItem(401, {
        "phone-activity-status": categoryField("Active for 12 months or longer"),
        "phone-hidden": textField("2087034955"),
        "canonical-e164": textField("+12087034955"),
        "linked-master-owner": appRefField(201),
        "linked-contact": appRefField(301),
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
      recently_used_template_ids: [],
    },
    summary: {
      language_preference: "English",
      total_messages_sent: 0,
      market_timezone: "Central",
      contact_window: "8AM-9PM Local",
      phone_activity_status: "Active for 12 months or longer",
    },
  };
}

test("queueOutboundMessage uses explicit message_text override without forcing template selection", async () => {
  let load_template_calls = 0;
  let build_send_queue_args = null;

  const result = await queueOutboundMessage(
    {
      phone: "12087034955",
      message_text: "Got it Jose, thanks. Would you be open to an offer on 5521 Laster Ln?",
    },
    {
      loadContextImpl: async () => buildContext(),
      resolveRouteImpl: () => ({
        use_case: "ownership_check",
        variant_group: "Stage 1 — Ownership Confirmation",
        tone: "Warm",
        stage: "Ownership",
        lifecycle_stage: null,
        template_filters: {},
        persona: "Warm Professional",
      }),
      loadTemplateImpl: async () => {
        load_template_calls += 1;
        return {
          item_id: 9991,
          source: "podio",
          text: "ignored",
        };
      },
      chooseTextgridNumberImpl: async () => ({ item_id: 701 }),
      findQueueItemsImpl: async () => [],
      buildSendQueueItemImpl: async (args) => {
        build_send_queue_args = args;
        return {
          ok: true,
          queue_item_id: 7771,
          template_relation_id: null,
          template_app_field_written: false,
        };
      },
    }
  );

  assert.equal(load_template_calls, 0);
  assert.equal(result.ok, true);
  assert.equal(result.queue_item_id, 7771);
  assert.equal(result.template_id, null);
  assert.equal(result.message_override_used, true);
  assert.equal(
    build_send_queue_args?.rendered_message_text,
    "Got it Jose, thanks. Would you be open to an offer on 5521 Laster Ln?"
  );
});

test("queueOutboundMessage enables stage-based Podio template fallback for live selection", async () => {
  let load_template_args = null;
  let load_context_args = null;

  const result = await queueOutboundMessage(
    {
      phone: "12087034955",
    },
    {
      loadContextImpl: async (args) => {
        load_context_args = args;
        return buildContext();
      },
      resolveRouteImpl: () => ({
        use_case: "ownership_check",
        variant_group: "Stage 1 — Ownership Confirmation",
        tone: "Warm",
        stage: "Ownership",
        lifecycle_stage: null,
        template_filters: {},
        persona: "Warm Professional",
      }),
      loadTemplateImpl: async (args) => {
        load_template_args = args;
        return {
          item_id: 9992,
          source: "podio",
          template_resolution_source: "podio_template",
          use_case: "ownership_check",
          variant_group: "Stage 1 — Ownership Confirmation",
          text: "Hi {{seller_first_name}}, are you the owner of {{property_address}}?",
        };
      },
      renderTemplateImpl: () => ({
        rendered_text: "Hi Sam, are you the owner of 5521 Laster Ln?",
        used_placeholders: ["{{seller_first_name}}", "{{property_address}}"],
      }),
      chooseTextgridNumberImpl: async () => ({ item_id: 701 }),
      findQueueItemsImpl: async () => [],
      buildSendQueueItemImpl: async () => ({
        ok: true,
        queue_item_id: 7772,
        template_relation_id: 9992,
        template_app_field_written: true,
      }),
    }
  );

  assert.equal(result.ok, true);
  assert.equal(load_context_args?.create_brain_if_missing, false);
  assert.equal(load_template_args?.allow_variant_group_fallback, true);
});
