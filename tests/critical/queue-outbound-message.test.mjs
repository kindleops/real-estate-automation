import test from "node:test";
import assert from "node:assert/strict";

import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";
import { ACTIONS } from "@/lib/sms/flow_map.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  textField,
  numberField,
} from "../helpers/test-helpers.js";

function buildContext(overrides = {}) {
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
      ...overrides.items,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: null,
      market_id: null,
      assigned_agent_id: null,
      ...overrides.ids,
    },
    recent: {
      touch_count: 0,
      recently_used_template_ids: [],
      ...overrides.recent,
    },
    summary: {
      language_preference: "English",
      total_messages_sent: 0,
      market_timezone: "Central",
      contact_window: "8AM-9PM Local",
      phone_activity_status: "Active for 12 months or longer",
      seller_first_name: "Jose",
      agent_name: "Sarah",
      property_address: "5521 Laster Ln",
      property_city: "Dallas",
      conversation_stage: "Ownership Confirmation",
      ...overrides.summary,
    },
  };
}

// Stub deps common to all tests — new SMS engine pipeline
function baseDeps(overrides = {}) {
  return {
    loadContextImpl: async () => buildContext(overrides.contextOverrides),
    classifyImpl: overrides.classifyImpl || (async () => ({
      language: "English",
      objection: null,
      emotion: "calm",
      stage_hint: "Ownership Confirmation",
      compliance_flag: null,
      positive_signals: [],
      confidence: 1,
      motivation_score: 50,
      source: "system",
      notes: "outbound_initiation",
    })),
    resolveRouteImpl: overrides.resolveRouteImpl || (() => ({
      use_case: "ownership_check",
      stage: "Ownership",
      lifecycle_stage: null,
      template_filters: {},
      persona: "Warm Professional",
      primary_category: "Residential",
    })),
    mapNextActionImpl: overrides.mapNextActionImpl || (() => ({
      action: ACTIONS.QUEUE_REPLY,
      use_case: "ownership_check",
      stage_code: "S1",
      reason: "default_ownership_outbound",
      delay_profile: "neutral",
    })),
    resolveTemplateImpl: overrides.resolveTemplateImpl || (() => ({
      resolved: true,
      template_text: "Hi {{seller_first_name}}, are you the owner of {{property_address}}?",
      use_case: "ownership_check",
      stage_code: "S1",
      language: "English",
      agent_style_fit: "Warm Professional",
      source: "csv_catalog",
      resolution_path: ["exact_match"],
      attachable_template_ref: null,
    })),
    personalizeTemplateImpl: overrides.personalizeTemplateImpl || ((text, ctx) => ({
      ok: true,
      text: text
        .replace("{{seller_first_name}}", ctx.seller_first_name || "Friend")
        .replace("{{property_address}}", ctx.property_address || "your property"),
      placeholders_used: ["seller_first_name", "property_address"],
      missing: [],
    })),
    computeScheduledSendImpl: overrides.computeScheduledSendImpl || (() => ({
      scheduled_local: "2025-01-15T10:00:00",
      scheduled_utc: "2025-01-15T16:00:00Z",
      timezone: "Central",
      latency_seconds: 120,
      delay_source: "neutral_band",
    })),
    smsQueueMessageImpl: overrides.smsQueueMessageImpl || (async () => ({
      ok: true,
      item_id: 7771,
      queue_id: "abc123",
      fields: {},
    })),
    chooseTextgridNumberImpl: overrides.chooseTextgridNumberImpl || (async () => ({ item_id: 701 })),
    findQueueItemsImpl: overrides.findQueueItemsImpl || (async () => []),
    ...overrides.extraDeps,
  };
}

// ══════════════════════════════════════════════════════════════════════════
// EXISTING: message_text override bypasses template resolution
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage uses explicit message_text override without forcing template selection", async () => {
  let resolve_template_calls = 0;
  let queue_args = null;

  const result = await queueOutboundMessage(
    {
      phone: "12087034955",
      message_text: "Got it Jose, thanks. Would you be open to an offer on 5521 Laster Ln?",
    },
    baseDeps({
      resolveTemplateImpl: () => {
        resolve_template_calls += 1;
        return { resolved: false };
      },
      smsQueueMessageImpl: async (args) => {
        queue_args = args;
        return { ok: true, item_id: 7771, queue_id: "abc123", fields: {} };
      },
    })
  );

  assert.equal(resolve_template_calls, 0, "template resolver should not be called for explicit message");
  assert.equal(result.ok, true);
  assert.equal(result.queue_item_id, 7771);
  assert.equal(result.message_override_used, true);
  assert.equal(
    queue_args?.rendered_text,
    "Got it Jose, thanks. Would you be open to an offer on 5521 Laster Ln?"
  );
});

// ══════════════════════════════════════════════════════════════════════════
// EXISTING (UPDATED): full pipeline template resolution
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage flows through SMS engine: flow_map → template_resolver → personalize → schedule → queue", async () => {
  let flow_map_called = false;
  let resolve_template_called = false;
  let personalize_called = false;
  let schedule_called = false;
  let queue_called = false;

  const result = await queueOutboundMessage(
    { phone: "12087034955" },
    baseDeps({
      mapNextActionImpl: (args) => {
        flow_map_called = true;
        assert.ok(args.classify_result, "flow_map receives classify_result");
        assert.ok(args.brain_state, "flow_map receives brain_state");
        return {
          action: ACTIONS.QUEUE_REPLY,
          use_case: "ownership_check",
          stage_code: "S1",
          reason: "default_ownership_outbound",
          delay_profile: "neutral",
        };
      },
      resolveTemplateImpl: (args) => {
        resolve_template_called = true;
        assert.equal(args.use_case, "ownership_check");
        assert.equal(args.language, "English");
        return {
          resolved: true,
          template_text: "Hi {{seller_first_name}}, is {{property_address}} yours?",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          agent_style_fit: "Warm Professional",
          source: "csv_catalog",
          attachable_template_ref: null,
        };
      },
      personalizeTemplateImpl: (text, ctx) => {
        personalize_called = true;
        assert.ok(ctx.seller_first_name, "personalize receives seller_first_name");
        assert.ok(ctx.property_address, "personalize receives property_address");
        return {
          ok: true,
          text: "Hi Jose, is 5521 Laster Ln yours?",
          placeholders_used: ["seller_first_name", "property_address"],
          missing: [],
        };
      },
      computeScheduledSendImpl: (args) => {
        schedule_called = true;
        assert.ok(args.now_utc, "scheduler receives now_utc");
        assert.ok(args.timezone, "scheduler receives timezone");
        return {
          scheduled_local: "2025-01-15T10:00:00",
          scheduled_utc: "2025-01-15T16:00:00Z",
          timezone: "Central",
          latency_seconds: 120,
          delay_source: "neutral_band",
        };
      },
      smsQueueMessageImpl: async (args) => {
        queue_called = true;
        assert.equal(args.rendered_text, "Hi Jose, is 5521 Laster Ln yours?");
        assert.ok(args.schedule, "queue receives schedule");
        assert.ok(args.links, "queue receives links");
        assert.equal(args.links.master_owner_id, 201);
        assert.equal(args.links.phone_id, 401);
        return { ok: true, item_id: 7772, queue_id: "def456", fields: {} };
      },
    })
  );

  assert.equal(result.ok, true);
  assert.equal(result.queue_item_id, 7772);
  assert.equal(result.pipeline, "sms_engine_v2");
  assert.equal(flow_map_called, true, "flow_map was called");
  assert.equal(resolve_template_called, true, "template_resolver was called");
  assert.equal(personalize_called, true, "personalize_template was called");
  assert.equal(schedule_called, true, "computeScheduledSend was called");
  assert.equal(queue_called, true, "queueMessage was called");
});

// ══════════════════════════════════════════════════════════════════════════
// NEW: compliance STOP from flow_map aborts queue creation
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage returns STOP when classify detects opt-out compliance", async () => {
  const result = await queueOutboundMessage(
    { phone: "12087034955", seed_message: "STOP" },
    baseDeps({
      classifyImpl: async () => ({
        language: "English",
        objection: null,
        emotion: "calm",
        stage_hint: null,
        compliance_flag: "stop_texting",
        positive_signals: [],
        confidence: 1,
        motivation_score: 0,
        source: "heuristic",
      }),
      mapNextActionImpl: () => ({
        action: ACTIONS.STOP,
        use_case: null,
        stage_code: null,
        reason: "compliance_stop",
        cancel_queued: true,
      }),
    })
  );

  assert.equal(result.ok, false);
  assert.equal(result.stage, "flow_map");
  assert.equal(result.action, ACTIONS.STOP);
  assert.equal(result.cancel_queued, true);
});

// ══════════════════════════════════════════════════════════════════════════
// NEW: template resolver failure returns template_not_found
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage returns template_not_found when resolver fails", async () => {
  const result = await queueOutboundMessage(
    { phone: "12087034955" },
    baseDeps({
      resolveTemplateImpl: () => ({
        resolved: false,
        fallback_reason: "no_matching_template_for_mf_landlord",
      }),
    })
  );

  assert.equal(result.ok, false);
  assert.equal(result.stage, "template");
  assert.equal(result.reason, "no_matching_template_for_mf_landlord");
});

// ══════════════════════════════════════════════════════════════════════════
// NEW: personalization failure returns missing placeholders
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage returns personalization_failed when placeholders missing", async () => {
  const result = await queueOutboundMessage(
    { phone: "12087034955" },
    baseDeps({
      personalizeTemplateImpl: () => ({
        ok: false,
        text: null,
        missing: ["offer_price"],
        reason: "missing_required_placeholders",
      }),
    })
  );

  assert.equal(result.ok, false);
  assert.equal(result.stage, "render");
  assert.match(result.reason, /personalization_failed|missing_required/);
});

// ══════════════════════════════════════════════════════════════════════════
// NEW: queue_message receives all enrichment fields
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage passes 15+ enrichment fields to smsQueueMessage", async () => {
  let queue_args = null;

  const result = await queueOutboundMessage(
    { phone: "12087034955", dnc_check: "✅ Cleared", delivery_confirmed: "⏳ Pending" },
    baseDeps({
      contextOverrides: {
        ids: {
          phone_item_id: 401,
          master_owner_id: 201,
          prospect_id: 301,
          property_id: 501,
          market_id: 601,
          assigned_agent_id: null,
        },
        summary: {
          language_preference: "English",
          seller_first_name: "Jose",
          agent_name: "Sarah",
          property_address: "5521 Laster Ln",
          property_city: "Dallas",
          property_type: "Residential",
          owner_type: "Individual",
          market_timezone: "Central",
          contact_window: "8AM-9PM Local",
          conversation_stage: "Ownership Confirmation",
        },
      },
      smsQueueMessageImpl: async (args) => {
        queue_args = args;
        return { ok: true, item_id: 7773, queue_id: "ghi789", fields: {} };
      },
    })
  );

  assert.equal(result.ok, true);
  assert.ok(queue_args, "queueMessage was called");

  // Links
  assert.equal(queue_args.links.master_owner_id, 201);
  assert.equal(queue_args.links.prospect_id, 301);
  assert.equal(queue_args.links.property_id, 501);
  assert.equal(queue_args.links.phone_id, 401);
  assert.equal(queue_args.links.market_id, 601);
  assert.equal(queue_args.links.textgrid_number_id, 701);

  // Context enrichment
  assert.equal(queue_args.context.property_address, "5521 Laster Ln");
  assert.equal(queue_args.context.property_type, "Residential");
  assert.equal(queue_args.context.owner_type, "Individual");
  assert.equal(queue_args.context.dnc_check, "✅ Cleared");
  assert.equal(queue_args.context.delivery_confirmed, "⏳ Pending");
  assert.ok(queue_args.context.send_priority, "send_priority is set");
  assert.ok(queue_args.context.touch_number != null, "touch_number is set");
  assert.ok(queue_args.context.contact_window, "contact_window is set");
  assert.ok(Array.isArray(queue_args.context.placeholders_used), "placeholders_used is array");

  // Schedule
  assert.ok(queue_args.schedule, "schedule is passed");
  assert.ok(queue_args.schedule.scheduled_local, "scheduled_local is set");

  // Resolution
  assert.ok(queue_args.resolution, "resolution is passed");
  assert.equal(queue_args.resolution.use_case, "ownership_check");
});

// ══════════════════════════════════════════════════════════════════════════
// NEW: caller schedule override bypasses latency engine
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage uses caller-provided schedule when scheduled_for_local is present", async () => {
  let queue_args = null;
  let compute_schedule_called = false;

  const result = await queueOutboundMessage(
    {
      phone: "12087034955",
      scheduled_for_local: "2025-06-01T14:30:00",
      scheduled_for_utc: "2025-06-01T19:30:00Z",
    },
    baseDeps({
      computeScheduledSendImpl: () => {
        compute_schedule_called = true;
        return { scheduled_local: "WRONG", scheduled_utc: "WRONG", timezone: "WRONG" };
      },
      smsQueueMessageImpl: async (args) => {
        queue_args = args;
        return { ok: true, item_id: 7774, queue_id: "jkl012", fields: {} };
      },
    })
  );

  assert.equal(result.ok, true);
  assert.equal(compute_schedule_called, false, "latency engine should not be called for explicit schedule");
  assert.equal(queue_args.schedule.scheduled_local, "2025-06-01T14:30:00");
  assert.equal(queue_args.schedule.scheduled_utc, "2025-06-01T19:30:00Z");
  assert.equal(queue_args.schedule.delay_source, "caller_override");
});

// ══════════════════════════════════════════════════════════════════════════
// NEW: duplicate guard still works with new pipeline
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage rejects duplicate pending queue item", async () => {
  const result = await queueOutboundMessage(
    { phone: "12087034955" },
    baseDeps({
      findQueueItemsImpl: async () => [
        createPodioItem(8881, {
          "queue-status": categoryField("Queued"),
          "phone-number": appRefField(401),
          "touch-number": numberField(1),
        }),
      ],
    })
  );

  assert.equal(result.ok, false);
  assert.equal(result.stage, "duplicate_guard");
  assert.equal(result.reason, "duplicate_pending_queue_item");
});

// ══════════════════════════════════════════════════════════════════════════
// NEW: caller_pre_routed bypasses ESCALATE / WAIT gates
// ══════════════════════════════════════════════════════════════════════════

test("queueOutboundMessage: explicit use_case bypasses ESCALATE gate", async () => {
  // flow_map returns ESCALATE (the bug scenario), but caller provided use_case
  const result = await queueOutboundMessage(
    { phone: "12087034955", use_case: "consider_selling" },
    baseDeps({
      mapNextActionImpl: () => ({
        action: ACTIONS.ESCALATE,
        use_case: null,
        stage_code: null,
        reason: "unrecognized_stage",
        delay_profile: "neutral",
      }),
    })
  );

  // Should NOT get ok:false from ESCALATE — caller already routed
  assert.equal(result.ok, true, "pre-routed call should bypass ESCALATE");
  assert.equal(result.stage, "queued");
});

test("queueOutboundMessage: explicit use_case bypasses WAIT gate", async () => {
  const result = await queueOutboundMessage(
    { phone: "12087034955", use_case: "ownership_check" },
    baseDeps({
      mapNextActionImpl: () => ({
        action: ACTIONS.WAIT,
        use_case: null,
        stage_code: null,
        reason: "wait_for_reply",
        delay_profile: "neutral",
      }),
    })
  );

  assert.equal(result.ok, true, "pre-routed call should bypass WAIT");
  assert.equal(result.stage, "queued");
});

test("queueOutboundMessage: explicit use_case does NOT bypass STOP gate", async () => {
  // STOP is a compliance gate — always honoured
  const result = await queueOutboundMessage(
    { phone: "12087034955", use_case: "consider_selling" },
    baseDeps({
      mapNextActionImpl: () => ({
        action: ACTIONS.STOP,
        use_case: null,
        stage_code: null,
        reason: "compliance_stop",
        cancel_queued: true,
      }),
    })
  );

  assert.equal(result.ok, false, "STOP is always honoured");
  assert.equal(result.action, ACTIONS.STOP);
});

test("queueOutboundMessage: ESCALATE still blocks when no explicit use_case", async () => {
  const result = await queueOutboundMessage(
    { phone: "12087034955" },
    baseDeps({
      mapNextActionImpl: () => ({
        action: ACTIONS.ESCALATE,
        use_case: null,
        stage_code: null,
        reason: "unrecognized_stage",
      }),
    })
  );

  assert.equal(result.ok, false, "ESCALATE should block without explicit use_case");
  assert.equal(result.stage, "flow_map");
  assert.equal(result.action, ACTIONS.ESCALATE);
});

test("queueOutboundMessage: WAIT still blocks when no explicit use_case", async () => {
  const result = await queueOutboundMessage(
    { phone: "12087034955" },
    baseDeps({
      mapNextActionImpl: () => ({
        action: ACTIONS.WAIT,
        use_case: null,
        stage_code: null,
        reason: "wait_for_reply",
      }),
    })
  );

  assert.equal(result.ok, false, "WAIT should block without explicit use_case");
  assert.equal(result.stage, "flow_map");
  assert.equal(result.action, ACTIONS.WAIT);
});
