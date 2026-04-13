/**
 * touch-one-queue-integrity.test.mjs
 *
 * Hard-fail assertions for Touch 1 (Stage 1 / first-touch) queue row creation.
 *
 * The pipeline rules:
 *   1. loadTemplateCandidates throws NO_STAGE_1_TEMPLATE_FOUND when
 *      strict_touch_one_podio_only=true and no valid Stage-1 template is found
 *      in Podio (local templates are explicitly excluded).
 *   2. buildSendQueueItem throws MISSING_CONTACT_WINDOW when strict_cold_outbound
 *      is true and the contact_window value has no matching category option in
 *      the Send Queue schema (i.e. the row cannot be routed by the queue runner).
 *   3. The valid path completes without errors when all required components are
 *      present (template_id, message_text, ownership_check use_case, resolvable
 *      contact_window, valid scheduled_local).
 */

import test from "node:test";
import assert from "node:assert/strict";

import { loadTemplateCandidates } from "@/lib/domain/templates/load-template.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";
import { validateSendQueueItem } from "@/lib/domain/queue/validate-send-queue-item.js";
import {
  createPodioItem,
  categoryField,
  numberField,
  textField,
  appRefField,
} from "../helpers/test-helpers.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function createActivePhoneItem() {
  return createPodioItem(401, {
    "phone-activity-status": categoryField("Active for 12 months or longer"),
    "phone-hidden": textField("2085550101"),
    "canonical-e164": textField("+12085550101"),
  });
}

function makeBaseContext(overrides = {}) {
  return {
    found: true,
    items: {
      phone_item: createActivePhoneItem(),
      brain_item: null,
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
    recent: { touch_count: 0 },
    summary: {
      total_messages_sent: 0,
      ...overrides.summary,
    },
  };
}

// A no-op remote fetcher — no Podio templates available.
async function noRemoteFetch() {
  return [];
}

function makeLocalFetcher(templates) {
  return () => templates;
}

function makeLocalTemplate(item_id, opts = {}) {
  return {
    item_id,
    use_case: opts.use_case ?? "ownership_check",
    variant_group: opts.variant_group ?? "Stage 1 — Ownership Confirmation",
    stage_code: opts.stage_code ?? "S1",
    stage_label: opts.stage_label ?? "Ownership Confirmation",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    is_first_touch: opts.is_first_touch ?? "Yes",
    property_type_scope: opts.property_type_scope ?? "Any Residential",
    text: opts.text ?? "Hi there, checking on your property. Do you still own it?",
    active: "Yes",
    deliverability_score: 80,
    spam_risk: 2,
    historical_reply_rate: 20,
    total_sends: 0,
    total_replies: 0,
    total_conversations: 0,
    cooldown_days: 3,
    version: 1,
    source: "local_registry",
  };
}

// ── FIX 1: NO_STAGE_1_TEMPLATE_FOUND ─────────────────────────────────────────

test("loadTemplateCandidates throws NO_STAGE_1_TEMPLATE_FOUND when strict_touch_one_podio_only has no Podio results", async () => {
  await assert.rejects(
    () =>
      loadTemplateCandidates({
        use_case: "ownership_check",
        language: "English",
        strict_touch_one_podio_only: true,
        remote_fetcher: noRemoteFetch,
        local_fetcher: makeLocalFetcher([
          // Local templates are NOT used in strict Podio-only mode.
          makeLocalTemplate("local-s1"),
        ]),
      }),
    (err) => {
      assert.equal(
        err.code,
        "NO_STAGE_1_TEMPLATE_FOUND",
        "error code must be NO_STAGE_1_TEMPLATE_FOUND"
      );
      assert.ok(
        err.diagnostics,
        "diagnostics object must be attached to the error"
      );
      return true;
    }
  );
});

test("loadTemplateCandidates throws NO_STAGE_1_TEMPLATE_FOUND when Podio returns templates but none pass Touch 1 truth filters", async () => {
  const wrong_use_case_template = makeLocalTemplate("podio-wrong-uc");
  wrong_use_case_template.use_case = "asking_price";
  wrong_use_case_template.source = "podio";

  const not_first_touch_template = makeLocalTemplate("podio-not-first-touch", {
    is_first_touch: "No",
  });
  not_first_touch_template.source = "podio";

  await assert.rejects(
    () =>
      loadTemplateCandidates({
        use_case: "ownership_check",
        language: "English",
        strict_touch_one_podio_only: true,
        remote_fetcher: async () => [wrong_use_case_template, not_first_touch_template],
        local_fetcher: makeLocalFetcher([]),
      }),
    (err) => {
      assert.equal(err.code, "NO_STAGE_1_TEMPLATE_FOUND");
      return true;
    }
  );
});

test("loadTemplateCandidates succeeds when Podio returns a valid Stage-1 ownership template", async () => {
  const valid_template = makeLocalTemplate("podio-valid-9001");
  valid_template.source = "podio";

  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    remote_fetcher: async () => [valid_template],
    local_fetcher: makeLocalFetcher([]),
  });

  assert.ok(candidates.length > 0, "must return at least one candidate");
  assert.equal(
    candidates[0].item_id,
    "podio-valid-9001",
    "the valid Podio Stage-1 template must be selected"
  );
});

// ── FIX 7: MISSING_CONTACT_WINDOW ────────────────────────────────────────────

test("buildSendQueueItem throws MISSING_CONTACT_WINDOW when strict_cold_outbound and contact_window has no valid format", async () => {
  // "morning calls preferred" is not a valid time-range and is not in the compat set.
  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: makeBaseContext(),
        rendered_message_text: "Hi Jose, checking on Elm St. Do you still own it?",
        template_id: 9901,
        template_item: {
          item_id: 9901,
          source: "podio",
          title: "Ownership Check",
          raw: { app: { app_id: 30647181 } },
        },
        strict_cold_outbound: true,
        contact_window: "morning calls preferred",
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-06-01 09:00:00",
        scheduled_for_utc: "2026-06-01 14:00:00",
        create_item: async () => ({ item_id: 5001 }),
        update_item: async () => {},
      }),
    (err) => {
      assert.equal(
        err.code,
        "MISSING_CONTACT_WINDOW",
        "error code must be MISSING_CONTACT_WINDOW"
      );
      assert.ok(
        err.contact_window,
        "error must include the unresolved contact_window value"
      );
      return true;
    }
  );
});

test("buildSendQueueItem succeeds with a real Master Owner compat contact window (12PM-2PM CT)", async () => {
  // "12PM-2PM CT" is a real-world Master Owner contact window that is in the
  // compat set.  Before the resolveContactWindowField fix it was omitted, causing
  // MISSING_CONTACT_WINDOW to throw for strict_cold_outbound rows.
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi Jose, checking on Elm St. Do you still own it?",
    template_id: 9901,
    template_item: {
      item_id: 9901,
      source: "podio",
      title: "Ownership Check",
      raw: { app: { app_id: 30647181 } },
    },
    strict_cold_outbound: true,
    contact_window: "12PM-2PM CT",
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-06-01 12:00:00",
    scheduled_for_utc: "2026-06-01 17:00:00",
    create_item: async () => ({ item_id: 5002 }),
    update_item: async () => {},
  });

  assert.equal(result.ok, true, "queue creation must succeed with compat contact_window");
  assert.equal(result.contact_window_written, true, "12PM-2PM CT must be written via compat layer");
});

test("buildSendQueueItem succeeds when strict_cold_outbound and contact_window matches schema option", async () => {
  // "9AM-8PM CT" is in the Send Queue attached schema (id=1).
  const result = await buildSendQueueItem({
    context: makeBaseContext(),
    rendered_message_text: "Hi Jose, checking on Elm St. Do you still own it?",
    template_id: 9901,
    template_item: {
      item_id: 9901,
      source: "podio",
      title: "Ownership Check",
      raw: { app: { app_id: 30647181 } },
    },
    strict_cold_outbound: true,
    contact_window: "9AM-8PM CT",
    textgrid_number_item_id: 501,
    scheduled_for_local: "2026-06-01 09:00:00",
    scheduled_for_utc: "2026-06-01 14:00:00",
    create_item: async () => ({ item_id: 5003 }),
    update_item: async () => {},
  });

  assert.equal(result.ok, true, "queue creation must succeed with valid contact_window");
  assert.equal(result.contact_window_written, true, "contact_window must be written to queue row");
  assert.equal(
    result.use_case_template_value,
    "ownership_check",
    "use_case_template must be forced to ownership_check"
  );
  assert.equal(
    result.message_type_value,
    "Cold Outbound",
    "message_type must be forced to Cold Outbound"
  );
});

// ── FIX 5: NO_TEMPLATE and NO_MESSAGE hard enforcement ───────────────────────

test("buildSendQueueItem throws NO_TEMPLATE when strict_cold_outbound and template_id is missing", async () => {
  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: makeBaseContext(),
        rendered_message_text: "Hi Jose, checking on Elm St.",
        template_id: null,
        strict_cold_outbound: true,
        contact_window: "9AM-8PM CT",
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-06-01 09:00:00",
        scheduled_for_utc: "2026-06-01 14:00:00",
        create_item: async () => ({ item_id: 6001 }),
        update_item: async () => {},
      }),
    (err) => {
      assert.equal(err.code, "NO_TEMPLATE");
      return true;
    }
  );
});

test("buildSendQueueItem throws when strict_cold_outbound and message_text is empty", async () => {
  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: makeBaseContext(),
        rendered_message_text: "",
        template_id: 9901,
        template_item: {
          item_id: 9901,
          source: "podio",
          title: "Ownership Check",
          raw: { app: { app_id: 30647181 } },
        },
        strict_cold_outbound: true,
        contact_window: "9AM-8PM CT",
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-06-01 09:00:00",
        scheduled_for_utc: "2026-06-01 14:00:00",
        create_item: async () => ({ item_id: 6002 }),
        update_item: async () => {},
      }),
    (err) => {
      // Empty message_text is caught early before the strict_cold_outbound block.
      assert.ok(err.message.includes("missing rendered_message_text") || err.code === "NO_MESSAGE");
      return true;
    }
  );
});

test("buildSendQueueItem throws NO_MESSAGE when strict_cold_outbound and message_text is too short", async () => {
  await assert.rejects(
    () =>
      buildSendQueueItem({
        context: makeBaseContext(),
        rendered_message_text: "Hi",
        template_id: 9901,
        template_item: {
          item_id: 9901,
          source: "podio",
          title: "Ownership Check",
          raw: { app: { app_id: 30647181 } },
        },
        strict_cold_outbound: true,
        contact_window: "9AM-8PM CT",
        textgrid_number_item_id: 501,
        scheduled_for_local: "2026-06-01 09:00:00",
        scheduled_for_utc: "2026-06-01 14:00:00",
        create_item: async () => ({ item_id: 6003 }),
        update_item: async () => {},
      }),
    (err) => {
      assert.equal(err.code, "NO_MESSAGE");
      assert.equal(err.reason, "too_short");
      return true;
    }
  );
});

// ── FIX 10: Send-time validation blocks Touch 1 with wrong use case ──────────

test("validateSendQueueItem rejects Touch 1 queue item with non-ownership use case", () => {
  const queue_item = createPodioItem(7001, {
    "queue-status": categoryField("Queued"),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "message-text": textField("Hey, ready to close the deal on your property?"),
    "touch-number": numberField(1),
    "use-case-template": categoryField("offer_reveal_cash"),
  });

  const result = validateSendQueueItem(queue_item);

  assert.equal(result.ok, false);
  assert.equal(result.reason, "invalid_touch_one_use_case");
  assert.equal(result.touch_number, 1);
  assert.equal(result.use_case_template, "offer_reveal_cash");
});

test("validateSendQueueItem accepts Touch 1 queue item with ownership_check use case", () => {
  const queue_item = createPodioItem(7002, {
    "queue-status": categoryField("Queued"),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "message-text": textField("Hi there, checking on your property. Do you still own it?"),
    "touch-number": numberField(1),
    "use-case-template": categoryField("ownership_check"),
  });

  const result = validateSendQueueItem(queue_item);

  assert.equal(result.ok, true, "Touch 1 with ownership_check must pass validation");
});

test("validateSendQueueItem accepts Touch 2+ queue item with any use case", () => {
  const queue_item = createPodioItem(7003, {
    "queue-status": categoryField("Queued"),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "message-text": textField("Following up on our conversation about your property."),
    "touch-number": numberField(3),
    "use-case-template": categoryField("asking_price_follow_up"),
  });

  const result = validateSendQueueItem(queue_item);

  assert.equal(result.ok, true, "Touch 2+ with any use case must pass validation");
});

// ── FIX 1: Renderability filter skip for Touch 1 ─────────────────────────────

test("loadTemplateCandidates does NOT filter by renderability when strict_touch_one_podio_only is true", async () => {
  // Template has placeholders that would fail renderability (missing context values)
  // but should still be selected for Touch 1.
  const template_with_placeholders = makeLocalTemplate("podio-t1-9100");
  template_with_placeholders.source = "podio";
  template_with_placeholders.text =
    "Hi {{seller_first_name}}, checking on {{property_address}}. Still own it?";

  // Context has NO seller_first_name or property_address — normally filtered out.
  const candidates = await loadTemplateCandidates({
    use_case: "ownership_check",
    language: "English",
    strict_touch_one_podio_only: true,
    context: {
      found: true,
      items: {},
      ids: {},
      recent: { touch_count: 0 },
      summary: { total_messages_sent: 0 },
    },
    remote_fetcher: async () => [template_with_placeholders],
    local_fetcher: () => [],
  });

  assert.ok(candidates.length > 0, "Touch 1 must NOT drop templates due to missing placeholders");
  assert.equal(candidates[0].item_id, "podio-t1-9100");
});
