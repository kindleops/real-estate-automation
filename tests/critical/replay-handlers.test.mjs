import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  handleTextgridInboundWebhook,
  __setTextgridInboundTestDeps,
  __resetTextgridInboundTestDeps,
} from "@/lib/flows/handle-textgrid-inbound.js";
import {
  handleTextgridDeliveryWebhook,
  __setTextgridDeliveryTestDeps,
  __resetTextgridDeliveryTestDeps,
} from "@/lib/flows/handle-textgrid-delivery.js";
import {
  handleDocusignWebhook,
  __setDocusignWebhookTestDeps,
  __resetDocusignWebhookTestDeps,
} from "@/lib/domain/contracts/handle-docusign-webhook.js";
import {
  handleTitleResponseWebhook,
  __setTitleWebhookTestDeps,
  __resetTitleWebhookTestDeps,
} from "@/lib/domain/title/handle-title-response-webhook.js";
import {
  handleClosingResponseWebhook,
  __setClosingWebhookTestDeps,
  __resetClosingWebhookTestDeps,
} from "@/lib/domain/closings/handle-closing-response-webhook.js";
import {
  appRefField,
  categoryField,
  createInMemoryIdempotencyLedger,
  createPodioItem,
  textField,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetTextgridInboundTestDeps();
  __resetTextgridDeliveryTestDeps();
  __resetDocusignWebhookTestDeps();
  __resetTitleWebhookTestDeps();
  __resetClosingWebhookTestDeps();
  if (globalThis.__replay_route_test_mod__?.__resetReplayInboundTestDeps) {
    globalThis.__replay_route_test_mod__.__resetReplayInboundTestDeps();
  }
});

async function getReplayRouteModule() {
  process.env.PODIO_CLIENT_ID ||= "test_podio_client_id";
  process.env.PODIO_CLIENT_SECRET ||= "test_podio_client_secret";
  process.env.PODIO_USERNAME ||= "test_podio_username";
  process.env.PODIO_PASSWORD ||= "test_podio_password";

  if (!globalThis.__replay_route_test_mod__) {
    globalThis.__replay_route_test_mod__ = await import("@/app/api/internal/testing/replay-inbound/route.js");
  }
  return globalThis.__replay_route_test_mod__;
}

function makeReplayRequest(payload) {
  return new Request("http://localhost/api/internal/testing/replay-inbound", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-internal-api-secret": String(process.env.INTERNAL_API_SECRET || ""),
    },
    body: JSON.stringify(payload ?? {}),
  });
}

test("replay-inbound accepts message_body/from_number/to_number and returns dry-run safety response", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({ language: "English", confidence: 0.99 }),
    extractUnderwritingSignals: () => ({ property_type: "Single Family", creative_strategy: "cash" }),
    routeSellerConversation: () => ({
      handled: true,
      should_queue_reply: true,
      next_expected_stage: "consider_selling",
      selected_use_case: "consider_selling",
      template_lookup_use_case: "consider_selling",
      detected_intent: "Ownership Confirmed",
      reasoning_summary: "Seller confirmed ownership",
      detected_language: "English",
    }),
    maybeQueueSellerStageReply: async ({ queue_message }) => {
      if (queue_message) {
        await queue_message({ dry_run: true, would_enqueue: true });
      }
      return { ok: true, queued: false, dry_run: true };
    },
    loadTemplate: async () => ({
      text: "Thanks for confirming.",
      template_id: "tmpl_1",
      item_id: "tmpl_1",
      language: "English",
      selector_use_case: "consider_selling",
      source: "local_registry",
      template_resolution_source: "local_template_fallback",
    }),
    personalizeTemplate: () => ({ ok: true, text: "Thanks for confirming.", missing: [] }),
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "Yes I own it",
      from_number: "+16127433952",
      to_number: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.dry_run, true);
  assert.equal(json.message_body, "Yes I own it");
  assert.equal(json.from_number, "+16127433952");
  assert.equal(json.to_number, "+19048774448");
  assert.equal(json.would_queue_reply, true);
  assert.equal(Boolean(json.selected_template_source), true);
  assert.equal(typeof json.rendered_message_text, "string");
  assert.equal(json.safety?.sms_sent, false);
  assert.equal(json.safety?.queue_created, false);
  assert.equal(json.safety?.podio_mutated, false);
});

test("replay-inbound accepts aliases body/message/from/to and normalizes fields", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({ language: "English" }),
    extractUnderwritingSignals: () => ({}),
    routeSellerConversation: () => ({
      handled: true,
      should_queue_reply: false,
      next_expected_stage: "ownership_check",
      selected_use_case: "ownership_check",
      template_lookup_use_case: "ownership_check",
    }),
    maybeQueueSellerStageReply: async () => ({ ok: true, queued: false }),
    loadTemplate: async () => null,
    personalizeTemplate: () => ({ ok: true, text: "n/a", missing: [] }),
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      body: "Yes I own it",
      from: "+16127433952",
      to: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.message_body, "Yes I own it");
  assert.equal(json.from_number, "+16127433952");
  assert.equal(json.to_number, "+19048774448");
  assert.equal(json.dry_run, true);

  const response_message_alias = await replayInboundPost(
    makeReplayRequest({
      message: "Still me",
      from: "+16120000000",
      to: "+19040000000",
      dry_run: true,
    })
  );
  const json_message_alias = await response_message_alias.json();
  assert.equal(response_message_alias.status, 200);
  assert.equal(json_message_alias.message_body, "Still me");
  assert.equal(json_message_alias.from_number, "+16120000000");
  assert.equal(json_message_alias.to_number, "+19040000000");
});

test("replay-inbound does not fail when lifecycle CSV is missing and logs replay.template_csv_missing", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";
  const warnings = [];

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({ language: "English" }),
    extractUnderwritingSignals: () => ({}),
    routeSellerConversation: () => ({
      handled: true,
      should_queue_reply: true,
      next_expected_stage: "consider_selling",
      selected_use_case: "consider_selling",
      template_lookup_use_case: "consider_selling",
    }),
    maybeQueueSellerStageReply: async ({ queue_message }) => {
      if (queue_message) await queue_message({ dry_run: true });
      return { ok: true, queued: false };
    },
    loadTemplate: async () => {
      throw new Error("ENOENT: no such file or directory, open '/vercel/path0/docs/templates/lifecycle-sms-template-pack.csv'");
    },
    personalizeTemplate: () => ({ ok: true, text: "n/a", missing: [] }),
    warn: (event, meta) => warnings.push({ event, meta }),
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message: "Yes I own it",
      from: "+16127433952",
      to: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.dry_run, true);
  assert.equal(json.safety?.queue_created, false);
  assert.equal(json.selected_template_source, null);
  assert.ok(warnings.some((entry) => entry.event === "replay.template_csv_missing"));
});

test("replay-inbound pipeline failures do not leak INTERNAL_API_SECRET", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret_very_private";

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => {
      throw new Error(`boom ${process.env.INTERNAL_API_SECRET}`);
    },
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "Yes I own it",
      from_number: "+16127433952",
      to_number: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 500);
  const serialized = JSON.stringify(json);
  assert.ok(!serialized.includes(process.env.INTERNAL_API_SECRET));
  assert.equal(json.error, "pipeline_error");
});

test("replay wrong number suppresses auto-reply, resolves no template, and stays terminal", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";
  let load_template_called = false;

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({
      language: "English",
      objection: "wrong_number",
      confidence: 0.99,
    }),
    loadTemplate: async () => {
      load_template_called = true;
      return {
        text: "<p>これは間違いでした</p>",
        template_id: "tmpl_wrong_person",
        language: "Japanese",
        selector_use_case: "wrong_person",
        source: "local_registry",
      };
    },
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "Wrong number",
      from_number: "+16127433952",
      to_number: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.detected_intent, "Ownership Denied / Wrong Person");
  assert.equal(json.selected_use_case, "wrong_person");
  assert.equal(json.next_stage, "terminal");
  assert.equal(json.would_queue_reply, false);
  assert.equal(json.suppression_reason, "wrong_number");
  assert.equal(json.rendered_message_text, null);
  assert.equal(json.selected_template_source, null);
  assert.equal(load_template_called, false);
  assert.equal(json.safety?.sms_sent, false);
  assert.equal(json.safety?.queue_created, false);
  assert.equal(json.safety?.podio_mutated, false);
  assert.ok(!JSON.stringify(json).includes("Japanese"));
});

test("replay rendered_message_text is sanitized before returning output", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({ language: "English" }),
    extractUnderwritingSignals: () => ({}),
    routeSellerConversation: () => ({
      handled: true,
      should_queue_reply: true,
      next_expected_stage: "consider_selling",
      selected_use_case: "consider_selling",
      template_lookup_use_case: "consider_selling",
      detected_intent: "Ownership Confirmed",
      reasoning_summary: "Seller confirmed ownership",
      detected_language: "English",
    }),
    maybeQueueSellerStageReply: async () => ({ ok: true, queued: false, dry_run: true }),
    loadTemplate: async () => ({
      text: "<p>Thanks <strong>for confirming</strong>.</p>",
      template_id: "tmpl_html",
      item_id: "tmpl_html",
      language: "English",
      selector_use_case: "consider_selling",
      source: "local_registry",
      template_resolution_source: "local_template_fallback",
    }),
    personalizeTemplate: () => ({ ok: true, text: "<p>Thanks <strong>for confirming</strong>.</p>", missing: [] }),
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "Yes I own it",
      from_number: "+16127433952",
      to_number: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.rendered_message_text, "Thanks for confirming.");
  assert.equal(/<[^>]+>/.test(json.rendered_message_text), false);
  assert.ok(
    json.alignment_assertions.some(
      (assertion) => assertion.assertion === "rendered_message_text_no_html_tags" && assertion.ok
    )
  );
});

test("replay offer request without verified ownership gates back to ownership_check", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({
      language: "English",
      objection: "send_offer_first",
      confidence: 0.99,
    }),
    loadTemplate: async ({ use_case }) => ({
      text: use_case === "ownership_check" ? "Just to confirm, are you the owner of the property?" : "wrong",
      template_id: "tmpl_ownership_gate",
      item_id: "tmpl_ownership_gate",
      language: "English",
      selector_use_case: use_case,
      source: "local_registry",
      template_resolution_source: "local_template_fallback",
    }),
    personalizeTemplate: (text) => ({ ok: true, text, missing: [] }),
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "How much are you offering?",
      from_number: "+16127433952",
      to_number: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.selected_use_case, "ownership_check");
  assert.equal(json.template_lookup_use_case, "ownership_check");
  assert.equal(json.next_stage, "ownership_check");
  assert.equal(json.would_queue_reply, true);
  assert.equal(json.selected_template_use_case, "ownership_check");
  assert.match(json.rendered_message_text, /confirm/i);
  assert.equal(json.safety?.queue_created, false);
});

test("replay tenant response without verified ownership does not auto-reply", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";
  let load_template_called = false;

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({ language: "English", confidence: 0.95 }),
    loadTemplate: async () => {
      load_template_called = true;
      return null;
    },
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "It is rented with tenants",
      from_number: "+16127433952",
      to_number: "+19048774448",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.detected_intent, "Property Info Provided");
  assert.equal(json.would_queue_reply, false);
  assert.equal(json.suppression_reason, "seller_flow_not_handled");
  assert.equal(json.rendered_message_text, null);
  assert.equal(load_template_called, false);
  assert.equal(json.safety?.queue_created, false);
});

test("replay Spanish source-of-info question routes to who_is_this dry-run reply", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";
  let template_lookup = null;

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({ language: "English", confidence: 0.95 }),
    extractUnderwritingSignals: () => ({}),
    loadTemplate: async (query) => {
      template_lookup = query;
      return {
        text: "Soy Chris. Trabajo con un comprador local y te escribi sobre la propiedad.",
        template_id: "tmpl_who_spanish",
        item_id: "tmpl_who_spanish",
        language: "Spanish",
        selector_use_case: "who_is_this",
        use_case: "who_is_this",
        stage_code: "ownership_check",
        source: "local_registry",
        template_resolution_source: "local_template_fallback",
      };
    },
    personalizeTemplate: (_template, _context) => ({
      ok: true,
      text: "Soy Chris. Trabajo con un comprador local y te escribi sobre la propiedad.",
      missing: [],
    }),
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "Hola buenas como encontraste mi información??",
      from_number: "+17133781814",
      to_number: "+12818458577",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.classification.language, "Spanish");
  assert.equal(json.classification.raw_language, "English");
  assert.equal(json.detected_language, "Spanish");
  assert.equal(json.detected_intent, "source_of_info_question");
  assert.equal(json.selected_use_case, "who_is_this");
  assert.equal(json.template_lookup_use_case, "who_is_this");
  assert.equal(json.would_queue_reply, true);
  assert.equal(json.rendered_message_text, "Soy Chris. Trabajo con un comprador local y te escribi sobre la propiedad.");
  assert.equal(json.safety?.sms_sent, false);
  assert.equal(json.safety?.queue_created, false);
  assert.equal(template_lookup?.use_case, "who_is_this");
  assert.equal(template_lookup?.language, "Spanish");
});

test("replay STOP opt-out still suppresses auto replies", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";
  let load_template_called = false;

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({
      language: "English",
      compliance_flag: "stop_texting",
      confidence: 0.99,
    }),
    loadTemplate: async () => {
      load_template_called = true;
      return null;
    },
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "STOP",
      from_number: "+17133781814",
      to_number: "+12818458577",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.selected_use_case, "stop_or_opt_out");
  assert.equal(json.next_stage, "terminal");
  assert.equal(json.would_queue_reply, false);
  assert.equal(json.suppression_reason, "seller_flow_no_auto_reply_needed");
  assert.equal(json.rendered_message_text, null);
  assert.equal(load_template_called, false);
  assert.equal(json.safety?.sms_sent, false);
  assert.equal(json.safety?.queue_created, false);
});

test("replay tenant response with ownership-confirmed context routes to underwriting follow-up", async () => {
  process.env.INTERNAL_API_SECRET = "replay_test_secret";

  const {
    POST: replayInboundPost,
    __setReplayInboundTestDeps,
  } = await getReplayRouteModule();

  __setReplayInboundTestDeps({
    classify: async () => ({ language: "English", confidence: 0.95 }),
    loadTemplate: async ({ use_case }) => ({
      text:
        use_case === "ask_condition_clarifier"
          ? "Got it. Are the tenants month-to-month or on a lease, and what does it rent for?"
          : "unexpected",
      template_id: "tmpl_underwriting_follow_up",
      item_id: "tmpl_underwriting_follow_up",
      language: "English",
      selector_use_case: use_case,
      source: "local_registry",
      template_resolution_source: "local_template_fallback",
    }),
    personalizeTemplate: (text) => ({ ok: true, text, missing: [] }),
    warn: () => {},
  });

  const response = await replayInboundPost(
    makeReplayRequest({
      message_body: "It is rented with tenants",
      from_number: "+16127433952",
      to_number: "+19048774448",
      prior_stage: "consider_selling",
      prior_use_case: "consider_selling",
      dry_run: true,
    })
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.ok, true);
  assert.equal(json.selected_use_case, "ask_condition_clarifier");
  assert.equal(json.template_lookup_use_case, "ask_condition_clarifier");
  assert.equal(json.would_queue_reply, true);
  assert.match(json.rendered_message_text, /lease|rent/i);
  assert.equal(json.safety?.queue_created, false);
});

test("inbound webhook ignores replay after first completion", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let logInboundCount = 0;
  const inboundLogPayloads = [];
  let updateBrainAfterInboundCount = 0;
  let createOfferCount = 0;

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    info: () => {},
    warn: () => {},
    loadContext: async () => ({
      found: true,
      ids: {
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Offer",
      use_case: "offer_follow_up",
      seller_profile: "motivated",
    }),
    logInboundMessageEvent: async (payload) => {
      logInboundCount += 1;
      inboundLogPayloads.push(payload);
    },
    updateBrainAfterInbound: async () => {
      updateBrainAfterInboundCount += 1;
    },
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async () => {
      createOfferCount += 1;
      return { ok: true, created: false };
    },
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: true }),
    maybeQueueUnderwritingFollowUp: async () => ({ ok: true, queued: false }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const payload = {
    message_id: "sms-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "I am interested.",
    status: "received",
  };

  const first = await handleTextgridInboundWebhook(payload);
  const second = await handleTextgridInboundWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.duplicate, undefined);
  assert.equal(second.ok, true);
  assert.equal(second.duplicate, true);
  assert.equal(logInboundCount, 1);
  assert.equal(inboundLogPayloads[0].conversation_item_id, 11);
  assert.equal(updateBrainAfterInboundCount, 1);
  assert.equal(createOfferCount, 1);
});

test("inbound webhook suppresses underwriting follow-up when seller-stage reply is handled", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const stage_updates = [];
  let underwriting_follow_up_count = 0;

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    info: () => {},
    warn: () => {},
    loadContext: async () => ({
      found: true,
      ids: {
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Ownership",
      use_case: "ownership_check",
      seller_profile: null,
    }),
    logInboundMessageEvent: async () => {},
    updateBrainAfterInbound: async () => {},
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async (payload) => {
      stage_updates.push(payload);
      return { ok: true };
    },
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async () => ({ ok: true, created: false }),
    maybeUpsertUnderwritingFromInbound: async () => ({ ok: true, extracted: true }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      queued: true,
      handled: true,
      reason: "seller_flow_reply_queued",
      brain_stage: "Offer",
      plan: {
        selected_use_case: "consider_selling",
      },
    }),
    maybeQueueUnderwritingFollowUp: async () => {
      underwriting_follow_up_count += 1;
      return { ok: true, queued: true };
    },
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-stage-1",
    from: "+15550000001",
    to: "+15550000002",
    body: "Yes, I own it.",
    status: "received",
  }, {
    auto_reply_enabled: true,
  });

  assert.equal(result.ok, true);
  assert.equal(result.seller_stage_reply.queued, true);
  assert.equal(result.underwriting_follow_up.reason, "suppressed_by_seller_stage_reply");
  assert.equal(underwriting_follow_up_count, 0);
  assert.deepEqual(
    stage_updates.map((entry) => entry.stage),
    ["Offer"]
  );
});

test("inbound webhook does not run a second offer pass when the initial offer already exists", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const create_offer_calls = [];

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    info: () => {},
    warn: () => {},
    loadContext: async () => ({
      found: true,
      ids: {
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Offer",
      use_case: "offer_reveal",
      seller_profile: "motivated",
    }),
    routeInboundOffer: async () => ({
      ok: true,
      offer_route: "no_offer_signal",
      reason: "legacy_test_explicitly_bypassed",
      meta: {},
    }),
    logInboundMessageEvent: async () => {},
    updateBrainAfterInbound: async () => {},
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async (payload) => {
      create_offer_calls.push(payload);
      return { ok: true, created: true, offer: { offer_item_id: 901 } };
    },
    maybeUpsertUnderwritingFromInbound: async () => ({
      ok: true,
      extracted: true,
      strategy: { auto_offer_ready: true },
      signals: { underwriting_auto_offer_ready: true },
    }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      queued: false,
      handled: false,
    }),
    maybeQueueUnderwritingFollowUp: async () => ({
      ok: true,
      queued: true,
      offer_ready: true,
    }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-offer-single-pass",
    from: "+15550000001",
    to: "+15550000002",
    body: "Make me an offer.",
    status: "received",
  });

  assert.equal(result.ok, true);
  assert.equal(create_offer_calls.length, 1);
  assert.equal(create_offer_calls[0].respect_underwriting_gate, undefined);
});

test("inbound webhook runs a single ungated second offer pass only after underwriting is ready", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const create_offer_calls = [];

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    info: () => {},
    warn: () => {},
    loadContext: async () => ({
      found: true,
      ids: {
        brain_item_id: 11,
        master_owner_id: 21,
        prospect_id: 31,
        property_id: 41,
        phone_item_id: 51,
      },
      items: {
        brain_item: createPodioItem(11),
        phone_item: createPodioItem(51),
      },
    }),
    classify: async () => ({
      language: "English",
      source: "test",
    }),
    resolveRoute: () => ({
      stage: "Offer",
      use_case: "offer_reveal",
      seller_profile: "motivated",
    }),
    routeInboundOffer: async () => ({
      ok: true,
      offer_route: "no_offer_signal",
      reason: "legacy_test_explicitly_bypassed",
      meta: {},
    }),
    logInboundMessageEvent: async () => {},
    updateBrainAfterInbound: async () => {},
    updateMasterOwnerAfterInbound: async () => ({ ok: true }),
    updateBrainStage: async () => ({ ok: true }),
    updateBrainLanguage: async () => ({ ok: true }),
    updateBrainSellerProfile: async () => ({ ok: true }),
    findLatestOpenOffer: async () => null,
    maybeProgressOfferStatus: async () => ({ ok: true, updated: false }),
    maybeCreateOfferFromContext: async (payload) => {
      create_offer_calls.push(payload);
      if (create_offer_calls.length === 1) {
        return { ok: true, created: false, reason: "offer_requires_underwriting_first" };
      }
      return { ok: true, created: true, offer: { offer_item_id: 902 } };
    },
    maybeUpsertUnderwritingFromInbound: async () => ({
      ok: true,
      extracted: true,
      strategy: { auto_offer_ready: false },
      signals: { underwriting_auto_offer_ready: false },
    }),
    maybeQueueSellerStageReply: async () => ({
      ok: true,
      queued: false,
      handled: false,
    }),
    maybeQueueUnderwritingFollowUp: async () => ({
      ok: true,
      queued: true,
      offer_ready: true,
    }),
    maybeCreateContractFromAcceptedOffer: async () => ({ ok: true, created: false }),
    syncPipelineState: async () => ({ pipeline_item_id: 61, current_stage: "Offer" }),
  });

  const result = await handleTextgridInboundWebhook({
    message_id: "sms-offer-second-pass",
    from: "+15550000001",
    to: "+15550000002",
    body: "Make me an offer.",
    status: "received",
  }, {
    auto_reply_enabled: true,
  });

  assert.equal(result.ok, true);
  assert.equal(create_offer_calls.length, 2);
  assert.equal(create_offer_calls[0].respect_underwriting_gate, undefined);
  assert.equal(create_offer_calls[1].respect_underwriting_gate, false);
});

test("delivery webhook ignores replay after exact queue correlation succeeds", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const queueUpdates = [];
  const deliveryLogPayloads = [];
  let deliveryEventCount = 0;
  let eventStatusUpdateCount = 0;
  let brainDeliveryUpdateCount = 0;

  const outboundEvent = createPodioItem(801, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("outbound:queue-123"),
    "text-2": textField("provider-1"),
    "processed-by": categoryField("Scheduled Campaign"),
    "source-app": categoryField("Send Queue"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByProviderMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async (item_id, payload) => {
      queueUpdates.push({ item_id, payload });
    },
    logDeliveryEvent: async (payload) => {
      deliveryEventCount += 1;
      deliveryLogPayloads.push(payload);
    },
    updateMessageEventStatus: async () => {
      eventStatusUpdateCount += 1;
    },
    findBestBrainMatch: async () => null,
    findLatestBrainByProspectId: async () => createPodioItem(701),
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => {
      throw new Error("should_not_update_phone");
    },
    updateBrainAfterDelivery: async () => {
      brainDeliveryUpdateCount += 1;
    },
    mapTextgridFailureBucket: () => "Soft Bounce",
  });

  const payload = {
    message_id: "provider-1",
    status: "delivered",
    client_reference_id: "queue-123",
  };

  const first = await handleTextgridDeliveryWebhook(payload);
  const second = await handleTextgridDeliveryWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.correlation_mode, "client_reference");
  assert.equal(second.ok, true);
  // Without the idempotency ledger the second call simply re-applies the
  // same status update (idempotent), so `duplicate` is no longer set.
  assert.equal(queueUpdates.length, 2);
  assert.equal(eventStatusUpdateCount, 2);
  assert.equal(brainDeliveryUpdateCount, 2);
});

test("delivery webhook updates verification send events without queue correlation", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let deliveryEventCount = 0;
  let eventStatusUpdateCount = 0;

  const verificationEvent = createPodioItem(811, {
    "trigger-name": textField("verification-textgrid-send:run-1"),
    "message-id": textField("provider-verify-1"),
    "text-2": textField("provider-verify-1"),
    "ai-output": textField(
      JSON.stringify({
        verification_run_id: "run-1",
        client_reference_id: "verify-textgrid-run-1",
        provider_message_id: "provider-verify-1",
      })
    ),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByProviderMessageId: async () => [verificationEvent],
    getItem: async () => null,
    fetchAllItems: async () => [],
    updateItem: async () => {
      throw new Error("queue_should_not_be_updated");
    },
    logDeliveryEvent: async () => {
      deliveryEventCount += 1;
    },
    updateMessageEventStatus: async () => {
      eventStatusUpdateCount += 1;
    },
    findBestBrainMatch: async () => null,
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => null,
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Other",
  });

  const result = await handleTextgridDeliveryWebhook({
    message_id: "provider-verify-1",
    status: "delivered",
  });

  assert.equal(result.ok, true);
  assert.equal(result.queue_item_count, 0);
  assert.equal(result.matched_event_count, 1);
  assert.equal(eventStatusUpdateCount, 1);
});

test("delivery webhook resolves brain through the phone-linked brain before owner/prospect fallback", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const deliveryLogPayloads = [];

  const outboundEvent = createPodioItem(812, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("outbound:queue-123"),
    "text-2": textField("provider-phone-brain-1"),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-phone-brain-1",
      })
    ),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    prospects: appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByProviderMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async () => {},
    logDeliveryEvent: async (payload) => {
      deliveryLogPayloads.push(payload);
    },
    updateMessageEventStatus: async () => {},
    findBestBrainMatch: async ({ phone_item_id, prospect_id, master_owner_id }) => {
      assert.equal(phone_item_id, 401);
      assert.equal(prospect_id, 301);
      assert.equal(master_owner_id, 201);
      return createPodioItem(702);
    },
    findLatestBrainByProspectId: async () => {
      throw new Error("prospect_fallback_should_not_run_when_phone_brain_exists");
    },
    findLatestBrainByMasterOwnerId: async () => {
      throw new Error("owner_fallback_should_not_run_when_phone_brain_exists");
    },
    updatePhoneNumberItem: async () => null,
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Other",
  });

  const result = await handleTextgridDeliveryWebhook({
    message_id: "provider-phone-brain-1",
    status: "delivered",
    client_reference_id: "queue-123",
  });

  assert.equal(result.ok, true);
  assert.equal(result.results[0].brain_id, 702);
});

test("delivery webhook normalizes raw TextGrid sent callbacks and updates queue state", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const queueUpdates = [];

  const outboundEvent = createPodioItem(821, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("outbound:queue-123"),
    "text-2": textField("provider-sent-1"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-sent-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByProviderMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async (item_id, payload) => {
      queueUpdates.push({ item_id, payload });
    },
    logDeliveryEvent: async () => {},
    updateMessageEventStatus: async () => {},
    findBestBrainMatch: async () => null,
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => null,
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Other",
  });

  const result = await handleTextgridDeliveryWebhook({
    SmsSid: "provider-sent-1",
    SmsStatus: "sent",
    From: "+15550000002",
    To: "+15550000001",
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_state, "Sent");
  assert.equal(queueUpdates.length, 1);
  assert.equal(queueUpdates[0].item_id, 123);
  assert.equal(queueUpdates[0].payload["delivery-confirmed"], "⏳ Pending");
});

test("delivery webhook normalizes raw TextGrid delivered callbacks and confirms delivery", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const queueUpdates = [];

  const outboundEvent = createPodioItem(822, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("outbound:queue-123"),
    "text-2": textField("provider-delivered-1"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-delivered-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByProviderMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async (item_id, payload) => {
      queueUpdates.push({ item_id, payload });
    },
    logDeliveryEvent: async () => {},
    updateMessageEventStatus: async () => {},
    findBestBrainMatch: async () => null,
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async () => null,
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Other",
  });

  const result = await handleTextgridDeliveryWebhook({
    MessageSid: "provider-delivered-1",
    MessageStatus: "delivered",
    From: "+15550000002",
    To: "+15550000001",
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_state, "Delivered");
  assert.equal(queueUpdates.length, 1);
  assert.equal(queueUpdates[0].item_id, 123);
  assert.equal(queueUpdates[0].payload["delivery-confirmed"], "✅ Confirmed");
  assert.equal(queueUpdates[0].payload["queue-status"], "Delivered");
});

test("delivery webhook suppresses future outreach on hard-bounce destination failures", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  const phoneUpdates = [];

  const outboundEvent = createPodioItem(823, {
    "trigger-name": textField("queue-send:123"),
    "message-id": textField("outbound:queue-123"),
    "text-2": textField("provider-failed-1"),
    "ai-output": textField(
      JSON.stringify({
        queue_item_id: 123,
        client_reference_id: "queue-123",
        provider_message_id: "provider-failed-1",
      })
    ),
    "master-owner": appRefField(201),
    "linked-seller": appRefField(301),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  const queueItem = createPodioItem(123, {
    "master-owner": appRefField(201),
    "prospects": appRefField(301),
    "properties": appRefField(601),
    "phone-number": appRefField(401),
    "textgrid-number": appRefField(501),
  });

  __setTextgridDeliveryTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findMessageEventItemsByProviderMessageId: async () => [outboundEvent],
    getItem: async (item_id) => (Number(item_id) === 123 ? queueItem : null),
    fetchAllItems: async () => [],
    updateItem: async () => {},
    logDeliveryEvent: async () => {},
    updateMessageEventStatus: async () => {},
    findBestBrainMatch: async () => null,
    findLatestBrainByProspectId: async () => null,
    findLatestBrainByMasterOwnerId: async () => null,
    updatePhoneNumberItem: async (item_id, payload) => {
      phoneUpdates.push({ item_id, payload });
    },
    updateBrainAfterDelivery: async () => null,
    mapTextgridFailureBucket: () => "Hard Bounce",
  });

  const result = await handleTextgridDeliveryWebhook({
    MessageSid: "provider-failed-1",
    MessageStatus: "undelivered",
    ErrorCode: "30003",
    ErrorMessage: "Destination unreachable",
    From: "+15550000002",
    To: "+15550000001",
  });

  assert.equal(result.ok, true);
  assert.equal(result.normalized_state, "Failed");
  assert.equal(phoneUpdates.length, 1);
  assert.equal(phoneUpdates[0].item_id, 401);
  assert.equal(phoneUpdates[0].payload["do-not-call"], "TRUE");
  assert.equal(phoneUpdates[0].payload["dnc-source"], "Carrier Flag");
  assert.ok(phoneUpdates[0].payload["opt-out-date"]?.start);
});

test("DocuSign webhook ignores replay after first contract mutation", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let contractUpdateCount = 0;
  let brainUpdateCount = 0;

  __setDocusignWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    findContractItems: async () => [createPodioItem(9001)],
    updateContractItem: async () => {
      contractUpdateCount += 1;
    },
    maybeCreateTitleRoutingFromSignedContract: async () => ({
      created: true,
      title_routing_item_id: 9101,
    }),
    maybeCreateClosingFromTitleRouting: async () => ({
      created: true,
      closing_item_id: 9201,
    }),
    createBuyerMatchFlow: async () => ({
      created: true,
      buyer_match_item_id: 9301,
    }),
    maybeSendTitleIntro: async () => ({ sent: true }),
    syncPipelineState: async () => ({ current_stage: "Contract" }),
    updateBrainFromExecution: async () => {
      brainUpdateCount += 1;
      return { ok: true, updated: true };
    },
  });

  const payload = {
    event_id: "doc-event-1",
    envelope_id: "env-1",
    status: "completed",
  };

  const first = await handleDocusignWebhook(payload);
  const second = await handleDocusignWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.normalized_status, "Completed");
  assert.equal(second.duplicate, true);
  assert.equal(contractUpdateCount, 1);
  assert.equal(brainUpdateCount, 1);
});

test("title webhook ignores replay after first state update", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let titleStatusUpdateCount = 0;
  let closingStatusUpdateCount = 0;

  __setTitleWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    getTitleRoutingItem: async () => createPodioItem(1001),
    findTitleRoutingItems: async () => [],
    findClosingItems: async () => [],
    getClosingItem: async () => createPodioItem(2001),
    classifyTitleResponse: () => ({
      normalized_event: "title_opened",
      routing_status: "Opened",
      closing_status: "Scheduled",
      reason: "title_open_signal_detected",
      confidence: 0.9,
      sender_email: "title@example.com",
      subject: "Need estoppel",
    }),
    updateTitleRoutingStatus: async () => {
      titleStatusUpdateCount += 1;
      return { updated: true };
    },
    updateClosingStatus: async () => {
      closingStatusUpdateCount += 1;
      return { updated: true };
    },
  });

  const payload = {
    event_id: "title-event-1",
    title_routing_item_id: 1001,
    closing_item_id: 2001,
    subject: "Need estoppel",
    body: "Please send docs",
    event: "email_reply",
  };

  const first = await handleTitleResponseWebhook(payload);
  const second = await handleTitleResponseWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.updated, true);
  assert.equal(second.duplicate, true);
  assert.equal(titleStatusUpdateCount, 1);
  assert.equal(closingStatusUpdateCount, 1);
});

test("closing webhook ignores replay after first close/revenue path", async () => {
  const ledger = createInMemoryIdempotencyLedger();
  let markClosedCount = 0;
  let revenueCreateCount = 0;

  __setClosingWebhookTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    info: () => {},
    warn: () => {},
    getClosingItem: async () =>
      createPodioItem(3001, {
        "closing-id": textField("CL-1"),
      }),
    findClosingItems: async () => [],
    maybeMarkClosed: async () => {
      markClosedCount += 1;
      return { updated: true };
    },
    createDealRevenueFromClosedClosing: async () => {
      revenueCreateCount += 1;
      return { created: true, deal_revenue_item_id: 4001 };
    },
    updateClosingStatus: async () => ({ updated: true }),
  });

  const payload = {
    event_id: "closing-event-1",
    closing_item_id: 3001,
    status: "funded",
    body: "Funds released today",
  };

  const first = await handleClosingResponseWebhook(payload);
  const second = await handleClosingResponseWebhook(payload);

  assert.equal(first.ok, true);
  assert.equal(first.updated, true);
  assert.equal(first.normalized_event, "funded");
  assert.equal(second.duplicate, true);
  assert.equal(markClosedCount, 1);
  assert.equal(revenueCreateCount, 1);
});
