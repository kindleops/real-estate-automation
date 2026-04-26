import test from "node:test";
import assert from "node:assert/strict";

import {
  handleUnknownInboundRouter,
  __setUnknownInboundRouterDeps,
  __resetUnknownInboundRouterDeps,
  UNKNOWN_BUCKETS,
} from "@/lib/domain/inbound/unknown-inbound-router.js";
import {
  handleTextgridInboundWebhook,
  __setTextgridInboundTestDeps,
  __resetTextgridInboundTestDeps,
} from "@/lib/flows/handle-textgrid-inbound.js";
import { createInMemoryIdempotencyLedger } from "../helpers/test-helpers.js";

function normalizeE164(value) {
  const digits = String(value ?? "").replace(/\D/g, "");
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  if (digits.startsWith("+")) return value;
  return value ? `+${digits}` : null;
}

function createFakeSupabase({ existingUnknownContact = null } = {}) {
  const contactsByPhone = new Map();
  const suppressionByPhone = new Map();
  const messageEventsByKey = new Map();

  if (existingUnknownContact?.phone_e164) {
    contactsByPhone.set(existingUnknownContact.phone_e164, existingUnknownContact);
  }

  function from(table) {
    const state = {
      table,
      mode: null,
      payload: null,
      filters: {},
    };

    return {
      select() {
        return this;
      },
      eq(key, value) {
        state.filters[key] = value;
        return this;
      },
      upsert(payload) {
        state.mode = "upsert";
        state.payload = payload;
        return this;
      },
      async maybeSingle() {
        if (state.mode === "upsert") {
          if (state.table === "unknown_inbound_contacts") {
            contactsByPhone.set(state.payload.phone_e164, state.payload);
            return { data: state.payload, error: null };
          }

          if (state.table === "sms_suppression_list") {
            suppressionByPhone.set(state.payload.phone_e164, state.payload);
            return { data: state.payload, error: null };
          }

          if (state.table === "message_events") {
            messageEventsByKey.set(state.payload.message_event_key, state.payload);
            return { data: state.payload, error: null };
          }

          return { data: state.payload, error: null };
        }

        if (state.table === "unknown_inbound_contacts") {
          return {
            data: contactsByPhone.get(state.filters.phone_e164) || null,
            error: null,
          };
        }

        return { data: null, error: null };
      },
    };
  }

  return {
    from,
    __state: {
      contactsByPhone,
      suppressionByPhone,
      messageEventsByKey,
    },
  };
}

async function runUnknownRouter(message_body, options = {}) {
  const queued = [];
  const alerts = [];
  const fakeDb = createFakeSupabase({ existingUnknownContact: options.existingUnknownContact || null });

  __setUnknownInboundRouterDeps({
    supabase: fakeDb,
    normalizePhone: normalizeE164,
    insertSupabaseSendQueueRow: async (payload) => {
      queued.push(payload);
      return { id: queued.length, ...payload };
    },
    notifyDiscordOps: async (payload) => {
      alerts.push(payload);
      return { ok: true };
    },
    info: () => {},
    warn: () => {},
  });

  const result = await handleUnknownInboundRouter({
    message_id: options.message_id || "SM-unknown",
    inbound_from: options.inbound_from || "+16125550111",
    inbound_to: options.inbound_to || "+16125550112",
    message_body,
    dry_run: Boolean(options.dry_run),
    auto_reply_enabled:
      options.auto_reply_enabled === undefined ? true : Boolean(options.auto_reply_enabled),
    inbound_user_initiated:
      options.inbound_user_initiated === undefined ? true : Boolean(options.inbound_user_initiated),
  });

  __resetUnknownInboundRouterDeps();

  return {
    result,
    queued,
    alerts,
    db: fakeDb.__state,
  };
}

test("unknown seller-ish message queues property clarification", async () => {
  const out = await runUnknownRouter("Yes I still own the property and may sell soon");

  assert.equal(out.result.unknown_router.bucket, UNKNOWN_BUCKETS.UNKNOWN_SELLER_REPLY);
  assert.equal(out.result.unknown_router.auto_reply_queued, true);
  assert.equal(out.queued.length, 1);
  assert.match(out.queued[0].message_body, /Which property are you referring to/i);
});

test("unknown buyer message queues buyer qualification reply", async () => {
  const out = await runUnknownRouter("I am a buyer looking for off market deals in Minneapolis");

  assert.equal(out.result.unknown_router.bucket, UNKNOWN_BUCKETS.UNKNOWN_BUYER_OR_INVESTOR);
  assert.equal(out.result.unknown_router.auto_reply_queued, true);
  assert.equal(out.queued.length, 1);
  assert.match(out.queued[0].message_body, /off-market deals/i);
});

test("unknown agent message queues partner/property clarification", async () => {
  const out = await runUnknownRouter("I am a realtor with a listing and broker team");

  assert.equal(out.result.unknown_router.bucket, UNKNOWN_BUCKETS.UNKNOWN_AGENT_OR_REALTOR);
  assert.equal(out.result.unknown_router.auto_reply_queued, true);
  assert.equal(out.queued.length, 1);
  assert.match(out.queued[0].message_body, /partnership opportunity/i);
});

test("unknown personal message does not auto-reply but alerts", async () => {
  const out = await runUnknownRouter("This is his brother texting from family phone");

  assert.equal(out.result.unknown_router.bucket, UNKNOWN_BUCKETS.UNKNOWN_PERSONAL);
  assert.equal(out.result.unknown_router.auto_reply_queued, false);
  assert.equal(out.queued.length, 0);
  assert.equal(out.alerts.length, 1);
});

test("unknown STOP creates suppression and no reply", async () => {
  const out = await runUnknownRouter("STOP texting me");

  assert.equal(out.result.unknown_router.bucket, UNKNOWN_BUCKETS.OPT_OUT);
  assert.equal(out.result.unknown_router.suppression_applied, true);
  assert.equal(out.result.unknown_router.auto_reply_queued, false);
  assert.equal(out.queued.length, 0);
  assert.equal(out.db.suppressionByPhone.size, 1);
});

test("unknown spam logs only and no reply", async () => {
  const out = await runUnknownRouter("Click here for free gift bitcoin airdrop https://spam.example");

  assert.equal(out.result.unknown_router.bucket, UNKNOWN_BUCKETS.SPAM);
  assert.equal(out.result.unknown_router.auto_reply_queued, false);
  assert.equal(out.result.unknown_router.suppression_applied, false);
  assert.equal(out.queued.length, 0);
  assert.equal(out.db.messageEventsByKey.size, 1);
});

test("known phone still routes existing path (unknown router not called)", async (t) => {
  const ledger = createInMemoryIdempotencyLedger();
  let unknown_router_called = false;

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    loadContextWithFallback: async () => ({
      found: true,
      ids: {
        phone_item_id: "ph_1",
        brain_item_id: "brain_1",
        master_owner_id: "owner_1",
      },
      items: {},
      summary: {},
      recent: {},
    }),
    handleUnknownInboundRouter: async () => {
      unknown_router_called = true;
      return { ok: true };
    },
    info: () => {},
    warn: () => {},
  });

  t.after(() => {
    __resetTextgridInboundTestDeps();
  });

  const result = await handleTextgridInboundWebhook(
    {
      SmsMessageSid: "SM-known",
      From: "+16125550111",
      To: "+16125550112",
      Body: "Yes",
      SmsStatus: "received",
    },
    { inbound_debug_stage: "after_phone_resolution" }
  );

  assert.equal(result.ok, true);
  assert.equal(result.stage, "after_phone_resolution");
  assert.equal(unknown_router_called, false);
});

test("recent outbound pair fallback prevents unknown route", async (t) => {
  const ledger = createInMemoryIdempotencyLedger();
  let unknown_router_called = false;

  __setTextgridInboundTestDeps({
    beginIdempotentProcessing: ledger.begin,
    completeIdempotentProcessing: ledger.complete,
    failIdempotentProcessing: ledger.fail,
    hashIdempotencyPayload: ledger.hash,
    normalizeInboundTextgridPhone: (value) => value,
    loadContextWithFallback: async () => ({
      found: true,
      ids: {
        phone_item_id: null,
        brain_item_id: null,
        master_owner_id: "owner_from_fallback",
      },
      items: {},
      summary: {},
      recent: {},
      fallback_pair_match: true,
      fallback_match_source: "recent_outbound_send_queue",
    }),
    handleUnknownInboundRouter: async () => {
      unknown_router_called = true;
      return { ok: true };
    },
    info: () => {},
    warn: () => {},
  });

  t.after(() => {
    __resetTextgridInboundTestDeps();
  });

  const result = await handleTextgridInboundWebhook(
    {
      SmsMessageSid: "SM-fallback",
      From: "+16125550111",
      To: "+16125550112",
      Body: "Reply",
      SmsStatus: "received",
    },
    { inbound_debug_stage: "after_phone_resolution" }
  );

  assert.equal(result.ok, true);
  assert.equal(result.stage, "after_phone_resolution");
  assert.equal(unknown_router_called, false);
});

test("unknown dry_run returns diagnostics without DB mutations", async () => {
  const out = await runUnknownRouter("I still own this property", { dry_run: true });

  assert.equal(out.result.unknown_router.dry_run, true);
  assert.equal(out.queued.length, 0);
  assert.equal(out.db.messageEventsByKey.size, 0);
  assert.equal(out.db.suppressionByPhone.size, 0);
});
