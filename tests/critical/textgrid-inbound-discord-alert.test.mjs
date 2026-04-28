import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  POST as postTextgridInbound,
  __resetTextgridInboundRouteTestDeps,
  __setTextgridInboundRouteTestDeps,
} from "@/app/api/webhooks/textgrid/inbound/route.js";
import {
  __resetInboundSmsAlertDeps,
  __setInboundSmsAlertDeps,
} from "@/lib/discord/inbound-alerts.js";

const INBOUND_URL = "http://localhost:3000/api/webhooks/textgrid/inbound";

const ENV_KEYS = [
  "DISCORD_BOT_TOKEN",
  "DISCORD_CHANNEL_INBOUND_REPLIES",
  "TEXTGRID_WEBHOOK_SIGNATURE_MODE",
  "INTERNAL_API_SECRET",
  "CRON_SECRET",
];

const savedEnv = new Map();
for (const key of ENV_KEYS) {
  savedEnv.set(key, process.env[key]);
}

afterEach(() => {
  __resetTextgridInboundRouteTestDeps();
  __resetInboundSmsAlertDeps();
  for (const key of ENV_KEYS) {
    const value = savedEnv.get(key);
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
});

function makeLogger() {
  return {
    info: () => {},
    warn: () => {},
    error: () => {},
  };
}

function configureDiscordCapture() {
  const posts = [];
  process.env.DISCORD_BOT_TOKEN = "discord-test-token";
  process.env.DISCORD_CHANNEL_INBOUND_REPLIES = "inbound-alert-channel";
  process.env.TEXTGRID_WEBHOOK_SIGNATURE_MODE = "off";

  __setInboundSmsAlertDeps({
    fetch: async (url, init = {}) => {
      posts.push({
        url,
        headers: init.headers || {},
        body: JSON.parse(init.body || "{}"),
      });
      return {
        ok: true,
        status: 200,
        json: async () => ({ id: `discord-alert-${posts.length}` }),
      };
    },
  });

  return posts;
}

function configureRoute({ buyerHandler, mainHandler } = {}) {
  __setTextgridInboundRouteTestDeps({
    logger: makeLogger(),
    maybeHandleBuyerTextgridInboundImpl:
      buyerHandler || (async () => ({ ok: true, matched: false })),
    handleTextgridInboundImpl:
      mainHandler || (async (payload) => ({
        ok: true,
        message_id: payload.message_id,
        inbound_from: payload.from,
        inbound_to: payload.to,
        body: payload.message_body,
        context: {
          ids: {
            master_owner_id: "owner-1",
            property_id: "property-1",
            market_id: "market-1",
          },
          summary: {
            seller_name: "Rae Owner",
            property_address: "123 Main St, Dallas, TX",
            market: "Dallas, TX",
          },
        },
        classification: {
          source: "affirmative",
          confidence: 0.91,
        },
        route: {
          stage: "Ownership Confirmation",
          use_case: "consider_selling",
        },
      })),
    verifyTextgridWebhookRequestImpl: () => ({ ok: true, required: false }),
    writeWebhookLogImpl: async () => {},
    logSupabaseInboundMessageEventImpl: async () => {},
  });
}

function requestFor(messageId, body = "Yes I want to sell") {
  return new Request(INBOUND_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      SmsMessageSid: messageId,
      SmsSid: messageId,
      MessageSid: messageId,
      From: "+16125550111",
      To: "+14693131600",
      Body: body,
      SmsStatus: "received",
    }),
  });
}

function payloadText(posts, index = 0) {
  return JSON.stringify(posts[index]?.body || {});
}

function fieldValue(posts, fieldName, index = 0) {
  const fields = posts[index]?.body?.embeds?.[0]?.fields || [];
  return fields.find((field) => field.name === fieldName)?.value || "";
}

test("valid inbound triggers Discord alert", async () => {
  const posts = configureDiscordCapture();
  configureRoute();

  const response = await postTextgridInbound(requestFor("SM-alert-valid-1"));

  assert.equal(response.status, 200);
  assert.equal(posts.length, 1);
  assert.equal(posts[0].url, "https://discord.com/api/v10/channels/inbound-alert-channel/messages");
  assert.deepEqual(posts[0].body.allowed_mentions, { parse: [] });
  assert.equal(fieldValue(posts, "Provider Message ID"), "SM-alert-valid-1");
  assert.equal(fieldValue(posts, "Body"), "Yes I want to sell");
  assert.match(payloadText(posts), /Rae Owner/);
  assert.match(payloadText(posts), /123 Main St/);
  assert.match(payloadText(posts), /affirmative/);
});

test("unknown inbound triggers Discord alert", async () => {
  const posts = configureDiscordCapture();
  configureRoute({
    mainHandler: async (payload) => ({
      ok: true,
      message_id: payload.message_id,
      inbound_from: payload.from,
      inbound_to: payload.to,
      body: payload.message_body,
      context: { unknown_inbound: true },
      unknown_router: {
        bucket: "unknown_seller_reply",
        auto_reply_queued: true,
      },
    }),
  });

  const response = await postTextgridInbound(requestFor("SM-alert-unknown-1", "Which property?"));

  assert.equal(response.status, 200);
  assert.equal(posts.length, 1);
  assert.match(payloadText(posts), /unknown_seller_reply/);
});

test("opt-out inbound triggers Discord alert", async () => {
  const posts = configureDiscordCapture();
  configureRoute({
    mainHandler: async (payload) => ({
      ok: true,
      message_id: payload.message_id,
      inbound_from: payload.from,
      inbound_to: payload.to,
      body: payload.message_body,
      seller_stage_reply: {
        plan: {
          selected_use_case: "stop_or_opt_out",
        },
      },
      route: {
        stage: "Terminal",
        use_case: "stop_or_opt_out",
      },
    }),
  });

  const response = await postTextgridInbound(requestFor("SM-alert-opt-out-1", "STOP texting me"));

  assert.equal(response.status, 200);
  assert.equal(posts.length, 1);
  assert.match(payloadText(posts), /stop_or_opt_out/);
});

test("buyer handler failure still triggers inbound alert", async () => {
  const posts = configureDiscordCapture();
  configureRoute({
    buyerHandler: async () => {
      throw new Error("buyer_handler_runtime_failure");
    },
  });

  const response = await postTextgridInbound(requestFor("SM-alert-buyer-fail-1"));
  const payload = await response.json();

  assert.equal(response.status, 200);
  assert.equal(payload.buyer_handler_failed, true);
  assert.equal(posts.length, 1);
  assert.match(payloadText(posts), /buyer_handler_runtime_failure/);
});

test("main handler failure still triggers inbound alert", async () => {
  const posts = configureDiscordCapture();
  configureRoute({
    mainHandler: async () => {
      throw new Error("main_handler_runtime_failure");
    },
  });

  const response = await postTextgridInbound(requestFor("SM-alert-main-fail-1"));
  const payload = await response.json();

  assert.equal(response.status, 500);
  assert.equal(payload.error, "textgrid_inbound_failed");
  assert.equal(posts.length, 1);
  assert.match(payloadText(posts), /main_handler_runtime_failure/);
  assert.match(payloadText(posts), /Inbound SMS Routing Failure/);
});

test("duplicate webhook retry with same MessageSid does not send duplicate alert", async () => {
  const posts = configureDiscordCapture();
  configureRoute();

  const first = await postTextgridInbound(requestFor("SM-alert-duplicate-1"));
  const second = await postTextgridInbound(requestFor("SM-alert-duplicate-1"));

  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  assert.equal(posts.length, 1);
});

test("alert payload never exposes secrets", async () => {
  const posts = configureDiscordCapture();
  process.env.INTERNAL_API_SECRET = "internal-secret-value";
  process.env.CRON_SECRET = "cron-secret-value";
  configureRoute({
    mainHandler: async () => {
      throw new Error(
        `routing failed INTERNAL_API_SECRET=${process.env.INTERNAL_API_SECRET} CRON_SECRET=${process.env.CRON_SECRET}`
      );
    },
  });

  const response = await postTextgridInbound(
    requestFor(
      "SM-alert-secret-redaction-1",
      `body has ${process.env.INTERNAL_API_SECRET} and ${process.env.CRON_SECRET}`
    )
  );

  assert.equal(response.status, 500);
  assert.equal(posts.length, 1);
  const alertBody = payloadText(posts);
  assert.ok(!alertBody.includes("internal-secret-value"));
  assert.ok(!alertBody.includes("cron-secret-value"));
  assert.ok(!alertBody.includes("INTERNAL_API_SECRET"));
  assert.ok(!alertBody.includes("CRON_SECRET"));
});
