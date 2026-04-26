import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  postInboundSmsDiscordCard,
  __setInboundSmsCardDeps,
  __resetInboundSmsCardDeps,
} from "@/lib/discord/inbound-sms-card.js";

afterEach(() => {
  __resetInboundSmsCardDeps();
});

test("postInboundSmsDiscordCard skips duplicate posts when message metadata already has Discord card info", async () => {
  let fetch_calls = 0;
  __setInboundSmsCardDeps({
    fetch: async () => {
      fetch_calls += 1;
      return {
        ok: true,
        json: async () => ({ id: "discord-msg-1" }),
      };
    },
  });

  const result = await postInboundSmsDiscordCard(
    {
      message_event_id: "msg-event-1",
      inbound_from: "+16025550111",
      inbound_message_body: "Hello",
      existing_metadata: {
        discord_message_id: "discord-existing-1",
      },
    },
    {
      env: {
        DISCORD_BOT_TOKEN: "token",
        DISCORD_CHANNEL_INBOUND_REPLIES: "inbound-chan-1",
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.skipped, true);
  assert.equal(result.reason, "discord_card_already_posted");
  assert.equal(fetch_calls, 0);
});

test("postInboundSmsDiscordCard falls back to debug logs channel when inbound replies channel is not configured", async () => {
  let posted_url = null;
  __setInboundSmsCardDeps({
    fetch: async (url) => {
      posted_url = url;
      return {
        ok: true,
        json: async () => ({ id: "discord-msg-2" }),
      };
    },
  });

  const result = await postInboundSmsDiscordCard(
    {
      message_event_id: "msg-event-2",
      inbound_from: "+16025550112",
      inbound_message_body: "Need more info",
    },
    {
      env: {
        DISCORD_BOT_TOKEN: "token",
        DISCORD_CHANNEL_DEBUG_LOGS: "debug-chan-1",
      },
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.fallback, true);
  assert.equal(result.channel_key, "debug_logs");
  assert.match(posted_url || "", /channels\/debug-chan-1\/messages$/);
});