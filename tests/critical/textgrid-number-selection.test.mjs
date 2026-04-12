import test from "node:test";
import assert from "node:assert/strict";

import { resolveMarketSendingProfile } from "@/lib/config/market-sending-zones.js";
import { chooseTextgridNumber } from "@/lib/domain/routing/choose-textgrid-number.js";

function createCandidateRecord({
  item_id,
  normalized_phone,
  market_name,
  status = "_ Active",
  priority = 5,
  daily_limit = 100,
  daily_sent = 0,
  hourly_limit = 50,
  hourly_sent = 0,
  hard_pause = "No",
  risk_spike_flag = "No",
  // Default to always-open window so market-matching tests are not time-sensitive.
  // Tests that specifically validate send-window logic should override these.
  allowed_send_window_start_local = "00:00",
  allowed_send_window_end_local = "23:59",
  area_code = "",
  last_used_at = "2026-04-08T00:00:00.000Z",
}) {
  return {
    item_id,
    normalized_phone,
    phone_number: normalized_phone,
    market_name,
    status,
    priority,
    daily_limit,
    daily_sent,
    hourly_limit,
    hourly_sent,
    hard_pause,
    risk_spike_flag,
    allowed_send_window_start_local,
    allowed_send_window_end_local,
    area_code,
    last_used_at,
  };
}

test("market sending profile normalizes aliases before routing", () => {
  const result = resolveMarketSendingProfile("St. Paul, MN");

  assert.equal(result.ok, true);
  assert.equal(result.normalized_market, "Minneapolis, MN");
  assert.equal(result.primary_cluster, "minneapolis_cluster");
});

test("TextGrid number selection uses exact market match first", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: { phone_item_id: 9001 },
      summary: { market_name: "Dallas, TX", market_area_code: "469" },
    },
    candidate_records: [
      createCandidateRecord({
        item_id: 11,
        normalized_phone: "+14693131600",
        market_name: "Dallas, TX",
        priority: 8,
        area_code: "469",
      }),
      createCandidateRecord({
        item_id: 12,
        normalized_phone: "+12818458577",
        market_name: "Houston, TX",
      }),
    ],
  });

  assert.equal(selected.item_id, 11);
  assert.equal(selected.selection_reason, "exact_market_match");
});

test("TextGrid number selection uses alias match when exact market has no number", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: { phone_item_id: 9002 },
      summary: { market_name: "Fort Worth, TX", market_area_code: "817" },
    },
    candidate_records: [
      createCandidateRecord({
        item_id: 21,
        normalized_phone: "+14693131600",
        market_name: "Dallas, TX",
      }),
    ],
  });

  assert.equal(selected.item_id, 21);
  assert.equal(selected.selection_reason, "alias_market_match");
});

test("TextGrid number selection uses regional fallback cluster when needed", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: { phone_item_id: 9003 },
      summary: { market_name: "Orlando, FL", market_area_code: "407" },
    },
    candidate_records: [
      createCandidateRecord({
        item_id: 31,
        normalized_phone: "+19048774448",
        market_name: "Jacksonville, FL",
      }),
      createCandidateRecord({
        item_id: 32,
        normalized_phone: "+17866052999",
        market_name: "Miami, FL",
      }),
    ],
  });

  assert.equal(selected.item_id, 31);
  assert.equal(selected.selection_reason, "regional_cluster_fallback");
});

test("TextGrid number selection excludes hard-paused numbers", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: { phone_item_id: 9004 },
      summary: { market_name: "Houston, TX" },
    },
    candidate_records: [
      createCandidateRecord({
        item_id: 41,
        normalized_phone: "+12818458577",
        market_name: "Houston, TX",
        hard_pause: "Yes",
      }),
      createCandidateRecord({
        item_id: 42,
        normalized_phone: "+14693131600",
        market_name: "Dallas, TX",
      }),
    ],
  });

  assert.equal(selected.item_id, 42);
  assert.equal(selected.selection_reason, "regional_cluster_fallback");
});

test("TextGrid number selection excludes numbers outside local send windows", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: { phone_item_id: 9005 },
      summary: { market_name: "Los Angeles, CA" },
    },
    candidate_records: [
      createCandidateRecord({
        item_id: 51,
        normalized_phone: "+13234104544",
        market_name: "Los Angeles, CA",
        allowed_send_window_start_local: "23:30",
        allowed_send_window_end_local: "23:40",
      }),
      createCandidateRecord({
        item_id: 52,
        normalized_phone: "+13235589881",
        market_name: "Los Angeles, CA",
        allowed_send_window_start_local: "00:00",
        allowed_send_window_end_local: "23:59",
      }),
    ],
  });

  assert.equal(selected.item_id, 52);
  assert.equal(selected.selection_reason, "exact_market_match");
});

test("TextGrid number selection safely returns routing_unmapped when unresolved", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: { phone_item_id: 9006 },
      summary: { market_name: "Unmapped" },
    },
    candidate_records: [
      createCandidateRecord({
        item_id: 61,
        normalized_phone: "+14693131600",
        market_name: "Dallas, TX",
      }),
    ],
  });

  assert.equal(selected.item_id, null);
  assert.equal(selected.selection_reason, "routing_unmapped");
});
