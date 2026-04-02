import test from "node:test";
import assert from "node:assert/strict";

import { resolveMarketSendingProfile } from "@/lib/config/market-sending-zones.js";
import { chooseTextgridNumber } from "@/lib/domain/routing/choose-textgrid-number.js";

function createCandidateRecord({
  item_id,
  normalized_phone,
  market_name,
  status = "_ Active",
  priority = 0,
  daily_limit = 100,
  daily_sent = 0,
  area_code = "",
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
    area_code,
  };
}

test("market sending profile maps seeded launch markets into a sending zone", () => {
  const result = resolveMarketSendingProfile("Fayetteville, NC");

  assert.equal(result.ok, true);
  assert.equal(result.sending_zone, "Charlotte, NC");
  assert.deepEqual(result.allowed_phone_markets, ["Charlotte, NC"]);
});

test("market sending profile excludes unmapped markets from launch routing", () => {
  const result = resolveMarketSendingProfile("Boise, ID");

  assert.equal(result.ok, false);
  assert.equal(result.reason, "market_unmapped_excluded");
  assert.deepEqual(result.allowed_phone_markets, []);
});

test("TextGrid number selection uses sending-zone market mapping instead of direct market equality", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: {
        phone_item_id: 9001,
      },
      summary: {
        market_name: "Fayetteville, NC",
        market_area_code: "910",
        language_preference: "English",
      },
    },
    rotation_key: "seller-zone-test",
    candidate_records: [
      createCandidateRecord({
        item_id: 11,
        normalized_phone: "+17045550111",
        market_name: "Charlotte, NC",
        priority: 7,
        area_code: "704",
      }),
      createCandidateRecord({
        item_id: 12,
        normalized_phone: "+12145550112",
        market_name: "Dallas, TX",
        priority: 10,
        area_code: "214",
      }),
    ],
  });

  assert.equal(selected.item_id, 11);
  assert.equal(selected.selection_reason, "primary_allowed_phone_market_match");
  assert.equal(selected.selection_diagnostics.raw_seller_market, "Fayetteville, NC");
  assert.equal(selected.selection_diagnostics.resolved_sending_zone, "Charlotte, NC");
  assert.deepEqual(selected.selection_diagnostics.allowed_phone_markets, ["Charlotte, NC"]);
  assert.equal(selected.selection_diagnostics.selected_phone_market, "Charlotte, NC");
});

test("TextGrid number selection prefers the best eligible number inside the allowed phone market", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: {
        phone_item_id: 9002,
      },
      summary: {
        market_name: "Austin, TX",
        market_area_code: "512",
        language_preference: "English",
      },
    },
    rotation_key: "seller-weighted-test",
    candidate_records: [
      createCandidateRecord({
        item_id: 21,
        normalized_phone: "+12145550121",
        market_name: "Dallas, TX",
        priority: 1,
        daily_limit: 100,
        daily_sent: 95,
        area_code: "214",
      }),
      createCandidateRecord({
        item_id: 22,
        normalized_phone: "+14695550122",
        market_name: "Dallas, TX",
        priority: 10,
        daily_limit: 100,
        daily_sent: 4,
        area_code: "469",
      }),
    ],
  });

  assert.equal(selected.item_id, 22);
  assert.equal(selected.selection_diagnostics.resolved_sending_zone, "Dallas, TX");
  assert.equal(selected.selection_diagnostics.selected_phone_market, "Dallas, TX");
});

test("TextGrid number selection returns diagnostics when seller market is unmapped", async () => {
  const selected = await chooseTextgridNumber({
    context: {
      ids: {
        phone_item_id: 9003,
      },
      summary: {
        market_name: "Boise, ID",
        language_preference: "English",
      },
    },
    rotation_key: "seller-unmapped-test",
    candidate_records: [
      createCandidateRecord({
        item_id: 31,
        normalized_phone: "+17045550131",
        market_name: "Charlotte, NC",
      }),
    ],
  });

  assert.equal(selected.item_id, null);
  assert.equal(selected.selection_reason, "market_unmapped_excluded");
  assert.equal(selected.selection_diagnostics.raw_seller_market, "Boise, ID");
  assert.equal(selected.selection_diagnostics.resolved_sending_zone, null);
  assert.deepEqual(selected.selection_diagnostics.allowed_phone_markets, []);
});
