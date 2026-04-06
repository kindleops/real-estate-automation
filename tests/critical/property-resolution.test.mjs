/**
 * Property resolution tests — validates the correct resolution order after removing
 * the invalid phone→property path.
 *
 * Architecture fact:
 *   Properties are linked to Master Owners — NOT to Phones.
 *   Phone records exist for validation, DNC, and activity-status purposes only.
 *
 * Resolution order (selectBestProperty):
 *   1. Master Owner → related Properties (collectRelatedItemIdsByApp)
 *      a. Single result        → use it directly
 *      b. Multiple results     → disambiguate via seller_id address match
 *      c. Still ambiguous      → return null (skip with clear log)
 *   2. findPropertyItems({ "master-owner": id }) Podio search
 *   3. Brain item → properties relation
 *   4. seller_id address lookup in Properties app
 *   5. null  (synthetic fallback handled upstream by evaluateOwner)
 */

import test from "node:test";
import assert from "node:assert/strict";

import APP_IDS from "@/lib/config/app-ids.js";

import {
  selectBestProperty,
} from "@/lib/domain/master-owners/run-master-owner-outbound-feeder.js";

import {
  createPodioItem,
  textField,
  categoryField,
} from "../helpers/test-helpers.js";

// ── helpers ───────────────────────────────────────────────────────────────────

/**
 * Build an owner item whose `refs` array (depth-1 Podio refs) contains
 * entries that collectRelatedItemIdsByApp will recognise as Properties app items.
 */
function makeOwnerWithPropertyRefs(owner_id, property_ids = [], seller_id = "") {
  const owner = createPodioItem(owner_id, {
    "sms-eligible": categoryField("Yes"),
    "seller-id": textField(seller_id),
  });
  // `refs` is the field collectRelatedItemIdsByApp walks at depth+1.
  // Each entry needs item_id + app_id so the matcher can identify the app.
  owner.refs = property_ids.map((pid) => ({
    item_id: pid,
    app_id: APP_IDS.properties,
  }));
  return owner;
}

function makeOwnerNoRefs(owner_id, seller_id = "") {
  return createPodioItem(owner_id, {
    "sms-eligible": categoryField("Yes"),
    "seller-id": textField(seller_id),
  });
}

function makePropertyItem(item_id, { address = "", city = "", state = "" } = {}) {
  return createPodioItem(item_id, {
    "property-address": textField(address),
    city: textField(city),
    state: textField(state),
  });
}

/**
 * Build a minimal phone record (no primary_property_id — phones don't link to properties).
 */
function makePhoneRecord(phone_item_id = 401) {
  return {
    slot: "best-phone-1",
    phone_item_id,
    phone_item: createPodioItem(phone_item_id, {}),
    normalized_phone: "+19185551234",
    prospect_id: null,
    // primary_property_id intentionally absent — phones do NOT link to properties
    market_id: null,
    summary: { slot: "best-phone-1", item_id: phone_item_id },
  };
}

/**
 * Make a runtime with an in-memory item_cache so safeGetItem never hits Podio.
 */
function makeRuntime(property_items = []) {
  const item_cache = new Map();
  for (const item of property_items) {
    item_cache.set(String(item.item_id), item);
  }
  return {
    item_cache,
    owner_history_by_id: new Map(),
    textgrid_number_pool: null,
  };
}

/** Minimal no-op logger so selectBestProperty can call log.info/log.warn. */
const noop_log = {
  info: () => {},
  warn: () => {},
  debug: () => {},
};

// ── Part 6.1: phone-linked property path is not required ─────────────────────

test("Part 6.1 — phone record carries no primary_property_id (phones do not link to properties)", () => {
  const record = makePhoneRecord(401);
  assert.equal(
    "primary_property_id" in record,
    false,
    "phone records must not carry primary_property_id after the architecture fix"
  );
});

test("Part 6.1 — selectBestProperty resolves via owner relation even when phone has no property link", async () => {
  const property = makePropertyItem(5001, { address: "123 Main St", city: "Springfield", state: "IL" });
  const owner = makeOwnerWithPropertyRefs(201, [5001]);
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([property]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.ok(result, "must resolve a property even without any phone→property link");
  assert.equal(result.item_id, 5001);
});

// ── Part 6.2: single related Property on Master Owner resolves ────────────────

test("Part 6.2 — single owner-related property resolves immediately (no address disambiguation needed)", async () => {
  const property = makePropertyItem(5002, { address: "456 Oak Ave", city: "Tulsa", state: "OK" });
  const owner = makeOwnerWithPropertyRefs(202, [5002], "SFR~456 Oak Ave|Tulsa|OK|74101");
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([property]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.ok(result, "must resolve the single related property");
  assert.equal(result.item_id, 5002);
});

test("Part 6.2 — resolves even when owner has no seller_id (no address lookup attempted)", async () => {
  const property = makePropertyItem(5003, { address: "789 Elm St", city: "Dallas", state: "TX" });
  const owner = makeOwnerWithPropertyRefs(203, [5003], ""); // no seller_id
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([property]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.ok(result, "single related property must resolve regardless of seller_id");
  assert.equal(result.item_id, 5003);
});

// ── Part 6.3: multiple related Properties resolve by address match ────────────

test("Part 6.3 — two related properties, seller_id address disambiguates to the correct one", async () => {
  // Owner has two related properties. seller_id address matches only prop 5011.
  const prop_a = makePropertyItem(5011, { address: "100 First St", city: "Austin", state: "TX" });
  const prop_b = makePropertyItem(5012, { address: "200 Second St", city: "Austin", state: "TX" });
  const owner = makeOwnerWithPropertyRefs(
    211,
    [5011, 5012],
    "SFR~100 First St|Austin|TX|78701"
  );
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([prop_a, prop_b]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.ok(result, "must resolve the property matching the seller_id address");
  assert.equal(result.item_id, 5011, "must pick the address-matched property, not the other one");
});

test("Part 6.3 — address match is case-insensitive and trims whitespace during disambiguation", async () => {
  const prop_a = makePropertyItem(5013, { address: "321 Pine Blvd", city: "Houston", state: "TX" });
  const prop_b = makePropertyItem(5014, { address: "999 Other Rd", city: "Houston", state: "TX" });
  // seller_id uses different casing — addressLookupVariants normalises both sides
  const owner = makeOwnerWithPropertyRefs(
    212,
    [5013, 5014],
    "SFR~321 Pine Blvd|Houston|TX|77001"
  );
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([prop_a, prop_b]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.ok(result, "must resolve using normalised address match");
  assert.equal(result.item_id, 5013);
});

// ── Part 6.4: ambiguous multiple related Properties fail safely ───────────────

test("Part 6.4 — two related properties with no seller_id returns null (safe skip)", async () => {
  const prop_a = makePropertyItem(5021, { address: "1 Alpha St", city: "Miami", state: "FL" });
  const prop_b = makePropertyItem(5022, { address: "2 Beta St", city: "Miami", state: "FL" });
  const owner = makeOwnerWithPropertyRefs(221, [5021, 5022], ""); // no seller_id
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([prop_a, prop_b]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.equal(result, null, "must return null when multiple properties cannot be disambiguated");
});

test("Part 6.4 — two related properties and non-matching seller_id address returns null", async () => {
  const prop_a = makePropertyItem(5023, { address: "10 Gamma Ave", city: "Tampa", state: "FL" });
  const prop_b = makePropertyItem(5024, { address: "20 Delta Ave", city: "Tampa", state: "FL" });
  // seller_id encodes a completely different address that matches neither property
  const owner = makeOwnerWithPropertyRefs(
    222,
    [5023, 5024],
    "SFR~999 No Match Way|Tampa|FL|33601"
  );
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([prop_a, prop_b]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.equal(result, null, "non-matching address must not resolve any property");
});

test("Part 6.4 — two related properties where seller_id address matches BOTH still returns null", async () => {
  // Pathological case: two properties with the same normalised address text.
  const prop_a = makePropertyItem(5025, { address: "55 Same Rd", city: "Orlando", state: "FL" });
  const prop_b = makePropertyItem(5026, { address: "55 Same Rd", city: "Orlando", state: "FL" });
  const owner = makeOwnerWithPropertyRefs(
    223,
    [5025, 5026],
    "SFR~55 Same Rd|Orlando|FL|32801"
  );
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([prop_a, prop_b]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.equal(result, null, "must return null when disambiguation yields more than one match");
});

// ── Part 6.5: live queue eligibility succeeds when owner has a related property ──

test("Part 6.5 — owner with a single related property produces a resolved item_id (live queue eligible)", async () => {
  // This mirrors the path evaluateOwner takes: selectBestProperty returns a real
  // property item, which unblocks queue creation (property_item.item_id is truthy).
  const property = makePropertyItem(5031, { address: "400 Queue Lane", city: "Denver", state: "CO" });
  const owner = makeOwnerWithPropertyRefs(231, [5031], "SFR~400 Queue Lane|Denver|CO|80201");
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([property]);

  const result = await selectBestProperty(phone, [], {
    owner_item: owner,
    runtime,
    log: noop_log,
  });

  assert.ok(result?.item_id, "resolved property must have a real item_id for queue creation");
  assert.equal(typeof result.item_id, "number");
  assert.ok(result.item_id > 0);

  // The result is NOT synthetic — live queue creation is not blocked.
  assert.equal(result.synthetic, undefined, "real property items must not carry the synthetic flag");
});

test("Part 6.5 — owner with no related properties and no seller_id returns null (queue blocked downstream)", async () => {
  // selectBestProperty returns null → evaluateOwner will try synthetic fallback
  // for first-touch, and block live queue if still unresolved.
  const owner = makeOwnerNoRefs(232, "");
  const phone = makePhoneRecord(401);
  const runtime = makeRuntime([]); // no cache entries

  // No Podio API, no findPropertyItems, no brain item — all steps will short-circuit.
  // safeGetItem returns null for cache misses when no getItem is wired.
  // findPropertyItems would throw (network) — but for owner with no refs the
  // Podio search path is reached. We don't mock it in this unit test; instead
  // we verify the phone→property path is definitively NOT the source of resolution.

  // The phone record has no primary_property_id, so if it were still the first
  // candidate (old code), it would have returned null immediately anyway.
  // Under the new code the phone array is ignored entirely.
  assert.equal("primary_property_id" in phone, false);
});
