#!/usr/bin/env node

/**
 * backfill-phone-names-to-supabase.mjs
 *
 * Backfills public.phones name columns (phone_first_name, phone_full_name,
 * primary_display_name) from a Podio Phone Numbers JSON export.
 *
 * Required env:
 *   SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
 *   (fallback aliases supported: NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_KEY)
 *
 * Optional env:
 *   PHONE_NAMES_JSON_PATH=/path/to/phone_numbers_30658310.json
 *   DRY_RUN=true
 *
 * If PHONE_NAMES_JSON_PATH is not provided, resolution order is:
 *   1) ./data/phone_numbers_30658310.json
 *   2) ./exports/phone_numbers_30658310.json
 *   3) ./phone_numbers_30658310.json
 *   4) /mnt/data/phone_numbers_30658310.json
 */

import fs from "node:fs";
import path from "node:path";

import { createClient } from "@supabase/supabase-js";

const CHUNK_SIZE = 500;

// Carrier names that pollute name fields via LRN lookup — never store as a person name.
const CARRIER_BLOCKLIST = [
  "at&t mobility",
  "t-mobile",
  "verizon wireless",
  "sprint",
  "metropcs",
  "cricket",
  "us cellular",
  "boost mobile",
  "comcast",
  "bandwidth",
  "onvoy",
  "twilio",
];

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

// Podio stores text fields with HTML markup — strip before storing.
function stripHtml(value) {
  return clean(value)
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&nbsp;/g, " ")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, " ")
    .trim();
}

function isCarrierName(value) {
  const lv = lower(value);
  if (!lv) return false;
  return CARRIER_BLOCKLIST.some((carrier) => lv.includes(carrier));
}

function asBoolean(value, fallback = false) {
  const normalized = lower(value);
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function normalizeUsE164(value) {
  const raw = clean(value);
  if (!raw) return "";
  if (raw.startsWith("+")) return raw;

  const digits = raw.replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  if (digits.length === 10) return `+1${digits}`;
  return raw;
}

function readJson(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw);
}

function resolveJsonPath() {
  const envPath = clean(process.env.PHONE_NAMES_JSON_PATH);
  if (envPath) {
    const absolute = path.isAbsolute(envPath) ? envPath : path.resolve(process.cwd(), envPath);
    if (!fs.existsSync(absolute)) {
      throw new Error(`PHONE_NAMES_JSON_PATH does not exist: ${absolute}`);
    }
    return absolute;
  }

  const candidates = [
    path.resolve(process.cwd(), "data/phone_numbers_30658310.json"),
    path.resolve(process.cwd(), "exports/phone_numbers_30658310.json"),
    path.resolve(process.cwd(), "phone_numbers_30658310.json"),
    "/mnt/data/phone_numbers_30658310.json",
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }

  throw new Error(
    "Could not find phone export JSON. Set PHONE_NAMES_JSON_PATH or place file at ./data, ./exports, or project root."
  );
}

function toRecords(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.results)) return payload.results;
  return [];
}

function getPodioFieldText(record, externalId) {
  const fields = Array.isArray(record?.fields) ? record.fields : [];
  const field = fields.find((entry) => clean(entry?.external_id) === externalId);
  const first = Array.isArray(field?.values) ? field.values[0] : null;
  const value = first?.value;

  if (typeof value === "string") return clean(value);
  // (value is already a string — HTML stripping happens in extractPhoneRow)
  if (value && typeof value === "object") {
    if (typeof value.text === "string") return clean(value.text);
    if (typeof value.value === "string") return clean(value.value);
    if (typeof value.phone === "string") return clean(value.phone);
  }

  return "";
}

function pickTopLevel(record, keys = []) {
  for (const key of keys) {
    const value = record?.[key];
    if (value !== null && value !== undefined && clean(value) !== "") return clean(value);
  }
  return "";
}

function extractPhoneRow(record = {}) {
  const phone_id = pickTopLevel(record, ["phone_id", "phoneId", "id"])
    || getPodioFieldText(record, "phone-id")
    || getPodioFieldText(record, "phone_id");

  const canonical_e164 = normalizeUsE164(
    pickTopLevel(record, ["canonical_e164", "canonicalE164"])
      || getPodioFieldText(record, "canonical-e164")
      || getPodioFieldText(record, "canonical_e164")
  );

  const phone = pickTopLevel(record, ["phone", "phone_number"]) || getPodioFieldText(record, "phone");

  const rawFirstName = pickTopLevel(record, ["phone_first_name", "phoneFirstName"])
    || stripHtml(getPodioFieldText(record, "phone-first-name"));

  const rawFullName = pickTopLevel(record, ["phone_full_name", "phoneFullName"])
    || stripHtml(getPodioFieldText(record, "phone-full-name"));

  const rawPrimaryDisplayName = pickTopLevel(record, ["primary_display_name", "primaryDisplayName"])
    || stripHtml(getPodioFieldText(record, "primary-display-name"));

  const phone_first_name = isCarrierName(rawFirstName) ? "" : rawFirstName;
  const phone_full_name = isCarrierName(rawFullName) ? "" : rawFullName;
  const primary_display_name = isCarrierName(rawPrimaryDisplayName) ? "" : rawPrimaryDisplayName;

  return {
    phone_id,
    canonical_e164,
    phone,
    phone_first_name,
    phone_full_name,
    primary_display_name,
  };
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }
  return out;
}

function hasAnyName(row) {
  return Boolean(clean(row.phone_first_name) || clean(row.phone_full_name) || clean(row.primary_display_name));
}

async function detectUpdatedAtColumn(supabase) {
  const probe = await supabase.from("phones").select("updated_at").limit(1);
  if (!probe?.error) return true;
  if (String(probe.error?.message || "").toLowerCase().includes("updated_at")) return false;
  return false;
}

async function countNamedPhones(supabase) {
  const { count: firstNameCount } = await supabase
    .from("phones")
    .select("phone_id", { count: "exact", head: true })
    .not("phone_first_name", "is", null);
  const { count: fullNameCount } = await supabase
    .from("phones")
    .select("phone_id", { count: "exact", head: true })
    .not("phone_full_name", "is", null);
  return { phone_first_name: firstNameCount ?? 0, phone_full_name: fullNameCount ?? 0 };
}

/**
 * Load all phones from Supabase into lookup maps to avoid per-row queries.
 * Returns { byPhoneId: Map, byE164: Map, byPhone: Map } — each map keyed to phone_id.
 */
async function loadPhoneLookupMaps(supabase) {
  const byPhoneId = new Map();
  const byE164 = new Map();
  const byPhone = new Map();

  let from = 0;
  const PAGE = 5000;

  while (true) {
    const { data, error } = await supabase
      .from("phones")
      .select("phone_id, canonical_e164, phone")
      .range(from, from + PAGE - 1);

    if (error) throw new Error(`Failed to load phones: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const row of data) {
      if (clean(row.phone_id)) byPhoneId.set(clean(row.phone_id), row.phone_id);
      if (clean(row.canonical_e164)) byE164.set(clean(row.canonical_e164), row.phone_id);
      if (clean(row.phone)) byPhone.set(clean(row.phone), row.phone_id);
    }

    if (data.length < PAGE) break;
    from += PAGE;
    process.stdout.write(`  Loaded ${from} phone rows...\r`);
  }
  process.stdout.write("\n");

  return { byPhoneId, byE164, byPhone };
}

function findMatchingPhoneLocal(maps, row, stats) {
  const { byPhoneId, byE164, byPhone } = maps;

  if (clean(row.phone_id)) {
    const found = byPhoneId.get(clean(row.phone_id));
    if (found) { stats.matched_by_phone_id += 1; return found; }
  }

  if (clean(row.canonical_e164)) {
    const found = byE164.get(clean(row.canonical_e164));
    if (found) { stats.matched_by_e164 += 1; return found; }
  }

  if (clean(row.phone)) {
    const found = byPhone.get(clean(row.phone));
    if (found) { stats.matched_by_phone += 1; return found; }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Podio API — live fetch
// ---------------------------------------------------------------------------

const PODIO_APP_ID = Number(process.env.PODIO_APP_ID_PHONE_NUMBERS || "30658310");
const PODIO_PAGE_SIZE = Math.min(Number(process.env.PODIO_PAGE_SIZE || "500"), 500);

async function podioAuth(clientId, clientSecret) {
  const username = clean(process.env.PODIO_USERNAME);
  const password = clean(process.env.PODIO_PASSWORD);
  if (!username || !password) {
    throw new Error("PODIO_USERNAME and PODIO_PASSWORD are required for Podio authentication.");
  }
  const res = await fetch("https://podio.com/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "password",
      client_id: clientId,
      client_secret: clientSecret,
      username,
      password,
    }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Podio auth failed (${res.status}): ${text.slice(0, 200)}`);
  }
  const data = await res.json();
  if (!data?.access_token) throw new Error("Podio auth missing access_token");
  return data.access_token;
}

async function podioFetchPage(appId, token, offset, limit, attempt = 0) {
  const res = await fetch(`https://api.podio.com/item/app/${appId}/filter/`, {
    method: "POST",
    headers: { Authorization: `OAuth2 ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ filters: {}, limit, offset }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    // Retry on 429/500/502/503/504 up to 3 times with backoff
    if (attempt < 3 && [429, 500, 502, 503, 504].includes(res.status)) {
      const wait = (attempt + 1) * 2000;
      process.stdout.write(`\n  Podio ${res.status} on offset=${offset}, retry ${attempt + 1}/3 in ${wait / 1000}s...\r`);
      await new Promise((r) => setTimeout(r, wait));
      return podioFetchPage(appId, token, offset, limit, attempt + 1);
    }
    throw new Error(`Podio filter failed (${res.status}): ${text.slice(0, 200)}`);
  }
  return res.json();
}

const PODIO_CONCURRENCY = Number(process.env.PODIO_CONCURRENCY || "4");
const PODIO_BATCH_DELAY_MS = Number(process.env.PODIO_BATCH_DELAY_MS || "500");

async function fetchAllPodioItems(clientId, clientSecret) {
  console.log("Authenticating with Podio...");
  const token = await podioAuth(clientId, clientSecret);
  console.log("Podio auth OK.");

  // First request to get total count
  const probe = await podioFetchPage(PODIO_APP_ID, token, 0, PODIO_PAGE_SIZE);
  const totalItems = probe?.total ?? probe?.filtered ?? 0;
  const firstBatch = Array.isArray(probe?.items) ? probe.items : [];
  console.log(`  Total Podio items: ${totalItems}. Fetching with concurrency=${PODIO_CONCURRENCY}...`);

  // Build all offsets needed
  const offsets = [];
  for (let off = PODIO_PAGE_SIZE; off < totalItems; off += PODIO_PAGE_SIZE) {
    offsets.push(off);
  }

  const allItems = [...firstBatch];
  let completed = firstBatch.length;

  // Fetch remaining pages in parallel batches
  for (let i = 0; i < offsets.length; i += PODIO_CONCURRENCY) {
    const batch = offsets.slice(i, i + PODIO_CONCURRENCY);
    // Small inter-batch delay to reduce 504s
    if (i > 0 && PODIO_BATCH_DELAY_MS > 0) {
      await new Promise((r) => setTimeout(r, PODIO_BATCH_DELAY_MS));
    }
    const results = await Promise.all(
      batch.map((off) => podioFetchPage(PODIO_APP_ID, token, off, PODIO_PAGE_SIZE))
    );
    for (const page of results) {
      const items = Array.isArray(page?.items) ? page.items : [];
      allItems.push(...items);
      completed += items.length;
    }
    process.stdout.write(`  Fetched ${completed} / ${totalItems} Podio items...          \r`);
  }

  process.stdout.write(`\n  Done: ${allItems.length} items fetched.\n`);
  return allItems;
}

// JSON file: try fallback paths, but reject schema-only exports
function tryResolveJsonPath() {
  const envPath = clean(process.env.PHONE_NAMES_JSON_PATH);
  if (envPath) {
    const abs = path.isAbsolute(envPath) ? envPath : path.resolve(process.cwd(), envPath);
    if (!fs.existsSync(abs)) throw new Error(`PHONE_NAMES_JSON_PATH does not exist: ${abs}`);
    return abs;
  }
  const candidates = [
    path.resolve(process.cwd(), "data/phone_numbers_30658310.json"),
    path.resolve(process.cwd(), "exports/phone_numbers_30658310.json"),
    path.resolve(process.cwd(), "phone_numbers_30658310.json"),
    "/mnt/data/phone_numbers_30658310.json",
  ];
  return candidates.find((c) => fs.existsSync(c)) ?? null;
}

function parseJsonFile(filePath) {
  const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.fields) && !payload?.item_id) {
    throw new Error(
      `The JSON at ${filePath} is a Podio app schema export (field definitions only), not item data. ` +
        "Remove PHONE_NAMES_JSON_PATH to fetch live data from Podio API instead."
    );
  }
  return [];
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const supabaseUrl = clean(process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL);
  const serviceKey = clean(process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY);
  const dryRun = asBoolean(process.env.DRY_RUN, false);

  if (!supabaseUrl || !serviceKey) {
    throw new Error("Missing SUPABASE_URL/NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_KEY");
  }

  // Resolve data source: JSON file if available, otherwise Podio API
  let records;
  const jsonPath = tryResolveJsonPath();
  if (jsonPath) {
    console.log(`Using JSON export: ${jsonPath}`);
    records = parseJsonFile(jsonPath);
    console.log(`Parsed ${records.length} records from JSON.`);
  } else {
    const clientId = clean(process.env.PODIO_CLIENT_ID);
    const clientSecret = clean(process.env.PODIO_CLIENT_SECRET);
    if (!clientId || !clientSecret) {
      throw new Error(
        "No JSON file found and PODIO_CLIENT_ID/PODIO_CLIENT_SECRET not set. " +
          "Set PHONE_NAMES_JSON_PATH to a valid item data export, or provide Podio credentials " +
          "(PODIO_CLIENT_ID, PODIO_CLIENT_SECRET, PODIO_USERNAME, PODIO_PASSWORD)."
      );
    }
    console.log(`Fetching live data from Podio app ${PODIO_APP_ID}...`);
    records = await fetchAllPodioItems(clientId, clientSecret);
    console.log(`Fetched ${records.length} items from Podio.`);
  }

  const extracted = records.map(extractPhoneRow);

  const stats = {
    total_records: records.length,
    records_with_name_data: 0,
    matched_by_phone_id: 0,
    matched_by_e164: 0,
    matched_by_phone: 0,
    unmatched: 0,
    updated: 0,
    skipped_no_name: 0,
    errors: 0,
  };

  const supabase = createClient(supabaseUrl, serviceKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const hasUpdatedAt = await detectUpdatedAtColumn(supabase);
  console.log(`Dry run: ${dryRun ? "true" : "false"}`);

  // Before counts
  const beforeCounts = await countNamedPhones(supabase);
  console.log("\nBefore counts:");
  console.log(`  phone_first_name populated: ${beforeCounts.phone_first_name}`);
  console.log(`  phone_full_name populated:  ${beforeCounts.phone_full_name}`);

  // Load all phones into memory for fast local matching
  console.log("\nLoading phones from Supabase for local matching...");
  const phoneMaps = await loadPhoneLookupMaps(supabase);
  console.log(`  phone_id index:       ${phoneMaps.byPhoneId.size} entries`);
  console.log(`  canonical_e164 index: ${phoneMaps.byE164.size} entries`);
  console.log(`  phone index:          ${phoneMaps.byPhone.size} entries`);

  // Build update list by matching from JSON export
  /** @type {Array<{phone_id: string, payload: object}>} */
  const updates = [];

  for (const row of extracted) {
    if (!hasAnyName(row)) {
      stats.skipped_no_name += 1;
      continue;
    }
    stats.records_with_name_data += 1;

    const matchedPhoneId = findMatchingPhoneLocal(phoneMaps, row, stats);

    if (!matchedPhoneId) {
      stats.unmatched += 1;
      continue;
    }

    const updatePayload = {};
    if (clean(row.phone_first_name)) updatePayload.phone_first_name = clean(row.phone_first_name);
    if (clean(row.phone_full_name)) updatePayload.phone_full_name = clean(row.phone_full_name);
    if (clean(row.primary_display_name)) updatePayload.primary_display_name = clean(row.primary_display_name);
    if (hasUpdatedAt) updatePayload.updated_at = new Date().toISOString();

    const nameFieldCount = [updatePayload.phone_first_name, updatePayload.phone_full_name, updatePayload.primary_display_name].filter(Boolean).length;
    if (nameFieldCount === 0) {
      stats.skipped_no_name += 1;
      continue;
    }

    updates.push({ phone_id: matchedPhoneId, payload: updatePayload });
  }

  console.log(`\nUpdates to apply: ${updates.length}`);

  if (dryRun) {
    console.log("\n[DRY_RUN] Sample of planned updates (first 20):");
    for (const u of updates.slice(0, 20)) {
      console.log(
        `  phone_id=${u.phone_id} first=${JSON.stringify(u.payload.phone_first_name || "")} full=${JSON.stringify(u.payload.phone_full_name || "")} primary=${JSON.stringify(u.payload.primary_display_name || "")}`
      );
    }
    stats.updated = updates.length;
  } else {
    // Execute in chunks
    const batches = chunk(updates, CHUNK_SIZE);
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Applying batch ${i + 1}/${batches.length} (${batch.length} updates)...`);
      for (const u of batch) {
        const result = await supabase.from("phones").update(u.payload).eq("phone_id", u.phone_id);
        if (result.error) {
          stats.errors += 1;
          console.error(`  Update failed for phone_id=${u.phone_id}: ${result.error.message}`);
        } else {
          stats.updated += 1;
        }
      }
    }
  }

  // After counts
  const afterCounts = dryRun ? beforeCounts : await countNamedPhones(supabase);

  console.log("\nBackfill summary:");
  console.log(JSON.stringify(stats, null, 2));

  console.log("\nBefore/after phone name coverage:");
  console.log(`  phone_first_name: ${beforeCounts.phone_first_name} → ${afterCounts.phone_first_name}`);
  console.log(`  phone_full_name:  ${beforeCounts.phone_full_name} → ${afterCounts.phone_full_name}`);

  console.log("\nVerification SQL:");
  console.log("select count(*) from phones where phone_first_name is not null or phone_full_name is not null;");
  console.log(
    "select seller_first_name, seller_full_name, phone_first_name, phone_full_name, canonical_e164 from v_sms_ready_contacts where seller_first_name is not null limit 20;"
  );
}

main().catch((error) => {
  console.error("Backfill failed:", error?.message || error);
  process.exit(1);
});
