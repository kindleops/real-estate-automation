#!/usr/bin/env node

/**
 * backfill-phone-names-to-supabase.mjs
 *
 * Backfills public.phones name columns from a Podio Phone Numbers JSON export.
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
 *   1) /mnt/data/phone_numbers_30658310.json
 *   2) ./data/phone_numbers_30658310.json
 *   3) ./exports/phone_numbers_30658310.json
 *   4) ./phone_numbers_30658310.json
 */

import fs from "node:fs";
import path from "node:path";

import { createClient } from "@supabase/supabase-js";

const CHUNK_SIZE = 500;

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
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
    "/mnt/data/phone_numbers_30658310.json",
    path.resolve(process.cwd(), "data/phone_numbers_30658310.json"),
    path.resolve(process.cwd(), "exports/phone_numbers_30658310.json"),
    path.resolve(process.cwd(), "phone_numbers_30658310.json"),
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
  const phone_id = pickTopLevel(record, ["phone_id", "phoneId"])
    || getPodioFieldText(record, "phone-id")
    || getPodioFieldText(record, "phone_id");

  const canonical_e164 = normalizeUsE164(
    pickTopLevel(record, ["canonical_e164", "canonicalE164"])
      || getPodioFieldText(record, "canonical-e164")
      || getPodioFieldText(record, "canonical_e164")
  );

  const phone = pickTopLevel(record, ["phone", "phone_number"]) || getPodioFieldText(record, "phone");

  const phone_first_name = pickTopLevel(record, ["phone_first_name", "phoneFirstName"])
    || getPodioFieldText(record, "phone-first-name");

  const phone_full_name = pickTopLevel(record, ["phone_full_name", "phoneFullName"])
    || getPodioFieldText(record, "phone-full-name");

  const primary_display_name = pickTopLevel(record, ["primary_display_name", "primaryDisplayName"])
    || getPodioFieldText(record, "primary-display-name");

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

async function findMatchingPhone(supabase, row, stats) {
  if (clean(row.phone_id)) {
    const byId = await supabase.from("phones").select("phone_id").eq("phone_id", row.phone_id).limit(1);
    if (!byId.error && Array.isArray(byId.data) && byId.data[0]?.phone_id) {
      stats.matched_by_phone_id += 1;
      return byId.data[0].phone_id;
    }
  }

  if (clean(row.canonical_e164)) {
    const byE164 = await supabase
      .from("phones")
      .select("phone_id")
      .eq("canonical_e164", row.canonical_e164)
      .limit(1);
    if (!byE164.error && Array.isArray(byE164.data) && byE164.data[0]?.phone_id) {
      stats.matched_by_e164 += 1;
      return byE164.data[0].phone_id;
    }
  }

  if (clean(row.phone)) {
    const byPhone = await supabase.from("phones").select("phone_id").eq("phone", row.phone).limit(1);
    if (!byPhone.error && Array.isArray(byPhone.data) && byPhone.data[0]?.phone_id) {
      stats.matched_by_phone += 1;
      return byPhone.data[0].phone_id;
    }
  }

  return null;
}

async function main() {
  const supabaseUrl = clean(process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL);
  const serviceKey = clean(process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY);
  const dryRun = asBoolean(process.env.DRY_RUN, false);

  if (!supabaseUrl || !serviceKey) {
    throw new Error("Missing SUPABASE_URL/NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_KEY");
  }

  const jsonPath = resolveJsonPath();
  const payload = readJson(jsonPath);
  const records = toRecords(payload);
  const extracted = records.map(extractPhoneRow);

  const stats = {
    total_json_records: records.length,
    records_with_any_name_data: 0,
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

  console.log(`Using JSON export: ${jsonPath}`);
  console.log(`Dry run: ${dryRun ? "true" : "false"}`);

  const batches = chunk(extracted, CHUNK_SIZE);

  for (let batchIndex = 0; batchIndex < batches.length; batchIndex += 1) {
    const current = batches[batchIndex];
    console.log(`Processing batch ${batchIndex + 1}/${batches.length} (${current.length} records)`);

    for (const row of current) {
      if (!hasAnyName(row)) {
        stats.skipped_no_name += 1;
        continue;
      }
      stats.records_with_any_name_data += 1;

      const matchedPhoneId = await findMatchingPhone(supabase, row, stats);
      if (!matchedPhoneId) {
        stats.unmatched += 1;
        continue;
      }

      const updatePayload = {};
      if (clean(row.phone_first_name)) updatePayload.phone_first_name = clean(row.phone_first_name);
      if (clean(row.phone_full_name)) updatePayload.phone_full_name = clean(row.phone_full_name);
      if (clean(row.primary_display_name)) updatePayload.primary_display_name = clean(row.primary_display_name);
      if (hasUpdatedAt) updatePayload.updated_at = new Date().toISOString();

      if (!Object.keys(updatePayload).length) {
        stats.skipped_no_name += 1;
        continue;
      }

      if (dryRun) {
        stats.updated += 1;
        console.log(
          `[DRY_RUN] phone_id=${matchedPhoneId} first=${JSON.stringify(updatePayload.phone_first_name || "")} full=${JSON.stringify(updatePayload.phone_full_name || "")} primary=${JSON.stringify(updatePayload.primary_display_name || "")}`
        );
        continue;
      }

      const updateResult = await supabase.from("phones").update(updatePayload).eq("phone_id", matchedPhoneId);
      if (updateResult.error) {
        stats.errors += 1;
        console.error(`Update failed for phone_id=${matchedPhoneId}: ${updateResult.error.message}`);
      } else {
        stats.updated += 1;
      }
    }
  }

  console.log("\nBackfill summary");
  console.log(JSON.stringify(stats, null, 2));

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
