#!/usr/bin/env node
/**
 * refresh-send-queue-schema.mjs
 *
 * Fetches the live Send Queue app schema from Podio and updates the
 * category-field option lists in schema-attached-supplement.generated.js.
 *
 * Run once after new category fields / options are added to the Send Queue
 * Podio app.  The supplement currently has options: [] for:
 *   - property-type
 *   - owner-type
 *   - category
 *   - use-case-template
 *
 * It also reports the field type for property-address so the supplement type
 * can be corrected if Podio uses 'location' instead of 'text'.
 *
 * Usage:
 *   node --import ./tests/register-aliases.mjs scripts/refresh-send-queue-schema.mjs
 *
 * Requires PODIO_* env vars (same as production).  Use dotenv-cli or export
 * them before running:
 *   PODIO_CLIENT_ID=... PODIO_CLIENT_SECRET=... PODIO_USERNAME=... PODIO_PASSWORD=... \
 *     node --import ./tests/register-aliases.mjs scripts/refresh-send-queue-schema.mjs
 */

import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

import { podioRequest } from "@/lib/providers/podio.js";
import APP_IDS from "@/lib/config/app-ids.js";

const SUPPLEMENT_PATH = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../src/lib/podio/schema-attached-supplement.generated.js"
);

// Fields we want to refresh in the Send Queue supplement.
const TARGET_FIELDS = [
  "property-type",
  "owner-type",
  "category",
  "use-case-template",
  "property-address",
];

// ── Fetch ────────────────────────────────────────────────────────────────────

console.log(`\nFetching Send Queue app schema (app_id=${APP_IDS.send_queue})…`);

const app = await podioRequest("get", `/app/${APP_IDS.send_queue}`, null, { type: "full" });

if (!app?.fields?.length) {
  console.error("ERROR: No fields returned from Podio. Check app_id and credentials.");
  process.exit(1);
}

// Build a map of external_id → { type, options }
const live_fields = {};
for (const field of app.fields) {
  const ext_id = field.external_id;
  const type = field.type;
  const raw_options = field.config?.settings?.options ?? [];
  const options = raw_options
    .filter((o) => o.status === "active" || o.status == null)
    .map((o) => ({ id: o.id, text: o.text }));

  live_fields[ext_id] = { type, options };
}

// ── Report ───────────────────────────────────────────────────────────────────

console.log("\n── Live field data ─────────────────────────────────────────────\n");

for (const ext_id of TARGET_FIELDS) {
  const field = live_fields[ext_id];
  if (!field) {
    console.log(`  ${ext_id}: NOT FOUND in app schema`);
    continue;
  }
  console.log(`  ${ext_id}:`);
  console.log(`    type    : ${field.type}`);
  console.log(`    options : ${field.options.length} option(s)`);
  for (const o of field.options) {
    console.log(`              id=${o.id}  "${o.text}"`);
  }
}

// ── Patch supplement ─────────────────────────────────────────────────────────

const PATCH_TARGETS = ["property-type", "owner-type", "category", "use-case-template"];

let source = readFileSync(SUPPLEMENT_PATH, "utf8");
let patched = false;

for (const ext_id of PATCH_TARGETS) {
  const live = live_fields[ext_id];
  if (!live) {
    console.warn(`\nWARN: ${ext_id} not found in live schema — skipping patch.`);
    continue;
  }

  if (!live.options.length) {
    console.warn(`\nWARN: ${ext_id} has no active options in Podio — skipping patch.`);
    continue;
  }

  // Build the replacement options block.
  const options_json = live.options
    .map((o) => `        { id: ${o.id}, text: ${JSON.stringify(o.text)} }`)
    .join(",\n");

  const new_block = `options: [\n${options_json},\n      ]`;

  // Match the block for this field.  The supplement uses the pattern:
  //   "property-type": { ...  options: [], }
  // We want to replace the `options: []` on the correct field block only.
  // We use a targeted regex anchored to the field's external_id.
  const field_pattern = new RegExp(
    `("${ext_id}"\\s*:\\s*\\{[^}]*?)options:\\s*\\[\\]`,
    "s"
  );

  const before = source;
  source = source.replace(field_pattern, `$1${new_block}`);

  if (source !== before) {
    console.log(`\n✓  Patched options for "${ext_id}" (${live.options.length} options).`);
    patched = true;
  } else {
    console.warn(`\nWARN: Could not locate options: [] block for "${ext_id}" in supplement.`);
  }
}

if (patched) {
  writeFileSync(SUPPLEMENT_PATH, source, "utf8");
  console.log(`\nWrote updated supplement → ${SUPPLEMENT_PATH}`);
} else {
  console.log("\nNo changes written (nothing to patch or all options already populated).");
}

// ── property-address type check ───────────────────────────────────────────────

const pa = live_fields["property-address"];
if (pa) {
  if (pa.type !== "text") {
    console.log(`
────────────────────────────────────────────────────────────────────────
ATTENTION — property-address type mismatch
  Supplement says : type: "text"
  Podio reports   : type: "${pa.type}"

  If Podio is using type "location", update the supplement manually:
    "property-address": {
      label: "Property Address",
      type: "location",   // ← change from "text"
      ...
    }

  The current write (plain string) is accepted by Podio location fields via
  geocoding, so creates likely succeed.  But the supplement type should match
  the actual field type to keep schema validation accurate.
────────────────────────────────────────────────────────────────────────`);
  } else {
    console.log('\n✓  property-address type matches supplement ("text").');
  }
}

console.log("\nDone.\n");
