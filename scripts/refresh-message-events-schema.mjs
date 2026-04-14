#!/usr/bin/env node
/**
 * refresh-message-events-schema.mjs
 *
 * Fetches the live Message Events app schema from Podio and updates the
 * category-field option lists in schema-attached-supplement.generated.js.
 *
 * The supplement currently inherits base schema options for:
 *   - source-app     (base has 3 options; live has more)
 *   - processed-by   (base has 1 option; live has more)
 *   - failure-bucket  (base has 0 options; live has more)
 *
 * Usage:
 *   node --import ./tests/register-aliases.mjs scripts/refresh-message-events-schema.mjs
 *
 * Requires PODIO_* env vars (same as production).  Use dotenv-cli or export
 * them before running:
 *   PODIO_CLIENT_ID=... PODIO_CLIENT_SECRET=... PODIO_USERNAME=... PODIO_PASSWORD=... \
 *     node --import ./tests/register-aliases.mjs scripts/refresh-message-events-schema.mjs
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

const BASE_SCHEMA_PATH = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../src/lib/podio/schema-attached.generated.js"
);

// Fields we want to refresh in the Message Events supplement.
const TARGET_FIELDS = [
  "source-app",
  "processed-by",
  "failure-bucket",
  "direction",
  "category",
  "status-3",
  "delivery-status",
  "ai-route",
  "is-final-failure",
  "is-opt-out",
];

// ── Fetch ────────────────────────────────────────────────────────────────────

console.log(`\nFetching Message Events app schema (app_id=${APP_IDS.message_events})…`);

const app = await podioRequest("get", `/app/${APP_IDS.message_events}`, null, { type: "full" });

if (!app?.fields?.length) {
  console.error("ERROR: No fields returned from Podio. Check app_id and credentials.");
  process.exit(1);
}

// Build a map of external_id → { type, options, label }
const live_fields = {};
for (const field of app.fields) {
  const ext_id = field.external_id;
  const type = field.type;
  const label = field.config?.label || field.label || ext_id;
  const raw_options = field.config?.settings?.options ?? [];
  const options = raw_options
    .filter((o) => o.status === "active" || o.status == null)
    .map((o) => ({ id: o.id, text: o.text }));

  live_fields[ext_id] = { type, options, label };
}

// ── Report ───────────────────────────────────────────────────────────────────

console.log("\n── Live field data ─────────────────────────────────────────────\n");

for (const ext_id of TARGET_FIELDS) {
  const field = live_fields[ext_id];
  if (!field) {
    console.log(`  ${ext_id}: NOT FOUND in app schema`);
    continue;
  }
  console.log(`  ${ext_id} ("${field.label}"):`);
  console.log(`    type    : ${field.type}`);
  console.log(`    options : ${field.options.length} option(s)`);
  for (const o of field.options) {
    console.log(`              id=${o.id}  "${o.text}"`);
  }
}

// Also report fields NOT in the supplement for awareness
console.log("\n── All live fields ─────────────────────────────────────────────\n");
for (const [ext_id, field] of Object.entries(live_fields)) {
  console.log(`  ${ext_id} ("${field.label}"): type="${field.type}", ${field.options.length} options`);
}

// ── Patch base schema ────────────────────────────────────────────────────────

let base_source = readFileSync(BASE_SCHEMA_PATH, "utf8");
let base_patched = false;

for (const ext_id of TARGET_FIELDS) {
  const live = live_fields[ext_id];
  if (!live || !live.options.length) continue;

  const options_json = live.options
    .map((o) => `          {\n            "id": ${o.id},\n            "text": ${JSON.stringify(o.text)}\n          }`)
    .join(",\n");

  const new_options = `"options": [\n${options_json}\n        ]`;

  // Match the options block for this field in the base schema.
  const field_pattern = new RegExp(
    `("${ext_id}"\\s*:\\s*\\{[^}]*?"options":\\s*)\\[[^\\]]*\\]`,
    "s"
  );

  const before = base_source;
  base_source = base_source.replace(field_pattern, `$1[\n${options_json}\n        ]`);

  if (base_source !== before) {
    console.log(`\n✓  Patched base schema options for "${ext_id}" (${live.options.length} options).`);
    base_patched = true;
  } else {
    console.warn(`\nWARN: Could not locate options block for "${ext_id}" in base schema.`);
  }
}

if (base_patched) {
  writeFileSync(BASE_SCHEMA_PATH, base_source, "utf8");
  console.log(`\nWrote updated base schema → ${BASE_SCHEMA_PATH}`);
} else {
  console.log("\nNo changes written to base schema.");
}

console.log("\nDone. Run the test suite to verify no regressions.");
