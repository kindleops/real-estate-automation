#!/usr/bin/env node
/**
 * refresh-brain-schema.mjs
 *
 * Fetches the live AI Conversation Brain app schema from Podio and updates the
 * category-field option lists in schema-attached-supplement.generated.js.
 *
 * Usage:
 *   set -a && source .env.local && set +a && \
 *     node --import ./tests/register-aliases.mjs scripts/refresh-brain-schema.mjs
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

// Category fields we want to refresh in the brain supplement.
const TARGET_FIELDS = [
  "conversation-stage",
  "ai-route",
  "current-seller-state",
  "follow-up-step",
  "last-detected-intent",
  "seller-profile",
  "language-preference",
  "status-ai-managed",
  "category",
  "category-2",
  "category-3",
  "category-4",
  "category-5",
  "gender",
  "risk-flags-ai",
  "deal-prioirty-tag",
  "follow-up-trigger-state",
];

// ── Fetch ────────────────────────────────────────────────────────────────────

console.log(`\nFetching AI Conversation Brain app schema (app_id=${APP_IDS.ai_conversation_brain})…`);

const app = await podioRequest("get", `/app/${APP_IDS.ai_conversation_brain}`, null, { type: "full" });

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

// Also report ALL live fields for awareness
console.log("\n── All live fields ─────────────────────────────────────────────\n");
for (const [ext_id, field] of Object.entries(live_fields)) {
  console.log(`  ${ext_id} ("${field.label}"): type="${field.type}", ${field.options.length} options`);
}

// ── Patch supplement ─────────────────────────────────────────────────────────

let supp_source = readFileSync(SUPPLEMENT_PATH, "utf8");
let supp_patched = false;

for (const ext_id of TARGET_FIELDS) {
  const live = live_fields[ext_id];
  if (!live || !live.options.length) continue;

  const options_literal = live.options
    .map((o) => `          { id: ${o.id}, text: ${JSON.stringify(o.text)} }`)
    .join(",\n");

  // Match the options array within the brain supplement section.
  // The supplement uses the form:  options: [\n  { id: 1, ... },\n  ...\n]
  // We need to locate the specific field block within the brain section.

  // Strategy: find `"${ext_id}":` followed by `options: [...]` and replace the array.
  const field_pattern = new RegExp(
    `("${ext_id}"\\s*:\\s*\\{[^]*?)(options:\\s*\\[)[^\\]]*?(\\])`,
    "s"
  );

  // Only match within the brain section — find the brain app block start
  const brain_section_start = supp_source.indexOf(`[String(APP_IDS.ai_conversation_brain)]`);
  if (brain_section_start === -1) {
    console.warn(`\nWARN: Could not locate brain section in supplement.`);
    break;
  }

  // Search only within the brain section (approximate — up to next top-level key)
  const brain_section = supp_source.slice(brain_section_start);
  const before = brain_section;
  const patched_section = brain_section.replace(field_pattern, `$1options: [\n${options_literal}\n        $3`);

  if (patched_section !== before) {
    supp_source = supp_source.slice(0, brain_section_start) + patched_section;
    console.log(`\n✓  Patched supplement options for "${ext_id}" (${live.options.length} options).`);
    supp_patched = true;
  } else {
    console.warn(`\nWARN: Could not locate options block for "${ext_id}" in supplement.`);
  }
}

// ── Also patch base schema ──────────────────────────────────────────────────

let base_source = readFileSync(BASE_SCHEMA_PATH, "utf8");
let base_patched = false;

for (const ext_id of TARGET_FIELDS) {
  const live = live_fields[ext_id];
  if (!live || !live.options.length) continue;

  const options_json = live.options
    .map((o) => `          {\n            "id": ${o.id},\n            "text": ${JSON.stringify(o.text)}\n          }`)
    .join(",\n");

  // Match the options block for this field in the base schema's brain section.
  const field_pattern = new RegExp(
    `("${ext_id}"\\s*:\\s*\\{[^}]*?"options":\\s*)\\[[^\\]]*\\]`,
    "s"
  );

  const before = base_source;
  base_source = base_source.replace(field_pattern, `$1[\n${options_json}\n        ]`);

  if (base_source !== before) {
    console.log(`\n✓  Patched base schema options for "${ext_id}" (${live.options.length} options).`);
    base_patched = true;
  }
}

if (supp_patched) {
  writeFileSync(SUPPLEMENT_PATH, supp_source, "utf8");
  console.log(`\nWrote updated supplement → ${SUPPLEMENT_PATH}`);
}

if (base_patched) {
  writeFileSync(BASE_SCHEMA_PATH, base_source, "utf8");
  console.log(`\nWrote updated base schema → ${BASE_SCHEMA_PATH}`);
}

if (!supp_patched && !base_patched) {
  console.log("\nNo changes written.");
}

console.log("\nDone. Run the test suite to verify no regressions.");
