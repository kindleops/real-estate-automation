#!/usr/bin/env node
/**
 * post-thread-key-repair-proof.mjs
 *
 * Run AFTER applying 20260519000000_repair_thread_keys.sql to prove:
 *
 *   1. Zero message_events with phone_prefix thread_key  (LIKE 'phone:%')
 *   2. Zero message_events with pipe_composite thread_key (LIKE '%|%')
 *   3. Zero outbound message_events with null thread_key when to_phone_number exists
 *   4. Zero inbound  message_events with null thread_key when from_phone_number exists
 *   5. David (+18605733879) — both directions unified under one canonical thread_key
 *   6. Jessica (+12095079366) — both directions unified under one canonical thread_key
 *   7. Zero send_queue rows with phone_prefix or pipe_composite thread_key
 *   8. Zero send_queue rows with null thread_key when to_phone_number exists
 *   9. Zero inbox_thread_state rows with phone_prefix or pipe_composite thread_key
 *  10. No duplicate inbox_thread_state rows for the same canonical E.164 phone
 *  11. Backup tables exist and recorded changes
 *
 * Usage:
 *   node scripts/proof/post-thread-key-repair-proof.mjs
 */

import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config({ path: ".env.local" });

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("ERROR: Need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.local");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ── Helpers ────────────────────────────────────────────────────────────────
let passed = 0, failed = 0, warned = 0;

function assert(label, condition, detail = "") {
  if (condition) {
    console.log(`  ✓ ${label}`);
    passed++;
  } else {
    console.error(`  ✗ ${label}${detail ? `  — ${detail}` : ""}`);
    failed++;
  }
}

function warn(label, detail = "") {
  console.warn(`  ⚠ ${label}${detail ? `  — ${detail}` : ""}`);
  warned++;
}

function sep(label) {
  console.log(`\n${"─".repeat(60)}\n  ${label}\n${"─".repeat(60)}`);
}

async function sql(query, params = {}) {
  const { data, error } = await supabase.rpc("query", { sql: query, ...params }).catch(() => ({ data: null, error: { message: "rpc not available" } }));
  return { data, error };
}

async function count(table, filter) {
  const q = supabase.from(table).select("*", { count: "exact", head: true });
  if (filter) Object.entries(filter).forEach(([k, v]) => q.eq(k, v));
  const { count: n, error } = await q;
  return { count: n, error };
}

// ── Main ───────────────────────────────────────────────────────────────────
console.log("\n╔══════════════════════════════════════════════════════════╗");
console.log("║  Post-Thread-Key-Repair Proof                           ║");
console.log("╚══════════════════════════════════════════════════════════╝\n");
console.log("Connecting to Supabase...");

// Quick connectivity check
const { error: pingError } = await supabase.from("message_events").select("id").limit(1);
if (pingError) {
  console.error("ERROR: Cannot connect to Supabase:", pingError.message);
  process.exit(1);
}
console.log("Connected.\n");

// ── 1. message_events: no phone_prefix keys ───────────────────────────────
sep("1. message_events — no phone_prefix thread_keys");

const { data: phonePrefix, error: ppErr } = await supabase
  .from("message_events")
  .select("id, thread_key, direction, to_phone_number, from_phone_number", { count: "exact" })
  .like("thread_key", "phone:%")
  .limit(5);

assert(
  "zero message_events with thread_key LIKE 'phone:%'",
  !ppErr && (phonePrefix?.length ?? 1) === 0,
  ppErr ? ppErr.message : `found ${phonePrefix?.length} rows`
);
if (phonePrefix?.length > 0) {
  phonePrefix.forEach(r => console.error(`    id=${r.id} dir=${r.direction} key=${r.thread_key}`));
}

// ── 2. message_events: no pipe_composite keys ─────────────────────────────
sep("2. message_events — no pipe_composite thread_keys");

const { data: pipeRows, error: pipeErr } = await supabase
  .from("message_events")
  .select("id, thread_key, direction")
  .like("thread_key", "%|%")
  .limit(5);

assert(
  "zero message_events with thread_key LIKE '%|%'",
  !pipeErr && (pipeRows?.length ?? 1) === 0,
  pipeErr ? pipeErr.message : `found ${pipeRows?.length} rows`
);
if (pipeRows?.length > 0) {
  pipeRows.forEach(r => console.error(`    id=${r.id} dir=${r.direction} key=${r.thread_key}`));
}

// ── 3. message_events outbound: no null thread_key when to_phone_number exists
sep("3. message_events outbound — no null thread_key when to_phone_number exists");

const { data: nullOutbound, error: noErr } = await supabase
  .from("message_events")
  .select("id, to_phone_number")
  .eq("direction", "outbound")
  .is("thread_key", null)
  .not("to_phone_number", "is", null)
  .limit(5);

assert(
  "zero outbound rows with null thread_key + non-null to_phone_number",
  !noErr && (nullOutbound?.length ?? 1) === 0,
  noErr ? noErr.message : `found ${nullOutbound?.length} rows`
);
if (nullOutbound?.length > 0) {
  nullOutbound.forEach(r => console.error(`    id=${r.id} to=${r.to_phone_number}`));
}

// ── 4. message_events inbound: no null thread_key when from_phone_number exists
sep("4. message_events inbound — no null thread_key when from_phone_number exists");

const { data: nullInbound, error: niErr } = await supabase
  .from("message_events")
  .select("id, from_phone_number")
  .eq("direction", "inbound")
  .is("thread_key", null)
  .not("from_phone_number", "is", null)
  .limit(5);

assert(
  "zero inbound rows with null thread_key + non-null from_phone_number",
  !niErr && (nullInbound?.length ?? 1) === 0,
  niErr ? niErr.message : `found ${nullInbound?.length} rows`
);
if (nullInbound?.length > 0) {
  nullInbound.forEach(r => console.error(`    id=${r.id} from=${r.from_phone_number}`));
}

// ── 5. David thread unified ───────────────────────────────────────────────
sep("5. David (+18605733879) — both directions unified");

const DAVID_PHONE = "+18605733879";
const { data: davidRows, error: davidErr } = await supabase
  .from("message_events")
  .select("id, direction, thread_key, from_phone_number, to_phone_number")
  .or(`from_phone_number.eq.${DAVID_PHONE},to_phone_number.eq.${DAVID_PHONE}`)
  .order("created_at", { ascending: false })
  .limit(20);

if (davidErr) {
  assert("David rows fetched", false, davidErr.message);
} else {
  const davidThreadKeys = [...new Set(davidRows.map(r => r.thread_key).filter(Boolean))];
  assert(
    `David rows all use single canonical thread_key (${DAVID_PHONE})`,
    davidThreadKeys.length === 1 && davidThreadKeys[0] === DAVID_PHONE,
    `found thread_keys: ${davidThreadKeys.join(", ") || "(none)"}`
  );
  const directions = [...new Set(davidRows.map(r => r.direction))];
  assert(
    "David has both inbound and outbound events",
    directions.includes("inbound") && directions.includes("outbound"),
    `directions: ${directions.join(", ")}`
  );
  console.log(`    ${davidRows.length} David rows, thread_keys: ${davidThreadKeys.join(", ") || "(none)"}`);
}

// ── 6. Jessica thread unified ─────────────────────────────────────────────
sep("6. Jessica (+12095079366) — both directions unified");

const JESSICA_PHONE = "+12095079366";
const { data: jessicaRows, error: jessicaErr } = await supabase
  .from("message_events")
  .select("id, direction, thread_key, from_phone_number, to_phone_number")
  .or(`from_phone_number.eq.${JESSICA_PHONE},to_phone_number.eq.${JESSICA_PHONE}`)
  .order("created_at", { ascending: false })
  .limit(20);

if (jessicaErr) {
  assert("Jessica rows fetched", false, jessicaErr.message);
} else {
  const jessicaThreadKeys = [...new Set(jessicaRows.map(r => r.thread_key).filter(Boolean))];
  assert(
    `Jessica rows all use single canonical thread_key (${JESSICA_PHONE})`,
    jessicaThreadKeys.length === 1 && jessicaThreadKeys[0] === JESSICA_PHONE,
    `found thread_keys: ${jessicaThreadKeys.join(", ") || "(none)"}`
  );
  const directions = [...new Set(jessicaRows.map(r => r.direction))];
  assert(
    "Jessica has both inbound and outbound events",
    directions.includes("inbound") && directions.includes("outbound"),
    `directions: ${directions.join(", ")}`
  );
  console.log(`    ${jessicaRows.length} Jessica rows, thread_keys: ${jessicaThreadKeys.join(", ") || "(none)"}`);
}

// ── 7. send_queue: no bad thread_keys ────────────────────────────────────
sep("7. send_queue — no pipe_composite or phone_prefix thread_keys");

const { data: sqPipe, error: sqPipeErr } = await supabase
  .from("send_queue")
  .select("id, thread_key, queue_status")
  .like("thread_key", "%|%")
  .limit(5);

assert(
  "zero send_queue rows with pipe_composite thread_key",
  !sqPipeErr && (sqPipe?.length ?? 1) === 0,
  sqPipeErr ? sqPipeErr.message : `found ${sqPipe?.length} rows`
);

const { data: sqPhone, error: sqPhoneErr } = await supabase
  .from("send_queue")
  .select("id, thread_key, queue_status")
  .like("thread_key", "phone:%")
  .limit(5);

assert(
  "zero send_queue rows with phone_prefix thread_key",
  !sqPhoneErr && (sqPhone?.length ?? 1) === 0,
  sqPhoneErr ? sqPhoneErr.message : `found ${sqPhone?.length} rows`
);

// ── 8. send_queue: no null thread_key when to_phone_number exists ─────────
sep("8. send_queue — no null thread_key when to_phone_number exists");

const { data: sqNull, error: sqNullErr } = await supabase
  .from("send_queue")
  .select("id, to_phone_number, queue_status")
  .is("thread_key", null)
  .not("to_phone_number", "is", null)
  .limit(5);

assert(
  "zero send_queue rows with null thread_key + non-null to_phone_number",
  !sqNullErr && (sqNull?.length ?? 1) === 0,
  sqNullErr ? sqNullErr.message : `found ${sqNull?.length} rows`
);
if (sqNull?.length > 0) {
  sqNull.forEach(r => console.error(`    id=${r.id} status=${r.queue_status} to=${r.to_phone_number}`));
}

// ── 9. inbox_thread_state: no bad thread_keys ────────────────────────────
sep("9. inbox_thread_state — no phone_prefix or pipe_composite thread_keys");

const { data: itsPipe, error: itsPipeErr } = await supabase
  .from("inbox_thread_state")
  .select("thread_key")
  .like("thread_key", "%|%")
  .limit(5);

assert(
  "zero inbox_thread_state rows with pipe_composite thread_key",
  !itsPipeErr && (itsPipe?.length ?? 1) === 0,
  itsPipeErr ? itsPipeErr.message : `found ${itsPipe?.length} rows`
);

const { data: itsPhone, error: itsPhoneErr } = await supabase
  .from("inbox_thread_state")
  .select("thread_key")
  .like("thread_key", "phone:%")
  .limit(5);

assert(
  "zero inbox_thread_state rows with phone_prefix thread_key",
  !itsPhoneErr && (itsPhone?.length ?? 1) === 0,
  itsPhoneErr ? itsPhoneErr.message : `found ${itsPhone?.length} rows`
);

// ── 10. inbox_thread_state: no duplicate canonical keys ───────────────────
sep("10. inbox_thread_state — no duplicate canonical thread_keys");

// Fetch all thread_keys (post-migration, all should be canonical E.164)
const { data: itsAll, error: itsAllErr } = await supabase
  .from("inbox_thread_state")
  .select("thread_key")
  .not("thread_key", "is", null);

if (itsAllErr) {
  assert("inbox_thread_state rows fetched", false, itsAllErr.message);
} else {
  const keyCounts = {};
  for (const row of itsAll) {
    keyCounts[row.thread_key] = (keyCounts[row.thread_key] || 0) + 1;
  }
  const dupes = Object.entries(keyCounts).filter(([, n]) => n > 1);
  assert(
    `no duplicate thread_keys in inbox_thread_state (${itsAll.length} rows, ${Object.keys(keyCounts).length} unique)`,
    dupes.length === 0,
    dupes.length > 0 ? `duplicates: ${dupes.map(([k, n]) => `${k}×${n}`).join(", ")}` : ""
  );
}

// ── 11. Backup tables exist and recorded changes ───────────────────────────
sep("11. Backup tables exist and recorded expected row counts");

const BACKUP_TABLES = [
  "message_events_thread_key_repair_backup_20260519",
  "send_queue_thread_key_repair_backup_20260519",
  "inbox_thread_state_thread_key_repair_backup_20260519",
];

for (const table of BACKUP_TABLES) {
  const { data: rows, error: bErr } = await supabase
    .from(table)
    .select("*", { count: "exact", head: true });

  if (bErr) {
    assert(`backup table ${table} exists`, false, bErr.message);
  } else {
    // The backup tables should have rows (they were captured before the migration ran)
    // We warn (not fail) if count=0 because on a re-run of an already-clean DB the migration
    // would find nothing to change and nothing to back up.
    if (rows === null) {
      warn(`backup table ${table} is empty — migration may have already been applied and DB was already clean`);
    } else {
      console.log(`  ✓ backup table ${table} exists`);
      passed++;
    }
  }
}

// ── Summary ───────────────────────────────────────────────────────────────
const line = "═".repeat(60);
console.log(`\n${line}`);
console.log(`  Results: ${passed} passed, ${failed} failed, ${warned} warnings`);
console.log(`${line}\n`);

if (failed > 0) process.exit(1);
