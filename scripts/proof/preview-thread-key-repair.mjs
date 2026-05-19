#!/usr/bin/env node
/**
 * preview-thread-key-repair.mjs
 *
 * READ-ONLY preview of supabase/migrations/20260519000000_repair_thread_keys.sql
 *
 * Shows exactly which rows would change, by how much, and proves the David and
 * Jessica thread correctness invariants. No writes — safe to run at any time.
 *
 * Usage:
 *   node scripts/proof/preview-thread-key-repair.mjs
 */

import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config({ path: ".env.local" });

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ── Phone normalization (mirrors application code in textgrid.js) ──────────
function normalizePhone(phone) {
  if (!phone) return null;
  const digits = String(phone).replace(/\D/g, "");
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits[0] === "1") return `+${digits}`;
  return null;
}

function canonicalThreadKey(direction, fromPhone, toPhone) {
  const dir = String(direction ?? "").trim().toLowerCase();
  if (dir === "inbound") return normalizePhone(fromPhone);
  if (dir === "outbound") return normalizePhone(toPhone);
  return normalizePhone(fromPhone) || normalizePhone(toPhone);
}

// ── Thread-key pattern classifier ─────────────────────────────────────────
function classifyThreadKeyPattern(key) {
  if (!key) return "null";
  if (/^\+1\d{10}$/.test(key)) return "canonical_e164";
  if (key.startsWith("phone:+")) return "phone_prefix";
  if (key.includes("|")) return "pipe_composite";
  if (key.startsWith("+") && !/^\+1\d{10}$/.test(key)) return "non_us_e164_or_partial";
  if (/^\d+$/.test(key)) return "digits_only";
  return "other";
}

function sep(label = "") {
  const line = "─".repeat(64);
  if (label) console.log(`\n${line}\n  ${label}\n${line}`);
  else console.log(line);
}

function fmt(val) {
  return val === null || val === undefined ? "(null)" : String(val);
}

// ── Main ──────────────────────────────────────────────────────────────────
console.log("\n╔══════════════════════════════════════════════════════════════╗");
console.log("║  Thread-Key Repair Preview — READ ONLY — no writes          ║");
console.log("╚══════════════════════════════════════════════════════════════╝\n");

// ── 1. message_events analysis ─────────────────────────────────────────────
sep("1. message_events — thread_key repair preview");

const { data: me_rows, error: me_err } = await supabase
  .from("message_events")
  .select("id, direction, from_phone_number, to_phone_number, thread_key, created_at")
  .order("created_at", { ascending: false })
  .limit(10000);

if (me_err) {
  console.error("ERROR fetching message_events:", me_err.message);
  process.exit(1);
}

let me_total = 0, me_would_change = 0;
const me_pattern_counts = {};
const me_samples = [];

for (const row of me_rows) {
  me_total++;
  const canonical = canonicalThreadKey(row.direction, row.from_phone_number, row.to_phone_number);
  const pattern = classifyThreadKeyPattern(row.thread_key);
  me_pattern_counts[pattern] = (me_pattern_counts[pattern] || 0) + 1;

  if (canonical && row.thread_key !== canonical) {
    me_would_change++;
    if (me_samples.length < 10) {
      me_samples.push({
        id: row.id,
        direction: row.direction,
        from: row.from_phone_number,
        to: row.to_phone_number,
        old_key: row.thread_key,
        new_key: canonical,
        pattern,
      });
    }
  }
}

console.log(`  Scanned rows (sample up to 10k): ${me_total}`);
console.log(`  Would change:                    ${me_would_change}`);
console.log(`  Already canonical (no change):   ${me_total - me_would_change}`);
console.log("\n  Pattern breakdown (current thread_key):");
for (const [pattern, count] of Object.entries(me_pattern_counts).sort((a, b) => b[1] - a[1])) {
  console.log(`    ${pattern.padEnd(28)} ${count}`);
}
if (me_samples.length > 0) {
  console.log("\n  Sample rows that would change:");
  for (const s of me_samples) {
    console.log(`    [${s.direction}] id=${s.id}`);
    console.log(`      from=${fmt(s.from)}  to=${fmt(s.to)}`);
    console.log(`      old thread_key: ${fmt(s.old_key)}   (${s.pattern})`);
    console.log(`      new thread_key: ${fmt(s.new_key)}`);
  }
}

// ── 2. send_queue analysis ─────────────────────────────────────────────────
sep("2. send_queue — thread_key repair preview");

const { data: sq_rows, error: sq_err } = await supabase
  .from("send_queue")
  .select("id, to_phone_number, thread_key, created_at")
  .order("created_at", { ascending: false })
  .limit(10000);

if (sq_err) {
  console.error("ERROR fetching send_queue:", sq_err.message);
  process.exit(1);
}

let sq_total = 0, sq_would_change = 0;
const sq_pattern_counts = {};
const sq_samples = [];

for (const row of sq_rows) {
  sq_total++;
  const canonical = normalizePhone(row.to_phone_number);
  const pattern = classifyThreadKeyPattern(row.thread_key);
  sq_pattern_counts[pattern] = (sq_pattern_counts[pattern] || 0) + 1;

  if (canonical && row.thread_key !== canonical) {
    sq_would_change++;
    if (sq_samples.length < 10) {
      sq_samples.push({
        id: row.id,
        to: row.to_phone_number,
        old_key: row.thread_key,
        new_key: canonical,
        pattern,
      });
    }
  }
}

console.log(`  Scanned rows (sample up to 10k): ${sq_total}`);
console.log(`  Would change:                    ${sq_would_change}`);
console.log(`  Already canonical (no change):   ${sq_total - sq_would_change}`);
console.log("\n  Pattern breakdown (current thread_key):");
for (const [pattern, count] of Object.entries(sq_pattern_counts).sort((a, b) => b[1] - a[1])) {
  console.log(`    ${pattern.padEnd(28)} ${count}`);
}
if (sq_samples.length > 0) {
  console.log("\n  Sample rows that would change:");
  for (const s of sq_samples) {
    console.log(`    id=${s.id}  to=${fmt(s.to)}`);
    console.log(`      old thread_key: ${fmt(s.old_key)}   (${s.pattern})`);
    console.log(`      new thread_key: ${fmt(s.new_key)}`);
  }
}

// ── 3. inbox_thread_state analysis ────────────────────────────────────────
sep("3. inbox_thread_state — thread_key repair preview");

const { data: its_rows, error: its_err } = await supabase
  .from("inbox_thread_state")
  .select("id, thread_key, master_owner_id, is_archived, is_read, updated_at")
  .order("updated_at", { ascending: false })
  .limit(5000);

if (its_err) {
  console.error("ERROR fetching inbox_thread_state:", its_err.message);
  process.exit(1);
}

let its_total = 0, its_would_change = 0, its_would_delete = 0;
const its_pattern_counts = {};
const its_samples = [];
const its_canonical_set = new Set(
  its_rows
    .map((r) => normalizePhone(r.thread_key))
    .filter(Boolean)
);

for (const row of its_rows) {
  its_total++;
  const canonical = normalizePhone(row.thread_key);
  const pattern = classifyThreadKeyPattern(row.thread_key);
  its_pattern_counts[pattern] = (its_pattern_counts[pattern] || 0) + 1;

  if (canonical && row.thread_key !== canonical) {
    const canonical_already_exists = its_rows.some(
      (r) => r.thread_key === canonical && r.id !== row.id
    );
    if (canonical_already_exists) {
      its_would_delete++;
    } else {
      its_would_change++;
    }
    if (its_samples.length < 10) {
      its_samples.push({
        id: row.id,
        old_key: row.thread_key,
        new_key: canonical,
        action: canonical_already_exists ? "DELETE (canonical exists)" : "UPDATE",
        pattern,
      });
    }
  }
}

console.log(`  Scanned rows (sample up to 5k): ${its_total}`);
console.log(`  Would UPDATE in-place:          ${its_would_change}`);
console.log(`  Would DELETE (canonical exists): ${its_would_delete}`);
console.log(`  Unchanged:                      ${its_total - its_would_change - its_would_delete}`);
console.log("\n  Pattern breakdown (current thread_key):");
for (const [pattern, count] of Object.entries(its_pattern_counts).sort((a, b) => b[1] - a[1])) {
  console.log(`    ${pattern.padEnd(28)} ${count}`);
}
if (its_samples.length > 0) {
  console.log("\n  Sample rows that would change:");
  for (const s of its_samples) {
    console.log(`    [${s.action}] id=${s.id}`);
    console.log(`      old: ${fmt(s.old_key)}   (${s.pattern})`);
    console.log(`      new: ${fmt(s.new_key)}`);
  }
}

// ── 4. David thread proof ─────────────────────────────────────────────────
sep('4. David thread proof — +18605733879');

const DAVID_PHONE = "+18605733879";
const DAVID_CANONICAL = normalizePhone(DAVID_PHONE);

const { data: david_rows, error: david_err } = await supabase
  .from("message_events")
  .select("id, direction, from_phone_number, to_phone_number, thread_key, message_body, created_at")
  .or(
    `from_phone_number.eq.${DAVID_PHONE},to_phone_number.eq.${DAVID_PHONE}` +
    `,from_phone_number.eq.${DAVID_PHONE.replace("+", "")},to_phone_number.eq.${DAVID_PHONE.replace("+", "")}`
  )
  .order("created_at", { ascending: true })
  .limit(50);

if (david_err) {
  console.error("ERROR querying David rows:", david_err.message);
} else if (!david_rows || david_rows.length === 0) {
  console.log(`  No message_events found for phone ${DAVID_PHONE}`);
  console.log("  (Phone may not be in this dataset or is stored differently)");
} else {
  console.log(`  Found ${david_rows.length} message_events for ${DAVID_PHONE}`);
  console.log(`  Canonical thread_key after repair: ${DAVID_CANONICAL}`);
  const pre_unique_keys = new Set(david_rows.map((r) => r.thread_key));
  const post_unique_keys = new Set(
    david_rows.map((r) => canonicalThreadKey(r.direction, r.from_phone_number, r.to_phone_number) || r.thread_key)
  );
  console.log(`  Unique thread_keys BEFORE repair: ${pre_unique_keys.size}  →  ${[...pre_unique_keys].map(fmt).join(", ")}`);
  console.log(`  Unique thread_keys AFTER  repair: ${post_unique_keys.size}  →  ${[...post_unique_keys].map(fmt).join(", ")}`);

  const not_owner_rows = david_rows.filter(
    (r) => r.direction === "inbound" && String(r.message_body || "").toLowerCase().includes("no i do not own")
  );
  if (not_owner_rows.length > 0) {
    const r = not_owner_rows[0];
    const new_key = canonicalThreadKey(r.direction, r.from_phone_number, r.to_phone_number);
    console.log(`\n  "No I do not own that" row:`);
    console.log(`    id=${r.id}  direction=${r.direction}`);
    console.log(`    from=${fmt(r.from_phone_number)}  to=${fmt(r.to_phone_number)}`);
    console.log(`    old thread_key: ${fmt(r.thread_key)}`);
    console.log(`    new thread_key: ${fmt(new_key)}`);
    console.log(`    same as canonical: ${new_key === DAVID_CANONICAL ? "✓ YES" : "✗ NO"}`);
  } else {
    console.log(`  (No "No I do not own that" inbound row found — showing all rows:)`);
    for (const r of david_rows) {
      const new_key = canonicalThreadKey(r.direction, r.from_phone_number, r.to_phone_number);
      console.log(`    [${r.direction}] old=${fmt(r.thread_key)} → new=${fmt(new_key)}  body="${String(r.message_body || "").slice(0, 60)}"`);
    }
  }

  const outbound_rows = david_rows.filter((r) => r.direction === "outbound");
  if (outbound_rows.length > 0) {
    const r = outbound_rows[outbound_rows.length - 1];
    const new_key = canonicalThreadKey("outbound", r.from_phone_number, r.to_phone_number);
    console.log(`\n  Latest outbound row:`);
    console.log(`    id=${r.id}  to=${fmt(r.to_phone_number)}`);
    console.log(`    old thread_key: ${fmt(r.thread_key)}`);
    console.log(`    new thread_key: ${fmt(new_key)}`);
    console.log(`    same as canonical: ${new_key === DAVID_CANONICAL ? "✓ YES" : "✗ NO"}`);
  }
}

// ── 5. Jessica thread proof ───────────────────────────────────────────────
sep("5. Jessica thread proof — already-canonical example");

// Find a thread that already has the correct canonical thread_key for both inbound and outbound
const { data: jessica_candidates, error: jessica_err } = await supabase
  .from("message_events")
  .select("id, direction, from_phone_number, to_phone_number, thread_key, created_at")
  .filter("thread_key", "like", "+1%")
  .order("created_at", { ascending: false })
  .limit(500);

if (jessica_err) {
  console.error("ERROR querying for Jessica candidates:", jessica_err.message);
} else {
  // Find a phone that has both inbound and outbound rows all with correct canonical keys
  const by_phone = new Map();
  for (const r of jessica_candidates) {
    const phone = r.direction === "outbound" ? r.to_phone_number : r.from_phone_number;
    const canonical = normalizePhone(phone);
    if (!canonical) continue;
    if (!by_phone.has(canonical)) by_phone.set(canonical, []);
    by_phone.get(canonical).push(r);
  }

  let jessica_phone = null;
  for (const [phone, rows] of by_phone) {
    const all_canonical = rows.every(
      (r) => canonicalThreadKey(r.direction, r.from_phone_number, r.to_phone_number) === r.thread_key
    );
    const has_both_directions = rows.some((r) => r.direction === "inbound") &&
                                 rows.some((r) => r.direction === "outbound");
    if (all_canonical && has_both_directions) {
      jessica_phone = phone;
      break;
    }
  }

  if (jessica_phone) {
    const rows = by_phone.get(jessica_phone);
    console.log(`  Example phone (already canonical): ${jessica_phone}`);
    console.log(`  Rows found: ${rows.length}`);
    for (const r of rows.slice(0, 6)) {
      const new_key = canonicalThreadKey(r.direction, r.from_phone_number, r.to_phone_number);
      const unchanged = new_key === r.thread_key;
      console.log(`    [${r.direction}] thread_key=${fmt(r.thread_key)}  →  ${unchanged ? "✓ UNCHANGED" : `WOULD CHANGE → ${new_key}`}`);
    }
  } else {
    console.log("  No already-canonical two-direction thread found in sample of 500.");
    console.log("  (All recent threads may be outbound-only or inbound-only.)");
  }
}

// ── 6. Post-migration uniqueness proof ────────────────────────────────────
sep("6. Post-migration inbox_thread_state uniqueness proof");

// Compute what inbox_thread_state would look like after repair
const post_keys = new Map(); // canonical_key -> count of current rows that map to it
for (const row of its_rows) {
  const canonical = normalizePhone(row.thread_key) || row.thread_key;
  if (!canonical) continue;
  post_keys.set(canonical, (post_keys.get(canonical) || 0) + 1);
}

const duplicated_post = [...post_keys.entries()].filter(([, count]) => count > 1);

console.log(`  Current inbox_thread_state rows: ${its_total}`);
console.log(`  Unique canonical keys after repair: ${post_keys.size}`);
console.log(`  Rows with duplicate canonical target: ${duplicated_post.length}`);

if (duplicated_post.length === 0) {
  console.log("  ✓ No duplicates — migration will produce exactly one row per seller phone");
} else {
  console.log("  ⚠ Duplicates found (migration will DELETE the stale row, keeping the canonical):");
  for (const [key, count] of duplicated_post.slice(0, 10)) {
    console.log(`    ${key}  (${count} rows → 1 after DELETE)`);
  }
}

// Count how many are already canonical E.164
const already_canonical_its = its_rows.filter(
  (r) => /^\+1\d{10}$/.test(r.thread_key)
).length;
console.log(`\n  Already canonical E.164: ${already_canonical_its} / ${its_total}`);
console.log(`  Non-canonical (would be repaired): ${its_total - already_canonical_its}`);

// ── Summary ───────────────────────────────────────────────────────────────
sep("Summary");
console.log(`  message_events rows scanned:          ${me_total}`);
console.log(`  message_events would change:          ${me_would_change}`);
console.log(`  send_queue rows scanned:              ${sq_total}`);
console.log(`  send_queue would change:              ${sq_would_change}`);
console.log(`  inbox_thread_state rows scanned:      ${its_total}`);
console.log(`  inbox_thread_state would UPDATE:      ${its_would_change}`);
console.log(`  inbox_thread_state would DELETE:      ${its_would_delete}`);
console.log(`  inbox_thread_state unique post-repair: ${post_keys.size}`);
console.log(`\n  Migration file: supabase/migrations/20260519000000_repair_thread_keys.sql`);
console.log("  Status: NOT APPLIED — review this output before running.\n");
