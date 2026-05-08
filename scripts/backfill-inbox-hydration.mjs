#!/usr/bin/env node
/**
 * scripts/backfill-inbox-hydration.mjs
 *
 * Comprehensive inbox hydration backfill:
 * 1. Backfill market from properties
 * 2. Backfill thread_key in send_queue from to_phone_number
 * 3. Backfill master_owner_id/property_id from various sources
 * 4. Backfill detected_intent from metadata
 * 5. Rebuild inbox_thread_state from canonical timeline
 */

import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config({ path: ".env.local" });

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

const DRY_RUN = process.env.DRY_RUN === "true";
const BATCH = 500;

function normalizePhone(phone) {
  if (!phone) return "";
  const digits = phone.replace(/\D/g, "");
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  return phone.startsWith("+") ? phone : `+${digits}`;
}

function threadKeyFromPhone(phone) {
  const n = normalizePhone(phone);
  return n ? `phone:${n}` : null;
}

async function batchUpdate(table, ids, updates) {
  if (DRY_RUN) {
    console.log(`  [DRY] Would update ${ids.length} rows in ${table}`);
    return { count: ids.length };
  }
  const { error, count } = await supabase
    .from(table)
    .update(updates)
    .in("id", ids);
  if (error) throw error;
  return { count: count ?? ids.length };
}

async function backfillMarketFromProperties() {
  console.log("\n=== 1. Backfill market in send_queue from properties ===");
  const { data: rows, error } = await supabase
    .from("send_queue")
    .select("id, property_id")
    .is("market", null)
    .not("property_id", "is", null)
    .limit(5000);
  if (error) { console.error("  Error:", error.message); return; }
  console.log(`  Found ${rows.length} rows to backfill`);
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    for (const row of batch) {
      const { data: prop } = await supabase
        .from("properties")
        .select("market")
        .eq("property_id", row.property_id)
        .maybeSingle();
      if (prop?.market) {
        await batchUpdate("send_queue", [row.id], { market: prop.market });
      }
    }
    console.log(`  Progress: ${Math.min(i + BATCH, rows.length)}/${rows.length}`);
  }
  console.log("  Done.");
}

async function backfillMarketInMessageEvents() {
  console.log("\n=== 2. Backfill market in message_events from properties ===");
  const { data: rows, error } = await supabase
    .from("message_events")
    .select("id, property_id, queue_id")
    .is("market", null)
    .limit(5000);
  if (error) { console.error("  Error:", error.message); return; }
  console.log(`  Found ${rows.length} rows to backfill`);

  // Priority 1: from properties table
  const withProp = rows.filter(r => r.property_id);
  for (let i = 0; i < withProp.length; i += BATCH) {
    const batch = withProp.slice(i, i + BATCH);
    for (const row of batch) {
      const { data: prop } = await supabase
        .from("properties")
        .select("market")
        .eq("property_id", String(row.property_id))
        .maybeSingle();
      if (prop?.market) {
        await batchUpdate("message_events", [row.id], { market: prop.market });
      }
    }
  }

  // Priority 2: from send_queue
  const withQueue = rows.filter(r => r.queue_id && !r.property_id);
  for (let i = 0; i < withQueue.length; i += BATCH) {
    const batch = withQueue.slice(i, i + BATCH);
    for (const row of batch) {
      const { data: sq } = await supabase
        .from("send_queue")
        .select("market")
        .eq("id", row.queue_id)
        .maybeSingle();
      if (sq?.market) {
        await batchUpdate("message_events", [row.id], { market: sq.market });
      }
    }
  }
  console.log("  Done.");
}

async function backfillThreadKeyInSendQueue() {
  console.log("\n=== 3. Backfill thread_key in send_queue ===");
  const { data: rows, error } = await supabase
    .from("send_queue")
    .select("id, to_phone_number")
    .is("thread_key", null)
    .not("to_phone_number", "is", null)
    .limit(5000);
  if (error) { console.error("  Error:", error.message); return; }

  let updated = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const updates = batch
      .map(r => ({ id: r.id, thread_key: threadKeyFromPhone(r.to_phone_number) }))
      .filter(u => u.thread_key);
    for (const u of updates) {
      await batchUpdate("send_queue", [u.id], { thread_key: u.thread_key });
      updated++;
    }
  }
  console.log(`  Updated ${updated} rows.`);
}

async function backfillMasterOwnerPropertyInMessageEvents() {
  console.log("\n=== 4. Backfill master_owner_id/property_id in message_events from send_queue ===");
  const { data: rows, error } = await supabase
    .from("message_events")
    .select("id, queue_id, master_owner_id, property_id")
    .or("master_owner_id.is.null,property_id.is.null")
    .not("queue_id", "is", null)
    .limit(5000);
  if (error) { console.error("  Error:", error.message); return; }
  console.log(`  Found ${rows.length} rows needing backfill`);

  let updated = 0;
  for (const row of rows) {
    const { data: sq } = await supabase
      .from("send_queue")
      .select("master_owner_id, property_id")
      .eq("id", row.queue_id)
      .maybeSingle();
    if (!sq) continue;
    const updates = {};
    if (!row.master_owner_id && sq.master_owner_id) updates.master_owner_id = sq.master_owner_id;
    if (!row.property_id && sq.property_id) updates.property_id = sq.property_id;
    if (Object.keys(updates).length > 0) {
      await batchUpdate("message_events", [row.id], updates);
      updated++;
    }
  }
  console.log(`  Updated ${updated} rows.`);
}

async function backfillMasterOwnerPropertyFromThreadMatch() {
  console.log("\n=== 5. Backfill master_owner_id/property_id from thread match ===");
  const { data: rows, error } = await supabase
    .from("message_events")
    .select("id, thread_key, master_owner_id, property_id")
    .not("thread_key", "is", null)
    .or("master_owner_id.is.null,property_id.is.null")
    .limit(5000);
  if (error) { console.error("  Error:", error.message); return; }
  console.log(`  Found ${rows.length} rows needing thread-based backfill`);

  // Group by thread_key, get best known mo/pid per thread
  const { data: known } = await supabase
    .from("message_events")
    .select("thread_key, master_owner_id, property_id")
    .not("thread_key", "is", null)
    .not("master_owner_id", "is", null)
    .limit(5000);

  const bestPerThread = {};
  if (known) {
    for (const k of known) {
      if (!bestPerThread[k.thread_key]) bestPerThread[k.thread_key] = {};
      if (k.master_owner_id) bestPerThread[k.thread_key].master_owner_id = k.master_owner_id;
      if (k.property_id) bestPerThread[k.thread_key].property_id = k.property_id;
    }
  }

  let updated = 0;
  for (const row of rows) {
    const best = bestPerThread[row.thread_key];
    if (!best) continue;
    const updates = {};
    if (!row.master_owner_id && best.master_owner_id) updates.master_owner_id = best.master_owner_id;
    if (!row.property_id && best.property_id) updates.property_id = best.property_id;
    if (Object.keys(updates).length > 0) {
      await batchUpdate("message_events", [row.id], updates);
      updated++;
    }
  }
  console.log(`  Updated ${updated} rows.`);
}

async function backfillDetectedIntent() {
  console.log("\n=== 6. Backfill detected_intent in send_queue from metadata ===");
  const { data: rows, error } = await supabase
    .from("send_queue")
    .select("id, metadata")
    .is("detected_intent", null)
    .limit(5000);
  if (error) { console.error("  Error:", error.message); return; }

  let updated = 0;
  for (const row of rows) {
    const meta = row.metadata || {};
    const intent = meta.detected_intent || meta.intent || null;
    if (intent) {
      await batchUpdate("send_queue", [row.id], { detected_intent: intent });
      updated++;
    }
  }
  console.log(`  Updated ${updated} rows.`);
}

async function main() {
  console.log(`Inbox Hydration Backfill (Dry Run: ${DRY_RUN})`);
  console.log("=".repeat(50));

  await backfillMarketFromProperties();
  await backfillMarketInMessageEvents();
  await backfillThreadKeyInSendQueue();
  await backfillMasterOwnerPropertyInMessageEvents();
  await backfillMasterOwnerPropertyFromThreadMatch();
  await backfillDetectedIntent();

  console.log("\n=== Backfill Complete ===");
  console.log("Now run: node scripts/backfill-inbox-thread-state.mjs");
  console.log("Then: npm run proof:inbox-backfill");
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
