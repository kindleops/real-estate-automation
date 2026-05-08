#!/usr/bin/env node
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config({ path: ".env.local" });

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

async function run() {
  console.log("=== SEND QUEUE FIELD AUDIT ===\n");

  const fields = ["thread_key","to_phone_number","from_phone_number","master_owner_id","property_id","market","detected_intent"];

  for (const field of fields) {
    const { count, error } = await supabase
      .from("send_queue")
      .select("id", { count: "exact", head: true })
      .is(field, null);

    if (error) { console.error(`Error counting ${field}:`, error.message); continue; }

    const { count: total } = await supabase
      .from("send_queue")
      .select("id", { count: "exact", head: true });

    console.log(`send_queue.${field}: ${count} missing out of ${total} (${((count/total)*100).toFixed(1)}%)`);
  }

  console.log("\n=== MESSAGE EVENTS FIELD AUDIT ===\n");

  for (const field of fields) {
    const { count, error } = await supabase
      .from("message_events")
      .select("id", { count: "exact", head: true })
      .is(field, null);

    if (error) { console.error(`Error counting ${field}:`, error.message); continue; }

    const { count: total } = await supabase
      .from("message_events")
      .select("id", { count: "exact", head: true });

    console.log(`message_events.${field}: ${count} missing out of ${total} (${((count/total)*100).toFixed(1)}%)`);
  }

  console.log("\n=== SEND QUEUE THREAD KEY BREAKDOWN ===\n");
  const { data: sqThreadKeys, error: sqErr } = await supabase
    .from("send_queue")
    .select("thread_key")
    .not("thread_key", "is", null)
    .limit(10000);
  if (!sqErr) {
    const unique = new Set(sqThreadKeys.map(r => r.thread_key));
    console.log(`send_queue: ${unique.size} unique thread_keys (sampled from ${sqThreadKeys.length} rows)`);
  }

  console.log("\n=== MESSAGE EVENTS THREAD KEY BREAKDOWN ===\n");
  const { data: meThreadKeys, error: meErr } = await supabase
    .from("message_events")
    .select("thread_key")
    .not("thread_key", "is", null)
    .limit(10000);
  if (!meErr) {
    const unique = new Set(meThreadKeys.map(r => r.thread_key));
    console.log(`message_events: ${unique.size} unique thread_keys (sampled from ${meThreadKeys.length} rows)`);
  }

  console.log("\n=== TARGET THREAD: phone:+19102422956 ===\n");
  const { data: sqTarget, error: sqtErr } = await supabase
    .from("send_queue")
    .select("id, thread_key, to_phone_number, from_phone_number, master_owner_id, property_id, market, detected_intent, message_body, queue_status, created_at")
    .or(`thread_key.eq.phone:+19102422956,to_phone_number.eq.+19102422956`)
    .order("created_at", { ascending: false });
  if (!sqtErr) {
    console.log(`send_queue rows for thread: ${sqTarget.length}`);
    sqTarget.forEach(r => console.log(`  #${r.id} [${r.queue_status}] ${r.created_at?.slice(0,19)} body="${(r.message_body||"").slice(0,60)}" thread_key=${r.thread_key} mo=${r.master_owner_id} pid=${r.property_id} mkt=${r.market} intent=${r.detected_intent}`));
  }

  const { data: meTarget, error: metErr } = await supabase
    .from("message_events")
    .select("id, thread_key, direction, message_body, master_owner_id, property_id, market, detected_intent, to_phone_number, from_phone_number, created_at")
    .or(`thread_key.eq.phone:+19102422956,from_phone_number.eq.+19102422956,to_phone_number.eq.+19102422956`)
    .order("created_at", { ascending: false });
  if (!metErr) {
    console.log(`\nmessage_events rows for thread: ${meTarget.length}`);
    meTarget.forEach(r => console.log(`  #${r.id} [${r.direction}] ${r.created_at?.slice(0,19)} body="${(r.message_body||"").slice(0,60)}" thread_key=${r.thread_key} mo=${r.master_owner_id} pid=${r.property_id} mkt=${r.market} intent=${r.detected_intent}`));
  }

  const { data: itsTarget, error: itsErr } = await supabase
    .from("inbox_thread_state")
    .select("*")
    .eq("thread_key", "phone:+19102422956")
    .single();
  if (!itsErr) {
    console.log(`\ninbox_thread_state for thread:`);
    console.log(`  message_count=${itsTarget.message_count} inbound=${itsTarget.inbound_count} outbound=${itsTarget.outbound_count}`);
    console.log(`  stage=${itsTarget.stage} status=${itsTarget.status} priority=${itsTarget.priority}`);
    console.log(`  master_owner_id=${itsTarget.master_owner_id} property_id=${itsTarget.property_id} market=${itsTarget.market}`);
    console.log(`  latest: "${itsTarget.latest_message_body?.slice(0,80)}" (${itsTarget.latest_direction} @ ${itsTarget.latest_message_at})`);
    console.log(`  pending_queue=${itsTarget.pending_queue_count} failed_queue=${itsTarget.failed_queue_count} blocked_queue=${itsTarget.blocked_queue_count}`);
  } else {
    console.log(`\ninbox_thread_state: NOT FOUND for this thread`);
  }
}

run().catch(e => { console.error(e); process.exit(1); });
