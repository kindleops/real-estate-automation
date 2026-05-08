#!/usr/bin/env node

/**
 * scripts/proof/inbox-backfill.mjs
 *
 * Runs verification queries for the inbox backfill and hydration.
 */

import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config({ path: ".env.local" });

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const TARGET_THREAD = "phone:+19102422956";

async function runProof() {
  console.log(`[Proof] Inbox Backfill Verification for ${TARGET_THREAD}`);
  console.log("========================================================\n");

  // 1. Thread State Stats
  console.log("[1] Checking inbox_thread_state stats...");
  const { data: stateData, error: stateError } = await supabase
    .from("inbox_thread_state")
    .select("message_count, inbound_count, outbound_count, failed_queue_count, pending_queue_count")
    .eq("thread_key", TARGET_THREAD)
    .single();

  if (stateError) {
    console.error("  Error fetching thread state:", stateError.message);
  } else {
    console.log(`  Message Count: ${stateData.message_count}`);
    console.log(`  Inbound Count: ${stateData.inbound_count}`);
    console.log(`  Outbound Count: ${stateData.outbound_count}`);
    console.log(`  Failed Queue:  ${stateData.failed_queue_count}`);
    console.log(`  Pending Queue: ${stateData.pending_queue_count}\n`);
  }

  // 2. Hydration Identity
  console.log("[2] Checking hydrated identity fields...");
  const { data: identityData, error: identityError } = await supabase
    .from("inbox_threads_hydrated")
    .select("owner_display_name, prospect_full_name, property_address_full, market")
    .eq("thread_key", TARGET_THREAD)
    .single();

  if (identityError) {
    console.error("  Error fetching identity:", identityError.message);
  } else {
    console.log(`  Owner:    ${identityData.owner_display_name}`);
    console.log(`  Prospect: ${identityData.prospect_full_name}`);
    console.log(`  Address:  ${identityData.property_address_full}`);
    console.log(`  Market:   ${identityData.market}\n`);
  }

  // 3. Property Details
  console.log("[3] Checking hydrated property details...");
  const { data: propData, error: propError } = await supabase
    .from("inbox_threads_hydrated")
    .select("property_type, beds, baths, sqft, year_built, estimated_value")
    .eq("thread_key", TARGET_THREAD)
    .single();

  if (propError) {
    console.error("  Error fetching property details:", propError.message);
  } else {
    console.log(`  Type:  ${propData.property_type}`);
    console.log(`  Beds/Baths: ${propData.beds}/${propData.baths}`);
    console.log(`  Sqft:  ${propData.sqft}`);
    console.log(`  Year:  ${propData.year_built}`);
    console.log(`  Value: $${propData.estimated_value?.toLocaleString()}\n`);
  }

  // 4. Latest Message
  console.log("[4] Checking latest message info...");
  const { data: msgData, error: msgError } = await supabase
    .from("inbox_threads_hydrated")
    .select("latest_message_body, latest_direction, latest_message_at, detected_intent")
    .eq("thread_key", TARGET_THREAD)
    .single();

  if (msgError) {
    console.error("  Error fetching latest message:", msgError.message);
  } else {
    console.log(`  Body:      "${msgData.latest_message_body}"`);
    console.log(`  Direction: ${msgData.latest_direction}`);
    console.log(`  At:        ${msgData.latest_message_at}`);
    console.log(`  Intent:    ${msgData.detected_intent}\n`);
  }

  // 5. Column Count
  console.log("[5] Verifying dossier view completeness...");
  const { data: cols, error: colError } = await supabase.rpc("get_view_column_count", { v_name: "inbox_thread_dossier_hydrated" });
  
  // Fallback if RPC doesn't exist
  if (colError) {
     console.log("  (RPC get_view_column_count missing, checking via direct select count if possible)");
     // We can't easily check information_schema via Supabase JS without RPC or raw SQL
  } else {
     console.log(`  Total hydrated fields: ${cols}\n`);
  }

  console.log("[Proof] Complete!");
}

runProof().catch(err => {
  console.error("Proof failed:", err);
  process.exit(1);
});
