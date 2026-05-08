#!/usr/bin/env node

/**
 * scripts/backfill-inbox-thread-state.mjs
 *
 * For every thread_key in message_events:
 * - compute summary stats
 * - apply business logic for stage/status/priority
 * - upsert into public.inbox_thread_state
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

const DRY_RUN = process.env.DRY_RUN === "true";
const LIMIT = parseInt(process.env.LIMIT || "5000");
const BATCH_SIZE = 200;

function lower(val) {
  return String(val ?? "").toLowerCase();
}

function includesAny(text, phrases = []) {
  const t = lower(text);
  return phrases.some(p => t.includes(lower(p)));
}

async function main() {
  console.log(`Starting Thread State Backfill (Dry Run: ${DRY_RUN})`);

  // 1. Get all unique thread_keys
  const { data: threadKeys, error: keyError } = await supabase
    .from("message_events")
    .select("thread_key")
    .not("thread_key", "is", null);

  if (keyError) {
    console.error("Error fetching thread keys:", keyError);
    return;
  }

  const uniqueKeys = [...new Set(threadKeys.map(k => k.thread_key))].slice(0, LIMIT);
  console.log(`Found ${uniqueKeys.length} unique thread keys to process.`);

  let totalProcessed = 0;
  let totalUpserted = 0;
  let totalErrors = 0;

  for (let i = 0; i < uniqueKeys.length; i += BATCH_SIZE) {
    const batchKeys = uniqueKeys.slice(i, i + BATCH_SIZE);
    const upsertBatch = [];

    for (const thread_key of batchKeys) {
      totalProcessed++;

      // Fetch messages for this thread
      const { data: messages, error: msgError } = await supabase
        .from("message_events")
        .select("*")
        .eq("thread_key", thread_key)
        .order("created_at", { ascending: false });

      if (msgError) {
        console.error(`Error fetching messages for ${thread_key}:`, msgError);
        totalErrors++;
        continue;
      }

      if (messages.length === 0) continue;

      // Fetch queue items for this thread
      const { data: queueItems, error: qError } = await supabase
        .from("send_queue")
        .select("queue_status")
        .eq("thread_key", thread_key);
      
      const qStats = {
        pending: 0,
        failed: 0,
        blocked: 0
      };
      if (queueItems) {
        queueItems.forEach(q => {
          if (q.queue_status === "queued") qStats.pending++;
          if (q.queue_status === "failed") qStats.failed++;
          if (q.queue_status === "blocked") qStats.blocked++;
        });
      }

      const latest = messages[0];
      const inbound = messages.filter(m => m.direction === "inbound");
      const outbound = messages.filter(m => m.direction === "outbound");
      
      const lastInbound = inbound[0];
      const lastOutbound = outbound[0];

      // Logic for status/stage
      let is_suppressed = false;
      let status = "unread";
      let stage = "needs_response";
      let priority = "normal";

      const stopPhrases = ["stop", "remove", "unsubscribe", "wrong number", "legal", "harassment", "quit"];
      const hotPhrases = ["price", "offer", "yes", "interested", "how much", "asking"];

      const allText = messages.map(m => m.message_body).join(" ");
      if (includesAny(allText, stopPhrases)) {
        is_suppressed = true;
        stage = "suppressed";
        status = "archived";
      } else if (includesAny(allText, hotPhrases)) {
        priority = "high";
        stage = "hot_leads";
      }

      if (!is_suppressed) {
        if (latest.direction === "inbound") {
          stage = "needs_response";
          status = "unread";
        } else {
          stage = "waiting";
          status = "read";
        }

        if (qStats.failed > 0) {
          stage = "failed_automation";
        } else if (qStats.pending > 0) {
          stage = "automated";
        }
      }

      const stateRow = {
        thread_key,
        seller_phone: latest.direction === "inbound" ? latest.from_phone_number : latest.to_phone_number,
        our_number: latest.direction === "inbound" ? latest.to_phone_number : latest.from_phone_number,
        master_owner_id: latest.master_owner_id,
        prospect_id: latest.prospect_id,
        property_id: latest.property_id,
        market: latest.market,
        message_count: messages.length,
        inbound_count: inbound.length,
        outbound_count: outbound.length,
        latest_message_event_id: latest.id,
        latest_message_body: latest.message_body,
        latest_message_at: latest.created_at,
        latest_direction: latest.direction,
        latest_event_type: latest.event_type,
        latest_delivery_status: latest.delivery_status,
        last_inbound_at: lastInbound?.created_at || null,
        last_outbound_at: lastOutbound?.created_at || null,
        pending_queue_count: qStats.pending,
        failed_queue_count: qStats.failed,
        blocked_queue_count: qStats.blocked,
        last_intent: latest.detected_intent,
        stage,
        status,
        priority,
        is_suppressed,
        updated_at: new Date().toISOString()
      };

      upsertBatch.push(stateRow);
    }

    if (!DRY_RUN && upsertBatch.length > 0) {
      const { error: upsertError } = await supabase
        .from("inbox_thread_state")
        .upsert(upsertBatch, { onConflict: "thread_key" });
      
      if (upsertError) {
        console.error("Upsert error:", upsertError);
        totalErrors += upsertBatch.length;
      } else {
        totalUpserted += upsertBatch.length;
      }
    } else {
      totalUpserted += upsertBatch.length;
    }

    console.log(`Processed ${totalProcessed} / ${uniqueKeys.length} threads...`);
  }

  console.log(`Thread State Summary: Processed=${totalProcessed}, Upserted=${totalUpserted}, Errors=${totalErrors}`);
  console.log("Backfill Complete.");
}

main().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
