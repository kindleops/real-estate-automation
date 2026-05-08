#!/usr/bin/env node

/**
 * scripts/backfill-inbox-messages.mjs
 *
 * Backfills public.message_events from:
 * - public.send_queue (outbound)
 * - existing inbound message_events (normalization & classification)
 *
 * GOAL: Make Supabase fully authoritative for all inbox messages.
 */

import { createClient } from "@supabase/supabase-js";
import { classify } from "../src/lib/domain/classification/classify.js";
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
const BATCH_SIZE = 500;

function clean(val) {
  return String(val ?? "").trim();
}

function normalizePhone(phone) {
  if (!phone) return "";
  const digits = phone.replace(/\D/g, "");
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  return phone.startsWith("+") ? phone : `+${digits}`;
}

function getThreadKey(phone) {
  const normalized = normalizePhone(phone);
  return normalized ? `phone:${normalized}` : null;
}

async function backfillOutbound() {
  console.log("--- Backfilling Outbound Messages from send_queue ---");
  
  let totalProcessed = 0;
  let totalInserted = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  const { data: queueItems, error } = await supabase
    .from("send_queue")
    .select("*")
    .order("created_at", { ascending: true })
    .limit(LIMIT);

  if (error) {
    console.error("Error fetching send_queue:", error);
    return;
  }

  console.log(`Found ${queueItems.length} queue items to process.`);

  for (let i = 0; i < queueItems.length; i += BATCH_SIZE) {
    const batch = queueItems.slice(i, i + BATCH_SIZE);
    
    for (const item of batch) {
      totalProcessed++;
      
      const provider_sid = item.provider_message_id || item.textgrid_message_id;
      const thread_key = item.thread_key || getThreadKey(item.to_phone_number);
      
      if (!thread_key) {
        totalSkipped++;
        continue;
      }

      const eventPayload = {
        direction: "outbound",
        event_type: "outbound_sms",
        thread_key,
        message_body: item.message_text || item.message_body,
        from_phone_number: item.from_phone_number,
        to_phone_number: item.to_phone_number,
        provider_message_sid: provider_sid,
        delivery_status: item.delivery_confirmed === "true" ? "delivered" : (item.sent_at ? "sent" : (item.failed_reason ? "failed" : "queued")),
        sent_at: item.sent_at,
        delivered_at: item.delivered_at,
        failed_at: item.failed_reason ? item.updated_at : null,
        queue_id: item.id,
        template_id: item.template_id,
        current_stage: item.current_stage,
        detected_intent: item.detected_intent,
        safety_status: item.safety_status,
        priority: item.priority,
        risk: item.risk,
        routing_allowed: item.routing_allowed,
        master_owner_id: item.master_owner_id,
        prospect_id: item.prospect_id,
        property_id: item.property_id,
        phone_number_id: item.phone_number_id,
        market_id: item.market_id,
        message_event_key: `backfill:queue:${item.id}`,
        metadata: {
          ...item.metadata,
          queue_snapshot: item
        },
        updated_at: new Date().toISOString()
      };

      if (DRY_RUN) {
        totalInserted++;
        continue;
      }

      // Check for existing message_event
      // Priority 1: provider_message_sid
      // Priority 2: queue_id
      // Priority 3: thread_key + message_text + created_at window (1 minute)
      
      let existing = null;
      if (provider_sid) {
        const { data } = await supabase.from("message_events").select("id").eq("provider_message_sid", provider_sid).maybeSingle();
        existing = data;
      }
      
      if (!existing && item.id) {
        const { data } = await supabase.from("message_events").select("id").eq("queue_id", item.id).maybeSingle();
        existing = data;
      }

      if (existing) {
        const { error: updateError } = await supabase
          .from("message_events")
          .update(eventPayload)
          .eq("id", existing.id);
        
        if (updateError) {
          totalErrors++;
          console.error(`Error updating message_event ${existing.id}:`, updateError);
        } else {
          totalUpdated++;
        }
      } else {
        const { error: insertError } = await supabase
          .from("message_events")
          .insert(eventPayload);
        
        if (insertError) {
          totalErrors++;
          console.error(`Error inserting message_event for queue ${item.id}:`, insertError);
        } else {
          totalInserted++;
        }
      }
    }
    console.log(`Processed ${totalProcessed} outbound items...`);
  }

  console.log(`Outbound Summary: Processed=${totalProcessed}, Inserted=${totalInserted}, Updated=${totalUpdated}, Skipped=${totalSkipped}, Errors=${totalErrors}`);
}

async function backfillInbound() {
  console.log("--- Backfilling Inbound Messages ---");
  
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalErrors = 0;

  const { data: inboundItems, error } = await supabase
    .from("message_events")
    .select("*")
    .eq("direction", "inbound")
    .order("created_at", { ascending: false })
    .limit(LIMIT);

  if (error) {
    console.error("Error fetching inbound message_events:", error);
    return;
  }

  console.log(`Found ${inboundItems.length} inbound items to process.`);

  for (let i = 0; i < inboundItems.length; i += BATCH_SIZE) {
    const batch = inboundItems.slice(i, i + BATCH_SIZE);
    
    for (const item of batch) {
      totalProcessed++;
      
      let needsUpdate = false;
      const updatePayload = {};

      // 1. Normalize thread_key
      if (!item.thread_key && item.from_phone_number) {
        updatePayload.thread_key = getThreadKey(item.from_phone_number);
        if (updatePayload.thread_key) needsUpdate = true;
      }

      // 2. Backfill classification (intent, language, safety)
      if (!item.detected_intent || !item.language) {
        try {
          const body = clean(item.message_body);
          const classification = await classify(body);
          
          if (classification) {
            let intent = classification.detected_intent;

            // OVERRIDE: Safe intent backfill rules
            const lowerBody = body.toLowerCase();
            
            // Seller Interest
            if (lowerBody.includes("yes and i want to sell") || lowerBody.includes("i'm interested in selling")) {
                intent = "seller_interested";
            }

            // Price Detection (asking_price_provided)
            // Matches: 110$, $110, 110k, $110k, 150000, $150,000 etc.
            const priceRegex = /(?:\$?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s?[kK]?|\d+\$)/;
            // Ensure the message is mostly just the price or the price is a significant part
            if (priceRegex.test(lowerBody) && body.length < 20) {
                intent = "asking_price_provided";
            }

            if (!item.detected_intent || item.detected_intent === 'unclear') {
              updatePayload.detected_intent = intent;
            }
            
            if (!item.language) updatePayload.language = classification.language;
            if (!item.classification_confidence) updatePayload.classification_confidence = classification.confidence;
            
            if (item.safety_status === "pending" || !item.safety_status) {
              updatePayload.safety_status = classification.compliance_flag === "stop_texting" ? "suppressed" : "safe";
            }
            needsUpdate = true;
          }
        } catch (e) {
          console.error(`Classification failed for message ${item.id}:`, e.message);
        }
      }

      if (needsUpdate) {
        if (DRY_RUN) {
          totalUpdated++;
          continue;
        }

        const { error: updateError } = await supabase
          .from("message_events")
          .update(updatePayload)
          .eq("id", item.id);
        
        if (updateError) {
          totalErrors++;
          console.error(`Error updating inbound message ${item.id}:`, updateError);
        } else {
          totalUpdated++;
        }
      }
    }
    console.log(`Processed ${totalProcessed} inbound items...`);
  }

  console.log(`Inbound Summary: Processed=${totalProcessed}, Updated=${totalUpdated}, Errors=${totalErrors}`);
}

async function main() {
  console.log(`Starting Inbox Backfill (Dry Run: ${DRY_RUN})`);
  
  await backfillOutbound();
  await backfillInbound();
  
  console.log("Backfill Complete.");
}

main().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
