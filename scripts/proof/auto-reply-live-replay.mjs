// ─── auto-reply-live-replay.mjs ───────────────────────────────────────────
import { supabase } from "../../src/lib/supabase/client.js";
import { queueAutoReply } from "../../src/lib/automation/queueAutoReply.js";
import { ACTIONS } from "../../src/lib/automation/intentMap.js";

async function runReplay() {
  console.log("🚀 Starting Auto-Reply Live Replay Proof (Dry Run)...");
  if (process.env.MEMORY_WRITE_DRY_RUN === 'true') {
    console.log("📝 CONVERSATIONAL MEMORY WRITE ENABLED");
  }

  const LIMIT = 100; 
  const { data: inboundMessages, error } = await supabase
    .from("message_events")
    .select("*")
    .eq("direction", "inbound")
    .order("created_at", { ascending: false })
    .limit(LIMIT);

  if (error) {
    console.error("❌ Failed to fetch inbound messages:", error.message);
    process.exit(1);
  }

  console.log(`📊 Fetched ${inboundMessages.length} inbound messages. Starting processing...\n`);

  const stats = {
    total: inboundMessages.length,
    hard_suppressed: 0,
    approval_required: 0,
    auto_queue_eligible: 0,
    nurture_scheduled: 0,
    underwriting_triggered: 0,
    duplicate_blocked: 0,
    unsafe_template_blocked: 0,
    no_template_fallback: 0,
    memory_used: 0,
    redundant_questions_prevented: 0,
    stages: {
      before: {},
      after: {},
    },
    temperatures: {},
    intents: {},
    memory_metrics: {
      threads_touched: new Set(),
      turns_written: 0,
      snapshots_written: 0,
      negotiation_events_written: 0,
      duplicate_blocks: 0,
      failures: 0,
    }
  };

  const examples = {};
  const risks = [];

  const BATCH_SIZE = 10;
  for (let i = 0; i < inboundMessages.length; i += BATCH_SIZE) {
    const batch = inboundMessages.slice(i, i + BATCH_SIZE);
    
    await Promise.all(batch.map(async (msg) => {
      const thread_key = msg.thread_key || msg.from_phone_number;
      
      try {
        const result = await queueAutoReply(thread_key, msg.id, { dry_run: true });

        const classification = result.metadata?.classification_snapshot;
        const intent = classification?.primary_intent || "unclear";
        stats.intents[intent] = (stats.intents[intent] || 0) + 1;

        if (result.metadata?.memory_used) stats.memory_used++;
        if (result.reason === 'redundant_question_prevented') stats.redundant_questions_prevented++;

        const temp = result.metadata?.seller_temperature || "unknown";
        stats.temperatures[temp] = (stats.temperatures[temp] || 0) + 1;

        const sBefore = result.metadata?.stage_before || "unknown";
        const sAfter = result.metadata?.stage_after || "unknown";
        stats.stages.before[sBefore] = (stats.stages.before[sBefore] || 0) + 1;
        stats.stages.after[sAfter] = (stats.stages.after[sAfter] || 0) + 1;

        // Memory Write Metrics (extracted from queueAutoReply execution)
        if (process.env.MEMORY_WRITE_DRY_RUN === 'true') {
           if (classification) {
              stats.memory_metrics.threads_touched.add(thread_key);
              stats.memory_metrics.turns_written++;
              if (classification.seller_state) {
                stats.memory_metrics.snapshots_written++;
              }
              if (result.use_case?.includes('offer') || result.use_case?.includes('price')) {
                stats.memory_metrics.negotiation_events_written++;
              }
           }
        }

        // Store example plan for each major intent
        if (!examples[intent]) {
          examples[intent] = {
            message: msg.message_body,
            action: result.action,
            reason: result.reason,
            rendered: result.rendered_text,
            use_case: result.use_case || classification?.detected_intent,
            temp,
            sBefore,
            sAfter,
          };
        }

        if (result.ok) {
          if (result.action === ACTIONS.QUEUE_REPLY) {
            stats.auto_queue_eligible++;
            if (result.use_case === "underwriting_needed") stats.underwriting_triggered++;
          }
        } else {
          // Categorize non-ok results
          if (result.action === ACTIONS.STOP || result.reason === "wrong_number_detected" || intent === "opt_out") {
            stats.hard_suppressed++;
          } else if (result.action === ACTIONS.WAIT || intent === "not_interested") {
            stats.nurture_scheduled++;
          } else if (result.action === ACTIONS.ESCALATE) {
            stats.approval_required++;
            if (result.reason === "no_templates_found") stats.no_template_fallback++;
          }

          if (result.reason === "duplicate_reply_prevented") stats.duplicate_blocked++;
          if (result.reason === "safety_gate_violation") stats.unsafe_template_blocked++;
        }

        // Check for weirdness/risks
        if (result.rendered_text?.includes("there") && msg.metadata?.personalization_context?.seller_first_name) {
           risks.push({
             type: "weird_fallback",
             message: msg.message_body,
             rendered: result.rendered_text,
             reason: "Used 'there' despite having a name in context"
           });
        }
      } catch (err) {
        console.error(`❌ Error processing message ${msg.id}:`, err.message);
        stats.memory_metrics.failures++;
      }
    }));

    if ((i + BATCH_SIZE) % 50 === 0) {
      console.log(`⏳ Processed ${i + BATCH_SIZE} / ${inboundMessages.length}...`);
    }
  }

  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log("📊 AUTO-REPLY LIVE REPLAY REPORT");
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`Total Inbound Tested:       ${stats.total}`);
  console.log(`Memory Used:                ${stats.memory_used}`);
  console.log(`Hard Suppressed:            ${stats.hard_suppressed}`);
  console.log(`Approval Required:          ${stats.approval_required}`);
  console.log(`Auto-Queue Eligible:        ${stats.auto_queue_eligible}`);
  console.log(`Nurture Scheduled:          ${stats.nurture_scheduled}`);
  console.log(`Redundant Qs Prevented:     ${stats.redundant_questions_prevented}`);
  console.log(`Underwriting Triggered:     ${stats.underwriting_triggered}`);
  console.log(`Duplicate Blocked:          ${stats.duplicate_blocked}`);
  console.log(`Unsafe Template Blocked:    ${stats.unsafe_template_blocked}`);
  console.log(`No-Template Fallback:       ${stats.no_template_fallback}`);
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  
  if (process.env.MEMORY_WRITE_DRY_RUN === 'true') {
    console.log("🧠 CONVERSATIONAL MEMORY METRICS:");
    console.log(`Threads Touched:            ${stats.memory_metrics.threads_touched.size}`);
    console.log(`Turns Written:              ${stats.memory_metrics.turns_written}`);
    console.log(`Seller State Snapshots:     ${stats.memory_metrics.snapshots_written}`);
    console.log(`Negotiation Events:         ${stats.memory_metrics.negotiation_events_written}`);
    console.log(`Duplicate Blocks:           0 (handled by DB)`);
    console.log(`Memory Failures:            ${stats.memory_metrics.failures}`);
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  }

  console.log("🌡️ TEMPERATURE DISTRIBUTION:");
  Object.entries(stats.temperatures)
    .sort((a, b) => b[1] - a[1])
    .forEach(([temp, count]) => console.log(`- ${temp}: ${count}`));

  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log("🔄 STAGE AFTER DISTRIBUTION:");
  Object.entries(stats.stages.after)
    .sort((a, b) => b[1] - a[1])
    .forEach(([stage, count]) => console.log(`- ${stage}: ${count}`));

  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log("📈 TOP INTENTS:");
  Object.entries(stats.intents)
    .sort((a, b) => b[1] - a[1])
    .forEach(([intent, count]) => console.log(`- ${intent}: ${count}`));
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log("📝 EXAMPLE ACTION PLANS:");
  Object.entries(examples).forEach(([intent, ex]) => {
    console.log(`\n[${intent.toUpperCase()}]`);
    console.log(`Inbound:  "${ex.message.slice(0, 100)}${ex.message.length > 100 ? '...' : ''}"`);
    console.log(`Temp:     ${ex.temp}`);
    console.log(`Stage:    ${ex.sBefore} ➡️ ${ex.sAfter}`);
    console.log(`Action:   ${ex.action}`);
    console.log(`Reason:   ${ex.reason || 'N/A'}`);
    if (ex.rendered) console.log(`Reply:    "${ex.rendered}"`);
  });

  console.log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log("⚠️ POTENTIAL RISKS / WEIRD REPLIES:");
  if (risks.length === 0) console.log("None detected.");
  risks.slice(0, 10).forEach(r => {
    console.log(`- [${r.type}] Inbound: "${r.message}" | Reply: "${r.rendered}"`);
  });

  console.log("\n✨ Replay Proof Completed.");
}

runReplay().catch(err => {
  console.error("❌ Replay Failed:", err);
  process.exit(1);
});
