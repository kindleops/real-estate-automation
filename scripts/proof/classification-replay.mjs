
import { classify } from "../../src/lib/domain/classification/classify.js";
import { supabase } from "../../src/lib/supabase/client.js";

async function runReplay() {
  console.log("Starting Live Traffic Replay Audit...");
  
  // Pull inbound messages from the last 30 days
  const { data: events, error } = await supabase
    .from('message_events')
    .select('message_body, detected_intent, classification_confidence, id')
    .eq('direction', 'inbound')
    .order('created_at', { ascending: false })
    .limit(1000);

  if (error) {
    console.error("Error fetching messages:", error);
    return;
  }

  console.log(`Auditing ${events.length} messages...\n`);

  let metrics = {
    total: events.length,
    old_unclear: 0,
    new_unclear: 0,
    improved: 0, // Unclear -> Meaningful
    regressed: 0, // Meaningful -> Unclear
    intent_changed: 0,
    confidence_gain: 0,
    safety_check_passed: 0,
    opt_out_preserved: 0,
    opt_out_newly_caught: 0,
    unresolved_unclear: []
  };

  for (const event of events) {
    const old_intent = event.detected_intent;
    const old_conf = event.classification_confidence ?? 0;
    
    if (old_intent === 'unclear') metrics.old_unclear++;

    const result = await classify(event.message_body);
    const new_intent = result.primary_intent;
    const new_conf = result.confidence;

    if (new_intent === 'unclear') {
      metrics.new_unclear++;
      metrics.unresolved_unclear.push(event.message_body);
    }

    if (old_intent === 'unclear' && new_intent !== 'unclear') {
      metrics.improved++;
    }

    if (old_intent !== 'unclear' && new_intent === 'unclear') {
      metrics.regressed++;
    }

    if (old_intent !== new_intent) {
      metrics.intent_changed++;
    }

    metrics.confidence_gain += (new_conf - old_conf);

    // Safety Audit: Opt-outs must never be lost
    if (old_intent === 'opt_out') {
      if (new_intent === 'opt_out') {
        metrics.opt_out_preserved++;
      } else {
        console.warn(`⚠️ SAFETY ALERT: Opt-out LOST for message: "${event.message_body}"`);
        console.warn(`   Old: ${old_intent}, New: ${new_intent}`);
      }
    }

    if (old_intent !== 'opt_out' && new_intent === 'opt_out') {
      metrics.opt_out_newly_caught++;
    }
  }

  const avg_conf_gain = metrics.confidence_gain / metrics.total;

  console.log("--------------------------------------------------");
  console.log("REPLAY AUDIT RESULTS");
  console.log("--------------------------------------------------");
  console.log(`Total Messages:       ${metrics.total}`);
  console.log(`Old 'unclear' count:  ${metrics.old_unclear}`);
  console.log(`New 'unclear' count:  ${metrics.new_unclear}`);
  console.log(`Reduction:            ${((metrics.old_unclear - metrics.new_unclear) / metrics.old_unclear * 100).toFixed(2)}%`);
  console.log(`Improved:             ${metrics.improved} (Unclear -> Meaningful)`);
  console.log(`Regressed:            ${metrics.regressed} (Meaningful -> Unclear)`);
  console.log(`Intent Changes:       ${metrics.intent_changed}`);
  console.log(`Avg Confidence Delta: ${avg_conf_gain.toFixed(4)}`);
  console.log(`Opt-outs Preserved:   ${metrics.opt_out_preserved}`);
  console.log(`Newly caught Opt-outs: ${metrics.opt_out_newly_caught}`);
  
  console.log("\nTop 10 Unresolved Unclear Patterns:");
  metrics.unresolved_unclear.slice(0, 10).forEach(msg => console.log(`- "${msg}"`));
}

runReplay();
