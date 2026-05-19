import * as Feeder from "../src/lib/domain/outbound/supabase-candidate-feeder.js";
import dotenv from "dotenv";
import path from "path";

dotenv.config({ path: path.resolve(process.cwd(), ".env.local") });

async function test() {
  const options = {
    dry_run: true,
    candidate_source: "v_sms_ready_contacts",
    within_contact_window_now: true,
    limit: 25,
    scan_limit: 1000,
    touch_number: 1,
    now: new Date().toISOString()
  };

  try {
    console.log("Starting dry-run...");
    const diagnostics = await Feeder.runSupabaseCandidateFeeder(options);
    
    console.log("\n--- Feeder Results ---");
    console.log("Scanned:", diagnostics.scanned_count);
    console.log("Eligible:", diagnostics.eligible_count);
    console.log("Queued:", diagnostics.queued_count);
    console.log("Skipped:", diagnostics.skipped_count);
    console.log("Duplicate Blocked:", diagnostics.duplicate_queue_block_count);
    
    const ceciliaInSample = diagnostics.sample_created_queue_items.find(item => 
      item.to_phone_number?.includes("7311")
    );
    
    if (ceciliaInSample) {
      console.log("\n❌ FAILURE: Cecilia is STILL in sample_created_queue_items!");
    } else {
      console.log("\n✅ SUCCESS: Cecilia is NOT in sample_created_queue_items.");
    }
  } catch (error) {
    console.error("Feeder Error:", error);
  }
}

test();
