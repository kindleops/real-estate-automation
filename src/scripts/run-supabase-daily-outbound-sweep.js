import { runSupabaseOutboundFeeder } from "../lib/domain/outbound/run-supabase-outbound-feeder.js";
import { info, error } from "../lib/logging/logger.js";

/**
 * run-supabase-daily-outbound-sweep.js
 * 
 * Automates the daily outbound queueing process using exclusively Supabase tables
 * and views for candidate discovery. Completely bypasses Podio views.
 */

async function runDailySweep({ dry_run = true, limit = 100, scan_limit = 500 } = {}) {
  const now = new Date().toISOString();
  info("supabase_daily_sweep.started", { now, dry_run, outbound_source: "supabase" });

  const summary = {
    started_at: now,
    dry_run,
    outbound_source: "supabase",
    total_queued: 0,
    total_scanned: 0,
    errors: [],
    details: {}
  };

  try {
    const result = await runSupabaseOutboundFeeder({
      dry_run,
      limit,
      scan_limit,
      now,
    });

    summary.details = result;

    if (result.ok) {
      summary.total_queued += (result.queued_count || 0);
      summary.total_scanned += (result.scanned_count || 0);
    } else {
      info("supabase_daily_sweep.failed", { errors: result.errors });
    }

  } catch (err) {
    error("supabase_daily_sweep.error", { error: err.message });
    summary.errors.push(err.message);
  }

  summary.completed_at = new Date().toISOString();
  info("supabase_daily_sweep.completed", summary);

  return summary;
}

// Execution entry point
const isMain = import.meta.url.endsWith(process.argv[1]);
if (isMain) {
  // Default to dry_run=true unless --live is explicitly passed
  const isLive = process.argv.includes("--live");
  const dry_run = !isLive;
  
  const limitArg = process.argv.find(arg => arg.startsWith("--limit="));
  const limit = limitArg ? parseInt(limitArg.split("=")[1]) : 100;

  const scanLimitArg = process.argv.find(arg => arg.startsWith("--scan-limit="));
  const scan_limit = scanLimitArg ? parseInt(scanLimitArg.split("=")[1]) : 500;

  runDailySweep({ dry_run, limit, scan_limit })
    .then((summary) => {
      console.log("Supabase Daily Sweep Summary:", JSON.stringify(summary, null, 2));
      process.exit(summary.errors.length > 0 ? 1 : 0);
    })
    .catch((err) => {
      console.error("Supabase Daily Sweep Fatal Error:", err);
      process.exit(1);
    });
}

export { runDailySweep };
export default runDailySweep;
