import runMasterOwnerOutboundFeeder from "../lib/domain/master-owners/run-master-owner-outbound-feeder.js";
import { info, error } from "../lib/logging/logger.js";
import {
  DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME,
} from "../lib/config/rollout-controls.js";

/**
 * run-daily-outbound-sweep.js
 * 
 * Automates the daily outbound queueing process by iterating through 
 * canonical Podio views and executing the SMS feeder logic.
 */

const CANONICAL_SWEEP_VIEWS = [
  DEFAULT_LIVE_FEEDER_SOURCE_VIEW_NAME, // "SMS / TIER #1 / ALL"
  "SMS / TIER #2 / FOLLOW UPS",
  "SMS / TIER #3 / RE-ENGAGEMENT",
];

async function runDailySweep({ dry_run = false, limit = 100, scan_limit = 500 } = {}) {
  const now = new Date().toISOString();
  info("daily_sweep.started", { now, dry_run, views: CANONICAL_SWEEP_VIEWS });

  const summary = {
    started_at: now,
    dry_run,
    views_processed: [],
    total_queued: 0,
    total_scanned: 0,
    errors: [],
  };

  for (const view_name of CANONICAL_SWEEP_VIEWS) {
    try {
      info("daily_sweep.processing_view", { view_name });
      
      const result = await runMasterOwnerOutboundFeeder({
        source_view_name: view_name,
        dry_run,
        limit,
        scan_limit,
        now,
      });

      summary.views_processed.push({
        view_name,
        ok: result.ok,
        queued_count: result.queued_count || 0,
        scanned_count: result.scanned_count || 0,
        reason: result.reason || null,
      });

      if (result.ok) {
        summary.total_queued += (result.queued_count || 0);
        summary.total_scanned += (result.scanned_count || 0);
      } else {
        info("daily_sweep.view_skipped_or_failed", { view_name, reason: result.reason });
      }

    } catch (err) {
      error("daily_sweep.view_error", { view_name, error: err.message });
      summary.errors.push({ view_name, error: err.message });
      summary.views_processed.push({
        view_name,
        ok: false,
        error: err.message,
      });
    }
  }

  summary.completed_at = new Date().toISOString();
  info("daily_sweep.completed", summary);

  return summary;
}

// Execution entry point
const isMain = import.meta.url.endsWith(process.argv[1]);
if (isMain) {
  const dry_run = process.argv.includes("--dry-run");
  const limit = parseInt(process.argv.find(arg => arg.startsWith("--limit="))?.split("=")[1] || "100");
  const scan_limit = parseInt(process.argv.find(arg => arg.startsWith("--scan-limit="))?.split("=")[1] || "500");

  runDailySweep({ dry_run, limit, scan_limit })
    .then((summary) => {
      console.log("Daily Sweep Summary:", JSON.stringify(summary, null, 2));
      process.exit(0);
    })
    .catch((err) => {
      console.error("Daily Sweep Failed:", err);
      process.exit(1);
    });
}

export { runDailySweep };
export default runDailySweep;
