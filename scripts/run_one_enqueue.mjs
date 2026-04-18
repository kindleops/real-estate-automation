#!/usr/bin/env node
import { runFeederWithRollout } from "@/lib/domain/master-owners/feed-master-owners-request.js";

function ts() { return new Date().toISOString(); }
function log(...args) { console.log(ts(), ...args); }

(async function main(){
  try {
    log('Starting single enqueue run: limit=100, scan_limit=300');
    const result = await runFeederWithRollout({ limit: 100, scan_limit: 300, dry_run: false });
    log('Enqueue run completed', JSON.stringify(result, null, 2));
    if (result && result.queued_count !== undefined) {
      log('queued_count=', result.queued_count);
    }
    process.exit(0);
  } catch (err) {
    console.error('enqueue-run-failed', err && (err.stack || err));
    process.exitCode = 1;
  }
})();
