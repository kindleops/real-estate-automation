#!/usr/bin/env node
/*
  Safer paced auto-enqueue
  - Uses smaller batches and longer pauses to avoid Podio rate-limit cooldowns.
  - Detects Podio cooldown and waits; logs to /tmp/auto-enqueue-paced.log
  - Writes each batch response to /tmp/auto-paced-batch-<n>.json
*/

const { execFileSync } = require('child_process');
const fs = require('fs');

const URL = 'https://real-estate-automation-three.vercel.app/api/internal/outbound/feed-master-owners';
const AUTH = 'Bearer f07dcf24c85c16c729f70651c8a3fc2d4b835976e2a583e4c6552acf30dac128';
const BATCH_LIMIT = 100; // smaller batch
const BATCH_COUNT = 20; // 100 * 20 = 2000
const SCAN_LIMIT = 300;
const PAUSE_SECONDS = 10; // pause between batches
const LOG_FILE = '/tmp/auto-enqueue-paced.log';

function ts() { return new Date().toISOString(); }
function log(...args) { const s = `[${ts()}] ${args.join(' ')}\n`; try { fs.appendFileSync(LOG_FILE, s); } catch(e){}; console.log(...args); }

function curlPost(bodyJson) {
  const body = typeof bodyJson === 'string' ? bodyJson : JSON.stringify(bodyJson);
  const args = ['-s', '-X', 'POST', URL, '-H', `Authorization: ${AUTH}`, '-H', 'Content-Type: application/json', '-d', body];
  try {
    return execFileSync('curl', args, { encoding: 'utf8', maxBuffer: 20 * 1024 * 1024 });
  } catch (err) {
    const stdout = (err.stdout || '').toString();
    const stderr = (err.stderr || '').toString();
    log('curl error:', err.status || err.message, stderr || stdout);
    return stdout || `{"ok":false,"error":"curl_failed","message":"${(err.message||'curl error').replace(/"/g,'')}")}`;
  }
}

async function waitSeconds(s) {
  log(`Waiting ${s}s`);
  await new Promise((res) => setTimeout(res, Math.max(0, Math.floor(s)) * 1000 + 500));
}

function safeParseJson(s) { try { return JSON.parse(s); } catch(e) { return null; } }

async function run() {
  log('Paced auto-enqueue started: batches=', BATCH_COUNT, 'batch_limit=', BATCH_LIMIT);
  // initial dry-run attempt to detect cooldown
  const dry = curlPost({ limit: BATCH_LIMIT, scan_limit: SCAN_LIMIT, dry_run: true });
  fs.writeFileSync('/tmp/auto-paced-dry.json', dry, 'utf8');
  const dryParsed = safeParseJson(dry);
  if (dryParsed && dryParsed.result && dryParsed.result.podio_cooldown && dryParsed.result.podio_cooldown.active) {
    const rem = dryParsed.result.podio_cooldown.retry_after_seconds_remaining || dryParsed.result.retry_after_seconds || dryParsed.result.podio_cooldown.retry_after_seconds || 60;
    log('Initial Podio cooldown active; waiting', rem, 's');
    await waitSeconds(rem + 3);
  }

  for (let i = 1; i <= BATCH_COUNT; i++) {
    log(`Batch ${i}/${BATCH_COUNT} starting`);
    const out = curlPost({ limit: BATCH_LIMIT, scan_limit: SCAN_LIMIT, dry_run: false });
    const file = `/tmp/auto-paced-batch-${i}.json`;
    try { fs.writeFileSync(file, out, 'utf8'); } catch(e) { log('write error', e); }
    const parsed = safeParseJson(out);
    if (parsed && parsed.result && parsed.result.podio_cooldown && parsed.result.podio_cooldown.active) {
      const rem = parsed.result.podio_cooldown.retry_after_seconds_remaining || parsed.result.retry_after_seconds || parsed.result.podio_cooldown.retry_after_seconds || 60;
      log(`Podio cooldown during batch ${i}; waiting ${rem}s and will retry batch ${i}`);
      await waitSeconds(rem + 3);
      i = i - 1; // retry
      continue;
    }
    const queued = parsed && parsed.result ? parsed.result.queued_count || 0 : 0;
    log(`Batch ${i} finished queued_count=${queued}`);
    await waitSeconds(PAUSE_SECONDS);
  }
  log('Paced auto-enqueue finished');
}

run().catch(e => { log('script error', e && e.stack || e); process.exitCode = 1; });
