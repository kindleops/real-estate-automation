#!/usr/bin/env node
/*
  Auto-enqueue script
  - Performs a dry-run to detect Podio cooldown.
  - Waits until cooldown expires, then sends 10 batches of 200 (2000 total).
  - On Podio cooldown during batches it will wait and retry the same batch.
  - Logs progress to /tmp/auto-enqueue.log and writes each batch response to /tmp/auto-batch-<n>.json
*/

const { execFileSync } = require('child_process');
const fs = require('fs');

const URL = 'https://real-estate-automation-three.vercel.app/api/internal/outbound/feed-master-owners';
const AUTH = 'Bearer f07dcf24c85c16c729f70651c8a3fc2d4b835976e2a583e4c6552acf30dac128';
const BATCH_LIMIT = 200;
const BATCH_COUNT = 10;
const SCAN_LIMIT = 500;
const LOG_FILE = '/tmp/auto-enqueue.log';

function ts() { return new Date().toISOString(); }
function log(...args) { const s = `[${ts()}] ${args.join(' ')}\n`; fs.appendFileSync(LOG_FILE, s); console.log(...args); }

function curlPost(bodyJson) {
  const body = typeof bodyJson === 'string' ? bodyJson : JSON.stringify(bodyJson);
  const args = ['-s', '-X', 'POST', URL, '-H', `Authorization: ${AUTH}`, '-H', 'Content-Type: application/json', '-d', body];
  try {
    const out = execFileSync('curl', args, { encoding: 'utf8', maxBuffer: 20 * 1024 * 1024 });
    return out;
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

function safeParseJson(s) {
  try { return JSON.parse(s); } catch (e) { return null; }
}

async function checkAndWaitForCooldown() {
  const dryBody = { limit: BATCH_LIMIT, scan_limit: SCAN_LIMIT, dry_run: true };
  log('Performing initial dry-run to detect Podio cooldown');
  const out = curlPost(dryBody);
  fs.writeFileSync('/tmp/auto-dryrun.json', out, 'utf8');
  const parsed = safeParseJson(out);
  if (parsed && parsed.result && parsed.result.podio_cooldown && parsed.result.podio_cooldown.active) {
    const rem = parsed.result.podio_cooldown.retry_after_seconds_remaining || parsed.result.retry_after_seconds || parsed.result.podio_cooldown.retry_after_seconds || 60;
    log('Detected active Podio cooldown, retry_after_seconds_remaining=', rem);
    await waitSeconds(rem + 3);
    return true;
  }
  log('No Podio cooldown detected; proceeding');
  return false;
}

async function runBatches() {
  for (let i = 1; i <= BATCH_COUNT; i++) {
    log(`Starting batch ${i}/${BATCH_COUNT}`);
    const body = { limit: BATCH_LIMIT, scan_limit: SCAN_LIMIT, dry_run: false };
    const out = curlPost(body);
    const file = `/tmp/auto-batch-${i}.json`;
    try { fs.writeFileSync(file, out, 'utf8'); } catch(e) { log('Error writing batch file', e); }
    const parsed = safeParseJson(out);
    if (parsed && parsed.result && parsed.result.podio_cooldown && parsed.result.podio_cooldown.active) {
      const rem = parsed.result.podio_cooldown.retry_after_seconds_remaining || parsed.result.retry_after_seconds || parsed.result.podio_cooldown.retry_after_seconds || 60;
      log(`Podio cooldown hit during batch ${i}; waiting ${rem}s before retrying batch ${i}`);
      fs.appendFileSync(LOG_FILE, `[${ts()}] podio cooldown during batch ${i}, waiting ${rem}s\n`);
      await waitSeconds(rem + 3);
      i = i - 1; // retry same batch
      continue;
    }
    const queued = parsed && parsed.result ? parsed.result.queued_count || 0 : 0;
    log(`Batch ${i} completed, queued_count=${queued}`);
    fs.appendFileSync(LOG_FILE, `[${ts()}] batch ${i} queued_count=${queued}\n`);
    await waitSeconds(2);
  }
}

async function main() {
  log('Auto-enqueue script started');
  try {
    await checkAndWaitForCooldown();
    await runBatches();
    log('All batches attempted');
  } catch (err) {
    log('Script error', err && err.stack ? err.stack : err);
    process.exitCode = 1;
  }
}

main();
