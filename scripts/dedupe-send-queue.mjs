#!/usr/bin/env node
import { findSendQueueItems, updateSendQueueItem } from "@/lib/podio/apps/send-queue.js";
import { getTextValue, getCategoryValue } from "@/lib/providers/podio.js";
import fs from 'fs';

function ts() { return new Date().toISOString(); }
function log(...args) { console.log(ts(), ...args); }

async function run() {
  log('Starting dedupe pass over Send Queue');
  const limit = 200;
  let offset = 0;
  const qmap = new Map();

  while (true) {
    log('fetching items', 'offset=', offset);
    const response = await findSendQueueItems({}, limit, offset);
    const items = Array.isArray(response)
      ? response
      : Array.isArray(response?.items)
      ? response.items
      : [];

    if (!items || items.length === 0) break;

    for (const item of items) {
      const qid = getTextValue(item, 'queue-id-2', '') || '';
      if (!qid) continue;
      const list = qmap.get(qid) || [];
      list.push(item);
      qmap.set(qid, list);
    }

    offset += items.length;
  }

  const report = { checked_groups: 0, duplicate_groups: 0, cancelled: [], errors: [] };

  for (const [qid, list] of qmap.entries()) {
    report.checked_groups += 1;
    if (list.length < 2) continue;

    report.duplicate_groups += 1;
    const sorted = [...list].sort((a,b)=> Number(a.item_id) - Number(b.item_id));
    const keeper = sorted[0];
    const others = sorted.slice(1);

    log('Duplicate group', qid, 'keeper=', keeper.item_id, 'dups=', others.map(o=>o.item_id));

    for (const other of others) {
      try {
        const status = getCategoryValue(other, 'queue-status', '') || '';
        if (['Sent','Delivered'].includes(String(status))) {
          log('Skipping cancel for sent/delivered item', other.item_id, 'status=', status);
          continue;
        }

        // Pass raw category text (not an object) so schema normalization works
        await updateSendQueueItem(other.item_id, { 'queue-status': 'Cancelled' });
        log('Cancelled duplicate item', other.item_id);
        report.cancelled.push(other.item_id);
      } catch (err) {
        log('Error cancelling', other.item_id, String(err));
        report.errors.push({ item_id: other.item_id, error: String(err) });
      }
    }
  }

  const outPath = '/tmp/dedupe-report.json';
  try { fs.writeFileSync(outPath, JSON.stringify(report, null, 2), 'utf8'); } catch(e) { log('write report failed', e); }
  log('Dedupe pass complete', JSON.stringify(report));
  log('Report at', outPath);
}

run().catch(e=>{ console.error('dedupe script error', e && e.stack || e); process.exitCode = 1; });
