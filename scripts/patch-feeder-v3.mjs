import fs from 'fs';

const filePath = '../real-estate-automation/src/lib/domain/outbound/supabase-candidate-feeder.js';
let content = fs.readFileSync(filePath, 'utf8');

// 1. Fix schedule_spread param logic
content = content.replace(
  'const use_spread = options.schedule_spread && !options.within_contact_window_now;',
  'const use_spread = options.schedule_spread;'
);

// 2. Add batch deduplication inside the loop
const loopReplacement = `
  const seenContacts = new Set();
  for (const candidate of source.rows) {
    if (summary.queued_count >= options.limit) {
      summary.skipped_count += 1;
      continue;
    }

    // Batch deduplication: One per owner+phone per batch
    const contactKey = candidate.master_owner_id + ":" + (candidate.canonical_e164 || candidate.phone_number || candidate.best_phone_id);
    if (seenContacts.has(contactKey)) {
        summary.skipped_count += 1;
        summary.duplicate_queue_block_count += 1;
        summary.sample_skips.push({
            reason_code: REASON_CODES.DUPLICATE_QUEUE_ITEM,
            reason: "batch_duplicate_suppressed",
            master_owner_id: candidate.master_owner_id,
            property_id: candidate.property_id,
        });
        continue;
    }
    seenContacts.add(contactKey);`;

content = content.replace(
  'for (const candidate of source.rows) {',
  loopReplacement
);

fs.writeFileSync(filePath, content);
console.log('Successfully updated feeder with spread fix and batch dedupe.');
