import { createClient } from '@supabase/supabase-js';

// Load from .env.local or use defaults
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || 'https://zkdjxiqbtvqrcvjguehz.supabase.co';
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '';

const supabase = createClient(supabaseUrl, supabaseKey);

console.log('[Proof] Checking for existing offer stage AI metadata...\n');

try {
  const { data: events, error } = await supabase
    .from('message_events')
    .select('id, thread_key, message_body, metadata, direction, created_at')
    .not('metadata->offer_stage_ai_triggered', 'is', null)
    .order('created_at', { ascending: false })
    .limit(5);

  if (error) {
    console.error('Database Error:', error.message);
    process.exit(1);
  }

  console.log(`Found ${events?.length || 0} events with offer_stage_ai_triggered`);
  
  if (events?.length > 0) {
    console.log('\nSample event:');
    console.log('  ID:', events[0].id);
    console.log('  Thread Key:', events[0].thread_key);
    console.log('  Triggered:', events[0].metadata?.offer_stage_ai_triggered);
    console.log('  Has Result:', Boolean(events[0].metadata?.offer_stage_ai_result));
    
    if (events[0].metadata?.offer_stage_ai_result) {
      const result = events[0].metadata.offer_stage_ai_result;
      console.log('\n  Result Summary:');
      console.log('    - send_mode:', result.send_mode);
      console.log('    - would_queue:', result.would_queue);
      console.log('    - would_auto_send:', result.would_auto_send);
      console.log('    - has walkaway_internal:', Boolean(result.walkaway_internal));
      
      if (result.draft_message && result.walkaway_internal) {
        const walkawayStr = result.walkaway_internal.toString();
        const inDraft = result.draft_message.includes(walkawayStr);
        console.log('    - walkaway in draft:', inDraft, '(should be false)');
      }
    }
    
    // Output thread_key for use in proof script
    console.log('\nTHREAD_KEY_FOR_PROOF=', events[0].thread_key);
  } else {
    console.log('\nNo existing metadata found. Need to create fixture.');
    console.log('THREAD_KEY_FOR_PROOF=NEED_FIXTURE');
  }
} catch (err) {
  console.error('Unexpected error:', err.message);
  process.exit(1);
}

process.exit(0);
