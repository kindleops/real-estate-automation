import { createClient } from '@supabase/supabase-js';

const supabaseUrl = 'https://lcppdrmrdfblstpcbgpf.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxjcHBkcm1yZGZibHN0cGNiZ3BmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQ3Nzk5MywiZXhwIjoyMDkyMDUzOTkzfQ.MVlBohwYFRtrALDePo50U6qFeqa5kIjzEEKLu_bDVGw';

const supabase = createClient(supabaseUrl, supabaseKey);

console.log('[Fixture] Creating test fixture for offer stage AI...\n');

// First, get a real thread_key from message_events
const { data: sampleEvents, error: sampleError } = await supabase
  .from('message_events')
  .select('thread_key')
  .limit(1);

if (sampleError || !sampleEvents?.length) {
  console.error('Failed to get sample thread:', sampleError?.message);
  process.exit(1);
}

const testThreadKey = sampleEvents[0].thread_key;
console.log('Using thread_key:', testThreadKey);

// Create fixture metadata
const fixtureMetadata = {
  test_fixture: true,
  offer_stage_ai_triggered: true,
  offer_stage_ai_result: {
    triggered: true,
    trigger_reason: 'offer_stage_ai_dry_run',
    asset_type: 'single_family',
    recommended_opening_offer: 58500,
    target_contract: 61750,
    walkaway_internal: 65000,
    offer_confidence_score: 1,
    safe_to_reveal_offer: true,
    missing_required_info: [],
    draft_message: "Based on what I'm seeing, I'd probably be around $58,500-$61,750 cash as-is depending on condition/title. Is that close enough to keep talking?",
    send_mode: 'dry_run_offer_ai',
    would_queue: false,
    would_auto_send: false,
    blocked_reason: null,
    action: 'offer_reveal',
    route: null,
    timestamp: new Date().toISOString(),
  }
};

// Insert fixture message event
const { data: inserted, error: insertError } = await supabase
  .from('message_events')
  .insert({
    thread_key: testThreadKey,
    message_body: 'TEST FIXTURE: How much will you pay for my house?',
    direction: 'inbound',
    metadata: fixtureMetadata,
    created_at: new Date().toISOString(),
  })
  .select('id, thread_key, metadata')
  .single();

if (insertError) {
  console.error('Failed to insert fixture:', insertError.message);
  process.exit(1);
}

console.log('\n✓ Fixture created successfully');
console.log('  ID:', inserted.id);
console.log('  Thread Key:', inserted.thread_key);
console.log('  Has offer_stage_ai_result:', Boolean(inserted.metadata?.offer_stage_ai_result));
console.log('\nTHREAD_KEY_FOR_PROOF=' + testThreadKey);
console.log('EVENT_ID_FOR_CLEANUP=' + inserted.id);
