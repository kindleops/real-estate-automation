/**
 * SMS System Test Suite
 * 
 * Run with: node test-system.js
 * Requires: .env.local with SUPABASE credentials
 */

require('dotenv').config({ path: '.env.local' });
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

async function testDatabaseConnection() {
  console.log('\n🔍 Testing database connection...');
  
  const { data, error } = await supabase
    .from('send_queue')
    .select('count')
    .limit(1);
  
  if (error) {
    console.error('❌ Database connection failed:', error.message);
    return false;
  }
  
  console.log('✅ Database connected successfully');
  return true;
}

async function testEnqueueMessage() {
  console.log('\n📤 Testing message enqueue...');
  
  const queueKey = `test_${Date.now()}_${Math.random().toString(36).substring(7)}`;
  
  const { data, error } = await supabase
    .from('send_queue')
    .insert({
      queue_key: queueKey,
      to_phone_number: '+15551234567',
      from_phone_number: '+15559876543',
      message_body: 'Test message from automated test suite',
      queue_status: 'queued',
      send_priority: 10,
      max_retries: 3,
      scheduled_for: new Date().toISOString(),
    })
    .select()
    .single();
  
  if (error) {
    console.error('❌ Enqueue failed:', error.message);
    return null;
  }
  
  console.log('✅ Message enqueued:', queueKey);
  
  // Force queue run
  const baseUrl = process.env.BASE_URL || 'http://localhost:3000';
  try {
    await fetch(`${baseUrl}/api/dev/force-run`);
    console.log('✅ Triggered queue runner');
  } catch (e) {
    console.log('⚠️  Could not trigger queue runner:', e.message);
  }
  
  return data;
}

async function testClaimJobs() {
  console.log('\n🔒 Testing atomic job claiming...');
  
  const lockToken = crypto.randomUUID();
  
  const { data, error } = await supabase.rpc('claim_queue_jobs', {
    batch_size: 5,
    lock_token_param: lockToken,
  });
  
  if (error) {
    console.error('❌ Job claiming failed:', error.message);
    return [];
  }
  
  console.log(`✅ Claimed ${data.length} jobs with lock_token: ${lockToken}`);
  return data;
}

async function testMarkJobSent(jobId, lockToken) {
  console.log('\n✓ Testing mark job as sent...');
  
  const { data, error } = await supabase.rpc('mark_job_sent', {
    job_id: jobId,
    lock_token_param: lockToken,
    provider_sid: 'test_sid_' + Date.now(),
  });
  
  if (error) {
    console.error('❌ Mark sent failed:', error.message);
    return false;
  }
  
  console.log('✅ Job marked as sent:', data);
  return data;
}

async function testMessageEventLog() {
  console.log('\n📝 Testing message event logging...');
  
  const eventKey = `test_event_${Date.now()}`;
  
  const { data, error } = await supabase
    .from('message_events')
    .insert({
      message_event_key: eventKey,
      provider_message_sid: 'test_sid_' + Date.now(),
      direction: 'outbound',
      event_type: 'outbound_sms',
      message_body: 'Test message',
      to_phone_number: '+15551234567',
      from_phone_number: '+15559876543',
      sent_at: new Date().toISOString(),
    })
    .select()
    .single();
  
  if (error) {
    console.error('❌ Event logging failed:', error.message);
    return null;
  }
  
  console.log('✅ Event logged:', eventKey);
  return data;
}

async function testIdempotency() {
  console.log('\n🔄 Testing idempotency...');
  
  const queueKey = `idempotency_test_${Date.now()}`;
  
  // Try to insert same queue_key twice
  const insert1 = await supabase
    .from('send_queue')
    .insert({
      queue_key: queueKey,
      to_phone_number: '+15551234567',
      message_body: 'Idempotency test',
    })
    .select()
    .single();
  
  const insert2 = await supabase
    .from('send_queue')
    .insert({
      queue_key: queueKey,
      to_phone_number: '+15551234567',
      message_body: 'Idempotency test duplicate',
    })
    .select()
    .single();
  
  if (!insert1.error && insert2.error && insert2.error.code === '23505') {
    console.log('✅ Idempotency working: duplicate rejected');
    return true;
  }
  
  console.error('❌ Idempotency failed');
  return false;
}

async function testUnlockStaleJobs() {
  console.log('\n🔓 Testing unlock stale jobs...');
  
  const { data, error } = await supabase.rpc('unlock_stale_jobs', {
    stale_minutes: 10,
  });
  
  if (error) {
    console.error('❌ Unlock stale jobs failed:', error.message);
    return 0;
  }
  
  console.log(`✅ Unlocked ${data} stale jobs`);
  return data;
}

async function testQueryPerformance() {
  console.log('\n⚡ Testing query performance...');
  
  const queries = [
    { 
      name: 'Queue status counts',
      fn: () => supabase
        .from('send_queue')
        .select('queue_status')
    },
    {
      name: 'Recent messages',
      fn: () => supabase
        .from('message_events')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(10)
    },
    {
      name: 'Queued messages ready to send',
      fn: () => supabase
        .from('send_queue')
        .select('*')
        .eq('queue_status', 'queued')
        .lte('scheduled_for', new Date().toISOString())
        .limit(10)
    },
  ];
  
  for (const query of queries) {
    const start = Date.now();
    const { data, error } = await query.fn();
    const duration = Date.now() - start;
    
    if (error) {
      console.log(`  ❌ ${query.name}: ${error.message}`);
    } else {
      console.log(`  ✅ ${query.name}: ${duration}ms (${data?.length || 0} rows)`);
    }
  }
}

async function cleanup() {
  console.log('\n🧹 Cleaning up test data...');
  
  // Delete test messages
  await supabase
    .from('send_queue')
    .delete()
    .like('queue_key', 'test_%');
  
  await supabase
    .from('message_events')
    .delete()
    .like('message_event_key', 'test_%');
  
  console.log('✅ Cleanup complete');
}

async function runAllTests() {
  console.log('╔════════════════════════════════════════╗');
  console.log('║   SMS AUTOMATION SYSTEM TEST SUITE    ║');
  console.log('╚════════════════════════════════════════╝');
  
  try {
    // Test 1: Database connection
    const connected = await testDatabaseConnection();
    if (!connected) {
      console.error('\n❌ Cannot proceed without database connection');
      process.exit(1);
    }
    
    // Test 2: Enqueue message
    const enqueuedMessage = await testEnqueueMessage();
    
    // Test 3: Claim jobs
    const claimedJobs = await testClaimJobs();
    
    // Test 4: Mark job as sent (if jobs were claimed)
    if (claimedJobs.length > 0) {
      await testMarkJobSent(claimedJobs[0].id, claimedJobs[0].lock_token);
    }
    
    // Test 5: Message event logging
    await testMessageEventLog();
    
    // Test 6: Idempotency
    await testIdempotency();
    
    // Test 7: Unlock stale jobs
    await testUnlockStaleJobs();
    
    // Test 8: Query performance
    await testQueryPerformance();
    
    // Cleanup
    await cleanup();
    
    console.log('\n╔════════════════════════════════════════╗');
    console.log('║          ALL TESTS COMPLETED           ║');
    console.log('╚════════════════════════════════════════╝\n');
    
  } catch (error) {
    console.error('\n❌ Test suite error:', error);
    process.exit(1);
  }
}

// Run tests
runAllTests();
