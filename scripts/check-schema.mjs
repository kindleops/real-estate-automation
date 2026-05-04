import { createClient } from '@supabase/supabase-js';
import fs from 'fs';

// Load .env.local
const envContent = fs.readFileSync('.env.local', 'utf8');
const envVars = {};
envContent.split('\n').forEach(line => {
  const match = line.match(/^([^#=]+)=(.*)$/);
  if (match) {
    envVars[match[1].trim()] = match[2].trim().replace(/^["']|["']$/g, '');
  }
});

const supabase = createClient(
  envVars.NEXT_PUBLIC_SUPABASE_URL,
  envVars.SUPABASE_SERVICE_ROLE_KEY
);

console.log('[Schema] Checking message_events table...\n');

// Try to get one row to see structure
const { data, error } = await supabase
  .from('message_events')
  .select('*')
  .limit(1);

if (error) {
  console.error('Error:', error.message);
  process.exit(1);
}

if (data && data.length > 0) {
  console.log('Columns in message_events:');
  Object.keys(data[0]).forEach(col => console.log('  -', col));
  console.log('\nSample metadata:', JSON.stringify(data[0].metadata, null, 2));
} else {
  console.log('No rows found in message_events');
}

process.exit(0);
