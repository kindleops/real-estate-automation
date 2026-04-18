import { createClient } from '@supabase/supabase-js';

if (!process.env.SUPABASE_URL) {
  throw new Error('Missing SUPABASE_URL environment variable');
}

if (!process.env.SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Missing SUPABASE_SERVICE_ROLE_KEY environment variable');
}

// Use service role key for full database access
export const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
  }
);

// Type definitions
export interface SendQueueJob {
  id: string;
  queue_key: string;
  message_body: string;
  to_phone_number: string;
  from_phone_number: string;
  lock_token: string;
  retry_count: number;
}

export interface MessageEvent {
  id?: string;
  message_event_key: string;
  provider_message_sid?: string;
  direction: 'inbound' | 'outbound';
  event_type: string;
  message_body?: string;
  to_phone_number?: string;
  from_phone_number?: string;
  phone_number_id?: string;
  queue_id?: string;
  metadata?: Record<string, any>;
  sent_at?: string;
  received_at?: string;
  delivered_at?: string;
  failed_at?: string;
  error_message?: string;
}
