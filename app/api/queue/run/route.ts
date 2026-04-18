import { NextRequest, NextResponse } from 'next/server';
import { supabase, SendQueueJob, MessageEvent } from '@/lib/supabase';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

interface QueueRunResult {
  success: boolean;
  processed: number;
  sent: number;
  failed: number;
  errors: string[];
  duration: number;
}

async function runQueue(request: NextRequest): Promise<NextResponse> {
  console.log("🔥 QUEUE ROUTE HIT", new Date().toISOString());
  
  console.log("ENV CHECK", {
    has_sid: !!process.env.TEXTGRID_ACCOUNT_SID,
    has_token: !!process.env.TEXTGRID_AUTH_TOKEN,
    has_supabase: !!process.env.SUPABASE_URL
  });

  const startTime = Date.now();
  
  const authHeader = request.headers.get('authorization');
  const cronSecret = process.env.CRON_SECRET;
  
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const result: QueueRunResult = {
    success: true,
    processed: 0,
    sent: 0,
    failed: 0,
    errors: [],
    duration: 0,
  };

  try {
    const { data: unlockResult, error: unlockError } = await supabase.rpc(
      'unlock_stale_jobs',
      { stale_minutes: 10 }
    );

    if (unlockError) {
      console.error('Error unlocking stale jobs:', unlockError);
      result.errors.push(`Unlock error: ${unlockError.message}`);
    } else if (unlockResult && unlockResult > 0) {
      console.log(`Unlocked ${unlockResult} stale jobs`);
    }

    const { data: jobs, error: queueError } = await supabase
      .from('send_queue')
      .select('*')
      .eq('queue_status', 'queued')
      .lte('scheduled_for', new Date().toISOString())
      .order('scheduled_for', { ascending: true })
      .limit(50);

    if (queueError) {
      throw new Error(`Failed to fetch jobs: ${queueError.message}`);
    }

    result.processed = jobs?.length || 0;
    console.log(`ROWS LOADED ${result.processed}`);

    for (const job of jobs || []) {
      try {
        console.log(`PROCESSING ROW ${job.id}`);
        const { data: claimed } = await supabase
          .from('send_queue')
          .update({ queue_status: 'sending' })
          .eq('id', job.id)
          .eq('queue_status', 'queued')
          .select()
          .single();

        if (!claimed) {
          console.log('SKIPPED - ALREADY CLAIMED', job.id);
          continue;
        }

        const { data: numberData, error: numberError } = await supabase.rpc(
          'select_available_number'
        );

        if (numberError || !numberData || numberData.length === 0) {
          throw new Error(
            numberError?.message || 'No available sending numbers - all at daily limit'
          );
        }

        const selectedNumber = numberData[0];
        const fromPhoneNumber = selectedNumber.phone_number;

        const auth = Buffer
          .from(`${process.env.TEXTGRID_ACCOUNT_SID}:${process.env.TEXTGRID_AUTH_TOKEN}`)
          .toString('base64');

        const response = await fetch(
          `https://api.textgrid.com/2010-04-01/Accounts/${process.env.TEXTGRID_ACCOUNT_SID}/Messages.json`,
          {
            method: 'POST',
            headers: {
              'Authorization': `Basic ${auth}`,
              'Content-Type': 'application/json'
            },
            body: JSON.stringify({
              From: fromPhoneNumber,
              To: job.to_phone_number,
              Body: job.message_body
            })
          }
        );

        const raw = await response.text();
        console.log("TEXTGRID RAW RESPONSE", raw);

        let data;
        try {
          data = JSON.parse(raw);
        } catch {
          throw new Error("INVALID JSON RESPONSE");
        }

        if (response.ok && data.sid) {
          console.log(`SEND SUCCESS ${job.id}`);
          const messageEvent: MessageEvent = {
            message_event_key: job.queue_key,
            provider_message_sid: data.sid,
            direction: 'outbound',
            event_type: 'outbound_sms',
            message_body: job.message_body,
            to_phone_number: job.to_phone_number,
            from_phone_number: fromPhoneNumber,
            queue_id: job.id,
            sent_at: new Date().toISOString(),
          };

          await supabase
            .from('message_events')
            .upsert(messageEvent, {
              onConflict: 'message_event_key',
              ignoreDuplicates: false,
            });

          await supabase
            .from('send_queue')
            .update({ queue_status: 'sent' })
            .eq('id', job.id);

          console.log("FINAL STATE UPDATE", {
            id: job.id,
            status: 'sent',
            sid: data.sid || data.message_sid
          });

          result.sent++;
          console.log(`✓ Sent message ${job.queue_key} (SID: ${data.sid})`);
        } else {
          const errorMessage = data.message || raw || 'Unknown send error';
          console.error(`Failed to send ${job.queue_key}:`, errorMessage);

          await supabase
            .from('send_queue')
            .update({ 
              queue_status: 'failed',
              retry_count: (job.retry_count || 0) + 1
            })
            .eq('id', job.id);

          result.failed++;
        }
      } catch (jobError) {
        const errorMessage = jobError instanceof Error ? jobError.message : 'Unknown error';
        console.error(`Error processing job ${job.queue_key}:`, jobError);

        result.errors.push(`Job ${job.queue_key}: ${errorMessage}`);
        result.failed++;

        try {
          await supabase
            .from('send_queue')
            .update({ 
              queue_status: 'failed',
              retry_count: (job.retry_count || 0) + 1
            })
            .eq('id', job.id);
        } catch (failError) {
          console.error('Error marking job as failed:', failError);
        }
      }
    }

    result.success = result.errors.length === 0;
    result.duration = Date.now() - startTime;

    console.log('Queue run completed:', result);

    return NextResponse.json(result, { status: 200 });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    console.error('Queue runner error:', error);

    result.success = false;
    result.errors.push(errorMessage);
    result.duration = Date.now() - startTime;

    return NextResponse.json(result, { status: 500 });
  }
}

export async function GET(request: NextRequest): Promise<NextResponse> {
  return runQueue(request);
}

export async function POST(request: NextRequest): Promise<NextResponse> {
  return runQueue(request);
}
