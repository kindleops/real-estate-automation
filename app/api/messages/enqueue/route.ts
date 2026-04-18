import { NextRequest, NextResponse } from 'next/server';
import { supabase } from '@/lib/supabase';

export const dynamic = 'force-dynamic';

interface EnqueueRequest {
  to_phone_number: string;
  message_body: string;
  from_phone_number?: string;
  scheduled_for?: string;
  send_priority?: number;
  max_retries?: number;
  metadata?: Record<string, any>;
}

export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const body: EnqueueRequest = await request.json();

    // Validate required fields
    if (!body.to_phone_number || !body.message_body) {
      return NextResponse.json(
        { error: 'Missing required fields: to_phone_number, message_body' },
        { status: 400 }
      );
    }

    // Normalize phone number (basic cleanup)
    const toPhone = body.to_phone_number.replace(/[^\d+]/g, '');

    // Generate deterministic queue_key (timestamp + phone + hash)
    const timestamp = Date.now();
    const hash = Buffer.from(
      `${toPhone}${body.message_body}${timestamp}`
    )
      .toString('base64')
      .substring(0, 8);
    const queueKey = `sms_${timestamp}_${toPhone}_${hash}`;

    // Look up or create phone_number record
    let phoneNumberId: string | null = null;

    const { data: existingPhone } = await supabase
      .from('phone_numbers')
      .select('id')
      .eq('phone_number', toPhone)
      .single();

    if (existingPhone) {
      phoneNumberId = existingPhone.id;
    } else {
      const { data: newPhone, error: phoneError } = await supabase
        .from('phone_numbers')
        .insert({ phone_number: toPhone })
        .select('id')
        .single();

      if (phoneError) {
        console.error('Error creating phone number:', phoneError);
      } else {
        phoneNumberId = newPhone.id;
      }
    }

    // Insert into send_queue (from_phone will be selected dynamically at send time)
    const queueData = {
      queue_key: queueKey,
      queue_status: 'queued',
      to_phone_number: toPhone,
      message_body: body.message_body,
      phone_number_id: phoneNumberId,
      scheduled_for: body.scheduled_for || new Date().toISOString(),
      send_priority: body.send_priority || 5,
      max_retries: body.max_retries || 3,
      metadata: body.metadata || {},
    };

    const { data, error } = await supabase
      .from('send_queue')
      .insert(queueData)
      .select()
      .single();

    if (error) {
      // Check for duplicate queue_key
      if (error.code === '23505') {
        return NextResponse.json(
          {
            error: 'Duplicate message',
            message: 'This message is already queued',
            queueKey,
          },
          { status: 409 }
        );
      }

      console.error('Error enqueueing message:', error);
      return NextResponse.json(
        { error: 'Database error', details: error.message },
        { status: 500 }
      );
    }

    console.log('✓ Message enqueued:', queueKey);

    return NextResponse.json(
      {
        success: true,
        queueId: data.id,
        queueKey: data.queue_key,
        scheduledFor: data.scheduled_for,
        status: data.queue_status,
      },
      { status: 201 }
    );
  } catch (error) {
    console.error('Enqueue error:', error);
    return NextResponse.json(
      {
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

// GET endpoint to check queue status
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const { searchParams } = new URL(request.url);
    const queueKey = searchParams.get('queue_key');
    const queueId = searchParams.get('queue_id');

    if (!queueKey && !queueId) {
      return NextResponse.json(
        { error: 'Missing queue_key or queue_id parameter' },
        { status: 400 }
      );
    }

    let query = supabase.from('send_queue').select('*');

    if (queueKey) {
      query = query.eq('queue_key', queueKey);
    } else if (queueId) {
      query = query.eq('id', queueId);
    }

    const { data, error } = await query.single();

    if (error) {
      if (error.code === 'PGRST116') {
        return NextResponse.json(
          { error: 'Queue job not found' },
          { status: 404 }
        );
      }

      console.error('Error fetching queue job:', error);
      return NextResponse.json(
        { error: 'Database error', details: error.message },
        { status: 500 }
      );
    }

    return NextResponse.json(data, { status: 200 });
  } catch (error) {
    console.error('Queue status error:', error);
    return NextResponse.json(
      {
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
