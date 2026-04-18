import { NextRequest, NextResponse } from 'next/server';
import { supabase, MessageEvent } from '@/lib/supabase';
import { textGridClient } from '@/lib/textgrid';

export const dynamic = 'force-dynamic';

interface InboundWebhookPayload {
  MessageSid: string;
  From: string;
  To: string;
  Body: string;
  AccountSid?: string;
  MessagingServiceSid?: string;
  NumMedia?: string;
  NumSegments?: string;
  SmsStatus?: string;
  ApiVersion?: string;
  [key: string]: any;
}

export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    // Parse incoming webhook payload
    const contentType = request.headers.get('content-type') || '';
    let payload: InboundWebhookPayload;

    if (contentType.includes('application/json')) {
      payload = await request.json();
    } else if (contentType.includes('application/x-www-form-urlencoded')) {
      const formData = await request.formData();
      payload = Object.fromEntries(formData.entries()) as any;
    } else {
      return NextResponse.json(
        { error: 'Unsupported content type' },
        { status: 400 }
      );
    }

    // Verify webhook signature (optional but recommended)
    const signature = request.headers.get('x-textgrid-signature') || '';
    const rawBody = JSON.stringify(payload);

    if (process.env.TEXTGRID_VERIFY_WEBHOOKS === 'true') {
      const isValid = textGridClient.verifyWebhookSignature(rawBody, signature);
      if (!isValid) {
        console.error('Invalid webhook signature');
        return NextResponse.json(
          { error: 'Invalid signature' },
          { status: 401 }
        );
      }
    }

    // Extract required fields
    const messageSid = payload.MessageSid || payload.message_sid || payload.sid;
    const fromPhone = payload.From || payload.from;
    const toPhone = payload.To || payload.to;
    const body = payload.Body || payload.body || '';

    if (!messageSid || !fromPhone || !toPhone) {
      console.error('Missing required webhook fields:', payload);
      return NextResponse.json(
        { error: 'Missing required fields' },
        { status: 400 }
      );
    }

    console.log('Received inbound SMS:', {
      messageSid,
      from: fromPhone,
      to: toPhone,
      bodyLength: body.length,
    });

    // Create deterministic message_event_key from provider SID
    const messageEventKey = `inbound_${messageSid}`;

    // Prepare message event
    const messageEvent: MessageEvent = {
      message_event_key: messageEventKey,
      provider_message_sid: messageSid,
      direction: 'inbound',
      event_type: 'inbound_sms',
      message_body: body,
      from_phone_number: fromPhone,
      to_phone_number: toPhone,
      received_at: new Date().toISOString(),
      metadata: {
        raw_payload: payload,
      },
    };

    // Insert into message_events (idempotent via ON CONFLICT DO NOTHING)
    const { data, error } = await supabase
      .from('message_events')
      .insert(messageEvent)
      .select()
      .single();

    if (error) {
      // Check if it's a duplicate (conflict on message_event_key)
      if (error.code === '23505') {
        console.log(
          `Duplicate inbound message ignored: ${messageEventKey}`
        );
        return NextResponse.json(
          { 
            success: true, 
            duplicate: true,
            message: 'Message already processed' 
          },
          { status: 200 }
        );
      }

      console.error('Error inserting inbound message:', error);
      return NextResponse.json(
        { error: 'Database error', details: error.message },
        { status: 500 }
      );
    }

    console.log('✓ Inbound message logged:', messageEventKey);

    // TODO: Add your business logic here
    // Examples:
    // - Look up contact by phone number
    // - Trigger conversation state update
    // - Queue auto-response
    // - Notify webhook consumers
    // - Update Podio (if needed)

    // Look up phone number to link to contact
    const { data: phoneData } = await supabase
      .from('phone_numbers')
      .select('id')
      .eq('phone_number', fromPhone)
      .single();

    if (phoneData) {
      // Update the message event with phone_number_id
      await supabase
        .from('message_events')
        .update({ phone_number_id: phoneData.id })
        .eq('id', data.id);
    }

    return NextResponse.json(
      {
        success: true,
        messageId: data.id,
        messageEventKey: messageEventKey,
      },
      { status: 200 }
    );
  } catch (error) {
    console.error('Inbound webhook error:', error);
    return NextResponse.json(
      {
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
