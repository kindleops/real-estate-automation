import { NextRequest, NextResponse } from 'next/server';
import { supabase } from '@/lib/supabase';
import { textGridClient } from '@/lib/textgrid';

export const dynamic = 'force-dynamic';

interface DeliveryWebhookPayload {
  MessageSid: string;
  MessageStatus: string;
  ErrorCode?: string;
  ErrorMessage?: string;
  To?: string;
  From?: string;
  [key: string]: any;
}

export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    // Parse incoming webhook payload
    const contentType = request.headers.get('content-type') || '';
    let payload: DeliveryWebhookPayload;

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
    const messageStatus = payload.MessageStatus || payload.status || payload.message_status;

    if (!messageSid) {
      console.error('Missing MessageSid in delivery webhook:', payload);
      return NextResponse.json(
        { error: 'Missing MessageSid' },
        { status: 400 }
      );
    }

    console.log('Received delivery status:', {
      messageSid,
      status: messageStatus,
    });

    // Determine which timestamp field to update based on status
    const updateData: any = {
      metadata: {
        delivery_status: messageStatus,
        delivery_payload: payload,
      },
    };

    // Map status to timestamp fields
    // Common statuses: sent, delivered, failed, undelivered
    if (messageStatus === 'delivered') {
      updateData.delivered_at = new Date().toISOString();
      updateData.event_type = 'delivered';
    } else if (messageStatus === 'failed' || messageStatus === 'undelivered') {
      updateData.failed_at = new Date().toISOString();
      updateData.event_type = 'failed';
      updateData.error_message = payload.ErrorMessage || payload.error_message || 'Delivery failed';
    } else if (messageStatus === 'sent') {
      // Already set by queue runner, but update if needed
      if (!updateData.sent_at) {
        updateData.sent_at = new Date().toISOString();
      }
    }

    // Update message_events by provider_message_sid
    const { data, error } = await supabase
      .from('message_events')
      .update(updateData)
      .eq('provider_message_sid', messageSid)
      .select()
      .single();

    if (error) {
      // Message might not exist yet (race condition) or already updated
      if (error.code === 'PGRST116') {
        console.warn(
          `No message found with SID ${messageSid} - might not be sent yet`
        );
        return NextResponse.json(
          {
            success: true,
            warning: 'Message not found',
            messageSid,
          },
          { status: 200 }
        );
      }

      console.error('Error updating delivery status:', error);
      return NextResponse.json(
        { error: 'Database error', details: error.message },
        { status: 500 }
      );
    }

    console.log(
      `✓ Updated delivery status for ${messageSid}: ${messageStatus}`
    );

    // Also update send_queue status if message was failed
    if (messageStatus === 'failed' || messageStatus === 'undelivered') {
      if (data.queue_id) {
        await supabase
          .from('send_queue')
          .update({
            queue_status: 'failed',
            metadata: {
              delivery_error: payload.ErrorMessage || payload.error_message,
            },
          })
          .eq('id', data.queue_id);

        console.log(`✓ Marked queue job as failed: ${data.queue_id}`);
      }
    } else if (messageStatus === 'delivered') {
      if (data.queue_id) {
        await supabase
          .from('send_queue')
          .update({
            queue_status: 'delivered',
          })
          .eq('id', data.queue_id);

        console.log(`✓ Marked queue job as delivered: ${data.queue_id}`);
      }
    }

    return NextResponse.json(
      {
        success: true,
        messageId: data.id,
        status: messageStatus,
      },
      { status: 200 }
    );
  } catch (error) {
    console.error('Delivery webhook error:', error);
    return NextResponse.json(
      {
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
