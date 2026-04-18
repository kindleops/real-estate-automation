// TextGrid SMS Provider Client
// This is a mock implementation - replace with actual TextGrid API

export interface SMSMessage {
  to: string;
  from: string;
  body: string;
  queueKey: string;
}

export interface SMSResponse {
  success: boolean;
  messageSid?: string;
  error?: string;
}

export class TextGridClient {
  private apiKey: string;
  private apiUrl: string;
  private fromNumber: string;

  constructor() {
    this.apiKey = process.env.TEXTGRID_API_KEY || '';
    this.apiUrl = process.env.TEXTGRID_API_URL || 'https://api.textgrid.com';
    this.fromNumber = process.env.TEXTGRID_FROM_NUMBER || '';

    if (!this.apiKey) {
      console.warn('TEXTGRID_API_KEY not set - using mock mode');
    }
  }

  async sendSMS(message: SMSMessage): Promise<SMSResponse> {
    try {
      // Use provided from number or default
      const fromNumber = message.from || this.fromNumber;

      if (!this.apiKey) {
        // Mock mode for development
        console.log('[MOCK] Sending SMS:', {
          to: message.to,
          from: fromNumber,
          body: message.body,
          queueKey: message.queueKey,
        });

        // Simulate API delay
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Generate deterministic mock SID based on queue_key
        const mockSid = `SM${Buffer.from(message.queueKey).toString('hex').substring(0, 32)}`;

        return {
          success: true,
          messageSid: mockSid,
        };
      }

      // Real TextGrid API call
      const response = await fetch(`${this.apiUrl}/messages`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.apiKey}`,
        },
        body: JSON.stringify({
          to: message.to,
          from: fromNumber,
          body: message.body,
          idempotency_key: message.queueKey,
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`TextGrid API error: ${response.status} - ${errorText}`);
      }

      const data = await response.json();

      return {
        success: true,
        messageSid: data.sid || data.message_sid || data.id,
      };
    } catch (error) {
      console.error('SMS send error:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  // Verify webhook signature (implement based on TextGrid's signature scheme)
  verifyWebhookSignature(payload: string, signature: string): boolean {
    if (!this.apiKey) {
      // In mock mode, accept all webhooks
      return true;
    }

    // Implement TextGrid's webhook signature verification
    // This is a placeholder - replace with actual verification logic
    try {
      const crypto = require('crypto');
      const expectedSignature = crypto
        .createHmac('sha256', this.apiKey)
        .update(payload)
        .digest('hex');

      return crypto.timingSafeEqual(
        Buffer.from(signature),
        Buffer.from(expectedSignature)
      );
    } catch (error) {
      console.error('Webhook signature verification error:', error);
      return false;
    }
  }
}

export const textGridClient = new TextGridClient();
