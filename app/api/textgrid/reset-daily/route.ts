import { NextRequest, NextResponse } from 'next/server';
import { supabase } from '@/lib/supabase';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest): Promise<NextResponse> {
  // Verify cron secret (optional but recommended)
  const authHeader = request.headers.get('authorization');
  const cronSecret = process.env.CRON_SECRET;

  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    // Reset daily usage counters for all numbers
    const { data, error } = await supabase.rpc('reset_textgrid_daily_usage');

    if (error) {
      throw new Error(`Failed to reset daily usage: ${error.message}`);
    }

    const resetCount = data || 0;

    console.log(`✓ Daily reset completed: ${resetCount} numbers reset`);

    return NextResponse.json(
      {
        success: true,
        resetCount,
        timestamp: new Date().toISOString(),
        message: `Reset ${resetCount} sending numbers to 0 messages sent`,
      },
      { status: 200 }
    );
  } catch (error) {
    console.error('Daily reset error:', error);

    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

// Allow POST as well for manual triggering
export async function POST(request: NextRequest): Promise<NextResponse> {
  return GET(request);
}
