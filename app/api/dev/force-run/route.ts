import { NextRequest, NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const baseUrl = process.env.VERCEL_URL 
      ? `https://${process.env.VERCEL_URL}` 
      : process.env.BASE_URL || 'http://localhost:3000';

    const res = await fetch(`${baseUrl}/api/queue/run`, {
      headers: {
        'Authorization': `Bearer ${process.env.CRON_SECRET || ''}`
      }
    });

    const data = await res.json();

    return NextResponse.json({ 
      ok: true, 
      data,
      triggered_at: new Date().toISOString() 
    });
  } catch (error) {
    return NextResponse.json({ 
      ok: false, 
      error: error instanceof Error ? error.message : 'Unknown error' 
    }, { status: 500 });
  }
}
