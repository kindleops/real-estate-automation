export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  return Response.json({
    ok: true,
    env: {
      SUPABASE_URL: !!process.env.SUPABASE_URL,
      SUPABASE_SERVICE_ROLE_KEY: !!process.env.SUPABASE_SERVICE_ROLE_KEY,
      NEXT_PUBLIC_SUPABASE_URL: !!process.env.NEXT_PUBLIC_SUPABASE_URL,
      SUPABASE_SERVICE_KEY: !!process.env.SUPABASE_SERVICE_KEY,
      TEXTGRID_ACCOUNT_SID: !!process.env.TEXTGRID_ACCOUNT_SID,
      TEXTGRID_AUTH_TOKEN: !!process.env.TEXTGRID_AUTH_TOKEN,
      CRON_SECRET: !!process.env.CRON_SECRET,
      VERCEL_ENV: process.env.VERCEL_ENV || null,
      NODE_ENV: process.env.NODE_ENV || null,
    },
  });
}
