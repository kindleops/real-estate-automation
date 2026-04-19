export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const supabase_env_keys = Object.keys(process.env)
    .filter((key) => key.includes("SUPABASE"))
    .sort();

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
      VERCEL_GIT_COMMIT_SHA: !!process.env.VERCEL_GIT_COMMIT_SHA,
      VERCEL_URL: !!process.env.VERCEL_URL,
      VERCEL_TARGET_ENV: !!process.env.VERCEL_TARGET_ENV,
      VERCEL_ENV: !!process.env.VERCEL_ENV,
      NODE_ENV: !!process.env.NODE_ENV,
      SUPABASE_ENV_KEYS: supabase_env_keys,
    },
  });
}
