import crypto from "node:crypto";

import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import { insertSupabaseSendQueueRow } from "@/lib/supabase/sms-engine.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function runDevSendTest({
  request_url = "http://localhost/api/dev/send-test",
  insertSupabaseSendQueueRowImpl = insertSupabaseSendQueueRow,
  runSendQueueImpl = runSendQueue,
} = {}) {
  const now = new Date().toISOString();
  const queue_key = `dev-send-test-${crypto.randomUUID()}`;

  const inserted = await insertSupabaseSendQueueRowImpl({
    queue_key,
    queue_id: queue_key,
    queue_status: "queued",
    scheduled_for: now,
    scheduled_for_utc: now,
    scheduled_for_local: now,
    timezone: "America/Chicago",
    contact_window: "8:00 AM - 9:00 PM",
    send_priority: 10,
    is_locked: false,
    retry_count: 0,
    max_retries: 3,
    message_body: "🔥 DEV SEND TEST",
    message_text: "🔥 DEV SEND TEST",
    to_phone_number: "+16127433952",
    from_phone_number: "+16128060495",
    character_count: "🔥 DEV SEND TEST".length,
    metadata: {
      source: "dev_send_test",
    },
  });

  const should_run_now =
    new URL(request_url).searchParams.get("run_now") !== "false";

  const run_result = should_run_now
    ? await runSendQueueImpl({
        limit: 50,
      })
    : null;

  return {
    ok: inserted?.ok !== false,
    inserted,
    run_result,
  };
}

export async function GET(request) {
  return Response.json(await runDevSendTest({ request_url: request.url }));
}
