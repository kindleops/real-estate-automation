import crypto from "node:crypto";

import { requireDevRouteAccess } from "@/lib/security/dev-route-guard.js";
import { insertSupabaseSendQueueRow } from "@/lib/supabase/sms-engine.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function clean(value) {
  return String(value ?? "").trim();
}

function getRequestUrl(request_url = "http://localhost/api/dev/send-test") {
  return new URL(request_url);
}

function buildQueueRunUrl(request_url) {
  const url = getRequestUrl(request_url);
  return new URL("/api/internal/queue/run", url.origin).toString();
}

function buildQueueRunHeaders() {
  const cron_secret = clean(process.env.CRON_SECRET);
  const headers = {};

  if (cron_secret) {
    headers.Authorization = `Bearer ${cron_secret}`;
    headers["x-vercel-cron-secret"] = cron_secret;
  }

  return headers;
}

function parsePhoneOverride(value, fallback) {
  const normalized = clean(value).replace(/\s+/g, "");
  return normalized || fallback;
}

async function parseQueueRunResponse(response) {
  const text = await response.text();

  try {
    return JSON.parse(text);
  } catch {
    return {
      ok: response.ok,
      status: response.status,
      raw: text,
    };
  }
}

export async function runDevSendTest({
  request_url = "http://localhost/api/dev/send-test",
  insertSupabaseSendQueueRowImpl = insertSupabaseSendQueueRow,
  fetchImpl = fetch,
} = {}) {
  const request_url_object = getRequestUrl(request_url);
  const to_phone_number = parsePhoneOverride(
    request_url_object.searchParams.get("to"),
    "+16127433952"
  );
  const from_phone_number = parsePhoneOverride(
    request_url_object.searchParams.get("from"),
    "+16128060495"
  );
  const now = new Date().toISOString();
  const queue_key = `dev-send-test-${crypto.randomUUID()}`;
  const message_body = "Test message from Supabase send_queue";

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
    message_body,
    message_text: message_body,
    to_phone_number,
    from_phone_number,
    character_count: message_body.length,
    metadata: {
      source: "dev_send_test",
    },
  });

  const should_run_now =
    request_url_object.searchParams.get("run_now") !== "false";

  const queue_run = should_run_now
    ? await parseQueueRunResponse(
        await fetchImpl(buildQueueRunUrl(request_url), {
          method: "GET",
          headers: buildQueueRunHeaders(),
        })
      )
    : null;

  return {
    ok: inserted?.ok !== false,
    inserted,
    queue_run,
  };
}

export async function handleDevSendTestRequest(request, deps = {}) {
  const denied = requireDevRouteAccess(request);

  if (denied) {
    return denied;
  }

  return Response.json(
    await runDevSendTest({
      request_url: request.url,
      insertSupabaseSendQueueRowImpl:
        deps.insertSupabaseSendQueueRowImpl || insertSupabaseSendQueueRow,
      fetchImpl: deps.fetchImpl || fetch,
    })
  );
}

export async function GET(request) {
  return handleDevSendTestRequest(request);
}

export async function POST(request) {
  return GET(request);
}
