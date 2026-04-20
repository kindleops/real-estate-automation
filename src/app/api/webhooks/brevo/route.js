import crypto from "node:crypto";

import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { supabase } from "@/lib/supabase/client.js";
import { suppressEmail } from "@/lib/email/email-suppression.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.webhooks.brevo" });

const SUPPRESSION_EVENTS = new Set([
  "hard_bounce",
  "spam",
  "unsubscribed",
  "invalid_email",
  "blocked",
]);

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function eventTypeOf(payload = {}) {
  return lower(payload.event || payload.event_type || payload.type);
}

function messageIdOf(payload = {}) {
  return clean(
    payload["message-id"] || payload.messageId || payload.message_id || payload.brevo_message_id
  ) || null;
}

function emailOf(payload = {}) {
  return lower(payload.email || payload.recipient || payload.to) || null;
}

function subjectOf(payload = {}) {
  return clean(payload.subject) || null;
}

function templateKeyOf(payload = {}) {
  return clean(payload.template_key || payload.tag || payload.tags?.[0]) || null;
}

function campaignKeyOf(payload = {}) {
  return clean(payload.campaign_key) || null;
}

function eventKeyOf(payload = {}, index = 0) {
  const explicit = clean(payload.event_key || payload.id || payload.uuid);
  if (explicit) return explicit;

  const hash = crypto
    .createHash("sha256")
    .update(JSON.stringify(payload), "utf8")
    .digest("hex")
    .slice(0, 20);

  return `brevo_${index}_${hash}`;
}

async function upsertEvent(event_row) {
  return supabase
    .from("email_events")
    .upsert(event_row, { onConflict: "event_key" });
}

async function updateQueueStatusByEvent({ brevo_message_id, event_type } = {}) {
  if (!brevo_message_id) return;

  let next_status = null;
  if (event_type === "delivered") next_status = "delivered";
  if (event_type === "opened") next_status = "opened";
  if (event_type === "clicked") next_status = "clicked";
  if (event_type === "soft_bounce") next_status = "failed";
  if (event_type === "hard_bounce") next_status = "failed";
  if (event_type === "spam") next_status = "failed";
  if (event_type === "blocked") next_status = "failed";
  if (event_type === "invalid_email") next_status = "failed";
  if (event_type === "unsubscribed") next_status = "failed";

  if (!next_status) return;

  const update_payload = {
    status: next_status,
    updated_at: new Date().toISOString(),
  };

  if (next_status === "failed") {
    update_payload.failure_reason = event_type;
  }

  await supabase
    .from("email_send_queue")
    .update(update_payload)
    .eq("brevo_message_id", brevo_message_id);
}

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "webhooks/brevo",
    status: "listening",
  });
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => null);
    const events = Array.isArray(body) ? body : body ? [body] : [];

    const results = [];

    for (let i = 0; i < events.length; i += 1) {
      const payload = events[i] || {};
      const event_type = eventTypeOf(payload);
      const brevo_message_id = messageIdOf(payload);
      const email_address = emailOf(payload);
      const event_key = eventKeyOf(payload, i);

      const event_row = {
        event_key,
        brevo_message_id,
        email_address,
        event_type: event_type || "unknown",
        subject: subjectOf(payload),
        template_key: templateKeyOf(payload),
        campaign_key: campaignKeyOf(payload),
        raw_payload: payload,
        created_at: new Date().toISOString(),
      };

      const { error } = await upsertEvent(event_row);
      if (error) {
        logger.warn("brevo.webhook.event_upsert_failed", {
          event_key,
          event_type: event_type || "unknown",
        });
      }

      await updateQueueStatusByEvent({ brevo_message_id, event_type });

      if (SUPPRESSION_EVENTS.has(event_type) && email_address) {
        await suppressEmail({
          email: email_address,
          reason: event_type,
          source: "brevo_webhook",
          raw_payload: payload,
        });
      }

      results.push({
        event_key,
        event_type: event_type || "unknown",
        brevo_message_id,
      });
    }

    return NextResponse.json({
      ok: true,
      route: "webhooks/brevo",
      events_received: events.length,
      results,
    });
  } catch (error) {
    logger.error("brevo.webhook.failed", { error: clean(error?.message) || "unknown" });
    return NextResponse.json(
      { ok: false, error: "brevo_webhook_failed" },
      { status: 500 }
    );
  }
}
