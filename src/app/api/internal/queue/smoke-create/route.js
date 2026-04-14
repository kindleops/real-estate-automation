import { NextResponse } from "next/server";
import crypto from "node:crypto";

import { child } from "@/lib/logging/logger.js";
import { requireCronAuth } from "@/lib/security/cron-auth.js";
import APP_IDS from "@/lib/config/app-ids.js";
import { createItem } from "@/lib/providers/podio.js";
import { QUEUE_FIELDS } from "@/lib/sms/queue_message.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.queue.smoke_create",
});

/**
 * POST /api/internal/queue/smoke-create
 *
 * Temporary internal diagnostic route.
 * Creates one minimal Send Queue row with queue-status = Queued
 * to verify the full Podio createItem path is healthy.
 *
 * Returns the created item_id and the normalized field payload.
 */
export async function POST(request) {
  const auth = requireCronAuth(request, logger);
  if (!auth.authorized) return auth.response;

  logger.info("smoke_create.started", {
    send_queue_app_id: APP_IDS.send_queue,
  });

  const smoke_id = crypto.randomBytes(8).toString("hex");
  const now = new Date();

  const fields = {
    [QUEUE_FIELDS.queue_id]: `smoke-${smoke_id}`,
    [QUEUE_FIELDS.queue_status]: "Queued",
    [QUEUE_FIELDS.message_text]: `[SMOKE TEST] Queue creation verification — ${smoke_id}`,
    [QUEUE_FIELDS.character_count]: 60,
    [QUEUE_FIELDS.message_type]: "Follow-Up",
    [QUEUE_FIELDS.touch_number]: 0,
    [QUEUE_FIELDS.max_retries]: 0,
    [QUEUE_FIELDS.retry_count]: 0,
    [QUEUE_FIELDS.scheduled_utc]: { start: now.toISOString() },
    [QUEUE_FIELDS.send_priority]: "_ Low",
    [QUEUE_FIELDS.dnc_check]: "✅ Cleared",
    [QUEUE_FIELDS.delivery_confirmed]: "⏳ Pending",
  };

  try {
    const created = await createItem(APP_IDS.send_queue, fields);

    logger.info("smoke_create.success", {
      item_id: created?.item_id || null,
      smoke_id,
      send_queue_app_id: APP_IDS.send_queue,
    });

    return NextResponse.json({
      ok: true,
      item_id: created?.item_id || null,
      smoke_id,
      send_queue_app_id: APP_IDS.send_queue,
      fields,
    });
  } catch (err) {
    logger.warn("smoke_create.failed", {
      smoke_id,
      send_queue_app_id: APP_IDS.send_queue,
      error: err?.message || "unknown",
      error_description: err?.response?.data?.error_description || null,
      field_keys: Object.keys(fields),
    });

    return NextResponse.json(
      {
        ok: false,
        error: "smoke_create_failed",
        smoke_id,
        send_queue_app_id: APP_IDS.send_queue,
        detail: err?.message || "unknown",
        error_description: err?.response?.data?.error_description || null,
        field_keys: Object.keys(fields),
      },
      { status: 500 }
    );
  }
}
