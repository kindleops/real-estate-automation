import crypto from "node:crypto";

import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { sendBrevoTransactionalEmail } from "@/lib/email/brevo-client.js";
import { queueEmail } from "@/lib/email/queue-email.js";
import { supabase } from "@/lib/supabase/client.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.internal.email.send-test" });

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function parseAllowlist(raw = "") {
  return String(raw)
    .split(/[\s,;]+/)
    .map((entry) => lower(entry))
    .filter(Boolean);
}

function isAllowlisted(email = "") {
  const allowlist = parseAllowlist(process.env.EMAIL_TEST_ALLOWLIST || "");
  return allowlist.includes(lower(email));
}

function nowIso() {
  return new Date().toISOString();
}

function queueId() {
  return `etest_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`;
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const email_address = lower(body?.email_address || body?.to);
    const template_key = clean(body?.template_key);
    const context = body?.context && typeof body.context === "object" ? body.context : {};

    if (!email_address || !template_key) {
      return NextResponse.json(
        { ok: false, error: "missing_required_fields", required: ["email_address", "template_key"] },
        { status: 400 }
      );
    }

    if (!isAllowlisted(email_address)) {
      return NextResponse.json(
        { ok: false, error: "email_not_allowlisted_for_send_test" },
        { status: 403 }
      );
    }

    const planned = await queueEmail({
      email_address,
      template_key,
      context,
      campaign_key: clean(body?.campaign_key) || null,
      dry_run: true,
    });

    if (!planned.ok || planned.reason === "missing_template_variables") {
      return NextResponse.json(
        {
          ok: false,
          error: planned.reason || "email_send_test_invalid",
          missing_variables: planned.missing_variables || [],
        },
        { status: 400 }
      );
    }

    if (planned.reason === "email_suppressed") {
      return NextResponse.json(
        { ok: false, error: "email_suppressed" },
        { status: 400 }
      );
    }

    const row = planned.planned_row;

    try {
      const send_result = await sendBrevoTransactionalEmail({
        to: email_address,
        subject: row.subject,
        htmlContent: row.html_body,
        textContent: row.text_body,
        sender: {
          name: clean(process.env.EMAIL_DEFAULT_SENDER_NAME) || "Acquisitions Team",
          email: clean(process.env.EMAIL_DEFAULT_SENDER_EMAIL),
        },
        replyTo: clean(process.env.EMAIL_DEFAULT_REPLY_TO)
          ? { email: clean(process.env.EMAIL_DEFAULT_REPLY_TO) }
          : null,
        tags: ["send_test", clean(template_key)].filter(Boolean),
        params: row.metadata || {},
      });

      const persisted = {
        ...row,
        queue_id: queueId(),
        status: "sent",
        sent_at: nowIso(),
        brevo_message_id: clean(send_result?.message_id) || null,
        created_at: nowIso(),
        updated_at: nowIso(),
      };

      await supabase.from("email_send_queue").insert(persisted);

      return NextResponse.json({
        ok: true,
        route: "internal/email/send-test",
        result: {
          sent: true,
          email_address,
          template_key,
          brevo_message_id: persisted.brevo_message_id,
        },
      });
    } catch (send_error) {
      const failed_row = {
        ...row,
        queue_id: queueId(),
        status: "failed",
        failure_reason: clean(send_error?.code || send_error?.message) || "send_test_failed",
        created_at: nowIso(),
        updated_at: nowIso(),
      };

      await supabase.from("email_send_queue").insert(failed_row);

      return NextResponse.json(
        {
          ok: false,
          error: clean(send_error?.code || send_error?.message) || "send_test_failed",
        },
        { status: 400 }
      );
    }
  } catch (error) {
    logger.error("email.send_test.failed", { error: clean(error?.message) || "unknown" });
    return NextResponse.json({ ok: false, error: "email_send_test_failed" }, { status: 500 });
  }
}
