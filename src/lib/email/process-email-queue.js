import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { sendBrevoTransactionalEmail } from "@/lib/email/brevo-client.js";
import { isEmailSuppressed } from "@/lib/email/email-suppression.js";

let _deps = {
  supabase_override: null,
  send_brevo_override: null,
  is_suppressed_override: null,
  now_iso_override: null,
};

function getDb() {
  return _deps.supabase_override || defaultSupabase;
}

function getSendBrevo() {
  return _deps.send_brevo_override || sendBrevoTransactionalEmail;
}

function getIsSuppressed() {
  return _deps.is_suppressed_override || isEmailSuppressed;
}

function nowIso() {
  return _deps.now_iso_override ? _deps.now_iso_override() : new Date().toISOString();
}

function clean(value) {
  return String(value ?? "").trim();
}

function asLimit(value, fallback = 25) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.min(Math.trunc(parsed), 200);
}

function isDue(row = {}, now_ts = Date.now()) {
  if (!row?.scheduled_for) return true;
  const ts = new Date(row.scheduled_for).getTime();
  return Number.isFinite(ts) ? ts <= now_ts : true;
}

async function resolveSenderIdentity(db, row = {}) {
  const brand_key = clean(row?.metadata?.brand_key);
  if (brand_key) {
    const { data } = await db
      .from("email_identities")
      .select("sender_name, sender_email, reply_to_email")
      .eq("brand_key", brand_key)
      .eq("is_active", true)
      .maybeSingle();

    if (data?.sender_email) {
      return {
        sender: {
          name: clean(data.sender_name) || "Acquisitions Team",
          email: clean(data.sender_email),
        },
        replyTo: clean(data.reply_to_email) ? { email: clean(data.reply_to_email) } : null,
      };
    }
  }

  return {
    sender: {
      name: clean(process.env.EMAIL_DEFAULT_SENDER_NAME) || "Acquisitions Team",
      email: clean(process.env.EMAIL_DEFAULT_SENDER_EMAIL),
    },
    replyTo: clean(process.env.EMAIL_DEFAULT_REPLY_TO)
      ? { email: clean(process.env.EMAIL_DEFAULT_REPLY_TO) }
      : null,
  };
}

export function __setProcessEmailQueueDeps(overrides = {}) {
  _deps = { ..._deps, ...overrides };
}

export function __resetProcessEmailQueueDeps() {
  _deps = {
    supabase_override: null,
    send_brevo_override: null,
    is_suppressed_override: null,
    now_iso_override: null,
  };
}

export async function processEmailQueue({ limit = 25, dry_run = false } = {}) {
  const db = getDb();
  const final_limit = asLimit(limit, 25);

  const { data, error } = await db
    .from("email_send_queue")
    .select("*")
    .eq("status", "queued")
    .order("created_at", { ascending: true })
    .limit(final_limit * 3);

  if (error) {
    return {
      ok: false,
      reason: "email_queue_query_failed",
      error: clean(error?.message) || null,
    };
  }

  const rows = (data || []).filter((row) => isDue(row)).slice(0, final_limit);

  const result = {
    ok: true,
    dry_run: Boolean(dry_run),
    attempted_count: rows.length,
    sent_count: 0,
    failed_count: 0,
    skipped_count: 0,
    results: [],
  };

  if (dry_run) {
    result.results = rows.map((row) => ({
      queue_id: row.queue_id,
      status: "planned",
      email_address: row.email_address,
      template_key: row.template_key,
    }));
    return result;
  }

  for (const row of rows) {
    const normalized_email = clean(row.email_address).toLowerCase();

    const suppression = await getIsSuppressed()(normalized_email);
    if (suppression?.suppressed) {
      await db
        .from("email_send_queue")
        .update({
          status: "failed",
          failure_reason: "email_suppressed",
          updated_at: nowIso(),
        })
        .eq("id", row.id);

      result.failed_count += 1;
      result.results.push({
        queue_id: row.queue_id,
        status: "failed",
        reason: "email_suppressed",
      });
      continue;
    }

    try {
      const identity = await resolveSenderIdentity(db, row);
      if (!clean(identity?.sender?.email)) {
        throw Object.assign(new Error("sender_identity_missing"), {
          code: "sender_identity_missing",
          retryable: false,
        });
      }

      const send_result = await getSendBrevo()({
        to: normalized_email,
        subject: row.subject,
        htmlContent: row.html_body,
        textContent: row.text_body,
        sender: identity.sender,
        replyTo: identity.replyTo,
        tags: [
          clean(row.template_key),
          clean(row.use_case),
          clean(row.campaign_key),
        ].filter(Boolean),
        params: row.metadata && typeof row.metadata === "object" ? row.metadata : {},
      });

      await db
        .from("email_send_queue")
        .update({
          status: "sent",
          sent_at: nowIso(),
          brevo_message_id: clean(send_result?.message_id) || null,
          failure_reason: null,
          updated_at: nowIso(),
        })
        .eq("id", row.id);

      result.sent_count += 1;
      result.results.push({
        queue_id: row.queue_id,
        status: "sent",
        brevo_message_id: clean(send_result?.message_id) || null,
      });
    } catch (error_send) {
      await db
        .from("email_send_queue")
        .update({
          status: "failed",
          failure_reason: clean(error_send?.code || error_send?.message) || "email_send_failed",
          updated_at: nowIso(),
        })
        .eq("id", row.id);

      result.failed_count += 1;
      result.results.push({
        queue_id: row.queue_id,
        status: "failed",
        reason: clean(error_send?.code || error_send?.message) || "email_send_failed",
      });
    }
  }

  return result;
}

export default processEmailQueue;
