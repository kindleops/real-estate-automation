import crypto from "node:crypto";

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { renderEmailTemplate } from "@/lib/email/render-email-template.js";
import { isEmailSuppressed } from "@/lib/email/email-suppression.js";

let _deps = {
  supabase_override: null,
  render_template_override: null,
  is_suppressed_override: null,
};

function getDb() {
  return _deps.supabase_override || defaultSupabase;
}

function getRenderTemplate() {
  return _deps.render_template_override || renderEmailTemplate;
}

function getIsSuppressed() {
  return _deps.is_suppressed_override || isEmailSuppressed;
}

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function buildQueueId() {
  return `emq_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`;
}

export function __setQueueEmailDeps(overrides = {}) {
  _deps = { ..._deps, ...overrides };
}

export function __resetQueueEmailDeps() {
  _deps = {
    supabase_override: null,
    render_template_override: null,
    is_suppressed_override: null,
  };
}

export async function queueEmail({
  owner_id = null,
  property_id = null,
  prospect_id = null,
  email_address,
  template_key,
  context = {},
  campaign_key = null,
  scheduled_for = null,
  dry_run = false,
} = {}) {
  const normalized_email = lower(email_address);
  const normalized_template_key = clean(template_key);

  if (!normalized_email) {
    return { ok: false, queued: false, reason: "missing_email_address" };
  }
  if (!normalized_template_key) {
    return { ok: false, queued: false, reason: "missing_template_key" };
  }

  const suppression = await getIsSuppressed()(normalized_email);
  if (suppression?.suppressed) {
    return {
      ok: true,
      queued: false,
      reason: "email_suppressed",
      suppression: suppression.suppression || null,
    };
  }

  const db = getDb();
  const { data: template, error: template_error } = await db
    .from("email_templates")
    .select("*")
    .eq("template_key", normalized_template_key)
    .eq("is_active", true)
    .maybeSingle();

  if (template_error || !template) {
    return {
      ok: false,
      queued: false,
      reason: "template_not_found",
      error: clean(template_error?.message) || null,
    };
  }

  const rendered = getRenderTemplate()(template, context || {});

  if (Array.isArray(rendered.missing_variables) && rendered.missing_variables.length > 0) {
    return {
      ok: true,
      queued: false,
      reason: "missing_template_variables",
      missing_variables: rendered.missing_variables,
      template_key: normalized_template_key,
    };
  }

  if (!clean(rendered.subject) || !clean(rendered.html_body)) {
    return {
      ok: false,
      queued: false,
      reason: "rendered_template_invalid",
    };
  }

  const queue_row = {
    queue_id: buildQueueId(),
    owner_id,
    property_id,
    prospect_id,
    email_address: normalized_email,
    template_key: normalized_template_key,
    use_case: template.use_case || null,
    stage_code: template.stage_code || null,
    subject: rendered.subject,
    html_body: rendered.html_body,
    text_body: rendered.text_body || null,
    status: "queued",
    scheduled_for: scheduled_for || null,
    campaign_key: clean(campaign_key) || null,
    metadata: {
      language: template.language || "English",
      stage_label: template.stage_label || null,
      context,
    },
    created_at: nowIso(),
    updated_at: nowIso(),
  };

  if (dry_run) {
    return {
      ok: true,
      queued: false,
      reason: "dry_run",
      planned_row: queue_row,
    };
  }

  const { data: inserted, error: insert_error } = await db
    .from("email_send_queue")
    .insert(queue_row)
    .select("*")
    .maybeSingle();

  if (insert_error) {
    return {
      ok: false,
      queued: false,
      reason: "queue_insert_failed",
      error: clean(insert_error?.message) || null,
    };
  }

  return {
    ok: true,
    queued: true,
    queue_row: inserted || queue_row,
  };
}

export default queueEmail;
