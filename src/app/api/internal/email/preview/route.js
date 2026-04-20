import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";
import { supabase } from "@/lib/supabase/client.js";
import { renderEmailTemplate } from "@/lib/email/render-email-template.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({ module: "api.internal.email.preview" });

function clean(value) {
  return String(value ?? "").trim();
}

async function fetchTemplate(template_key = "") {
  const { data, error } = await supabase
    .from("email_templates")
    .select("*")
    .eq("template_key", clean(template_key))
    .eq("is_active", true)
    .maybeSingle();

  if (error || !data) return null;
  return data;
}

async function handlePreview(payload = {}) {
  const template_key = clean(payload.template_key);
  const context = payload.context && typeof payload.context === "object" ? payload.context : {};

  if (!template_key) {
    return { ok: false, error: "missing_template_key", status: 400 };
  }

  const template = await fetchTemplate(template_key);
  if (!template) {
    return { ok: false, error: "template_not_found", status: 404 };
  }

  const rendered = renderEmailTemplate(template, context);

  return {
    ok: true,
    route: "internal/email/preview",
    preview: {
      template_key,
      use_case: template.use_case,
      stage_code: template.stage_code,
      language: template.language,
      subject: rendered.subject,
      html_body: rendered.html_body,
      text_body: rendered.text_body,
      missing_variables: rendered.missing_variables,
      would_send: false,
    },
  };
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const result = await handlePreview(body);
    return NextResponse.json(result, { status: result.status || (result.ok ? 200 : 400) });
  } catch (error) {
    logger.error("email.preview.failed", { error: clean(error?.message) || "unknown" });
    return NextResponse.json({ ok: false, error: "email_preview_failed" }, { status: 500 });
  }
}

export async function GET(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const template_key = clean(searchParams.get("template_key"));
    const result = await handlePreview({ template_key, context: {} });

    return NextResponse.json(result, { status: result.status || (result.ok ? 200 : 400) });
  } catch (error) {
    logger.error("email.preview.failed", { error: clean(error?.message) || "unknown" });
    return NextResponse.json({ ok: false, error: "email_preview_failed" }, { status: 500 });
  }
}
