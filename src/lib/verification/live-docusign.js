import {
  createEnvelope,
  getEnvelope,
  sendEnvelope,
} from "@/lib/providers/docusign.js";

function clean(value) {
  return String(value ?? "").trim();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function normalizeAction(value = "") {
  const raw = clean(value).toLowerCase();
  if (["status", "create_send", "create_only"].includes(raw)) return raw;
  return "status";
}

export async function runLiveDocusignVerification({
  action = "status",
  envelope_id = null,
  subject = "",
  template_id = null,
  documents = [],
  signers = [],
  email_blurb = "",
  metadata = {},
  dry_run = true,
  fetch_status_after_send = true,
  confirm_live = false,
} = {}) {
  const normalized_action = normalizeAction(action);
  const normalized_documents = safeArray(documents).slice(0, 2);
  const normalized_signers = safeArray(signers).slice(0, 2);
  const live_requested = !dry_run;

  if (safeArray(documents).length > 2) {
    return {
      ok: false,
      reason: "documents_limit_exceeded",
      max_documents: 2,
    };
  }

  if (safeArray(signers).length > 2) {
    return {
      ok: false,
      reason: "signers_limit_exceeded",
      max_signers: 2,
    };
  }

  if (live_requested && !confirm_live) {
    return {
      ok: false,
      reason: "confirm_live_required",
      action: normalized_action,
      live_requested: true,
    };
  }

  if (normalized_action === "status") {
    return getEnvelope({
      envelope_id,
      dry_run,
    });
  }

  const created = await createEnvelope({
    subject,
    template_id: clean(template_id) || null,
    documents: normalized_documents,
    signers: normalized_signers,
    email_blurb,
    metadata,
    dry_run,
  });

  if (!created.ok || normalized_action === "create_only" || dry_run) {
    return {
      ok: created.ok,
      action: normalized_action,
      created,
      sent: null,
      status: null,
    };
  }

  const sent = await sendEnvelope({
    envelope_id: created.envelope_id,
    dry_run: false,
  });

  const status =
    fetch_status_after_send && sent?.ok
      ? await getEnvelope({
          envelope_id: created.envelope_id,
          dry_run: false,
        })
      : null;

  return {
    ok: Boolean(created.ok && sent?.ok && (status ? status.ok : true)),
    action: normalized_action,
    created,
    sent,
    status,
  };
}

export default runLiveDocusignVerification;
