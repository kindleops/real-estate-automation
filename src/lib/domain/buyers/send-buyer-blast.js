import ENV from "@/lib/config/env.js";
import { BUYER_MATCH_FIELDS, getBuyerMatchItem, updateBuyerMatchItem } from "@/lib/podio/apps/buyer-match.js";
import { createBuyerMatchFlow } from "@/lib/flows/create-buyer-match-flow.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import {
  buildBuyerBlastContent,
  buildBuyerMatchDiagnostics,
  loadBuyerDispositionContext,
  resolveExistingBuyerMatch,
} from "@/lib/domain/buyers/match-engine.js";
import { upsertBuyerDispositionThread } from "@/lib/domain/buyers/buyer-threads.js";
import { chooseTextgridNumber } from "@/lib/domain/routing/choose-textgrid-number.js";
import {
  buildBuyerDispositionPackageFiles,
  createStoredDocumentPackage,
} from "@/lib/domain/documents/document-packages.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { sendEmail } from "@/lib/providers/email.js";
import { hasTextgridSendCredentials, sendTextgridSMS } from "@/lib/providers/textgrid.js";
import {
  createMessageEvent,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getTextValue,
} from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function addDaysIso(days = 0) {
  const next = new Date();
  next.setUTCDate(next.getUTCDate() + Number(days || 0));
  return next.toISOString();
}

function hoursBetween(left, right) {
  const left_ts = new Date(left).getTime();
  const right_ts = new Date(right).getTime();
  if (Number.isNaN(left_ts) || Number.isNaN(right_ts)) return null;
  return Math.abs(left_ts - right_ts) / 3_600_000;
}

function appendNotes(...values) {
  return values
    .map((value) => clean(value))
    .filter(Boolean)
    .join("\n");
}

function getPrimaryBuyerEmail(recipient = {}) {
  return clean(recipient?.emails?.[0]) || null;
}

function getPrimaryBuyerPhone(recipient = {}) {
  return clean(recipient?.phones?.[0]) || null;
}

function supportsBuyerBlastSms() {
  return Boolean(
    ENV.ENABLE_LIVE_SENDING &&
      ENV.ENABLE_BUYER_SMS_BLAST &&
      hasTextgridSendCredentials()
  );
}

function prefersSmsChannel(recipient = {}) {
  const method = lower(recipient?.preferred_contact_method);
  return (
    method.includes("sms") ||
    method.includes("text") ||
    method.includes("phone")
  );
}

export function pickBuyerBlastChannel(
  recipient = {},
  {
    sms_enabled = supportsBuyerBlastSms(),
  } = {}
) {
  const has_email = Boolean(getPrimaryBuyerEmail(recipient));
  const has_phone = Boolean(getPrimaryBuyerPhone(recipient));

  if (sms_enabled && has_phone && (prefersSmsChannel(recipient) || !has_email)) {
    return "sms";
  }

  if (has_email) return "email";
  if (sms_enabled && has_phone) return "sms";
  return null;
}

export function buildBuyerSmsBlastText({
  context = {},
  candidate = {},
  package_summary_url = null,
} = {}) {
  const parts = [
    clean(context.property_address) || "Off-market opportunity",
    clean(context.property_type),
    context.purchase_price
      ? `Buy ${Math.round(Number(context.purchase_price)).toLocaleString("en-US", {
          style: "currency",
          currency: "USD",
          maximumFractionDigits: 0,
        })}`
      : "",
    clean(context.closing_date_target) ? `Close ${clean(context.closing_date_target)}` : "",
    clean(package_summary_url) ? `Package ${clean(package_summary_url)}` : "",
    "Reply interested, pass, or questions.",
  ].filter(Boolean);

  return parts.join(" | ").slice(0, 320);
}

function buildBuyerBlastDeliveryPlan(
  recipients = [],
  {
    sms_enabled = supportsBuyerBlastSms(),
  } = {}
) {
  return recipients.map((recipient) => ({
    ...recipient,
    planned_channel: pickBuyerBlastChannel(recipient, { sms_enabled }),
    live_eligible: Number(recipient?.score || 0) >= 45
      ? Boolean(pickBuyerBlastChannel(recipient, { sms_enabled }))
      : false,
  }));
}

function buildBuyerBlastTriggerName(buyer_match_item_id = null, company_item_id = null) {
  return `buyer-blast:${clean(buyer_match_item_id)}:${clean(company_item_id)}`;
}

async function logBuyerBlastEvent({
  buyer_match_item = null,
  context = {},
  recipient = {},
  content = {},
  send_result = null,
  package_record = null,
  dry_run = true,
  channel = "email",
  outbound_number_item_id = null,
}) {
  const status = send_result?.ok
    ? "Sent"
    : dry_run
      ? "Pending"
      : "Failed";

  return createMessageEvent({
    "message-id": clean(send_result?.provider_message_id || send_result?.message_id || ""),
    "timestamp": { start: nowIso() },
    "direction": "Outbound",
    "source-app": "Buyer Disposition",
    "processed-by": dry_run ? "Buyer Blast Dry Run" : "Buyer Blast",
    "trigger-name": buildBuyerBlastTriggerName(
      buyer_match_item?.item_id || null,
      recipient?.item_id || null
    ),
    "message": clean(content?.text),
    "character-count": clean(content?.text).length,
    "status-3": status,
    "status-2": dry_run ? "preview" : clean(send_result?.reason || "sent"),
    "property": context.property_item_id ? [context.property_item_id] : undefined,
    "master-owner": context.master_owner_item_id ? [context.master_owner_item_id] : undefined,
    "ai-output": JSON.stringify({
      version: 1,
      event_kind: "buyer_blast",
      buyer_match_item_id: buyer_match_item?.item_id || null,
      company_item_id: recipient?.item_id || null,
      company_name: recipient?.company_name || null,
      recipient_email: getPrimaryBuyerEmail(recipient),
      recipient_phone: getPrimaryBuyerPhone(recipient),
      channel: clean(channel) || "email",
      dry_run,
      send_ok: send_result?.ok !== false,
      provider_message_id:
        clean(send_result?.provider_message_id || send_result?.message_id || "") || null,
      outbound_number_item_id: outbound_number_item_id || null,
      package_manifest_key: package_record?.manifest_key || null,
      package_manifest_url: package_record?.manifest_access_url || null,
      package_primary_key: package_record?.primary_file?.key || null,
      package_primary_url: package_record?.primary_file?.access_url || null,
    }),
    ...(outbound_number_item_id ? { "textgrid-number": outbound_number_item_id } : {}),
  });
}

export function buildPreviewRecipients(
  diagnostics = {},
  max_buyers = 5,
  {
    min_score = 0,
    require_email = false,
  } = {}
) {
  return (diagnostics?.diagnostics?.top_candidates || [])
    .filter((candidate) => Number(candidate?.score || 0) >= Number(min_score || 0))
    .map((candidate) => ({
      item_id: candidate?.item_id || null,
      company_name: clean(candidate?.company_name),
      score: Number(candidate?.score || 0),
      reasons: Array.isArray(candidate?.reasons) ? candidate.reasons : [],
      emails: Array.isArray(candidate?.emails) ? candidate.emails : [],
      phones: Array.isArray(candidate?.phones) ? candidate.phones : [],
      preferred_contact_method: clean(candidate?.preferred_contact_method),
      blast_preview: candidate?.blast_preview || null,
    }))
    .filter((candidate) => !require_email || candidate.emails.length > 0)
    .slice(0, max_buyers);
}

export async function sendBuyerBlast({
  buyer_match_id = null,
  property_id = null,
  dry_run = true,
  max_buyers = 5,
  force = false,
} = {}) {
  let buyer_match_item =
    buyer_match_id ? await getBuyerMatchItem(buyer_match_id) : null;

  if (!buyer_match_item?.item_id && property_id) {
    const bootstrap = await createBuyerMatchFlow({
      property_id,
      dry_run,
      candidate_limit: Math.max(max_buyers, 10),
    });

    if (bootstrap?.buyer_match_item_id && !dry_run) {
      buyer_match_item = await getBuyerMatchItem(bootstrap.buyer_match_item_id);
    }

    if (!buyer_match_item?.item_id && dry_run && bootstrap?.ok) {
      const preview_recipients = buildPreviewRecipients(bootstrap?.diagnostics, max_buyers);
      const blast_delivery_plan = buildBuyerBlastDeliveryPlan(preview_recipients, {
        sms_enabled: supportsBuyerBlastSms(),
      });
      return {
        ok: true,
        sent: false,
        dry_run: true,
        reason: "buyer_blast_preview_ready",
        buyer_match_item_id: bootstrap?.buyer_match_item_id || null,
        buyer_match_id: bootstrap?.buyer_match_id || null,
        disposition_strategy: bootstrap?.disposition_strategy || null,
        live_blast_supported: Boolean(bootstrap?.live_blast_supported),
        live_sms_supported: supportsBuyerBlastSms(),
        recipients: preview_recipients,
        blast_eligible_recipients: blast_delivery_plan.filter(
          (recipient) => Number(recipient?.score || 0) >= 45 && recipient?.planned_channel
        ),
        blast_delivery_plan,
        diagnostics: bootstrap?.diagnostics || null,
      };
    }
  }

  if (!buyer_match_item?.item_id) {
    const existing = await resolveExistingBuyerMatch({
      property_id,
      buyer_match_item_id: buyer_match_id,
    });
    buyer_match_item = existing || null;
  }

  if (!buyer_match_item?.item_id) {
    return {
      ok: false,
      sent: false,
      dry_run,
      reason: "buyer_match_not_found",
      buyer_match_id,
      property_id,
    };
  }

  const linked_property_id = getFirstAppReferenceId(
    buyer_match_item,
    BUYER_MATCH_FIELDS.property,
    property_id
  );
  const linked_contract_id = getFirstAppReferenceId(
    buyer_match_item,
    BUYER_MATCH_FIELDS.contract,
    null
  );
  const linked_closing_id = getFirstAppReferenceId(
    buyer_match_item,
    BUYER_MATCH_FIELDS.closing,
    null
  );

  const diagnostics = await buildBuyerMatchDiagnostics({
    property_id: linked_property_id,
    contract_id: linked_contract_id,
    closing_id: linked_closing_id,
    candidate_limit: Math.max(max_buyers, 10),
  });

  if (!diagnostics?.ok) {
    return {
      ok: false,
      sent: false,
      dry_run,
      reason: diagnostics?.reason || "buyer_blast_diagnostics_failed",
      buyer_match_item_id: buyer_match_item.item_id,
      diagnostics,
    };
  }

  const context =
    (await loadBuyerDispositionContext({
      property_id: linked_property_id,
      contract_id: linked_contract_id,
      closing_id: linked_closing_id,
    })) || diagnostics.context;

  const sms_enabled = supportsBuyerBlastSms();
  const preview_recipients = buildPreviewRecipients(diagnostics, max_buyers);
  const blast_delivery_plan = buildBuyerBlastDeliveryPlan(preview_recipients, {
    sms_enabled,
  });
  const blast_recipients = blast_delivery_plan.filter(
    (recipient) => Number(recipient?.score || 0) >= 45 && recipient?.planned_channel
  );
  const package_preview = {
    files: buildBuyerDispositionPackageFiles({
      context,
      diagnostics,
    }).map((file) => ({
      filename: file.filename,
      content_type: file.content_type,
    })),
  };
  const match_status = clean(
    getCategoryValue(buyer_match_item, BUYER_MATCH_FIELDS.match_status, "")
  );

  if (!dry_run && !context.live_blast_supported) {
    return {
      ok: false,
      sent: false,
      dry_run: false,
      reason: "live_blast_not_supported_for_disposition_strategy",
      buyer_match_item_id: buyer_match_item.item_id,
      disposition_strategy: context.disposition_strategy,
      live_blast_supported: false,
      recipients: preview_recipients,
      diagnostics,
    };
  }

  if (!dry_run && !context.contract_item_id && !context.closing_item_id) {
    return {
      ok: false,
      sent: false,
      dry_run: false,
      reason: "live_blast_requires_contract_or_closing",
      buyer_match_item_id: buyer_match_item.item_id,
      recipients: preview_recipients,
      diagnostics,
    };
  }

  if (
    !dry_run &&
    !force &&
    ["Buyers Chosen", "Assigned", "Closed"].includes(match_status)
  ) {
    return {
      ok: false,
      sent: false,
      dry_run: false,
      reason: "buyer_blast_not_allowed_after_buyer_selection",
      buyer_match_item_id: buyer_match_item.item_id,
      match_status,
      recipients: preview_recipients,
      diagnostics,
    };
  }

  const recent_package_sent = getDateValue(
    buyer_match_item,
    BUYER_MATCH_FIELDS.package_sent_date,
    null
  );
  const buyer_response_status = clean(
    getCategoryValue(buyer_match_item, BUYER_MATCH_FIELDS.buyer_response_status, "")
  );

  if (
    !dry_run &&
    !force &&
    recent_package_sent &&
    hoursBetween(recent_package_sent, nowIso()) !== null &&
    hoursBetween(recent_package_sent, nowIso()) < 12 &&
    ["Sent", "Opened", "Interested", "Needs More Info"].includes(buyer_response_status || "Sent")
  ) {
    return {
      ok: false,
      sent: false,
      dry_run: false,
      reason: "buyer_blast_already_sent_recently",
      buyer_match_item_id: buyer_match_item.item_id,
      recent_package_sent,
      recipients: preview_recipients,
      diagnostics,
    };
  }

  if (dry_run) {
    return {
      ok: true,
      sent: false,
      dry_run: true,
      reason: "buyer_blast_preview_ready",
      buyer_match_item_id: buyer_match_item.item_id,
      disposition_strategy: context.disposition_strategy,
      live_blast_supported: Boolean(context.live_blast_supported),
      live_sms_supported: sms_enabled,
      recipients: preview_recipients,
      blast_eligible_recipients: blast_recipients,
      blast_delivery_plan,
      package_preview,
      diagnostics,
    };
  }

  if (!blast_recipients.length) {
    return {
      ok: false,
      sent: false,
      dry_run: false,
      reason: "no_blast_eligible_buyers",
      buyer_match_item_id: buyer_match_item.item_id,
      disposition_strategy: context.disposition_strategy,
      recipients: preview_recipients,
      diagnostics,
    };
  }

  const package_record = await createStoredDocumentPackage({
    namespace: "buyer-packages",
    entity_type: "buyer-match",
    entity_id: buyer_match_item.item_id,
    label: "buyer-disposition-package",
    metadata: {
      buyer_match_item_id: buyer_match_item.item_id,
      property_item_id: context.property_item_id || null,
      contract_item_id: context.contract_item_id || null,
      closing_item_id: context.closing_item_id || null,
      disposition_strategy: context.disposition_strategy || null,
    },
    files: buildBuyerDispositionPackageFiles({
      context,
      diagnostics,
    }),
    dry_run: false,
  });

  if (!package_record?.ok) {
    await recordSystemAlert({
      subsystem: "buyer_blast",
      code: "package_archive_failed",
      severity: "high",
      retryable: true,
      summary: `Buyer package archive failed: ${clean(package_record?.reason) || "unknown_error"}`,
      dedupe_key: `buyer-blast:${buyer_match_item.item_id}:package`,
      affected_ids: [buyer_match_item.item_id],
      metadata: {
        buyer_match_item_id: buyer_match_item.item_id,
        disposition_strategy: context.disposition_strategy,
      },
    });
  }

  const results = [];
  const blast_context = {
    ids: {
      market_id: context.market_item_id || null,
      phone_item_id: buyer_match_item.item_id || null,
    },
    summary: {
      market_name: context.market_name || null,
      language_preference: "English",
    },
  };
  const outbound_sms_number = sms_enabled
    ? await chooseTextgridNumber({
        context: blast_context,
        rotation_key: `buyer-blast:${buyer_match_item.item_id}`,
      })
    : null;

  for (const recipient of blast_recipients) {
    const primary_email = getPrimaryBuyerEmail(recipient);
    const primary_phone = getPrimaryBuyerPhone(recipient);
    const planned_channel = clean(recipient?.planned_channel) || "email";
    const content = buildBuyerBlastContent({
      context,
      candidate: recipient,
      package_summary_url: package_record?.primary_file?.access_url || null,
      package_manifest_url: package_record?.manifest_access_url || null,
    });
    const sms_text = buildBuyerSmsBlastText({
      context,
      candidate: recipient,
      package_summary_url: package_record?.primary_file?.access_url || null,
    });

    if (planned_channel === "email" && !primary_email) {
      const blast_event = await logBuyerBlastEvent({
        buyer_match_item,
        context,
        recipient,
        content,
        send_result: {
          ok: false,
          reason: "missing_buyer_email",
        },
        package_record,
        dry_run: false,
        channel: "email",
      });

      await upsertBuyerDispositionThread({
        buyer_match_item,
        company_item_id: recipient.item_id,
        company_name: recipient.company_name,
        recipient_email: null,
        recipient_phone: primary_phone,
        channel: "email",
        direction: "Outbound",
        interaction_kind: "blast_skipped",
        interaction_status: "Missing Email",
        subject: content.subject,
        message: content.text,
        related_event_item_id: blast_event?.item_id || null,
        metadata: {
          send_reason: "missing_buyer_email",
          disposition_strategy: context.disposition_strategy || null,
        },
        timestamp: nowIso(),
        send_ok: false,
      }).catch(() => null);

      results.push({
        ok: false,
        company_item_id: recipient.item_id,
        company_name: recipient.company_name,
        channel: "email",
        reason: "missing_buyer_email",
      });
      continue;
    }

    if (planned_channel === "sms" && !primary_phone) {
      const blast_event = await logBuyerBlastEvent({
        buyer_match_item,
        context,
        recipient,
        content: {
          subject: content.subject,
          text: sms_text,
        },
        send_result: {
          ok: false,
          reason: "missing_buyer_phone",
        },
        package_record,
        dry_run: false,
        channel: "sms",
      });

      await upsertBuyerDispositionThread({
        buyer_match_item,
        company_item_id: recipient.item_id,
        company_name: recipient.company_name,
        recipient_email: primary_email,
        recipient_phone: null,
        channel: "sms",
        direction: "Outbound",
        interaction_kind: "blast_skipped",
        interaction_status: "Missing Phone",
        subject: content.subject,
        message: sms_text,
        related_event_item_id: blast_event?.item_id || null,
        metadata: {
          send_reason: "missing_buyer_phone",
          disposition_strategy: context.disposition_strategy || null,
        },
        timestamp: nowIso(),
        send_ok: false,
      }).catch(() => null);

      results.push({
        ok: false,
        company_item_id: recipient.item_id,
        company_name: recipient.company_name,
        channel: "sms",
        reason: "missing_buyer_phone",
      });
      continue;
    }

    if (planned_channel === "sms" && !outbound_sms_number?.normalized_phone) {
      const blast_event = await logBuyerBlastEvent({
        buyer_match_item,
        context,
        recipient,
        content: {
          subject: content.subject,
          text: sms_text,
        },
        send_result: {
          ok: false,
          reason: "missing_buyer_sms_outbound_number",
        },
        package_record,
        dry_run: false,
        channel: "sms",
      });

      await upsertBuyerDispositionThread({
        buyer_match_item,
        company_item_id: recipient.item_id,
        company_name: recipient.company_name,
        recipient_email: primary_email,
        recipient_phone: primary_phone,
        channel: "sms",
        direction: "Outbound",
        interaction_kind: "blast_failed",
        interaction_status: "No SMS Number",
        subject: content.subject,
        message: sms_text,
        related_event_item_id: blast_event?.item_id || null,
        metadata: {
          send_reason: "missing_buyer_sms_outbound_number",
          disposition_strategy: context.disposition_strategy || null,
        },
        timestamp: nowIso(),
        send_ok: false,
      }).catch(() => null);

      results.push({
        ok: false,
        company_item_id: recipient.item_id,
        company_name: recipient.company_name,
        channel: "sms",
        reason: "missing_buyer_sms_outbound_number",
      });
      continue;
    }

    let send_result;
    if (planned_channel === "sms") {
      try {
        send_result = await sendTextgridSMS({
          to: primary_phone,
          from: outbound_sms_number.normalized_phone,
          body: sms_text,
          message_type: "sms",
          client_reference_id: `buyer-blast:${buyer_match_item.item_id}:${recipient.item_id}`,
        });
      } catch (error) {
        send_result = {
          ok: false,
          reason: clean(error?.message) || "buyer_blast_sms_send_failed",
        };
      }
    } else {
      try {
        send_result = await sendEmail({
          to: [primary_email],
          subject: content.subject,
          text: content.text,
          dry_run: false,
        });
      } catch (error) {
        send_result = {
          ok: false,
          reason: clean(error?.message) || "buyer_blast_send_failed",
        };
      }
    }

    const blast_event = await logBuyerBlastEvent({
      buyer_match_item,
      context,
      recipient,
      content: planned_channel === "sms"
        ? {
            subject: content.subject,
            text: sms_text,
          }
        : content,
      send_result,
      package_record,
      dry_run: false,
      channel: planned_channel,
      outbound_number_item_id:
        planned_channel === "sms" ? outbound_sms_number?.item_id || null : null,
    });

    await upsertBuyerDispositionThread({
      buyer_match_item,
      company_item_id: recipient.item_id,
      company_name: recipient.company_name,
      recipient_email: primary_email,
      recipient_phone: primary_phone,
      channel: planned_channel,
      direction: "Outbound",
      interaction_kind: send_result?.ok !== false ? "blast_sent" : "blast_failed",
      interaction_status: send_result?.ok !== false ? "Sent" : "Failed",
      subject: content.subject,
      message: planned_channel === "sms" ? sms_text : content.text,
      provider_message_id:
        clean(send_result?.provider_message_id || send_result?.message_id || "") || null,
      related_event_item_id: blast_event?.item_id || null,
      metadata: {
        send_reason: clean(send_result?.reason) || null,
        disposition_strategy: context.disposition_strategy || null,
      },
      timestamp: nowIso(),
      send_ok: send_result?.ok !== false,
    }).catch(() => null);

    results.push({
      ok: send_result?.ok !== false,
      company_item_id: recipient.item_id,
      company_name: recipient.company_name,
      recipient_email: primary_email,
      recipient_phone: primary_phone,
      channel: planned_channel,
      reason: clean(send_result?.reason) || "buyer_blast_sent",
      provider_message_id:
        clean(send_result?.provider_message_id || send_result?.message_id || "") || null,
    });
  }

  const sent_count = results.filter((result) => result.ok).length;
  const failed_recipient_count = results.filter((result) => result?.ok === false).length;
  const payload = {
    [BUYER_MATCH_FIELDS.package_sent_date]: { start: nowIso() },
    [BUYER_MATCH_FIELDS.buyer_response_status]: sent_count ? "Sent" : "Not Sent",
    [BUYER_MATCH_FIELDS.match_status]: sent_count ? "Sent to Buyers" : "Buyers Selected",
    [BUYER_MATCH_FIELDS.automation_status]: sent_count ? "Waiting" : "Running",
    [BUYER_MATCH_FIELDS.next_buyer_follow_up]: {
      start: sent_count ? addDaysIso(1) : nowIso(),
    },
    [BUYER_MATCH_FIELDS.internal_notes]: appendNotes(
      getTextValue(buyer_match_item, BUYER_MATCH_FIELDS.internal_notes, ""),
      `[${nowIso()}] Buyer blast ${sent_count ? `sent to ${sent_count} buyer(s)` : "attempted with no successful sends"}.`,
      package_record?.ok
        ? `[${nowIso()}] Buyer package archived at ${package_record.manifest_key}${package_record.manifest_access_url ? ` (${package_record.manifest_access_url})` : ""}.`
        : `[${nowIso()}] Buyer package archive unavailable: ${clean(package_record?.reason) || "not_configured"}.`
    ),
  };

  await updateBuyerMatchItem(buyer_match_item.item_id, payload);

  const pipeline = await syncPipelineState({
    property_id: context.property_item_id,
    master_owner_id: context.master_owner_item_id,
    contract_item_id: context.contract_item_id,
    closing_item_id: context.closing_item_id,
    buyer_match_item_id: buyer_match_item.item_id,
    market_id: context.market_item_id,
    notes: sent_count
      ? "Buyer blast sent and pipeline advanced into buyer response waiting state."
      : "Buyer blast attempted but no buyer package was delivered.",
  });

  if (failed_recipient_count > 0 || sent_count === 0) {
    await recordSystemAlert({
      subsystem: "buyer_blast",
      code: "send_failures",
      severity: sent_count === 0 ? "high" : "warning",
      retryable: true,
      summary: `Buyer blast delivered to ${sent_count} recipient(s) with ${failed_recipient_count} failure(s).`,
      dedupe_key: `buyer-blast:${buyer_match_item.item_id}:send`,
      affected_ids: [buyer_match_item.item_id],
      metadata: {
        sent_count,
        failed_recipient_count,
        package_ok: Boolean(package_record?.ok),
      },
    });
  } else {
    await resolveSystemAlert({
      subsystem: "buyer_blast",
      code: "send_failures",
      dedupe_key: `buyer-blast:${buyer_match_item.item_id}:send`,
      resolution_message: "Buyer blast completed without send failures.",
    });
  }

  return {
    ok: true,
    sent: sent_count > 0,
    dry_run: false,
    reason: sent_count ? "buyer_blast_sent" : "buyer_blast_attempted_no_deliveries",
    buyer_match_item_id: buyer_match_item.item_id,
    disposition_strategy: context.disposition_strategy,
    live_blast_supported: Boolean(context.live_blast_supported),
    live_sms_supported: sms_enabled,
    sent_count,
    skipped_count: results.filter((result) => !result.ok).length,
    recipients: preview_recipients,
    blast_eligible_recipients: blast_recipients,
    blast_delivery_plan,
    results,
    diagnostics,
    package_record,
    pipeline,
  };
}

export default sendBuyerBlast;
