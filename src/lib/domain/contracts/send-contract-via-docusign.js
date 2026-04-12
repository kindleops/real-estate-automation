// ─── send-contract-via-docusign.js ───────────────────────────────────────
import {
  CONTRACT_FIELDS,
  getContractItem,
  updateContractItem,
} from "@/lib/podio/apps/contracts.js";
import { getEnvelope, sendEnvelope } from "@/lib/providers/docusign.js";
import { createDocusignEnvelopeFromContract } from "@/lib/domain/contracts/create-docusign-envelope-from-contract.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);
  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.value?.item_id) return first.value.item_id;
  if (first?.start) return first.start;

  return null;
}

function buildDefaultSubject(contract_item) {
  return (
    clean(getFieldValue(contract_item, CONTRACT_FIELDS.title)) ||
    clean(getFieldValue(contract_item, CONTRACT_FIELDS.contract_id)) ||
    `Purchase Agreement ${contract_item?.item_id || ""}`.trim()
  );
}

export async function sendContractViaDocusign({
  contract_item_id = null,
  contract_item = null,
  subject = null,
  documents = [],
  recipients = [],
  signers = [],
  seller_recipient = null,
  buyer_recipient = null,
  internal_cc = [],
  template_id = null,
  email_blurb = "",
  metadata = {},
  dry_run = false,
} = {}) {
  let resolved_contract_item = contract_item || null;

  if (!resolved_contract_item && contract_item_id) {
    resolved_contract_item = await getContractItem(contract_item_id);
  }

  const resolved_contract_item_id =
    resolved_contract_item?.item_id ||
    contract_item_id ||
    null;

  if (!resolved_contract_item_id) {
    return {
      ok: false,
      sent: false,
      reason: "missing_contract_item_id",
      contract_item_id: null,
    };
  }

  const envelope_result = await createDocusignEnvelopeFromContract({
    contract_item_id: resolved_contract_item_id,
    contract_item: resolved_contract_item,
    subject: clean(subject) || buildDefaultSubject(resolved_contract_item),
    documents,
    recipients,
    signers,
    seller_recipient,
    buyer_recipient,
    internal_cc,
    template_id,
    email_blurb,
    metadata,
    dry_run,
  });

  if (!envelope_result?.ok) {
    return {
      ok: false,
      sent: false,
      reason: envelope_result?.reason || "envelope_create_failed",
      contract_item_id: resolved_contract_item_id,
      envelope_result,
    };
  }

  const send_result = await sendEnvelope({
    envelope_id: envelope_result.envelope_id,
    dry_run,
  });

  if (!send_result?.ok) {
    return {
      ok: false,
      sent: false,
      reason: send_result?.reason || "envelope_send_failed",
      contract_item_id: resolved_contract_item_id,
      envelope_result,
      send_result,
    };
  }

  const envelope_status =
    !dry_run && send_result?.ok
      ? await getEnvelope({
          envelope_id:
            send_result?.envelope_id ||
            envelope_result?.envelope_id ||
            null,
          dry_run: false,
        })
      : null;

  const resolved_envelope_id =
    send_result?.envelope_id ||
    envelope_result?.envelope_id ||
    null;
  const resolved_signing_link =
    send_result?.signing_link ||
    envelope_result?.signing_link ||
    envelope_status?.signing_link ||
    null;
  const resolved_sent_at =
    send_result?.timestamps?.sent_at ||
    envelope_status?.timestamps?.sent_at ||
    nowIso();

  if (!dry_run) {
    const contract_update = {
      [CONTRACT_FIELDS.contract_status]: "Sent",
      [CONTRACT_FIELDS.docusign_envelope_id]: resolved_envelope_id || undefined,
      [CONTRACT_FIELDS.contract_sent_timestamp]: { start: resolved_sent_at },
    };

    if (resolved_signing_link) {
      contract_update[CONTRACT_FIELDS.docusign_signing_link] = resolved_signing_link;
    }

    await updateContractItem(resolved_contract_item_id, contract_update);
  }

  const pipeline = await syncPipelineState({
    contract_item_id: resolved_contract_item_id,
    notes: dry_run
      ? "DocuSign dry run completed for contract."
      : "Contract sent via DocuSign.",
  });

  return {
    ok: true,
    sent: true,
    reason: dry_run ? "docusign_dry_run_completed" : "contract_sent_via_docusign",
    contract_item_id: resolved_contract_item_id,
    envelope_id: resolved_envelope_id,
    send_status:
      envelope_status?.status ||
      send_result?.status ||
      envelope_result?.status ||
      null,
    recipient_summary:
      envelope_status?.recipient_summary ||
      envelope_result?.recipient_summary ||
      null,
    signing_link: resolved_signing_link,
    timestamps:
      envelope_status?.timestamps ||
      send_result?.timestamps ||
      envelope_result?.timestamps ||
      null,
    pipeline,
    envelope_result,
    send_result,
    envelope_status,
  };
}

export default sendContractViaDocusign;
