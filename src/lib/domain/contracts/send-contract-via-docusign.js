// ─── send-contract-via-docusign.js ───────────────────────────────────────
import {
  CONTRACT_FIELDS,
  getContractItem,
  updateContractItem,
} from "@/lib/podio/apps/contracts.js";
import { createEnvelope, sendEnvelope } from "@/lib/providers/docusign.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
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

function normalizeDocuments(documents = []) {
  return safeArray(documents)
    .map((doc, index) => ({
      document_id:
        clean(doc.document_id) ||
        clean(doc.id) ||
        String(index + 1),
      name:
        clean(doc.name) ||
        `Contract ${index + 1}`,
      file_base64:
        clean(doc.file_base64) ||
        clean(doc.base64) ||
        "",
      file_extension:
        clean(doc.file_extension) ||
        clean(doc.extension) ||
        "pdf",
    }))
    .filter((doc) => doc.file_base64);
}

function normalizeSigners(signers = []) {
  return safeArray(signers)
    .map((signer, index) => ({
      signer_id:
        clean(signer.signer_id) ||
        clean(signer.id) ||
        String(index + 1),
      name: clean(signer.name),
      email: clean(signer.email),
      routing_order: clean(signer.routing_order) || String(index + 1),
      role_name: clean(signer.role_name) || "",
      recipient_type: clean(signer.recipient_type) || "signer",
    }))
    .filter((signer) => signer.name && signer.email);
}

export async function sendContractViaDocusign({
  contract_item_id = null,
  contract_item = null,
  subject = null,
  documents = [],
  signers = [],
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

  const normalized_documents = normalizeDocuments(documents);
  const normalized_signers = normalizeSigners(signers);

  if (!normalized_documents.length) {
    return {
      ok: false,
      sent: false,
      reason: "missing_documents",
      contract_item_id: resolved_contract_item_id,
    };
  }

  if (!normalized_signers.length) {
    return {
      ok: false,
      sent: false,
      reason: "missing_signers",
      contract_item_id: resolved_contract_item_id,
    };
  }

  const resolved_subject =
    clean(subject) ||
    buildDefaultSubject(resolved_contract_item);

  const envelope_result = await createEnvelope({
    subject: resolved_subject,
    documents: normalized_documents,
    signers: normalized_signers,
    template_id,
    email_blurb,
    metadata: {
      contract_item_id: resolved_contract_item_id,
      ...(metadata && typeof metadata === "object" ? metadata : {}),
    },
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

  await updateContractItem(resolved_contract_item_id, {
    [CONTRACT_FIELDS.contract_status]: dry_run ? "Draft" : "Sent",
    [CONTRACT_FIELDS.docusign_envelope_id]:
      send_result?.envelope_id ||
      envelope_result?.envelope_id ||
      undefined,
    [CONTRACT_FIELDS.contract_sent_timestamp]:
      dry_run ? undefined : { start: nowIso() },
  });
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
    envelope_id:
      send_result?.envelope_id ||
      envelope_result?.envelope_id ||
      null,
    pipeline,
    envelope_result,
    send_result,
  };
}

export default sendContractViaDocusign;
