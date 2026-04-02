// ─── maybe-send-contract-for-signing.js ──────────────────────────────────
import {
  CONTRACT_FIELDS,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  buildContractArchiveFiles,
  createStoredDocumentPackage,
} from "@/lib/domain/documents/document-packages.js";
import { sendContractViaDocusign } from "@/lib/domain/contracts/send-contract-via-docusign.js";
import { createMessageEvent } from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function deriveContractItemId(contract) {
  return (
    contract?.contract_item_id ||
    contract?.item_id ||
    contract?.contract?.contract_item_id ||
    null
  );
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

function normalizeDocuments(documents = []) {
  return safeArray(documents)
    .map((doc, index) => ({
      document_id:
        clean(doc?.document_id) ||
        clean(doc?.id) ||
        String(index + 1),
      name:
        clean(doc?.name) ||
        `Contract ${index + 1}`,
      file_base64:
        clean(doc?.file_base64) ||
        clean(doc?.base64) ||
        "",
      file_extension:
        clean(doc?.file_extension) ||
        clean(doc?.extension) ||
        "pdf",
    }))
    .filter((doc) => doc.file_base64);
}

function normalizeSigners(signers = []) {
  return safeArray(signers)
    .map((signer, index) => ({
      signer_id:
        clean(signer?.signer_id) ||
        clean(signer?.id) ||
        String(index + 1),
      name: clean(signer?.name),
      email: clean(signer?.email),
      routing_order: clean(signer?.routing_order) || String(index + 1),
      role_name: clean(signer?.role_name) || "",
      recipient_type: clean(signer?.recipient_type) || "signer",
    }))
    .filter(Boolean);
}

function isSendableContractStatus(status = "") {
  const normalized = lower(status);
  return ["draft", "sent", "viewed"].includes(normalized);
}

function isTerminalContractStatus(status = "") {
  const normalized = lower(status);
  return ["fully executed", "cancelled", "closed"].includes(normalized);
}

function hasExistingEnvelope(contract_item = null) {
  return Boolean(
    clean(getFieldValue(contract_item, CONTRACT_FIELDS.docusign_envelope_id))
  );
}

function deriveResolvedSubject(contract_item = null, subject = null) {
  return (
    clean(subject) ||
    clean(getFieldValue(contract_item, CONTRACT_FIELDS.title)) ||
    clean(getFieldValue(contract_item, CONTRACT_FIELDS.contract_id)) ||
    `Purchase Agreement ${contract_item?.item_id || ""}`.trim()
  );
}

function validateSigningInputs({
  contract_item = null,
  contract_item_id = null,
  documents = [],
  signers = [],
  template_id = null,
} = {}) {
  const normalized_documents = normalizeDocuments(documents);
  const normalized_signers = normalizeSigners(signers);

  if (!contract_item_id) {
    return {
      ok: false,
      reason: "missing_contract_item_id",
      contract_item_id: null,
    };
  }

  if (!contract_item?.item_id) {
    return {
      ok: false,
      reason: "contract_not_found",
      contract_item_id,
    };
  }

  const contract_status = clean(
    getFieldValue(contract_item, CONTRACT_FIELDS.contract_status)
  );

  if (isTerminalContractStatus(contract_status)) {
    return {
      ok: false,
      reason: "contract_in_terminal_status",
      contract_item_id,
      contract_status,
    };
  }

  if (contract_status && !isSendableContractStatus(contract_status)) {
    return {
      ok: false,
      reason: "contract_not_sendable",
      contract_item_id,
      contract_status,
    };
  }

  if (hasExistingEnvelope(contract_item)) {
    return {
      ok: false,
      reason: "docusign_envelope_already_exists",
      contract_item_id,
      contract_status,
      envelope_id: clean(
        getFieldValue(contract_item, CONTRACT_FIELDS.docusign_envelope_id)
      ),
    };
  }

  if (!normalized_documents.length && !clean(template_id)) {
    return {
      ok: false,
      reason: "missing_documents_or_template",
      contract_item_id,
      contract_status,
    };
  }

  if (!normalized_signers.length) {
    return {
      ok: false,
      reason: "missing_signers",
      contract_item_id,
      contract_status,
    };
  }

  const invalid_signer = normalized_signers.find(
    (signer) => !signer.name || !signer.email
  );

  if (invalid_signer) {
    return {
      ok: false,
      reason: "invalid_signer",
      contract_item_id,
      contract_status,
    };
  }

  return {
    ok: true,
    reason: "ready_to_send",
    contract_item_id,
    contract_status,
    documents: normalized_documents,
    signers: normalized_signers,
  };
}

export async function maybeSendContractForSigning({
  contract = null,
  documents = [],
  signers = [],
  subject = null,
  template_id = null,
  email_blurb = "",
  metadata = {},
  dry_run = false,
  auto_send = true,
} = {}) {
  const contract_item_id = deriveContractItemId(contract);

  if (!contract_item_id) {
    return {
      ok: false,
      attempted: false,
      sent: false,
      reason: "missing_contract_item_id",
      contract_item_id: null,
    };
  }

  const contract_item =
    contract?.fields
      ? contract
      : await getContractItem(contract_item_id);

  const validation = validateSigningInputs({
    contract_item,
    contract_item_id,
    documents,
    signers,
    template_id,
  });

  if (!validation.ok) {
    return {
      ok: false,
      attempted: false,
      sent: false,
      reason: validation.reason,
      contract_item_id: validation.contract_item_id,
      contract_status: validation.contract_status || null,
      envelope_id: validation.envelope_id || null,
    };
  }

  if (!auto_send) {
    return {
      ok: true,
      attempted: false,
      sent: false,
      reason: "auto_send_disabled",
      contract_item_id: validation.contract_item_id,
      contract_status: validation.contract_status || null,
      ready: true,
      documents_count: validation.documents.length,
      signers_count: validation.signers.length,
    };
  }

  const resolved_subject = deriveResolvedSubject(contract_item, subject);
  const document_archive =
    validation.documents.length
      ? await createStoredDocumentPackage({
          namespace: "contracts",
          entity_type: "contract",
          entity_id: validation.contract_item_id,
          label: "contract-signing-documents",
          metadata: {
            contract_item_id: validation.contract_item_id,
            subject: resolved_subject,
            signers: validation.signers.map((signer) => ({
              signer_id: signer.signer_id,
              email: signer.email,
              role_name: signer.role_name,
            })),
          },
          files: buildContractArchiveFiles({
            documents: validation.documents,
          }),
          dry_run,
        })
      : null;

  if (document_archive?.ok) {
    await createMessageEvent({
      "message-id": `contract-archive:${validation.contract_item_id}:${document_archive.package_id}`,
      "timestamp": { start: new Date().toISOString() },
      "direction": "Outbound",
      "source-app": "Contracts",
      "processed-by": "Contract Document Archive",
      "trigger-name": `contract-archive:${validation.contract_item_id}`,
      "message": `Contract signing package archived at ${document_archive.manifest_key}`,
      "status-3": dry_run ? "Pending" : "Sent",
      "property": getFieldValue(contract_item, CONTRACT_FIELDS.property)
        ? [getFieldValue(contract_item, CONTRACT_FIELDS.property)]
        : undefined,
      "master-owner": getFieldValue(contract_item, CONTRACT_FIELDS.master_owner)
        ? [getFieldValue(contract_item, CONTRACT_FIELDS.master_owner)]
        : undefined,
      "ai-output": JSON.stringify({
        version: 1,
        event_kind: "contract_archive",
        contract_item_id: validation.contract_item_id,
        manifest_key: document_archive.manifest_key,
        manifest_access_url: document_archive.manifest_access_url || null,
        files: document_archive.files || [],
      }),
    });
  }

  const send_result = await sendContractViaDocusign({
    contract_item_id: validation.contract_item_id,
    contract_item,
    subject: resolved_subject,
    documents: validation.documents,
    signers: validation.signers,
    template_id,
    email_blurb,
    metadata,
    dry_run,
  });

  return {
    ok: Boolean(send_result?.ok),
    attempted: true,
    sent: Boolean(send_result?.sent),
    reason: send_result?.reason || "contract_send_attempted",
    contract_item_id: validation.contract_item_id,
    contract_status: validation.contract_status || null,
    envelope_id: send_result?.envelope_id || null,
    documents_count: validation.documents.length,
    signers_count: validation.signers.length,
    document_archive,
    send_result,
  };
}

export default maybeSendContractForSigning;
