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
import { resolveContractTemplate } from "@/lib/domain/contracts/resolve-contract-template.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
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

function inferSignerRole(signer = {}) {
  const normalized = lower(
    signer?.role_name || signer?.recipient_type || signer?.signer_id || ""
  );

  if (normalized.includes("buyer")) return "buyer";
  if (normalized.includes("cc")) return "internal_cc";
  return "seller";
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
  resolved_template = null,
} = {}) {
  const normalized_documents = normalizeDocuments(documents);
  const normalized_signers = normalizeSigners(signers);
  const resolved_template_id =
    clean(template_id) || clean(resolved_template?.docusign_template_id);

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

  if (!normalized_documents.length && !resolved_template_id) {
    return {
      ok: false,
      reason: resolved_template
        ? "missing_documents_or_resolved_template"
        : "missing_documents_or_template",
      contract_item_id,
      contract_status,
      resolved_template,
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
      invalid_signer,
    };
  }

  return {
    ok: true,
    reason: "ready_to_send",
    contract_item_id,
    contract_status,
    documents: normalized_documents,
    signers: normalized_signers,
    template_id: resolved_template_id || null,
    resolved_template: resolved_template || null,
  };
}

const defaultDeps = {
  createStoredDocumentPackage,
  createMessageEvent,
  sendContractViaDocusign,
  resolveContractTemplate,
  syncPipelineState,
};

let runtimeDeps = { ...defaultDeps };

export function __setMaybeSendContractForSigningTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetMaybeSendContractForSigningTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function buildContractSendBlocker({
  validation = null,
  resolved_template = null,
} = {}) {
  const reason = clean(validation?.reason);
  const invalid_signer = validation?.invalid_signer || null;
  const invalid_signer_role = inferSignerRole(invalid_signer);

  switch (reason) {
    case "missing_documents_or_template":
    case "missing_documents_or_resolved_template":
      return {
        blocked: "Yes",
        automation_status: "Escalated",
        current_engine: "Contracts",
        blocker_type: "Missing Docs",
        next_system_action: resolved_template?.ok
          ? "prepare_contract_documents"
          : "resolve_contract_template",
        blocker_summary: resolved_template?.ok
          ? "Contract is missing a sendable template package or file documents."
          : "No active auto-generation contract template could be resolved.",
      };
    case "missing_signers":
      return {
        blocked: "Yes",
        automation_status: "Escalated",
        current_engine: "Contracts",
        blocker_type: "Missing Docs",
        next_system_action: "collect_contract_signers",
        blocker_summary: "Contract send is blocked until signer records are attached.",
      };
    case "invalid_signer":
      return {
        blocked: "Yes",
        automation_status: "Escalated",
        current_engine: "Contracts",
        blocker_type: "Missing Docs",
        next_system_action:
          invalid_signer_role === "seller"
            ? "collect_seller_email"
            : invalid_signer_role === "buyer"
              ? "collect_buyer_email"
              : "repair_contract_signer",
        blocker_summary:
          invalid_signer_role === "seller"
            ? "Seller signer data is incomplete for contract send."
            : invalid_signer_role === "buyer"
              ? "Buyer signer data is incomplete for contract send."
              : "One or more contract signers are incomplete.",
      };
    default:
      return null;
  }
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

  const template_resolution =
    !normalizeDocuments(documents).length && !clean(template_id)
      ? await runtimeDeps.resolveContractTemplate({
          contract_item,
          contract_item_id,
        })
      : null;

  const validation = validateSigningInputs({
    contract_item,
    contract_item_id,
    documents,
    signers,
    template_id,
    resolved_template: template_resolution?.ok ? template_resolution : null,
  });

  if (!validation.ok) {
    const blocker = buildContractSendBlocker({
      validation,
      resolved_template: template_resolution,
    });

    if (blocker && validation.contract_item_id) {
      await runtimeDeps.syncPipelineState({
        contract_item_id: validation.contract_item_id,
        blocked: blocker.blocked,
        automation_status: blocker.automation_status,
        current_engine: blocker.current_engine,
        blocker_type: blocker.blocker_type,
        blocker_summary: blocker.blocker_summary,
        next_system_action: blocker.next_system_action,
        notes: blocker.blocker_summary,
        ai_next_move_summary: blocker.next_system_action,
      });
    }

    return {
      ok: false,
      attempted: false,
      sent: false,
      reason: validation.reason,
      contract_item_id: validation.contract_item_id,
      contract_status: validation.contract_status || null,
      envelope_id: validation.envelope_id || null,
      template_resolution,
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
      template_resolution,
    };
  }

  const resolved_subject = deriveResolvedSubject(contract_item, subject);
  const document_archive =
    validation.documents.length
      ? await runtimeDeps.createStoredDocumentPackage({
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
    await runtimeDeps.createMessageEvent({
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

  const send_result = await runtimeDeps.sendContractViaDocusign({
    contract_item_id: validation.contract_item_id,
    contract_item,
    subject: resolved_subject,
    documents: validation.documents,
    signers: validation.signers,
    template_id: validation.template_id,
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
    template_resolution,
    document_archive,
    send_result,
  };
}

export default maybeSendContractForSigning;
