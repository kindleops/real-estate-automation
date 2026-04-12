import {
  CONTRACT_FIELDS,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import { createEnvelope } from "@/lib/providers/docusign.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
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

function inferCanonicalRole(recipient = {}) {
  const raw =
    recipient.role ||
    recipient.role_key ||
    recipient.role_name ||
    recipient.recipient_role ||
    recipient.recipient_type;
  const normalized = lower(raw);

  if (["buyer", "buyer signer"].includes(normalized)) return "buyer";
  if (["internal_cc", "internal cc", "cc", "carbon copy"].includes(normalized)) {
    return "internal_cc";
  }

  return "seller";
}

function normalizeRecipient(recipient = {}, index = 0) {
  const role = inferCanonicalRole(recipient);

  return {
    id:
      clean(recipient.id) ||
      clean(recipient.signer_id) ||
      String(index + 1),
    name: clean(recipient.name),
    email: clean(recipient.email),
    role,
    role_name:
      clean(recipient.role_name) ||
      (role === "buyer"
        ? clean(process.env.DOCUSIGN_BUYER_ROLE_NAME) || "Buyer"
        : role === "internal_cc"
          ? "internal_cc"
          : clean(process.env.DOCUSIGN_SELLER_ROLE_NAME) || "Seller"),
    routing_order:
      clean(recipient.routing_order) ||
      clean(recipient.routingOrder) ||
      String(index + 1),
    recipient_type:
      role === "internal_cc"
        ? "carbon_copy"
        : clean(recipient.recipient_type) || "signer",
  };
}

function buildRecipients({
  recipients = [],
  signers = [],
  seller_recipient = null,
  buyer_recipient = null,
  internal_cc = [],
} = {}) {
  const explicit_recipients = [
    ...(seller_recipient ? [{ ...seller_recipient, role: "seller" }] : []),
    ...(buyer_recipient ? [{ ...buyer_recipient, role: "buyer" }] : []),
    ...safeArray(internal_cc).map((recipient) => ({
      ...recipient,
      role: "internal_cc",
      recipient_type: "carbon_copy",
    })),
  ];

  const base = explicit_recipients.length ? explicit_recipients : safeArray(recipients).length
    ? recipients
    : signers;

  return safeArray(base)
    .map(normalizeRecipient)
    .filter((recipient) => recipient.name && recipient.email);
}

export async function createDocusignEnvelopeFromContract({
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
      reason: "missing_contract_item_id",
      contract_item_id: null,
      envelope_id: null,
    };
  }

  const normalized_documents = normalizeDocuments(documents);
  const normalized_recipients = buildRecipients({
    recipients,
    signers,
    seller_recipient,
    buyer_recipient,
    internal_cc,
  });

  if (!normalized_documents.length && !clean(template_id)) {
    return {
      ok: false,
      reason: "missing_documents_or_template",
      contract_item_id: resolved_contract_item_id,
      envelope_id: null,
    };
  }

  if (!normalized_recipients.length) {
    return {
      ok: false,
      reason: "missing_recipients",
      contract_item_id: resolved_contract_item_id,
      envelope_id: null,
    };
  }

  const resolved_subject =
    clean(subject) ||
    buildDefaultSubject(resolved_contract_item);

  const result = await createEnvelope({
    subject: resolved_subject,
    documents: normalized_documents,
    recipients: normalized_recipients,
    template_id: clean(template_id) || null,
    email_blurb,
    metadata: {
      contract_item_id: resolved_contract_item_id,
      contract_id:
        clean(getFieldValue(resolved_contract_item, CONTRACT_FIELDS.contract_id)) ||
        null,
      ...(metadata && typeof metadata === "object" ? metadata : {}),
    },
    dry_run,
  });

  return {
    ...result,
    contract_item_id: resolved_contract_item_id,
    contract_item: resolved_contract_item,
    subject: resolved_subject,
    recipients: normalized_recipients,
  };
}

export default createDocusignEnvelopeFromContract;
