// ─── maybe-create-contract-from-accepted-offer.js ───────────────────────
import {
  CONTRACT_FIELDS,
  findContractItems,
} from "@/lib/podio/apps/contracts.js";
import { createContractFromOffer } from "@/lib/domain/contracts/create-contract-from-offer.js";
import { maybeSendContractForSigning } from "@/lib/domain/contracts/maybe-send-contract-for-signing.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);

  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (first?.value?.item_id) return first.value.item_id;
  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.start) return first.start;

  return null;
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

function isAcceptedOffer({
  offer_item = null,
  offer_status = null,
  offer_progress = null,
} = {}) {
  const normalized_status =
    clean(offer_status) ||
    clean(getFieldValue(offer_item, "offer-status")) ||
    "";

  if (normalized_status.toLowerCase() === "accepted (ready for contract)") {
    return true;
  }

  if (
    clean(offer_progress?.result?.status).toLowerCase() ===
    "accepted (ready for contract)"
  ) {
    return true;
  }

  if (clean(offer_progress?.reason).toLowerCase().includes("accept")) {
    return true;
  }

  return false;
}

async function findLatestContractByOfferId(offer_item_id) {
  if (!offer_item_id) return null;

  const matches = await findContractItems(
    { [CONTRACT_FIELDS.offer]: offer_item_id },
    50,
    0
  );

  return sortNewestFirst(matches)[0] || null;
}

function isTerminalContractStatus(status = "") {
  const normalized = clean(status).toLowerCase();
  return ["fully executed", "closed", "cancelled"].includes(normalized);
}

export async function maybeCreateContractFromAcceptedOffer({
  offer_item = null,
  offer_item_id = null,
  offer_status = null,
  offer_progress = null,
  context = null,
  route = null,
  underwriting = null,
  pipeline_item_id = null,
  title_company_item_id = null,
  contract_status = "Draft",
  notes = "",
  source_message = "",
  documents = [],
  signers = [],
  subject = null,
  template_id = null,
  email_blurb = "",
  metadata = {},
  auto_send = false,
  dry_run = false,
} = {}) {
  const resolved_offer_item_id =
    offer_item?.item_id ||
    offer_item_id ||
    null;

  if (!resolved_offer_item_id) {
    return {
      ok: false,
      created: false,
      sent: false,
      reason: "missing_offer_item_id",
    };
  }

  if (
    !isAcceptedOffer({
      offer_item,
      offer_status,
      offer_progress,
    })
  ) {
    return {
      ok: true,
      created: false,
      sent: false,
      reason: "offer_not_accepted",
      offer_item_id: resolved_offer_item_id,
    };
  }

  const existing_contract = await findLatestContractByOfferId(
    resolved_offer_item_id
  );

  if (existing_contract?.item_id) {
    const existing_contract_status = clean(
      getFieldValue(existing_contract, CONTRACT_FIELDS.contract_status)
    );

    if (!isTerminalContractStatus(existing_contract_status)) {
      const maybe_send_existing = await maybeSendContractForSigning({
        contract: existing_contract,
        documents,
        signers,
        subject,
        template_id,
        email_blurb,
        metadata,
        auto_send,
        dry_run,
      });
      const pipeline = await syncPipelineState({
        offer_item_id: resolved_offer_item_id,
        contract_item_id: existing_contract.item_id,
        notes: maybe_send_existing?.sent
          ? "Existing contract sent for signature."
          : "Existing contract found for accepted offer.",
      });

      return {
        ok: true,
        created: false,
        sent: Boolean(maybe_send_existing?.sent),
        reason: "existing_contract_found",
        offer_item_id: resolved_offer_item_id,
        contract_item_id: existing_contract.item_id,
        existing_contract,
        send_result: maybe_send_existing,
        pipeline,
      };
    }
  }

  const created_contract = await createContractFromOffer({
    offer_item_id: resolved_offer_item_id,
    offer_item,
    context,
    route,
    underwriting,
    pipeline_item_id,
    title_company_item_id,
    contract_status,
    notes,
    source_message,
  });

  if (!created_contract?.ok || !created_contract?.contract_item_id) {
    return {
      ok: false,
      created: false,
      sent: false,
      reason: created_contract?.reason || "contract_create_failed",
      offer_item_id: resolved_offer_item_id,
      created_contract,
    };
  }

  const maybe_send = await maybeSendContractForSigning({
    contract: {
      contract_item_id: created_contract.contract_item_id,
    },
    documents,
    signers,
    subject,
    template_id,
    email_blurb,
    metadata,
    auto_send,
    dry_run,
  });

  return {
    ok: true,
    created: true,
    sent: Boolean(maybe_send?.sent),
    reason: "contract_created_from_accepted_offer",
    offer_item_id: resolved_offer_item_id,
    contract_item_id: created_contract.contract_item_id,
    contract: created_contract,
    send_result: maybe_send,
  };
}

export default maybeCreateContractFromAcceptedOffer;
