// ─── create-contract-from-offer.js ───────────────────────────────────────
import {
  CONTRACT_FIELDS,
  createContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  OFFER_FIELDS,
  getOfferItem,
} from "@/lib/podio/apps/offers.js";
import {
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getMoneyValue,
} from "@/lib/providers/podio.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function asAppRef(value) {
  if (!value) return undefined;
  return [value];
}

function buildContractId({
  offer_item_id = null,
  property_id = null,
} = {}) {
  const stamp = Date.now();
  if (offer_item_id) return `CTR-${offer_item_id}-${stamp}`;
  if (property_id) return `CTR-P-${property_id}-${stamp}`;
  return `CTR-${stamp}`;
}

function normalizeContractStatus(value = "Draft") {
  const raw = clean(value).toLowerCase();

  if (!raw) return "Draft";
  if (raw === "draft") return "Draft";
  if (raw === "sent") return "Sent";
  if (raw === "viewed") return "Viewed";
  if (raw === "seller signed") return "Seller Signed";
  if (raw === "buyer signed") return "Buyer Signed";
  if (raw === "signed" || raw === "fully executed") return "Fully Executed";
  if (raw === "sent to title") return "Sent To Title";
  if (raw === "opened") return "Opened";
  if (raw === "clear to close") return "Clear To Close";
  if (raw === "closed") return "Closed";
  if (["declined", "voided", "cancelled"].includes(raw)) return "Cancelled";

  return clean(value) || "Draft";
}

function normalizeContractType(value = "") {
  const raw = clean(value).toLowerCase();

  if (!raw) return "Cash";
  if (raw.includes("novation")) return "Novation";
  if (raw.includes("multi")) return "Multifamily";
  if (raw.includes("creative") || raw.includes("subject") || raw.includes("seller")) {
    return "Creative";
  }

  return "Cash";
}

function normalizeTemplateType(contract_type = "") {
  const raw = clean(contract_type).toLowerCase();

  if (raw === "novation") return "Novation";
  if (raw === "multifamily") return "Mutlifamily";
  if (raw === "creative") return "Creative";
  return "Standard Purchase";
}

function deriveOfferRefs({
  offer_item = null,
  context = null,
  pipeline_item_id = null,
  title_company_item_id = null,
} = {}) {
  const ids = context?.ids || {};

  return {
    master_owner_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.master_owner, null) ||
      ids.master_owner_id ||
      null,
    prospect_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.prospect, null) ||
      ids.prospect_id ||
      null,
    property_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.property, null) ||
      ids.property_id ||
      null,
    phone_item_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.phone_number, null) ||
      ids.phone_item_id ||
      null,
    email_item_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.email_address, null) ||
      null,
    conversation_item_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.conversation, null) ||
      ids.brain_item_id ||
      null,
    assigned_agent_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.assigned_agent, null) ||
      ids.assigned_agent_id ||
      null,
    market_item_id:
      getFirstAppReferenceId(offer_item, OFFER_FIELDS.market, null) ||
      ids.market_id ||
      null,
    pipeline_item_id: pipeline_item_id || null,
    title_company_item_id: title_company_item_id || null,
  };
}

function deriveContractValues({
  offer_item = null,
  route = null,
  underwriting = null,
  explicit_purchase_price = null,
  explicit_emd_amount = null,
  explicit_closing_timeline = null,
  explicit_contract_strategy = null,
  notes = "",
  source_message = "",
} = {}) {
  const underwriting_signals = underwriting?.signals || {};
  const underwriting_strategy = underwriting?.strategy || {};

  const contract_type = normalizeContractType(
    clean(explicit_contract_strategy) ||
      clean(getCategoryValue(offer_item, OFFER_FIELDS.offer_type, "")) ||
      clean(underwriting_signals.underwriting_strategy) ||
      clean(underwriting_strategy.strategy) ||
      clean(route?.use_case) ||
      "Cash"
  );

  return {
    purchase_price:
      asNumber(explicit_purchase_price) ??
      getMoneyValue(offer_item, OFFER_FIELDS.offer_sent_price, null) ??
      asNumber(underwriting_signals.asking_price) ??
      asNumber(underwriting_signals.desired_price) ??
      null,
    emd_amount: asNumber(explicit_emd_amount) ?? null,
    closing_timeline_days: asNumber(explicit_closing_timeline) ?? null,
    closing_date_target:
      getDateValue(offer_item, OFFER_FIELDS.closing_date_target, null) || null,
    contract_type,
    template_type: normalizeTemplateType(contract_type),
    creative_terms:
      clean(underwriting_signals.underwriting_reason) ||
      clean(underwriting_strategy.reason) ||
      clean(notes) ||
      clean(source_message) ||
      "",
  };
}

export async function createContractFromOffer({
  offer_item_id = null,
  offer_item = null,
  context = null,
  route = null,
  underwriting = null,
  pipeline_item_id = null,
  title_company_item_id = null,
  purchase_price = null,
  emd_amount = null,
  closing_timeline = null,
  contract_strategy = null,
  contract_status = "Draft",
  contract_id = null,
  notes = "",
  source_message = "",
} = {}) {
  let resolved_offer_item = offer_item || null;

  if (!resolved_offer_item && offer_item_id) {
    resolved_offer_item = await getOfferItem(offer_item_id);
  }

  const resolved_offer_item_id =
    resolved_offer_item?.item_id ||
    offer_item_id ||
    null;

  if (!resolved_offer_item_id) {
    return {
      ok: false,
      created: false,
      reason: "missing_offer_item_id",
    };
  }

  let refs = deriveOfferRefs({
    offer_item: resolved_offer_item,
    context,
    pipeline_item_id,
    title_company_item_id,
  });

  if (!refs.pipeline_item_id) {
    const pipeline = await syncPipelineState({
      property_id: refs.property_id,
      master_owner_id: refs.master_owner_id,
      prospect_id: refs.prospect_id,
      conversation_item_id: refs.conversation_item_id,
      offer_item_id: resolved_offer_item_id,
      assigned_agent_id: refs.assigned_agent_id,
      market_id: refs.market_item_id,
      notes: "Preparing contract from accepted offer.",
    });

    if (pipeline?.pipeline_item_id) {
      refs = {
        ...refs,
        pipeline_item_id: pipeline.pipeline_item_id,
      };
    }
  }

  const values = deriveContractValues({
    offer_item: resolved_offer_item,
    route,
    underwriting,
    explicit_purchase_price: purchase_price,
    explicit_emd_amount: emd_amount,
    explicit_closing_timeline: closing_timeline,
    explicit_contract_strategy: contract_strategy,
    notes,
    source_message,
  });

  const generated_contract_id =
    clean(contract_id) ||
    buildContractId({
      offer_item_id: resolved_offer_item_id,
      property_id: refs.property_id,
    });

  const payload = {
    [CONTRACT_FIELDS.title]: generated_contract_id,
    [CONTRACT_FIELDS.contract_id]: generated_contract_id,
    [CONTRACT_FIELDS.contract_status]: normalizeContractStatus(contract_status),
    [CONTRACT_FIELDS.contract_type]: values.contract_type,
    [CONTRACT_FIELDS.template_type]: values.template_type,
    ...(refs.master_owner_id
      ? { [CONTRACT_FIELDS.master_owner]: asAppRef(refs.master_owner_id) }
      : {}),
    ...(refs.prospect_id
      ? { [CONTRACT_FIELDS.prospect]: asAppRef(refs.prospect_id) }
      : {}),
    ...(refs.property_id
      ? { [CONTRACT_FIELDS.property]: asAppRef(refs.property_id) }
      : {}),
    ...(resolved_offer_item_id
      ? { [CONTRACT_FIELDS.offer]: asAppRef(resolved_offer_item_id) }
      : {}),
    ...(refs.phone_item_id
      ? { [CONTRACT_FIELDS.phone]: asAppRef(refs.phone_item_id) }
      : {}),
    ...(refs.email_item_id
      ? { [CONTRACT_FIELDS.email]: asAppRef(refs.email_item_id) }
      : {}),
    ...(refs.conversation_item_id
      ? { [CONTRACT_FIELDS.conversation]: asAppRef(refs.conversation_item_id) }
      : {}),
    ...(refs.assigned_agent_id
      ? { [CONTRACT_FIELDS.assigned_agent]: asAppRef(refs.assigned_agent_id) }
      : {}),
    ...(refs.market_item_id
      ? { [CONTRACT_FIELDS.market]: asAppRef(refs.market_item_id) }
      : {}),
    ...(refs.pipeline_item_id
      ? { [CONTRACT_FIELDS.pipeline]: asAppRef(refs.pipeline_item_id) }
      : {}),
    ...(refs.title_company_item_id
      ? { [CONTRACT_FIELDS.title_company_legacy]: asAppRef(refs.title_company_item_id) }
      : {}),
    ...(values.purchase_price !== null
      ? { [CONTRACT_FIELDS.purchase_price_final]: values.purchase_price }
      : {}),
    ...(values.emd_amount !== null
      ? { [CONTRACT_FIELDS.emd_amount]: values.emd_amount }
      : {}),
    ...(values.closing_timeline_days !== null
      ? { [CONTRACT_FIELDS.closing_timeline_days]: values.closing_timeline_days }
      : {}),
    ...(values.closing_date_target
      ? { [CONTRACT_FIELDS.closing_date_target]: { start: values.closing_date_target } }
      : {}),
    ...(values.creative_terms
      ? { [CONTRACT_FIELDS.creative_terms]: values.creative_terms }
      : {}),
  };

  const created = await createContractItem(payload);
  const pipeline = await syncPipelineState({
    pipeline_item_id: refs.pipeline_item_id || null,
    property_id: refs.property_id,
    master_owner_id: refs.master_owner_id,
    prospect_id: refs.prospect_id,
    conversation_item_id: refs.conversation_item_id,
    offer_item_id: resolved_offer_item_id,
    contract_item_id: created?.item_id || null,
    assigned_agent_id: refs.assigned_agent_id,
    market_id: refs.market_item_id,
    notes: "Contract created from accepted offer.",
  });

  return {
    ok: true,
    created: true,
    reason: "contract_created_from_offer",
    contract_item_id: created?.item_id || null,
    contract_id: generated_contract_id,
    offer_item_id: resolved_offer_item_id,
    pipeline,
    payload,
    raw: created,
  };
}

export default createContractFromOffer;
