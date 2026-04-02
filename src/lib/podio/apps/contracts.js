import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.contracts;
export const CONTRACT_FIELDS = {
  title: "title",
  contract_id: "contract-id",
  contract_version: "contract-version",
  contract_status: "category",
  contract_type: "contract-type",
  state: "state",
  template_type: "template-type",
  offer: "offer",
  property: "property",
  master_owner: "master-owner",
  prospect: "prospect",
  phone: "phone",
  email: "email",
  conversation: "conversation",
  assigned_agent: "assigned-agent",
  title_company: "title-company",
  title_company_legacy: "title-company-2",
  market: "market",
  purchase_price_final: "purchase-price-final",
  emd_amount: "money",
  closing_date_target: "date",
  closing_timeline_days: "number",
  assignment_allowed: "assignment-allowed",
  inspection_period_days: "inspection-period-days",
  creative_terms: "creative-terms",
  contract_document: "contract-document",
  docusign_envelope_id: "docusign-envelope-id",
  docusign_signing_link: "docusign-signing-link",
  contract_sent_timestamp: "contract-sent-timestamp",
  contract_viewed_timestamp: "contract-viewed-timestamp",
  seller_signed_timestamp: "seller-signed-timestamp",
  buyer_signed_timestamp: "buyer-signed-timestamp",
  fully_executed_timestamp: "title-opened-timestamp",
  title_routing: "title-routing",
  buyer_match: "buyer-match",
  pipeline: "pipeline",
};

export const createContractItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getContractItem = (item_id) =>
  getItem(item_id);

export const updateContractItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findContractItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findContractByContractId = (contract_id) =>
  findByField(APP_ID, "contract-id", contract_id);

export default {
  APP_ID,
  CONTRACT_FIELDS,
  createContractItem,
  getContractItem,
  updateContractItem,
  findContractItems,
  findContractByContractId,
};
