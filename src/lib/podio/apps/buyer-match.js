import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.buyer_match;

export const BUYER_MATCH_FIELDS = {
  title: "title",
  buyer_match_id: "buyer-match-id",
  match_status: "match-status",
  disposition_strategy: "disposition-strategy",
  pipeline: "pipe",
  contract: "contract",
  property: "property",
  master_owner: "master-owner",
  offer: "offer",
  closing: "closing",
  deal_revenue: "deal-revenue",
  market: "market-2",
  primary_buyer: "primary-buyer",
  backup_buyer_1: "backup-buyer-1",
  backup_buyer_2: "backup-buyer-2",
  property_profile: "property-profile",
  buyer_type_match: "buyer-type-match",
  buyer_match_score: "buyer-match-score",
  reason_for_match: "reason-for-match",
  package_sent_date: "package-sent-date",
  buyer_response_status: "buyer-response-status",
  buyer_offered_price: "buyer-offered-price",
  buyer_notes: "buyer-notes",
  buyer_proof_of_funds_received: "buyer-proof-of-funds-received",
  buyer_emd_ready: "buyer-emd-ready",
  assignment_fee: "assignment-fee",
  final_acquisition_price: "final-acquisition-price",
  final_disposition_price: "final-disposition-price",
  assignment_status: "assignment-status",
  selected_buyer: "selected-buyer",
  buyer_assigned_date: "buyer-assigned-date",
  buyer_match_start_date: "buyer-match-start-date",
  next_buyer_follow_up: "next-buyer-follow-up",
  urgency_level: "urgency-level",
  automation_status: "automation-status",
  dispo_outcome: "dispo-outcome",
  internal_notes: "internal-notes",
  ai_buyer_match_summary: "ai-buyer-match-summary",
};

export const createBuyerMatchItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getBuyerMatchItem = (item_id) =>
  getItem(item_id);

export const updateBuyerMatchItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findBuyerMatchItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findBuyerMatchById = (buyer_match_id) =>
  findByField(APP_ID, BUYER_MATCH_FIELDS.buyer_match_id, buyer_match_id);

export async function findLatestBuyerMatchByContractId(contract_item_id) {
  if (!contract_item_id) return null;
  const matches = await findBuyerMatchItems(
    { [BUYER_MATCH_FIELDS.contract]: contract_item_id },
    50,
    0
  );
  return [...matches].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0))[0] || null;
}

export async function findLatestBuyerMatchByClosingId(closing_item_id) {
  if (!closing_item_id) return null;
  const matches = await findBuyerMatchItems(
    { [BUYER_MATCH_FIELDS.closing]: closing_item_id },
    50,
    0
  );
  return [...matches].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0))[0] || null;
}

export async function findLatestBuyerMatchByPropertyId(property_item_id) {
  if (!property_item_id) return null;
  const matches = await findBuyerMatchItems(
    { [BUYER_MATCH_FIELDS.property]: property_item_id },
    50,
    0
  );
  return [...matches].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0))[0] || null;
}

export default {
  APP_ID,
  BUYER_MATCH_FIELDS,
  createBuyerMatchItem,
  getBuyerMatchItem,
  updateBuyerMatchItem,
  findBuyerMatchItems,
  findBuyerMatchById,
  findLatestBuyerMatchByContractId,
  findLatestBuyerMatchByClosingId,
  findLatestBuyerMatchByPropertyId,
};
