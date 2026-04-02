import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.underwriting;
export const UNDERWRITING_FIELDS = {
  title: "title",
  underwriting_id: "underwriting-id",
  underwriting_type: "underwriting-type",
  underwriting_status: "underwriting-status",
  property: "property-2",
  master_owner: "master-owner",
  prospect: "prospect",
  conversation: "conversation",
  phone_number: "phone-number",
  offer: "offer",
  market: "market",
  reason_sent_to_underwriting: "reason-sent-to-underwriting",
  seller_asking_price: "seller-asking-price",
  seller_counter_offer: "seller-counter-offer",
  escalation_summary: "escalation-summary",
  creative_strategy: "creative-strategy",
  purchase_price: "purchase-price",
  down_payment: "down-payment",
  monthly_payment: "monthly-payment",
  interest_rate: "interest-rate",
  loan_terms_months: "loan-terms-months",
  balloon_payment: "balloon-payment",
  existing_mortgage_balance: "existing-mortgage-balance",
  existing_mortgage_payment: "existing-mortgage-payment",
  estimated_payoff: "estimated-payoff",
  creative_terms_summary: "creative-terms-summary",
  number_of_units_snapshot: "number-of-units-snapshot",
  occupancy_at_underwriting: "occupancy-at-underwriting",
  current_gross_rents: "current-gross-rents",
  estimated_expenses: "estimated-expenses",
  noi: "noi",
  cap_rate: "cap-rate",
  mf_exit_strategy: "mf-exit-strategy",
  mf_summary: "mf-summary",
  novation_list_price: "novation-list-price",
  target_net_to_seller: "target-net-to-seller",
  our_estimated_spread: "our-estimated-spread",
  estimated_repair_scope: "estimated-repair-scope",
  estimated_repair_cost: "estimated-repair-cost",
  estimated_days_to_sell: "estimated-days-to-sell",
  mls_target_date: "mls-target-date",
  novation_summary: "novation-summary",
  ai_recommended_strategy: "ai-recommeneded-strategy",
  ai_recommended_next_move: "ai-recommended-next-move",
  ai_risk_summary: "ai-risk-summary",
  ai_offer_terms_justification: "ai-offer-terms-justification",
  ai_confidence_score: "ai-confidence-score",
  automation_result: "automation-result",
  rejection_failure_reason: "rejection-failure-reason",
  automation_status: "automation-status",
  current_engine_step: "current-engine-step",
  triggered_at: "triggered-at",
  completed_at: "completed-at",
  sent_to_offers_date: "sent-to-offers-date",
  retry_count: "retry-count",
};

export const createUnderwritingItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getUnderwritingItem = (item_id) =>
  getItem(item_id);

export const updateUnderwritingItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findUnderwritingItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findUnderwritingById = (underwriting_id) =>
  findByField(APP_ID, UNDERWRITING_FIELDS.underwriting_id, underwriting_id);

export default {
  APP_ID,
  UNDERWRITING_FIELDS,
  createUnderwritingItem,
  getUnderwritingItem,
  updateUnderwritingItem,
  findUnderwritingItems,
  findUnderwritingById,
};
