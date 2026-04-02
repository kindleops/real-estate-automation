import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.deal_revenue;
export const DEAL_REVENUE_FIELDS = {
  revenue_id: "title",
  contract: "contract",
  closing: "closing",
  property: "property",
  master_owner: "master-owner",
  buyer: "buyer",
  title_company: "title-company",
  market: "market",
  purchase_price: "purchase-price",
  sold_price: "sold-price",
  assignment_fee: "assignment-fee",
  expected_wire_date: "expected-wire-date",
  wire_received: "wire-received",
  wire_received_date: "wire-received-date",
  wire_received_amount: "wire-received-amount",
  partial_payment: "partial-payment",
  remaining_balance: "remaining-balance",
  revenue_status: "revenue-status",
  account_wired_to: "account-wired-to",
  wire_confirmation_number: "wire-confirmation-number",
};

export const createDealRevenueItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getDealRevenueItem = (item_id) =>
  getItem(item_id);

export const updateDealRevenueItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findDealRevenueItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findDealRevenueById = (deal_revenue_id) =>
  findByField(APP_ID, DEAL_REVENUE_FIELDS.revenue_id, deal_revenue_id);

export default {
  APP_ID,
  DEAL_REVENUE_FIELDS,
  createDealRevenueItem,
  getDealRevenueItem,
  updateDealRevenueItem,
  findDealRevenueItems,
  findDealRevenueById,
};
