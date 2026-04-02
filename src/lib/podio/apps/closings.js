import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.closings;
export const CLOSING_FIELDS = {
  closing_id: "title",
  closing_title: "closing-title",
  closing_status: "closing-status",
  contract: "contract",
  property: "property",
  master_owner: "master-owner",
  prospect: "prospect",
  title_routing: "title-routing",
  buyer_match: "buyer-match",
  market: "market",
  title_company: "title-company",
  closing_date_time: "closing-date-time",
  confirmed_date: "confirmed-date",
  rescheduled_date: "rescheduled-date",
  timezone: "timezone",
  ready_to_close: "ready-to-close",
  docs_complete: "docs-complete",
  funds_ready: "funds-ready",
  outstanding_items: "outstanding-items",
  pre_close_notes: "pre-close-notes",
  actual_closing_date: "actual-closing-date",
  closed_successfully: "closed-successfully",
  closer_delay_reasons: "closer-delay-reasons",
  post_close_notes: "post-close-notes",
};

export const createClosingItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getClosingItem = (item_id) =>
  getItem(item_id);

export const updateClosingItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findClosingItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findClosingById = (closing_id) =>
  findByField(APP_ID, CLOSING_FIELDS.closing_id, closing_id);

export default {
  APP_ID,
  CLOSING_FIELDS,
  createClosingItem,
  getClosingItem,
  updateClosingItem,
  findClosingItems,
  findClosingById,
};
