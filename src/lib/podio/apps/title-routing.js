import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.title_routing;
export const TITLE_ROUTING_FIELDS = {
  title: "title",
  title_routing_id: "title-routing-id",
  routing_status: "title-file-status",
  contract: "contract-2",
  closing: "closing",
  property: "property-2",
  master_owner: "master-owner",
  prospect: "prospect-2",
  title_company: "title-company",
  market: "market",
  assigned_agent: "assigned-agent",
  file_routed_date: "file-routed-date",
  title_opened_date: "title-opened-date",
  commitment_received_date: "commitment-received-date",
  clear_to_close_date: "clear-to-close-date",
  expected_closing_date: "expected-closing-date",
  preliminary_title_issues: "preliminary-title-issues",
  seller_docs_needed: "seller-docs-needed",
  payoff_needed: "payoff-needed",
  probate_issue: "probate-issue",
  lien_issue: "lien-issue",
  open_permit_issue: "open-permit-issue",
  boundary_legal_issue: "boundary-legal-issue",
  entity_signing_issue: "entity-signing-issue",
  primary_title_contact: "primary-title-contact",
  title_contact_email: "title-contact-email",
  title_contact_phone: "title-contact-phone",
  last_title_update: "last-title-update",
  next_title_follow_up: "next-title-follow-up",
  title_notes: "title-notes",
  internal_notes: "internal-notes",
  resolved: "resolved",
  cancelled_reason: "cancelled-reason",
  final_outcome_notes: "final-outcome-notes",
};

export const createTitleRoutingItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getTitleRoutingItem = (item_id) =>
  getItem(item_id);

export const updateTitleRoutingItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findTitleRoutingItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findTitleRoutingById = (title_routing_id) =>
  findByField(APP_ID, "title-routing-id", title_routing_id);

export default {
  APP_ID,
  TITLE_ROUTING_FIELDS,
  createTitleRoutingItem,
  getTitleRoutingItem,
  updateTitleRoutingItem,
  findTitleRoutingItems,
  findTitleRoutingById,
};
