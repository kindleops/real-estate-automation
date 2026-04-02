import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  filterAppItemsByView,
  findByField,
  getAppViews,
  getAppView,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.master_owners;

export const MASTER_OWNER_FIELDS = {
  seller_id: "seller-id",
  owner_full_name: "owner-full-name",
  markets: "markets",
  sms_eligible: "sms-elgible",
  contact_status: "contact-status",
  contact_status_2: "contact-status-2",
  best_phone_1: "best-phone-1",
  best_phone_2: "best-phone-2",
  best_phone_3: "best-phone-3",
  timezone: "timezone",
  best_contact_window: "best-contact-window",
  last_outbound: "last-outbound",
  last_inbound: "last-inbound",
  last_contacted_at: "last-contacted-at",
  next_follow_up_at: "next-follow-up-at",
  master_owner_priority_score: "master-owner-priority-score",
  priority_tier: "priority-tier",
  contactability_score: "contactability-score",
  financial_pressure_score: "financial-pressure-score",
  urgency_score: "urgency-score",
  portfolio_tax_delinquent_count: "portfolio-tax-delinquent-count",
  portfolio_lien_count: "portfolio-lien-count",
  portfolio_property_count: "portfolio-property-count",
  property_type_majority: "property-type-majority",
  language_primary: "language-primary",
  outbound_number: "outbound-number",
  linked_conversations: "linked-conversations",
  sms_agent: "sms-agent",
  assigned_agent: "assigned-agent",
  follow_up_cadence: "follow-up-cadence",
  message_variant_seed: "message-variant-seed",
  offer: "offer",
  contract: "contract",
  closing: "closing",
};

export const createMasterOwnerItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getMasterOwnerItem = (item_id) =>
  getItem(item_id);

export const updateMasterOwnerItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findMasterOwnerItems = (filters = {}, limitOrOptions = 30, offset = 0) => {
  if (typeof limitOrOptions === "number") {
    return filterAppItems(APP_ID, filters, {
      limit: limitOrOptions,
      offset,
    });
  }

  return filterAppItems(APP_ID, filters, limitOrOptions || {});
};

export const findMasterOwnerItemsByView = (view_id, limitOrOptions = 30, offset = 0) => {
  if (typeof limitOrOptions === "number") {
    return filterAppItemsByView(APP_ID, view_id, {
      limit: limitOrOptions,
      offset,
    });
  }

  return filterAppItemsByView(APP_ID, view_id, limitOrOptions || {});
};

export const listMasterOwnerViews = (options = {}) =>
  getAppViews(APP_ID, options);

export const getMasterOwnerView = (view_id_or_name) =>
  getAppView(APP_ID, view_id_or_name);

export const findMasterOwnerBySellerId = (seller_id) =>
  findByField(APP_ID, MASTER_OWNER_FIELDS.seller_id, seller_id);

export const findSmsEligibleMasterOwnerItems = ({
  limit = 30,
  offset = 0,
  sort_by = MASTER_OWNER_FIELDS.master_owner_priority_score,
  sort_desc = true,
} = {}) =>
  filterAppItems(
    APP_ID,
    { [MASTER_OWNER_FIELDS.sms_eligible]: "Yes" },
    {
      limit,
      offset,
      ...(sort_by ? { sort_by, sort_desc } : {}),
    }
  );

export default {
  APP_ID,
  MASTER_OWNER_FIELDS,
  createMasterOwnerItem,
  getMasterOwnerItem,
  updateMasterOwnerItem,
  findMasterOwnerItems,
  findMasterOwnerItemsByView,
  findMasterOwnerBySellerId,
  findSmsEligibleMasterOwnerItems,
  listMasterOwnerViews,
  getMasterOwnerView,
};
