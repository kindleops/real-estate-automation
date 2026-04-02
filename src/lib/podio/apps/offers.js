import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
  getCategoryValue,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.offers;

export const OFFER_FIELDS = {
  title: "title",
  offer_id: "offer-id",
  offer_status: "offer-status",
  master_owner: "relationship",
  property: "property",
  offer_type: "offer-type",
  assigned_agent: "assigned-agent",
  offer_date: "offer-date",
  offer_expiration_date: "offer-expiration-date-2",
  follow_up_date: "follow-up-window",
  market: "relationship-2",
  prospect: "prospect",
  phone_number: "phone-number",
  email_address: "email-address",
  conversation: "conversation",
  offer_sent_price: "offer-sent-price-2",
  seller_asking_price: "seller-asking-price-3",
  seller_counter_offer: "seller-counter-offer-3",
  accepted_date: "accepted-date",
  rejected_date: "rejected-date",
  converted_to_contract: "converted-to-contract",
  under_contract_date: "under-contract-date",
  closing_date_target: "closing-date-target",
  contract: "contract",
  deal_killed_reason: "deal-killed-reason",
  notes: "notes",
};

export const OPEN_STATUSES = new Set([
  "Offer Sent",
  "Viewed",
  "Counter Received",
  "Revised Offer Sent",
  "Negotiating",
]);

function clean(value) {
  return String(value ?? "").trim();
}

export function normalizeOfferStatus(value = "") {
  const raw = clean(value).toLowerCase();

  if (["draft", "approved", "sent", "offer sent"].includes(raw)) return "Offer Sent";
  if (raw === "viewed") return "Viewed";
  if (["countered", "counter received"].includes(raw)) return "Counter Received";
  if (["revised", "revised sent", "revised offer sent"].includes(raw)) {
    return "Revised Offer Sent";
  }
  if (raw === "negotiating") return "Negotiating";
  if (["accepted", "accepted ready for contract"].includes(raw)) {
    return "Accepted (Ready for Contract)";
  }
  if (raw === "rejected") return "Rejected";
  if (raw === "expired") return "Expired";

  return clean(value);
}

export function isOpenOfferStatus(status) {
  return OPEN_STATUSES.has(normalizeOfferStatus(status));
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

function filterOpenOffers(items = []) {
  return items.filter((item) =>
    isOpenOfferStatus(getCategoryValue(item, OFFER_FIELDS.offer_status, ""))
  );
}

export const createOfferItem = (fields = {}) => createItem(APP_ID, fields);

export const getOfferItem = (item_id) => getItem(item_id);

export const updateOfferItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findOfferItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findOfferByOfferId = (offer_id) =>
  findByField(APP_ID, OFFER_FIELDS.offer_id, offer_id);

export async function findLatestOpenOfferByProspectId(prospect_id) {
  if (!prospect_id) return null;

  const items = await findOfferItems(
    { [OFFER_FIELDS.prospect]: prospect_id },
    50,
    0
  );

  return sortNewestFirst(filterOpenOffers(items))[0] || null;
}

export async function findLatestOpenOfferByMasterOwnerId(master_owner_id) {
  if (!master_owner_id) return null;

  const items = await findOfferItems(
    { [OFFER_FIELDS.master_owner]: master_owner_id },
    50,
    0
  );

  return sortNewestFirst(filterOpenOffers(items))[0] || null;
}

export async function findLatestOpenOfferByPropertyId(property_id) {
  if (!property_id) return null;

  const items = await findOfferItems(
    { [OFFER_FIELDS.property]: property_id },
    50,
    0
  );

  return sortNewestFirst(filterOpenOffers(items))[0] || null;
}

export async function findLatestOpenOffer({
  offer_id = null,
  prospect_id = null,
  master_owner_id = null,
  property_id = null,
} = {}) {
  if (offer_id) {
    const direct = await findOfferByOfferId(offer_id);
    if (direct && isOpenOfferStatus(getCategoryValue(direct, OFFER_FIELDS.offer_status, ""))) {
      return direct;
    }
  }

  const by_prospect = await findLatestOpenOfferByProspectId(prospect_id);
  if (by_prospect) return by_prospect;

  const by_master_owner = await findLatestOpenOfferByMasterOwnerId(master_owner_id);
  if (by_master_owner) return by_master_owner;

  const by_property = await findLatestOpenOfferByPropertyId(property_id);
  if (by_property) return by_property;

  return null;
}

export default {
  APP_ID,
  OFFER_FIELDS,
  OPEN_STATUSES,
  isOpenOfferStatus,
  createOfferItem,
  getOfferItem,
  updateOfferItem,
  findOfferItems,
  findOfferByOfferId,
  findLatestOpenOfferByProspectId,
  findLatestOpenOfferByMasterOwnerId,
  findLatestOpenOfferByPropertyId,
  findLatestOpenOffer,
};
