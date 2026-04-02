import APP_IDS from "@/lib/config/app-ids.js";
import {
  filterAppItems,
  getItem,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.buyer_officers;

export const BUYER_OFFICER_FIELDS = {
  company_id: "seller-id",
  owner_full_name: "owner-full-name",
  owner_type: "owner-type",
  officer_first_name: "title",
  officer_last_name: "owner-last-name",
  contact_name: "name-of-contact",
  contact_order_score: "contact-order-score",
  contact_tags: "contact-matching-tags",
  contact_address: "tax-mailing-address",
};

export const getBuyerOfficerItem = (item_id) =>
  getItem(item_id);

export const findBuyerOfficerItems = (filters = {}, limit = 50, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export default {
  APP_ID,
  BUYER_OFFICER_FIELDS,
  getBuyerOfficerItem,
  findBuyerOfficerItems,
};
