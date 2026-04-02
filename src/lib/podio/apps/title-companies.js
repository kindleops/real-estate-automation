import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.title_companies;
export const TITLE_COMPANY_FIELDS = {
  title: "title",
  market: "market",
  rating: "rating",
  address: "address",
  contact_manager: "contact-manager",
  new_order_email: "new-order-email",
  phone: "phone",
  underwriter: "underwriter",
  notes: "notes",
};

export const createTitleCompanyItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getTitleCompanyItem = (item_id) =>
  getItem(item_id);

export const updateTitleCompanyItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findTitleCompanyItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findTitleCompanyByName = (title) =>
  findByField(APP_ID, "title", title);

export default {
  APP_ID,
  TITLE_COMPANY_FIELDS,
  createTitleCompanyItem,
  getTitleCompanyItem,
  updateTitleCompanyItem,
  findTitleCompanyItems,
  findTitleCompanyByName,
};
