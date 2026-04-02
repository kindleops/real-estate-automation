import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.prospects;

export const createProspectItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getProspectItem = (item_id) =>
  getItem(item_id);

export const updateProspectItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findProspectItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset });

export const findProspectBySellerId = (seller_id) =>
  findByField(APP_ID, "seller-id", seller_id);

export default {
  APP_ID,
  createProspectItem,
  getProspectItem,
  updateProspectItem,
  findProspectItems,
  findProspectBySellerId,
};