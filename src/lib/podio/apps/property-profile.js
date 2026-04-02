import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.property_profile;

export const createPropertyProfileItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getPropertyProfileItem = (item_id) =>
  getItem(item_id);

export const updatePropertyProfileItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findPropertyProfileItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset });

export const findPropertyProfileByTitle = (title) =>
  findByField(APP_ID, "title", title);

export default {
  APP_ID,
  createPropertyProfileItem,
  getPropertyProfileItem,
  updatePropertyProfileItem,
  findPropertyProfileItems,
  findPropertyProfileByTitle,
};
