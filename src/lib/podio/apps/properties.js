import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.properties;

export const createPropertyItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getPropertyItem = (item_id) =>
  getItem(item_id);

export const updatePropertyItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findPropertyItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset });

export const findPropertyByPropertyId = (property_id) =>
  findByField(APP_ID, "property-id", property_id);

export default {
  APP_ID,
  createPropertyItem,
  getPropertyItem,
  updatePropertyItem,
  findPropertyItems,
  findPropertyByPropertyId,
};