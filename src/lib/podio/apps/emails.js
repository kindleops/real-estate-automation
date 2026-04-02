import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.emails;

export const createEmailItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getEmailItem = (item_id) =>
  getItem(item_id);

export const updateEmailItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findEmailItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset });

export const findEmailByEmail = (email) =>
  findByField(APP_ID, "email", email);

export default {
  APP_ID,
  createEmailItem,
  getEmailItem,
  updateEmailItem,
  findEmailItems,
  findEmailByEmail,
};