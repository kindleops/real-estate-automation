import APP_IDS from "@/lib/config/app-ids.js";
import {
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.contract_templates;

export const getContractTemplateItem = (item_id) =>
  getItem(item_id);

export const updateContractTemplateItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findContractTemplates = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset });

export const findContractTemplateByTitle = (title) =>
  findByField(APP_ID, "title", title);

export default {
  APP_ID,
  getContractTemplateItem,
  updateContractTemplateItem,
  findContractTemplates,
  findContractTemplateByTitle,
};