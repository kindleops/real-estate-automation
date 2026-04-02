import APP_IDS from "@/lib/config/app-ids.js";
import {
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.agents;

export const getAgentItem = (item_id) => getItem(item_id);

export const updateAgentItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findAgents = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset });

export const findAgentByTitle = (title) =>
  findByField(APP_ID, "title", title);

export default {
  APP_ID,
  getAgentItem,
  updateAgentItem,
  findAgents,
  findAgentByTitle,
};