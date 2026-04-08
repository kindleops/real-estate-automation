import APP_IDS from "@/lib/config/app-ids.js";
import {
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.textgrid_numbers;
export const TEXTGRID_NUMBER_FIELDS = {
  title: "title",
  friendly_name: "friendly-name",
  market: "market",
  status: "status",
  rotation_weight: "rotation-weight-1-10",
  hard_pause: "hard-pause",
  pause_reason: "pause-reason",
  pause_until: "pause-until",
  sent_today: "sent-today",
  sent_last_hour: "sent-last-hour",
  daily_send_cap: "daily-send-cap",
  hourly_send_cap: "hourly-send-cap",
  risk_spike_flag: "risk-spike-flag",
  last_used_at: "last-used-at",
  allowed_send_window_start_local: "allowed-send-window-start-local",
  allowed_send_window_end_local: "allowed-send-window-end-local",
  markets: "markets",
};

export async function getTextgridNumberItem(item_id) {
  return getItem(item_id);
}

export async function updateTextgridNumberItem(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

export async function findTextgridNumbers(filters = {}, limit = 30, offset = 0) {
  return filterAppItems(APP_ID, filters, { limit, offset });
}

export async function findTextgridNumberByTitle(title) {
  return findByField(APP_ID, TEXTGRID_NUMBER_FIELDS.title, title);
}

export default {
  APP_ID,
  TEXTGRID_NUMBER_FIELDS,
  getTextgridNumberItem,
  updateTextgridNumberItem,
  findTextgridNumbers,
  findTextgridNumberByTitle,
};
