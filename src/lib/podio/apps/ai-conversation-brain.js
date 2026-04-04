import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.ai_conversation_brain;

export const BRAIN_FIELDS = {
  phone_number: "phone-number",
  prospect: "prospect",
  master_owner: "master-owner",
  properties: "properties",
  linked_message_events: "linked-message-events",
  ai_agent_assigned: "ai-agent-assigned",
  sms_agent: "sms-agent",
  last_template_sent: "last-template-sent",
  last_sent_time: "last-sent-time",
  conversation_stage: "conversation-stage",
  ai_route: "ai-route",
  seller_profile: "seller-profile",
  language_preference: "language-preference",
  status_ai_managed: "status-ai-managed",
  follow_up_trigger_state: "follow-up-trigger-state",
  ai_next_message: "ai-next-message",
  last_outbound_message: "last-outbound-message",
  last_inbound_message: "last-inbound-message",
  last_contact_timestamp: "last-contact-timestamp",
};

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_id = Number(a?.item_id || 0);
    const b_id = Number(b?.item_id || 0);
    return b_id - a_id;
  });
}

export async function createBrainItem(fields = {}) {
  return createItem(APP_ID, fields);
}

export async function getBrainItem(item_id) {
  return getItem(item_id);
}

export async function updateBrainItem(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

export async function findBrainItems(filters = {}, limit = 30, offset = 0) {
  return filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );
}

export async function findBrainByPhoneId(phone_item_id) {
  if (!phone_item_id) return null;
  return findByField(APP_ID, BRAIN_FIELDS.phone_number, phone_item_id);
}

export async function findLatestBrainByProspectId(prospect_id) {
  if (!prospect_id) return null;

  const items = await findBrainItems(
    { [BRAIN_FIELDS.prospect]: prospect_id },
    50,
    0
  );

  return sortNewestFirst(items)[0] || null;
}

export async function findLatestBrainByMasterOwnerId(master_owner_id) {
  if (!master_owner_id) return null;

  const items = await findBrainItems(
    { [BRAIN_FIELDS.master_owner]: master_owner_id },
    50,
    0
  );

  return sortNewestFirst(items)[0] || null;
}

export async function findBestBrainMatch({
  phone_item_id = null,
  prospect_id = null,
  master_owner_id = null,
} = {}) {
  const by_phone = await findBrainByPhoneId(phone_item_id);
  if (by_phone) return by_phone;

  const by_prospect = await findLatestBrainByProspectId(prospect_id);
  if (by_prospect) return by_prospect;

  const by_master_owner = await findLatestBrainByMasterOwnerId(master_owner_id);
  if (by_master_owner) return by_master_owner;

  return null;
}

export default {
  APP_ID,
  BRAIN_FIELDS,
  createBrainItem,
  getBrainItem,
  updateBrainItem,
  findBrainItems,
  findBrainByPhoneId,
  findLatestBrainByProspectId,
  findLatestBrainByMasterOwnerId,
  findBestBrainMatch,
};
