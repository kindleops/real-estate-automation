import {
  findBrainByPhoneId,
  findBrainItems,
} from "@/lib/podio/apps/ai-conversation-brain.js";

export async function findBrain({ phone_item_id = null, filters = null }) {
  if (phone_item_id) {
    const found = await findBrainByPhoneId(phone_item_id);
    if (found) return found;
  }

  if (filters) {
    const res = await findBrainItems(filters, 1, 0);
    return res?.items?.[0] ?? res?.[0] ?? null;
  }

  return null;
}

export default findBrain;