// ─── resolve-brain.js ────────────────────────────────────────────────────
import {
  getFirstMatchingItem,
  getItem,
} from "@/lib/providers/podio.js";

import APP_IDS from "@/lib/config/app-ids.js";
import {
  BRAIN_FIELDS,
  createBrainItem,
  findBrainByPhoneId,
} from "@/lib/podio/apps/ai-conversation-brain.js";
import { toPodioDateField } from "@/lib/utils/dates.js";

// TODO(@eng): Re-enable once AI Conversation Brain schema is migrated off
// the legacy acquisitions phone app to the live Communications Engine
// phone app (ticket: ENG-412).
const BRAIN_PHONE_LINK_ENABLED = false;

async function safeGetItem(item_id) {
  if (!item_id) return null;

  try {
    return await getItem(item_id);
  } catch {
    return null;
  }
}

export async function findBrainFallback({
  prospect_id = null,
  master_owner_id = null,
} = {}) {
  if (prospect_id) {
    const hit = await getFirstMatchingItem(
      APP_IDS.ai_conversation_brain,
      { prospect: prospect_id },
      { sort_desc: true }
    );

    if (hit) return hit;
  }

  if (master_owner_id) {
    const hit = await getFirstMatchingItem(
      APP_IDS.ai_conversation_brain,
      { "master-owner": master_owner_id },
      { sort_desc: true }
    );

    if (hit) return hit;
  }

  return null;
}

export async function resolveBrain({
  phone_item_id = null,
  prospect_id = null,
  master_owner_id = null,
} = {}) {
  return (
    (BRAIN_PHONE_LINK_ENABLED
      ? await findBrainByPhoneId(phone_item_id)
      : null) ||
    (await findBrainFallback({ prospect_id, master_owner_id }))
  );
}

export async function createBrain({
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  phone_item_id = null,
  logger = null,
} = {}) {
  const fields = {
    [BRAIN_FIELDS.master_owner]: master_owner_id,
    [BRAIN_FIELDS.prospect]: prospect_id,
    [BRAIN_FIELDS.conversation_stage]: "Ownership",
    [BRAIN_FIELDS.language_preference]: "English",
    [BRAIN_FIELDS.status_ai_managed]: "_ Warm Lead",
    [BRAIN_FIELDS.follow_up_trigger_state]: "Waiting",
    [BRAIN_FIELDS.last_contact_timestamp]: toPodioDateField(new Date()),
    ...(property_id ? { [BRAIN_FIELDS.properties]: [property_id] } : {}),
    ...(BRAIN_PHONE_LINK_ENABLED && phone_item_id
      ? { [BRAIN_FIELDS.phone_number]: phone_item_id }
      : {}),
  };

  const created = await createBrainItem(fields);
  const created_id = created?.item_id ?? null;

  if (logger?.info) {
    logger.info("context.brain_created", {
      created_item_id: created_id,
    });
  }

  const brain_item =
    (created_id ? await safeGetItem(created_id) : null) ||
    (await findBrainFallback({ prospect_id, master_owner_id }));

  if (!brain_item && logger?.warn) {
    logger.warn("context.brain_create_refetch_failed", {
      created_item_id: created_id,
      master_owner_id,
      prospect_id,
    });
  }

  return brain_item;
}

export default resolveBrain;
