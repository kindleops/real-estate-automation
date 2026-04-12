import APP_IDS from "@/lib/config/app-ids.js";
import { getAppReferenceIds } from "@/lib/providers/podio.js";
import { getAttachedFieldSchema } from "@/lib/podio/schema.js";
import {
  applyBrainStateUpdate,
  buildLinkedMessageEventsFields,
} from "@/lib/domain/brain/brain-authority.js";
import {
  BRAIN_FIELDS,
  getBrainItem,
} from "@/lib/podio/apps/ai-conversation-brain.js";

const defaultDeps = {
  getAppReferenceIds,
  getAttachedFieldSchema,
  getBrainItem,
  applyBrainStateUpdate,
};

let runtimeDeps = { ...defaultDeps };

export function __setLinkMessageEventToBrainTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetLinkMessageEventToBrainTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function toId(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

export async function linkMessageEventToBrain({
  brain_item = null,
  brain_id = null,
  message_event_id = null,
} = {}) {
  const resolved_brain_id = toId(brain_id || brain_item?.item_id || null);
  const resolved_message_event_id = toId(message_event_id);

  if (!resolved_brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
    };
  }

  if (!resolved_message_event_id) {
    return {
      ok: false,
      reason: "missing_message_event_id",
      brain_id: resolved_brain_id,
    };
  }

  const link_field_schema = runtimeDeps.getAttachedFieldSchema(
    APP_IDS.ai_conversation_brain,
    BRAIN_FIELDS.linked_message_events
  );

  if (!link_field_schema) {
    return {
      ok: true,
      skipped: true,
      reason: "brain_schema_has_no_message_event_link_field",
      brain_id: resolved_brain_id,
      message_event_id: resolved_message_event_id,
    };
  }

  const current_brain_item = await runtimeDeps.getBrainItem(resolved_brain_id);

  if (!current_brain_item?.item_id) {
    return {
      ok: false,
      reason: "brain_not_found",
      brain_id: resolved_brain_id,
      message_event_id: resolved_message_event_id,
    };
  }

  const existing_message_event_ids = runtimeDeps.getAppReferenceIds(
    current_brain_item,
    BRAIN_FIELDS.linked_message_events
  )
    .map((id) => toId(id))
    .filter(Boolean);

  if (existing_message_event_ids.includes(resolved_message_event_id)) {
    return {
      ok: true,
      skipped: true,
      reason: "message_event_already_linked",
      brain_id: resolved_brain_id,
      message_event_id: resolved_message_event_id,
    };
  }

  const next_message_event_ids = [
    ...existing_message_event_ids,
    resolved_message_event_id,
  ];

  await runtimeDeps.applyBrainStateUpdate({
    brain_id: resolved_brain_id,
    reason: "message_event_linked",
    fields: buildLinkedMessageEventsFields({
      current_message_event_ids: existing_message_event_ids,
      message_event_id: resolved_message_event_id,
    }),
  });

  return {
    ok: true,
    linked: true,
    brain_id: resolved_brain_id,
    message_event_id: resolved_message_event_id,
    linked_message_event_ids: next_message_event_ids,
  };
}

export default linkMessageEventToBrain;
