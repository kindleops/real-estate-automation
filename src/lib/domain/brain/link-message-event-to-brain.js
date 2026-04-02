export async function linkMessageEventToBrain({
  brain_item = null,
  brain_id = null,
  message_event_id = null,
} = {}) {
  const resolved_brain_id = brain_id || brain_item?.item_id || null;

  if (!resolved_brain_id) {
    return {
      ok: false,
      reason: "missing_brain_id",
    };
  }

  if (!message_event_id) {
    return {
      ok: false,
      reason: "missing_message_event_id",
      brain_id: resolved_brain_id,
    };
  }

  return {
    ok: true,
    skipped: true,
    reason: "brain_schema_has_no_message_event_link_field",
    brain_id: resolved_brain_id,
    message_event_id,
  };
}

export default linkMessageEventToBrain;
