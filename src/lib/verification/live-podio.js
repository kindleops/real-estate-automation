import crypto from "node:crypto";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

export async function runLivePodioRoundtripVerification({
  note = "",
  delete_after = true,
  confirm_live = false,
} = {}) {
  if (!confirm_live) {
    return {
      ok: false,
      reason: "confirm_live_required",
    };
  }

  const [{ deleteItem }, messageEvents] = await Promise.all([
    import("@/lib/providers/podio.js"),
    import("@/lib/podio/apps/message-events.js"),
  ]);
  const {
    createMessageEvent,
    getMessageEvent,
    updateMessageEvent,
  } = messageEvents;

  const run_id = `podio-live-${crypto.randomUUID()}`;
  const message_id = `verification:podio:${run_id}`;
  const trigger_name = `verification-podio-roundtrip:${run_id}`;
  const base_message =
    clean(note) || "Podio live verification roundtrip";

  const created = await createMessageEvent({
    "message-id": message_id,
    "timestamp": { start: nowIso() },
    "trigger-name": trigger_name,
    "direction": "System",
    "source-app": "Internal Verification",
    "processed-by": "Verification Harness",
    "message": base_message,
    "ai-output": JSON.stringify({
      verification_run_id: run_id,
      step: "created",
    }),
  });

  const created_item = await getMessageEvent(created?.item_id);

  await updateMessageEvent(created?.item_id, {
    "message": `${base_message} [updated]`,
    "ai-output": JSON.stringify({
      verification_run_id: run_id,
      step: "updated",
    }),
  });

  const updated_item = await getMessageEvent(created?.item_id);

  let deleted = false;
  if (delete_after && created?.item_id) {
    await deleteItem(created.item_id);
    deleted = true;
  }

  return {
    ok: Boolean(created?.item_id),
    run_id,
    message_id,
    trigger_name,
    created_item_id: created?.item_id || null,
    created_message: created_item?.item_id ? base_message : null,
    updated_message: updated_item?.item_id ? `${base_message} [updated]` : null,
    deleted,
  };
}

export default runLivePodioRoundtripVerification;
