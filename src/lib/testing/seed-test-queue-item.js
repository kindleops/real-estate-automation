import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";
import { processSendQueue } from "@/lib/domain/queue/process-send-queue.js";

function clean(value) {
  return String(value ?? "").trim();
}

export async function seedTestQueueItem({
  phone = "",
  use_case = null,
  language = null,
  send_now = false,
} = {}) {
  const queued = await queueOutboundMessage({
    inbound_from: clean(phone),
    use_case,
    language,
  });

  const queue_item_id = queued?.queue_item_id || null;

  if (!send_now) {
    return {
      ok: Boolean(queued?.ok),
      queued,
      processed: null,
    };
  }

  if (!queue_item_id) {
    return {
      ok: false,
      queued,
      processed: {
        ok: false,
        sent: false,
        reason: "missing_queue_item_id_after_queue",
      },
    };
  }

  const processed = await processSendQueue({
    queue_item_id,
  });

  return {
    ok: Boolean(queued?.ok && processed?.ok),
    queued,
    processed,
  };
}

export default seedTestQueueItem;
