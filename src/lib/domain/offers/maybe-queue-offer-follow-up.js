// ─── maybe-queue-offer-follow-up.js ──────────────────────────────────────
import { offerFollowUp } from "@/lib/domain/offers/offer-follow-up.js";
import { queueOutboundMessage } from "@/lib/flows/queue-outbound-message.js";

export async function maybeQueueOfferFollowUp({
  offer = null,
  inbound_from = null,
  create_brain_if_missing = false,
} = {}) {
  const follow_up = offerFollowUp({ offer });

  if (!follow_up.ok) {
    return {
      ok: false,
      queued: false,
      reason: follow_up.reason || "offer_follow_up_failed",
    };
  }

  if (!follow_up.should_queue_message) {
    return {
      ok: true,
      queued: false,
      follow_up,
    };
  }

  const queued = await queueOutboundMessage({
    inbound_from,
    create_brain_if_missing,
    use_case: follow_up.recommended_use_case,
    message_type: "Follow-Up",
    send_priority: follow_up.priority,
    queue_status: "Queued",
  });

  return {
    ok: true,
    queued: Boolean(queued?.ok),
    follow_up,
    queued_result: queued,
  };
}

export default maybeQueueOfferFollowUp;
