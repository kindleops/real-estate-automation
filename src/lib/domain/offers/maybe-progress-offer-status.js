// ─── maybe-progress-offer-status.js ──────────────────────────────────────
import { updateOfferStatus } from "@/lib/domain/offers/update-offer-status.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function includesAny(text, needles = []) {
  const normalized = lower(text);
  return needles.some((needle) => normalized.includes(lower(needle)));
}

function decideNextOfferStatus({ message = "", classification = null } = {}) {
  const objection = classification?.objection || null;
  const compliance_flag = classification?.compliance_flag || null;
  const msg = clean(message);

  if (compliance_flag === "stop_texting") {
    return {
      should_update: false,
      status: null,
      reason: "compliance_stop",
    };
  }

  if (
    includesAny(msg, [
      "accepted",
      "i'll take it",
      "that works",
      "works for me",
      "sounds good",
      "deal",
      "let's do it",
      "lets do it",
      "send the contract",
      "send docs",
      "send paperwork",
      "move forward",
      "okay let's move forward",
      "ok let's move forward",
    ])
  ) {
    return {
      should_update: true,
      status: "Accepted",
      reason: "seller_acceptance_signal",
    };
  }

  if (
    includesAny(msg, [
      "too low",
      "need more",
      "can you do better",
      "higher",
      "counter",
      "come up",
      "raise your offer",
      "not enough",
      "expected more",
      "want more",
    ]) ||
    objection === "need_more_money" ||
    objection === "has_other_buyer" ||
    objection === "wants_retail"
  ) {
    return {
      should_update: true,
      status: "Countered",
      reason: "seller_counter_signal",
    };
  }

  if (
    includesAny(msg, [
      "not interested",
      "pass",
      "no thanks",
      "won't work",
      "wont work",
      "not going to happen",
      "reject",
      "i'm out",
      "im out",
    ])
  ) {
    return {
      should_update: true,
      status: "Rejected",
      reason: "seller_rejection_signal",
    };
  }

  return {
    should_update: false,
    status: null,
    reason: "no_offer_status_change",
  };
}

export async function maybeProgressOfferStatus({
  offer_item_id = null,
  message = "",
  classification = null,
  notes = "",
} = {}) {
  if (!offer_item_id) {
    return {
      ok: false,
      updated: false,
      reason: "missing_offer_item_id",
    };
  }

  const decision = decideNextOfferStatus({
    message,
    classification,
  });

  if (!decision.should_update) {
    return {
      ok: true,
      updated: false,
      reason: decision.reason,
    };
  }

  const result = await updateOfferStatus({
    offer_item_id,
    status: decision.status,
    notes: notes || decision.reason,
  });

  return {
    ok: true,
    updated: true,
    reason: decision.reason,
    result,
  };
}

export default maybeProgressOfferStatus;