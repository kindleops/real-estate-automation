// ─── offer-follow-up.js ──────────────────────────────────────────────────
import { normalizeOfferStatus } from "@/lib/podio/apps/offers.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function toNumber(value, fallback = null) {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  return fallback;
}

function nowTs() {
  return Date.now();
}

function toTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isNaN(ts) ? null : ts;
}

function daysSince(value) {
  const ts = toTimestamp(value);
  if (!ts) return null;
  return Math.floor((nowTs() - ts) / 86400000);
}

function resolveSentAt(offer = {}) {
  return (
    offer.offer_date ||
    offer.offerDate ||
    offer["offer-date"] ||
    null
  );
}

function resolveCounteredAt(offer = {}) {
  return (
    offer.follow_up_window ||
    offer.followUpWindow ||
    offer["follow-up-window"] ||
    resolveSentAt(offer) ||
    null
  );
}

function resolveExpiredAt(offer = {}) {
  return (
    offer.offer_expiration_date ||
    offer.offerExpirationDate ||
    offer["offer-expiration-date-2"] ||
    null
  );
}

function buildReasonedResponse({
  action = "none",
  follow_up_type = null,
  priority = "_ Normal",
  should_queue_message = false,
  should_update_status = false,
  next_status = null,
  recommended_use_case = null,
  recommended_stage = null,
  days_open = null,
  rationale = [],
} = {}) {
  return {
    ok: true,
    action,
    follow_up_type,
    priority,
    should_queue_message,
    should_update_status,
    next_status,
    recommended_use_case,
    recommended_stage,
    days_open,
    rationale,
  };
}

export function offerFollowUp({
  offer = null,
  max_days_before_follow_up = 2,
  max_days_before_expire = 7,
  reengage_after_expired_days = 5,
} = {}) {
  if (!offer) {
    return {
      ok: false,
      reason: "missing_offer",
    };
  }

  const status = normalizeOfferStatus(
    offer.offer_status ||
      offer.status ||
      offer["offer-status"] ||
      "Offer Sent"
  );

  const sent_at = resolveSentAt(offer);
  const countered_at = resolveCounteredAt(offer);
  const expired_at = resolveExpiredAt(offer);

  const days_since_sent = daysSince(sent_at);
  const days_since_countered = daysSince(countered_at);
  const days_since_expired = daysSince(expired_at);

  const rationale = [];

  if (status === "Accepted (Ready for Contract)") {
    rationale.push("Offer already accepted.");
    return buildReasonedResponse({
      action: "none",
      follow_up_type: "accepted_terminal",
      should_queue_message: false,
      should_update_status: false,
      recommended_stage: "Contract",
      rationale,
    });
  }

  if (status === "Rejected") {
    rationale.push("Offer already rejected.");
    return buildReasonedResponse({
      action: "none",
      follow_up_type: "rejected_terminal",
      should_queue_message: false,
      should_update_status: false,
      recommended_stage: "Follow-Up",
      rationale,
    });
  }

  if (
    status === "Offer Sent" ||
    status === "Viewed" ||
    status === "Revised Offer Sent"
  ) {
    if (days_since_sent === null) {
      rationale.push("Offer marked sent but sent timestamp is missing.");
      return buildReasonedResponse({
        action: "review_before_follow_up",
        follow_up_type: "offer_missing_timestamp",
        priority: "_ Normal",
        should_queue_message: false,
        should_update_status: false,
        recommended_use_case: "offer_reveal_cash_follow_up",
        recommended_stage: "Offer",
        rationale,
      });
    }

    if (days_since_sent >= max_days_before_expire) {
      rationale.push(`Sent offer is stale at ${days_since_sent} days.`);
      return buildReasonedResponse({
        action: "expire_offer",
        follow_up_type: "offer_expired",
        priority: "_ Low",
        should_queue_message: false,
        should_update_status: true,
        next_status: "Expired",
        recommended_use_case: "reengagement",
        recommended_stage: "Follow-Up",
        days_open: days_since_sent,
        rationale,
      });
    }

    if (days_since_sent >= max_days_before_follow_up) {
      rationale.push(`Sent offer has been open ${days_since_sent} days without response.`);
      return buildReasonedResponse({
        action: "send_follow_up",
        follow_up_type: status === "Viewed" ? "viewed_offer_nudge" : "offer_nudge",
        priority: "_ Normal",
        should_queue_message: true,
        should_update_status: false,
        recommended_use_case: "offer_reveal_cash_follow_up",
        recommended_stage: "Offer",
        days_open: days_since_sent,
        rationale,
      });
    }

    rationale.push(`${status} is only ${days_since_sent} day(s) old.`);
    return buildReasonedResponse({
      action: "wait",
      follow_up_type: "offer_recently_sent",
      priority: "_ Low",
      should_queue_message: false,
      should_update_status: false,
      recommended_use_case: "offer_reveal_cash_follow_up",
      recommended_stage: "Offer",
      days_open: days_since_sent,
      rationale,
    });
  }

  if (status === "Counter Received" || status === "Negotiating") {
    if (days_since_countered === null) {
      rationale.push("Offer was countered but counter timestamp is missing.");
      return buildReasonedResponse({
        action: "review_counter",
        follow_up_type: "negotiation_missing_timestamp",
        priority: "_ Urgent",
        should_queue_message: false,
        should_update_status: false,
        recommended_use_case: "narrow_range",
        recommended_stage: "Offer",
        rationale,
      });
    }

    if (days_since_countered >= 2) {
      rationale.push(`Counter has been waiting ${days_since_countered} days.`);
      return buildReasonedResponse({
        action: "send_follow_up",
        follow_up_type: status === "Negotiating" ? "negotiation_follow_up" : "counter_follow_up",
        priority: "_ Urgent",
        should_queue_message: true,
        should_update_status: false,
        recommended_use_case: "narrow_range",
        recommended_stage: "Offer",
        days_open: days_since_countered,
        rationale,
      });
    }

    rationale.push(`${status} is still fresh at ${days_since_countered} day(s).`);
    return buildReasonedResponse({
      action: "wait",
      follow_up_type: "counter_recent",
      priority: "_ Normal",
      should_queue_message: false,
      should_update_status: false,
      recommended_use_case: "narrow_range",
      recommended_stage: "Offer",
      days_open: days_since_countered,
      rationale,
    });
  }

  if (status === "Expired") {
    if (days_since_expired === null) {
      rationale.push("Offer is expired but expired timestamp is missing.");
      return buildReasonedResponse({
        action: "reengage_review",
        follow_up_type: "expired_missing_timestamp",
        priority: "_ Low",
        should_queue_message: false,
        should_update_status: false,
        recommended_use_case: "reengagement",
        recommended_stage: "Follow-Up",
        rationale,
      });
    }

    if (days_since_expired >= reengage_after_expired_days) {
      rationale.push(`Expired offer has been dormant ${days_since_expired} days.`);
      return buildReasonedResponse({
        action: "send_follow_up",
        follow_up_type: "expired_reengagement",
        priority: "_ Low",
        should_queue_message: true,
        should_update_status: false,
        recommended_use_case: "reengagement",
        recommended_stage: "Follow-Up",
        days_open: days_since_expired,
        rationale,
      });
    }

    rationale.push(`Expired offer only expired ${days_since_expired} day(s) ago.`);
    return buildReasonedResponse({
      action: "wait",
      follow_up_type: "expired_recent",
      priority: "_ Low",
      should_queue_message: false,
      should_update_status: false,
      recommended_use_case: "reengagement",
      recommended_stage: "Follow-Up",
      days_open: days_since_expired,
      rationale,
    });
  }

  rationale.push(`Unhandled offer status: ${status}`);
  return buildReasonedResponse({
    action: "review",
    follow_up_type: "unknown_status",
    priority: "_ Normal",
    should_queue_message: false,
    should_update_status: false,
    recommended_use_case: "offer_reveal_cash_follow_up",
    recommended_stage: "Offer",
    rationale,
  });
}

export default offerFollowUp;
