// ─── should-suppress-outreach.js ─────────────────────────────────────────
import { getCategoryValue, getDateValue } from "@/lib/providers/podio.js";
import { validateActivePhone } from "@/lib/domain/compliance/validate-active-phone.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function isTruthyLabel(value) {
  const raw = lower(value);

  return [
    "true",
    "yes",
    "y",
    "1",
    "dnc",
    "opted out",
    "opt-out",
    "opt out",
    "suppressed",
    "blocked",
    "do not call",
    "do not contact",
    "stop",
  ].includes(raw);
}

function extractDncState(phone_item = null) {
  return clean(getCategoryValue(phone_item, "do-not-call", "FALSE") || "FALSE");
}

function extractDncSource(phone_item = null) {
  return clean(getCategoryValue(phone_item, "dnc-source", "") || "");
}

function extractOptOutDate(phone_item = null) {
  return getDateValue(phone_item, "opt-out-date", null);
}

function extractBrainManagedStatus(brain_item = null) {
  return clean(getCategoryValue(brain_item, "status-ai-managed", "") || "");
}

function extractFollowUpTriggerState(brain_item = null) {
  return clean(getCategoryValue(brain_item, "follow-up-trigger-state", "") || "");
}

function extractComplianceFlag(classification = null) {
  return clean(classification?.compliance_flag || "");
}

function isPostContactPhoneSuppressionSource(dnc_source = "") {
  const normalized = lower(dnc_source);
  return normalized === "internal opt-out" || normalized === "carrier flag";
}

export function deriveOutreachSuppressionSignals({
  phone_item = null,
  brain_item = null,
  classification = null,
} = {}) {
  const do_not_call = extractDncState(phone_item);
  const dnc_source = extractDncSource(phone_item);
  const opt_out_date = extractOptOutDate(phone_item);
  const status_ai_managed = extractBrainManagedStatus(brain_item);
  const follow_up_trigger_state = extractFollowUpTriggerState(brain_item);
  const compliance_flag = extractComplianceFlag(classification);
  const pre_contact_phone_flag = isTruthyLabel(do_not_call);
  const phone_post_contact_suppression =
    isPostContactPhoneSuppressionSource(dnc_source) || Boolean(opt_out_date);

  return {
    do_not_call,
    dnc_source,
    opt_out_date,
    status_ai_managed,
    follow_up_trigger_state,
    compliance_flag,
    pre_contact_phone_flag,
    phone_post_contact_suppression,
  };
}

function withSuppressionDetails(signals = {}, overrides = {}) {
  const {
    true_post_contact_suppression = false,
    skip_reason = null,
    ...rest
  } = overrides;

  return {
    ...signals,
    ...rest,
    true_post_contact_suppression: Boolean(true_post_contact_suppression),
    skip_reason: skip_reason || null,
  };
}

export function shouldSuppressOutreach({
  phone_item = null,
  brain_item = null,
  classification = null,
  } = {}) {
  const phone_validation = validateActivePhone(phone_item);
  const signals = deriveOutreachSuppressionSignals({
    phone_item,
    brain_item,
    classification,
  });

  if (!phone_validation.ok) {
    return {
      suppress: true,
      reason: phone_validation.reason,
      details: withSuppressionDetails(signals, {
        true_post_contact_suppression: false,
        skip_reason: phone_validation.reason,
        activity_status: phone_validation.activity_status,
      }),
    };
  }

  if (signals.phone_post_contact_suppression) {
    return {
      suppress: true,
      reason: "phone_post_contact_suppression",
      details: withSuppressionDetails(signals, {
        true_post_contact_suppression: true,
        skip_reason: "phone_post_contact_suppression",
      }),
    };
  }

  if (signals.compliance_flag === "stop_texting") {
    return {
      suppress: true,
      reason: "classification_stop_texting",
      details: withSuppressionDetails(signals, {
        true_post_contact_suppression: true,
        skip_reason: "classification_stop_texting",
      }),
    };
  }

  if (
    [
      "_ under contract",
      "_ closed",
      "under contract",
      "closed",
      "dnc",
      "wrong number",
    ].includes(lower(signals.status_ai_managed))
  ) {
    return {
      suppress: true,
      reason: "brain_status_terminal",
      details: withSuppressionDetails(signals, {
        true_post_contact_suppression: true,
        skip_reason: "brain_status_terminal",
      }),
    };
  }

  if (
    ["paused", "manual override", "completed", "expired"].includes(
      lower(signals.follow_up_trigger_state)
    )
  ) {
    return {
      suppress: true,
      reason: "follow_up_trigger_paused",
      details: withSuppressionDetails(signals, {
        true_post_contact_suppression: true,
        skip_reason: "follow_up_trigger_paused",
      }),
    };
  }

  return {
    suppress: false,
    reason: null,
    details: withSuppressionDetails(signals, {
      true_post_contact_suppression: false,
      skip_reason: null,
      activity_status: phone_validation.activity_status,
    }),
  };
}

export default shouldSuppressOutreach;
