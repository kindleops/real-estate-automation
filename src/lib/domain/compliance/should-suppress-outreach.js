// ─── should-suppress-outreach.js ─────────────────────────────────────────
import { getCategoryValue } from "@/lib/providers/podio.js";
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

function extractBrainManagedStatus(brain_item = null) {
  return clean(getCategoryValue(brain_item, "status-ai-managed", "") || "");
}

function extractFollowUpTriggerState(brain_item = null) {
  return clean(getCategoryValue(brain_item, "follow-up-trigger-state", "") || "");
}

function extractComplianceFlag(classification = null) {
  return clean(classification?.compliance_flag || "");
}

export function shouldSuppressOutreach({
  phone_item = null,
  brain_item = null,
  classification = null,
} = {}) {
  const phone_validation = validateActivePhone(phone_item);

  if (!phone_validation.ok) {
    return {
      suppress: true,
      reason: phone_validation.reason,
      details: {
        activity_status: phone_validation.activity_status,
      },
    };
  }

  const do_not_call = extractDncState(phone_item);
  const dnc_source = extractDncSource(phone_item);
  const status_ai_managed = extractBrainManagedStatus(brain_item);
  const follow_up_trigger_state = extractFollowUpTriggerState(brain_item);
  const compliance_flag = extractComplianceFlag(classification);

  if (isTruthyLabel(do_not_call)) {
    return {
      suppress: true,
      reason: "phone_dnc",
      details: {
        do_not_call,
        dnc_source,
      },
    };
  }

  if (compliance_flag === "stop_texting") {
    return {
      suppress: true,
      reason: "classification_stop_texting",
      details: {
        compliance_flag,
      },
    };
  }

  if (["_ under contract", "_ closed"].includes(lower(status_ai_managed))) {
    return {
      suppress: true,
      reason: "brain_status_terminal",
      details: {
        status_ai_managed,
      },
    };
  }

  if (["paused", "manual override"].includes(lower(follow_up_trigger_state))) {
    return {
      suppress: true,
      reason: "follow_up_trigger_paused",
      details: {
        follow_up_trigger_state,
      },
    };
  }

  return {
    suppress: false,
    reason: null,
    details: {
      activity_status: phone_validation.activity_status,
      do_not_call,
      dnc_source,
      status_ai_managed,
      follow_up_trigger_state,
      compliance_flag,
    },
  };
}

export default shouldSuppressOutreach;
