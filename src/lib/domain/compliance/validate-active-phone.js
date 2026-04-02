// ─── validate-active-phone.js ────────────────────────────────────────────
import { getCategoryValue } from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export function getPhoneActivityStatus(phone_item = null) {
  return clean(
    getCategoryValue(phone_item, "phone-activity-status", "Unknown") || "Unknown"
  );
}

export function isPhoneActive(phone_item = null) {
  return lower(getPhoneActivityStatus(phone_item)).startsWith("active");
}

export function validateActivePhone(phone_item = null) {
  const activity_status = getPhoneActivityStatus(phone_item);
  const normalized = lower(activity_status);

  if (!normalized.startsWith("active")) {
    return {
      ok: false,
      activity_status,
      reason: `phone_not_active:${normalized || "unknown"}`,
    };
  }

  return {
    ok: true,
    activity_status,
    reason: null,
  };
}

export default validateActivePhone;
