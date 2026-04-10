// ─── phone-disqualification.js ───────────────────────────────────────────
import { getCategoryValue } from "@/lib/providers/podio.js";

function lower(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function derivePhoneDisqualification(phone_item = null) {
  const status = lower(
    getCategoryValue(phone_item, "phone-activity-status", "Unknown")
  );

  if (!status || status === "unknown") {
    return null;
  }

  if (!status.startsWith("active")) {
    return `phone_not_active:${status || "unknown"}`;
  }

  return null;
}

export default derivePhoneDisqualification;
