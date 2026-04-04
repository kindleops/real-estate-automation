import { getCategoryValue } from "@/lib/providers/podio.js";
import { MASTER_OWNER_FIELDS } from "@/lib/podio/apps/master-owners.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export function mapFollowUpCadenceToDays(value = null) {
  const raw = lower(value);

  if (raw === "passive") return 14;
  if (raw === "aggressive") return 3;
  return 7;
}

export function deriveFollowUpCadenceDays(master_owner_item = null) {
  return mapFollowUpCadenceToDays(
    getCategoryValue(master_owner_item, MASTER_OWNER_FIELDS.follow_up_cadence, null)
  );
}

export function addDaysIso(base = new Date(), days = 0) {
  const date = base instanceof Date ? new Date(base.getTime()) : new Date(base);

  if (Number.isNaN(date.getTime())) return null;

  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return date.toISOString();
}

export function isNoReplyFollowUpEligible(use_case = null) {
  const raw = lower(use_case);

  if (!raw) return false;

  if (["stop_or_opt_out", "wrong_person", "not_interested"].includes(raw)) {
    return false;
  }

  return true;
}

export default {
  mapFollowUpCadenceToDays,
  deriveFollowUpCadenceDays,
  addDaysIso,
  isNoReplyFollowUpEligible,
};
