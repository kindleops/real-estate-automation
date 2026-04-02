import {
  findPhoneRecord,
  findPhoneByHiddenNumber,
  findPhoneByCanonicalE164,
} from "@/lib/podio/apps/phone-numbers.js";

export async function findPhone({ raw_phone = null, canonical_e164 = null, phone_hidden = null }) {
  if (raw_phone) {
    const found = await findPhoneRecord(raw_phone);
    if (found) return found;
  }

  if (phone_hidden) {
    const found = await findPhoneByHiddenNumber(phone_hidden);
    if (found) return found;
  }

  if (canonical_e164) {
    const found = await findPhoneByCanonicalE164(canonical_e164);
    if (found) return found;
  }

  return null;
}

export default findPhone;