import APP_IDS from "@/lib/config/app-ids.js";
import { child } from "@/lib/logging/logger.js";
import {
  getItem,
  updateItem,
  filterAppItems,
  normalizeUsPhone10,
  toCanonicalUsE164,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.phone_numbers;
const DEBUG_PHONE_LOOKUP_TARGET = "2059230168";
const logger = child({
  module: "podio.apps.phone_numbers",
  app_id: APP_ID,
});

export const PHONE_FIELDS = {
  phone_full_name: "phone-full-name",
  phone_first_name: "phone-first-name",
  phone: "phone",
  phone_hidden: "phone-hidden",
  canonical_e164: "canonical-e164",
  linked_master_owner: "linked-master-owner",
  linked_owner: "linked-owner",
  linked_contact: "linked-contact",
  primary_property: "primary-property",
  market: "market",
  do_not_call: "do-not-call",
  dnc_source: "dnc-source",
  opt_out_date: "opt-out-date",
  last_compliance_check: "last-compliance-check",
  total_messages_sent: "total-messages-sent",
  total_replies: "total-replies",
  last_reply_date: "last-reply-date",
  phone_activity_status: "phone-activity-status",
  engagement_tier: "engagement-tier",
};

export async function getPhoneNumberItem(item_id) {
  return getItem(item_id);
}

export async function updatePhoneNumberItem(item_id, fields = {}, revision = null) {
  return updateItem(item_id, fields, revision);
}

export async function findPhoneNumbers(filters = {}, limit = 30, offset = 0) {
  return filterAppItems(APP_ID, filters, { limit, offset });
}

function digitsOnly(value) {
  return String(value ?? "").replace(/\D/g, "");
}

function shouldDebugPhoneLookup(field, value) {
  if (field !== PHONE_FIELDS.phone_hidden) return false;
  return digitsOnly(value) === DEBUG_PHONE_LOOKUP_TARGET;
}

function toPodioRichTextParagraph(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (/^<p>[\s\S]*<\/p>$/.test(text)) return text;
  return `<p>${text}</p>`;
}

async function findFirstPhoneByTextField(field, raw) {
  const normalized = String(raw ?? "").trim();
  if (!normalized) return null;

  const attempts = [
    { label: "plain_text", value: normalized },
    { label: "rich_text_html", value: toPodioRichTextParagraph(normalized) },
  ].filter((attempt, index, all) => all.findIndex((other) => other.value === attempt.value) === index);

  for (const attempt of attempts) {
    const filters = { [field]: attempt.value };
    const response = await filterAppItems(APP_ID, filters, { limit: 1, offset: 0 });
    const item = response?.items?.[0] ?? null;

    if (shouldDebugPhoneLookup(field, raw)) {
      logger.info("phone_lookup.audit_filter_attempt", {
        field,
        raw,
        attempt: attempt.label,
        podio_request: {
          app_id: APP_ID,
          filters,
          limit: 1,
          offset: 0,
        },
        raw_match_count: response?.filtered ?? response?.total ?? response?.count ?? response?.items?.length ?? 0,
        returned_item_ids: Array.isArray(response?.items)
          ? response.items.map((candidate) => candidate?.item_id).filter(Boolean)
          : [],
      });
    }

    if (item) return item;
  }

  return null;
}

export async function findPhoneByHiddenNumber(raw) {
  const normalized = normalizeUsPhone10(raw);
  if (!normalized) return null;

  return findFirstPhoneByTextField(PHONE_FIELDS.phone_hidden, normalized);
}

export async function findPhoneByCanonicalE164(value) {
  if (!value) return null;

  return findFirstPhoneByTextField(PHONE_FIELDS.canonical_e164, value);
}

export async function findPhoneRecord(raw_phone) {
  const d10 = normalizeUsPhone10(raw_phone);

  if (!d10 || d10.length < 10) return null;

  const canonical_e164 = toCanonicalUsE164(d10);

  return (
    (await findPhoneByHiddenNumber(d10)) ??
    (await findPhoneByCanonicalE164(canonical_e164)) ??
    (await findPhoneByCanonicalE164(d10))
  );
}

export default {
  APP_ID,
  PHONE_FIELDS,
  getPhoneNumberItem,
  updatePhoneNumberItem,
  findPhoneNumbers,
  findPhoneByHiddenNumber,
  findPhoneByCanonicalE164,
  findPhoneRecord,
};
