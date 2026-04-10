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

const defaultDeps = {
  getItem,
  updateItem,
  filterAppItems,
  logger,
};

let runtimeDeps = { ...defaultDeps };

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

export function __setPhoneNumbersTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetPhoneNumbersTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function getPhoneNumberItem(item_id) {
  return runtimeDeps.getItem(item_id);
}

export async function updatePhoneNumberItem(item_id, fields = {}, revision = null) {
  return runtimeDeps.updateItem(item_id, fields, revision);
}

export async function findPhoneNumbers(filters = {}, limit = 30, offset = 0) {
  return runtimeDeps.filterAppItems(APP_ID, filters, { limit, offset });
}

function digitsOnly(value) {
  return String(value ?? "").replace(/\D/g, "");
}

function shouldDebugPhoneLookup(field, value) {
  if (
    field !== PHONE_FIELDS.phone_hidden &&
    field !== PHONE_FIELDS.canonical_e164 &&
    field !== PHONE_FIELDS.phone
  ) {
    return false;
  }
  return digitsOnly(value) === DEBUG_PHONE_LOOKUP_TARGET;
}

function toPodioRichTextParagraph(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (/^<p>[\s\S]*<\/p>$/.test(text)) return text;
  return `<p>${text}</p>`;
}

function uniqueLookupAttempts(attempts = []) {
  return attempts.filter((attempt, index, all) => {
    const value = String(attempt?.value ?? "").trim();
    if (!value) return false;
    return all.findIndex((other) => String(other?.value ?? "").trim() === value) === index;
  });
}

async function findFirstPhoneByFieldAttempts(field, attempts = [], raw = null) {
  const normalized_attempts = uniqueLookupAttempts(attempts);

  for (const attempt of normalized_attempts) {
    const filters = { [field]: attempt.value };
    const response = await runtimeDeps.filterAppItems(APP_ID, filters, { limit: 1, offset: 0 });
    const item = response?.items?.[0] ?? null;

    if (shouldDebugPhoneLookup(field, raw)) {
      runtimeDeps.logger.info("phone_lookup.audit_filter_attempt", {
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

async function findFirstPhoneByTextField(field, raw) {
  const normalized = String(raw ?? "").trim();
  if (!normalized) return null;

  return findFirstPhoneByFieldAttempts(
    field,
    [
      { label: "plain_text", value: normalized },
      { label: "rich_text_html", value: toPodioRichTextParagraph(normalized) },
    ],
    raw
  );
}

function formatUsPhoneNational(d10) {
  if (String(d10 ?? "").length !== 10) return "";
  return `(${d10.slice(0, 3)}) ${d10.slice(3, 6)}-${d10.slice(6)}`;
}

export async function findPhoneByRawPhoneField(raw) {
  const trimmed = String(raw ?? "").trim();
  const d10 = normalizeUsPhone10(trimmed);
  const canonical_e164 = toCanonicalUsE164(d10);

  return findFirstPhoneByFieldAttempts(
    PHONE_FIELDS.phone,
    [
      { label: "raw_input", value: trimmed },
      { label: "digits_10", value: d10 },
      { label: "digits_11", value: d10 ? `1${d10}` : "" },
      { label: "canonical_e164", value: canonical_e164 },
      { label: "national_format", value: formatUsPhoneNational(d10) },
    ],
    raw
  );
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
    (await findPhoneByCanonicalE164(d10)) ??
    (await findPhoneByRawPhoneField(raw_phone))
  );
}

export default {
  APP_ID,
  PHONE_FIELDS,
  __setPhoneNumbersTestDeps,
  __resetPhoneNumbersTestDeps,
  getPhoneNumberItem,
  updatePhoneNumberItem,
  findPhoneNumbers,
  findPhoneByHiddenNumber,
  findPhoneByCanonicalE164,
  findPhoneByRawPhoneField,
  findPhoneRecord,
};
