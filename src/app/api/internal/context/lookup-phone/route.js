import { NextResponse } from "next/server";

import APP_IDS from "@/lib/config/app-ids.js";
import { child } from "@/lib/logging/logger.js";
import { loadContext } from "@/lib/domain/context/load-context.js";
import {
  PHONE_FIELDS,
  findPhoneByCanonicalE164,
  findPhoneByHiddenNumber,
  findPhoneRecord,
} from "@/lib/podio/apps/phone-numbers.js";
import {
  getTextValue,
  normalizeUsPhone10,
  toCanonicalUsE164,
} from "@/lib/providers/podio.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.context.lookup_phone",
});

function clean(value) {
  return String(value ?? "").trim();
}

function summarizePhoneItem(item) {
  if (!item) return null;

  return {
    item_id: item.item_id,
    title: item.title || null,
    phone_hidden: getTextValue(item, PHONE_FIELDS.phone_hidden, ""),
    canonical_e164: getTextValue(item, PHONE_FIELDS.canonical_e164, ""),
  };
}

function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

async function lookupPhone(phone) {
  const input_phone = clean(phone);
  const normalized_digits = normalizeUsPhone10(input_phone);
  const canonical_e164 = toCanonicalUsE164(normalized_digits);

  if (!input_phone) {
    return {
      ok: false,
      reason: "missing_phone",
      input_phone,
      normalized_digits,
      canonical_e164,
    };
  }

  if (!normalized_digits) {
    return {
      ok: false,
      reason: "invalid_phone",
      input_phone,
      normalized_digits,
      canonical_e164,
    };
  }

  const [by_hidden, by_e164, by_e164_digits, found] = await Promise.all([
    findPhoneByHiddenNumber(normalized_digits),
    canonical_e164 ? findPhoneByCanonicalE164(canonical_e164) : Promise.resolve(null),
    findPhoneByCanonicalE164(normalized_digits),
    findPhoneRecord(normalized_digits),
  ]);

  let matched_by = null;

  if (found?.item_id) {
    if (by_hidden?.item_id === found.item_id) matched_by = PHONE_FIELDS.phone_hidden;
    else if (by_e164?.item_id === found.item_id) matched_by = PHONE_FIELDS.canonical_e164;
    else if (by_e164_digits?.item_id === found.item_id) {
      matched_by = `${PHONE_FIELDS.canonical_e164} (10-digit fallback)`;
    }
  }

  const context = await loadContext({
    inbound_from: normalized_digits,
    create_brain_if_missing: false,
  }).catch((error) => ({
    found: false,
    reason: "load_context_failed",
    error: error?.message || String(error),
  }));

  return {
    ok: true,
    input_phone,
    normalized_digits,
    canonical_e164,
    phone_numbers_app_id: APP_IDS.phone_numbers,
    search_order: [
      { app_id: APP_IDS.phone_numbers, field: PHONE_FIELDS.phone_hidden, value: normalized_digits },
      { app_id: APP_IDS.phone_numbers, field: PHONE_FIELDS.canonical_e164, value: canonical_e164 },
      { app_id: APP_IDS.phone_numbers, field: PHONE_FIELDS.canonical_e164, value: normalized_digits },
    ],
    found: Boolean(found?.item_id),
    matched_by,
    matches: {
      phone_hidden: summarizePhoneItem(by_hidden),
      canonical_e164: summarizePhoneItem(by_e164),
      canonical_e164_digits_fallback: summarizePhoneItem(by_e164_digits),
      first_match: summarizePhoneItem(found),
    },
    context: {
      found: context?.found ?? false,
      reason: context?.reason || null,
      inbound_from: context?.inbound_from || normalized_digits,
      ids: context?.ids || null,
    },
  };
}

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);
    const phone = clean(searchParams.get("phone"));

    logger.info("context_lookup_phone.requested", {
      method: "GET",
      phone,
    });

    const result = await lookupPhone(phone);

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/context/lookup-phone",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("context_lookup_phone.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "context_lookup_phone_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));
    const phone = clean(body?.phone);

    logger.info("context_lookup_phone.requested", {
      method: "POST",
      phone,
    });

    const result = await lookupPhone(phone);

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/context/lookup-phone",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("context_lookup_phone.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "context_lookup_phone_failed",
      },
      { status: 500 }
    );
  }
}
