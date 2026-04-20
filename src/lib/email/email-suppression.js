import { supabase as defaultSupabase } from "@/lib/supabase/client.js";

let _deps = {
  supabase_override: null,
};

function getDb() {
  return _deps.supabase_override || defaultSupabase;
}

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

export function __setEmailSuppressionDeps(overrides = {}) {
  _deps = { ..._deps, ...overrides };
}

export function __resetEmailSuppressionDeps() {
  _deps = { supabase_override: null };
}

export async function isEmailSuppressed(email = "") {
  const normalized_email = lower(email);
  if (!normalized_email) {
    return {
      ok: false,
      suppressed: false,
      reason: "missing_email",
    };
  }

  const db = getDb();
  const { data, error } = await db
    .from("email_suppression")
    .select("email_address, reason, source, created_at")
    .eq("email_address", normalized_email)
    .maybeSingle();

  if (error) {
    return {
      ok: false,
      suppressed: false,
      reason: "suppression_lookup_failed",
      error: clean(error?.message) || "suppression_lookup_failed",
    };
  }

  return {
    ok: true,
    suppressed: Boolean(data),
    suppression: data || null,
  };
}

export async function suppressEmail({ email, reason, source, raw_payload } = {}) {
  const normalized_email = lower(email);
  const normalized_reason = clean(reason) || "suppressed";

  if (!normalized_email) {
    return {
      ok: false,
      suppressed: false,
      reason: "missing_email",
    };
  }

  const db = getDb();
  const row = {
    email_address: normalized_email,
    reason: normalized_reason,
    source: clean(source) || null,
    raw_payload: raw_payload && typeof raw_payload === "object" ? raw_payload : {},
  };

  const { error } = await db
    .from("email_suppression")
    .upsert(row, { onConflict: "email_address" });

  if (error) {
    return {
      ok: false,
      suppressed: false,
      reason: "suppression_upsert_failed",
      error: clean(error?.message) || "suppression_upsert_failed",
    };
  }

  return {
    ok: true,
    suppressed: true,
    suppression: row,
  };
}

export default {
  isEmailSuppressed,
  suppressEmail,
};
