import crypto from "node:crypto";

import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { insertSupabaseSendQueueRow } from "@/lib/supabase/sms-engine.js";
import { notifyDiscordOps } from "@/lib/discord/notify-discord-ops.js";
import { normalizePhone } from "@/lib/providers/textgrid.js";
import { info, warn } from "@/lib/logging/logger.js";

const UNKNOWN_BUCKETS = Object.freeze({
  UNKNOWN_SELLER_REPLY: "UNKNOWN_SELLER_REPLY",
  UNKNOWN_AGENT_OR_REALTOR: "UNKNOWN_AGENT_OR_REALTOR",
  UNKNOWN_BUYER_OR_INVESTOR: "UNKNOWN_BUYER_OR_INVESTOR",
  UNKNOWN_TITLE_OR_LENDER: "UNKNOWN_TITLE_OR_LENDER",
  UNKNOWN_VENDOR_OR_CONTRACTOR: "UNKNOWN_VENDOR_OR_CONTRACTOR",
  UNKNOWN_PERSONAL: "UNKNOWN_PERSONAL",
  WRONG_NUMBER: "WRONG_NUMBER",
  OPT_OUT: "OPT_OUT",
  SPAM: "SPAM",
  UNCLEAR_UNKNOWN: "UNCLEAR_UNKNOWN",
});

const DEFAULT_REPLY = "Thanks for reaching out. What property or topic is this regarding?";

const BUCKET_REPLIES = Object.freeze({
  [UNKNOWN_BUCKETS.UNKNOWN_SELLER_REPLY]: "Thanks for reaching out. Which property are you referring to?",
  [UNKNOWN_BUCKETS.UNKNOWN_AGENT_OR_REALTOR]: "Thanks for reaching out. Are you contacting us about a property, buyer, or partnership opportunity?",
  [UNKNOWN_BUCKETS.UNKNOWN_BUYER_OR_INVESTOR]: "Got it - are you looking for off-market deals in a specific city or price range?",
  [UNKNOWN_BUCKETS.UNKNOWN_TITLE_OR_LENDER]: "Thanks - can you send the property address or file name so I can route this correctly?",
  [UNKNOWN_BUCKETS.UNKNOWN_VENDOR_OR_CONTRACTOR]: "Thanks - what property or project is this regarding?",
  [UNKNOWN_BUCKETS.UNCLEAR_UNKNOWN]: DEFAULT_REPLY,
  [UNKNOWN_BUCKETS.UNKNOWN_PERSONAL]: null,
  [UNKNOWN_BUCKETS.WRONG_NUMBER]: null,
  [UNKNOWN_BUCKETS.OPT_OUT]: null,
  [UNKNOWN_BUCKETS.SPAM]: null,
});

const OPT_OUT_KEYWORDS = ["stop", "end", "cancel", "unsubscribe", "quit"];

function clean(value) {
  return String(value ?? "").trim();
}

function toBool(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const v = clean(value).toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return fallback;
}

function normalizeBody(value) {
  return clean(value)
    .toLowerCase()
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[^\p{L}\p{N}'\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function bodyContainsAny(normalized_body, terms = []) {
  return terms.some((term) => normalized_body.includes(term));
}

function findOptOutKeyword(normalized_body) {
  for (const keyword of OPT_OUT_KEYWORDS) {
    if (normalized_body === keyword || normalized_body.includes(` ${keyword} `) || normalized_body.startsWith(`${keyword} `) || normalized_body.endsWith(` ${keyword}`)) {
      return keyword.toUpperCase();
    }
  }
  return null;
}

function classifyUnknownInbound(message_body = "") {
  const normalized = normalizeBody(message_body);

  if (!normalized) {
    return { bucket: UNKNOWN_BUCKETS.UNCLEAR_UNKNOWN, confidence: 0.2, reason: "empty_or_unparseable" };
  }

  const opt_out_keyword = findOptOutKeyword(normalized);
  if (opt_out_keyword) {
    return {
      bucket: UNKNOWN_BUCKETS.OPT_OUT,
      confidence: 0.99,
      reason: "opt_out_keyword",
      opt_out_keyword,
    };
  }

  if (/\bwrong\s+(number|person|contact)\b/i.test(normalized)) {
    return { bucket: UNKNOWN_BUCKETS.WRONG_NUMBER, confidence: 0.98, reason: "wrong_number_phrase" };
  }

  if (/\b(lawyer|attorney|cease\s+and\s+desist|legal|court|sue|lawsuit|harass|police|fraud)\b/i.test(normalized)) {
    return { bucket: UNKNOWN_BUCKETS.UNCLEAR_UNKNOWN, confidence: 0.8, reason: "legal_sensitive" };
  }

  if (/\b(viagra|casino|bitcoin|crypto\s+airdrop|loan\s+guaranteed|click\s+here|free\s+gift)\b/i.test(normalized) || /(https?:\/\/|www\.)/i.test(message_body)) {
    return { bucket: UNKNOWN_BUCKETS.SPAM, confidence: 0.95, reason: "spam_pattern" };
  }

  if (bodyContainsAny(normalized, ["agent", "realtor", "broker", "mls", "listing", "commission"])) {
    return { bucket: UNKNOWN_BUCKETS.UNKNOWN_AGENT_OR_REALTOR, confidence: 0.86, reason: "agent_terms" };
  }

  if (bodyContainsAny(normalized, ["buyer", "investor", "cash buyer", "off market", "deals", "cap rate"])) {
    return { bucket: UNKNOWN_BUCKETS.UNKNOWN_BUYER_OR_INVESTOR, confidence: 0.85, reason: "buyer_terms" };
  }

  if (bodyContainsAny(normalized, ["title", "escrow", "closing", "hud", "settlement", "lender", "loan", "underwriter"])) {
    return { bucket: UNKNOWN_BUCKETS.UNKNOWN_TITLE_OR_LENDER, confidence: 0.86, reason: "title_or_lender_terms" };
  }

  if (bodyContainsAny(normalized, ["contractor", "repair", "invoice", "roof", "plumbing", "electric", "hvac", "project"])) {
    return { bucket: UNKNOWN_BUCKETS.UNKNOWN_VENDOR_OR_CONTRACTOR, confidence: 0.84, reason: "vendor_terms" };
  }

  if (bodyContainsAny(normalized, ["property", "house", "home", "still own", "sell", "selling", "offer"])) {
    return { bucket: UNKNOWN_BUCKETS.UNKNOWN_SELLER_REPLY, confidence: 0.82, reason: "sellerish_terms" };
  }

  if (bodyContainsAny(normalized, ["mom", "dad", "wife", "husband", "brother", "sister", "family", "friend"])) {
    return { bucket: UNKNOWN_BUCKETS.UNKNOWN_PERSONAL, confidence: 0.7, reason: "personal_terms" };
  }

  return { bucket: UNKNOWN_BUCKETS.UNCLEAR_UNKNOWN, confidence: 0.5, reason: "fallback_unclear" };
}

function selectUnknownReply(bucket) {
  return BUCKET_REPLIES[bucket] ?? DEFAULT_REPLY;
}

function shouldBlockAutoReply({ bucket, is_opt_out, is_spam, legal_sensitive }) {
  if (is_opt_out || is_spam || legal_sensitive) return true;
  if ([UNKNOWN_BUCKETS.UNKNOWN_PERSONAL, UNKNOWN_BUCKETS.WRONG_NUMBER].includes(bucket)) return true;
  return false;
}

async function getUnknownContactRow(db, phone_e164) {
  const { data, error } = await db
    .from("unknown_inbound_contacts")
    .select("*")
    .eq("phone_e164", phone_e164)
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

async function upsertUnknownContactState(db, {
  phone_e164,
  inbound_message_body,
  bucket,
  confidence,
  linked_master_owner_id = null,
  linked_property_id = null,
  linked_prospect_id = null,
  auto_reply_sent = false,
  metadata = {},
}) {
  const now = new Date().toISOString();
  const existing = await getUnknownContactRow(db, phone_e164).catch(() => null);

  const message_count = Number(existing?.message_count || 0) + 1;
  const auto_reply_count = Number(existing?.auto_reply_count || 0) + (auto_reply_sent ? 1 : 0);

  const payload = {
    phone_e164,
    first_seen_at: existing?.first_seen_at || now,
    last_seen_at: now,
    message_count,
    last_message_body: clean(inbound_message_body) || null,
    unknown_bucket: bucket,
    classification_confidence: confidence,
    resolved_status: existing?.resolved_status || "unresolved",
    linked_master_owner_id: linked_master_owner_id || existing?.linked_master_owner_id || null,
    linked_property_id: linked_property_id || existing?.linked_property_id || null,
    linked_prospect_id: linked_prospect_id || existing?.linked_prospect_id || null,
    auto_reply_sent_at: auto_reply_sent ? now : existing?.auto_reply_sent_at || null,
    auto_reply_count,
    metadata: {
      ...(existing?.metadata || {}),
      ...(metadata || {}),
    },
    updated_at: now,
  };

  const { data, error } = await db
    .from("unknown_inbound_contacts")
    .upsert(payload, { onConflict: "phone_e164", ignoreDuplicates: false })
    .select()
    .maybeSingle();

  if (error) throw error;
  return data || payload;
}

async function upsertSmsSuppression(db, {
  phone_e164,
  reason,
  opt_out_keyword = null,
  source = "textgrid_inbound_unknown_router",
  metadata = {},
}) {
  const now = new Date().toISOString();
  const payload = {
    phone_e164,
    suppressed_at: now,
    reason,
    source,
    opt_out_keyword,
    metadata,
    updated_at: now,
  };

  const { data, error } = await db
    .from("sms_suppression_list")
    .upsert(payload, { onConflict: "phone_e164", ignoreDuplicates: false })
    .select()
    .maybeSingle();

  if (error) throw error;
  return data || payload;
}

async function insertUnknownInboundMessageEvent(db, {
  message_id,
  inbound_from,
  inbound_to,
  message_body,
  bucket,
  is_opt_out,
  opt_out_keyword,
  metadata = {},
}) {
  const now = new Date().toISOString();
  const message_event_key = `inbound_unknown_${clean(message_id) || crypto.randomUUID()}`;

  const row = {
    message_event_key,
    provider_message_sid: clean(message_id) || null,
    direction: "inbound",
    event_type: "inbound_unknown",
    message_body: clean(message_body) || null,
    from_phone_number: inbound_from,
    to_phone_number: inbound_to,
    received_at: now,
    event_timestamp: now,
    created_at: now,
    is_opt_out: Boolean(is_opt_out),
    opt_out_keyword: opt_out_keyword || null,
    metadata: {
      unknown_inbound: true,
      unknown_bucket: bucket,
      context_found: false,
      source: "textgrid_inbound_unknown_router",
      ...(metadata || {}),
    },
  };

  const { data, error } = await db
    .from("message_events")
    .upsert(row, { onConflict: "message_event_key", ignoreDuplicates: false })
    .select()
    .maybeSingle();

  if (error) throw error;
  return data || row;
}

function buildSuggestedAction(bucket) {
  if (bucket === UNKNOWN_BUCKETS.OPT_OUT) return "Honor opt-out and review suppression logs.";
  if (bucket === UNKNOWN_BUCKETS.WRONG_NUMBER) return "Mark as wrong-number and verify targeting source.";
  if (bucket === UNKNOWN_BUCKETS.SPAM) return "No engagement. Monitor for repeated spam patterns.";
  if (bucket === UNKNOWN_BUCKETS.UNKNOWN_PERSONAL) return "Manual triage recommended before any outreach.";
  return "Review inbound and map to owner/property if identifiable.";
}

const defaultDeps = {
  supabase: defaultSupabase,
  insertSupabaseSendQueueRow,
  notifyDiscordOps,
  normalizePhone,
  info,
  warn,
};

let runtimeDeps = { ...defaultDeps };

export function __setUnknownInboundRouterDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetUnknownInboundRouterDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function handleUnknownInboundRouter({
  message_id = null,
  inbound_from = null,
  inbound_to = null,
  message_body = "",
  dry_run = false,
  auto_reply_enabled = true,
  inbound_user_initiated = true,
} = {}) {
  const db = runtimeDeps.supabase;
  const safe_from = runtimeDeps.normalizePhone(inbound_from) || clean(inbound_from) || null;
  const safe_to = runtimeDeps.normalizePhone(inbound_to) || clean(inbound_to) || null;
  const normalized_body = normalizeBody(message_body);

  const classification = classifyUnknownInbound(message_body);
  const bucket = classification.bucket;
  const is_opt_out = bucket === UNKNOWN_BUCKETS.OPT_OUT;
  const is_spam = bucket === UNKNOWN_BUCKETS.SPAM;
  const legal_sensitive = /\b(lawyer|attorney|legal|lawsuit|cease\s+and\s+desist|court)\b/i.test(normalized_body);
  const opt_out_keyword = classification.opt_out_keyword || null;

  const reply_text = selectUnknownReply(bucket);
  const blocked_auto_reply = shouldBlockAutoReply({ bucket, is_opt_out, is_spam, legal_sensitive });

  let unknown_contact_before = null;
  if (!dry_run && safe_from) {
    unknown_contact_before = await getUnknownContactRow(db, safe_from).catch((error) => {
      runtimeDeps.warn("unknown_inbound.contact_lookup_failed", {
        inbound_from: safe_from,
        message: error?.message || "lookup_failed",
      });
      return null;
    });
  }

  const last_auto_reply_at = unknown_contact_before?.auto_reply_sent_at || null;
  const within_24h = Boolean(last_auto_reply_at) && (Date.now() - new Date(last_auto_reply_at).getTime()) < (24 * 60 * 60 * 1000);

  const should_auto_reply = Boolean(
    !dry_run &&
    auto_reply_enabled &&
    inbound_user_initiated &&
    !blocked_auto_reply &&
    !within_24h &&
    clean(reply_text)
  );

  let suppression_applied = false;
  let message_event_created = false;
  let auto_reply_queued = false;
  let discord_alert_sent = false;
  let unknown_contact_written = false;

  if (!dry_run && safe_from && (is_opt_out || bucket === UNKNOWN_BUCKETS.WRONG_NUMBER)) {
    try {
      await upsertSmsSuppression(db, {
        phone_e164: safe_from,
        reason: is_opt_out ? "opt_out_unknown_inbound" : "wrong_number_unknown_inbound",
        opt_out_keyword,
        metadata: {
          unknown_inbound: true,
          unknown_bucket: bucket,
          inbound_to: safe_to,
        },
      });
      suppression_applied = true;
    } catch (error) {
      runtimeDeps.warn("unknown_inbound.suppression_upsert_failed", {
        inbound_from: safe_from,
        bucket,
        message: error?.message || "suppression_failed",
      });
    }
  }

  if (!dry_run) {
    try {
      await insertUnknownInboundMessageEvent(db, {
        message_id,
        inbound_from: safe_from,
        inbound_to: safe_to,
        message_body,
        bucket,
        is_opt_out,
        opt_out_keyword,
        metadata: {
          suppression_applied,
          auto_reply_planned: should_auto_reply,
        },
      });
      message_event_created = true;
    } catch (error) {
      runtimeDeps.warn("unknown_inbound.message_event_failed", {
        inbound_from: safe_from,
        bucket,
        message: error?.message || "message_event_failed",
      });
    }
  }

  if (!dry_run && should_auto_reply && safe_from && safe_to) {
    try {
      await runtimeDeps.insertSupabaseSendQueueRow({
        queue_status: "queued",
        scheduled_for: new Date().toISOString(),
        to_phone_number: safe_from,
        from_phone_number: safe_to,
        message_body: reply_text,
        message_text: reply_text,
        message_type: "Unknown Inbound Auto Reply",
        use_case_template: "unknown_inbound_auto_reply",
        metadata: {
          unknown_inbound: true,
          unknown_bucket: bucket,
          inbound_message_body: clean(message_body) || null,
          auto_reply_reason: classification.reason,
          source: "textgrid_inbound_unknown_router",
        },
      });
      auto_reply_queued = true;
    } catch (error) {
      runtimeDeps.warn("unknown_inbound.auto_reply_queue_failed", {
        inbound_from: safe_from,
        bucket,
        message: error?.message || "queue_failed",
      });
    }
  }

  if (!dry_run && safe_from) {
    try {
      await upsertUnknownContactState(db, {
        phone_e164: safe_from,
        inbound_message_body: message_body,
        bucket,
        confidence: classification.confidence,
        auto_reply_sent: auto_reply_queued,
        metadata: {
          reason: classification.reason,
          opt_out_keyword,
          legal_sensitive,
          dry_run: false,
        },
      });
      unknown_contact_written = true;
    } catch (error) {
      runtimeDeps.warn("unknown_inbound.contact_upsert_failed", {
        inbound_from: safe_from,
        message: error?.message || "contact_upsert_failed",
      });
    }
  }

  if (!dry_run) {
    try {
      await runtimeDeps.notifyDiscordOps({
        event_type: is_opt_out
          ? "opt_out"
          : bucket === UNKNOWN_BUCKETS.WRONG_NUMBER
            ? "wrong_number"
            : "inbound_unknown",
        severity: is_opt_out ? "warning" : bucket === UNKNOWN_BUCKETS.SPAM ? "debug" : "info",
        domain: "inbound",
        title: is_opt_out ? "Unknown Inbound Opt-Out" : "Unknown Inbound SMS",
        summary: `From ${safe_from || "unknown"} to ${safe_to || "unknown"}: ${clean(message_body).slice(0, 200) || "(empty)"}`,
        fields: [
          { name: "Bucket", value: bucket, inline: true },
          { name: "Auto Reply Queued", value: String(auto_reply_queued), inline: true },
          { name: "Suppression Applied", value: String(suppression_applied), inline: true },
          { name: "Suggested Action", value: buildSuggestedAction(bucket), inline: false },
        ],
        metadata: {
          unknown_inbound: true,
          unknown_bucket: bucket,
          from: safe_from,
          to: safe_to,
        },
        should_alert_critical: is_opt_out,
      });
      discord_alert_sent = true;
    } catch (_) {
      // non-blocking by design
      discord_alert_sent = false;
    }
  }

  runtimeDeps.info("unknown_inbound.handled", {
    inbound_from: safe_from,
    inbound_to: safe_to,
    bucket,
    dry_run,
    auto_reply_queued,
    suppression_applied,
    message_event_created,
  });

  return {
    ok: true,
    route: "webhooks/textgrid/inbound",
    context: {
      found: false,
      unknown_inbound: true,
    },
    unknown_router: {
      bucket,
      auto_reply_queued,
      suppression_applied,
      message_event_created,
      discord_alert_sent,
      unknown_contact_written,
      dry_run,
      auto_reply_candidate: reply_text || null,
      auto_reply_blocked: blocked_auto_reply,
      auto_reply_recently_sent_within_24h: within_24h,
      classification_confidence: classification.confidence,
      opt_out_keyword,
      safe_from,
      safe_to,
    },
  };
}

export {
  UNKNOWN_BUCKETS,
  classifyUnknownInbound,
  selectUnknownReply,
};

export default handleUnknownInboundRouter;