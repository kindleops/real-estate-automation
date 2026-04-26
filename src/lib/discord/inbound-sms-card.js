import { resolveChannelForEvent } from "@/lib/discord/discord-channel-router.js";
import { buildInboundSmsActionComponents } from "@/lib/discord/discord-components/sms-reply-components.js";
import { clean } from "@/lib/utils/strings.js";
import { normalizePhone } from "@/lib/utils/phones.js";

function ensureObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function getStreetAddress(value = "") {
  const normalized = clean(String(value || "").replace(/\s+/g, " "));
  if (!normalized) return "";
  const comma_index = normalized.indexOf(",");
  return comma_index === -1 ? normalized : clean(normalized.slice(0, comma_index));
}

function maskPhone(value = "") {
  const phone = normalizePhone(value);
  if (!phone) return "unknown";
  return `${phone.slice(0, 2)}******${phone.slice(-2)}`;
}

function preview(value = "", max = 240) {
  return clean(String(value || "").replace(/\s+/g, " ")).slice(0, max) || "n/a";
}

function boolText(value) {
  return value ? "yes" : "no";
}

export function buildInboundSmsCard({
  message_event_id = "",
  inbound_from = "",
  seller_name = "",
  property_address = "",
  market = "",
  current_stage = "",
  classification_intent = "",
  classification_result = "",
  language = "",
  inbound_message_body = "",
  suggested_reply_preview = "",
  selected_template_id = "",
  selected_template_source = "",
  confidence = null,
  safety_state = "",
  autopilot_enabled = false,
  autopilot_status = "review_required",
  outbound_queue_id = "",
  context_incomplete = false,
  channel_warning = "",
} = {}) {
  const review_mode = context_incomplete ? "manual_only" : "full";
  const embed = {
    title: "📩 Inbound SMS Reply",
    color: context_incomplete ? 0xf59e0b : 0x3b82f6,
    fields: [
      { name: "Seller Phone", value: maskPhone(inbound_from), inline: true },
      { name: "Seller", value: clean(seller_name) || "unknown", inline: true },
      { name: "Address", value: getStreetAddress(property_address) || "unknown", inline: false },
      { name: "Market", value: clean(market) || "unknown", inline: true },
      { name: "Current Stage", value: clean(current_stage) || "unknown", inline: true },
      { name: "Intent", value: clean(classification_intent) || "unknown", inline: true },
      { name: "Classification", value: clean(classification_result) || "unknown", inline: true },
      { name: "Language", value: clean(language) || "unknown", inline: true },
      { name: "Review Status", value: clean(safety_state) || (context_incomplete ? "Manual review required — context incomplete" : "review_required"), inline: true },
      { name: "Autopilot", value: clean(autopilot_status) || `${boolText(autopilot_enabled)}`, inline: true },
      { name: "Inbound Message", value: preview(inbound_message_body, 900), inline: false },
      { name: "Suggested Reply", value: preview(suggested_reply_preview, 900), inline: false },
      { name: "Template", value: clean(selected_template_id) ? `${clean(selected_template_id)} (${clean(selected_template_source) || "unknown"})` : "unresolved", inline: true },
      { name: "Queue ID", value: clean(outbound_queue_id) || "not queued", inline: true },
      { name: "Confidence", value: confidence === null || confidence === undefined ? "unknown" : String(confidence), inline: true },
      { name: "Message Event", value: clean(message_event_id) || "untracked", inline: true },
    ],
    footer: {
      text: clean(channel_warning) || `message_event_id=${clean(message_event_id) || "untracked"}`,
    },
  };

  return {
    embeds: [embed],
    components: buildInboundSmsActionComponents({
      message_event_id,
      suggested_reply: suggested_reply_preview,
      review_mode,
    }),
    review_mode,
  };
}

const defaultDeps = {
  fetch: globalThis.fetch,
};

let runtimeDeps = { ...defaultDeps };

export function __setInboundSmsCardDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetInboundSmsCardDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function postInboundSmsDiscordCard(payload = {}, opts = {}) {
  const env = opts.env || process.env;
  const fetch_impl = opts.fetch || runtimeDeps.fetch;
  const token = clean(env.DISCORD_BOT_TOKEN);
  const resolution = resolveChannelForEvent("inbound_sms_reply", { env });
  const channel_warning = resolution.fallback ? "warning: inbound_replies channel not configured, fallback used" : "";
  const existing_metadata = ensureObject(payload.existing_metadata);

  if (!opts.force && (clean(existing_metadata.discord_message_id) || clean(existing_metadata.discord_card_posted_at))) {
    return {
      ok: true,
      skipped: true,
      reason: "discord_card_already_posted",
      channel_id: resolution.channel_id,
      channel_key: resolution.channel_key,
      fallback: resolution.fallback,
    };
  }

  const card = buildInboundSmsCard({
    ...payload,
    channel_warning,
  });

  if (!token || !resolution.channel_id || typeof fetch_impl !== "function") {
    return {
      ok: false,
      skipped: true,
      reason: "discord_not_configured",
      channel_id: resolution.channel_id,
      channel_key: resolution.channel_key,
      fallback: resolution.fallback,
      card,
    };
  }

  const response = await fetch_impl(`https://discord.com/api/v10/channels/${resolution.channel_id}/messages`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bot ${token}`,
    },
    body: JSON.stringify({
      embeds: card.embeds,
      components: card.components,
      allowed_mentions: { parse: [] },
    }),
  }).catch(() => null);

  if (!response?.ok) {
    return {
      ok: false,
      reason: "discord_post_failed",
      channel_id: resolution.channel_id,
      channel_key: resolution.channel_key,
      fallback: resolution.fallback,
      status: response?.status || null,
      card,
    };
  }

  const data = await response.json().catch(() => ({}));
  return {
    ok: true,
    channel_id: resolution.channel_id,
    channel_key: resolution.channel_key,
    fallback: resolution.fallback,
    discord_message_id: clean(data?.id) || null,
    card,
  };
}
