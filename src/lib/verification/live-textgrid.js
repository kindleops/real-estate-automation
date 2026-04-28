import crypto from "node:crypto";

import {
  sendTextgridSMS,
} from "@/lib/providers/textgrid.js";
import {
  buildVerificationTextgridClientReferenceId,
  buildVerificationTextgridSendTriggerName,
  isVerificationTextgridSendEventItem,
  parseMessageEventMetadata,
  serializeMessageEventMetadata,
} from "@/lib/domain/events/message-event-metadata.js";
import { getSystemFlag } from "@/lib/system-control.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function toVerificationMessageBody(body = "", note = "") {
  const trimmed_body = clean(body);
  const trimmed_note = clean(note);

  if (trimmed_body) return trimmed_body;
  if (trimmed_note) return `Verification: ${trimmed_note}`;
  return `Verification ping ${new Date().toISOString()}`;
}

async function resolveLiveTextgridVerificationSendGate(deps = {}) {
  const env = deps.env || process.env;
  const get_system_flag = deps.getSystemFlag || getSystemFlag;
  const system_control_enabled = await get_system_flag("verification_textgrid_send_enabled");
  const env_enabled = clean(env.ALLOW_LIVE_TEXTGRID_VERIFICATION_SENDS) === "true";

  if (system_control_enabled && env_enabled) {
    return { ok: true };
  }

  return {
    ok: false,
    skipped: true,
    status: 423,
    reason: "verification_textgrid_send_disabled",
    system_control_enabled: Boolean(system_control_enabled),
    env_enabled,
  };
}

export async function runLiveTextgridSendVerification({
  to,
  from,
  body = "",
  note = "",
  confirm_live = false,
} = {}, deps = {}) {
  if (!confirm_live) {
    return {
      ok: false,
      reason: "confirm_live_required",
    };
  }

  const run_id = `textgrid-live-${crypto.randomUUID()}`;
  const client_reference_id = buildVerificationTextgridClientReferenceId(run_id);
  const message_body = toVerificationMessageBody(body, note);

  if (message_body.length > 320) {
    return {
      ok: false,
      reason: "verification_message_too_long",
      max_length: 320,
      character_count: message_body.length,
    };
  }

  const send_gate = await resolveLiveTextgridVerificationSendGate(deps);
  if (!send_gate.ok) {
    return {
      ok: false,
      ...send_gate,
      run_id,
      client_reference_id,
    };
  }

  const send_textgrid_sms = deps.sendTextgridSMS || sendTextgridSMS;
  let send_result;
  try {
    send_result = await send_textgrid_sms({
      to,
      from,
      body: message_body,
      client_reference_id,
      message_type: "sms",
    });
  } catch (error) {
    return {
      ok: false,
      reason: clean(error?.message) || "textgrid_send_failed",
      run_id,
      client_reference_id,
      send_result: {
        success: false,
        ok: false,
        sid: null,
        message_id: null,
        error_message: clean(error?.message) || "textgrid_send_failed",
        error_status: error?.status ?? null,
        error_data: error?.data ?? null,
        endpoint: error?.endpoint ?? null,
      },
    };
  }

  const provider_sid =
    clean(send_result?.sid) ||
    clean(send_result?.message_id) ||
    null;

  if (!provider_sid) {
    return {
      ok: false,
      reason: "SEND FAILED - NO SID",
      run_id,
      client_reference_id,
      send_result,
    };
  }

  const trigger_name = buildVerificationTextgridSendTriggerName(run_id);
  let event = null;
  let bookkeeping_error = null;

  try {
    const { createMessageEvent } = await import("@/lib/podio/apps/message-events.js");
    event = await createMessageEvent({
      "message-id": provider_sid,
      "text-2": provider_sid,
      "timestamp": { start: nowIso() },
      "trigger-name": trigger_name,
      "direction": "Outbound",
      "source-app": "Internal Verification",
      "processed-by": "Verification Harness",
      "message": message_body,
      "character-count": message_body.length,
      "status-3": "Sent",
      "status-2": clean(send_result.status) || "sent",
      "ai-output": serializeMessageEventMetadata({
        version: 1,
        event_kind: "verification_textgrid_send",
        verification_run_id: run_id,
        client_reference_id,
        provider_message_id: provider_sid,
      }),
    });
  } catch (error) {
    bookkeeping_error = clean(error?.message) || "verification_event_log_failed";
  }

  return {
    ok: true,
    run_id,
    client_reference_id,
    provider_message_id: provider_sid,
    event_item_id: event?.item_id || null,
    status: clean(send_result.status) || "sent",
    to: send_result.to,
    from: send_result.from,
    body: message_body,
    partial: Boolean(bookkeeping_error),
    bookkeeping_error,
  };
}

export async function getLiveTextgridVerificationStatus({
  run_id = null,
  provider_message_id = null,
} = {}) {
  const [{ getCategoryValue, getTextValue }, messageEvents] = await Promise.all([
    import("@/lib/providers/podio.js"),
    import("@/lib/podio/apps/message-events.js"),
  ]);
  const {
    findMessageEventsByMessageId,
    findMessageEventsByProviderMessageSid,
    findMessageEventsByTriggerName,
  } = messageEvents;

  const normalized_run_id = clean(run_id);
  const normalized_provider_message_id = clean(provider_message_id);

  const send_events = normalized_run_id
    ? await findMessageEventsByTriggerName(
        buildVerificationTextgridSendTriggerName(normalized_run_id),
        20,
        0
      )
    : normalized_provider_message_id
      ? await findMessageEventsByProviderMessageSid(normalized_provider_message_id, 50, 0)
      : [];

  const verification_send_events = sortNewestFirst(
    (Array.isArray(send_events) ? send_events : []).filter((event_item) =>
      isVerificationTextgridSendEventItem(event_item)
    )
  );

  const primary_send_event = verification_send_events[0] || null;
  const resolved_provider_message_id =
    normalized_provider_message_id ||
    clean(getTextValue(primary_send_event, "text-2", "")) ||
    clean(getTextValue(primary_send_event, "message-id", ""));

  const delivery_events = resolved_provider_message_id
    ? sortNewestFirst(
        await findMessageEventsByProviderMessageSid(resolved_provider_message_id, 50, 0)
      ).filter((event_item) => !isVerificationTextgridSendEventItem(event_item))
    : [];

  const latest_delivery_event = delivery_events[0] || null;
  const latest_delivery_status =
    clean(getCategoryValue(latest_delivery_event, "status-3", "")) ||
    clean(getCategoryValue(primary_send_event, "status-3", "")) ||
    null;

  const send_meta = parseMessageEventMetadata(primary_send_event);

  return {
    ok: Boolean(primary_send_event?.item_id),
    reason: primary_send_event?.item_id
      ? "verification_event_found"
      : "verification_event_not_found",
    run_id:
      normalized_run_id ||
      clean(send_meta?.verification_run_id) ||
      null,
    provider_message_id: resolved_provider_message_id || null,
    client_reference_id: clean(send_meta?.client_reference_id) || null,
    send_event_item_id: primary_send_event?.item_id || null,
    send_status: clean(getCategoryValue(primary_send_event, "status-3", "")) || null,
    latest_delivery_status: latest_delivery_status || null,
    delivery_event_count: delivery_events.length,
    latest_delivery_event_item_id: latest_delivery_event?.item_id || null,
  };
}

export default {
  runLiveTextgridSendVerification,
  getLiveTextgridVerificationStatus,
};
