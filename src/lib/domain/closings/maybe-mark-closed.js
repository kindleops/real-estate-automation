// ─── maybe-mark-closed.js ────────────────────────────────────────────────
import { CLOSING_FIELDS, getClosingItem } from "@/lib/podio/apps/closings.js";
import { updateClosingStatus } from "@/lib/domain/closings/update-closing-status.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function includesAny(text, needles = []) {
  const normalized = lower(text);
  return needles.some((needle) => normalized.includes(lower(needle)));
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);

  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.value?.item_id) return first.value.item_id;
  if (first?.start) return first.start;

  return null;
}

function isAlreadyClosed(closing_item = null) {
  const closing_status = clean(
    getFieldValue(closing_item, CLOSING_FIELDS.closing_status)
  );
  return ["closed", "completed"].includes(lower(closing_status));
}

function normalizeCloseSignal({
  event = "",
  status = "",
  subject = "",
  body = "",
} = {}) {
  const combined = `${clean(event)} ${clean(status)} ${clean(subject)} ${clean(body)}`;
  const text = lower(combined);

  if (
    includesAny(text, [
      "closed",
      "closing complete",
      "deal complete",
      "file complete",
      "completed closing",
    ])
  ) {
    return {
      should_close: true,
      reason: "closed_signal_detected",
      source_signal: "closed",
    };
  }

  if (
    includesAny(text, [
      "funded",
      "funding complete",
      "wired",
      "disbursed",
      "disbursement complete",
      "funds sent",
      "funds released",
    ])
  ) {
    return {
      should_close: true,
      reason: "funded_signal_detected",
      source_signal: "funded",
    };
  }

  if (
    includesAny(text, [
      "recorded",
      "recording confirmed",
      "deed recorded",
      "document recorded",
    ])
  ) {
    return {
      should_close: true,
      reason: "recorded_signal_detected",
      source_signal: "recorded",
    };
  }

  return {
    should_close: false,
    reason: "no_close_signal",
    source_signal: null,
  };
}

export async function maybeMarkClosed({
  closing_item_id = null,
  closing_item = null,
  event = "",
  status = "",
  subject = "",
  body = "",
  notes = "",
} = {}) {
  let resolved_closing_item = closing_item || null;

  if (!resolved_closing_item && closing_item_id) {
    resolved_closing_item = await getClosingItem(closing_item_id);
  }

  const resolved_closing_item_id =
    resolved_closing_item?.item_id ||
    closing_item_id ||
    null;

  if (!resolved_closing_item_id) {
    return {
      ok: false,
      updated: false,
      reason: "missing_closing_item_id",
    };
  }

  if (resolved_closing_item && isAlreadyClosed(resolved_closing_item)) {
    return {
      ok: true,
      updated: false,
      reason: "closing_already_closed",
      closing_item_id: resolved_closing_item_id,
      closing_status: "Completed",
    };
  }

  const decision = normalizeCloseSignal({
    event,
    status,
    subject,
    body,
  });

  if (!decision.should_close) {
    return {
      ok: true,
      updated: false,
      reason: decision.reason,
      closing_item_id: resolved_closing_item_id,
      source_signal: decision.source_signal,
    };
  }

  const update_result = await updateClosingStatus({
    closing_item_id: resolved_closing_item_id,
    closing_item: resolved_closing_item,
    status: "Completed",
    notes:
      clean(notes) ||
      `Closing marked closed from ${decision.source_signal} signal.`,
  });

  return {
    ok: Boolean(update_result?.ok),
    updated: Boolean(update_result?.updated),
    reason: update_result?.reason || decision.reason,
    closing_item_id: resolved_closing_item_id,
    closing_status: "Completed",
    source_signal: decision.source_signal,
    update_result,
  };
}

export default maybeMarkClosed;
