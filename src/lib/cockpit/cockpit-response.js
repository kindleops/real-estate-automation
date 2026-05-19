import { NextResponse } from "next/server";

/**
 * Build the standard cockpit response envelope.
 * Shape: { ok, action, dry_run?, blocked?, reason?, queue_item_id?, thread_key?, diagnostics? }
 */
export function cockpitOk(action, payload = {}, status = 200) {
  return NextResponse.json({ ok: true, action, ...payload }, { status });
}

export function cockpitBlocked(action, reason, diagnostics = null, status = 422) {
  return NextResponse.json(
    { ok: false, blocked: true, action, reason, ...(diagnostics ? { diagnostics } : {}) },
    { status }
  );
}

export function cockpitError(action, error, status = 500) {
  return NextResponse.json({ ok: false, action, error }, { status });
}

export function cockpitValidationError(action, error, status = 400) {
  return NextResponse.json({ ok: false, action, error }, { status });
}

export function cockpitDisabled(action, flag_key) {
  return NextResponse.json(
    { ok: false, blocked: true, action, reason: "system_control_disabled", flag_key },
    { status: 423 }
  );
}

export function cockpitUnauthorized(action) {
  return NextResponse.json({ ok: false, action, error: "cockpit_unauthorized" }, { status: 401 });
}

export function cockpitNotReady(action, notes = "") {
  return NextResponse.json(
    {
      ok: false,
      action,
      error: "BACKEND_ENDPOINT_NOT_READY",
      notes: notes || "This endpoint requires additional backend wiring before it is safe to use.",
    },
    { status: 501 }
  );
}
