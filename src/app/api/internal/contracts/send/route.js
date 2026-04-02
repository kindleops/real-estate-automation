import { NextResponse } from "next/server";

import { getRolloutControls, resolveMutationDryRun, resolveScopedId } from "@/lib/config/rollout-controls.js";
import { child } from "@/lib/logging/logger.js";
import { sendContract } from "@/lib/domain/contracts/send-contract.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.contracts.send",
});

function clean(value) {
  return String(value ?? "").trim();
}

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

function statusForResult(result) {
  const reason = clean(result?.reason).toLowerCase();
  if (reason.includes("not_implemented")) return 501;
  return result?.ok === false ? 400 : 200;
}

export async function GET(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const rollout = getRolloutControls();
    const contract_scope = resolveScopedId({
      requested_id: asNumber(searchParams.get("contract_id")),
      safe_id: rollout.single_contract_id,
      resource: "contract",
    });
    if (!contract_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: contract_scope.reason,
        },
        { status: 400 }
      );
    }
    const subject = clean(searchParams.get("subject"));
    const template_id = clean(searchParams.get("template_id"));
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(searchParams.get("dry_run"), false),
    });

    logger.info("contracts_send.requested", {
      method: "GET",
      contract_id: contract_scope.effective_id,
      template_id: template_id || null,
      dry_run: dry_run_resolution.effective_dry_run,
    });

    const result = await sendContract({
      contract_id: contract_scope.effective_id,
      subject: subject || null,
      template_id: template_id || null,
      dry_run: dry_run_resolution.effective_dry_run,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/contracts/send",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("contracts_send.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "contracts_send_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const body = await request.json().catch(() => ({}));
    const rollout = getRolloutControls();
    const contract_scope = resolveScopedId({
      requested_id: asNumber(body?.contract_id),
      safe_id: rollout.single_contract_id,
      resource: "contract",
    });
    if (!contract_scope.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: contract_scope.reason,
        },
        { status: 400 }
      );
    }
    const subject = clean(body?.subject);
    const template_id = clean(body?.template_id);
    const email_blurb = clean(body?.email_blurb);
    const dry_run_resolution = resolveMutationDryRun({
      requested_dry_run: asBoolean(body?.dry_run, false),
    });
    const auto_send = asBoolean(body?.auto_send, true);
    const documents = Array.isArray(body?.documents) ? body.documents : [];
    const signers = Array.isArray(body?.signers) ? body.signers : [];
    const metadata =
      body?.metadata && typeof body.metadata === "object" && !Array.isArray(body.metadata)
        ? body.metadata
        : {};

    logger.info("contracts_send.requested", {
      method: "POST",
      contract_id: contract_scope.effective_id,
      template_id: template_id || null,
      document_count: documents.length,
      signer_count: signers.length,
      dry_run: dry_run_resolution.effective_dry_run,
    });

    const result = await sendContract({
      contract_id: contract_scope.effective_id,
      subject: subject || null,
      documents,
      signers,
      template_id: template_id || null,
      email_blurb,
      metadata,
      dry_run: dry_run_resolution.effective_dry_run,
      auto_send,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/contracts/send",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("contracts_send.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "contracts_send_failed",
      },
      { status: 500 }
    );
  }
}
