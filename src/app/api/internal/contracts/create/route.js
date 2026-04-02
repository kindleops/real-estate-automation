import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { maybeCreateContractFromAcceptedOffer } from "@/lib/domain/contracts/maybe-create-contract-from-accepted-offer.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.contracts.create",
});

function clean(value) {
  return String(value ?? "").trim();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function statusForResult(result) {
  return result?.ok === false ? 400 : 200;
}

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);

    const offer_item_id = asNumber(searchParams.get("offer_item_id"));
    const auto_send = asBoolean(searchParams.get("auto_send"), false);
    const dry_run = asBoolean(searchParams.get("dry_run"), false);
    const template_id = clean(searchParams.get("template_id"));
    const subject = clean(searchParams.get("subject"));

    logger.info("contracts_create.requested", {
      method: "GET",
      offer_item_id,
      auto_send,
      dry_run,
      template_id: template_id || null,
    });

    const result = await maybeCreateContractFromAcceptedOffer({
      offer_item_id,
      auto_send,
      dry_run,
      template_id: template_id || null,
      subject: subject || null,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/contracts/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("contracts_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "contracts_create_failed",
      },
      { status: 500 }
    );
  }
}

export async function POST(request) {
  try {
    const body = await request.json().catch(() => ({}));

    const offer_item_id = asNumber(body?.offer_item_id);
    const auto_send = asBoolean(body?.auto_send, false);
    const dry_run = asBoolean(body?.dry_run, false);
    const template_id = clean(body?.template_id);
    const subject = clean(body?.subject);
    const email_blurb = clean(body?.email_blurb);
    const documents = Array.isArray(body?.documents) ? body.documents : [];
    const signers = Array.isArray(body?.signers) ? body.signers : [];
    const metadata =
      body?.metadata && typeof body.metadata === "object" && !Array.isArray(body.metadata)
        ? body.metadata
        : {};

    logger.info("contracts_create.requested", {
      method: "POST",
      offer_item_id,
      auto_send,
      dry_run,
      template_id: template_id || null,
      document_count: documents.length,
      signer_count: signers.length,
    });

    const result = await maybeCreateContractFromAcceptedOffer({
      offer_item_id,
      auto_send,
      dry_run,
      template_id: template_id || null,
      subject: subject || null,
      email_blurb,
      documents,
      signers,
      metadata,
    });

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "internal/contracts/create",
        result,
      },
      { status: statusForResult(result) }
    );
  } catch (error) {
    logger.error("contracts_create.failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "contracts_create_failed",
      },
      { status: 500 }
    );
  }
}
