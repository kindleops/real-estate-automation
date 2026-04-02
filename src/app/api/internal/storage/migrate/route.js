import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import {
  cleanupVerifiedLocalArtifacts,
  listLocalStorageArtifacts,
  migrateLocalArtifactsToS3,
} from "@/lib/domain/documents/storage-migration.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.internal.storage.migrate",
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

function asNumber(value, fallback = 25) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseKeys(value) {
  if (Array.isArray(value)) return value.map((entry) => clean(entry)).filter(Boolean);
  return clean(value)
    .split(",")
    .map((entry) => clean(entry))
    .filter(Boolean);
}

function statusForResult(result) {
  if (result?.ok === false && result?.reason === "s3_storage_not_configured") return 400;
  return result?.ok === false ? 409 : 200;
}

async function runAction({
  action = "preview",
  prefix = "",
  limit = 25,
  dry_run = true,
  keys = [],
} = {}) {
  const normalized_action = clean(action).toLowerCase() || "preview";

  if (normalized_action === "cleanup") {
    return cleanupVerifiedLocalArtifacts({
      prefix,
      limit,
      dry_run,
      keys,
    });
  }

  if (dry_run || normalized_action === "preview") {
    return listLocalStorageArtifacts({
      prefix,
      limit,
    });
  }

  return migrateLocalArtifactsToS3({
    prefix,
    limit,
    dry_run: false,
    keys,
  });
}

export async function GET(request) {
  try {
    const auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });
    if (!auth.authorized) return auth.response;

    const { searchParams } = new URL(request.url);
    const result = await runAction({
      action: clean(searchParams.get("action")) || "preview",
      prefix: clean(searchParams.get("prefix")),
      limit: asNumber(searchParams.get("limit"), 25),
      dry_run: asBoolean(searchParams.get("dry_run"), true),
      keys: parseKeys(searchParams.get("keys")),
    });

    return NextResponse.json(result, {
      status: statusForResult(result),
    });
  } catch (error) {
    logger.error("storage.migrate_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "storage_migration_failed",
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
    const result = await runAction({
      action: clean(body?.action) || "preview",
      prefix: clean(body?.prefix),
      limit: asNumber(body?.limit, 25),
      dry_run: asBoolean(body?.dry_run, true),
      keys: parseKeys(body?.keys),
    });

    return NextResponse.json(result, {
      status: statusForResult(result),
    });
  } catch (error) {
    logger.error("storage.migrate_failed", { error });
    return NextResponse.json(
      {
        ok: false,
        error: "storage_migration_failed",
      },
      { status: 500 }
    );
  }
}
