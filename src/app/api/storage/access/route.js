import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { readFile, verifySignedStorageAccess } from "@/lib/providers/storage.js";
import { requireSharedSecretAuth } from "@/lib/security/shared-secret.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.storage.access",
});

function clean(value) {
  return String(value ?? "").trim();
}

function buildDispositionHeader(filename = "", disposition = "inline") {
  const normalized_disposition = clean(disposition) || "inline";
  const normalized_filename = clean(filename).replace(/["\r\n]+/g, " ");

  if (!normalized_filename) {
    return normalized_disposition;
  }

  return `${normalized_disposition}; filename="${normalized_filename}"`;
}

export async function GET(request) {
  try {
    const { searchParams } = new URL(request.url);
    const key = clean(searchParams.get("key"));
    const expires = clean(searchParams.get("expires"));
    const signature = clean(searchParams.get("signature"));
    const disposition = clean(searchParams.get("disposition")) || "inline";
    const requested_filename = clean(searchParams.get("filename"));

    if (!key) {
      return NextResponse.json(
        {
          ok: false,
          error: "missing_storage_key",
        },
        { status: 400 }
      );
    }

    const internal_auth = requireSharedSecretAuth(request, logger, {
      env_name: "INTERNAL_API_SECRET",
      header_names: ["x-internal-api-secret"],
    });

    if (!internal_auth.authorized) {
      const signed = verifySignedStorageAccess({
        key,
        expires,
        signature,
        disposition,
        filename: requested_filename,
      });

      if (!signed.ok) {
        return NextResponse.json(
          {
            ok: false,
            error: signed.reason,
          },
          { status: signed.reason === "storage_signature_expired" ? 410 : 401 }
        );
      }
    }

    const stored = await readFile({ key });
    if (!stored?.ok || !stored?.body) {
      return NextResponse.json(
        {
          ok: false,
          error: stored?.reason || "storage_object_not_found",
        },
        { status: stored?.reason === "storage_object_not_found" ? 404 : 500 }
      );
    }

    const metadata = stored.metadata || {};
    const filename = requested_filename || clean(metadata.filename);

    return new NextResponse(new Uint8Array(stored.body), {
      status: 200,
      headers: {
        "Content-Type": clean(metadata.content_type) || "application/octet-stream",
        "Content-Length": String(stored.body.byteLength || 0),
        "Content-Disposition": buildDispositionHeader(filename, disposition),
        "Cache-Control": "private, max-age=60",
      },
    });
  } catch (error) {
    logger.error("storage.access_failed", { error });

    return NextResponse.json(
      {
        ok: false,
        error: "storage_access_failed",
      },
      { status: 500 }
    );
  }
}
