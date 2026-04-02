import crypto from "node:crypto";

import {
  getObjectMetadata,
  getSignedUrl,
  getStorageConfigSummary,
  readFile,
  uploadFile,
} from "@/lib/providers/storage.js";
import { S3_STORAGE_PROVIDER } from "@/lib/providers/storage-s3.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function toVerificationBody(note = "") {
  const trimmed_note = clean(note);
  return [
    "REA live storage verification",
    `timestamp=${nowIso()}`,
    trimmed_note ? `note=${trimmed_note}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

export function getLiveStorageVerificationStatus() {
  return {
    ok: getStorageConfigSummary({
      provider_override: S3_STORAGE_PROVIDER,
    }).configured,
    provider: S3_STORAGE_PROVIDER,
    local: getStorageConfigSummary({
      provider_override: "local",
    }),
    s3: getStorageConfigSummary({
      provider_override: S3_STORAGE_PROVIDER,
    }),
  };
}

export async function runLiveStorageVerification({
  note = "",
  confirm_live = false,
  verify_signed_url = true,
  fetch_impl = globalThis.fetch,
} = {}) {
  if (!confirm_live) {
    return {
      ok: false,
      reason: "confirm_live_required",
    };
  }

  const config = getStorageConfigSummary({
    provider_override: S3_STORAGE_PROVIDER,
  });
  if (!config.configured) {
    return {
      ok: false,
      reason: "s3_storage_not_configured",
      missing: config.missing || [],
    };
  }

  const run_id = `storage-live-${crypto.randomUUID()}`;
  const key = `verification/storage/${run_id}/probe.txt`;
  const body = toVerificationBody(note);

  const upload = await uploadFile({
    key,
    body,
    content_type: "text/plain; charset=utf-8",
    filename: "probe.txt",
    metadata: {
      verification: true,
      run_id,
      channel: "live_storage_verification",
    },
    provider_override: S3_STORAGE_PROVIDER,
    fetch_impl,
  });

  if (!upload?.ok) {
    return {
      ok: false,
      reason: upload?.reason || "storage_upload_failed",
      run_id,
      key,
      upload,
    };
  }

  const metadata = await getObjectMetadata({
    key,
    provider_override: S3_STORAGE_PROVIDER,
    fetch_impl,
  });
  const read_result = await readFile({
    key,
    provider_override: S3_STORAGE_PROVIDER,
    fetch_impl,
  });
  const read_text =
    read_result?.ok && read_result?.body
      ? Buffer.from(read_result.body).toString("utf8")
      : "";
  const body_match = read_result?.ok && read_text === body;

  const signed = await getSignedUrl({
    key,
    provider_override: S3_STORAGE_PROVIDER,
    disposition: "inline",
    filename: "probe.txt",
    expires_in_seconds: 900,
  });

  let signed_url_check = {
    ok: false,
    reason: "signed_url_not_checked",
  };

  if (verify_signed_url) {
    if (!signed?.ok || !clean(signed?.url)) {
      signed_url_check = {
        ok: false,
        reason: signed?.reason || "signed_url_generation_failed",
      };
    } else if (typeof fetch_impl !== "function") {
      signed_url_check = {
        ok: false,
        reason: "storage_fetch_not_available",
      };
    } else {
      try {
        const response = await fetch_impl(signed.url, {
          method: "GET",
        });
        const response_text = response?.ok ? await response.text() : "";
        signed_url_check = {
          ok: Boolean(response?.ok) && response_text === body,
          reason: response?.ok
            ? response_text === body
              ? "signed_url_verified"
              : "signed_url_body_mismatch"
            : "signed_url_fetch_failed",
          status_code: response?.status || null,
        };
      } catch (error) {
        signed_url_check = {
          ok: false,
          reason: clean(error?.message) || "signed_url_fetch_failed",
        };
      }
    }
  }

  return {
    ok:
      metadata?.ok === true &&
      read_result?.ok === true &&
      body_match &&
      signed?.ok === true &&
      (!verify_signed_url || signed_url_check.ok),
    reason:
      metadata?.ok !== true
        ? metadata?.reason || "storage_metadata_failed"
        : read_result?.ok !== true
          ? read_result?.reason || "storage_read_failed"
          : !body_match
            ? "storage_body_mismatch"
            : signed?.ok !== true
              ? signed?.reason || "signed_url_generation_failed"
              : verify_signed_url && !signed_url_check.ok
                ? signed_url_check.reason
                : "live_storage_verification_completed",
    provider: S3_STORAGE_PROVIDER,
    run_id,
    key,
    upload,
    metadata,
    read: {
      ok: read_result?.ok === true,
      reason: read_result?.reason || null,
      body_match,
      content_type: read_result?.metadata?.content_type || null,
    },
    signed,
    signed_url_check,
  };
}

export default {
  getLiveStorageVerificationStatus,
  runLiveStorageVerification,
};
