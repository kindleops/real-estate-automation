import fs from "node:fs/promises";
import path from "node:path";

import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import {
  getObjectMetadata,
  getStorageConfigSummary,
  getStorageLocalRootPath,
  LOCAL_STORAGE_PROVIDER,
  readFile,
  uploadFile,
} from "@/lib/providers/storage.js";
import { S3_STORAGE_PROVIDER } from "@/lib/providers/storage-s3.js";

const DEFAULT_MIGRATION_LIMIT = 25;
const MAX_MIGRATION_LIMIT = 100;

function clean(value) {
  return String(value ?? "").trim();
}

function safeArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function toCountMap(results = []) {
  return results.reduce((acc, result) => {
    const key = clean(result?.status) || "unknown";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
}

function clampLimit(value, fallback = DEFAULT_MIGRATION_LIMIT) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(MAX_MIGRATION_LIMIT, Math.floor(parsed));
}

function normalizePrefix(prefix = "") {
  return clean(prefix).replace(/^\/+/, "").replace(/\\/g, "/");
}

function buildLocalArtifactPaths(key, local_root = getStorageLocalRootPath()) {
  const normalized_key = normalizePrefix(key);
  const file_path = path.join(local_root, normalized_key);
  return {
    key: normalized_key,
    file_path,
    metadata_path: `${file_path}.meta.json`,
  };
}

function parseManifest(body = null) {
  try {
    const parsed = JSON.parse(Buffer.isBuffer(body) ? body.toString("utf8") : String(body || ""));
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function isManifestArtifact(artifact = {}) {
  return (
    clean(artifact?.key).endsWith("/manifest.json") ||
    clean(artifact?.local_metadata?.filename) === "manifest.json" ||
    artifact?.local_metadata?.metadata?.manifest === true
  );
}

async function walkLocalStorage(root, prefix = "", limit = DEFAULT_MIGRATION_LIMIT) {
  const normalized_prefix = normalizePrefix(prefix);
  const keys = [];

  async function visit(current) {
    if (keys.length >= limit) return;
    const entries = await fs.readdir(current, { withFileTypes: true }).catch(() => []);

    for (const entry of entries) {
      if (keys.length >= limit) break;
      const next = path.join(current, entry.name);
      if (entry.isDirectory()) {
        await visit(next);
        continue;
      }

      if (!entry.isFile() || !entry.name.endsWith(".meta.json")) continue;

      const relative = path.relative(root, next).replace(/\\/g, "/");
      const key = relative.replace(/\.meta\.json$/, "");
      if (normalized_prefix && !key.startsWith(normalized_prefix)) continue;
      keys.push(key);
    }
  }

  await visit(root);
  return keys;
}

async function validateManifestTarget({
  manifest = null,
  fetch_impl = globalThis.fetch,
} = {}) {
  const file_keys = safeArray(manifest?.files).map((file) => clean(file?.key)).filter(Boolean);
  const missing_keys = [];

  for (const key of file_keys) {
    const metadata = await getObjectMetadata({
      key,
      provider_override: S3_STORAGE_PROVIDER,
      fetch_impl,
    });
    if (!metadata?.ok) {
      missing_keys.push(key);
    }
  }

  return {
    ok: missing_keys.length === 0,
    referenced_files: file_keys.length,
    missing_keys,
  };
}

async function inspectArtifact({
  key,
  fetch_impl = globalThis.fetch,
} = {}) {
  const local_metadata = await getObjectMetadata({
    key,
    provider_override: LOCAL_STORAGE_PROVIDER,
  });
  const target_metadata = await getObjectMetadata({
    key,
    provider_override: S3_STORAGE_PROVIDER,
    fetch_impl,
  });

  const local_meta = local_metadata?.metadata || null;
  const target_meta = target_metadata?.metadata || null;
  const sha256_match =
    clean(local_meta?.sha256) &&
    clean(local_meta?.sha256) === clean(target_meta?.sha256);

  let target_status = "missing";
  if (target_metadata?.ok && target_meta) {
    target_status = sha256_match ? "matching" : "conflict";
  }

  return {
    key,
    local_ok: local_metadata?.ok === true,
    target_ok: target_metadata?.ok === true,
    local_metadata: local_meta,
    target_metadata: target_meta,
    target_status,
    local_only: target_status === "missing",
    sha256_match,
  };
}

async function deleteLocalArtifact(key, local_root = getStorageLocalRootPath()) {
  const paths = buildLocalArtifactPaths(key, local_root);
  const deleted = {
    file_deleted: false,
    metadata_deleted: false,
  };

  try {
    await fs.unlink(paths.file_path);
    deleted.file_deleted = true;
  } catch (error) {
    if (error?.code !== "ENOENT") {
      return {
        ok: false,
        reason: "local_storage_delete_failed",
        error_message: clean(error?.message) || null,
        ...deleted,
      };
    }
  }

  try {
    await fs.unlink(paths.metadata_path);
    deleted.metadata_deleted = true;
  } catch (error) {
    if (error?.code !== "ENOENT") {
      return {
        ok: false,
        reason: "local_storage_metadata_delete_failed",
        error_message: clean(error?.message) || null,
        ...deleted,
      };
    }
  }

  return {
    ok: true,
    reason: "local_artifact_deleted",
    ...deleted,
  };
}

export async function listLocalStorageArtifacts({
  prefix = "",
  limit = DEFAULT_MIGRATION_LIMIT,
  fetch_impl = globalThis.fetch,
} = {}) {
  const local_root = getStorageLocalRootPath();
  const keys = await walkLocalStorage(local_root, prefix, clampLimit(limit));
  const artifacts = [];

  for (const key of keys) {
    artifacts.push(
      await inspectArtifact({
        key,
        fetch_impl,
      })
    );
  }

  return {
    ok: true,
    local_root,
    prefix: normalizePrefix(prefix),
    limit: clampLimit(limit),
    artifacts,
  };
}

export async function migrateLocalArtifactsToS3({
  keys = [],
  prefix = "",
  limit = DEFAULT_MIGRATION_LIMIT,
  dry_run = true,
  fetch_impl = globalThis.fetch,
} = {}) {
  const target = getStorageConfigSummary({
    provider_override: S3_STORAGE_PROVIDER,
  });

  if (!target.configured) {
    return {
      ok: false,
      migrated: false,
      reason: "s3_storage_not_configured",
      missing: target.missing || [],
    };
  }

  const selected_keys = safeArray(keys).map((key) => clean(key)).filter(Boolean);
  const scan = await listLocalStorageArtifacts({
    prefix,
    limit: selected_keys.length ? selected_keys.length : limit,
    fetch_impl,
  });

  const candidates = (scan.artifacts || []).filter((artifact) =>
    selected_keys.length ? selected_keys.includes(artifact.key) : true
  );

  if (dry_run) {
    return {
      ok: true,
      migrated: false,
      dry_run: true,
      reason: "storage_migration_preview_ready",
      target_provider: S3_STORAGE_PROVIDER,
      candidates,
      counts: toCountMap(candidates.map((artifact) => ({
        status:
          artifact.target_status === "matching"
            ? "already_present"
            : artifact.target_status === "conflict"
              ? "conflict"
              : "pending",
      }))),
    };
  }

  const results = [];

  const ordered_candidates = [...candidates].sort((left, right) => {
    const left_manifest = isManifestArtifact(left);
    const right_manifest = isManifestArtifact(right);
    if (left_manifest === right_manifest) return clean(left?.key).localeCompare(clean(right?.key));
    return left_manifest ? 1 : -1;
  });

  for (const artifact of ordered_candidates) {
    if (!artifact.local_ok || !artifact.local_metadata) {
      results.push({
        key: artifact.key,
        status: "failed",
        reason: "local_storage_metadata_missing",
      });
      continue;
    }

    if (artifact.target_status === "matching") {
      results.push({
        key: artifact.key,
        status: "already_present",
        reason: "matching_target_artifact_exists",
      });
      continue;
    }

    if (artifact.target_status === "conflict") {
      results.push({
        key: artifact.key,
        status: "conflict",
        reason: "target_artifact_sha256_conflict",
        local_sha256: artifact.local_metadata.sha256 || null,
        target_sha256: artifact.target_metadata?.sha256 || null,
      });
      continue;
    }

    const local_file = await readFile({
      key: artifact.key,
      provider_override: LOCAL_STORAGE_PROVIDER,
    });
    if (!local_file?.ok || !local_file?.body) {
      results.push({
        key: artifact.key,
        status: "failed",
        reason: local_file?.reason || "local_storage_read_failed",
      });
      continue;
    }

    const uploaded = await uploadFile({
      key: artifact.key,
      body: local_file.body,
      content_type: local_file.metadata?.content_type,
      filename: local_file.metadata?.filename,
      metadata: local_file.metadata?.metadata || {},
      provider_override: S3_STORAGE_PROVIDER,
      fetch_impl,
    });

    if (!uploaded?.ok) {
      results.push({
        key: artifact.key,
        status: "failed",
        reason: uploaded?.reason || "s3_storage_upload_failed",
        error_message: uploaded?.error_message || null,
      });
      continue;
    }

    const target_metadata = await getObjectMetadata({
      key: artifact.key,
      provider_override: S3_STORAGE_PROVIDER,
      fetch_impl,
    });
    const metadata_match =
      target_metadata?.ok &&
      clean(target_metadata?.metadata?.sha256) === clean(local_file.metadata?.sha256);
    let manifest_validation = null;

    if (
      clean(local_file.metadata?.filename) === "manifest.json" ||
      local_file.metadata?.metadata?.manifest === true
    ) {
      manifest_validation = await validateManifestTarget({
        manifest: parseManifest(local_file.body),
        fetch_impl,
      });
    }

    results.push({
      key: artifact.key,
      status:
        metadata_match && (!manifest_validation || manifest_validation.ok)
          ? "migrated"
          : "failed",
      reason:
        metadata_match && (!manifest_validation || manifest_validation.ok)
          ? "migrated_to_s3"
          : !metadata_match
            ? "target_metadata_verification_failed"
            : "manifest_reference_missing_after_migration",
      metadata_match,
      manifest_validation,
    });
  }

  const counts = toCountMap(results);
  const failed_count = Number(counts.failed || 0) + Number(counts.conflict || 0);

  if (failed_count > 0) {
    await recordSystemAlert({
      subsystem: "storage",
      code: "migration_failures",
      severity: failed_count === results.length ? "high" : "warning",
      retryable: true,
      summary: `Storage migration completed with ${failed_count} failure/conflict item(s).`,
      dedupe_key: `storage-migration:${normalizePrefix(prefix) || "manual"}`,
      metadata: {
        prefix: normalizePrefix(prefix) || null,
        counts,
      },
    }).catch(() => null);
  } else {
    await resolveSystemAlert({
      subsystem: "storage",
      code: "migration_failures",
      dedupe_key: `storage-migration:${normalizePrefix(prefix) || "manual"}`,
      resolution_message: "Storage migration completed without failures.",
      metadata: {
        counts,
      },
    }).catch(() => null);
  }

  return {
    ok: failed_count === 0,
    migrated: counts.migrated > 0,
    dry_run: false,
    reason: failed_count === 0 ? "storage_migration_completed" : "storage_migration_partial_failure",
    target_provider: S3_STORAGE_PROVIDER,
    counts,
    results,
  };
}

export async function cleanupVerifiedLocalArtifacts({
  keys = [],
  prefix = "",
  limit = DEFAULT_MIGRATION_LIMIT,
  dry_run = true,
  fetch_impl = globalThis.fetch,
} = {}) {
  const target = getStorageConfigSummary({
    provider_override: S3_STORAGE_PROVIDER,
  });

  if (!target.configured) {
    return {
      ok: false,
      cleaned_up: false,
      reason: "s3_storage_not_configured",
      missing: target.missing || [],
    };
  }

  const selected_keys = safeArray(keys).map((key) => clean(key)).filter(Boolean);
  const scan = await listLocalStorageArtifacts({
    prefix,
    limit: selected_keys.length ? selected_keys.length : limit,
    fetch_impl,
  });
  const candidates = (scan.artifacts || []).filter((artifact) =>
    selected_keys.length ? selected_keys.includes(artifact.key) : true
  );
  const local_root = scan.local_root || getStorageLocalRootPath();
  const results = [];

  for (const artifact of candidates) {
    if (!artifact.local_ok) {
      results.push({
        key: artifact.key,
        status: "local_missing",
        reason: "local_artifact_not_found",
      });
      continue;
    }

    if (artifact.target_status !== "matching") {
      results.push({
        key: artifact.key,
        status:
          artifact.target_status === "conflict" ? "conflict" : "not_cleanup_eligible",
        reason:
          artifact.target_status === "conflict"
            ? "target_artifact_sha256_conflict"
            : "target_artifact_not_verified",
      });
      continue;
    }

    if (dry_run) {
      results.push({
        key: artifact.key,
        status: "cleanup_eligible",
        reason: "verified_target_artifact_exists",
      });
      continue;
    }

    const deletion = await deleteLocalArtifact(artifact.key, local_root);
    results.push({
      key: artifact.key,
      status: deletion?.ok ? "deleted_local" : "failed",
      reason: deletion?.reason || "local_cleanup_failed",
      file_deleted: Boolean(deletion?.file_deleted),
      metadata_deleted: Boolean(deletion?.metadata_deleted),
      error_message: deletion?.error_message || null,
    });
  }

  const counts = toCountMap(results);
  const failed_count = Number(counts.failed || 0) + Number(counts.conflict || 0);

  if (!dry_run) {
    if (failed_count > 0) {
      await recordSystemAlert({
        subsystem: "storage",
        code: "cleanup_failures",
        severity: failed_count === results.length ? "high" : "warning",
        retryable: true,
        summary: `Storage cleanup completed with ${failed_count} failure/conflict item(s).`,
        dedupe_key: `storage-cleanup:${normalizePrefix(prefix) || "manual"}`,
        metadata: {
          prefix: normalizePrefix(prefix) || null,
          counts,
        },
      }).catch(() => null);
    } else {
      await resolveSystemAlert({
        subsystem: "storage",
        code: "cleanup_failures",
        dedupe_key: `storage-cleanup:${normalizePrefix(prefix) || "manual"}`,
        resolution_message: "Verified local storage cleanup completed without failures.",
        metadata: {
          counts,
        },
      }).catch(() => null);
    }
  }

  return {
    ok: failed_count === 0,
    cleaned_up: dry_run ? false : Number(counts.deleted_local || 0) > 0,
    dry_run,
    reason: dry_run
      ? "storage_cleanup_preview_ready"
      : failed_count === 0
        ? "storage_cleanup_completed"
        : "storage_cleanup_partial_failure",
    target_provider: S3_STORAGE_PROVIDER,
    counts,
    results,
  };
}

export default {
  listLocalStorageArtifacts,
  migrateLocalArtifactsToS3,
  cleanupVerifiedLocalArtifacts,
};
