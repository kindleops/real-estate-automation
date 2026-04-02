import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import ENV from "@/lib/config/env.js";
import { uploadFile } from "@/lib/providers/storage.js";
import {
  cleanupVerifiedLocalArtifacts,
  listLocalStorageArtifacts,
  migrateLocalArtifactsToS3,
} from "@/lib/domain/documents/storage-migration.js";

function createFakeS3Fetch() {
  const objects = new Map();

  return async (url, options = {}) => {
    const target = new URL(url);
    const storage_key = `${target.host}${target.pathname}`;
    const method = String(options.method || "GET").toUpperCase();

    if (method === "PUT") {
      const body = Buffer.isBuffer(options.body)
        ? options.body
        : Buffer.from(options.body || "");
      objects.set(storage_key, {
        body,
        content_type:
          options.headers?.["content-type"] ||
          options.headers?.["Content-Type"] ||
          "application/octet-stream",
      });

      return {
        ok: true,
        status: 200,
        headers: {
          get: () => null,
        },
        arrayBuffer: async () => body,
        text: async () => "",
      };
    }

    if (method === "GET") {
      const object = objects.get(storage_key);
      if (!object) {
        return {
          ok: false,
          status: 404,
          headers: {
            get: () => null,
          },
          arrayBuffer: async () => new ArrayBuffer(0),
          text: async () => "",
        };
      }

      return {
        ok: true,
        status: 200,
        headers: {
          get: (name) =>
            String(name || "").toLowerCase() === "content-type"
              ? object.content_type
              : null,
        },
        arrayBuffer: async () =>
          object.body.buffer.slice(
            object.body.byteOffset,
            object.body.byteOffset + object.body.byteLength
          ),
        text: async () => object.body.toString("utf8"),
      };
    }

    return {
      ok: false,
      status: 405,
      headers: {
        get: () => null,
      },
      arrayBuffer: async () => new ArrayBuffer(0),
      text: async () => "",
    };
  };
}

test("storage migration previews and migrates local artifacts into S3-compatible storage", async () => {
  const original = {
    STORAGE_PROVIDER: ENV.STORAGE_PROVIDER,
    STORAGE_LOCAL_ROOT: ENV.STORAGE_LOCAL_ROOT,
    STORAGE_BUCKET: ENV.STORAGE_BUCKET,
    STORAGE_REGION: ENV.STORAGE_REGION,
    STORAGE_S3_ENDPOINT: ENV.STORAGE_S3_ENDPOINT,
    STORAGE_S3_ACCESS_KEY_ID: ENV.STORAGE_S3_ACCESS_KEY_ID,
    STORAGE_S3_SECRET_ACCESS_KEY: ENV.STORAGE_S3_SECRET_ACCESS_KEY,
    STORAGE_S3_FORCE_PATH_STYLE: ENV.STORAGE_S3_FORCE_PATH_STYLE,
  };
  const local_root = await fs.mkdtemp(path.join(os.tmpdir(), "rea-storage-migration-"));
  const fakeFetch = createFakeS3Fetch();

  try {
    ENV.STORAGE_PROVIDER = "local";
    ENV.STORAGE_LOCAL_ROOT = local_root;
    ENV.STORAGE_BUCKET = "deal-docs";
    ENV.STORAGE_REGION = "us-east-1";
    ENV.STORAGE_S3_ENDPOINT = "https://s3.example.test";
    ENV.STORAGE_S3_ACCESS_KEY_ID = "AKIA_TEST_ACCESS";
    ENV.STORAGE_S3_SECRET_ACCESS_KEY = "secret-test-key";
    ENV.STORAGE_S3_FORCE_PATH_STYLE = true;

    await uploadFile({
      key: "tests/migration/opportunity-summary.txt",
      body: "buyer package summary",
      content_type: "text/plain",
      filename: "opportunity-summary.txt",
      provider_override: "local",
    });

    await uploadFile({
      key: "tests/migration/manifest.json",
      body: JSON.stringify({
        files: [
          {
            key: "tests/migration/opportunity-summary.txt",
          },
        ],
      }),
      content_type: "application/json",
      filename: "manifest.json",
      metadata: {
        manifest: true,
      },
      provider_override: "local",
    });

    const preview = await listLocalStorageArtifacts({
      prefix: "tests/migration",
      limit: 10,
      fetch_impl: fakeFetch,
    });

    assert.equal(preview.ok, true);
    assert.equal(preview.artifacts.length, 2);
    assert.equal(preview.artifacts.every((artifact) => artifact.local_only), true);

    const migrated = await migrateLocalArtifactsToS3({
      prefix: "tests/migration",
      limit: 10,
      dry_run: false,
      fetch_impl: fakeFetch,
    });

    assert.equal(migrated.ok, true);
    assert.equal(migrated.counts.migrated, 2);
    assert.equal(
      migrated.results.find((result) => result.key === "tests/migration/manifest.json")?.manifest_validation?.ok,
      true
    );

    const cleanupPreview = await cleanupVerifiedLocalArtifacts({
      prefix: "tests/migration",
      limit: 10,
      dry_run: true,
      fetch_impl: fakeFetch,
    });

    assert.equal(cleanupPreview.ok, true);
    assert.equal(cleanupPreview.counts.cleanup_eligible, 2);

    const cleanup = await cleanupVerifiedLocalArtifacts({
      prefix: "tests/migration",
      limit: 10,
      dry_run: false,
      fetch_impl: fakeFetch,
    });

    assert.equal(cleanup.ok, true);
    assert.equal(cleanup.counts.deleted_local, 2);

    const afterCleanup = await listLocalStorageArtifacts({
      prefix: "tests/migration",
      limit: 10,
      fetch_impl: fakeFetch,
    });
    assert.equal(afterCleanup.artifacts.length, 0);
  } finally {
    ENV.STORAGE_PROVIDER = original.STORAGE_PROVIDER;
    ENV.STORAGE_LOCAL_ROOT = original.STORAGE_LOCAL_ROOT;
    ENV.STORAGE_BUCKET = original.STORAGE_BUCKET;
    ENV.STORAGE_REGION = original.STORAGE_REGION;
    ENV.STORAGE_S3_ENDPOINT = original.STORAGE_S3_ENDPOINT;
    ENV.STORAGE_S3_ACCESS_KEY_ID = original.STORAGE_S3_ACCESS_KEY_ID;
    ENV.STORAGE_S3_SECRET_ACCESS_KEY = original.STORAGE_S3_SECRET_ACCESS_KEY;
    ENV.STORAGE_S3_FORCE_PATH_STYLE = original.STORAGE_S3_FORCE_PATH_STYLE;
    await fs.rm(local_root, { recursive: true, force: true });
  }
});
