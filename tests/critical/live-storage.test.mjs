import test from "node:test";
import assert from "node:assert/strict";

import ENV from "@/lib/config/env.js";
import { runLiveStorageVerification } from "@/lib/verification/live-storage.js";

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

test("live storage verification requires explicit confirm_live", async () => {
  const result = await runLiveStorageVerification({
    confirm_live: false,
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "confirm_live_required");
});

test("live storage verification can exercise the S3-compatible path with a tiny roundtrip", async () => {
  const original = {
    STORAGE_BUCKET: ENV.STORAGE_BUCKET,
    STORAGE_REGION: ENV.STORAGE_REGION,
    STORAGE_S3_ENDPOINT: ENV.STORAGE_S3_ENDPOINT,
    STORAGE_S3_ACCESS_KEY_ID: ENV.STORAGE_S3_ACCESS_KEY_ID,
    STORAGE_S3_SECRET_ACCESS_KEY: ENV.STORAGE_S3_SECRET_ACCESS_KEY,
    STORAGE_S3_FORCE_PATH_STYLE: ENV.STORAGE_S3_FORCE_PATH_STYLE,
  };
  const fakeFetch = createFakeS3Fetch();

  try {
    ENV.STORAGE_BUCKET = "deal-docs";
    ENV.STORAGE_REGION = "us-east-1";
    ENV.STORAGE_S3_ENDPOINT = "https://s3.example.test";
    ENV.STORAGE_S3_ACCESS_KEY_ID = "AKIA_TEST_ACCESS";
    ENV.STORAGE_S3_SECRET_ACCESS_KEY = "secret-test-key";
    ENV.STORAGE_S3_FORCE_PATH_STYLE = true;

    const result = await runLiveStorageVerification({
      confirm_live: true,
      fetch_impl: fakeFetch,
    });

    assert.equal(result.ok, true);
    assert.equal(result.provider, "s3");
    assert.equal(result.upload.ok, true);
    assert.equal(result.metadata.ok, true);
    assert.equal(result.read.ok, true);
    assert.equal(result.read.body_match, true);
    assert.equal(result.signed.ok, true);
    assert.equal(result.signed_url_check.ok, true);
  } finally {
    ENV.STORAGE_BUCKET = original.STORAGE_BUCKET;
    ENV.STORAGE_REGION = original.STORAGE_REGION;
    ENV.STORAGE_S3_ENDPOINT = original.STORAGE_S3_ENDPOINT;
    ENV.STORAGE_S3_ACCESS_KEY_ID = original.STORAGE_S3_ACCESS_KEY_ID;
    ENV.STORAGE_S3_SECRET_ACCESS_KEY = original.STORAGE_S3_SECRET_ACCESS_KEY;
    ENV.STORAGE_S3_FORCE_PATH_STYLE = original.STORAGE_S3_FORCE_PATH_STYLE;
  }
});
