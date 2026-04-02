import test from "node:test";
import assert from "node:assert/strict";

import {
  buildS3ObjectUrl,
  buildS3PresignedUrl,
  buildS3SignedRequest,
  buildS3StorageConfig,
} from "@/lib/providers/storage-s3.js";

function createS3Env(overrides = {}) {
  return {
    STORAGE_S3_ENDPOINT: "https://s3.example.test",
    STORAGE_BUCKET: "deal-docs",
    STORAGE_REGION: "us-east-1",
    STORAGE_S3_ACCESS_KEY_ID: "AKIA_TEST_ACCESS",
    STORAGE_S3_SECRET_ACCESS_KEY: "secret-test-key",
    STORAGE_S3_FORCE_PATH_STYLE: "true",
    ...overrides,
  };
}

test("S3 storage config reports missing fields honestly", () => {
  const config = buildS3StorageConfig({
    STORAGE_S3_ENDPOINT: "",
    STORAGE_BUCKET: "",
    STORAGE_REGION: "",
    STORAGE_S3_ACCESS_KEY_ID: "",
    STORAGE_S3_SECRET_ACCESS_KEY: "",
  });

  assert.equal(config.configured, false);
  assert.deepEqual(config.missing, [
    "STORAGE_S3_ENDPOINT",
    "STORAGE_BUCKET",
    "STORAGE_REGION",
    "STORAGE_S3_ACCESS_KEY_ID",
    "STORAGE_S3_SECRET_ACCESS_KEY",
  ]);
});

test("S3 presigned URL and signed request generation produce usable access artifacts", () => {
  const config = buildS3StorageConfig(createS3Env());
  const now = new Date("2026-04-01T12:00:00.000Z");
  const key = "contracts/123/purchase-agreement.pdf";

  assert.equal(config.configured, true);

  const presigned = buildS3PresignedUrl({
    key,
    disposition: "attachment",
    filename: "purchase-agreement.pdf",
    expires_in_seconds: 900,
    config,
    now,
  });

  assert.equal(presigned.ok, true);
  assert.equal(presigned.expires_in_seconds, 900);

  const presignedUrl = new URL(presigned.url);
  assert.match(presignedUrl.pathname, /\/deal-docs\/contracts\/123\/purchase-agreement\.pdf$/);
  assert.equal(
    presignedUrl.searchParams.get("X-Amz-Algorithm"),
    "AWS4-HMAC-SHA256"
  );
  assert.equal(presignedUrl.searchParams.get("X-Amz-Expires"), "900");
  assert.match(
    presignedUrl.searchParams.get("X-Amz-Signature"),
    /^[a-f0-9]{64}$/
  );
  assert.equal(
    presignedUrl.searchParams.get("response-content-disposition"),
    'attachment; filename="purchase-agreement.pdf"'
  );

  const objectUrl = buildS3ObjectUrl({ key, config });
  const signedRequest = buildS3SignedRequest({
    method: "PUT",
    url: objectUrl,
    body_hash:
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    config,
    now,
    headers: {
      "content-type": "application/pdf",
    },
  });

  assert.match(signedRequest.authorization, /^AWS4-HMAC-SHA256 Credential=/);
  assert.equal(signedRequest.headers["content-type"], "application/pdf");
  assert.equal(
    signedRequest.headers["x-amz-content-sha256"],
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  );
  assert.equal(signedRequest.headers["x-amz-date"], "20260401T120000Z");
});
