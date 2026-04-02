import test from "node:test";
import assert from "node:assert/strict";

import {
  getSignedUrl,
  readFile,
  uploadFile,
  verifySignedStorageAccess,
} from "@/lib/providers/storage.js";

test("local storage provider persists metadata and verifies signed access", async () => {
  const key = `tests/storage/${Date.now()}-hello.txt`;

  const uploaded = await uploadFile({
    key,
    body: "hello storage",
    content_type: "text/plain; charset=utf-8",
    filename: "hello.txt",
    metadata: {
      scope: "test",
    },
  });

  assert.equal(uploaded.ok, true);
  assert.equal(uploaded.key, key);
  assert.equal(uploaded.filename, "hello.txt");
  assert.equal(uploaded.metadata.scope, "test");

  const stored = await readFile({ key });
  assert.equal(stored.ok, true);
  assert.equal(stored.metadata.filename, "hello.txt");
  assert.equal(stored.body.toString("utf8"), "hello storage");

  const signed = await getSignedUrl({
    key,
    filename: "hello.txt",
    disposition: "inline",
    expires_in_seconds: 600,
  });

  assert.equal(signed.ok, true);
  assert.match(signed.path, /\/api\/storage\/access\?/);
  assert.match(signed.url, /http:\/\/localhost:3000\/api\/storage\/access\?/);

  const params = new URL(signed.url).searchParams;
  const verified = verifySignedStorageAccess({
    key: params.get("key"),
    expires: params.get("expires"),
    signature: params.get("signature"),
    disposition: params.get("disposition"),
    filename: params.get("filename"),
  });

  assert.equal(verified.ok, true);
  assert.equal(verified.verified, true);
});
