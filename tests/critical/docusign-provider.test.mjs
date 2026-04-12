import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";

import {
  __resetDocusignTestDeps,
  __setDocusignTestDeps,
  buildDocusignAuthorizationUrl,
  createEnvelope,
  getDocusignAccessToken,
  getDocusignConfigSummary,
} from "@/lib/providers/docusign.js";
import { createDocusignEnvelopeFromContract } from "@/lib/domain/contracts/create-docusign-envelope-from-contract.js";
import { createPodioItem, textField } from "../helpers/test-helpers.js";

const ENV_KEYS = [
  "DOCUSIGN_INTEGRATION_KEY",
  "DOCUSIGN_API_KEY",
  "DOCUSIGN_USER_ID",
  "DOCUSIGN_ACCOUNT_ID",
  "DOCUSIGN_BASE_URL",
  "DOCUSIGN_OAUTH_BASE_URL",
  "DOCUSIGN_CLIENT_SECRET",
  "DOCUSIGN_PRIVATE_KEY",
  "DOCUSIGN_REDIRECT_URI_LOCAL",
  "DOCUSIGN_REDIRECT_URI_PREVIEW",
  "DOCUSIGN_REDIRECT_URI_PROD",
  "DOCUSIGN_SELLER_ROLE_NAME",
  "DOCUSIGN_BUYER_ROLE_NAME",
];

const ORIGINAL_ENV = Object.fromEntries(
  ENV_KEYS.map((key) => [key, process.env[key]])
);

function restoreEnv() {
  for (const key of ENV_KEYS) {
    if (ORIGINAL_ENV[key] === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = ORIGINAL_ENV[key];
    }
  }
}

function applyEnv(overrides = {}) {
  restoreEnv();
  for (const [key, value] of Object.entries(overrides)) {
    if (value === undefined || value === null) {
      delete process.env[key];
    } else {
      process.env[key] = String(value);
    }
  }
}

test.afterEach(() => {
  restoreEnv();
  __resetDocusignTestDeps();
});

test("DocuSign config summary recognizes JWT and auth-code env vars", () => {
  const summary = getDocusignConfigSummary({
    DOCUSIGN_INTEGRATION_KEY: "integration-key",
    DOCUSIGN_USER_ID: "user-id",
    DOCUSIGN_ACCOUNT_ID: "account-id",
    DOCUSIGN_BASE_URL: "https://demo.docusign.net/restapi",
    DOCUSIGN_OAUTH_BASE_URL: "https://account-d.docusign.com",
    DOCUSIGN_CLIENT_SECRET: "client-secret",
    DOCUSIGN_PRIVATE_KEY: "-----BEGIN RSA PRIVATE KEY-----\nkey\n-----END RSA PRIVATE KEY-----",
    DOCUSIGN_REDIRECT_URI_LOCAL: "http://localhost:3000/api/docusign/callback",
    DOCUSIGN_REDIRECT_URI_PREVIEW: "https://preview.vercel.app/api/docusign/callback",
    DOCUSIGN_REDIRECT_URI_PROD: "https://prod.example.com/api/docusign/callback",
  });

  assert.equal(summary.jwt_ready, true);
  assert.equal(summary.auth_code_ready, true);
  assert.equal(summary.environment, "demo");
  assert.equal(summary.integration_key_present, true);
  assert.equal(summary.private_key_present, true);
});

test("DocuSign auth URL builder resolves redirect uri and PKCE challenge", () => {
  const result = buildDocusignAuthorizationUrl({
    target: "preview",
    state: "debug-state",
    code_verifier: "pkce-verifier-123",
    env: {
      DOCUSIGN_INTEGRATION_KEY: "integration-key",
      DOCUSIGN_CLIENT_SECRET: "client-secret",
      DOCUSIGN_OAUTH_BASE_URL: "https://account-d.docusign.com",
      DOCUSIGN_REDIRECT_URI_PREVIEW:
        "https://preview.vercel.app/api/docusign/callback",
    },
  });

  assert.equal(result.ok, true);
  assert.equal(
    result.redirect_uri,
    "https://preview.vercel.app/api/docusign/callback"
  );
  assert.match(result.authorization_url, /code_challenge=/);
  assert.match(result.authorization_url, /code_challenge_method=S256/);
  assert.match(result.authorization_url, /state=debug-state/);
});

test("DocuSign JWT access tokens are cached between requests", async () => {
  const { privateKey } = crypto.generateKeyPairSync("rsa", {
    modulusLength: 2048,
  });
  let fetchCount = 0;

  applyEnv({
    DOCUSIGN_INTEGRATION_KEY: "integration-key",
    DOCUSIGN_USER_ID: "user-id",
    DOCUSIGN_ACCOUNT_ID: "account-id",
    DOCUSIGN_BASE_URL: "https://demo.docusign.net/restapi",
    DOCUSIGN_OAUTH_BASE_URL: "https://account-d.docusign.com",
    DOCUSIGN_PRIVATE_KEY: privateKey.export({ type: "pkcs1", format: "pem" }),
  });

  __setDocusignTestDeps({
    fetch: async () => {
      fetchCount += 1;
      return new Response(
        JSON.stringify({
          access_token: "token-1",
          expires_in: 3600,
          token_type: "Bearer",
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
          },
        }
      );
    },
    recordSystemAlert: async () => {},
  });

  const first = await getDocusignAccessToken();
  const second = await getDocusignAccessToken();

  assert.equal(first.ok, true);
  assert.equal(second.ok, true);
  assert.equal(first.access_token, "token-1");
  assert.equal(second.access_token, "token-1");
  assert.equal(fetchCount, 1);
});

test("contract envelope helper builds explicit seller, buyer, and internal_cc recipients", async () => {
  const contract_item = createPodioItem(4501, {
    title: textField("Agreement for 3229 Fargo Ct"),
    "contract-id": textField("CTR-4501"),
  });

  const result = await createDocusignEnvelopeFromContract({
    contract_item,
    template_id: "template-123",
    seller_recipient: {
      name: "Seller One",
      email: "seller@example.com",
    },
    buyer_recipient: {
      name: "Buyer One",
      email: "buyer@example.com",
    },
    internal_cc: [
      {
        name: "Ops",
        email: "ops@example.com",
      },
    ],
    email_blurb: "Please review and sign.",
    dry_run: true,
  });

  assert.equal(result.ok, true);
  assert.equal(result.contract_item_id, 4501);
  assert.equal(result.subject, "Agreement for 3229 Fargo Ct");
  assert.equal(result.recipient_summary.seller_count, 1);
  assert.equal(result.recipient_summary.buyer_count, 1);
  assert.equal(result.recipient_summary.internal_cc_count, 1);
  assert.equal(result.raw.templateRoles.length, 3);
});

test("non-template envelope dry run keeps internal_cc as carbon copy", async () => {
  const result = await createEnvelope({
    subject: "Purchase agreement",
    documents: [
      {
        document_id: "1",
        name: "contract.pdf",
        file_base64: Buffer.from("pdf").toString("base64"),
        file_extension: "pdf",
      },
    ],
    recipients: [
      {
        name: "Seller One",
        email: "seller@example.com",
        role: "seller",
        role_name: "Seller",
      },
      {
        name: "Ops",
        email: "ops@example.com",
        role: "internal_cc",
      },
    ],
    dry_run: true,
  });

  assert.equal(result.ok, true);
  assert.equal(result.recipient_summary.total, 2);
  assert.equal(result.raw.recipients.signers.length, 1);
  assert.equal(result.raw.recipients.carbonCopies.length, 1);
});
