import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  buildTextgridBearerToken,
  buildTextgridSendHeaders,
  buildTextgridSendPayload,
  getTextgridSendEndpoint,
  mapTextgridFailureBucket,
  normalizePhone,
  sendTextgridSMS,
} from "@/lib/providers/textgrid.js";

const saved_fetch = globalThis.fetch;
const saved_sid = process.env.TEXTGRID_ACCOUNT_SID;
const saved_token = process.env.TEXTGRID_AUTH_TOKEN;

afterEach(() => {
  globalThis.fetch = saved_fetch;
  if (typeof saved_sid === "string") {
    process.env.TEXTGRID_ACCOUNT_SID = saved_sid;
  } else {
    delete process.env.TEXTGRID_ACCOUNT_SID;
  }
  if (typeof saved_token === "string") {
    process.env.TEXTGRID_AUTH_TOKEN = saved_token;
  } else {
    delete process.env.TEXTGRID_AUTH_TOKEN;
  }
});

test("getTextgridSendEndpoint: embeds account SID in the fixed versioned path", () => {
  const endpoint = getTextgridSendEndpoint("ACtest123");
  assert.equal(
    endpoint,
    "https://api.textgrid.com/2010-04-01/Accounts/ACtest123/Messages.json"
  );
});

test("buildTextgridBearerToken: base64 encodes account_sid:auth_token", () => {
  assert.equal(
    buildTextgridBearerToken({
      account_sid: "ABCD12345",
      auth_token: "1234567890",
    }),
    "QUJDRDEyMzQ1OjEyMzQ1Njc4OTA="
  );
});

test("buildTextgridSendHeaders: uses Bearer + base64(account_sid:auth_token)", () => {
  const headers = buildTextgridSendHeaders({
    account_sid: "ABCD12345",
    auth_token: "1234567890",
  });

  assert.deepEqual(headers, {
    Authorization: "Bearer QUJDRDEyMzQ1OjEyMzQ1Njc4OTA=",
    "Content-Type": "application/json",
  });
});

test("buildTextgridSendPayload: only includes body, from, and to", () => {
  assert.deepEqual(
    buildTextgridSendPayload({
      body: "Hello there",
      from: "+15550001111",
      to: "+15550002222",
    }),
    {
      body: "Hello there",
      from: "+15550001111",
      to: "+15550002222",
    }
  );
});

test("mapTextgridFailureBucket: HTTP 404 -> Hard Bounce", () => {
  const bucket = mapTextgridFailureBucket({
    success: false,
    ok: false,
    error_status: 404,
    error_message: "Not Found",
  });
  assert.equal(bucket, "Hard Bounce");
});

test("mapTextgridFailureBucket: HTTP 500 -> Soft Bounce", () => {
  const bucket = mapTextgridFailureBucket({
    success: false,
    ok: false,
    error_status: 500,
    error_message: "Internal Server Error",
  });
  assert.equal(bucket, "Soft Bounce");
});

test("sendTextgridSMS: posts exact endpoint, auth header, and body; returns sid on success", async () => {
  process.env.TEXTGRID_ACCOUNT_SID = "ACtest-sid-001";
  process.env.TEXTGRID_AUTH_TOKEN = "test-auth-token";

  let captured_url = null;
  let captured_init = null;

  globalThis.fetch = async (url, init) => {
    captured_url = url;
    captured_init = init;
    return new Response(
      JSON.stringify({
        sid: "SM123",
        status: "queued",
      }),
      {
        status: 200,
        headers: {
          "Content-Type": "application/json",
        },
      }
    );
  };

  const result = await sendTextgridSMS({
    to: "9188102617",
    from: "9188102618",
    body: "Hello from TextGrid",
  });

  assert.equal(
    captured_url,
    "https://api.textgrid.com/2010-04-01/Accounts/ACtest-sid-001/Messages.json"
  );
  assert.equal(captured_init.method, "POST");
  assert.deepEqual(captured_init.headers, {
    Authorization: `Bearer ${Buffer.from("ACtest-sid-001:test-auth-token").toString("base64")}`,
    "Content-Type": "application/json",
  });
  assert.equal(
    captured_init.body,
    JSON.stringify({
      body: "Hello from TextGrid",
      from: "+19188102618",
      to: "+19188102617",
    })
  );

  assert.equal(result.success, true);
  assert.equal(result.ok, true);
  assert.equal(result.sid, "SM123");
  assert.equal(result.message_id, "SM123");
});

test("sendTextgridSMS: throws when response body is not valid JSON", async () => {
  process.env.TEXTGRID_ACCOUNT_SID = "ACtest-sid-001";
  process.env.TEXTGRID_AUTH_TOKEN = "test-auth-token";

  globalThis.fetch = async () =>
    new Response("not-json", {
      status: 200,
      headers: {
        "Content-Type": "text/plain",
      },
    });

  await assert.rejects(
    () =>
      sendTextgridSMS({
        to: "9188102617",
        from: "9188102618",
        body: "Hello from TextGrid",
      }),
    /Invalid JSON response: not-json/
  );
});

test("sendTextgridSMS: throws when HTTP status is not ok", async () => {
  process.env.TEXTGRID_ACCOUNT_SID = "ACtest-sid-001";
  process.env.TEXTGRID_AUTH_TOKEN = "test-auth-token";

  globalThis.fetch = async () =>
    new Response(JSON.stringify({ message: "No route" }), {
      status: 404,
      headers: {
        "Content-Type": "application/json",
      },
    });

  await assert.rejects(
    () =>
      sendTextgridSMS({
        to: "9188102617",
        from: "9188102618",
        body: "Hello from TextGrid",
      }),
    /TextGrid HTTP failure:/
  );
});

test("sendTextgridSMS: throws when sid is missing", async () => {
  process.env.TEXTGRID_ACCOUNT_SID = "ACtest-sid-001";
  process.env.TEXTGRID_AUTH_TOKEN = "test-auth-token";

  globalThis.fetch = async () =>
    new Response(JSON.stringify({ status: "queued" }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    });

  await assert.rejects(
    () =>
      sendTextgridSMS({
        to: "9188102617",
        from: "9188102618",
        body: "Hello from TextGrid",
      }),
    /Missing SID \(NOT SENT\):/
  );
});

test("sendTextgridSMS: throws when carrier status is failed", async () => {
  process.env.TEXTGRID_ACCOUNT_SID = "ACtest-sid-001";
  process.env.TEXTGRID_AUTH_TOKEN = "test-auth-token";

  globalThis.fetch = async () =>
    new Response(JSON.stringify({ sid: "SM123", status: "failed" }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    });

  await assert.rejects(
    () =>
      sendTextgridSMS({
        to: "9188102617",
        from: "9188102618",
        body: "Hello from TextGrid",
      }),
    /Carrier rejected message:/
  );
});

test("normalizePhone: 10-digit -> E.164", () => {
  assert.equal(normalizePhone("9188102617"), "+19188102617");
});
