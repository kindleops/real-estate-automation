import test from "node:test";
import assert from "node:assert/strict";

import { normalizeInboundReplayExampleBody } from "@/lib/diagnostics/normalize-inbound-replay-example-body.js";

test("diagnostics/inbound-replay normalizes body aliases (body/message_body/inbound_message_body/text) to inbound body", async () => {
  const normalizedForKey = (key, value) =>
    normalizeInboundReplayExampleBody({
      [key]: value,
    });

  assert.equal(normalizedForKey("body", "Yes I own it"), "Yes I own it");
  assert.equal(normalizedForKey("message_body", "Yes I own it"), "Yes I own it");
  assert.equal(normalizedForKey("inbound_message_body", "Yes I own it"), "Yes I own it");
  assert.equal(normalizedForKey("text", "Yes I own it"), "Yes I own it");
});

test("diagnostics/inbound-replay trims whitespace and treats whitespace-only aliases as missing", async () => {
  assert.equal(
    normalizeInboundReplayExampleBody({ body: "   Yes I own it   " }),
    "Yes I own it"
  );

  // Whitespace-only should be treated as missing input.
  assert.equal(
    normalizeInboundReplayExampleBody({ body: "   " }),
    ""
  );

  // If body is whitespace-only, later aliases can still supply the message.
  assert.equal(
    normalizeInboundReplayExampleBody({ body: "   ", message_body: "Hi" }),
    "Hi"
  );
});
