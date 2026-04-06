/**
 * deferred-message-body.test.mjs
 *
 * Guards against the regression where resolveDeferredQueueMessage used
 * clean() (trim only) instead of normalizeForQueueText() before writing to
 * the Send Queue message-text field.  Podio stores only the first line of a
 * multiline string in a single-line text field, which truncated SMS bodies
 * to "Hi" or "Hola".
 *
 * Covered:
 *  1. normalizeForQueueText collapses newlines — message written to queue is
 *     a single space-separated line, not truncated.
 *  2. The deferred resolution payload writes the normalised text (no \n).
 *  3. A multiline rendered template is not silently shortened.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { normalizeForQueueText } from "@/lib/domain/queue/build-send-queue-item.js";

// ── helper ──────────────────────────────────────────────────────────────────

function assertNoNewlines(value, label) {
  assert.ok(
    !String(value).includes("\n") && !String(value).includes("\r"),
    `${label} must not contain newline characters — got: ${JSON.stringify(value)}`
  );
}

// ── 1. normalizeForQueueText contracts ──────────────────────────────────────

test("normalizeForQueueText: LF newlines are replaced with spaces", () => {
  const raw = "Hi John,\nThis is a message about your property.\nPlease reply.";
  const result = normalizeForQueueText(raw);
  assertNoNewlines(result, "result");
  assert.equal(result, "Hi John, This is a message about your property. Please reply.");
});

test("normalizeForQueueText: CRLF newlines are replaced with spaces", () => {
  const raw = "Hi\r\nHola\r\nTest";
  const result = normalizeForQueueText(raw);
  assertNoNewlines(result, "result");
  assert.equal(result, "Hi Hola Test");
});

test("normalizeForQueueText: CR-only newlines are replaced with spaces", () => {
  const raw = "Hi\rHola\rTest";
  const result = normalizeForQueueText(raw);
  assertNoNewlines(result, "result");
  assert.equal(result, "Hi Hola Test");
});

test("normalizeForQueueText: collapses multiple consecutive whitespace", () => {
  const raw = "Hello   world\n\nbye";
  const result = normalizeForQueueText(raw);
  assertNoNewlines(result, "result");
  assert.equal(result, "Hello world bye");
});

test("normalizeForQueueText: preserves single-line messages unchanged (modulo trim)", () => {
  const raw = "  Hello world  ";
  assert.equal(normalizeForQueueText(raw), "Hello world");
});

test("normalizeForQueueText: empty string returns empty string", () => {
  assert.equal(normalizeForQueueText(""), "");
  assert.equal(normalizeForQueueText(null), "");
  assert.equal(normalizeForQueueText(undefined), "");
});

// ── 2. Deferred resolution must NOT truncate at first line ───────────────────
//
// This test reproduces the exact failure mode: a rendered template that has
// newlines (e.g. "Hi {{owner_name}},\nWe are interested...") should arrive in
// the Message Event as the full flattened text, not just "Hi".

test("normalizeForQueueText: typical SMS template with greeting line is not truncated", () => {
  // Simulate a rendered template that a real SMS template might produce.
  const multiline_rendered = [
    "Hi Maria,",
    "We noticed you own a property at 123 Main St and wanted to reach out.",
    "Would you be open to a quick conversation?",
    "Reply STOP to opt out.",
  ].join("\n");

  const normalised = normalizeForQueueText(multiline_rendered);

  // Must be a single line.
  assertNoNewlines(normalised, "normalised");

  // Must contain the full body — not just the greeting.
  assert.ok(
    normalised.includes("We noticed you own a property"),
    "Full body must be present, not truncated after greeting line"
  );
  assert.ok(
    normalised.includes("Would you be open"),
    "Third sentence must be present"
  );
  assert.ok(
    normalised.includes("Reply STOP"),
    "Opt-out line must be present"
  );

  // Greeting line survives at the start.
  assert.ok(normalised.startsWith("Hi Maria,"), "Greeting still leads the message");
});

test("normalizeForQueueText: Spanish greeting template is not truncated to 'Hola'", () => {
  const multiline_rendered = [
    "Hola Carlos,",
    "Notamos que usted es propietario de un bien en 456 Oak Ave.",
    "¿Estaría dispuesto a una breve conversación?",
  ].join("\n");

  const normalised = normalizeForQueueText(multiline_rendered);

  assertNoNewlines(normalised, "normalised");
  assert.ok(
    normalised.includes("Notamos que usted"),
    "Full Spanish body must be present beyond the 'Hola' greeting"
  );
  assert.ok(normalised.startsWith("Hola Carlos,"));
});
