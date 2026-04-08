// ─── sms-text-normalization.test.mjs ─────────────────────────────────────
// Tests for F. normalizeTextForSms() and normalizeForQueueText() from
// build-send-queue-item.js — confirms that malformed punctuation spacing
// is corrected before any message is written to the Send Queue.

import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeTextForSms,
  normalizeForQueueText,
} from "@/lib/domain/queue/build-send-queue-item.js";

// ── normalizeTextForSms ───────────────────────────────────────────────────

test("normalizeTextForSms: removes space before comma", () => {
  assert.equal(normalizeTextForSms("Hi Jose ,"), "Hi Jose,");
  assert.equal(normalizeTextForSms("Hi Jose , how are you"), "Hi Jose, how are you");
});

test("normalizeTextForSms: removes space before period", () => {
  assert.equal(normalizeTextForSms("this is Ricky ."), "this is Ricky.");
  assert.equal(normalizeTextForSms("Nice to meet you ."), "Nice to meet you.");
});

test("normalizeTextForSms: removes space before exclamation and question marks", () => {
  assert.equal(normalizeTextForSms("How are you ?"), "How are you?");
  assert.equal(normalizeTextForSms("Great !"), "Great!");
});

test("normalizeTextForSms: no change when punctuation is already correct", () => {
  const msg = "Hi Jose, this is Ryan. Do you still own 5521 Laster Ln?";
  assert.equal(normalizeTextForSms(msg), msg);
});

test("normalizeTextForSms: collapses multiple internal spaces", () => {
  assert.equal(normalizeTextForSms("Hi  Jose"), "Hi Jose");
});

test("normalizeTextForSms: trims leading/trailing whitespace", () => {
  assert.equal(normalizeTextForSms("  hello  "), "hello");
});

test("normalizeTextForSms: handles empty and null safely", () => {
  assert.equal(normalizeTextForSms(""), "");
  assert.equal(normalizeTextForSms(null), "");
  assert.equal(normalizeTextForSms(undefined), "");
});

test("normalizeTextForSms: em-dash normalization — extra surrounding spaces collapsed", () => {
  // em-dash with double spaces around it
  assert.equal(normalizeTextForSms("word  \u2014  word"), "word \u2014 word");
});

test("normalizeTextForSms: real message body production example", () => {
  const raw = "Hi Jose , this is Ricky . Do you still own 5521 Laster Ln ?";
  const expected = "Hi Jose, this is Ricky. Do you still own 5521 Laster Ln?";
  assert.equal(normalizeTextForSms(raw), expected);
});

// ── normalizeForQueueText (full pipeline) ────────────────────────────────

test("normalizeForQueueText: strips HTML then normalizes punctuation", () => {
  const raw = "<p>Hi Jose ,</p><p>Do you still own 123 Main St ?</p>";
  const result = normalizeForQueueText(raw);
  assert.equal(result, "Hi Jose, Do you still own 123 Main St?");
});

test("normalizeForQueueText: collapses newlines into spaces", () => {
  const raw = "Hi Jose ,\nthis is Ricky .";
  const result = normalizeForQueueText(raw);
  assert.equal(result, "Hi Jose, this is Ricky.");
});

test("normalizeForQueueText: handles empty string", () => {
  assert.equal(normalizeForQueueText(""), "");
});
