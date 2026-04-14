import test from "node:test";
import assert from "node:assert/strict";

import {
  PHONE_FIELDS,
  findPhoneRecord,
  __setPhoneNumbersTestDeps,
  __resetPhoneNumbersTestDeps,
} from "@/lib/podio/apps/phone-numbers.js";

test("findPhoneRecord returns null when normalized text-field lookups find nothing", async (t) => {
  const calls = [];

  __setPhoneNumbersTestDeps({
    logger: { info() {} },
    filterAppItems: async (_app_id, filters) => {
      calls.push(filters);
      // No phone item exists in Podio for this number
      return { items: [] };
    },
  });

  t.after(() => {
    __resetPhoneNumbersTestDeps();
  });

  const result = await findPhoneRecord("+16127433952");

  // Should return null (not throw) when the number doesn't exist
  assert.equal(result, null, "should return null when no phone item matches");
  assert.ok(
    calls.some((filters) => filters[PHONE_FIELDS.phone_hidden] === "6127433952"),
    "should try phone-hidden lookup"
  );
  assert.ok(
    calls.some((filters) => filters[PHONE_FIELDS.canonical_e164] === "+16127433952"),
    "should try canonical-e164 lookup"
  );
  assert.ok(
    calls.some((filters) => filters[PHONE_FIELDS.canonical_e164] === "6127433952"),
    "should try canonical-e164 10-digit fallback"
  );
  // Raw phone-type field is NOT tried — Podio doesn't support filtering on phone-type fields
  assert.ok(
    !calls.some((filters) => filters[PHONE_FIELDS.phone]),
    "should NOT attempt to filter on phone-type field"
  );
});

test("findPhoneRecord prefers normalized lookups before raw phone fallback", async (t) => {
  const calls = [];

  __setPhoneNumbersTestDeps({
    logger: { info() {} },
    filterAppItems: async (_app_id, filters) => {
      calls.push(filters);

      if (filters[PHONE_FIELDS.phone_hidden] === "6127433952") {
        return { items: [{ item_id: 501 }] };
      }

      if (filters[PHONE_FIELDS.phone]) {
        return { items: [{ item_id: 999 }] };
      }

      return { items: [] };
    },
  });

  t.after(() => {
    __resetPhoneNumbersTestDeps();
  });

  const result = await findPhoneRecord("+16127433952");

  assert.equal(result?.item_id, 501);
  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.[PHONE_FIELDS.phone_hidden], "6127433952");
});
