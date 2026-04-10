import test from "node:test";
import assert from "node:assert/strict";

import {
  PHONE_FIELDS,
  findPhoneRecord,
  __setPhoneNumbersTestDeps,
  __resetPhoneNumbersTestDeps,
} from "@/lib/podio/apps/phone-numbers.js";

test("findPhoneRecord falls back to raw phone field when normalized fields are missing", async (t) => {
  const calls = [];

  __setPhoneNumbersTestDeps({
    logger: { info() {} },
    filterAppItems: async (_app_id, filters) => {
      calls.push(filters);

      if (filters[PHONE_FIELDS.phone] === "(612) 743-3952") {
        return { items: [{ item_id: 90210 }] };
      }

      return { items: [] };
    },
  });

  t.after(() => {
    __resetPhoneNumbersTestDeps();
  });

  const result = await findPhoneRecord("+16127433952");

  assert.equal(result?.item_id, 90210);
  assert.ok(
    calls.some((filters) => filters[PHONE_FIELDS.phone_hidden] === "6127433952"),
    "should try phone-hidden lookup first"
  );
  assert.ok(
    calls.some((filters) => filters[PHONE_FIELDS.canonical_e164] === "+16127433952"),
    "should try canonical-e164 lookup before raw phone fallback"
  );
  assert.ok(
    calls.some((filters) => filters[PHONE_FIELDS.canonical_e164] === "6127433952"),
    "should try canonical-e164 10-digit fallback before raw phone fallback"
  );
  assert.ok(
    calls.some((filters) => filters[PHONE_FIELDS.phone] === "(612) 743-3952"),
    "should try raw phone-field national formatting as a final fallback"
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
