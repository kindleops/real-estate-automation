import test from "node:test";
import assert from "node:assert/strict";

import { isRetryablePodioRequestError } from "@/lib/providers/podio.js";
import {
  clearTemplateBatchCache,
  fetchTemplatesCached,
} from "@/lib/domain/templates/load-template.js";

test("Podio retry logic treats server-too-long responses as transient", () => {
  assert.equal(
    isRetryablePodioRequestError({
      message: "The server took too long to respond, please try again",
    }),
    true
  );

  assert.equal(
    isRetryablePodioRequestError({
      response: {
        status: 503,
      },
    }),
    true
  );
});

test("Podio retry logic still rejects non-transient validation failures", () => {
  assert.equal(
    isRetryablePodioRequestError({
      message: '[Podio] Invalid category value "Runtime Lock"',
      response: {
        status: 400,
      },
    }),
    false
  );
});

test("template batch cache reuses identical filter fetches", async () => {
  clearTemplateBatchCache();

  let calls = 0;
  const fetcher = async (filter_set) => {
    calls += 1;
    return [{ item_id: calls, filter_set }];
  };

  const first = await fetchTemplatesCached(
    {
      language: "English",
      "use-case": "ownership_check",
    },
    { fetcher }
  );
  const second = await fetchTemplatesCached(
    {
      "use-case": "ownership_check",
      language: "English",
    },
    { fetcher }
  );

  assert.equal(calls, 1);
  assert.equal(first, second);

  clearTemplateBatchCache();
});
