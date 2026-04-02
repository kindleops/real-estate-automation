import test from "node:test";
import assert from "node:assert/strict";

import {
  getPodioRetryAfterSeconds,
  getLatestPodioRateLimitStatus,
  isPodioRateLimitError,
  isRetryablePodioRequestError,
  recordPodioRateLimitObservation,
  resetPodioRateLimitObservability,
} from "@/lib/providers/podio.js";
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

test("Podio rate-limit helpers classify wait-window responses", () => {
  const error = {
    status: 420,
    message:
      "You have hit the rate limit. Please wait 3600 seconds before trying again.",
  };

  assert.equal(isPodioRateLimitError(error), true);
  assert.equal(getPodioRetryAfterSeconds(error), 3600);
});

test("Podio rate-limit observability tracks the latest quota snapshot", () => {
  resetPodioRateLimitObservability();

  const observation = recordPodioRateLimitObservation({
    method: "post",
    path: "/item/app/123/filter/",
    status: 200,
    duration_ms: 187,
    attempt: 1,
    headers: {
      "x-rate-limit-limit": "1000",
      "x-rate-limit-remaining": "90",
    },
  });

  const latest = getLatestPodioRateLimitStatus();

  assert.equal(observation.operation, "filter_items");
  assert.equal(observation.rate_limit_limit, 1000);
  assert.equal(observation.rate_limit_remaining, 90);
  assert.equal(observation.low_remaining_threshold, 100);
  assert.equal(latest.observed, true);
  assert.equal(latest.path, "/item/app/123/filter/");
  assert.equal(latest.rate_limit_remaining, 90);
  assert.equal(latest.low_remaining_threshold, 100);

  resetPodioRateLimitObservability();
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
