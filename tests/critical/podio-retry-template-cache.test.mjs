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
  loadTemplateCandidates,
} from "@/lib/domain/templates/load-template.js";
import { normalizeTemplateItem } from "@/lib/podio/apps/templates.js";

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

test("template loader falls back to generic same-use-case templates when category matching fails", async () => {
  const calls = [];
  const generic_template = {
    item_id: 7001,
    use_case: "ownership_check",
    variant_group: "Stage 1 — Ownership Confirmation",
    tone: "Warm",
    gender_variant: "Neutral",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    text: "Hi {{owner_name}}, are you the owner of {{property_address}}?",
    active: "Yes",
    category_primary: null,
    category_secondary: null,
    deliverability_score: 90,
    spam_risk: 2,
    historical_reply_rate: 20,
    total_conversations: 0,
    total_replies: 0,
  };

  const candidates = await loadTemplateCandidates({
    category: "Residential",
    secondary_category: "Single Family",
    use_case: "ownership_check",
    variant_group: "Stage 1 — Ownership Confirmation",
    tone: "Warm",
    gender_variant: "Neutral",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    recently_used_template_ids: [],
    fallback_agent_type: "Warm Professional",
    remote_fetcher: async (filter_set) => {
      calls.push(filter_set);
      if (filter_set["property-type"] || filter_set.category_primary) return [];
      if (filter_set["use-case"] === "ownership_check") return [generic_template];
      return [];
    },
    local_fetcher: () => [],
  });

  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].item_id, 7001);
  assert.ok(calls.some((filter_set) => filter_set["property-type"] === "Residential"));
  assert.ok(calls.some((filter_set) => !filter_set["property-type"]));
});

test("template loader accepts legacy stage labels and templates without spam risk values", async () => {
  const calls = [];
  const candidates = await loadTemplateCandidates({
    category: "Residential",
    secondary_category: "Outbound Initial",
    use_case: "ownership_check",
    variant_group: "Stage 1 Ownership Check",
    tone: "Warm",
    gender_variant: "Neutral",
    language: "English",
    sequence_position: "V1",
    paired_with_agent_type: "Warm Professional",
    recently_used_template_ids: [],
    fallback_agent_type: "Warm Professional",
    remote_fetcher: async (filter_set) => {
      calls.push(filter_set);
      return [
        {
          item_id: 8001,
          use_case: "ownership_check",
          variant_group: filter_set.stage,
          tone: "Warm",
          gender_variant: "Neutral",
          language: "English",
          sequence_position: "V1",
          paired_with_agent_type: "Warm Professional",
          text: "Hi {{agent_first_name}}, are you the owner of {{property_address}}?",
          active: "Yes",
          category_primary: null,
          category_secondary: null,
          deliverability_score: 90,
          spam_risk: null,
          historical_reply_rate: 20,
          total_conversations: 0,
          total_replies: 0,
        },
      ];
    },
    local_fetcher: () => [],
  });

  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].item_id, 8001);
  assert.ok(
    calls.some(
      (filter_set) => filter_set.stage === "Stage 1 — Ownership Confirmation"
    )
  );
});

test("template normalization keeps missing spam risk as null instead of forcing exclusion", () => {
  const normalized = normalizeTemplateItem({
    item_id: 9001,
    fields: [
      {
        external_id: "text",
        values: [{ value: "Hi {{agent_first_name}}" }],
      },
      {
        external_id: "active",
        values: [{ value: { text: "Yes" } }],
      },
    ],
  });

  assert.equal(normalized.spam_risk, null);
});
