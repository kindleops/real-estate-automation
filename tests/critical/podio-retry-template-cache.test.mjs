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
  loadTemplate,
  loadTemplateCandidates,
} from "@/lib/domain/templates/load-template.js";
import { normalizeTemplateItem } from "@/lib/podio/apps/templates.js";

function buildTemplateContext(overrides = {}) {
  return {
    summary: {
      seller_first_name: "Sam",
      agent_first_name: "Rachel",
      property_address: "123 Main Street",
      property_city: "Tulsa",
      offer_price: "$155,000",
      repair_cost: "$18,000",
      ...overrides,
    },
  };
}

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
    text: "Hi {{seller_first_name}}, are you the owner of {{property_address}}?",
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
    context: buildTemplateContext(),
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
    context: buildTemplateContext(),
    recently_used_template_ids: [],
    fallback_agent_type: "Warm Professional",
    allow_variant_group_fallback: true,
    remote_fetcher: async (filter_set) => {
      calls.push(filter_set);
      if (!filter_set.stage) return [];
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

test("template loader prefers active Podio templates over local fallbacks", async () => {
  const selected = await loadTemplate({
    category: "Residential",
    use_case: "ownership_check",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    context: buildTemplateContext(),
    remote_fetcher: async (filter_set) => {
      if (filter_set["use-case"] !== "ownership_check") return [];
      return [
        {
          item_id: 9101,
          source: "podio",
          use_case: "ownership_check",
          variant_group: "Stage 1 — Ownership Confirmation",
          tone: "Warm",
          gender_variant: "Neutral",
          language: "English",
          sequence_position: "1st Touch",
          paired_with_agent_type: "Warm Professional",
          text: "Hi {{seller_first_name}}, are you the owner of {{property_address}}?",
          active: "Yes",
          category_primary: "Residential",
          category_secondary: null,
          deliverability_score: 40,
          spam_risk: 2,
          historical_reply_rate: 6,
          total_conversations: 2,
          total_replies: 1,
        },
      ];
    },
    local_fetcher: () => [
      {
        item_id: "local-9101",
        source: "local_registry",
        use_case: "ownership_check",
        variant_group: "Stage 1 — Ownership Confirmation",
        tone: "Warm",
        gender_variant: "Neutral",
        language: "English",
        sequence_position: "1st Touch",
        paired_with_agent_type: "Warm Professional",
        text: "Hi {{seller_first_name}}, are you the owner of {{property_address}}?",
        active: "Yes",
        category_primary: "Residential",
        category_secondary: null,
        deliverability_score: 99,
        spam_risk: 0,
        historical_reply_rate: 99,
        total_conversations: 99,
        total_replies: 99,
      },
    ],
  });

  assert.equal(selected?.item_id, 9101);
  assert.equal(selected?.source, "podio");
});

test("template loader rejects templates when required placeholder data is missing", async () => {
  const candidates = await loadTemplateCandidates({
    category: "Residential",
    use_case: "offer_reveal_cash",
    variant_group: "Stage 5A Cash Offer Reveal",
    tone: "Warm",
    language: "English",
    sequence_position: "V1",
    paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
    context: buildTemplateContext({
      offer_price: "",
    }),
    remote_fetcher: async () => [
      {
        item_id: 9201,
        use_case: "offer_reveal_cash",
        variant_group: "Stage 5A Cash Offer Reveal",
        tone: "Warm",
        gender_variant: "Neutral",
        language: "English",
        sequence_position: "V1",
        paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
        text: "I can do around {{offer_price}} on {{property_address}}.",
        active: "Yes",
        category_primary: "Residential",
        category_secondary: null,
        deliverability_score: 90,
        spam_risk: 2,
        historical_reply_rate: 20,
        total_conversations: 5,
        total_replies: 2,
      },
    ],
    local_fetcher: () => [],
  });

  assert.equal(candidates.length, 0);
});

test("stage-6 canonical routes resolve live Podio aliases before local fallbacks", async () => {
  const cases = [
    {
      canonical_use_case: "ask_timeline",
      alias_use_case: "text_me_later_specific",
      variant_group: "Stage 6B Ask Timeline",
      item_id: 9301,
      text: "No problem. When should I circle back on {{property_address}}?",
    },
    {
      canonical_use_case: "ask_condition_clarifier",
      alias_use_case: "condition_question_set",
      variant_group: "Stage 6C Ask Condition Clarifier",
      item_id: 9302,
      text: "Before I respond to that, is {{property_address}} occupied or does it need work?",
    },
    {
      canonical_use_case: "narrow_range",
      alias_use_case: "can_you_do_better",
      variant_group: "Stage 6D Narrow Range",
      item_id: 9303,
      text: "What number would make sense for {{property_address}}?",
    },
  ];

  for (const scenario of cases) {
    const calls = [];
    const selected = await loadTemplate({
      category: "Residential",
      use_case: scenario.canonical_use_case,
      variant_group: scenario.variant_group,
      tone: "Warm",
      language: "English",
      sequence_position: "V1",
      paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
      context: buildTemplateContext(),
      remote_fetcher: async (filter_set) => {
        calls.push(filter_set);

        if (filter_set["use-case"] === scenario.canonical_use_case) {
          throw Object.assign(
            new Error(
              `[Podio] Invalid category value "${scenario.canonical_use_case}"`
            ),
            {
              status: 400,
              response: { status: 400 },
            }
          );
        }

        if (filter_set["use-case"] === scenario.alias_use_case) {
          return [
            {
              item_id: scenario.item_id,
              source: "podio",
              use_case: scenario.alias_use_case,
              variant_group: scenario.variant_group,
              tone: "Warm",
              gender_variant: "Neutral",
              language: "English",
              sequence_position: "V1",
              paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
              text: scenario.text,
              active: "Yes",
              category_primary: "Residential",
              category_secondary: null,
              deliverability_score: 80,
              spam_risk: 2,
              historical_reply_rate: 20,
              total_conversations: 10,
              total_replies: 5,
            },
          ];
        }

        return [];
      },
      local_fetcher: () => [
        {
          item_id: `local-${scenario.item_id}`,
          source: "local_registry",
          use_case: scenario.canonical_use_case,
          variant_group: scenario.variant_group,
          tone: "Warm",
          gender_variant: "Neutral",
          language: "English",
          sequence_position: "V1",
          paired_with_agent_type: "Fallback / Market-Local / Specialist-Close",
          text: scenario.text,
          active: "Yes",
          category_primary: "Residential",
          category_secondary: null,
          deliverability_score: 99,
          spam_risk: 0,
          historical_reply_rate: 99,
          total_conversations: 99,
          total_replies: 99,
        },
      ],
    });

    assert.equal(selected?.item_id, scenario.item_id);
    assert.equal(selected?.source, "podio");
    assert.ok(calls.some((filter_set) => filter_set["use-case"] === scenario.canonical_use_case));
    assert.ok(calls.some((filter_set) => filter_set["use-case"] === scenario.alias_use_case));
  }
});
