import test from "node:test";
import assert from "node:assert/strict";

import {
  activatePodioRateLimitCooldown,
  buildPodioBackpressureSkipResult,
  clearPodioRateLimitCooldown,
  getPodioRetryAfterSeconds,
  getPodioRateLimitPressureState,
  getPodioRateLimitCooldown,
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
import { renderTemplate } from "@/lib/domain/templates/render-template.js";
import { buildSendQueueItem } from "@/lib/domain/queue/build-send-queue-item.js";
import { normalizeTemplateItem } from "@/lib/podio/apps/templates.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  locationField,
  textField,
} from "../helpers/test-helpers.js";

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

test("Podio cooldown activates from retry-after windows and reports remaining wait time", async () => {
  await clearPodioRateLimitCooldown({ suppress_log: true });

  await activatePodioRateLimitCooldown({
    method: "post",
    path: "/item/app/30541680/filter/",
    status: 420,
    headers: {
      "x-rate-limit-limit": "250",
      "x-rate-limit-remaining": "0",
    },
    retry_after_seconds: 3600,
    error: new Error(
      "You have hit the rate limit. Please wait 3600 seconds before trying again."
    ),
  });

  const cooldown = await getPodioRateLimitCooldown();

  assert.equal(cooldown.active, true);
  assert.equal(cooldown.path, "/item/app/30541680/filter/");
  assert.equal(cooldown.operation, "filter_items");
  assert.equal(cooldown.rate_limit_remaining, 0);
  assert.ok(
    cooldown.retry_after_seconds_remaining >= 3590 &&
      cooldown.retry_after_seconds_remaining <= 3600
  );

  await clearPodioRateLimitCooldown({ suppress_log: true });
});

test("Podio cooldown clears cleanly after reset", async () => {
  await clearPodioRateLimitCooldown({ suppress_log: true });

  await activatePodioRateLimitCooldown({
    method: "post",
    path: "/item/app/30541680/filter/",
    status: 420,
    retry_after_seconds: 60,
    headers: {
      "x-rate-limit-limit": "250",
      "x-rate-limit-remaining": "0",
    },
    error: new Error(
      "You have hit the rate limit. Please wait 60 seconds before trying again."
    ),
  });

  await clearPodioRateLimitCooldown({ suppress_log: true });
  const cooldown = await getPodioRateLimitCooldown();

  assert.equal(cooldown.active, false);
  assert.equal(cooldown.cooldown_until, null);
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

test("template batch cache expires so Podio template changes can replace stale runtime matches", async () => {
  clearTemplateBatchCache();

  let calls = 0;
  const fetcher = async () => {
    calls += 1;
    return [{ item_id: calls }];
  };

  const first = await fetchTemplatesCached(
    {
      language: "English",
      "use-case": "ownership_check",
    },
    { fetcher, cache_ttl_ms: 5 }
  );

  await new Promise((resolve) => setTimeout(resolve, 10));

  const second = await fetchTemplatesCached(
    {
      "use-case": "ownership_check",
      language: "English",
    },
    { fetcher, cache_ttl_ms: 5 }
  );

  assert.equal(calls, 2);
  assert.notEqual(first, second);

  clearTemplateBatchCache();
});

test("Podio backpressure helper activates for low remaining budget without requiring a hard cooldown", async () => {
  resetPodioRateLimitObservability();
  await clearPodioRateLimitCooldown({ suppress_log: true });

  recordPodioRateLimitObservation({
    method: "post",
    path: "/item/app/30541680/filter/",
    status: 200,
    duration_ms: 180,
    attempt: 1,
    headers: {
      "x-rate-limit-limit": "1000",
      "x-rate-limit-remaining": "42",
    },
  });

  const pressure = await getPodioRateLimitPressureState({
    min_remaining: 50,
    max_age_ms: 60_000,
  });
  const skip = await buildPodioBackpressureSkipResult(
    {
      scanned_count: 0,
    },
    {
      min_remaining: 50,
      max_age_ms: 60_000,
    }
  );

  assert.equal(pressure.active, true);
  assert.equal(pressure.reason, "podio_rate_limit_low_remaining");
  assert.equal(pressure.observation?.rate_limit_remaining, 42);
  assert.equal(skip?.ok, true);
  assert.equal(skip?.skipped, true);
  assert.equal(skip?.reason, "podio_rate_limit_low_remaining");
  assert.equal(skip?.podio_backpressure?.observation?.path, "/item/app/30541680/filter/");

  resetPodioRateLimitObservability();
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

test("template normalization reads live Communications Engine fields", () => {
  const normalized = normalizeTemplateItem({
    item_id: 9002,
    fields: [
      {
        external_id: "template-id",
        values: [{ value: 321 }],
      },
      {
        external_id: "use-case",
        values: [{ value: { text: "ownership_check" } }],
      },
      {
        external_id: "use-case-2",
        values: [{ value: { text: "ownership_check" } }],
      },
      {
        external_id: "stage",
        values: [{ value: { text: "Stage 1 — Ownership Confirmation" } }],
      },
      {
        external_id: "stage-label",
        values: [{ value: { text: "Ownership Confirmation" } }],
      },
      {
        external_id: "stage-code",
        values: [{ value: { text: "ownership_confirmation" } }],
      },
      {
        external_id: "category-2",
        values: [{ value: { text: "Outreach" } }],
      },
      {
        external_id: "text",
        values: [{ value: "Hi {{seller_first_name}}" }],
      },
      {
        external_id: "active",
        values: [{ value: { text: "Yes" } }],
      },
    ],
  });

  assert.equal(normalized.template_id, 321);
  assert.equal(normalized.use_case, "ownership_check");
  assert.equal(normalized.use_case_label, "ownership_check");
  assert.equal(normalized.canonical_routing_slug, "ownership_check");
  assert.equal(normalized.variant_group, "Stage 1 — Ownership Confirmation");
  assert.equal(normalized.stage_label, "Ownership Confirmation");
  assert.equal(normalized.stage_code, "ownership_confirmation");
  assert.equal(normalized.category_secondary, "Outreach");
});

test("template candidate loader uses the live category-2 field for secondary category filters", async () => {
  const filter_sets = [];

  const candidates = await loadTemplateCandidates({
    category: "Residential",
    secondary_category: "Outbound Initial",
    use_case: "ownership_check",
    language: "English",
    paired_with_agent_type: "Warm Professional",
    remote_fetcher: async (filter_set) => {
      filter_sets.push(filter_set);
      return [];
    },
    local_fetcher: () => [],
  });

  assert.deepEqual(candidates, []);
  assert.ok(
    filter_sets.some((filter_set) => filter_set["category-2"] === "Outbound Initial"),
    "expected at least one Podio filter set to target Templates::category-2"
  );
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
  assert.equal(selected?.template_resolution_source, "podio_template");
  assert.equal(selected?.template_fallback_reason, null);
});

test("template loader falls back to local templates when Podio template fetch fails", async () => {
  const selected = await loadTemplate({
    category: "Residential",
    use_case: "ownership_check",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    context: buildTemplateContext(),
    remote_fetcher: async () => {
      throw new Error("podio_templates_unavailable");
    },
  });

  assert.equal(selected?.source, "local_registry");
  assert.match(selected?.item_id || "", /^local-template:/);
  assert.equal(selected?.template_resolution_source, "local_template_fallback");
  assert.equal(selected?.template_fallback_reason, "podio_template_fetch_failed");
});

test("template loader uses stage-based Podio fallback before local templates when enabled", async () => {
  const selected = await loadTemplate({
    category: "Residential",
    use_case: "ownership_check",
    variant_group: "Stage 1 — Ownership Confirmation",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    context: buildTemplateContext(),
    allow_variant_group_fallback: true,
    remote_fetcher: async (filter_set) => {
      if (!filter_set.stage || filter_set["use-case"]) return [];
      return [
        {
          item_id: 9201,
          source: "podio",
          use_case: null,
          variant_group: filter_set.stage,
          tone: "Warm",
          gender_variant: "Neutral",
          language: "English",
          sequence_position: "1st Touch",
          paired_with_agent_type: "Warm Professional",
          text: "Hi {{seller_first_name}}, are you the owner of {{property_address}}?",
          active: "Yes",
          category_primary: "Residential",
          category_secondary: null,
          deliverability_score: 60,
          spam_risk: 2,
          historical_reply_rate: 10,
          total_conversations: 3,
          total_replies: 1,
        },
      ];
    },
    local_fetcher: () => [
      {
        item_id: "local-9201",
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

  assert.equal(selected?.item_id, 9201);
  assert.equal(selected?.source, "podio");
  assert.equal(selected?.template_resolution_source, "podio_template");
});

test("template loader falls back to agent-free Stage 1 local templates when agent metadata is missing", async () => {
  const selected = await loadTemplate({
    category: "Residential",
    use_case: "ownership_check",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    context: buildTemplateContext({
      agent_first_name: "",
    }),
    remote_fetcher: async () => [],
  });

  assert.equal(selected?.item_id, "local-template:ownership_check:no-agent:v1");
  assert.match(selected?.text || "", /property_address/);
});

test("template loader resolves Stage 1 fallback even when property category metadata is missing", async () => {
  const selected = await loadTemplate({
    category: null,
    secondary_category: null,
    use_case: "ownership_check",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    context: buildTemplateContext({
      agent_first_name: "",
    }),
    remote_fetcher: async () => [],
  });

  assert.equal(selected?.item_id, "local-template:ownership_check:no-agent:v1");
});

test("agent-free Stage 1 fallback renders and builds a queue item when property metadata is missing", async () => {
  const context = {
    found: true,
    summary: {
      seller_first_name: "Sam",
      agent_first_name: "",
      property_address: "123 Main Street",
      property_city: "Tulsa",
      contact_window: "8AM-9PM Local",
      market_timezone: "Central",
      total_messages_sent: 0,
    },
    recent: {
      touch_count: 0,
    },
    ids: {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      property_id: 601,
      market_id: null,
      assigned_agent_id: null,
    },
    items: {
      phone_item: createPodioItem(401, {
        "phone-activity-status": categoryField("Active for 12 months or longer"),
        "phone-hidden": textField("9185550000"),
        "canonical-e164": textField("+19185550000"),
        "linked-master-owner": appRefField(201),
        "linked-contact": appRefField(301),
        "primary-property": appRefField(601),
      }),
      master_owner_item: createPodioItem(201, {
        "owner-full-name": textField("Sam Seller"),
        "owner-type": categoryField("INDIVIDUAL | ABSENTEE"),
      }),
      property_item: createPodioItem(601, {
        "property-address": locationField({
          street_address: "123 Main Street",
          city: "Tulsa",
          state: "OK",
          postal_code: "74103",
        }),
      }),
      brain_item: null,
      agent_item: null,
      market_item: null,
    },
  };

  const selected = await loadTemplate({
    category: null,
    secondary_category: null,
    use_case: "ownership_check",
    tone: "Warm",
    language: "English",
    sequence_position: "1st Touch",
    paired_with_agent_type: "Warm Professional",
    context,
    remote_fetcher: async () => [],
  });

  assert.equal(selected?.item_id, "local-template:ownership_check:no-agent:v1");

  const rendered = renderTemplate({
    template_text: selected?.text,
    context,
    use_case: selected?.use_case,
    variant_group: selected?.variant_group,
  });

  assert.equal(rendered.ok, true);
  assert.match(rendered.rendered_text, /123 Main Street/);

  let created_fields = null;
  const queued = await buildSendQueueItem({
    context,
    rendered_message_text: rendered.rendered_text,
    template_id: selected?.item_id,
    template_item: selected,
    textgrid_number_item_id: 701,
    scheduled_for_local: "2026-04-08 09:00:00",
    contact_window: "8AM-9PM Local",
    create_item: async (_app_id, fields) => {
      created_fields = fields;
      return { item_id: 9001 };
    },
    update_item: async () => {},
  });

  assert.equal(queued.ok, true);
  assert.equal(queued.queue_item_id, 9001);
  assert.match(queued.message_text || "", /Do you still own it\?/);
  assert.deepEqual(created_fields?.properties, [601]);
  assert.equal(queued.property_address_written, true);
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
