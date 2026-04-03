import test from "node:test";
import assert from "node:assert/strict";

import { LIFECYCLE_STAGES, STAGES } from "@/lib/config/stages.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";

function buildClassification(overrides = {}) {
  return {
    language: "English",
    emotion: "calm",
    stage_hint: "Ownership",
    compliance_flag: null,
    objection: null,
    ...overrides,
  };
}

test("blank outbound routing stays on ownership and targets outbound initial templates", () => {
  const route = resolveRoute({
    classification: buildClassification(),
    brain_item: null,
    phone_item: null,
    message: "",
  });

  assert.equal(route.use_case, "ownership_check");
  assert.equal(route.stage, STAGES.OWNERSHIP);
  assert.equal(route.lifecycle_stage, LIFECYCLE_STAGES.OWNERSHIP);
  assert.equal(route.secondary_category, "Outbound Initial");
  assert.equal(route.template_filters.secondary_category, "Outbound Initial");
});

test("contract skip-ahead messaging resolves to the exact contract request flow", () => {
  const route = resolveRoute({
    classification: buildClassification(),
    brain_item: null,
    phone_item: null,
    message: "Send the contract over and I will review it.",
  });

  assert.equal(route.use_case, "asks_contract");
  assert.equal(route.stage, STAGES.CONTRACT);
  assert.equal(route.lifecycle_stage, LIFECYCLE_STAGES.CONTRACT);
  assert.equal(route.variant_group, "Stage 6 — Contract Request");
  assert.equal(route.secondary_category, "Close / Handoff");
});

test("closing skip-ahead messaging resolves to clear-to-close templates", () => {
  const route = resolveRoute({
    classification: buildClassification(),
    brain_item: null,
    phone_item: null,
    message: "We are clear to close now.",
  });

  assert.equal(route.use_case, "clear_to_close");
  assert.equal(route.stage, STAGES.CONTRACT);
  assert.equal(route.lifecycle_stage, LIFECYCLE_STAGES.CLOSING);
  assert.equal(route.variant_group, "Stage 6 — Clear to Close");
  assert.equal(route.tone, "Direct");
});

test("written-offer routing aligns send_package with the workbook template family", () => {
  const route = resolveRoute({
    classification: buildClassification({
      objection: "wants_written_offer",
    }),
    brain_item: null,
    phone_item: null,
    message: "Send the full package.",
  });

  assert.equal(route.use_case, "send_package");
  assert.equal(route.stage, STAGES.CONTRACT);
  assert.equal(route.lifecycle_stage, LIFECYCLE_STAGES.CONTRACT);
  assert.equal(route.primary_category, "Corporate / Institutional");
  assert.equal(route.secondary_category, "Close / Handoff");
  assert.equal(route.variant_group, "Stage 6 — Package Send");
  assert.equal(route.tone, "Corporate");
  assert.equal(route.template_filters.paired_with_agent_type, "Specialist-Corporate");
  assert.equal(route.template_filters.fallback_agent_type, "Specialist-Corporate");
});
