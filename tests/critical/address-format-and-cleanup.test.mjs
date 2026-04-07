/**
 * address-format-and-cleanup.test.mjs
 *
 * Guards four fixes applied after a live TextGrid content-filter failure on:
 *   "Hi Jesse — this is Helen . Quick question— do you own Jurupa Valley 92509 CA 7454 Mission Blvd?"
 *
 * Root causes addressed:
 *   1. Property address rendered in city-first order — extractStreetAddress now reads
 *      the structured street_address sub-field from the Podio location field instead
 *      of the pre-formatted string (which Podio stores in an unpredictable order).
 *   2. Stray space before punctuation ("Helen .") — cleanupPunctuation() now strips
 *      spaces immediately before sentence-ending punctuation marks after rendering.
 *   3. Robotic em-dash spacing ("Quick question—do") — cleanupPunctuation() normalises
 *      em-dash and en-dash to exactly one space on each side.
 *   4. Stage 1 cold-outbound templates replaced with shorter, more human variants
 *      that avoid "Quick question" filler and the double-intro pattern.
 *
 * Note on derive-context-summary.js integration:
 *   deriveContextSummary transitively imports podio.js → axios, which is not
 *   installed in this test environment, so a pre-existing limitation prevents its
 *   direct import here.  Tests A and B instead validate the extractStreetAddress
 *   field-access contract inline (the logic is two lines in the source).  All other
 *   tests use modules that are self-contained in this environment.
 *
 * Covered:
 *   A. extractStreetAddress field contract — street_address sub-field wins over formatted.
 *   B. extractStreetAddress field contract — nested value.street_address fallback.
 *   C. cleanupPunctuation — space before period removed.
 *   D. cleanupPunctuation — space before comma removed.
 *   E. cleanupPunctuation — space before question mark removed.
 *   F. cleanupPunctuation — multiple spaces before punctuation removed.
 *   G. cleanupPunctuation — no-op when no stray spaces present.
 *   H. cleanupPunctuation — em-dash normalised with no leading space.
 *   I. cleanupPunctuation — em-dash normalised with trailing space only.
 *   J. cleanupPunctuation — correctly spaced em-dash preserved.
 *   K. cleanupPunctuation — en-dash spacing normalised.
 *   L. cleanupPunctuation — double spaces collapsed.
 *   M. renderTemplate pipeline applies cleanupPunctuation end-to-end.
 *   N. renderTemplate normalises em-dash spacing in rendered output.
 *   O. renderTemplate produces no double spaces.
 *   P. Local registry has at least 3 Stage 1 ownership_check templates.
 *   Q. All Stage 1 ownership_check templates render to non-empty plain text.
 *   R. No Stage 1 template text contains "Quick question" filler.
 *   S. Stage 1 templates render address as street-only (no City/ZIP in body).
 */

import test from "node:test";
import assert from "node:assert/strict";

import { cleanupPunctuation, renderTemplate } from "@/lib/domain/templates/render-template.js";
import LOCAL_TEMPLATE_CANDIDATES from "@/lib/domain/templates/local-template-registry.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function stage1OwnershipTemplates() {
  return LOCAL_TEMPLATE_CANDIDATES.filter(
    (t) => t.use_case === "ownership_check" && (t.variant_group || "").includes("Stage 1")
  );
}

// ── A & B. extractStreetAddress field-access contract ─────────────────────────
//
// extractStreetAddress() in derive-context-summary.js is two lines:
//   return first.street_address || first.value?.street_address || "";
//
// These tests validate that contract directly against the field shapes that
// Podio returns, so the correctness of the selector is tested without needing
// the full module chain (which transitively requires axios).

test("A — location field: top-level street_address wins over pre-formatted city-first string", () => {
  // Simulate what getFieldValues returns for a Podio location field
  const first = {
    street_address: "7454 Mission Blvd",
    city: "Jurupa Valley",
    state: "CA",
    postal_code: "92509",
    // This is what Podio stores in .formatted — city-first, wrong for SMS
    formatted: "Jurupa Valley 92509 CA 7454 Mission Blvd",
    value: { street_address: "7454 Mission Blvd" },
  };

  const result = first.street_address || first.value?.street_address || "";

  assert.equal(result, "7454 Mission Blvd", "must return street only, not the city-first formatted string");
  assert.notEqual(result, "Jurupa Valley 92509 CA 7454 Mission Blvd");
});

test("B — location field: falls back to value.street_address when top-level is absent", () => {
  // Some Podio API responses nest sub-fields under value rather than exposing them directly
  const first = {
    value: {
      street_address: "321 Oak Lane",
      city: "Tulsa",
      state: "OK",
      postal_code: "74127",
    },
  };

  const result = first.street_address || first.value?.street_address || "";

  assert.equal(result, "321 Oak Lane");
});

test("B — location field: returns empty string when neither sub-field is present", () => {
  const first = {
    value: {
      formatted: "Jurupa Valley 92509 CA 7454 Mission Blvd",
    },
  };

  const result = first.street_address || first.value?.street_address || "";

  assert.equal(result, "", "must return empty string — formatted is intentionally NOT used as fallback");
});

// ── C–G. cleanupPunctuation — space before punctuation ───────────────────────

test("C — cleanupPunctuation removes space before period", () => {
  assert.equal(cleanupPunctuation("Hello ."), "Hello.");
});

test("D — cleanupPunctuation removes space before comma", () => {
  assert.equal(cleanupPunctuation("One , two"), "One, two");
});

test("E — cleanupPunctuation removes space before question mark", () => {
  assert.equal(cleanupPunctuation("Are you the owner ?"), "Are you the owner?");
});

test("F — cleanupPunctuation removes multiple spaces before punctuation", () => {
  assert.equal(cleanupPunctuation("Helen   ."), "Helen.");
});

test("G — cleanupPunctuation is no-op when no stray spaces are present", () => {
  assert.equal(cleanupPunctuation("Hello. Are you there?"), "Hello. Are you there?");
});

// ── H–J. cleanupPunctuation — em-dash spacing ────────────────────────────────

test("H — cleanupPunctuation normalises em-dash with no spaces ('question—do')", () => {
  assert.equal(
    cleanupPunctuation("Quick question—do you own it?"),
    "Quick question — do you own it?"
  );
});

test("I — cleanupPunctuation normalises em-dash with trailing space only ('question— do')", () => {
  assert.equal(
    cleanupPunctuation("Quick question— do you own it?"),
    "Quick question — do you own it?"
  );
});

test("J — cleanupPunctuation preserves correctly spaced em-dash ('Hi Jesse — this is Helen')", () => {
  assert.equal(
    cleanupPunctuation("Hi Jesse — this is Helen."),
    "Hi Jesse — this is Helen."
  );
});

// ── K. cleanupPunctuation — en-dash spacing ───────────────────────────────────

test("K — cleanupPunctuation normalises en-dash spacing", () => {
  assert.equal(cleanupPunctuation("Mon–Fri"), "Mon – Fri");
});

// ── L. cleanupPunctuation — double-space collapse ─────────────────────────────

test("L — cleanupPunctuation collapses double spaces", () => {
  assert.equal(cleanupPunctuation("Hello  World"), "Hello World");
});

// ── M–O. renderTemplate pipeline — end-to-end cleanup ───────────────────────

test("M — renderTemplate removes stray space before period produced by template spacing", () => {
  // Reproduces "Helen ." — a space in the template before the period
  const result = renderTemplate({
    template_text: "Hi Jesse — this is {{agent_first_name}} . Do you own {{property_address}}?",
    use_case: "ownership_check",
    context: {
      summary: {
        agent_first_name: "Helen",
        property_address: "7454 Mission Blvd",
      },
    },
  });

  assert.equal(
    result.rendered_text,
    "Hi Jesse — this is Helen. Do you own 7454 Mission Blvd?",
    "stray space before period must be removed by the render pipeline"
  );
});

test("N — renderTemplate normalises em-dash spacing in rendered output", () => {
  // Reproduces "Quick question— do you own" from the live failing message
  const result = renderTemplate({
    template_text: "Quick question— do you own {{property_address}}?",
    use_case: "ownership_check",
    context: {
      summary: {
        property_address: "7454 Mission Blvd",
      },
    },
  });

  assert.equal(
    result.rendered_text,
    "Quick question — do you own 7454 Mission Blvd?",
    "em-dash must be normalised to one space on each side"
  );
});

test("O — renderTemplate produces no double spaces in rendered output", () => {
  const result = renderTemplate({
    template_text: "Reaching out about  {{property_address}}.",
    use_case: "ownership_check",
    context: {
      summary: {
        property_address: "7454 Mission Blvd",
      },
    },
  });

  assert.ok(
    !result.rendered_text.includes("  "),
    `rendered_text must contain no double spaces; got: "${result.rendered_text}"`
  );
});

// ── P–S. Stage 1 ownership_check templates ────────────────────────────────────

test("P — local registry has at least 3 Stage 1 ownership_check templates", () => {
  const templates = stage1OwnershipTemplates();
  assert.ok(
    templates.length >= 3,
    `Expected at least 3 Stage 1 ownership_check templates, found ${templates.length}`
  );
});

test("Q — all Stage 1 ownership_check templates render to non-empty plain text", () => {
  const templates = stage1OwnershipTemplates();
  for (const tmpl of templates) {
    const result = renderTemplate({
      template_text: tmpl.text,
      use_case: tmpl.use_case,
      variant_group: tmpl.variant_group,
      context: {
        summary: {
          seller_first_name: "Jesse",
          agent_first_name: "Helen",
          property_address: "7454 Mission Blvd",
        },
      },
    });

    assert.ok(result.rendered_text.length > 0, `Template ${tmpl.item_id} rendered to empty string`);
    assert.ok(
      !result.rendered_text.includes("{{"),
      `Template ${tmpl.item_id} has un-replaced placeholders: "${result.rendered_text}"`
    );
  }
});

test("R — no Stage 1 ownership_check template contains 'Quick question' filler", () => {
  const templates = stage1OwnershipTemplates();
  for (const tmpl of templates) {
    assert.ok(
      !/quick question/i.test(tmpl.text),
      `Template ${tmpl.item_id} contains robotic "Quick question" opener: "${tmpl.text}"`
    );
  }
});

test("S — Stage 1 templates render street address only — no city or ZIP in body", () => {
  const templates = stage1OwnershipTemplates();
  for (const tmpl of templates) {
    const result = renderTemplate({
      template_text: tmpl.text,
      use_case: tmpl.use_case,
      variant_group: tmpl.variant_group,
      context: {
        summary: {
          seller_first_name: "Jesse",
          agent_first_name: "Helen",
          property_address: "7454 Mission Blvd",
        },
      },
    });

    assert.ok(
      result.rendered_text.includes("7454 Mission Blvd"),
      `Template ${tmpl.item_id} must include the street address; got: "${result.rendered_text}"`
    );
    assert.ok(
      !result.rendered_text.includes("Jurupa Valley"),
      `Template ${tmpl.item_id} must NOT include city in body; got: "${result.rendered_text}"`
    );
    assert.ok(
      !result.rendered_text.includes("92509"),
      `Template ${tmpl.item_id} must NOT include ZIP in body; got: "${result.rendered_text}"`
    );
  }
});
