import test from "node:test";
import assert from "node:assert/strict";

import { renderTemplate } from "@/lib/domain/templates/render-template.js";

test("renderTemplate replaces legacy single-brace placeholders", () => {
  const result = renderTemplate({
    template_text:
      "Hi, this is {agent_first_name}. Quick question — are you the owner of {property_address}?",
    context: {
      summary: {
        agent_name: "Ryan Kindle",
        property_address: "2717 S 124TH EAST AVE",
      },
    },
  });

  assert.equal(
    result.rendered_text,
    "Hi, this is Ryan. Quick question — are you the owner of 2717 S 124TH EAST AVE?"
  );
});

test("renderTemplate still replaces double-brace placeholders", () => {
  const result = renderTemplate({
    template_text:
      "Hi, this is {{agent_first_name}}. Quick question — are you the owner of {{property_address}}?",
    context: {
      summary: {
        agent_name: "Ryan Kindle",
        property_address: "2717 S 124TH EAST AVE",
      },
    },
  });

  assert.equal(
    result.rendered_text,
    "Hi, this is Ryan. Quick question — are you the owner of 2717 S 124TH EAST AVE?"
  );
});
