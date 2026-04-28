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

test("renderTemplate agent aliases render first name only", () => {
  const result = renderTemplate({
    template_text:
      "{{agent_name}}/{{agent_first_name}}/{{sms_agent_name}}/{{sender_name}}/{{rep_name}}",
    context: {
      summary: {
        agent_name: "Helen Marie Carter",
        agent_first_name: "Helen Marie Carter",
      },
    },
  });

  assert.equal(result.rendered_text, "Helen/Helen/Helen/Helen/Helen");
});

test("renderTemplate maps legacy Stage 1 first_name and street_address placeholders to seller data", () => {
  const result = renderTemplate({
    template_text:
      "Hi {{first_name}}, checking on {{street_address}}. Do you still own it?",
    use_case: "ownership_check",
    context: {
      summary: {
        seller_first_name: "Maria",
        property_address: "123 Main St",
        agent_name: "Ryan Kindle",
      },
    },
  });

  assert.equal(
    result.rendered_text,
    "Hi Maria, checking on 123 Main St. Do you still own it?"
  );
});
