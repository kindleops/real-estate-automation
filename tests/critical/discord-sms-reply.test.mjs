/**
 * Discord SMS Reply Tests
 * Tests for safety checks, endpoint, action handlers, and integration
 */

import { describe, it } from "node:test";
import assert from "node:assert";

import {
  generateReplyHash,
  validateReplyText,
  validateInboundMessageEvent,
  validateRecipientNotSuppressed,
  validateFromPhoneIsOurs,
  validateNoDuplicateReply,
  runReplySmsSafetyChecks,
} from "@/lib/discord/reply-sms-safety-checks.js";

import {
  buildSmsReplyActionButtons,
  buildInboundContextButtons,
  buildInboundSmsActionComponents,
  buildSuggestedReplyPreview,
} from "@/lib/discord/discord-components/sms-reply-components.js";
import { resolveReplyContentForMode } from "@/lib/discord/reply-sms-content-resolver.js";

// ─────────────────────────────────────────────────────────────────────────────
// Safety Checks Tests
// ─────────────────────────────────────────────────────────────────────────────

describe("Discord SMS Reply — Safety Checks", () => {
  describe("validateReplyText", () => {
    it("accepts valid reply text", () => {
      const result = validateReplyText("Hi, interested in this property?");
      assert.strictEqual(result.valid, true);
      assert.strictEqual(result.text, "Hi, interested in this property?");
    });

    it("rejects empty reply text", () => {
      const result = validateReplyText("");
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "empty_reply_text");
    });

    it("rejects reply text exceeding max length", () => {
      const long_text = "x".repeat(500);
      const result = validateReplyText(long_text);
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "reply_text_exceeds_max_length");
    });

    it("accepts reply text at max length boundary", () => {
      const max_text = "x".repeat(480);
      const result = validateReplyText(max_text);
      assert.strictEqual(result.valid, true);
    });
  });

  describe("generateReplyHash", () => {
    it("generates consistent hash for same inputs", () => {
      const hash1 = generateReplyHash("Test reply", "event-123");
      const hash2 = generateReplyHash("Test reply", "event-123");
      assert.strictEqual(hash1, hash2);
    });

    it("generates different hash for different replies", () => {
      const hash1 = generateReplyHash("Reply 1", "event-123");
      const hash2 = generateReplyHash("Reply 2", "event-123");
      assert.notStrictEqual(hash1, hash2);
    });

    it("generates different hash for different event IDs", () => {
      const hash1 = generateReplyHash("Test reply", "event-123");
      const hash2 = generateReplyHash("Test reply", "event-456");
      assert.notStrictEqual(hash1, hash2);
    });

    it("hash starts with version prefix", () => {
      const hash = generateReplyHash("Test", "event-123");
      assert.ok(hash.startsWith("reply:"));
    });
  });

  describe("validateInboundMessageEvent", () => {
    it("rejects missing message_event_id", async () => {
      const result = await validateInboundMessageEvent("", null);
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "missing_message_event_id");
    });

    it("rejects when no supabase client", async () => {
      const result = await validateInboundMessageEvent("event-123", null);
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "missing_supabase");
    });

    it("rejects non-existent message event", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              maybeSingle: async () => ({
                data: null,
                error: null,
              }),
            }),
          }),
        }),
      };

      const result = await validateInboundMessageEvent("nonexistent", mock_supabase);
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "message_event_not_found");
    });

    it("rejects outbound event (only accepts inbound)", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              maybeSingle: async () => ({
                data: {
                  id: "event-123",
                  direction: "outbound",
                  from_phone_number: "+16025551234",
                  to_phone_number: "+14155552345",
                  master_owner_id: "owner-1",
                  metadata: {},
                },
                error: null,
              }),
            }),
          }),
        }),
      };

      const result = await validateInboundMessageEvent("event-123", mock_supabase);
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "not_inbound_direction");
    });

    it("accepts valid inbound event", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              maybeSingle: async () => ({
                data: {
                  id: "event-123",
                  direction: "inbound",
                  from_phone_number: "+16025551234",
                  to_phone_number: "+14155552345",
                  master_owner_id: "owner-1",
                  prospect_id: "prospect-1",
                  property_id: "prop-1",
                  textgrid_number_id: "tgn-1",
                  conversation_brain_id: "brain-1",
                  metadata: {},
                  created_at: new Date().toISOString(),
                },
                error: null,
              }),
            }),
          }),
        }),
      };

      const result = await validateInboundMessageEvent("event-123", mock_supabase);
      assert.strictEqual(result.valid, true);
      assert.strictEqual(result.event.id, "event-123");
      assert.strictEqual(result.reason, "inbound_event_valid");
    });
  });

  describe("recipientNotSuppressed", () => {
    it("rejects if recipient opted out", async () => {
      const result = await validateRecipientNotSuppressed(
        "+16025551234",
        { event_type: "opt_out" },
        { from: () => ({ select: () => ({ eq: () => ({ eq: () => ({ maybeSingle: async () => ({ data: null, error: null }) }) }) }) }) }
      );
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "recipient_opted_out");
    });

    it("rejects wrong_number scenarios", async () => {
      const result = await validateRecipientNotSuppressed(
        "+16025551234",
        { event_type: "wrong_number" },
        { from: () => ({ select: () => ({ eq: () => ({ eq: () => ({ maybeSingle: async () => ({ data: null, error: null }) }) }) }) }) }
      );
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "wrong_number_scenario");
    });

    it("accepts non-suppressed number with mock supabase", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              eq: () => ({
                maybeSingle: async () => ({
                  data: null,
                  error: null,
                }),
              }),
            }),
          }),
        }),
      };

      const result = await validateRecipientNotSuppressed(
        "+16025551234",
        { event_type: "inbound_known_reply" },
        mock_supabase
      );
      assert.strictEqual(result.valid, true);
      assert.strictEqual(result.reason, "recipient_not_suppressed");
    });
  });

  describe("validateFromPhoneIsOurs", () => {
    it("accepts our textgrid number", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              maybeSingle: async () => ({
                data: {
                  id: "tgn-1",
                  phone_number: "+14155552345",
                  status: "active",
                },
                error: null,
              }),
            }),
          }),
        }),
      };

      const result = await validateFromPhoneIsOurs("+14155552345", mock_supabase);
      assert.strictEqual(result.valid, true);
      assert.strictEqual(result.textgrid_number_id, "tgn-1");
    });

    it("rejects non-existent number in inventory", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              maybeSingle: async () => ({
                data: null,
                error: null,
              }),
            }),
          }),
        }),
      };

      const result = await validateFromPhoneIsOurs("+19999999999", mock_supabase);
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "phone_not_in_textgrid_inventory");
    });

    it("rejects inactive textgrid number", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              maybeSingle: async () => ({
                data: {
                  id: "tgn-1",
                  phone_number: "+14155552345",
                  status: "suspended",
                },
                error: null,
              }),
            }),
          }),
        }),
      };

      const result = await validateFromPhoneIsOurs("+14155552345", mock_supabase);
      assert.strictEqual(result.valid, false);
      assert.strictEqual(result.reason, "textgrid_number_not_active");
    });
  });

  describe("comprehensive runReplySmsSafetyChecks", () => {
    it("passes all checks for valid reply scenario", async () => {
      const inbound_event = {
        id: "event-123",
        from_phone_number: "+16025551234",
        to_phone_number: "+14155552345",
        master_owner_id: "owner-1",
        prospect_id: "prospect-1",
        property_id: "prop-1",
        textgrid_number_id: "tgn-1",
        conversation_brain_id: "brain-1",
        metadata: { event_type: "inbound_known_reply" },
        created_at: new Date().toISOString(),
      };

      const mock_supabase = {
        from: (table) => ({
          select: () => ({
            eq: () => {
              if (table === "message_events") {
                return {
                  maybeSingle: async () => ({ data: { ...inbound_event, direction: "inbound" }, error: null }),
                };
              }
              if (table === "sms_suppression_list") {
                return {
                  eq: () => ({ maybeSingle: async () => ({ data: null, error: null }) }),
                };
              }
              if (table === "textgrid_numbers") {
                return {
                  maybeSingle: async () => ({
                    data: { id: "tgn-1", phone_number: "+14155552345", status: "active" },
                    error: null,
                  }),
                };
              }
              return {
                gt: () => ({
                  order: () => ({
                    limit: async () => ({ data: [], error: null }),
                  }),
                }),
              };
            },
            or: () => ({
              lt: () => ({
                order: () => ({
                  limit: async () => ({ data: [], error: null }),
                }),
              }),
            }),
          }),
          or: () => ({
            lt: () => ({
              order: () => ({
                limit: async () => ({ data: [], error: null }),
              }),
            }),
          }),
        }),
      };

      const result = await runReplySmsSafetyChecks(
        {
          message_event_id: "event-123",
          reply_text: "Hi! Interested in your property.",
          supabase: mock_supabase,
        }
      );

      assert.strictEqual(result.safe, true);
      assert.strictEqual(result.reason, "all_checks_passed");
      assert.ok(result.reply_hash);
      assert.ok(result.verified_event);
    });

    it("fails when reply text is invalid", async () => {
      const result = await runReplySmsSafetyChecks(
        {
          message_event_id: "event-123",
          reply_text: "", // Empty
          supabase: null,
        }
      );

      assert.strictEqual(result.safe, false);
      assert.strictEqual(result.reason, "reply_text_invalid");
    });

    it("fails when inbound event not found", async () => {
      const mock_supabase = {
        from: () => ({
          select: () => ({
            eq: () => ({
              maybeSingle: async () => ({
                data: null,
                error: null,
              }),
            }),
          }),
        }),
      };

      const result = await runReplySmsSafetyChecks(
        {
          message_event_id: "nonexistent",
          reply_text: "Valid reply text",
          supabase: mock_supabase,
        }
      );

      assert.strictEqual(result.safe, false);
      assert.strictEqual(result.reason, "inbound_event_invalid");
    });
  });
});

describe("Discord SMS Reply — Reply Mode Resolution", () => {
  const inbound_event = {
    id: "event-123",
    direction: "inbound",
    from_phone_number: "+16025551234",
    to_phone_number: "+14155552345",
    master_owner_id: "owner-1",
    metadata: { current_stage: "S2" },
    created_at: new Date().toISOString(),
  };

  it("supports auto_template success", async () => {
    const result = await resolveReplyContentForMode(
      {
        message_event_id: "event-123",
        reply_mode: "auto_template",
        supabase: null,
      },
      {
        inbound_event_override: inbound_event,
        resolveAutoTemplateReplyImpl: async () => ({
          ok: true,
          reply_text: "Auto template rendered",
          rendered_message_preview: "Auto template rendered",
          selected_template_id: "tmpl-1",
          selected_template_use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
          template_source: "supabase_sms_templates",
        }),
      }
    );

    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.reply_mode, "auto_template");
    assert.strictEqual(result.selected_template_id, "tmpl-1");
    assert.strictEqual(result.template_source, "supabase_sms_templates");
  });

  it("falls back to manual when auto_template cannot render", async () => {
    const result = await resolveReplyContentForMode(
      {
        message_event_id: "event-123",
        reply_mode: "auto_template",
        reply_text: "Manual fallback text",
        supabase: null,
      },
      {
        inbound_event_override: inbound_event,
        resolveAutoTemplateReplyImpl: async () => ({
          ok: false,
          reason: "missing_rendered_template",
          message: "No template selected",
        }),
      }
    );

    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.reply_mode, "manual");
    assert.strictEqual(result.reply_text, "Manual fallback text");
  });

  it("rejects invalid template_id without manual fallback", async () => {
    const result = await resolveReplyContentForMode(
      {
        message_event_id: "event-123",
        reply_mode: "template",
        template_id: "bad-id",
        reply_text: "",
        supabase: null,
      },
      {
        inbound_event_override: inbound_event,
        fetchTemplateByIdImpl: async () => null,
      }
    );

    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.reason, "invalid_template_id");
  });

  it("rejects missing rendered template when template render fails and no manual fallback", async () => {
    const result = await resolveReplyContentForMode(
      {
        message_event_id: "event-123",
        reply_mode: "template",
        template_id: "tmpl-1",
        reply_text: "",
        supabase: null,
      },
      {
        inbound_event_override: inbound_event,
        fetchTemplateByIdImpl: async () => ({
          id: "tmpl-1",
          template_body: "{{seller_first_name}}",
          use_case: "ownership_check",
          stage_code: "S1",
          language: "English",
        }),
        renderTemplateMessageImpl: () => ({
          ok: false,
          reason: "missing_rendered_template",
          message: "missing_placeholder_values",
        }),
      }
    );

    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.reason, "missing_rendered_template");
  });

  it("blocks duplicate reply hashes", async () => {
    const dup_hash = generateReplyHash("Duplicate text", "event-123");

    const mock_supabase = {
      from: () => ({
        select: () => ({
          eq: () => ({
            gt: () => ({
              order: () => ({
                limit: async () => ({
                  data: [
                    {
                      id: "q-1",
                      metadata: { reply_hash: dup_hash },
                      created_at: new Date().toISOString(),
                      message_body: "Duplicate text",
                    },
                  ],
                  error: null,
                }),
              }),
            }),
          }),
        }),
      }),
    };

    const result = await validateNoDuplicateReply(
      "event-123",
      "Duplicate text",
      mock_supabase,
      10
    );

    assert.strictEqual(result.valid, false);
    assert.strictEqual(result.reason, "duplicate_reply_detected");
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Component Builder Tests
// ─────────────────────────────────────────────────────────────────────────────

describe("Discord SMS Reply Components", () => {
  describe("buildSmsReplyActionButtons", () => {
    it("returns empty array if no message_event_id", () => {
      const buttons = buildSmsReplyActionButtons({});
      assert.strictEqual(buttons.length, 0);
    });

    it("includes Approve Template button", () => {
      const buttons = buildSmsReplyActionButtons({
        message_event_id: "event-123",
        suggested_reply: "Call us at 555-1234",
      });

      assert.ok(buttons.length > 0);
      assert.ok(buttons[0].components.some((b) => b.label.includes("Approve Template")));
    });

    it("includes Manual Reply button", () => {
      const buttons = buildSmsReplyActionButtons({
        message_event_id: "event-123",
        suggested_reply: "Test suggestion",
      });

      assert.ok(buttons[0].components.some((b) => b.label.includes("Manual Reply")));
    });

    it("includes hot lead and suppress buttons", () => {
      const buttons = buildSmsReplyActionButtons({
        message_event_id: "event-123",
      });

      const component = buttons[0];
      assert.ok(component.components.some((b) => b.label.includes("Hot Lead")));
      assert.ok(component.components.some((b) => b.label.includes("Suppress")));
    });

    it("truncates long message_event_id", () => {
      const long_id = "x".repeat(100);
      const buttons = buildSmsReplyActionButtons({
        message_event_id: long_id,
      });

      assert.ok(buttons.length > 0);
      // Custom ID should be truncated
      const custom_id = buttons[0].components[0].custom_id;
      assert.ok(custom_id.length <= 100);
    });
  });

  describe("buildInboundContextButtons", () => {
    it("includes podio button when podio_item_id provided", () => {
      const buttons = buildInboundContextButtons({
        podio_item_id: "podio-123",
      });

      assert.ok(buttons.length > 0);
      assert.ok(buttons[0].components.some((b) => b.label.includes("Podio")));
    });

    it("includes context button when master_owner_id provided", () => {
      const buttons = buildInboundContextButtons({
        master_owner_id: "owner-123",
      });

      assert.ok(buttons.length > 0);
      assert.ok(buttons[0].components.some((b) => b.label.includes("Context")));
    });

    it("returns empty array if no context IDs", () => {
      const buttons = buildInboundContextButtons({});
      assert.strictEqual(buttons.length, 0);
    });
  });

  describe("buildSuggestedReplyPreview", () => {
    it("returns null for empty reply", () => {
      const preview = buildSuggestedReplyPreview("");
      assert.strictEqual(preview, null);
    });

    it("includes reply text in field", () => {
      const preview = buildSuggestedReplyPreview("Call us at 555-1234");
      assert.ok(preview.value.includes("Call us at 555-1234"));
    });

    it("truncates long replies", () => {
      const long_reply = "x".repeat(500);
      const preview = buildSuggestedReplyPreview(long_reply);
      assert.ok(preview.value.length < 400); // Truncated
    });
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Export functions for DI
// ─────────────────────────────────────────────────────────────────────────────

export function __setDiscordSmsReplyTestDeps(overrides = {}) {
  // Placeholder
}

export function __resetDiscordSmsReplyTestDeps() {
  // Placeholder
}
