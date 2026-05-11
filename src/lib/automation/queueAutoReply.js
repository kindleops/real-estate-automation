// ─── queueAutoReply.js ──────────────────────────────────────────────────
import { supabase as defaultSupabase } from "@/lib/supabase/client.js";
import { classify as defaultClassify } from "@/lib/domain/classification/classify.js";
import {
  selectNextTemplate as defaultSelect,
  validateTemplateForIntent as defaultValidate,
  renderSafeTemplate as defaultRender,
} from "./templateSelector.js";
import { ACTIONS, getIntentRoute } from "./intentMap.js";
import { evaluateContactWindow as defaultWindowCheck } from "@/lib/supabase/sms-engine.js";

const defaultDeps = {
  supabase: defaultSupabase,
  classify: defaultClassify,
  selectNextTemplate: defaultSelect,
  validateTemplateForIntent: defaultValidate,
  renderSafeTemplate: defaultRender,
  evaluateContactWindow: defaultWindowCheck,
};

let deps = { ...defaultDeps };

export function __setQueueDeps(overrides = {}) {
  deps = { ...deps, ...overrides };
}

export function __resetQueueDeps() {
  deps = { ...defaultDeps };
}

/**
 * Main entry point for auto-reply logic.
 *
 * @param {string} thread_key
 * @param {string} inbound_message_id
 * @param {object} [options]
 * @param {boolean} [options.dry_run=false]
 * @returns {Promise<object>} Result of the auto-reply attempt
 */
export async function queueAutoReply(thread_key, inbound_message_id, { dry_run = false } = {}) {
  // 1. Deduplication
  if (!dry_run) {
    const { data: existing, error: checkError } = await deps.supabase
      .from("send_queue")
      .select("id")
      .eq("inbound_message_id", inbound_message_id)
      .maybeSingle();

    if (existing) {
      return {
        ok: false,
        reason: "duplicate_reply_prevented",
        queue_id: existing.id,
      };
    }
  }

  // 2. Fetch Inbound Message Context
  const { data: inbound, error: inboundError } = await deps.supabase
    .from("message_events")
    .select("*")
    .eq("id", inbound_message_id)
    .single();

  if (inboundError || !inbound) {
    return { ok: false, reason: "inbound_message_not_found" };
  }

  // 3. Classify
  const brain_id = inbound.conversation_brain_id;
  const classification = await deps.classify(inbound.message_body, {
    brain_id,
    language_preference: inbound.language || "English",
  });

  const route = getIntentRoute(classification.primary_intent);

  // 4. Update Conversation State (Internal DB or Podio)
  // Logic to update brain stage goes here...

  // 5. Select Template
  const context = {
    classification,
    language: classification.language || inbound.language || "English",
    thread_key,
    agent_style_fit: null,
    property_type_scope: null,
    deal_strategy: null,
    variables: inbound.metadata?.personalization_context || {},
  };

  const selection = await deps.selectNextTemplate(context);

  if (!selection.ok) {
    return {
      ok: false,
      action: selection.action,
      reason: selection.reason,
    };
  }

  const template = selection.template;

  // 6. Render
  const render = deps.renderSafeTemplate(template, context.variables);
  if (!render.ok) {
    return { ok: false, reason: "render_failed", error: render.reason };
  }

  // 7. Safety Gates
  const safety = deps.validateTemplateForIntent(template, context);
  if (!safety.ok) {
    return { ok: false, reason: "safety_gate_violation", error: safety.reason };
  }

  // 8. Contact Window Check
  const windowCheck = deps.evaluateContactWindow({
    timezone: inbound.metadata?.timezone || "America/New_York",
    contact_window: inbound.metadata?.contact_window,
  });

  const scheduled_for = windowCheck.allowed ? new Date().toISOString() : null;

  // 9. Queue
  if (dry_run) {
    return {
      ok: true,
      action: ACTIONS.QUEUE_REPLY,
      queue_id: "dry_run_placeholder",
      use_case: selection.use_case,
      rendered_text: render.text,
      metadata: {
        classification_snapshot: classification,
        template_selection_reason: template.matches,
        personalization_context: context.variables,
        scheduled_for,
      }
    };
  }

  const { data: queued, error: queueError } = await deps.supabase
    .from("send_queue")
    .insert({
      thread_key,
      inbound_message_id,
      message_body: render.text,
      to_phone_number: inbound.from_phone_number,
      from_phone_number: inbound.to_phone_number,
      queue_status: "queued",
      scheduled_for,
      template_id: template.template_id,
      selected_template_id: template.id,
      sms_agent_id: inbound.sms_agent_id,
      current_stage: selection.stage_code,
      stage_before: inbound.current_stage,
      stage_after: selection.stage_code,
      detected_intent: classification.primary_intent,
      ai_confidence: classification.confidence,
      metadata: {
        classification_snapshot: classification,
        template_selection_reason: template.matches,
        personalization_context: context.variables,
      },
    })
    .select()
    .single();

  if (queueError) {
    return {
      ok: false,
      reason: "queue_insert_failed",
      error: queueError.message,
    };
  }

  return {
    ok: true,
    action: ACTIONS.QUEUE_REPLY,
    queue_id: queued.id,
    use_case: selection.use_case,
    rendered_text: render.text,
  };
}


export default { queueAutoReply };

