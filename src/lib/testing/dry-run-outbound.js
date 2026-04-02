import { loadContext } from "@/lib/domain/context/load-context.js";
import { classify } from "@/lib/domain/classification/classify.js";
import { resolveRoute } from "@/lib/domain/routing/resolve-route.js";
import { loadTemplate } from "@/lib/domain/templates/load-template.js";
import { renderTemplate } from "@/lib/domain/templates/render-template.js";
import { chooseTextgridNumber } from "@/lib/domain/routing/choose-textgrid-number.js";
import { normalizeUsPhone10 } from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

export async function dryRunOutbound({
  phone = "",
  seed_message = "",
  use_case = null,
  language = null,
} = {}) {
  const inbound_from = clean(phone);
  const normalized_phone = normalizeUsPhone10(inbound_from);

  if (!inbound_from) {
    return {
      ok: false,
      preview: false,
      reason: "missing_phone",
    };
  }

  if (!normalized_phone) {
    return {
      ok: false,
      preview: false,
      reason: "invalid_phone",
      phone: inbound_from,
    };
  }

  const context = await loadContext({
    inbound_from: normalized_phone,
    create_brain_if_missing: true,
  });

  if (!context?.found) {
    return {
      ok: false,
      preview: false,
      reason: context?.reason || "context_not_found",
      context,
    };
  }

  const classification = clean(seed_message)
    ? await classify(clean(seed_message), context.items?.brain_item || null)
    : {
        language: language || context?.summary?.language_preference || "English",
        emotion: "calm",
        stage_hint: context?.summary?.conversation_stage || "Ownership",
        compliance_flag: null,
      };

  const route = resolveRoute({
    classification,
    brain_item: context.items?.brain_item || null,
    phone_item: context.items?.phone_item || null,
    message: clean(seed_message),
  });

  const resolved_language =
    language ||
    route?.language ||
    classification?.language ||
    context?.summary?.language_preference ||
    "English";

  const resolved_use_case =
    use_case ||
    route?.use_case ||
    "ownership_check";

  const selected_template = await loadTemplate({
    category: route?.template_filters?.category || route?.primary_category || "Residential",
    secondary_category:
      route?.template_filters?.secondary_category || route?.secondary_category || null,
    use_case: resolved_use_case,
    variant_group: route?.variant_group || "Stage 1 — Ownership Confirmation",
    tone: route?.tone || "Warm",
    gender_variant: "Neutral",
    language: resolved_language,
    sequence_position: route?.sequence_position || "1st Touch",
    paired_with_agent_type:
      route?.template_filters?.paired_with_agent_type || route?.persona || "Warm Professional",
    recently_used_template_ids: context?.recent?.recently_used_template_ids || [],
  });

  if (!selected_template?.item_id) {
    return {
      ok: false,
      preview: false,
      reason: "template_not_found",
      context,
      classification,
      route,
    };
  }

  const render_result = renderTemplate({
    template_text: selected_template.text || "",
    context,
    overrides: {
      language: resolved_language,
      conversation_stage: route?.stage,
      ai_route: route?.brain_ai_route,
    },
  });

  const selected_number = await chooseTextgridNumber({
    context,
    classification,
    route,
    preferred_language: resolved_language,
  });

  if (!selected_number?.item_id) {
    return {
      ok: false,
      preview: true,
      reason: "textgrid_number_not_found",
      phone: normalized_phone,
      context,
      classification,
      route,
      template_item: selected_template,
      template_id: selected_template.item_id,
      rendered_message_text: render_result.rendered_text,
      missing_placeholders: render_result.missing_placeholders,
      textgrid_number: selected_number || null,
    };
  }

  return {
    ok: true,
    preview: true,
    reason: "outbound_preview_built",
    phone: normalized_phone,
    context,
    classification,
    route,
    template_item: selected_template,
    template_id: selected_template.item_id,
    rendered_message_text: render_result.rendered_text,
    missing_placeholders: render_result.missing_placeholders,
    textgrid_number: selected_number,
  };
}

export default dryRunOutbound;
