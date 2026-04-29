const fs = require('fs');
const file = 'src/lib/flows/handle-textgrid-inbound.js';
let content = fs.readFileSync(file, 'utf8');

// Add imports
content = content.replace(
  'import { maybeQueueSellerStageReply } from "@/lib/domain/seller-flow/maybe-queue-seller-stage-reply.js";',
  'import { maybeQueueSellerStageReply } from "@/lib/domain/seller-flow/maybe-queue-seller-stage-reply.js";\nimport { resolveSellerAutoReplyPlan } from "@/lib/domain/seller-flow/resolve-seller-auto-reply-plan.js";'
);
content = content.replace(
  '  maybeQueueSellerStageReply,\n  updateMasterOwnerAfterInbound,',
  '  maybeQueueSellerStageReply,\n  resolveSellerAutoReplyPlan,\n  updateMasterOwnerAfterInbound,'
);

// Prevent unknown router from queueing
content = content.replace(
  'auto_reply_enabled: inbound_autopilot_enabled,',
  'auto_reply_enabled: false,'
);

// Insert auto reply plan generation before seller_stage_preview is defined
const auto_reply_plan_code = `      const auto_reply_plan = await runtimeDeps.resolveSellerAutoReplyPlan({
        inbound_event: { item_id: inbound_message_event_id, provider_message_id: extracted.message_id, from: inbound_from, to: inbound_to },
        message_body,
        classification,
        route,
        conversation_context: context,
        current_stage: context?.summary?.conversation_stage || null,
        prior_use_case: route?.use_case || null,
        recent_outbound: latest_outbound_event,
        underwriting_signals: signals,
        auto_reply_enabled: inbound_autopilot_enabled,
        force_queue_reply: false,
        now: new Date().toISOString()
      });

      let explicit_use_case = auto_reply_plan.selected_use_case;
      let explicit_template_lookup_use_case = auto_reply_plan.selected_use_case;
      let extra_template_render_overrides = {};
      let extra_queue_context = {
        auto_reply_plan,
        inbound_message_event_id,
        autopilot_reply: true,
        autopilot_override_window_seconds: inbound_autopilot_delay_seconds,
        discord_review_status: auto_reply_plan.should_queue_reply ? "autopilot_pending" : "manual_review_required",
        action_type: "autopilot_inbound_reply",
      };

      const autopilot_schedule = runtimeDeps.buildInboundAutopilotSchedule(
        inbound_autopilot_delay_seconds,
        new Date().toISOString()
      );

      const is_preview = !auto_reply_plan.should_queue_reply;
`;

const replace_sfh_cash_preview = `      if (offer_route === "sfh_cash_preview") {
        explicit_use_case = "offer_reveal_cash";
        explicit_template_lookup_use_case = "offer_reveal_cash";
        const cash_offer = offer_routing?.meta?.cash_offer ?? null;
        const snapshot_id = offer_routing?.meta?.snapshot_id ?? null;
        extra_template_render_overrides = {
          offer_price: formatOfferCurrency(cash_offer),
          smart_cash_offer_display: formatOfferCurrency(cash_offer),
        };
        extra_queue_context.offer_route = offer_route;
        extra_queue_context.cash_offer_amount = cash_offer;
        extra_queue_context.cash_offer_snapshot_id = snapshot_id;
      } else if (offer_route === "condition_clarifier") {
        explicit_use_case = "ask_condition_clarifier";
        explicit_template_lookup_use_case = "ask_condition_clarifier";
        extra_queue_context.offer_route = offer_route;
        extra_queue_context.condition_clarifier_reason = offer_routing?.reason || null;
      } else if (offer_route === "manual_review") {
        // Keep plan but prevent auto queueing
      }

      seller_stage_preview = await runtimeDeps.maybeQueueSellerStageReply({
        inbound_from,
        context,
        classification,
        message: message_body,
        maybe_offer: initial_offer,
        existing_offer,
        explicit_use_case,
        explicit_template_lookup_use_case,
        force_queue_reply: !is_preview,
        extra_queue_context,
        extra_template_render_overrides,
        preview_only: is_preview,
        scheduled_for_local: is_preview ? undefined : autopilot_schedule.scheduled_for_local,
        scheduled_for_utc: is_preview ? undefined : autopilot_schedule.scheduled_for_utc,
        send_priority_override: is_preview ? undefined : "_ Urgent",
      });

      seller_stage_reply = seller_stage_preview;

      if (!is_preview && seller_stage_reply?.ok && seller_stage_reply?.queue_item_id) {
        autopilot_queue_row = {
          id: seller_stage_reply.queue_item_id,
          queue_status: "queued",
          scheduled_for: autopilot_schedule.scheduled_for,
          scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
          scheduled_for_local: autopilot_schedule.scheduled_for_local,
          metadata: extra_queue_context,
        };
      }`;

const target_chunk_1 = `      if (offer_route === "sfh_cash_preview") {
        const cash_offer = offer_routing?.meta?.cash_offer ?? null;
        const snapshot_id = offer_routing?.meta?.snapshot_id ?? null;
        seller_stage_preview = await runtimeDeps.maybeQueueSellerStageReply({
          inbound_from,
          context,
          classification,
          message: message_body,
          maybe_offer: initial_offer,
          existing_offer,
          explicit_use_case: "offer_reveal_cash",
          explicit_template_lookup_use_case: "offer_reveal_cash",
          force_queue_reply: true,
          extra_queue_context: {
            offer_route,
            cash_offer_amount: cash_offer,
            cash_offer_snapshot_id: snapshot_id,
          },
          extra_template_render_overrides: {
            offer_price: formatOfferCurrency(cash_offer),
            smart_cash_offer_display: formatOfferCurrency(cash_offer),
          },
          cash_offer_snapshot_id: snapshot_id,
          preview_only: true,
        });
      } else if (offer_route === "condition_clarifier") {
        seller_stage_preview = await runtimeDeps.maybeQueueSellerStageReply({
          inbound_from,
          context,
          classification,
          message: message_body,
          maybe_offer: initial_offer,
          existing_offer,
          explicit_use_case: "ask_condition_clarifier",
          explicit_template_lookup_use_case: "ask_condition_clarifier",
          force_queue_reply: true,
          extra_queue_context: {
            offer_route,
            condition_clarifier_reason: offer_routing?.reason || null,
          },
          preview_only: true,
        });
      } else if (offer_route === "manual_review") {
        seller_stage_preview = {
          ok: true,
          queued: false,
          handled: true,
          reason: "offer_manual_review_no_auto_send",
          plan: {
            selected_use_case: null,
            detected_intent: null,
          },
          brain_stage: null,
        };

        safeWarn("textgrid.inbound_offer_manual_review", {
          message_id: extracted.message_id,
          inbound_from,
          master_owner_id,
          property_id,
          offer_route_reason: offer_routing?.reason || null,
        });
      } else {
        seller_stage_preview = await runtimeDeps.maybeQueueSellerStageReply({
          inbound_from,
          context,
          classification,
          message: message_body,
          maybe_offer: initial_offer,
          existing_offer,
          preview_only: true,
        });
      }

      seller_stage_reply = seller_stage_preview;`;

content = content.replace(target_chunk_1, auto_reply_plan_code + replace_sfh_cash_preview);

// Prevent underwriting follow up from queueing separate reply if we already have auto reply plan
const target_chunk_2 = `      underwriting_follow_up = !inbound_autopilot_enabled
        ? { ok: true, queued: false, reason: "manual_review_required" }
        : seller_stage_preview?.handled
        ? { ok: true, queued: false, reason: "suppressed_by_seller_stage_reply" }
        : await runtimeDeps.maybeQueueUnderwritingFollowUp({
            inbound_from,
            underwriting,
            classification,
            route,
            context,
            message: message_body,
          });`;

const replace_underwriting = `      underwriting_follow_up = !inbound_autopilot_enabled
        ? { ok: true, queued: false, reason: "manual_review_required" }
        : auto_reply_plan?.should_queue_reply
        ? { ok: true, queued: false, reason: "suppressed_by_auto_reply_plan" }
        : await runtimeDeps.maybeQueueUnderwritingFollowUp({
            inbound_from,
            underwriting,
            classification,
            route,
            context,
            message: message_body,
            dry_run: true // Never queue separate reply, just get the preview/offer_ready state
          });`;
content = content.replace(target_chunk_2, replace_underwriting);

const target_chunk_3 = `      const autopilot_ready = Boolean(
        inbound_autopilot_enabled &&
        seller_stage_preview?.ok &&
        seller_stage_preview?.handled &&
        clean(suggested_reply_preview)
      );

      if (autopilot_ready && inbound_message_event_id) {
        const existing_autopilot_queue = await runtimeDeps.findInboundAutopilotQueue({
          message_event_id: inbound_message_event_id,
          supabase: runtimeDeps.getSupabaseClient?.() || null,
          includeStatuses: ["queued", "sending"],
        }).catch(() => null);

        if (existing_autopilot_queue?.id) {
          autopilot_queue_row = existing_autopilot_queue;
        } else {
          const autopilot_schedule = runtimeDeps.buildInboundAutopilotSchedule(
            inbound_autopilot_delay_seconds,
            new Date().toISOString()
          );

          const queue_extra_context = {
            ...(seller_stage_preview?.queue_result?.queue_context || {}),
            inbound_message_event_id: inbound_message_event_id,
            autopilot_reply: true,
            autopilot_override_window_seconds: inbound_autopilot_delay_seconds,
            discord_review_status: "autopilot_pending",
            action_type: "autopilot_inbound_reply",
          };

          let queued_autopilot_reply = null;
          if (offer_route === "sfh_cash_preview") {
            const cash_offer = offer_routing?.meta?.cash_offer ?? null;
            const snapshot_id = offer_routing?.meta?.snapshot_id ?? null;
            queued_autopilot_reply = await runtimeDeps.maybeQueueSellerStageReply({
              inbound_from,
              context,
              classification,
              message: message_body,
              maybe_offer: initial_offer,
              existing_offer,
              explicit_use_case: "offer_reveal_cash",
              explicit_template_lookup_use_case: "offer_reveal_cash",
              force_queue_reply: true,
              extra_queue_context: {
                offer_route,
                cash_offer_amount: cash_offer,
                cash_offer_snapshot_id: snapshot_id,
                ...queue_extra_context,
              },
              extra_template_render_overrides: {
                offer_price: formatOfferCurrency(cash_offer),
                smart_cash_offer_display: formatOfferCurrency(cash_offer),
              },
              cash_offer_snapshot_id: snapshot_id,
              preview_only: false,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              send_priority_override: "_ Urgent",
            });
          } else if (offer_route === "condition_clarifier") {
            queued_autopilot_reply = await runtimeDeps.maybeQueueSellerStageReply({
              inbound_from,
              context,
              classification,
              message: message_body,
              maybe_offer: initial_offer,
              existing_offer,
              explicit_use_case: "ask_condition_clarifier",
              explicit_template_lookup_use_case: "ask_condition_clarifier",
              force_queue_reply: true,
              extra_queue_context: {
                offer_route,
                condition_clarifier_reason: offer_routing?.reason || null,
                ...queue_extra_context,
              },
              preview_only: false,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              send_priority_override: "_ Urgent",
            });
          } else {
            queued_autopilot_reply = await runtimeDeps.maybeQueueSellerStageReply({
              inbound_from,
              context,
              classification,
              message: message_body,
              maybe_offer: initial_offer,
              existing_offer,
              extra_queue_context: queue_extra_context,
              preview_only: false,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              send_priority_override: "_ Urgent",
            });
          }

          if (queued_autopilot_reply?.ok && queued_autopilot_reply?.queue_item_id) {
            autopilot_queue_row = {
              id: queued_autopilot_reply.queue_item_id,
              queue_status: "queued",
              scheduled_for: autopilot_schedule.scheduled_for,
              scheduled_for_utc: autopilot_schedule.scheduled_for_utc,
              scheduled_for_local: autopilot_schedule.scheduled_for_local,
              metadata: {
                inbound_message_event_id: inbound_message_event_id,
                autopilot_reply: true,
                autopilot_override_window_seconds: inbound_autopilot_delay_seconds,
                discord_review_status: "autopilot_pending",
              },
            };
            seller_stage_reply = {
              ...queued_autopilot_reply,
              preview_result:
                seller_stage_preview?.preview_result ||
                seller_stage_preview?.queue_result ||
                null,
            };
          }
        }
      }`;

content = content.replace(target_chunk_3, `      // Replaced by auto_reply_plan + seller_stage_preview single pass`);

fs.writeFileSync(file, content);
