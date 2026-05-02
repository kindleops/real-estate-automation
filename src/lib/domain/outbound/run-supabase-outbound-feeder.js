import { loadSupabaseOutboundCandidates } from "./load-supabase-outbound-candidates.js";
import { 
  evaluateCandidateEligibility, 
  chooseTextgridNumber,
  renderOutboundTemplate,
  resolveNextOutboundTouch,
  REASON_CODES 
} from "./supabase-candidate-feeder.js";
import { insertSupabaseSendQueueRow } from "../../supabase/sms-engine.js";

/**
 * runSupabaseOutboundFeeder
 * Feeds Supabase-native candidates into the send queue.
 */
export async function runSupabaseOutboundFeeder(input = {}, deps = {}) {
  const now = input.now || new Date().toISOString();

  const limit = Math.max(1, Math.min(Number(input.limit) || 25, 500));
  const scan_limit = Math.max(limit, Math.min(Number(input.scan_limit ?? input.candidate_fetch_limit) || 500, 5000));
  const candidate_offset = Math.max(0, Math.trunc(Number(input.candidate_offset ?? input.scan_offset ?? input.offset) || 0));
  const dry_run = Boolean(input.dry_run);
  const debug = Boolean(input.debug || input.debug_mode);

  const options = {
    dry_run,
    debug,
    limit,
    scan_limit,
    candidate_offset,
    candidate_source: input.candidate_source || null,
    market: input.market || null,
    state: input.state || null,
    template_use_case: input.template_use_case || input.use_case || "ownership_check",
    touch_number: Number(input.touch_number) || 1,
    campaign_session_id: input.campaign_session_id || `session-${now.slice(0, 10)}`,
    within_contact_window_now: input.within_contact_window_now !== undefined ? Boolean(input.within_contact_window_now) : true,
    now,
  };

  const summary = {
    ok: true,
    dry_run,
    debug_enabled: debug,
    scanned_count: 0,
    eligible_count: 0,
    queued_count: 0,
    skipped_count: 0,
    skip_reasons: {},
    selected_template_source_counts: {},
    selected_templates: {},
    errors: []
  };

  if (debug) {
    summary.duplicate_policy = {
      match_basis: [
        "master_owner_id",
        "property_id",
        "touch_number",
        "to_phone_number",
        "template_use_case"
      ],
      blocking_statuses: ["queued", "sending", "sent"]
    };
    summary.first_10_duplicate_skips = [];
    summary.first_10_skip_details = [];
    summary.first_10_eligible_candidates = [];
    summary.first_10_would_queue = [];
    summary.first_10_rendered_message_previews = [];
    summary.first_10_routing_selection_details = [];
    summary.first_10_template_selection_details = [];
    summary.first_10_candidate_touch_context = [];
    summary.first_10_resolved_next_touches = [];
  }

  const recordSkip = (reason, details = {}) => {
    summary.skipped_count += 1;
    summary.skip_reasons[reason] = (summary.skip_reasons[reason] || 0) + 1;
    if (debug && summary.first_10_skip_details.length < 10) {
      summary.first_10_skip_details.push({ reason_code: reason, ...details });
    }
    if (debug && reason === "DUPLICATE_QUEUE_ITEM" && summary.first_10_duplicate_skips.length < 10) {
      summary.first_10_duplicate_skips.push(details.duplicate_check || {});
    }
  };

  try {
    const source = await loadSupabaseOutboundCandidates(options, deps);
    summary.scanned_count = source.scanned_count;
    summary.source = source.source;

    for (const candidate of source.rows) {
      if (summary.queued_count >= options.limit) {
        recordSkip("CAMPAIGN_LIMIT_REACHED", {
          master_owner_id: candidate.master_owner_id,
          property_id: candidate.property_id
        });
        continue;
      }

      // Step 0: Resolve Next Touch Progression
      const resolved = await resolveNextOutboundTouch(candidate, options, deps);
      
      if (debug && summary.first_10_candidate_touch_context.length < 10) {
        const hc = resolved.history_context || {};
        summary.first_10_candidate_touch_context.push({
          master_owner_id: candidate.master_owner_id,
          property_id: candidate.property_id,
          phone_id: candidate.phone_id,
          current_touch_number: candidate.touch_number,
          candidate_touch_number: candidate.touch_number,
          next_touch_number: candidate.raw?.next_touch_number,
          last_sent_touch_number: candidate.last_touch_number,
          last_sent_use_case: candidate.use_case_template,
          last_outbound_at: candidate.last_outbound_at,
          next_eligible_at: candidate.next_eligible_at,
          candidate_use_case: candidate.template_use_case,
          proposed_next_use_case: resolved.template_use_case,
          history_latest_sent_touch_number: hc.history_latest_sent_touch_number,
          history_latest_sent_use_case: hc.history_latest_sent_use_case,
          history_row_count: hc.history_row_count,
          has_touch_1_ownership_check: hc.has_touch_1_ownership_check,
          has_touch_2_consider_selling: hc.has_touch_2_consider_selling
        });
      }

      if (debug && summary.first_10_resolved_next_touches.length < 10) {
        summary.first_10_resolved_next_touches.push({
          master_owner_id: candidate.master_owner_id,
          property_id: candidate.property_id,
          ...resolved
        });
      }

      if (!resolved.ok) {
        recordSkip(resolved.reason_code || "PROGRESSION_BLOCKED", {
          master_owner_id: candidate.master_owner_id,
          property_id: candidate.property_id
        });
        continue;
      }

      // Update candidate with resolved touch for eligibility/rendering
      const activeCandidate = {
        ...candidate,
        touch_number: resolved.touch_number,
        template_use_case: resolved.template_use_case,
        template_lookup_use_case: resolved.template_use_case,
        stage_code: resolved.stage_code,
        sequence_position: resolved.sequence_position,
        is_first_touch: resolved.is_first_touch,
        is_follow_up: !resolved.is_first_touch
      };

      // Safety Guard 1: Eligibility check (contact window, suppression, duplicates)
      const eligibility = await evaluateCandidateEligibility(activeCandidate, options, deps);
      if (!eligibility.ok) {
        recordSkip(eligibility.reason_code || "INELIGIBLE", {
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id,
          duplicate_check: eligibility.duplicate_check,
          resolved_touch: {
            touch_number: activeCandidate.touch_number,
            template_use_case: activeCandidate.template_use_case
          }
        });
        continue;
      }
      summary.eligible_count += 1;
      if (debug && summary.first_10_eligible_candidates.length < 10) {
        summary.first_10_eligible_candidates.push({
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id,
          scheduled_for: eligibility.scheduled_for,
          touch_number: activeCandidate.touch_number,
          use_case: activeCandidate.template_use_case
        });
      }

      // Safety Guard 2: Routing check
      const routing = await chooseTextgridNumber(activeCandidate, options, deps);
      if (!routing.ok) {
        recordSkip(routing.reason_code || "ROUTING_BLOCKED", {
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id
        });
        continue;
      }
      if (debug && summary.first_10_routing_selection_details.length < 10) {
        summary.first_10_routing_selection_details.push({
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id,
          ...routing
        });
      }

      // Safety Guard 3: Template rendering and guards
      const rendered = await renderOutboundTemplate(activeCandidate, options, deps);
      if (!rendered.ok) {
        recordSkip(rendered.reason_code || "TEMPLATE_ERROR", {
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id,
          render_error: rendered.render_error_message
        });
        continue;
      }

      const template_id = rendered.selected_template?.id || rendered.selected_template?.template_id;
      const template_source = rendered.selected_template?.source || "supabase";
      if (template_id) {
        summary.selected_templates[template_id] = (summary.selected_templates[template_id] || 0) + 1;
        summary.selected_template_source_counts[template_source] = (summary.selected_template_source_counts[template_source] || 0) + 1;
      }

      if (debug && summary.first_10_template_selection_details.length < 10) {
        summary.first_10_template_selection_details.push({
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id,
          template_id,
          template_source,
          use_case: rendered.template_use_case,
          language: rendered.language,
          stage_code: rendered.stage_code
        });
      }

      if (debug && summary.first_10_rendered_message_previews.length < 10) {
        summary.first_10_rendered_message_previews.push({
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id,
          body: rendered.rendered_message_body
        });
      }

      const queue_payload = {
        ...rendered.queue_payload,
        scheduled_for: eligibility.scheduled_for || now,
        status: "queued"
      };

      if (debug && summary.first_10_would_queue.length < 10) {
        summary.first_10_would_queue.push({
          master_owner_id: activeCandidate.master_owner_id,
          property_id: activeCandidate.property_id,
          payload: queue_payload
        });
      }

      // Write to Send Queue
      if (!dry_run) {
        const queueResult = await insertSupabaseSendQueueRow(queue_payload, deps);

        if (!queueResult.ok) {
          recordSkip("QUEUE_INSERT_FAILED", {
            master_owner_id: candidate.master_owner_id,
            property_id: candidate.property_id,
            error: queueResult.error
          });
          continue;
        }
      }

      summary.queued_count += 1;
    }
  } catch (err) {
    summary.ok = false;
    summary.errors.push(err.message);
  }

  return summary;
}
