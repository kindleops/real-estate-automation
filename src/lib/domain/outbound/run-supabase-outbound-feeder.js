import { loadSupabaseOutboundCandidates } from "./load-supabase-outbound-candidates.js";
import { 
  evaluateCandidateEligibility, 
  chooseTextgridNumber,
  renderOutboundTemplate,
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

  const options = {
    dry_run,
    limit,
    scan_limit,
    candidate_offset,
    candidate_source: input.candidate_source || null,
    market: input.market || null,
    state: input.state || null,
    template_use_case: input.template_use_case || input.use_case || "ownership_check",
    touch_number: Number(input.touch_number) || 1,
    campaign_session_id: input.campaign_session_id || `session-${now.slice(0, 10)}`,
    now,
  };

  const summary = {
    ok: true,
    dry_run,
    scanned_count: 0,
    eligible_count: 0,
    queued_count: 0,
    skipped_count: 0,
    skip_reasons: {},
    selected_template_source_counts: {},
    selected_templates: {},
    errors: []
  };

  const recordSkip = (reason) => {
    summary.skipped_count += 1;
    summary.skip_reasons[reason] = (summary.skip_reasons[reason] || 0) + 1;
  };

  try {
    const source = await loadSupabaseOutboundCandidates(options, deps);
    summary.scanned_count = source.scanned_count;
    summary.source = source.source;

    for (const candidate of source.rows) {
      if (summary.queued_count >= options.limit) {
        recordSkip("CAMPAIGN_LIMIT_REACHED");
        continue;
      }

      // Safety Guard 1: Eligibility check (contact window, suppression, duplicates)
      const eligibility = await evaluateCandidateEligibility(candidate, options, deps);
      if (!eligibility.ok) {
        recordSkip(eligibility.reason_code || "INELIGIBLE");
        continue;
      }
      summary.eligible_count += 1;

      // Safety Guard 2: Routing check
      const routing = await chooseTextgridNumber(candidate, options, deps);
      if (!routing.ok) {
        recordSkip(routing.reason_code || "ROUTING_BLOCKED");
        continue;
      }

      // Safety Guard 3: Template rendering and guards
      // We rely on the Supabase template engine to enforce hard unit count guards.
      const rendered = await renderOutboundTemplate(candidate, options, deps);
      if (!rendered.ok) {
        recordSkip(rendered.reason_code || "TEMPLATE_ERROR");
        continue;
      }

      const template_id = rendered.selected_template?.id || rendered.selected_template?.template_id;
      if (template_id) {
        summary.selected_templates[template_id] = (summary.selected_templates[template_id] || 0) + 1;
      }

      // Write to Send Queue
      if (!dry_run) {
        const queueResult = await insertSupabaseSendQueueRow({
          ...rendered.queue_payload,
          scheduled_for: eligibility.scheduled_for || now,
          status: "queued"
        }, deps);

        if (!queueResult.ok) {
          recordSkip("QUEUE_INSERT_FAILED");
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
