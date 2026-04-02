import APP_IDS from "@/lib/config/app-ids.js";
import {
  createItem,
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.pipelines;

export const PIPELINE_FIELDS = {
  title: "title",
  pipeline_id: "pipeline-id",
  pipeline_status: "pipeline-status",
  current_stage: "current-stage",
  property: "property",
  master_owner: "master-owner",
  prospect: "prospect",
  conversation: "conversation",
  offer: "offer",
  contract: "contract",
  title_routing: "title-routing",
  closing: "closing",
  buyer_match: "buyer-match",
  deal_revenue: "deal-revenue",
  assigned_agent: "assigned-agent",
  market: "market",
  automation_status: "automation-status",
  current_engine: "current-engine",
  next_system_action: "next-system-action",
  next_action_date: "next-action-date",
  last_automation_update: "last-automation-update",
  deal_created_date: "deal-created-date",
  last_stage_change: "last-stage-change",
  expected_close_date: "expected-close-date",
  actual_close_date: "actual-close-date",
  number_of_days_in_current_stage: "number-of-days-in-current-stage",
  blocked: "blocked",
  blocker_type: "blocker-type",
  blocker_summary: "blocker-summary",
  escalation_needed: "escalation-needed",
  won_lost_reason: "won-lost-reason",
  outcome_notes: "outcome-notes",
  pipeline_summary: "pipeline-summary",
  internal_notes: "internal-notes",
  ai_next_move_summary: "ai-next-move-summary",
};

export const createPipelineItem = (fields = {}) =>
  createItem(APP_ID, fields);

export const getPipelineItem = (item_id) =>
  getItem(item_id);

export const updatePipelineItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findPipelineItems = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const findPipelineByPipelineId = (pipeline_id) =>
  findByField(APP_ID, PIPELINE_FIELDS.pipeline_id, pipeline_id);

export default {
  APP_ID,
  PIPELINE_FIELDS,
  createPipelineItem,
  getPipelineItem,
  updatePipelineItem,
  findPipelineItems,
  findPipelineByPipelineId,
};
