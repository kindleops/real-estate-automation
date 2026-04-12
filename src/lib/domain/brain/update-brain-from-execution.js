import { CONTRACT_FIELDS, getContractItem } from "@/lib/podio/apps/contracts.js";
import { TITLE_ROUTING_FIELDS } from "@/lib/podio/apps/title-routing.js";
import { CLOSING_FIELDS } from "@/lib/podio/apps/closings.js";
import {
  BRAIN_FIELDS,
  getBrainItem,
} from "@/lib/podio/apps/ai-conversation-brain.js";
import {
  getCategoryValue,
  getCategoryValues,
  getDateValue,
  getFirstAppReferenceId,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";
import { resolveBrain } from "@/lib/domain/context/resolve-brain.js";
import {
  applyBrainStateUpdate,
  buildDeterministicBrainStateFields,
} from "@/lib/domain/brain/brain-authority.js";
import {
  EXECUTION_BRAIN_MILESTONES,
  buildExecutionConversationState,
} from "@/lib/domain/communications-engine/state-machine.js";
import { toPodioDateField } from "@/lib/utils/dates.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function toId(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

const defaultDeps = {
  getContractItem,
  getBrainItem,
  resolveBrain,
  applyBrainStateUpdate,
};

let runtimeDeps = { ...defaultDeps };

export function __setExecutionBrainTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetExecutionBrainTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

function inferExecutionMilestoneFromContract({
  milestone = null,
  normalized_status = null,
  contract_status = null,
} = {}) {
  const explicit = clean(milestone);
  if (explicit) return explicit;

  const normalized = lower(normalized_status || contract_status);

  if (!normalized) return null;
  if (normalized.includes("cancel") || normalized.includes("declined") || normalized.includes("void")) {
    return EXECUTION_BRAIN_MILESTONES.CONTRACT_CANCELLED;
  }
  if (normalized.includes("seller signed") || normalized.includes("buyer signed")) {
    return EXECUTION_BRAIN_MILESTONES.CONTRACT_SIGNED;
  }
  if (normalized.includes("completed") || normalized.includes("fully executed")) {
    return EXECUTION_BRAIN_MILESTONES.CONTRACT_FULLY_EXECUTED;
  }
  if (normalized.includes("deliver") || normalized.includes("view")) {
    return EXECUTION_BRAIN_MILESTONES.CONTRACT_VIEWED;
  }
  if (normalized.includes("sent")) {
    return EXECUTION_BRAIN_MILESTONES.CONTRACT_SENT;
  }
  if (normalized.includes("created") || normalized.includes("draft")) {
    return EXECUTION_BRAIN_MILESTONES.CONTRACT_CREATED;
  }

  return null;
}

function inferExecutionMilestoneFromTitle({
  milestone = null,
  routing_status = null,
  normalized_event = null,
} = {}) {
  const explicit = clean(milestone);
  if (explicit) return explicit;

  const normalized = lower(normalized_event || routing_status);

  if (!normalized) return null;
  if (normalized.includes("cancel")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_CANCELLED;
  }
  if (normalized.includes("closed") || normalized.includes("funded") || normalized.includes("recorded")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_CLOSED;
  }
  if (normalized.includes("clear")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_CLEAR_TO_CLOSE;
  }
  if (normalized.includes("opened")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_OPENED;
  }
  if (normalized.includes("review")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_REVIEWING;
  }
  if (normalized.includes("probate")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PROBATE;
  }
  if (normalized.includes("payoff")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_PAYOFF;
  }
  if (normalized.includes("seller")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_SELLER;
  }
  if (normalized.includes("buyer")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_BUYER;
  }
  if (normalized.includes("doc")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_WAITING_ON_DOCS;
  }
  if (normalized.includes("routed")) {
    return EXECUTION_BRAIN_MILESTONES.TITLE_ROUTED;
  }

  return null;
}

function inferExecutionMilestoneFromClosing({
  milestone = null,
  closing_status = null,
  normalized_event = null,
} = {}) {
  const explicit = clean(milestone);
  if (explicit) return explicit;

  const normalized = lower(normalized_event || closing_status);

  if (!normalized) return null;
  if (normalized.includes("cancel")) {
    return EXECUTION_BRAIN_MILESTONES.CLOSING_CANCELLED;
  }
  if (
    normalized.includes("completed") ||
    normalized.includes("closed") ||
    normalized.includes("funded") ||
    normalized.includes("recorded")
  ) {
    return EXECUTION_BRAIN_MILESTONES.CLOSING_COMPLETED;
  }
  if (normalized.includes("pending docs")) {
    return EXECUTION_BRAIN_MILESTONES.CLOSING_PENDING_DOCS;
  }
  if (normalized.includes("clear to close") || normalized.includes("confirm")) {
    return EXECUTION_BRAIN_MILESTONES.CLOSING_CONFIRMED;
  }
  if (normalized.includes("schedule")) {
    return EXECUTION_BRAIN_MILESTONES.CLOSING_SCHEDULED;
  }

  return null;
}

export function inferExecutionMilestone({
  source = null,
  milestone = null,
  normalized_status = null,
  contract_status = null,
  routing_status = null,
  closing_status = null,
  normalized_event = null,
} = {}) {
  const normalized_source = lower(source);

  if (normalized_source === "contract") {
    return inferExecutionMilestoneFromContract({
      milestone,
      normalized_status,
      contract_status,
    });
  }

  if (normalized_source === "title") {
    return inferExecutionMilestoneFromTitle({
      milestone,
      routing_status,
      normalized_event,
    });
  }

  if (normalized_source === "closing") {
    return inferExecutionMilestoneFromClosing({
      milestone,
      closing_status,
      normalized_event,
    });
  }

  if (normalized_source === "revenue") {
    return clean(milestone) || EXECUTION_BRAIN_MILESTONES.REVENUE_CONFIRMED;
  }

  return clean(milestone) || null;
}

function extractCurrentBrainState(brain_item = null) {
  if (!brain_item) return {};

  return {
    conversation_stage: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.conversation_stage,
      null
    ),
    lifecycle_stage_number: getNumberValue(
      brain_item,
      BRAIN_FIELDS.lifecycle_stage_number,
      null
    ),
    current_conversation_branch: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.ai_route,
      null
    ),
    current_seller_state: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.current_seller_state,
      null
    ),
    follow_up_step: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.follow_up_step,
      null
    ),
    next_follow_up_due_at: getDateValue(
      brain_item,
      BRAIN_FIELDS.next_follow_up_due_at,
      null
    ),
    last_detected_intent: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.last_detected_intent,
      null
    ),
    status_ai_managed: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.status_ai_managed,
      null
    ),
    deal_priority_tag: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.deal_priority_tag,
      null
    ),
    risk_flags_ai: getCategoryValues(brain_item, BRAIN_FIELDS.risk_flags_ai),
    follow_up_trigger_state: getCategoryValue(
      brain_item,
      BRAIN_FIELDS.follow_up_trigger_state,
      null
    ),
    full_conversation_summary_ai: getTextValue(
      brain_item,
      BRAIN_FIELDS.full_conversation_summary_ai,
      ""
    ),
  };
}

async function resolveExecutionContractItem({
  contract_item = null,
  title_routing_item = null,
  closing_item = null,
} = {}) {
  if (contract_item?.item_id) return contract_item;

  const contract_item_id =
    getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.contract, null) ||
    getFirstAppReferenceId(closing_item, CLOSING_FIELDS.contract, null) ||
    null;

  if (!contract_item_id) return null;

  return runtimeDeps.getContractItem(contract_item_id);
}

async function resolveExecutionBrainRecord({
  contract_item = null,
  title_routing_item = null,
  closing_item = null,
} = {}) {
  const resolved_contract_item = await resolveExecutionContractItem({
    contract_item,
    title_routing_item,
    closing_item,
  });

  const direct_brain_id = toId(
    getFirstAppReferenceId(resolved_contract_item, CONTRACT_FIELDS.conversation, null)
  );

  let brain_item = null;

  if (direct_brain_id) {
    try {
      brain_item = await runtimeDeps.getBrainItem(direct_brain_id);
    } catch {
      brain_item = null;
    }
  }

  if (!brain_item) {
    brain_item = await runtimeDeps.resolveBrain({
      phone_item_id: getFirstAppReferenceId(
        resolved_contract_item,
        CONTRACT_FIELDS.phone,
        null
      ),
      prospect_id:
        getFirstAppReferenceId(resolved_contract_item, CONTRACT_FIELDS.prospect, null) ||
        getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.prospect, null) ||
        getFirstAppReferenceId(closing_item, CLOSING_FIELDS.prospect, null),
      master_owner_id:
        getFirstAppReferenceId(resolved_contract_item, CONTRACT_FIELDS.master_owner, null) ||
        getFirstAppReferenceId(
          title_routing_item,
          TITLE_ROUTING_FIELDS.master_owner,
          null
        ) ||
        getFirstAppReferenceId(closing_item, CLOSING_FIELDS.master_owner, null),
    });
  }

  return {
    contract_item: resolved_contract_item,
    brain_id: toId(brain_item?.item_id) || direct_brain_id || null,
    brain_item: brain_item || null,
  };
}

export async function updateBrainFromExecution({
  source = null,
  milestone = null,
  contract_item = null,
  title_routing_item = null,
  closing_item = null,
  normalized_status = null,
  contract_status = null,
  routing_status = null,
  closing_status = null,
  normalized_event = null,
  notes = "",
  now = new Date(),
} = {}) {
  const resolved_milestone = inferExecutionMilestone({
    source,
    milestone,
    normalized_status,
    contract_status,
    routing_status,
    closing_status,
    normalized_event,
  });

  if (!resolved_milestone) {
    return {
      ok: false,
      updated: false,
      reason: "no_execution_milestone",
      source: clean(source) || null,
    };
  }

  const resolved = await resolveExecutionBrainRecord({
    contract_item,
    title_routing_item,
    closing_item,
  });

  if (!resolved.brain_id) {
    return {
      ok: false,
      updated: false,
      reason: "brain_not_found_for_execution",
      milestone: resolved_milestone,
      source: clean(source) || null,
    };
  }

  const execution_state = buildExecutionConversationState({
    milestone: resolved_milestone,
    current_state: extractCurrentBrainState(resolved.brain_item),
    note:
      clean(notes) ||
      clean(normalized_event) ||
      clean(routing_status) ||
      clean(closing_status) ||
      clean(normalized_status) ||
      clean(contract_status),
  });

  if (execution_state?.blocked_reason) {
    return {
      ok: true,
      updated: false,
      reason: execution_state.blocked_reason,
      brain_id: resolved.brain_id,
      milestone: resolved_milestone,
      source: clean(source) || null,
    };
  }

  const fields = {
    ...buildDeterministicBrainStateFields({
      deterministic_state: execution_state,
    }),
    [BRAIN_FIELDS.follow_up_trigger_state]:
      execution_state.follow_up_trigger_state,
    [BRAIN_FIELDS.last_contact_timestamp]: toPodioDateField(now),
  };

  const result = await runtimeDeps.applyBrainStateUpdate({
    brain_id: resolved.brain_id,
    reason: `${clean(source) || "execution"}:${resolved_milestone}`,
    fields,
  });

  return {
    ...result,
    updated: Boolean(result?.ok),
    brain_id: resolved.brain_id,
    milestone: resolved_milestone,
    source: clean(source) || null,
    contract_item_id: resolved.contract_item?.item_id || null,
  };
}

export default updateBrainFromExecution;
