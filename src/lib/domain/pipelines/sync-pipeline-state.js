import {
  createPipelineItem,
  findPipelineByPipelineId,
  findPipelineItems,
  getPipelineItem,
  PIPELINE_FIELDS,
  updatePipelineItem,
} from "@/lib/podio/apps/pipelines.js";
import {
  BUYER_MATCH_FIELDS,
  findBuyerMatchItems,
  getBuyerMatchItem,
} from "@/lib/podio/apps/buyer-match.js";
import {
  CLOSING_FIELDS,
  findClosingItems,
  getClosingItem,
} from "@/lib/podio/apps/closings.js";
import {
  CONTRACT_FIELDS,
  findContractItems,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import { getDealRevenueItem } from "@/lib/podio/apps/deal-revenue.js";
import { getMasterOwnerItem, MASTER_OWNER_FIELDS } from "@/lib/podio/apps/master-owners.js";
import { OFFER_FIELDS, findOfferItems, getOfferItem, normalizeOfferStatus } from "@/lib/podio/apps/offers.js";
import { getPropertyItem } from "@/lib/podio/apps/properties.js";
import { getProspectItem } from "@/lib/podio/apps/prospects.js";
import {
  findTitleRoutingItems,
  getTitleRoutingItem,
  TITLE_ROUTING_FIELDS,
} from "@/lib/podio/apps/title-routing.js";
import {
  getAppReferenceIds,
  getCategoryValue,
  getDateValue,
  getFirstAppReferenceId,
  getTextValue,
  getNumberValue,
} from "@/lib/providers/podio.js";

const STAGE_ORDER = Object.freeze({
  "New Lead": 1,
  Contacted: 2,
  Negotiating: 3,
  "Offer Sent": 4,
  "Offer Accepted": 5,
  "Contract Sent": 6,
  "Fully Executed": 7,
  "Buyer Match": 8,
  "Routed to Title": 9,
  "Title Reviewing": 10,
  "Clear to Close": 11,
  "Closing Scheduled": 12,
  Closed: 13,
  Dead: 14,
});

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function addDaysIso(days = 0) {
  const next = new Date();
  next.setUTCDate(next.getUTCDate() + Number(days || 0));
  return next.toISOString();
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function toTimestamp(value) {
  if (!value) return null;
  const timestamp = new Date(value).getTime();
  return Number.isNaN(timestamp) ? null : timestamp;
}

function toAppRef(value) {
  return value ? [value] : undefined;
}

function appendNote(existing_notes, next_note) {
  const prior = clean(existing_notes);
  const next = clean(next_note);
  if (!next) return prior || undefined;
  if (!prior) return next;
  return `${prior}\n${next}`;
}

function chooseLatestByDate(values = []) {
  let winner = null;
  let winner_ts = null;

  for (const value of values) {
    const ts = toTimestamp(value);
    if (ts === null) continue;
    if (winner_ts === null || ts > winner_ts) {
      winner = value;
      winner_ts = ts;
    }
  }

  return winner;
}

function chooseMostAdvancedStage(stages = []) {
  return stages
    .filter(Boolean)
    .sort((left, right) => (STAGE_ORDER[right] || 0) - (STAGE_ORDER[left] || 0))[0] || null;
}

function normalizePipelineStatus(value = "Active") {
  const raw = lower(value);

  if (raw.includes("won") || raw === "closed won") return "Closed Won";
  if (raw.includes("lost") || raw === "closed lost") return "Closed Lost";
  if (raw.includes("stall")) return "Stalled";
  if (raw.includes("archive")) return "Archived";
  return "Active";
}

function normalizeAutomationStatus(value = "Running") {
  const raw = lower(value);

  if (raw.includes("pause")) return "Paused";
  if (raw.includes("escal")) return "Escalated";
  if (raw.includes("complete")) return "Complete";
  if (raw.includes("wait")) return "Waiting";
  return "Running";
}

function normalizeEngine(value = "Acquisitions") {
  const raw = lower(value);

  if (raw.includes("underwriting")) return "Underwriting";
  if (raw.includes("offer")) return "Offers";
  if (raw.includes("contract")) return "Contracts";
  if (raw.includes("title")) return "Title Routing";
  if (raw.includes("closing")) return "Closings";
  if (raw.includes("buyer")) return "Buyer Match";
  if (raw.includes("revenue")) return "Deal Revenue";
  return "Acquisitions";
}

function buildPipelineId({
  property_id = null,
  master_owner_id = null,
  prospect_id = null,
  conversation_id = null,
} = {}) {
  const anchor =
    property_id ||
    master_owner_id ||
    prospect_id ||
    conversation_id ||
    Date.now();

  return `PIPE-${anchor}`;
}

async function findExistingPipeline({
  pipeline_item_id = null,
  pipeline_id = null,
  property_id = null,
  master_owner_id = null,
  prospect_id = null,
  conversation_id = null,
  offer_item_id = null,
  contract_item_id = null,
  title_routing_item_id = null,
  closing_item_id = null,
  buyer_match_item_id = null,
  deal_revenue_item_id = null,
} = {}) {
  if (pipeline_item_id) {
    const direct = await getPipelineItem(pipeline_item_id);
    if (direct?.item_id) return direct;
  }

  if (pipeline_id) {
    const direct = await findPipelineByPipelineId(pipeline_id);
    if (direct?.item_id) return direct;
  }

  const candidates = [
    [PIPELINE_FIELDS.closing, closing_item_id],
    [PIPELINE_FIELDS.title_routing, title_routing_item_id],
    [PIPELINE_FIELDS.contract, contract_item_id],
    [PIPELINE_FIELDS.offer, offer_item_id],
    [PIPELINE_FIELDS.deal_revenue, deal_revenue_item_id],
    [PIPELINE_FIELDS.buyer_match, buyer_match_item_id],
    [PIPELINE_FIELDS.property, property_id],
    [PIPELINE_FIELDS.prospect, prospect_id],
    [PIPELINE_FIELDS.master_owner, master_owner_id],
    [PIPELINE_FIELDS.conversation, conversation_id],
  ];

  for (const [field, value] of candidates) {
    if (!value) continue;
    const matches = await findPipelineItems({ [field]: value }, 10, 0);
    const latest = sortNewestFirst(matches)[0] || null;
    if (latest?.item_id) return latest;
  }

  return null;
}

async function resolveLatest(items = [], getter = null) {
  const latest = sortNewestFirst(items)[0] || null;
  if (latest?.fields || !getter || !latest?.item_id) return latest;
  return getter(latest.item_id);
}

async function loadRelatedRecords(anchors = {}) {
  const {
    property_id = null,
    master_owner_id = null,
    prospect_id = null,
    conversation_id = null,
    offer_item_id = null,
    contract_item_id = null,
    title_routing_item_id = null,
    closing_item_id = null,
    buyer_match_item_id = null,
    deal_revenue_item_id = null,
  } = anchors;

  const [
    property_item,
    master_owner_item,
    prospect_item,
    offer_item_direct,
    contract_item_direct,
    title_routing_item_direct,
    closing_item_direct,
    buyer_match_item_direct,
    deal_revenue_item,
  ] = await Promise.all([
    property_id ? getPropertyItem(property_id) : Promise.resolve(null),
    master_owner_id ? getMasterOwnerItem(master_owner_id) : Promise.resolve(null),
    prospect_id ? getProspectItem(prospect_id) : Promise.resolve(null),
    offer_item_id ? getOfferItem(offer_item_id) : Promise.resolve(null),
    contract_item_id ? getContractItem(contract_item_id) : Promise.resolve(null),
    title_routing_item_id ? getTitleRoutingItem(title_routing_item_id) : Promise.resolve(null),
    closing_item_id ? getClosingItem(closing_item_id) : Promise.resolve(null),
    buyer_match_item_id ? getBuyerMatchItem(buyer_match_item_id) : Promise.resolve(null),
    deal_revenue_item_id ? getDealRevenueItem(deal_revenue_item_id) : Promise.resolve(null),
  ]);

  const [offer_matches, contract_matches, title_matches, closing_matches, buyer_match_matches] =
    await Promise.all([
    offer_item_direct
      ? Promise.resolve([offer_item_direct])
      : property_id
        ? findOfferItems({ [OFFER_FIELDS.property]: property_id }, 50, 0)
        : prospect_id
          ? findOfferItems({ [OFFER_FIELDS.prospect]: prospect_id }, 50, 0)
          : master_owner_id
            ? findOfferItems({ [OFFER_FIELDS.master_owner]: master_owner_id }, 50, 0)
            : Promise.resolve([]),
    contract_item_direct
      ? Promise.resolve([contract_item_direct])
      : property_id
        ? findContractItems({ [CONTRACT_FIELDS.property]: property_id }, 50, 0)
        : prospect_id
          ? findContractItems({ [CONTRACT_FIELDS.prospect]: prospect_id }, 50, 0)
          : master_owner_id
            ? findContractItems({ [CONTRACT_FIELDS.master_owner]: master_owner_id }, 50, 0)
            : Promise.resolve([]),
    title_routing_item_direct
      ? Promise.resolve([title_routing_item_direct])
      : property_id
        ? findTitleRoutingItems({ [TITLE_ROUTING_FIELDS.property]: property_id }, 50, 0)
        : prospect_id
          ? findTitleRoutingItems({ [TITLE_ROUTING_FIELDS.prospect]: prospect_id }, 50, 0)
          : master_owner_id
            ? findTitleRoutingItems({ [TITLE_ROUTING_FIELDS.master_owner]: master_owner_id }, 50, 0)
            : Promise.resolve([]),
    closing_item_direct
      ? Promise.resolve([closing_item_direct])
      : property_id
        ? findClosingItems({ [CLOSING_FIELDS.property]: property_id }, 50, 0)
        : prospect_id
          ? findClosingItems({ [CLOSING_FIELDS.prospect]: prospect_id }, 50, 0)
          : master_owner_id
            ? findClosingItems({ [CLOSING_FIELDS.master_owner]: master_owner_id }, 50, 0)
            : Promise.resolve([]),
    buyer_match_item_direct
      ? Promise.resolve([buyer_match_item_direct])
      : closing_item_id
        ? findBuyerMatchItems({ [BUYER_MATCH_FIELDS.closing]: closing_item_id }, 50, 0)
        : contract_item_id
          ? findBuyerMatchItems({ [BUYER_MATCH_FIELDS.contract]: contract_item_id }, 50, 0)
          : property_id
            ? findBuyerMatchItems({ [BUYER_MATCH_FIELDS.property]: property_id }, 50, 0)
            : Promise.resolve([]),
  ]);

  const offer_item = await resolveLatest(offer_matches, getOfferItem);
  const contract_item =
    contract_item_direct ||
    (offer_item?.item_id
      ? await resolveLatest(
          await findContractItems({ [CONTRACT_FIELDS.offer]: offer_item.item_id }, 50, 0),
          getContractItem
        )
      : await resolveLatest(contract_matches, getContractItem));
  const title_routing_item =
    title_routing_item_direct ||
    (contract_item?.item_id
      ? await resolveLatest(
          await findTitleRoutingItems({ [TITLE_ROUTING_FIELDS.contract]: contract_item.item_id }, 50, 0),
          getTitleRoutingItem
        )
      : await resolveLatest(title_matches, getTitleRoutingItem));
  const closing_item =
    closing_item_direct ||
    (title_routing_item?.item_id
      ? await resolveLatest(
          await findClosingItems({ [CLOSING_FIELDS.title_routing]: title_routing_item.item_id }, 50, 0),
          getClosingItem
        )
      : contract_item?.item_id
        ? await resolveLatest(
            await findClosingItems({ [CLOSING_FIELDS.contract]: contract_item.item_id }, 50, 0),
            getClosingItem
          )
        : await resolveLatest(closing_matches, getClosingItem));
  const buyer_match_item =
    buyer_match_item_direct ||
    (closing_item?.item_id
      ? await resolveLatest(
          await findBuyerMatchItems({ [BUYER_MATCH_FIELDS.closing]: closing_item.item_id }, 50, 0),
          getBuyerMatchItem
        )
      : contract_item?.item_id
        ? await resolveLatest(
            await findBuyerMatchItems({ [BUYER_MATCH_FIELDS.contract]: contract_item.item_id }, 50, 0),
            getBuyerMatchItem
          )
        : property_item?.item_id
          ? await resolveLatest(
              await findBuyerMatchItems({ [BUYER_MATCH_FIELDS.property]: property_item.item_id }, 50, 0),
              getBuyerMatchItem
            )
          : await resolveLatest(buyer_match_matches, getBuyerMatchItem));

  const resolved_offer_item = offer_item || null;
  const resolved_contract_item = contract_item || null;
  const resolved_title_routing_item = title_routing_item || null;
  const resolved_closing_item = closing_item || null;

  return {
    property_item,
    master_owner_item,
    prospect_item,
    conversation_item_id: conversation_id || null,
    offer_item: resolved_offer_item,
    contract_item: resolved_contract_item,
    title_routing_item: resolved_title_routing_item,
    closing_item: resolved_closing_item,
    buyer_match_item: buyer_match_item || null,
    deal_revenue_item: deal_revenue_item || null,
  };
}

function deriveStageState(records = {}) {
  const offer_status = normalizeOfferStatus(
    getCategoryValue(records.offer_item, OFFER_FIELDS.offer_status, "")
  );
  const buyer_match_status = clean(
    getCategoryValue(records.buyer_match_item, BUYER_MATCH_FIELDS.match_status, "")
  );
  const buyer_response_status = clean(
    getCategoryValue(records.buyer_match_item, BUYER_MATCH_FIELDS.buyer_response_status, "")
  );
  const assignment_status = clean(
    getCategoryValue(records.buyer_match_item, BUYER_MATCH_FIELDS.assignment_status, "")
  );
  const dispo_outcome = clean(
    getCategoryValue(records.buyer_match_item, BUYER_MATCH_FIELDS.dispo_outcome, "")
  );
  const contract_status = clean(
    getCategoryValue(records.contract_item, CONTRACT_FIELDS.contract_status, "")
  );
  const title_status = clean(
    getCategoryValue(records.title_routing_item, TITLE_ROUTING_FIELDS.routing_status, "")
  );
  const closing_status = clean(
    getCategoryValue(records.closing_item, CLOSING_FIELDS.closing_status, "")
  );

  const title_status_lower = lower(title_status);
  const closing_status_lower = lower(closing_status);
  const contract_status_lower = lower(contract_status);
  const offer_status_lower = lower(offer_status);
  const buyer_match_status_lower = lower(buyer_match_status);
  const buyer_response_status_lower = lower(buyer_response_status);
  const assignment_status_lower = lower(assignment_status);
  const dispo_outcome_lower = lower(dispo_outcome);
  const buyer_match_score = getNumberValue(
    records.buyer_match_item,
    BUYER_MATCH_FIELDS.buyer_match_score,
    null
  );

  if (closing_status_lower === "completed") {
    return {
      current_stage: "Closed",
      pipeline_status: "Closed Won",
      automation_status: "Complete",
      current_engine: "Deal Revenue",
      blocked: "No",
      escalation_needed: "No",
      won_lost_reason: "Closed",
      next_system_action: "Confirm revenue posting and archive the pipeline.",
    };
  }

  if (title_status_lower === "closed" || contract_status_lower === "closed") {
    return {
      current_stage: "Closed",
      pipeline_status: "Closed Won",
      automation_status: "Complete",
      current_engine: "Deal Revenue",
      blocked: "No",
      escalation_needed: "No",
      won_lost_reason: "Closed",
      next_system_action: "Confirm revenue posting and archive the pipeline.",
    };
  }

  if (
    closing_status_lower === "cancelled" ||
    title_status_lower === "cancelled" ||
    contract_status_lower === "cancelled" ||
    offer_status_lower === "rejected" ||
    offer_status_lower === "expired"
  ) {
    return {
      current_stage: "Dead",
      pipeline_status: "Closed Lost",
      automation_status: "Complete",
      current_engine: "Acquisitions",
      blocked: "Yes",
      escalation_needed: "No",
      won_lost_reason:
        closing_status_lower === "cancelled"
          ? "Buyer Backed Out"
          : title_status_lower === "cancelled"
            ? "Title"
            : contract_status_lower === "cancelled"
              ? "Seller Backed Out"
              : offer_status_lower === "expired"
                ? "No Response"
                : "Price",
      blocker_type:
        dispo_outcome_lower === "deal cancelled"
          ? "Buyer Fallout"
          : dispo_outcome_lower === "seller backed out"
            ? "Seller Delay"
            : dispo_outcome_lower === "no buyer found"
              ? "No Buyer Found"
              : title_status_lower === "cancelled"
                ? "Title Issue"
                : offer_status_lower === "expired"
                  ? "No Response"
                  : offer_status_lower === "rejected"
                    ? "Pricing Gap"
                    : "Other",
      next_system_action: "Archive the deal or document the postmortem.",
    };
  }

  if (
    ["scheduled", "confirmed", "rescheduled"].includes(closing_status_lower)
  ) {
    return {
      current_stage: "Closing Scheduled",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Closings",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Drive the closing checklist to the finish line.",
    };
  }

  if (
    title_status_lower === "clear to close" ||
    closing_status_lower === "confirmed"
  ) {
    return {
      current_stage: "Clear to Close",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Closings",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Schedule the closing and confirm funds/docs.",
    };
  }

  if (
    buyer_match_status_lower === "dead" ||
    dispo_outcome_lower === "no buyer found"
  ) {
    return {
      current_stage: "Buyer Match",
      pipeline_status: "Stalled",
      automation_status: "Waiting",
      current_engine: "Buyer Match",
      blocked: "Yes",
      blocker_type: "No Buyer Found",
      escalation_needed: "Yes",
      next_system_action: "Refresh the buyer search or pivot the disposition strategy.",
    };
  }

  if (
    title_status_lower === "waiting on buyer" ||
    [
      "matching",
      "buyers selected",
      "sent to buyers",
      "buyers interested",
      "buyers chosen",
    ].includes(buyer_match_status_lower)
  ) {
    const blocked = title_status_lower === "waiting on buyer" ? "Yes" : "No";
    const waiting_on_responses =
      buyer_match_status_lower === "sent to buyers" ||
      ["sent", "opened", "needs more info"].includes(buyer_response_status_lower);

    return {
      current_stage: "Buyer Match",
      pipeline_status: blocked === "Yes" ? "Stalled" : "Active",
      automation_status: waiting_on_responses ? "Waiting" : "Running",
      current_engine: "Buyer Match",
      blocked,
      blocker_type: blocked === "Yes" ? "Buyer Delay" : undefined,
      escalation_needed:
        blocked === "Yes" || (buyer_match_score !== null && buyer_match_score < 45)
          ? "Yes"
          : "No",
      next_system_action:
        buyer_match_status_lower === "buyers interested"
          ? "Qualify interest, collect proof of funds, and choose the best buyer."
          : buyer_match_status_lower === "buyers chosen"
            ? "Finalize buyer selection and coordinate assignment or double-close paperwork."
            : buyer_match_status_lower === "sent to buyers"
              ? "Wait on buyer responses and follow up with the shortlist."
              : "Rank buyers, tighten the shortlist, and prepare the dispo package.",
    };
  }

  if (
    buyer_match_status_lower === "assigned" ||
    assignment_status_lower === "assigned" ||
    assignment_status_lower === "buyer confirmed"
  ) {
    return {
      current_stage: "Buyer Match",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Buyer Match",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Coordinate assignment docs, title handoff, and buyer funds readiness.",
    };
  }

  if (
    buyer_match_status_lower === "closed" &&
    !title_status_lower &&
    !closing_status_lower
  ) {
    return {
      current_stage: "Buyer Match",
      pipeline_status: "Active",
      automation_status: "Complete",
      current_engine: "Buyer Match",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Confirm the dispo file is archived and synced downstream.",
    };
  }

  if (
    [
      "opened",
      "title reviewing",
      "waiting on docs",
      "waiting on payoff",
      "waiting on probate",
      "waiting on seller",
      "waiting on buyer",
    ].includes(title_status_lower)
  ) {
    const blocker_type =
      title_status_lower === "waiting on docs"
        ? "Missing Docs"
        : title_status_lower === "waiting on probate"
          ? "Probate"
          : title_status_lower === "waiting on payoff"
            ? "Funding"
            : title_status_lower === "waiting on seller"
              ? "Seller Delay"
              : title_status_lower === "waiting on buyer"
                ? "Buyer Delay"
                : title_status_lower === "title reviewing"
                  ? "Title Issue"
                  : null;

    return {
      current_stage: "Title Reviewing",
      pipeline_status: blocker_type ? "Stalled" : "Active",
      automation_status: blocker_type ? "Waiting" : "Running",
      current_engine: "Title Routing",
      blocked: blocker_type ? "Yes" : "No",
      blocker_type: blocker_type || undefined,
      escalation_needed:
        blocker_type && ["Probate", "Title Issue", "Funding"].includes(blocker_type)
          ? "Yes"
          : "No",
      next_system_action:
        blocker_type
          ? `Resolve title blocker: ${blocker_type}.`
          : "Monitor title milestones and clear conditions.",
    };
  }

  if (contract_status_lower === "clear to close") {
    return {
      current_stage: "Clear to Close",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Closings",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Schedule the closing and confirm funds/docs.",
    };
  }

  if (contract_status_lower === "opened") {
    return {
      current_stage: "Title Reviewing",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Title Routing",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Monitor title milestones and clear conditions.",
    };
  }

  if (contract_status_lower === "sent to title") {
    return {
      current_stage: "Routed to Title",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Title Routing",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Confirm title file opened and track title milestones.",
    };
  }

  if (title_status_lower === "routed") {
    return {
      current_stage: "Routed to Title",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Title Routing",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Confirm title file opened and track title milestones.",
    };
  }

  if (contract_status_lower === "fully executed") {
    return {
      current_stage: "Fully Executed",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Title Routing",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Assign title company and route to title immediately.",
    };
  }

  if (["sent", "viewed", "seller signed", "buyer signed"].includes(contract_status_lower)) {
    return {
      current_stage: "Contract Sent",
      pipeline_status: "Active",
      automation_status: "Waiting",
      current_engine: "Contracts",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Drive signatures and convert the contract to fully executed.",
    };
  }

  if (offer_status_lower === "accepted (ready for contract)") {
    return {
      current_stage: "Offer Accepted",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Contracts",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Generate and send the contract package.",
    };
  }

  if (["counter received", "negotiating"].includes(offer_status_lower)) {
    return {
      current_stage: "Negotiating",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Offers",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Counter, re-underwrite, and push the deal to acceptance.",
    };
  }

  if (["offer sent", "revised offer sent", "viewed"].includes(offer_status_lower)) {
    return {
      current_stage: "Offer Sent",
      pipeline_status: "Active",
      automation_status: "Waiting",
      current_engine: "Offers",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Follow up on the outstanding offer.",
    };
  }

  const last_contacted =
    getDateValue(records.master_owner_item, MASTER_OWNER_FIELDS.last_contacted_at, null) ||
    getDateValue(records.master_owner_item, MASTER_OWNER_FIELDS.last_outbound, null) ||
    getDateValue(records.master_owner_item, MASTER_OWNER_FIELDS.last_inbound, null) ||
    null;

  if (last_contacted || records.conversation_item_id) {
    return {
      current_stage: "Contacted",
      pipeline_status: "Active",
      automation_status: "Running",
      current_engine: "Acquisitions",
      blocked: "No",
      escalation_needed: "No",
      next_system_action: "Qualify motivation and move the seller into negotiation.",
    };
  }

  return {
    current_stage: "New Lead",
    pipeline_status: "Active",
    automation_status: "Running",
    current_engine: "Acquisitions",
    blocked: "No",
    escalation_needed: "No",
    next_system_action: "Start outreach and establish contact.",
  };
}

function buildSummary({
  records = {},
  current_stage = null,
  pipeline_status = null,
  automation_status = null,
}) {
  const property_address =
    clean(getTextValue(records.property_item, "property-address", "")) ||
    clean(getTextValue(records.property_item, "full-name", ""));
  const owner_name =
    clean(getTextValue(records.master_owner_item, MASTER_OWNER_FIELDS.owner_full_name, "")) ||
    clean(records.master_owner_item?.title);

  return [
    current_stage ? `Stage: ${current_stage}` : "",
    pipeline_status ? `Pipeline: ${pipeline_status}` : "",
    automation_status ? `Automation: ${automation_status}` : "",
    owner_name ? `Owner: ${owner_name}` : "",
    property_address ? `Property: ${property_address}` : "",
    records.offer_item?.item_id ? `Offer: ${records.offer_item.item_id}` : "",
    records.contract_item?.item_id ? `Contract: ${records.contract_item.item_id}` : "",
    records.title_routing_item?.item_id
      ? `Title Routing: ${records.title_routing_item.item_id}`
      : "",
    records.closing_item?.item_id ? `Closing: ${records.closing_item.item_id}` : "",
    records.buyer_match_item?.item_id ? `Buyer Match: ${records.buyer_match_item.item_id}` : "",
    clean(getCategoryValue(records.buyer_match_item, BUYER_MATCH_FIELDS.match_status, ""))
      ? `Dispo: ${clean(getCategoryValue(records.buyer_match_item, BUYER_MATCH_FIELDS.match_status, ""))}`
      : "",
    records.deal_revenue_item?.item_id
      ? `Revenue: ${records.deal_revenue_item.item_id}`
      : "",
  ]
    .filter(Boolean)
    .join(" | ");
}

function buildPipelineTitle(records = {}) {
  const property_address =
    clean(getTextValue(records.property_item, "property-address", "")) ||
    clean(getTextValue(records.property_item, "full-name", ""));
  const owner_name =
    clean(getTextValue(records.master_owner_item, MASTER_OWNER_FIELDS.owner_full_name, "")) ||
    clean(records.master_owner_item?.title);

  return [owner_name, property_address, "Pipeline"].filter(Boolean).join(" - ") || "Pipeline";
}

function deriveExpectedCloseDate(records = {}) {
  return chooseLatestByDate([
    getDateValue(records.contract_item, CONTRACT_FIELDS.closing_date_target, null),
    getDateValue(records.title_routing_item, TITLE_ROUTING_FIELDS.expected_closing_date, null),
  ]);
}

function deriveActualCloseDate(records = {}) {
  return chooseLatestByDate([
    getDateValue(records.closing_item, CLOSING_FIELDS.actual_closing_date, null),
  ]);
}

function deriveLastStageChange({
  existing_pipeline = null,
  current_stage = null,
}) {
  const existing_stage = clean(
    getCategoryValue(existing_pipeline, PIPELINE_FIELDS.current_stage, "")
  );
  const existing_last_stage_change = getDateValue(
    existing_pipeline,
    PIPELINE_FIELDS.last_stage_change,
    null
  );

  if (existing_stage && existing_stage === current_stage && existing_last_stage_change) {
    return existing_last_stage_change;
  }

  return nowIso();
}

function deriveDaysInStage(last_stage_change) {
  const last_stage_change_ts = toTimestamp(last_stage_change);
  if (last_stage_change_ts === null) return undefined;

  const diff_ms = Date.now() - last_stage_change_ts;
  return Math.max(0, Math.floor(diff_ms / 86_400_000));
}

function deriveNextActionDate({
  current_stage = null,
  pipeline_status = null,
  automation_status = null,
  blocked = null,
} = {}) {
  const normalized_stage = clean(current_stage);
  const normalized_pipeline_status = clean(pipeline_status);
  const normalized_automation_status = clean(automation_status);
  const normalized_blocked = clean(blocked);

  if (
    ["Closed Won", "Closed Lost", "Archived"].includes(normalized_pipeline_status) ||
    ["Closed", "Dead"].includes(normalized_stage)
  ) {
    return null;
  }

  if (normalized_blocked === "Yes") {
    return addDaysIso(1);
  }

  if (
    [
      "New Lead",
      "Contacted",
      "Negotiating",
      "Offer Accepted",
      "Buyer Match",
      "Fully Executed",
      "Clear to Close",
      "Closing Scheduled",
    ].includes(normalized_stage)
  ) {
    return nowIso();
  }

  if (
    ["Offer Sent", "Contract Sent", "Routed to Title", "Title Reviewing"].includes(
      normalized_stage
    ) ||
    normalized_automation_status === "Waiting"
  ) {
    return addDaysIso(1);
  }

  return addDaysIso(1);
}

function deriveBlockedSummary(stage_state = {}, records = {}) {
  if (stage_state.blocked !== "Yes") return "";

  const pieces = [
    clean(stage_state.blocker_type),
    clean(stage_state.next_system_action),
    clean(
      getTextValue(records.title_routing_item, TITLE_ROUTING_FIELDS.preliminary_title_issues, "")
    ),
    clean(
      getTextValue(records.title_routing_item, TITLE_ROUTING_FIELDS.seller_docs_needed, "")
    ),
    clean(getTextValue(records.closing_item, CLOSING_FIELDS.outstanding_items, "")),
    clean(getTextValue(records.buyer_match_item, BUYER_MATCH_FIELDS.internal_notes, "")),
  ].filter(Boolean);

  return pieces.join(" | ");
}

function mergeIdentifiers(existing_pipeline = null, identifiers = {}) {
  const from_pipeline = (field) =>
    getFirstAppReferenceId(existing_pipeline, field, null) || null;

  return {
    property_id:
      identifiers.property_id || from_pipeline(PIPELINE_FIELDS.property) || null,
    master_owner_id:
      identifiers.master_owner_id || from_pipeline(PIPELINE_FIELDS.master_owner) || null,
    prospect_id:
      identifiers.prospect_id || from_pipeline(PIPELINE_FIELDS.prospect) || null,
    conversation_id:
      identifiers.conversation_item_id || from_pipeline(PIPELINE_FIELDS.conversation) || null,
    offer_item_id:
      identifiers.offer_item_id || from_pipeline(PIPELINE_FIELDS.offer) || null,
    contract_item_id:
      identifiers.contract_item_id || from_pipeline(PIPELINE_FIELDS.contract) || null,
    title_routing_item_id:
      identifiers.title_routing_item_id || from_pipeline(PIPELINE_FIELDS.title_routing) || null,
    closing_item_id:
      identifiers.closing_item_id || from_pipeline(PIPELINE_FIELDS.closing) || null,
    buyer_match_item_id:
      identifiers.buyer_match_item_id || from_pipeline(PIPELINE_FIELDS.buyer_match) || null,
    deal_revenue_item_id:
      identifiers.deal_revenue_item_id || from_pipeline(PIPELINE_FIELDS.deal_revenue) || null,
    assigned_agent_id:
      identifiers.assigned_agent_id || from_pipeline(PIPELINE_FIELDS.assigned_agent) || null,
    market_id:
      identifiers.market_id || from_pipeline(PIPELINE_FIELDS.market) || null,
  };
}

export function buildPipelinePayload({
  existing_pipeline = null,
  records = {},
  identifiers = {},
  notes = "",
  forced_stage = null,
  forced_pipeline_status = null,
  forced_automation_status = null,
  forced_current_engine = null,
  forced_next_system_action = null,
  forced_blocked = null,
  forced_blocker_type = null,
  forced_blocker_summary = null,
  forced_escalation_needed = null,
  forced_won_lost_reason = null,
  forced_outcome_notes = null,
  forced_ai_next_move_summary = null,
} = {}) {
  const stage_state = deriveStageState(records);
  const current_stage = forced_stage || stage_state.current_stage;
  const existing_stage = clean(
    getCategoryValue(existing_pipeline, PIPELINE_FIELDS.current_stage, "")
  );
  const stage_changed = Boolean(
    current_stage &&
      existing_stage &&
      current_stage !== existing_stage
  );
  const pipeline_status = normalizePipelineStatus(
    forced_pipeline_status || stage_state.pipeline_status
  );
  const automation_status = normalizeAutomationStatus(
    forced_automation_status || stage_state.automation_status
  );
  const current_engine = normalizeEngine(
    forced_current_engine || stage_state.current_engine
  );
  const next_system_action =
    clean(forced_next_system_action) || clean(stage_state.next_system_action) || undefined;
  const blocked =
    clean(forced_blocked) || clean(stage_state.blocked) || "No";
  const blocker_type =
    clean(forced_blocker_type) || clean(stage_state.blocker_type) || undefined;
  const blocker_summary =
    clean(forced_blocker_summary) ||
    deriveBlockedSummary(stage_state, records) ||
    undefined;
  const escalation_needed =
    clean(forced_escalation_needed) ||
    clean(stage_state.escalation_needed) ||
    "No";
  const won_lost_reason =
    clean(forced_won_lost_reason) ||
    clean(stage_state.won_lost_reason) ||
    undefined;
  const expected_close_date = deriveExpectedCloseDate(records);
  const actual_close_date = deriveActualCloseDate(records);
  const next_action_date = deriveNextActionDate({
    current_stage,
    pipeline_status,
    automation_status,
    blocked,
  });
  const last_stage_change = deriveLastStageChange({
    existing_pipeline,
    current_stage,
  });
  const deal_created_date =
    getDateValue(existing_pipeline, PIPELINE_FIELDS.deal_created_date, null) ||
    nowIso();
  const number_of_days_in_current_stage = deriveDaysInStage(last_stage_change);
  const ai_next_move_summary =
    clean(forced_ai_next_move_summary) ||
    next_system_action ||
    undefined;

  const payload = {
    [PIPELINE_FIELDS.title]: buildPipelineTitle(records),
    [PIPELINE_FIELDS.pipeline_status]: pipeline_status,
    [PIPELINE_FIELDS.current_stage]: current_stage,
    [PIPELINE_FIELDS.automation_status]: automation_status,
    [PIPELINE_FIELDS.current_engine]: current_engine,
    [PIPELINE_FIELDS.next_system_action]: next_system_action,
    ...(next_action_date
      ? { [PIPELINE_FIELDS.next_action_date]: { start: next_action_date } }
      : {}),
    [PIPELINE_FIELDS.last_automation_update]: { start: nowIso() },
    [PIPELINE_FIELDS.deal_created_date]: { start: deal_created_date },
    [PIPELINE_FIELDS.last_stage_change]: { start: last_stage_change },
    [PIPELINE_FIELDS.number_of_days_in_current_stage]:
      number_of_days_in_current_stage ?? undefined,
    [PIPELINE_FIELDS.blocked]: blocked,
    [PIPELINE_FIELDS.escalation_needed]: escalation_needed,
    [PIPELINE_FIELDS.blocker_type]: blocker_type,
    [PIPELINE_FIELDS.blocker_summary]: blocker_summary,
    [PIPELINE_FIELDS.won_lost_reason]: won_lost_reason,
    [PIPELINE_FIELDS.pipeline_summary]: buildSummary({
      records,
      current_stage,
      pipeline_status,
      automation_status,
    }),
    [PIPELINE_FIELDS.ai_next_move_summary]: ai_next_move_summary,
    ...(expected_close_date
      ? { [PIPELINE_FIELDS.expected_close_date]: { start: expected_close_date } }
      : {}),
    ...(actual_close_date
      ? { [PIPELINE_FIELDS.actual_close_date]: { start: actual_close_date } }
      : {}),
  };

  if (forced_outcome_notes || won_lost_reason) {
    payload[PIPELINE_FIELDS.outcome_notes] = appendNote(
      getTextValue(existing_pipeline, PIPELINE_FIELDS.outcome_notes, ""),
      clean(forced_outcome_notes) ||
        (won_lost_reason ? `[${nowIso()}] Outcome: ${won_lost_reason}.` : "")
    );
  }

  if (notes || stage_changed || !existing_pipeline?.item_id) {
    payload[PIPELINE_FIELDS.internal_notes] = appendNote(
      getTextValue(existing_pipeline, PIPELINE_FIELDS.internal_notes, ""),
      clean(notes) || `[${nowIso()}] Pipeline synced to stage ${current_stage}.`
    );
  }

  if (identifiers.property_id) {
    payload[PIPELINE_FIELDS.property] = toAppRef(identifiers.property_id);
  }
  if (identifiers.master_owner_id) {
    payload[PIPELINE_FIELDS.master_owner] = toAppRef(identifiers.master_owner_id);
  }
  if (identifiers.prospect_id) {
    payload[PIPELINE_FIELDS.prospect] = toAppRef(identifiers.prospect_id);
  }
  if (identifiers.conversation_id) {
    payload[PIPELINE_FIELDS.conversation] = toAppRef(identifiers.conversation_id);
  }
  if (identifiers.offer_item_id) {
    payload[PIPELINE_FIELDS.offer] = toAppRef(identifiers.offer_item_id);
  }
  if (identifiers.contract_item_id) {
    payload[PIPELINE_FIELDS.contract] = toAppRef(identifiers.contract_item_id);
  }
  if (identifiers.title_routing_item_id) {
    payload[PIPELINE_FIELDS.title_routing] = toAppRef(identifiers.title_routing_item_id);
  }
  if (identifiers.closing_item_id) {
    payload[PIPELINE_FIELDS.closing] = toAppRef(identifiers.closing_item_id);
  }
  if (identifiers.buyer_match_item_id) {
    payload[PIPELINE_FIELDS.buyer_match] = toAppRef(identifiers.buyer_match_item_id);
  }
  if (identifiers.deal_revenue_item_id) {
    payload[PIPELINE_FIELDS.deal_revenue] = toAppRef(identifiers.deal_revenue_item_id);
  }
  if (identifiers.assigned_agent_id) {
    payload[PIPELINE_FIELDS.assigned_agent] = toAppRef(identifiers.assigned_agent_id);
  }
  if (identifiers.market_id) {
    payload[PIPELINE_FIELDS.market] = toAppRef(identifiers.market_id);
  }

  return {
    current_stage,
    pipeline_status,
    automation_status,
    current_engine,
    payload,
  };
}

export async function syncPipelineState({
  pipeline_item_id = null,
  pipeline_id = null,
  create_if_missing = true,
  property_id = null,
  master_owner_id = null,
  prospect_id = null,
  conversation_item_id = null,
  offer_item_id = null,
  contract_item_id = null,
  title_routing_item_id = null,
  closing_item_id = null,
  buyer_match_item_id = null,
  deal_revenue_item_id = null,
  assigned_agent_id = null,
  market_id = null,
  stage = null,
  pipeline_status = null,
  automation_status = null,
  current_engine = null,
  next_system_action = null,
  blocked = null,
  blocker_type = null,
  blocker_summary = null,
  escalation_needed = null,
  won_lost_reason = null,
  outcome_notes = null,
  ai_next_move_summary = null,
  notes = "",
} = {}) {
  let existing_pipeline = await findExistingPipeline({
    pipeline_item_id,
    pipeline_id,
    property_id,
    master_owner_id,
    prospect_id,
    conversation_id: conversation_item_id,
    offer_item_id,
    contract_item_id,
    title_routing_item_id,
    closing_item_id,
    buyer_match_item_id,
    deal_revenue_item_id,
  });

  if (!existing_pipeline?.item_id && !create_if_missing) {
    return {
      ok: true,
      created: false,
      updated: false,
      reason: "pipeline_not_created",
      pipeline_item_id: null,
      pipeline_id: clean(pipeline_id) || null,
      current_stage: clean(stage) || null,
      pipeline_status: clean(pipeline_status) || null,
      automation_status: clean(automation_status) || null,
      current_engine: clean(current_engine) || null,
      payload: null,
    };
  }

  const identifiers = mergeIdentifiers(existing_pipeline, {
    property_id,
    master_owner_id,
    prospect_id,
    conversation_item_id,
    offer_item_id,
    contract_item_id,
    title_routing_item_id,
    closing_item_id,
    buyer_match_item_id,
    deal_revenue_item_id,
    assigned_agent_id,
    market_id,
  });

  const records = await loadRelatedRecords(identifiers);

  if (!identifiers.property_id) {
    identifiers.property_id =
      records.property_item?.item_id ||
      getFirstAppReferenceId(records.offer_item, OFFER_FIELDS.property, null) ||
      getFirstAppReferenceId(records.contract_item, CONTRACT_FIELDS.property, null) ||
      getFirstAppReferenceId(records.title_routing_item, TITLE_ROUTING_FIELDS.property, null) ||
      getFirstAppReferenceId(records.closing_item, CLOSING_FIELDS.property, null) ||
      null;
  }

  if (!identifiers.master_owner_id) {
    identifiers.master_owner_id =
      records.master_owner_item?.item_id ||
      getFirstAppReferenceId(records.offer_item, OFFER_FIELDS.master_owner, null) ||
      getFirstAppReferenceId(records.contract_item, CONTRACT_FIELDS.master_owner, null) ||
      getFirstAppReferenceId(records.title_routing_item, TITLE_ROUTING_FIELDS.master_owner, null) ||
      getFirstAppReferenceId(records.closing_item, CLOSING_FIELDS.master_owner, null) ||
      null;
  }

  if (!identifiers.prospect_id) {
    identifiers.prospect_id =
      records.prospect_item?.item_id ||
      getFirstAppReferenceId(records.offer_item, OFFER_FIELDS.prospect, null) ||
      getFirstAppReferenceId(records.contract_item, CONTRACT_FIELDS.prospect, null) ||
      getFirstAppReferenceId(records.title_routing_item, TITLE_ROUTING_FIELDS.prospect, null) ||
      getFirstAppReferenceId(records.closing_item, CLOSING_FIELDS.prospect, null) ||
      null;
  }

  if (!identifiers.offer_item_id) {
    identifiers.offer_item_id = records.offer_item?.item_id || null;
  }
  if (!identifiers.contract_item_id) {
    identifiers.contract_item_id = records.contract_item?.item_id || null;
  }
  if (!identifiers.title_routing_item_id) {
    identifiers.title_routing_item_id = records.title_routing_item?.item_id || null;
  }
  if (!identifiers.closing_item_id) {
    identifiers.closing_item_id = records.closing_item?.item_id || null;
  }
  if (!identifiers.buyer_match_item_id) {
    identifiers.buyer_match_item_id =
      records.buyer_match_item?.item_id ||
      getFirstAppReferenceId(records.contract_item, CONTRACT_FIELDS.buyer_match, null) ||
      getFirstAppReferenceId(records.closing_item, CLOSING_FIELDS.buyer_match, null) ||
      null;
  }
  if (!identifiers.deal_revenue_item_id) {
    identifiers.deal_revenue_item_id = records.deal_revenue_item?.item_id || null;
  }
  if (!identifiers.assigned_agent_id) {
    identifiers.assigned_agent_id =
      getFirstAppReferenceId(records.contract_item, CONTRACT_FIELDS.assigned_agent, null) ||
      getFirstAppReferenceId(records.title_routing_item, TITLE_ROUTING_FIELDS.assigned_agent, null) ||
      null;
  }
  if (!identifiers.market_id) {
    identifiers.market_id =
      getFirstAppReferenceId(records.contract_item, CONTRACT_FIELDS.market, null) ||
      getFirstAppReferenceId(records.title_routing_item, TITLE_ROUTING_FIELDS.market, null) ||
      getFirstAppReferenceId(records.buyer_match_item, BUYER_MATCH_FIELDS.market, null) ||
      getFirstAppReferenceId(records.closing_item, CLOSING_FIELDS.market, null) ||
      null;
  }

  const resolved_pipeline_id =
    clean(pipeline_id) ||
    clean(getTextValue(existing_pipeline, PIPELINE_FIELDS.pipeline_id, "")) ||
    buildPipelineId({
      property_id: identifiers.property_id,
      master_owner_id: identifiers.master_owner_id,
      prospect_id: identifiers.prospect_id,
      conversation_id: identifiers.conversation_id,
    });

  const payload_result = buildPipelinePayload({
    existing_pipeline,
    records,
    identifiers,
    notes,
    forced_stage: stage,
    forced_pipeline_status: pipeline_status,
    forced_automation_status: automation_status,
    forced_current_engine: current_engine,
    forced_next_system_action: next_system_action,
    forced_blocked: blocked,
    forced_blocker_type: blocker_type,
    forced_blocker_summary: blocker_summary,
    forced_escalation_needed: escalation_needed,
    forced_won_lost_reason: won_lost_reason,
    forced_outcome_notes: outcome_notes,
    forced_ai_next_move_summary: ai_next_move_summary,
  });

  const payload = {
    [PIPELINE_FIELDS.pipeline_id]: resolved_pipeline_id,
    ...payload_result.payload,
  };

  if (existing_pipeline?.item_id) {
    await updatePipelineItem(existing_pipeline.item_id, payload);

    return {
      ok: true,
      created: false,
      updated: true,
      reason: "pipeline_synced",
      pipeline_item_id: existing_pipeline.item_id,
      pipeline_id: resolved_pipeline_id,
      current_stage: payload_result.current_stage,
      pipeline_status: payload_result.pipeline_status,
      automation_status: payload_result.automation_status,
      current_engine: payload_result.current_engine,
      payload,
    };
  }

  const created = await createPipelineItem(payload);

  return {
    ok: true,
    created: true,
    updated: false,
    reason: "pipeline_created",
    pipeline_item_id: created?.item_id || null,
    pipeline_id: resolved_pipeline_id,
    current_stage: payload_result.current_stage,
    pipeline_status: payload_result.pipeline_status,
    automation_status: payload_result.automation_status,
    current_engine: payload_result.current_engine,
    payload,
    raw: created,
  };
}

export default syncPipelineState;
