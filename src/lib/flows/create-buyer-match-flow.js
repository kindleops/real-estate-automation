import {
  BUYER_MATCH_FIELDS,
  createBuyerMatchItem,
  updateBuyerMatchItem,
} from "@/lib/podio/apps/buyer-match.js";
import { CLOSING_FIELDS, updateClosingItem } from "@/lib/podio/apps/closings.js";
import { CONTRACT_FIELDS, updateContractItem } from "@/lib/podio/apps/contracts.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import {
  buildBuyerMatchDiagnostics,
  resolveExistingBuyerMatch,
} from "@/lib/domain/buyers/match-engine.js";
import { getCategoryValue, getDateValue, getFirstAppReferenceId, getTextValue } from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function addDaysIso(days = 0) {
  const next = new Date();
  next.setUTCDate(next.getUTCDate() + Number(days || 0));
  return next.toISOString();
}

function asAppRef(value) {
  return value ? [value] : undefined;
}

function buildBuyerMatchId({
  contract_item_id = null,
  closing_item_id = null,
  property_item_id = null,
} = {}) {
  const stamp = Date.now();

  if (contract_item_id) return `BM-CT-${contract_item_id}-${stamp}`;
  if (closing_item_id) return `BM-CL-${closing_item_id}-${stamp}`;
  if (property_item_id) return `BM-P-${property_item_id}-${stamp}`;

  return `BM-${stamp}`;
}

function buildBuyerMatchTitle(diagnostics = {}) {
  const disposition_strategy = clean(diagnostics?.context?.disposition_strategy);
  const property_address = clean(diagnostics?.context?.property_address);

  return [disposition_strategy || "Buyer Match", property_address || "Property"].join(" - ");
}

function appendNotes(...values) {
  return values
    .map((value) => clean(value))
    .filter(Boolean)
    .join("\n");
}

function summarizeTopCandidates(top_candidates = [], limit = 5) {
  return top_candidates
    .slice(0, limit)
    .map((candidate, index) => {
      const score = Number(candidate?.score || 0);
      const company_name = clean(candidate?.company_name) || `Buyer ${index + 1}`;
      const reasons = Array.isArray(candidate?.reasons) ? candidate.reasons.join(", ") : "";
      return `${index + 1}. ${company_name} (${score})${reasons ? ` - ${reasons}` : ""}`;
    })
    .join("\n");
}

function buildAiSummary(diagnostics = {}) {
  const top_candidates = diagnostics?.diagnostics?.top_candidates || [];
  const viable_candidate_count = Number(diagnostics?.diagnostics?.viable_candidate_count || 0);

  return appendNotes(
    `Context: ${clean(diagnostics?.context?.property_address) || "Unknown property"} | ${clean(diagnostics?.context?.disposition_strategy) || "Unknown strategy"} | Target: ${clean(diagnostics?.context?.primary_target_type) || "Unknown"}`,
    `Viable Candidates: ${viable_candidate_count}`,
    top_candidates.length ? summarizeTopCandidates(top_candidates, 5) : "No ranked buyers from current intelligence."
  );
}

function choosePreservedCategory(existing_item, field, fallback = "") {
  return clean(getCategoryValue(existing_item, field, "")) || fallback || undefined;
}

function shouldPreserveBuyerSelections(existing_item = null) {
  const selected_buyer_id = getFirstAppReferenceId(
    existing_item,
    BUYER_MATCH_FIELDS.selected_buyer,
    null
  );
  const match_status = clean(
    getCategoryValue(existing_item, BUYER_MATCH_FIELDS.match_status, "")
  ).toLowerCase();

  return Boolean(
    selected_buyer_id ||
      ["buyers chosen", "assigned", "closed"].includes(match_status)
  );
}

function buildBuyerMatchPayload({
  existing_item = null,
  diagnostics = {},
  generated_buyer_match_id = null,
} = {}) {
  const context = diagnostics?.context || {};
  const top_candidates = diagnostics?.diagnostics?.top_candidates || [];
  const preserve_selections = shouldPreserveBuyerSelections(existing_item);
  const existing_match_status = clean(
    getCategoryValue(existing_item, BUYER_MATCH_FIELDS.match_status, "")
  );

  const top_ids = top_candidates.map((candidate) => Number(candidate?.item_id || 0)).filter(Boolean);
  const primary_buyer_id = top_ids[0] || null;
  const backup_buyer_1_id = top_ids[1] || null;
  const backup_buyer_2_id = top_ids[2] || null;
  const selected_buyer_id = getFirstAppReferenceId(
    existing_item,
    BUYER_MATCH_FIELDS.selected_buyer,
    null
  );

  const match_status =
    existing_match_status &&
    ["Sent to Buyers", "Buyers Interested", "Buyers Chosen", "Assigned", "Closed", "Dead"].includes(
      existing_match_status
    )
      ? existing_match_status
      : top_candidates.length
        ? "Buyers Selected"
        : "Matching";

  const payload = {
    [BUYER_MATCH_FIELDS.title]: buildBuyerMatchTitle(diagnostics),
    [BUYER_MATCH_FIELDS.buyer_match_id]: generated_buyer_match_id,
    [BUYER_MATCH_FIELDS.match_status]: match_status,
    [BUYER_MATCH_FIELDS.disposition_strategy]:
      clean(context.disposition_strategy) || "Assignment",
    [BUYER_MATCH_FIELDS.buyer_type_match]:
      clean(context.primary_target_type) || "Unknown",
    [BUYER_MATCH_FIELDS.buyer_match_score]:
      Number(top_candidates[0]?.score || 0),
    [BUYER_MATCH_FIELDS.reason_for_match]:
      clean(top_candidates[0]?.reasons?.join(", ")) ||
      "No strong buyer match has been selected yet.",
    [BUYER_MATCH_FIELDS.buyer_match_start_date]:
      getDateValue(existing_item, BUYER_MATCH_FIELDS.buyer_match_start_date, null) ||
      { start: nowIso() },
    [BUYER_MATCH_FIELDS.next_buyer_follow_up]: {
      start:
        top_candidates.length && context.live_blast_supported
          ? nowIso()
          : addDaysIso(1),
    },
    [BUYER_MATCH_FIELDS.urgency_level]:
      clean(context.urgency_level) || "Medium",
    [BUYER_MATCH_FIELDS.automation_status]:
      top_candidates.length ? "Running" : "Waiting",
    [BUYER_MATCH_FIELDS.buyer_response_status]:
      choosePreservedCategory(existing_item, BUYER_MATCH_FIELDS.buyer_response_status, "Not Sent"),
    [BUYER_MATCH_FIELDS.assignment_status]:
      choosePreservedCategory(
        existing_item,
        BUYER_MATCH_FIELDS.assignment_status,
        top_candidates.length ? "In Progress" : "Not Started"
      ),
    [BUYER_MATCH_FIELDS.ai_buyer_match_summary]: buildAiSummary(diagnostics),
    [BUYER_MATCH_FIELDS.internal_notes]: appendNotes(
      getTextValue(existing_item, BUYER_MATCH_FIELDS.internal_notes, ""),
      `[${nowIso()}] Buyer match ${existing_item?.item_id ? "updated" : "created"} with ${top_candidates.length} ranked candidates.${context.live_blast_supported ? "" : " Live blast remains limited for this disposition strategy."}`
    ),
    ...(context.property_item_id
      ? { [BUYER_MATCH_FIELDS.property]: asAppRef(context.property_item_id) }
      : {}),
    ...(context.offer_item_id
      ? { [BUYER_MATCH_FIELDS.offer]: asAppRef(context.offer_item_id) }
      : {}),
    ...(context.contract_item_id
      ? { [BUYER_MATCH_FIELDS.contract]: asAppRef(context.contract_item_id) }
      : {}),
    ...(context.master_owner_item_id
      ? { [BUYER_MATCH_FIELDS.master_owner]: asAppRef(context.master_owner_item_id) }
      : {}),
    ...(context.closing_item_id
      ? { [BUYER_MATCH_FIELDS.closing]: asAppRef(context.closing_item_id) }
      : {}),
    ...(context.market_item_id
      ? { [BUYER_MATCH_FIELDS.market]: asAppRef(context.market_item_id) }
      : {}),
    ...(context.property_profile_item_id
      ? { [BUYER_MATCH_FIELDS.property_profile]: asAppRef(context.property_profile_item_id) }
      : {}),
    ...(context.purchase_price !== null && context.purchase_price !== undefined
      ? { [BUYER_MATCH_FIELDS.final_acquisition_price]: context.purchase_price }
      : {}),
  };

  if (!preserve_selections) {
    if (primary_buyer_id) {
      payload[BUYER_MATCH_FIELDS.primary_buyer] = asAppRef(primary_buyer_id);
    }
    if (backup_buyer_1_id) {
      payload[BUYER_MATCH_FIELDS.backup_buyer_1] = asAppRef(backup_buyer_1_id);
    }
    if (backup_buyer_2_id) {
      payload[BUYER_MATCH_FIELDS.backup_buyer_2] = asAppRef(backup_buyer_2_id);
    }
  }

  if (selected_buyer_id) {
    payload[BUYER_MATCH_FIELDS.selected_buyer] = asAppRef(selected_buyer_id);
  }

  return payload;
}

async function syncLinkedRecords({
  contract_item_id = null,
  closing_item_id = null,
  buyer_match_item_id = null,
} = {}) {
  const updates = [];

  if (contract_item_id && buyer_match_item_id) {
    updates.push(
      updateContractItem(contract_item_id, {
        [CONTRACT_FIELDS.buyer_match]: asAppRef(buyer_match_item_id),
      })
    );
  }

  if (closing_item_id && buyer_match_item_id) {
    updates.push(
      updateClosingItem(closing_item_id, {
        [CLOSING_FIELDS.buyer_match]: asAppRef(buyer_match_item_id),
      })
    );
  }

  await Promise.all(updates);
}

export async function createBuyerMatchFlow({
  property_id = null,
  contract_id = null,
  closing_id = null,
  dry_run = false,
  candidate_limit = 10,
} = {}) {
  const diagnostics = await buildBuyerMatchDiagnostics({
    property_id,
    contract_id,
    closing_id,
    candidate_limit,
  });

  if (!diagnostics?.ok) {
    return {
      ok: false,
      created: false,
      updated: false,
      reason: diagnostics?.reason || "buyer_match_diagnostics_failed",
      dry_run,
      diagnostics,
    };
  }

  const context = diagnostics.context || {};
  const existing_item = await resolveExistingBuyerMatch({
    property_id: context.property_item_id || property_id,
    contract_id: context.contract_item_id || contract_id,
    closing_id: context.closing_item_id || closing_id,
  });
  const buyer_match_id =
    clean(getTextValue(existing_item, BUYER_MATCH_FIELDS.buyer_match_id, "")) ||
    buildBuyerMatchId({
      property_item_id: context.property_item_id || property_id,
      contract_item_id: context.contract_item_id || contract_id,
      closing_item_id: context.closing_item_id || closing_id,
    });
  const payload = buildBuyerMatchPayload({
    existing_item,
    diagnostics,
    generated_buyer_match_id: buyer_match_id,
  });

  if (dry_run) {
    return {
      ok: true,
      created: false,
      updated: false,
      dry_run: true,
      reason: "buyer_match_diagnostics_ready",
      buyer_match_item_id: existing_item?.item_id || null,
      buyer_match_id,
      disposition_strategy: context.disposition_strategy,
      live_blast_supported: Boolean(context.live_blast_supported),
      diagnostics,
      payload_preview: payload,
    };
  }

  let buyer_match_item = null;
  if (existing_item?.item_id) {
    await updateBuyerMatchItem(existing_item.item_id, payload);
    buyer_match_item = existing_item;
  } else {
    buyer_match_item = await createBuyerMatchItem(payload);
  }

  const buyer_match_item_id =
    buyer_match_item?.item_id ||
    existing_item?.item_id ||
    null;

  await syncLinkedRecords({
    contract_item_id: context.contract_item_id,
    closing_item_id: context.closing_item_id,
    buyer_match_item_id,
  });

  const pipeline = await syncPipelineState({
    property_id: context.property_item_id,
    master_owner_id: context.master_owner_item_id,
    contract_item_id: context.contract_item_id,
    closing_item_id: context.closing_item_id,
    buyer_match_item_id,
    market_id: context.market_item_id,
    notes: "Buyer match diagnostics synced into disposition pipeline.",
  });

  if (pipeline?.pipeline_item_id && buyer_match_item_id) {
    await updateBuyerMatchItem(buyer_match_item_id, {
      [BUYER_MATCH_FIELDS.pipeline]: asAppRef(pipeline.pipeline_item_id),
    });
  }

  return {
    ok: true,
    created: !existing_item?.item_id,
    updated: Boolean(existing_item?.item_id),
    dry_run: false,
    reason: existing_item?.item_id
      ? "buyer_match_updated"
      : "buyer_match_created",
    buyer_match_item_id,
    buyer_match_id,
    disposition_strategy: context.disposition_strategy,
    live_blast_supported: Boolean(context.live_blast_supported),
    diagnostics,
    pipeline,
    payload,
  };
}

export default createBuyerMatchFlow;
