import {
  CONTRACT_TEMPLATE_FIELDS,
  findContractTemplates,
  getContractTemplateItem,
} from "@/lib/podio/apps/contract-templates.js";
import { CONTRACT_FIELDS } from "@/lib/podio/apps/contracts.js";
import {
  getCategoryValue,
  getNumberValue,
  getTextValue,
} from "@/lib/providers/podio.js";

function clean(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return clean(value).toLowerCase();
}

function normalizeBoolean(value) {
  const raw = lower(value);
  if (["yes", "true", "active"].includes(raw)) return "yes";
  if (["no", "false", "inactive"].includes(raw)) return "no";
  return raw;
}

function getTemplateMetadata(template_item = null) {
  return {
    item_id: template_item?.item_id || null,
    title: clean(getTextValue(template_item, CONTRACT_TEMPLATE_FIELDS.title, "")),
    state: clean(getCategoryValue(template_item, CONTRACT_TEMPLATE_FIELDS.state, "")),
    contract_type: clean(
      getCategoryValue(template_item, CONTRACT_TEMPLATE_FIELDS.contract_type, "")
    ),
    template_type: clean(
      getCategoryValue(template_item, CONTRACT_TEMPLATE_FIELDS.template_type, "")
    ),
    active: normalizeBoolean(
      getCategoryValue(template_item, CONTRACT_TEMPLATE_FIELDS.active, "")
    ),
    use_for_auto_generation: normalizeBoolean(
      getCategoryValue(
        template_item,
        CONTRACT_TEMPLATE_FIELDS.use_for_auto_generation,
        ""
      )
    ),
    template_status: clean(
      getCategoryValue(template_item, CONTRACT_TEMPLATE_FIELDS.template_status, "")
    ),
    default_for_state_type: normalizeBoolean(
      getCategoryValue(
        template_item,
        CONTRACT_TEMPLATE_FIELDS.default_for_state_type,
        ""
      )
    ),
    assignment_allowed: normalizeBoolean(
      getCategoryValue(template_item, CONTRACT_TEMPLATE_FIELDS.assignment_allowed, "")
    ),
    priority: getNumberValue(template_item, CONTRACT_TEMPLATE_FIELDS.priority, 0) ?? 0,
    docusign_template_id: clean(
      getTextValue(template_item, CONTRACT_TEMPLATE_FIELDS.docusign_template_id, "")
    ),
    docusign_template_name: clean(
      getTextValue(template_item, CONTRACT_TEMPLATE_FIELDS.docusign_template_name, "")
    ),
  };
}

function getContractMetadata(contract_item = null, overrides = {}) {
  return {
    state:
      clean(overrides.state) ||
      clean(getCategoryValue(contract_item, CONTRACT_FIELDS.state, "")),
    contract_type:
      clean(overrides.contract_type) ||
      clean(getCategoryValue(contract_item, CONTRACT_FIELDS.contract_type, "")),
    template_type:
      clean(overrides.template_type) ||
      clean(getCategoryValue(contract_item, CONTRACT_FIELDS.template_type, "")),
    assignment_allowed:
      normalizeBoolean(overrides.assignment_allowed) ||
      normalizeBoolean(
        getCategoryValue(contract_item, CONTRACT_FIELDS.assignment_allowed, "")
      ),
  };
}

function isUsableTemplate(meta = {}) {
  if (meta.active === "no") return false;
  if (meta.use_for_auto_generation === "no") return false;
  if (!meta.docusign_template_id) return false;

  const status = lower(meta.template_status);
  if (["draft", "deprecated", "archived"].includes(status)) return false;

  return true;
}

function matchKind(template_value = "", expected_value = "") {
  const template_raw = lower(template_value);
  const expected_raw = lower(expected_value);

  if (!expected_raw) return "not_required";
  if (!template_raw) return "wildcard";
  if (template_raw === expected_raw) return "exact";

  return "mismatch";
}

function scoreTemplate(meta = {}, contract = {}) {
  const state_match = matchKind(meta.state, contract.state);
  const contract_type_match = matchKind(meta.contract_type, contract.contract_type);
  const template_type_match = matchKind(meta.template_type, contract.template_type);
  const assignment_match = matchKind(meta.assignment_allowed, contract.assignment_allowed);

  if (
    [contract_type_match, template_type_match, assignment_match].includes("mismatch")
  ) {
    return null;
  }

  const state_allowed = ["exact", "wildcard", "not_required"].includes(state_match);
  if (!state_allowed) {
    return null;
  }

  let score = 0;

  if (meta.active === "yes") score += 1000;
  if (meta.use_for_auto_generation === "yes") score += 900;
  if (lower(meta.template_status) === "active") score += 800;

  if (state_match === "exact") score += 500;
  if (state_match === "wildcard") score += 150;

  if (contract_type_match === "exact") score += 450;
  if (contract_type_match === "wildcard") score += 100;

  if (template_type_match === "exact") score += 400;
  if (template_type_match === "wildcard") score += 75;

  if (assignment_match === "exact") score += 150;
  if (assignment_match === "wildcard" || assignment_match === "not_required") {
    score += 40;
  }

  if (meta.default_for_state_type === "yes") score += 250;
  score += Number(meta.priority || 0);

  return {
    score,
    state_match,
    contract_type_match,
    template_type_match,
    assignment_match,
  };
}

const defaultDeps = {
  findContractTemplates,
  getContractTemplateItem,
};

let runtimeDeps = { ...defaultDeps };

export function __setContractTemplateResolverTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetContractTemplateResolverTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function resolveContractTemplate({
  contract_item = null,
  contract_item_id = null,
  state = null,
  contract_type = null,
  template_type = null,
  assignment_allowed = null,
} = {}) {
  const contract = getContractMetadata(contract_item, {
    state,
    contract_type,
    template_type,
    assignment_allowed,
  });
  const templates = await runtimeDeps.findContractTemplates({}, 200, 0);

  const scored_candidates = templates
    .map((template_item) => {
      const meta = getTemplateMetadata(template_item);
      if (!isUsableTemplate(meta)) {
        return {
          item: template_item,
          meta,
          rejected: true,
          rejection_reason: "template_not_usable",
        };
      }

      const score = scoreTemplate(meta, contract);
      if (!score) {
        return {
          item: template_item,
          meta,
          rejected: true,
          rejection_reason: "contract_criteria_mismatch",
        };
      }

      return {
        item: template_item,
        meta,
        rejected: false,
        ...score,
      };
    })
    .sort((left, right) => {
      if (Boolean(left.rejected) !== Boolean(right.rejected)) {
        return left.rejected ? 1 : -1;
      }
      return Number(right.score || 0) - Number(left.score || 0);
    });

  const winner = scored_candidates.find((candidate) => !candidate.rejected) || null;

  if (!winner?.item?.item_id) {
    return {
      ok: false,
      reason: "no_usable_contract_template_found",
      contract_item_id: contract_item?.item_id || contract_item_id || null,
      contract,
      diagnostics: {
        total_candidates: scored_candidates.length,
        candidates: scored_candidates.map((candidate) => ({
          item_id: candidate.meta?.item_id || null,
          title: candidate.meta?.title || null,
          rejected: Boolean(candidate.rejected),
          rejection_reason: candidate.rejection_reason || null,
        })),
      },
    };
  }

  return {
    ok: true,
    reason: "contract_template_resolved",
    contract_item_id: contract_item?.item_id || contract_item_id || null,
    template_item_id: winner.item.item_id,
    docusign_template_id: winner.meta.docusign_template_id,
    docusign_template_name:
      winner.meta.docusign_template_name || winner.meta.title || null,
    template_item:
      winner.item?.fields || !runtimeDeps.getContractTemplateItem
        ? winner.item
        : await runtimeDeps.getContractTemplateItem(winner.item.item_id).catch(
            () => winner.item
          ),
    diagnostics: {
      contract,
      total_candidates: scored_candidates.length,
      chosen: {
        item_id: winner.meta.item_id,
        title: winner.meta.title,
        state_match: winner.state_match,
        contract_type_match: winner.contract_type_match,
        template_type_match: winner.template_type_match,
        assignment_match: winner.assignment_match,
        used_state_fallback: winner.state_match === "wildcard",
        default_for_state_type: winner.meta.default_for_state_type === "yes",
        priority: winner.meta.priority,
        score: winner.score,
      },
      candidates: scored_candidates.map((candidate) => ({
        item_id: candidate.meta?.item_id || null,
        title: candidate.meta?.title || null,
        rejected: Boolean(candidate.rejected),
        rejection_reason: candidate.rejection_reason || null,
        state_match: candidate.state_match || null,
        contract_type_match: candidate.contract_type_match || null,
        template_type_match: candidate.template_type_match || null,
        assignment_match: candidate.assignment_match || null,
        score: candidate.score || null,
      })),
    },
  };
}

export default resolveContractTemplate;
