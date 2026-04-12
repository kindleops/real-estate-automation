import APP_IDS from "@/lib/config/app-ids.js";
import {
  getItem,
  updateItem,
  filterAppItems,
  findByField,
} from "@/lib/providers/podio.js";

const APP_ID = APP_IDS.contract_templates;

export const CONTRACT_TEMPLATE_FIELDS = {
  title: "title",
  template_id: "template-id",
  state: "state",
  contract_type: "contract-type",
  template_type: "template-type",
  version: "version",
  active: "active",
  docusign_template_id: "docusign-template-id",
  docusign_template_name: "docusign-template-name",
  template_source: "template-source",
  use_for_auto_generation: "use-for-auto-generation",
  priority: "priority",
  default_for_state_type: "default-for-state-type",
  assignment_allowed: "assignment-allowed",
  default_closing_timeline_days: "default-closing-timeline-days",
  special_conditions: "special-conditions",
  template_status: "template-status",
  last_updated: "last-updated",
  notes: "notes",
};

export const getContractTemplateItem = (item_id) =>
  getItem(item_id);

export const updateContractTemplateItem = (item_id, fields = {}, revision = null) =>
  updateItem(item_id, fields, revision);

export const findContractTemplates = (filters = {}, limit = 30, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset });

export const findContractTemplateByTitle = (title) =>
  findByField(APP_ID, "title", title);

export default {
  APP_ID,
  CONTRACT_TEMPLATE_FIELDS,
  getContractTemplateItem,
  updateContractTemplateItem,
  findContractTemplates,
  findContractTemplateByTitle,
};
