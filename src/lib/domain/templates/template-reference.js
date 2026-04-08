import APP_IDS from "@/lib/config/app-ids.js";
import { getAttachedFieldSchema } from "@/lib/podio/schema.js";

function clean(value) {
  return String(value ?? "").trim();
}

function toItemId(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function uniqueItemIds(values = []) {
  return [...new Set(values.map((value) => toItemId(value)).filter(Boolean))];
}

export function getTemplateItemAppId(template_item = null) {
  return (
    toItemId(template_item?.app?.app_id) ||
    toItemId(template_item?.raw?.app?.app_id) ||
    toItemId(template_item?.app_id) ||
    toItemId(template_item?.appId) ||
    (toItemId(template_item?.item_id) &&
    clean(template_item?.source).toLowerCase() !== "local_registry"
      ? APP_IDS.templates
      : null) ||
    null
  );
}

export function getTemplateSelectionDetails({
  template_id = null,
  template_item = null,
} = {}) {
  const explicit_template_id =
    template_id !== null && template_id !== undefined && template_id !== ""
      ? template_id
      : template_item?.item_id ?? null;
  const selected_template_source =
    clean(template_item?.source) ||
    (clean(explicit_template_id).startsWith("local-template:")
      ? "local_registry"
      : toItemId(explicit_template_id)
        ? "podio"
        : null);

  return {
    selected_template_id: explicit_template_id,
    selected_template_item_id:
      toItemId(template_item?.item_id) || toItemId(explicit_template_id),
    selected_template_bridge_id: toItemId(template_item?.template_id),
    selected_template_app_id: getTemplateItemAppId(template_item),
    selected_template_source: selected_template_source || null,
    selected_template_title:
      clean(template_item?.title) ||
      clean(template_item?.name) ||
      clean(template_item?.raw?.title) ||
      null,
    selected_template_use_case: clean(template_item?.use_case) || null,
    selected_template_variant_group: clean(template_item?.variant_group) || null,
  };
}

export function resolveTemplateFieldReference({
  host_app_id,
  host_field_external_id,
  template_id = null,
  template_item = null,
} = {}) {
  const selected = getTemplateSelectionDetails({
    template_id,
    template_item,
  });
  const field_schema = getAttachedFieldSchema(host_app_id, host_field_external_id);
  const target_app_ids = uniqueItemIds(field_schema?.referenced_app_ids || []);

  if (!selected.selected_template_id) {
    return {
      ...selected,
      target_app_ids,
      attached_template_id: null,
      field_value: undefined,
      attachment_strategy: "not_requested",
      attachment_reason: "no_template_selected",
    };
  }

  if (
    selected.selected_template_source === "local_registry" ||
    clean(selected.selected_template_id).startsWith("local-template:")
  ) {
    return {
      ...selected,
      target_app_ids,
      attached_template_id: null,
      field_value: undefined,
      attachment_strategy: "skipped_local_template",
      attachment_reason: "local_template_not_attachable",
    };
  }

  if (
    selected.selected_template_item_id &&
    (!target_app_ids.length ||
      (selected.selected_template_app_id &&
        target_app_ids.includes(selected.selected_template_app_id)))
  ) {
    return {
      ...selected,
      target_app_ids,
      attached_template_id: selected.selected_template_item_id,
      field_value: [selected.selected_template_item_id],
      attachment_strategy: "selected_template_item_id",
      attachment_reason: null,
    };
  }

  if (selected.selected_template_bridge_id) {
    return {
      ...selected,
      target_app_ids,
      attached_template_id: selected.selected_template_bridge_id,
      field_value: [selected.selected_template_bridge_id],
      attachment_strategy: "template_id_bridge",
      attachment_reason: null,
    };
  }

  if (!template_item && selected.selected_template_item_id) {
    return {
      ...selected,
      target_app_ids,
      attached_template_id: selected.selected_template_item_id,
      field_value: [selected.selected_template_item_id],
      attachment_strategy: "raw_id_passthrough",
      attachment_reason: null,
    };
  }

  return {
    ...selected,
    target_app_ids,
    attached_template_id: null,
    field_value: undefined,
    attachment_strategy: "unresolved",
    attachment_reason: target_app_ids.length
      ? "template_app_mismatch"
      : "template_relation_target_unknown",
  };
}

