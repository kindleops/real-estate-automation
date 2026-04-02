// ─── link-event-records.js ───────────────────────────────────────────────
import {
  getFirstAppReferenceId,
  updateItem,
} from "@/lib/providers/podio.js";

const EVENT_FIELDS = {
  master_owner: "master-owner",
  prospect: "linked-seller",
  property: "property",
  textgrid_number: "textgrid-number",
  phone_number: "phone-number",
};

function asSingleAppRef(value) {
  if (!value) return undefined;
  return [value];
}

function firstDefined(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== "") {
      return value;
    }
  }
  return null;
}

export async function linkEventRecords({
  event_item = null,
  event_item_id = null,
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  phone_item_id = null,
  textgrid_number_item_id = null,
} = {}) {
  const resolved_event_item_id = event_item_id || event_item?.item_id || null;

  if (!resolved_event_item_id) {
    return {
      ok: false,
      reason: "missing_event_item_id",
    };
  }

  const existing_master_owner_id =
    event_item ? getFirstAppReferenceId(event_item, EVENT_FIELDS.master_owner, null) : null;

  const existing_prospect_id =
    event_item ? getFirstAppReferenceId(event_item, EVENT_FIELDS.prospect, null) : null;

  const existing_property_id =
    event_item ? getFirstAppReferenceId(event_item, EVENT_FIELDS.property, null) : null;

  const existing_phone_item_id =
    event_item ? getFirstAppReferenceId(event_item, EVENT_FIELDS.phone_number, null) : null;

  const existing_textgrid_number_item_id =
    event_item ? getFirstAppReferenceId(event_item, EVENT_FIELDS.textgrid_number, null) : null;

  const resolved_master_owner_id = firstDefined(master_owner_id, existing_master_owner_id);
  const resolved_prospect_id = firstDefined(prospect_id, existing_prospect_id);
  const resolved_property_id = firstDefined(property_id, existing_property_id);
  const resolved_phone_item_id = firstDefined(phone_item_id, existing_phone_item_id);
  const resolved_textgrid_number_item_id = firstDefined(
    textgrid_number_item_id,
    existing_textgrid_number_item_id
  );

  const fields = {
    ...(resolved_master_owner_id
      ? { [EVENT_FIELDS.master_owner]: asSingleAppRef(resolved_master_owner_id) }
      : {}),
    ...(resolved_prospect_id
      ? { [EVENT_FIELDS.prospect]: asSingleAppRef(resolved_prospect_id) }
      : {}),
    ...(resolved_property_id
      ? { [EVENT_FIELDS.property]: asSingleAppRef(resolved_property_id) }
      : {}),
    ...(resolved_phone_item_id
      ? { [EVENT_FIELDS.phone_number]: asSingleAppRef(resolved_phone_item_id) }
      : {}),
    ...(resolved_textgrid_number_item_id
      ? { [EVENT_FIELDS.textgrid_number]: asSingleAppRef(resolved_textgrid_number_item_id) }
      : {}),
  };

  if (!Object.keys(fields).length) {
    return {
      ok: false,
      reason: "no_link_fields_to_update",
      event_item_id: resolved_event_item_id,
    };
  }

  await updateItem(resolved_event_item_id, fields);

  return {
    ok: true,
    event_item_id: resolved_event_item_id,
    linked: {
      master_owner_id: resolved_master_owner_id,
      prospect_id: resolved_prospect_id,
      property_id: resolved_property_id,
      phone_item_id: resolved_phone_item_id,
      textgrid_number_item_id: resolved_textgrid_number_item_id,
    },
  };
}

export default linkEventRecords;