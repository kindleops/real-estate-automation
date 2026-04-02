import {
  TITLE_COMPANY_FIELDS,
  findTitleCompanyItems,
} from "@/lib/podio/apps/title-companies.js";
import {
  TITLE_ROUTING_FIELDS,
  getTitleRoutingItem,
  updateTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import { getPropertyItem } from "@/lib/podio/apps/properties.js";
import { getFirstAppReferenceId, getNumberValue, getTextValue } from "@/lib/providers/podio.js";

function asAppRef(value) {
  return value ? [value] : undefined;
}

function sortBestFirst(items = []) {
  return [...items].sort((a, b) => {
    const a_rating = Number(getNumberValue(a, TITLE_COMPANY_FIELDS.rating, 0) || 0);
    const b_rating = Number(getNumberValue(b, TITLE_COMPANY_FIELDS.rating, 0) || 0);
    return b_rating - a_rating;
  });
}

function resolveMarketId({ title_routing_item = null, property_item = null, market_id = null } = {}) {
  return (
    market_id ||
    getFirstAppReferenceId(title_routing_item, TITLE_ROUTING_FIELDS.market, null) ||
    getFirstAppReferenceId(property_item, "market-2", null) ||
    getFirstAppReferenceId(property_item, "market", null) ||
    null
  );
}

export async function selectTitleCompany({
  property_id = null,
  market_id = null,
  title_routing_id = null,
} = {}) {
  const [title_routing_item, property_item] = await Promise.all([
    title_routing_id ? getTitleRoutingItem(title_routing_id) : Promise.resolve(null),
    property_id ? getPropertyItem(property_id) : Promise.resolve(null),
  ]);

  const resolved_market_id = resolveMarketId({
    title_routing_item,
    property_item,
    market_id,
  });

  if (!resolved_market_id) {
    return {
      ok: false,
      selected: false,
      reason: "missing_market_id",
      property_id,
      title_routing_id,
    };
  }

  const matches = await findTitleCompanyItems(
    { [TITLE_COMPANY_FIELDS.market]: resolved_market_id },
    100,
    0
  );

  const selected = sortBestFirst(matches)[0] || null;

  if (!selected?.item_id) {
    return {
      ok: false,
      selected: false,
      reason: "no_title_company_for_market",
      market_id: resolved_market_id,
    };
  }

  if (title_routing_item?.item_id) {
    await updateTitleRoutingItem(title_routing_item.item_id, {
      [TITLE_ROUTING_FIELDS.title_company]: asAppRef(selected.item_id),
      [TITLE_ROUTING_FIELDS.primary_title_contact]:
        getTextValue(selected, TITLE_COMPANY_FIELDS.contact_manager, "") || undefined,
      [TITLE_ROUTING_FIELDS.title_contact_email]:
        getTextValue(selected, TITLE_COMPANY_FIELDS.new_order_email, "") || undefined,
      [TITLE_ROUTING_FIELDS.title_contact_phone]:
        getTextValue(selected, TITLE_COMPANY_FIELDS.phone, "") || undefined,
    });
  }

  return {
    ok: true,
    selected: true,
    reason: "title_company_selected",
    market_id: resolved_market_id,
    title_routing_id: title_routing_item?.item_id || title_routing_id || null,
    title_company_item_id: selected.item_id,
    title_company_name: getTextValue(selected, TITLE_COMPANY_FIELDS.title, ""),
    title_company: selected,
  };
}

export default selectTitleCompany;
