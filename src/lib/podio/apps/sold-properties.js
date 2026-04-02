import APP_IDS from "@/lib/config/app-ids.js";
import {
  filterAppItems,
} from "@/lib/providers/podio.js";

export const APP_IDS_SOLD_PROPERTIES = Object.freeze([
  APP_IDS.sold_properties,
  APP_IDS.sold_properties_legacy,
]);

export const SOLD_PROPERTY_FIELDS = {
  property_id: "property-id",
  company_name: "full-name",
  owner_type: "owner-type-2",
  property_address: "property-address",
  market_label: "market",
  market: "market-2",
  zip_code: "relationship",
  market_status: "market-status",
  last_sale_price: "last-sale-price-2",
  estimated_value: "estimated-value-2",
  price_off_value: "price-off-value",
  mls_sold_date: "mls-sold-date",
  mls_sold_price: "mls-sold-price",
  property_class: "property-class",
  property_type: "property-type",
  property_style: "property-style",
  ppsf: "ppsf",
  ppu: "ppu",
  potential_flip_spread: "potential-flip-spread",
  beds: "bedrooms",
  baths: "bathrooms",
  square_feet: "square-feet",
  units: "number-of-units",
};

async function findInApp(app_id, filters = {}, limit = 50, offset = 0) {
  const response = await filterAppItems(app_id, filters, { limit, offset });
  const items = response?.items ?? response ?? [];
  return items.map((item) => ({
    ...item,
    __app_id: app_id,
  }));
}

export async function findSoldPropertyItems(filters = {}, limit = 50, offset = 0) {
  const per_app_limit = Math.max(1, Math.ceil(limit / APP_IDS_SOLD_PROPERTIES.length));
  const results = await Promise.all(
    APP_IDS_SOLD_PROPERTIES.map((app_id) =>
      findInApp(app_id, filters, per_app_limit, offset)
    )
  );

  return results.flat();
}

export default {
  APP_IDS_SOLD_PROPERTIES,
  SOLD_PROPERTY_FIELDS,
  findSoldPropertyItems,
};
