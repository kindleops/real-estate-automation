import APP_IDS from "@/lib/config/app-ids.js";
import {
  getItem,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.zip_codes;

export const ZIP_CODE_FIELDS = {
  title: "title",
  market: "market",
  market_temperature: "market-temperature",
  price_trend: "price-trend",
  median_rent: "median-rent",
  cash_buyer_activity_score: "cash-buyer-activity-score",
  flip_volume_score: "flip-volume-score",
  landlord_density_score: "landlord-density-score",
  cap_rate_estimate: "cap-rate-estimate",
  median_sales_price: "median-sales-price",
  price_per_sqft: "price-per-sqft",
  appreciation_1_year: "appreciation-1-year",
  active_listings_count: "active-listings-count",
  sold_listings_count: "sold-listings-count",
};

export const getZipCodeItem = (item_id) =>
  getItem(item_id);

export default {
  APP_ID,
  ZIP_CODE_FIELDS,
  getZipCodeItem,
};
