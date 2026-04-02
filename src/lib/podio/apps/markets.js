import APP_IDS from "@/lib/config/app-ids.js";
import {
  getItem,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.markets;

export const MARKET_FIELDS = {
  title: "title",
  zip_codes: "zip-codes",
  median_home_value: "median-home-value",
  median_rent: "median-rent",
  days_on_market: "days-on-market",
  cash_buyer_density_score: "cash-buyer-density-score",
  hedge_fund_density_score: "hedge-fund-density-score",
  mf_buyer_density_score: "mf-buyer-density-score",
  avg_price_per_unit: "avg-price-per-unit",
  avg_cap_rate: "avg-cap-rate",
  best_strategy: "best-strategy",
  market_hotness_score: "market-hotness-score",
  follow_up_intensity: "follow-up-intensity",
  market_volatility_score: "market-volatility-score",
  smart_offer_floor: "smart-offer-floor",
  smart_offer_ceiling: "smart-offer-ceiling",
  rehab_multiplier: "rehab-multiplier",
};

export const getMarketItem = (item_id) =>
  getItem(item_id);

export default {
  APP_ID,
  MARKET_FIELDS,
  getMarketItem,
};
