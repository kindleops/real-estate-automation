import APP_IDS from "@/lib/config/app-ids.js";
import {
  fetchAllItems,
  filterAppItems,
  getItem,
} from "@/lib/providers/podio.js";

export const APP_ID = APP_IDS.companies;

export const COMPANY_FIELDS = {
  company_id: "seller-id",
  owner_full_name: "owner-full-name",
  owner_type: "owner-type",
  owner_first_name: "title",
  owner_last_name: "owner-last-name",
  property_profile: "property-profile",
  entity_age: "entity-age",
  contact_phones: "contact-phones",
  contact_emails: "contact-emails",
  primary_officers: "primary-officers",
  preferred_contact_method: "preferred-contact-method",
  total_properties_owned: "total-properties-owned",
  estimated_portfolio_value: "estimated-portfolio-value",
  out_of_state_owner: "out-of-state-owner",
};

export const getCompanyItem = (item_id) =>
  getItem(item_id);

export const findCompanyItems = (filters = {}, limit = 50, offset = 0) =>
  filterAppItems(APP_ID, filters, { limit, offset }).then(
    (response) => response?.items ?? response ?? []
  );

export const fetchAllCompanyItems = (filters = {}, options = {}) =>
  fetchAllItems(APP_ID, filters, options);

export default {
  APP_ID,
  COMPANY_FIELDS,
  getCompanyItem,
  findCompanyItems,
  fetchAllCompanyItems,
};
