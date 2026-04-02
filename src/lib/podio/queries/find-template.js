import {
  getTemplateItem,
  findTemplates,
  findActiveTemplates,
} from "@/lib/podio/apps/templates.js";

export async function findTemplate({
  item_id = null,
  filters = null,
  active_only = false,
  limit = 1,
  offset = 0,
}) {
  if (item_id) {
    return getTemplateItem(item_id);
  }

  if (active_only) {
    const res = await findActiveTemplates(limit, offset);
    return res?.items?.[0] ?? res?.[0] ?? null;
  }

  if (filters) {
    const res = await findTemplates(filters, limit, offset);
    return res?.items?.[0] ?? res?.[0] ?? null;
  }

  return null;
}

export default findTemplate;