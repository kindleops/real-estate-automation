// ─── load-recent-templates.js ────────────────────────────────────────────

const DEFAULT_LIMIT = 10;

export function loadRecentTemplates({
  brain_item = null,
  limit = DEFAULT_LIMIT,
} = {}) {
  const fields = Array.isArray(brain_item?.fields) ? brain_item.fields : [];
  const by_external_id = Object.fromEntries(
    fields.map((field) => [field.external_id, field])
  );

  const last_template_id =
    by_external_id["last-template-sent"]?.values?.[0]?.value?.item_id ?? null;

  const all_ids = [
    ...(last_template_id ? [last_template_id] : []),
  ];

  const seen = new Set();
  const recent_template_ids = all_ids
    .filter((id) => {
      if (!id || seen.has(id)) return false;
      seen.add(id);
      return true;
    })
    .slice(0, limit);

  return {
    ok: true,
    count: recent_template_ids.length,
    last_template_id,
    recent_template_ids,
  };
}

export default loadRecentTemplates;
