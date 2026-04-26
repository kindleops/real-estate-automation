import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-sync-token",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const ALLOWED_TABLES = new Set([
  "master_owners",
  "sub_owners",
  "prospects",
  "phones",
  "emails",
  "properties",
]);

const NUMERIC_FIELDS = new Set([
  // contact/master pipeline
  "best_contact_slot",
  "contactability_score",
  "financial_pressure_score",
  "urgency_score",
  "priority_score",
  "portfolio_total_value",
  "portfolio_total_equity",
  "portfolio_total_loan_balance",
  "portfolio_total_loan_payment",
  "portfolio_total_tax_amount",
  "portfolio_total_units",
  "property_count",
  "tax_delinquent_count",
  "oldest_tax_delinquent_year",
  "active_lien_count",
  "max_ownership_years",
  "message_variant_seed",
  "source_slot",
  "raw_contact_score",
  "contact_score_final",
  "raw_phone_score",
  "phone_score_final",
  "raw_email_field",
  "best_email_linkage_score_raw",
  "email_score_final",
  "rank_position",
  "master_owner_priority_score",
  "sort_rank",
  "best_phone_score",
  "best_slot",
  "contact_rank_position",
  "email_linkage_score_raw",
  "email_rank",

  // properties pipeline
  "assd_improvement_value",
  "assd_land_value",
  "assd_total_value",
  "assd_year",
  "building_square_feet",
  "calculated_improvement_value",
  "calculated_land_value",
  "calculated_total_value",
  "effective_year_built",
  "estimated_value",
  "lot_size_depth_feet",
  "lot_size_frontage_feet",
  "num_of_fireplaces",
  "past_due_amount",
  "situs_census_tract",
  "stories",
  "sum_buildings_nbr",
  "sum_commercial_units",
  "sum_garage_sqft",
  "tax_amt",
  "tax_delinquent_year",
  "tax_year",
  "total_loan_amt",
  "total_loan_balance",
  "total_loan_payment",
  "cash_offer",
  "equity_amount",
  "equity_percent",
  "estimated_repair_cost",
  "estimated_repair_cost_per_sqft",
  "hoa_fee_amount",
  "id",
  "latitude",
  "longitude",
  "lot_acreage",
  "lot_square_feet",
  "mls_current_listing_price",
  "mls_sold_price",
  "ownership_years",
  "ai_score",
  "sale_price",
  "saleprice",
  "total_baths",
  "total_bedrooms",
  "units_count",
  "year_built",
  "offer_ppsf",
  "offer_ppu",
  "offer_ppbd",
  "offer_ppls",
  "avg_sqft_per_unit",
  "beds_per_unit",
  "structured_motivation_score",
  "deal_strength_score",
  "tag_distress_score",
  "final_acquisition_score",
]);

const BOOLEAN_FIELDS = new Set([
  // contact/master pipeline
  "likely_owner",
  "likely_renting",
  "is_primary_prospect",
  "sms_eligible",
  "email_eligible",
  "is_best_phone_for_slot",
  "is_best_phone_for_owner",
  "is_best_email_for_slot",
  "is_best_email_for_owner",

  // properties pipeline
  "tax_delinquent",
  "active_lien",
  "highlighted",
  "is_corporate_owner",
  "out_of_state_owner",
  "removed_owner",
]);

const JSON_FIELDS = new Set([
  // contact/master pipeline
  "owner_entity_ids_json",
  "address_bases_json",
  "owner_locations_json",
  "markets_json",
  "zip_codes_json",
  "counties_json",
  "seller_tags_json",
  "joined_property_ids_json",
  "joined_prospect_ids_json",
  "joined_phone_ids_json",
  "joined_email_ids_json",
  "joined_sub_owner_ids_json",
  "person_flags_json",
  "linked_property_ids_json",
  "phones_json",
  "emails_json",
  "linked_source_slots_json",
  "linked_prospect_ids_json",
  "linked_individual_keys_json",
  "linked_languages_json",

  // properties pipeline
  "property_flags_json",
  "raw_payload_json",
]);

const DATE_FIELDS = new Set([
  "sale_date",
  "recording_date",
  "default_date",
  "mls_sold_date",
  "exported_at_utc",
  "processed_at_utc",
]);

function parseJsonField(value: unknown): unknown {
  if (value === "" || value === null || value === undefined) return null;
  if (Array.isArray(value)) return value;
  if (typeof value === "object") return value;

  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
}

function cleanNumeric(value: unknown): number | null {
  if (value === "" || value === null || value === undefined) return null;

  const s = String(value)
    .replace(/[$,%]/g, "")
    .replace(/,/g, "")
    .trim();

  if (!s) return null;

  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function cleanBoolean(value: unknown): boolean | null {
  if (value === "" || value === null || value === undefined) return null;
  if (typeof value === "boolean") return value;

  const s = String(value).trim().toLowerCase();

  if (["true", "t", "yes", "y", "1"].includes(s)) return true;
  if (["false", "f", "no", "n", "0"].includes(s)) return false;

  return null;
}

function cleanDate(value: unknown): string | null {
  if (value === "" || value === null || value === undefined) return null;

  const s = String(value).trim();
  if (!s) return null;

  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;

  // Keep date-only columns date-only; timestamp columns get full ISO.
  if (["exported_at_utc", "processed_at_utc"].includes(s)) {
    return d.toISOString();
  }

  return d.toISOString().slice(0, 10);
}

function cleanField(key: string, value: unknown): unknown {
  if (value === "" || value === undefined) return null;
  if (JSON_FIELDS.has(key)) return parseJsonField(value);
  if (NUMERIC_FIELDS.has(key)) return cleanNumeric(value);
  if (BOOLEAN_FIELDS.has(key)) return cleanBoolean(value);
  if (DATE_FIELDS.has(key)) return cleanDate(value);
  return value;
}

function cleanRow(row: Record<string, unknown>): Record<string, unknown> {
  const cleaned: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(row)) {
    cleaned[key] = cleanField(key, value);
  }

  return cleaned;
}

function normalizeRowForTable(
  table: string,
  row: Record<string, unknown>,
): Record<string, unknown> {
  if (table === "prospects") {
    const rawSourceSlot = row.source_slot;
    const rawSlotLabel = String(row.slot_label ?? "");
    let parsedSlot: number | null = null;

    if (rawSourceSlot !== null && rawSourceSlot !== undefined && rawSourceSlot !== "") {
      const match = String(rawSourceSlot).match(/[0-2]/);
      if (match) parsedSlot = Number(match[0]);
    }

    if (parsedSlot === null && rawSlotLabel) {
      const match = rawSlotLabel.match(/[0-2]/);
      if (match) parsedSlot = Number(match[0]);
    }

    row.source_slot = parsedSlot ?? 0;
  }

  if (table === "phones" || table === "emails") {
    if (!row.primary_prospect_id) row.primary_prospect_id = null;
    if (!row.canonical_prospect_id) row.canonical_prospect_id = null;
  }

  if (table === "properties") {
    if (!row.property_export_id && row.upsert_key) {
      row.property_export_id = row.upsert_key;
    }

    if (!row.property_id && row.id) {
      row.property_id = String(row.id);
    }

    if (!row.master_owner_id) row.master_owner_id = null;
    if (!row.master_key) row.master_key = null;
  }

  return row;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const syncToken = req.headers.get("x-sync-token") ?? "";
    const expectedToken = Deno.env.get("SYNC_TOKEN") ?? "";

    if (!expectedToken || syncToken !== expectedToken) {
      return new Response(JSON.stringify({ ok: false, error: "unauthorized" }), {
        status: 401,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

    if (!supabaseUrl) throw new Error("Missing SUPABASE_URL");
    if (!serviceRole) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false },
    });

    const body = await req.json().catch(() => ({}));
    const table = String(body.table ?? "").trim();
    const rows = Array.isArray(body.rows) ? body.rows : [];
    const conflictColumn = String(body.conflictColumn ?? "upsert_key").trim();

    if (!table) throw new Error("Missing table");
    if (!ALLOWED_TABLES.has(table)) {
      throw new Error(`Table not allowed: ${table}`);
    }

    if (!rows.length) {
      return new Response(
        JSON.stringify({ ok: true, table, upserted: 0, batches: 0 }),
        {
          status: 200,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        },
      );
    }

    const chunkSize = 250;
    let upserted = 0;
    let batches = 0;

    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows
        .slice(i, i + chunkSize)
        .map((row) =>
          normalizeRowForTable(table, cleanRow(row as Record<string, unknown>))
        );

      const { error } = await supabase.from(table).upsert(chunk, {
        onConflict: conflictColumn,
        ignoreDuplicates: false,
      });

      if (error) {
        console.error("FAILED_TABLE:", table);
        console.error("FAILED_BATCH:", batches + 1);
        console.error("FAILED_SAMPLE:", JSON.stringify(chunk.slice(0, 2)));
        throw new Error(`${table} batch ${batches + 1}: ${error.message}`);
      }

      upserted += chunk.length;
      batches += 1;
    }

    const { error: logError } = await supabase.from("import_log").insert({
      table_name: table,
      rows_attempted: rows.length,
      rows_processed: upserted,
      status: "success",
      error_text: null,
      payload_sample: rows
        .slice(0, 2)
        .map((row) =>
          normalizeRowForTable(table, cleanRow(row as Record<string, unknown>))
        ),
      processed_at_utc: new Date().toISOString(),
    });

    if (logError) console.error("import_log insert failed:", logError.message);

    return new Response(JSON.stringify({ ok: true, table, upserted, batches }), {
      status: 200,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);

    return new Response(JSON.stringify({ ok: false, error: message }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});