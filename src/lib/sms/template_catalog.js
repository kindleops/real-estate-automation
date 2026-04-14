// ─── template_catalog.js ──────────────────────────────────────────────────
// Loads the final multilingual CSV template pack, normalizes rows, validates
// required columns, converts Yes/No columns to booleans, and builds in-memory
// indexes for fast lookup.
//
// The CSV is the authoritative outbound template catalog.

import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeLanguage } from "@/lib/sms/language_aliases.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ══════════════════════════════════════════════════════════════════════════
// COLUMN DEFINITIONS
// ══════════════════════════════════════════════════════════════════════════

const REQUIRED_COLUMNS = Object.freeze([
  "Template ID",
  "Active?",
  "Use Case",
  "Language",
  "Template Text",
]);

const BOOLEAN_COLUMNS = Object.freeze([
  "Active?",
  "Is First Touch",
  "Is Follow-Up",
]);

// Columns known to be junk separators in the CSV — skip silently
const JUNK_COLUMN_PATTERN = /^-(?:\d+(?:\.\d+)?)?$/;

// The CSV contains both "Agent Style FIt" (typo) and "Agent Style Fit" —
// we prefer the correctly-spelled version.
const TYPO_COLUMN_MAP = Object.freeze({
  "Agent Style FIt": null,  // ignore the typo column
});

// ══════════════════════════════════════════════════════════════════════════
// CSV PARSING
// ══════════════════════════════════════════════════════════════════════════

function parseCSVLine(line) {
  const fields = [];
  let current = "";
  let in_quotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (in_quotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          in_quotes = false;
        }
      } else {
        current += ch;
      }
    } else if (ch === '"') {
      in_quotes = true;
    } else if (ch === ",") {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

function normalizeBoolean(value) {
  const trimmed = String(value ?? "").trim().toLowerCase();
  if (trimmed === "yes" || trimmed === "true" || trimmed === "1") return true;
  if (trimmed === "no" || trimmed === "false" || trimmed === "0" || trimmed === "") return false;
  return false;
}

function cleanString(value) {
  return String(value ?? "").trim();
}

function isJunkColumn(header) {
  return JUNK_COLUMN_PATTERN.test(header.trim());
}

// ══════════════════════════════════════════════════════════════════════════
// CSV LOADING
// ══════════════════════════════════════════════════════════════════════════

function parseCSV(csv_text) {
  const lines = csv_text.split(/\r?\n/).filter((line) => line.trim() !== "");
  if (lines.length < 2) {
    throw new Error("template_catalog: CSV file must have a header row and at least one data row");
  }

  const headers = parseCSVLine(lines[0]);
  const clean_headers = headers.map((h) => h.trim());

  // Validate required columns exist
  const header_set = new Set(clean_headers);
  const missing = REQUIRED_COLUMNS.filter((col) => !header_set.has(col));
  if (missing.length > 0) {
    throw new Error(`template_catalog: CSV missing required columns: ${missing.join(", ")}`);
  }

  // Build column index map, skipping junk and typo columns
  const column_map = [];
  for (let i = 0; i < clean_headers.length; i++) {
    const header = clean_headers[i];
    if (isJunkColumn(header)) continue;
    if (header in TYPO_COLUMN_MAP) continue;
    column_map.push({ index: i, name: header });
  }

  const rows = [];
  for (let line_num = 1; line_num < lines.length; line_num++) {
    const fields = parseCSVLine(lines[line_num]);
    const row = {};

    for (const { index, name } of column_map) {
      const raw_value = index < fields.length ? fields[index] : "";
      if (BOOLEAN_COLUMNS.includes(name)) {
        row[name] = normalizeBoolean(raw_value);
      } else {
        row[name] = cleanString(raw_value);
      }
    }

    // Skip rows with blank template text
    if (!row["Template Text"]) continue;

    rows.push(row);
  }

  return rows;
}

// ══════════════════════════════════════════════════════════════════════════
// ROW NORMALIZATION
// ══════════════════════════════════════════════════════════════════════════

function normalizeRow(row) {
  const language_raw = row["Language"] || "English";
  const canonical_language = normalizeLanguage(language_raw) || language_raw;

  return {
    template_id: row["Template ID"] || null,
    active: row["Active?"] === true,
    use_case: row["Use Case"] || null,
    stage_code: row["Stage Code"] || null,
    stage_label: row["Stage Label"] || null,
    language: canonical_language,
    language_raw: language_raw,
    agent_style_fit: row["Agent Style Fit"] || null,
    property_type_scope: row["Property Type Scope"] || null,
    deal_strategy: row["Deal Strategy"] || null,
    is_first_touch: row["Is First Touch"] === true,
    is_follow_up: row["Is Follow-Up"] === true,
    template_text: row["Template Text"] || "",
    english_translation: row["English Translation"] || "",
  };
}

// ══════════════════════════════════════════════════════════════════════════
// IN-MEMORY INDEXES
// ══════════════════════════════════════════════════════════════════════════

function buildIndexes(rows) {
  const by_use_case = new Map();
  const by_language = new Map();
  const by_use_case_language = new Map();
  const by_template_id = new Map();
  const by_stage_code = new Map();

  for (const row of rows) {
    // by template_id
    if (row.template_id) {
      by_template_id.set(row.template_id, row);
    }

    // by use_case
    const uc = (row.use_case || "").toLowerCase();
    if (uc) {
      if (!by_use_case.has(uc)) by_use_case.set(uc, []);
      by_use_case.get(uc).push(row);
    }

    // by language
    const lang = (row.language || "").toLowerCase();
    if (lang) {
      if (!by_language.has(lang)) by_language.set(lang, []);
      by_language.get(lang).push(row);
    }

    // by use_case + language composite
    const composite_key = `${uc}|${lang}`;
    if (uc && lang) {
      if (!by_use_case_language.has(composite_key)) by_use_case_language.set(composite_key, []);
      by_use_case_language.get(composite_key).push(row);
    }

    // by stage_code
    const sc = (row.stage_code || "").toLowerCase();
    if (sc) {
      if (!by_stage_code.has(sc)) by_stage_code.set(sc, []);
      by_stage_code.get(sc).push(row);
    }
  }

  return { by_use_case, by_language, by_use_case_language, by_template_id, by_stage_code };
}

// ══════════════════════════════════════════════════════════════════════════
// CATALOG SINGLETON
// ══════════════════════════════════════════════════════════════════════════

let _catalog = null;
let _catalog_path = null;

const DEFAULT_CSV_PATH = resolve(__dirname, "../../../docs/templates/lifecycle-sms-template-pack.csv");

/**
 * Load or return cached catalog from the CSV path.
 * @param {string} [csv_path] - Override CSV path (mainly for testing)
 * @returns {{ rows, indexes, path }}
 */
export function loadCatalog(csv_path = DEFAULT_CSV_PATH) {
  if (_catalog && _catalog_path === csv_path) return _catalog;

  const resolved_path = resolve(csv_path);
  const csv_text = readFileSync(resolved_path, "utf-8");
  const raw_rows = parseCSV(csv_text);
  const rows = raw_rows.map(normalizeRow);
  const indexes = buildIndexes(rows);

  _catalog = { rows, indexes, path: resolved_path };
  _catalog_path = csv_path;
  return _catalog;
}

/**
 * Force reload the catalog (useful after CSV updates or for testing).
 */
export function reloadCatalog(csv_path = DEFAULT_CSV_PATH) {
  _catalog = null;
  _catalog_path = null;
  return loadCatalog(csv_path);
}

/**
 * Reset catalog singleton (for testing only).
 */
export function __resetCatalog() {
  _catalog = null;
  _catalog_path = null;
}

// ══════════════════════════════════════════════════════════════════════════
// QUERY HELPERS
// ══════════════════════════════════════════════════════════════════════════

/**
 * Find all templates matching a use case (case-insensitive).
 * @param {string} use_case
 * @param {{ active_only?: boolean }} options
 * @returns {Array}
 */
export function findByUseCase(use_case, { active_only = true } = {}) {
  const catalog = loadCatalog();
  const key = String(use_case || "").toLowerCase();
  const rows = catalog.indexes.by_use_case.get(key) || [];
  return active_only ? rows.filter((r) => r.active) : rows;
}

/**
 * Find all templates matching a use case + language pair.
 * @param {string} use_case
 * @param {string} language - Canonical language
 * @param {{ active_only?: boolean }} options
 * @returns {Array}
 */
export function findByUseCaseAndLanguage(use_case, language, { active_only = true } = {}) {
  const catalog = loadCatalog();
  const key = `${String(use_case || "").toLowerCase()}|${String(language || "").toLowerCase()}`;
  const rows = catalog.indexes.by_use_case_language.get(key) || [];
  return active_only ? rows.filter((r) => r.active) : rows;
}

/**
 * Retrieve a single template by its ID.
 * @param {string} template_id
 * @returns {object|null}
 */
export function findByTemplateId(template_id) {
  const catalog = loadCatalog();
  return catalog.indexes.by_template_id.get(template_id) || null;
}

/**
 * Return the total row count in the loaded catalog.
 */
export function catalogSize() {
  const catalog = loadCatalog();
  return catalog.rows.length;
}

/**
 * Return all loaded rows (defensive copy).
 */
export function allRows() {
  const catalog = loadCatalog();
  return [...catalog.rows];
}

export { parseCSV, normalizeRow, buildIndexes, REQUIRED_COLUMNS, BOOLEAN_COLUMNS };

export default {
  loadCatalog,
  reloadCatalog,
  __resetCatalog,
  findByUseCase,
  findByUseCaseAndLanguage,
  findByTemplateId,
  catalogSize,
  allRows,
};
