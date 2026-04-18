import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

const RUNTIME_STATE_ROOT = "/tmp/real-estate-automation-runtime-state";

function clean(value) {
  return String(value ?? "").trim();
}

function sanitizeSegment(value = "", fallback = "state") {
  const normalized = clean(value)
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  return normalized || fallback;
}

function buildNamespacePath(namespace = "runtime") {
  const segments = clean(namespace)
    .split("/")
    .map((segment) => sanitizeSegment(segment))
    .filter(Boolean);

  return path.join(RUNTIME_STATE_ROOT, ...(segments.length ? segments : ["runtime"]));
}

function buildRuntimeStateFilename(key = "") {
  const normalized_key = clean(key) || "state";
  const digest = crypto
    .createHash("sha256")
    .update(normalized_key, "utf8")
    .digest("hex");
  const slug = sanitizeSegment(normalized_key).slice(0, 80) || "state";

  return `${digest.slice(0, 16)}-${slug}.json`;
}

export function buildRuntimeStateRecordId(namespace = "runtime", key = "") {
  return `${clean(namespace)}:${clean(key)}`;
}

export function parseRuntimeStateRecordId(record_id = "") {
  const normalized = clean(record_id);
  const separator_index = normalized.indexOf(":");

  if (separator_index === -1) {
    return {
      namespace: normalized || null,
      key: null,
    };
  }

  return {
    namespace: normalized.slice(0, separator_index) || null,
    key: normalized.slice(separator_index + 1) || null,
  };
}

export function buildRuntimeStateFilePath(namespace = "runtime", key = "") {
  return path.join(
    buildNamespacePath(namespace),
    buildRuntimeStateFilename(key)
  );
}

async function ensureParentDirectory(file_path) {
  await fs.mkdir(path.dirname(file_path), { recursive: true });
}

export async function readRuntimeState({
  namespace = "runtime",
  key = "",
} = {}) {
  const file_path = buildRuntimeStateFilePath(namespace, key);

  try {
    const raw = await fs.readFile(file_path, "utf8");
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

export async function writeRuntimeState({
  namespace = "runtime",
  key = "",
  state = {},
} = {}) {
  const file_path = buildRuntimeStateFilePath(namespace, key);
  await ensureParentDirectory(file_path);
  await fs.writeFile(file_path, JSON.stringify(state, null, 2), "utf8");

  return {
    ok: true,
    file_path,
    state,
  };
}

export async function createRuntimeStateIfAbsent({
  namespace = "runtime",
  key = "",
  state = {},
} = {}) {
  const file_path = buildRuntimeStateFilePath(namespace, key);
  await ensureParentDirectory(file_path);

  try {
    const handle = await fs.open(file_path, "wx");
    try {
      await handle.writeFile(JSON.stringify(state, null, 2), "utf8");
    } finally {
      await handle.close();
    }

    return {
      ok: true,
      created: true,
      file_path,
      state,
    };
  } catch (error) {
    if (error?.code !== "EEXIST") {
      throw error;
    }

    return {
      ok: true,
      created: false,
      file_path,
      state: await readRuntimeState({ namespace, key }),
    };
  }
}

export default {
  buildRuntimeStateRecordId,
  parseRuntimeStateRecordId,
  buildRuntimeStateFilePath,
  readRuntimeState,
  writeRuntimeState,
  createRuntimeStateIfAbsent,
};
