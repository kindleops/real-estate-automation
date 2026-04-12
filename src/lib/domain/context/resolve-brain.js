// ─── resolve-brain.js ────────────────────────────────────────────────────
import { getItem } from "@/lib/providers/podio.js";

import {
  BRAIN_FIELDS,
  createBrainItem,
  findBestBrainMatch,
} from "@/lib/podio/apps/ai-conversation-brain.js";
import { toPodioDateField } from "@/lib/utils/dates.js";
import { buildBrainCreateDefaults } from "@/lib/domain/communications-engine/state-machine.js";

const defaultDeps = {
  getItem,
  createBrainItem,
  findBestBrainMatch,
};

let runtimeDeps = { ...defaultDeps };

function toId(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

export function __setResolveBrainTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetResolveBrainTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

async function safeGetItem(item_id) {
  const resolved_item_id = toId(item_id);
  if (!resolved_item_id) return null;

  try {
    return await runtimeDeps.getItem(resolved_item_id);
  } catch {
    return null;
  }
}

async function findBrainMatch({
  phone_item_id = null,
  prospect_id = null,
  master_owner_id = null,
} = {}) {
  return runtimeDeps.findBestBrainMatch({
    phone_item_id: toId(phone_item_id),
    prospect_id: toId(prospect_id),
    master_owner_id: toId(master_owner_id),
  });
}

export async function resolveBrain({
  phone_item_id = null,
  prospect_id = null,
  master_owner_id = null,
} = {}) {
  const trace = {
    owner_id: toId(master_owner_id),
    phone_item_id: toId(phone_item_id),
    prospect_id: toId(prospect_id),
    master_owner_id: toId(master_owner_id),
  };

  console.log("➡️ entering resolve-brain", trace);

  try {
    const brain_item = await findBrainMatch({
      phone_item_id,
      prospect_id,
      master_owner_id,
    });

    console.log("⬅️ exiting resolve-brain", {
      owner_id: trace.owner_id,
      brain_item_id: brain_item?.item_id ?? null,
    });

    return brain_item;
  } catch (error) {
    console.error("💥 resolve-brain failed", {
      ...trace,
      message: error?.message ?? null,
      podio_status:
        error?.status ??
        error?.response?.status ??
        error?.cause?.status ??
        null,
    });
    throw error;
  }
}

export async function createBrain({
  master_owner_id = null,
  prospect_id = null,
  property_id = null,
  phone_item_id = null,
  logger = null,
} = {}) {
  const trace = {
    owner_id: toId(master_owner_id),
    phone_item_id: toId(phone_item_id),
    prospect_id: toId(prospect_id),
    master_owner_id: toId(master_owner_id),
    property_id: toId(property_id),
  };

  console.log("➡️ entering create-brain", trace);

  const defaults = buildBrainCreateDefaults({
    master_owner_id,
    prospect_id,
    property_id,
    phone_item_id,
    phone_link_enabled: true,
  });

  const fields = {
    ...defaults,
    ...(defaults[BRAIN_FIELDS.last_contact_timestamp]
      ? {}
      : { [BRAIN_FIELDS.last_contact_timestamp]: toPodioDateField(new Date()) }),
  };

  try {
    const created = await runtimeDeps.createBrainItem(fields);
    const created_id = created?.item_id ?? null;

    if (logger?.info) {
      logger.info("context.brain_created", {
        created_item_id: created_id,
        phone_item_id: toId(phone_item_id),
        master_owner_id: toId(master_owner_id),
        prospect_id: toId(prospect_id),
        property_id: toId(property_id),
        phone_link_written: Boolean(toId(phone_item_id)),
      });
    }

    const brain_item =
      (created_id ? await safeGetItem(created_id) : null) ||
      (await findBrainMatch({
        phone_item_id,
        prospect_id,
        master_owner_id,
      }));

    if (!brain_item && logger?.warn) {
      logger.warn("context.brain_create_refetch_failed", {
        created_item_id: created_id,
        phone_item_id: toId(phone_item_id),
        master_owner_id: toId(master_owner_id),
        prospect_id: toId(prospect_id),
      });
    }

    console.log("⬅️ exiting create-brain", {
      owner_id: trace.owner_id,
      created_item_id: created_id,
      brain_item_id: brain_item?.item_id ?? null,
    });

    return brain_item;
  } catch (error) {
    console.error("💥 create-brain failed", {
      ...trace,
      message: error?.message ?? null,
      podio_status:
        error?.status ??
        error?.response?.status ??
        error?.cause?.status ??
        null,
    });
    throw error;
  }
}

export default resolveBrain;
