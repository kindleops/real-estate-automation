import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  __resetResolveBrainTestDeps,
  __setResolveBrainTestDeps,
  createBrain,
  resolveBrain,
} from "@/lib/domain/context/resolve-brain.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetResolveBrainTestDeps();
});

test("resolveBrain uses the phone-linked brain match as the primary truth path", async () => {
  let received = null;

  __setResolveBrainTestDeps({
    findBestBrainMatch: async (payload) => {
      received = payload;
      return createPodioItem(701, {
        "phone-number": appRefField(51),
        "conversation-stage": categoryField("Ownership Confirmation"),
      });
    },
  });

  const result = await resolveBrain({
    phone_item_id: 51,
    prospect_id: 31,
    master_owner_id: 21,
  });

  assert.equal(result?.item_id, 701);
  assert.deepEqual(received, {
    phone_item_id: 51,
    prospect_id: 31,
    master_owner_id: 21,
  });
});

test("createBrain writes the live phone-number relation into AI Conversation Brain", async () => {
  let created_fields = null;
  const logger_entries = [];

  __setResolveBrainTestDeps({
    createBrainItem: async (fields) => {
      created_fields = fields;
      return { item_id: 901 };
    },
    getItem: async () =>
      createPodioItem(901, {
        "phone-number": appRefField(51),
        "conversation-stage": categoryField("Ownership Confirmation"),
      }),
  });

  const result = await createBrain({
    phone_item_id: 51,
    prospect_id: 31,
    master_owner_id: 21,
    property_id: 41,
    logger: {
      info: (event, meta) => logger_entries.push({ event, meta }),
    },
  });

  assert.equal(result?.item_id, 901);
  assert.equal(created_fields["phone-number"], 51);
  assert.equal(created_fields["master-owner"], 21);
  assert.equal(created_fields.prospect, 31);
  assert.deepEqual(created_fields.properties, [41]);
  assert.equal(created_fields["conversation-stage"], "Ownership Confirmation");
  assert.equal(logger_entries[0]?.event, "context.brain_created");
  assert.equal(logger_entries[0]?.meta?.phone_link_written, true);
});

test("createBrain falls back to phone-aware brain matching when direct refetch misses", async () => {
  let fallback_payload = null;

  __setResolveBrainTestDeps({
    createBrainItem: async () => ({ item_id: 902 }),
    getItem: async () => {
      throw new Error("temporary_refetch_failure");
    },
    findBestBrainMatch: async (payload) => {
      fallback_payload = payload;
      return createPodioItem(903, {
        "phone-number": appRefField(51),
      });
    },
  });

  const result = await createBrain({
    phone_item_id: 51,
    prospect_id: 31,
    master_owner_id: 21,
  });

  assert.equal(result?.item_id, 903);
  assert.deepEqual(fallback_payload, {
    phone_item_id: 51,
    prospect_id: 31,
    master_owner_id: 21,
  });
});
