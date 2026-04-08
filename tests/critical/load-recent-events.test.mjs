import test from "node:test";
import assert from "node:assert/strict";

import { loadRecentEvents } from "@/lib/domain/context/load-recent-events.js";
import {
  createPodioItem,
  dateField,
  textField,
} from "../helpers/test-helpers.js";

function makeEvent(item_id, timestamp) {
  return createPodioItem(item_id, {
    "message-id": textField(`msg-${item_id}`),
    timestamp: dateField(timestamp),
    message: textField(`Message ${item_id}`),
  });
}

test("loadRecentEvents stops after phone-number scope when enough events are found", async () => {
  const calls = [];

  const result = await loadRecentEvents(
    {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      limit: 2,
    },
    {
      filterAppItemsImpl: async (_app_id, filters) => {
        calls.push(filters);
        return {
          items: [
            makeEvent(1001, "2026-04-08T19:10:00.000Z"),
            makeEvent(1002, "2026-04-08T19:09:00.000Z"),
          ],
        };
      },
    }
  );

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], { "phone-number": 401 });
  assert.equal(result.count, 2);
  assert.deepEqual(
    result.events.map((event) => event.item_id),
    [1001, 1002]
  );
});

test("loadRecentEvents backfills master-owner and prospect scopes only when needed", async () => {
  const calls = [];

  const result = await loadRecentEvents(
    {
      phone_item_id: 401,
      master_owner_id: 201,
      prospect_id: 301,
      limit: 3,
    },
    {
      filterAppItemsImpl: async (_app_id, filters) => {
        calls.push(filters);

        if (filters["phone-number"]) {
          return {
            items: [
              makeEvent(2001, "2026-04-08T19:08:00.000Z"),
            ],
          };
        }

        if (filters["master-owner"]) {
          return {
            items: [
              makeEvent(2002, "2026-04-08T19:09:00.000Z"),
              makeEvent(2001, "2026-04-08T19:08:00.000Z"),
            ],
          };
        }

        return {
          items: [
            makeEvent(2003, "2026-04-08T19:10:00.000Z"),
          ],
        };
      },
    }
  );

  assert.equal(calls.length, 3);
  assert.deepEqual(calls[0], { "phone-number": 401 });
  assert.deepEqual(calls[1], { "master-owner": 201 });
  assert.deepEqual(calls[2], { "linked-seller": 301 });
  assert.equal(result.count, 3);
  assert.deepEqual(
    result.events.map((event) => event.item_id),
    [2003, 2002, 2001]
  );
});
