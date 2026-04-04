import test from "node:test";
import assert from "node:assert/strict";

import { PodioError } from "@/lib/providers/podio.js";
import { runSendQueue } from "@/lib/domain/queue/run-send-queue.js";
import { handleQueueRunRequest } from "@/lib/domain/queue/queue-run-request.js";
import { appRefField, categoryField, createPodioItem } from "../helpers/test-helpers.js";

test("runSendQueue skips revision-capped queue items and continues later work", async () => {
  const warn_calls = [];
  const alert_calls = [];
  const processed_ids = [];

  const revision_error = new PodioError(
    "This item has exceeded the maximum number of revisions.",
    {
      method: "PUT",
      path: "/item/3281484514",
      status: 400,
    }
  );

  const result = await runSendQueue(
    {
      limit: 10,
      now: "2026-04-04T12:00:00.000Z",
    },
    {
      fetchAllItems: async () => [
        createPodioItem(3281484514, {
          "queue-status": categoryField("Queued"),
          "master-owner": appRefField(5001),
        }),
        createPodioItem(3281484515, {
          "queue-status": categoryField("Queued"),
          "master-owner": appRefField(5001),
        }),
      ],
      processSendQueueItem: async (queue_item_id) => {
        processed_ids.push(queue_item_id);
        if (queue_item_id === 3281484514) throw revision_error;
        return {
          ok: true,
          sent: true,
        };
      },
      recordSystemAlert: async (payload) => {
        alert_calls.push(payload);
      },
      resolveSystemAlert: async () => {},
      withRunLock: async ({ fn }) => fn(),
      warn: (event, meta) => {
        warn_calls.push({ event, meta });
      },
      info: () => {},
    }
  );

  assert.deepEqual(processed_ids, [3281484514, 3281484515]);
  assert.equal(result.ok, true);
  assert.equal(result.processed_count, 2);
  assert.equal(result.sent_count, 1);
  assert.equal(result.failed_count, 0);
  assert.equal(result.skipped_count, 1);
  assert.deepEqual(result.results[0], {
    queue_item_id: 3281484514,
    ok: true,
    skipped: true,
    reason: "queue_item_revision_limit_exceeded",
    failure_bucket: "revision_limit_exceeded",
    manual_review_required: true,
  });
  assert.equal(result.results[1].queue_item_id, 3281484515);
  assert.equal(result.results[1].ok, true);

  assert.equal(warn_calls.length, 1);
  assert.equal(warn_calls[0].event, "queue.run_item_skipped_revision_limit");
  assert.equal(warn_calls[0].meta.queue_item_id, 3281484514);
  assert.equal(warn_calls[0].meta.failure_bucket, "revision_limit_exceeded");

  assert.equal(alert_calls.length, 1);
  assert.equal(alert_calls[0].code, "revision_limit_exceeded");
  assert.equal(alert_calls[0].affected_ids[0], 3281484514);
  assert.equal(alert_calls[0].metadata.failure_bucket, "revision_limit_exceeded");
  assert.equal(alert_calls[0].metadata.recovery, "manual_review_required");
});

test("queue run route returns success when run summary contains revision-limit skips", async () => {
  const response = await handleQueueRunRequest(
    new Request("http://localhost/api/internal/queue/run?limit=2"),
    "GET",
    {
      requireCronAuth: () => ({
        authorized: true,
        auth: {
          authenticated: true,
          is_vercel_cron: false,
        },
      }),
      runSendQueue: async () => ({
        ok: true,
        processed_count: 2,
        sent_count: 1,
        failed_count: 0,
        skipped_count: 1,
        results: [
          {
            queue_item_id: 3281484514,
            ok: true,
            skipped: true,
            reason: "queue_item_revision_limit_exceeded",
            failure_bucket: "revision_limit_exceeded",
          },
          {
            queue_item_id: 3281484515,
            ok: true,
            sent: true,
          },
        ],
      }),
      logger: {
        info: () => {},
        error: () => {},
      },
    }
  );

  assert.equal(response.status, 200);

  const payload = await response.json();
  assert.equal(payload.ok, true);
  assert.equal(payload.route, "internal/queue/run");
  assert.equal(payload.result.skipped_count, 1);
  assert.equal(payload.result.failed_count, 0);
  assert.equal(
    payload.result.results[0].failure_bucket,
    "revision_limit_exceeded"
  );
});