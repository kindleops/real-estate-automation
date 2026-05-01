import test from "node:test";
import assert from "node:assert/strict";
import { runSendQueue } from "../../src/lib/domain/queue/run-send-queue.js";

const NOW = "2026-05-01T12:00:00Z";

function makeStubs() {
  return {
    deps: {
      loadRunnableSendQueueRows: async () => ({
        rows: [],
        raw_rows: [],
        skipped: [],
        preclaim_scanned_count: 0,
        eligible_claim_count: 0,
      }),
      claimSendQueueRow: async (row) => ({
        ok: true,
        claimed: true,
        reason: "claimed",
        row,
        lock_token: "test-lock",
      }),
      processSendQueueItem: async () => ({ ok: true, sent: true }),
      withRunLock: async ({ fn }) => fn(),
      getSystemFlag: async () => true,
      evaluateContactWindow: () => ({ allowed: true }),
      info: () => {},
      warn: () => {},
    }
  };
}

test("manual inbox send without selected_template_id is eligible", async () => {
  const { deps } = makeStubs();
  
  const manual_row = {
    id: 1001,
    queue_key: "inbox:send_now:phone:+12146072916:123",
    message_body: "Hello world",
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    metadata: {}
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [manual_row],
    raw_rows: [manual_row],
    skipped: [],
    preclaim_scanned_count: 1,
    eligible_claim_count: 1,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.eligible_claim_count, 1);
  assert.equal(result.processed_count, 1);
  assert.equal(result.sent_count, 1);
});

test("manual inbox send without candidate_snapshot is eligible", async () => {
  const { deps } = makeStubs();
  
  const manual_row = {
    id: 1002,
    message_type: "manual_reply",
    message_body: "Hello world",
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    metadata: {}
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [manual_row],
    raw_rows: [manual_row],
    skipped: [],
    preclaim_scanned_count: 1,
    eligible_claim_count: 1,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.eligible_claim_count, 1);
  assert.equal(result.processed_count, 1);
});

test("manual inbox send missing body is excluded", async () => {
  const { deps } = makeStubs();
  
  const manual_row = {
    id: 1003,
    use_case_template: "inbox_manual_send_now",
    message_body: "", // Empty
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    metadata: {}
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [],
    raw_rows: [manual_row],
    skipped: [{ row: manual_row, reason: "missing_message_body" }],
    preclaim_scanned_count: 1,
    eligible_claim_count: 0,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.eligible_claim_count, 0);
  assert.equal(result.sent_count, 0);
  assert.equal(result.first_10_excluded[0].reason, "missing_message_body");
});

test("manual inbox send missing to_phone_number is excluded", async () => {
  const { deps } = makeStubs();
  
  const manual_row = {
    id: 1004,
    queue_key: "inbox:send_now:xyz",
    message_body: "Hello",
    to_phone_number: "", // Empty
    from_phone_number: "+18885551212",
    queue_status: "queued",
    metadata: {}
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [],
    raw_rows: [manual_row],
    skipped: [{ row: manual_row, reason: "missing_to_phone_number" }],
    preclaim_scanned_count: 1,
    eligible_claim_count: 0,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.eligible_claim_count, 0);
  assert.equal(result.first_10_excluded[0].reason, "missing_to_phone_number");
});

test("normal campaign row missing selected_template_id is still excluded", async () => {
  const { deps } = makeStubs();
  
  const campaign_row = {
    id: 2001,
    queue_key: "campaign:123",
    message_body: "Hello",
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    metadata: {} // Missing template and snapshot
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [],
    raw_rows: [campaign_row],
    skipped: [{ row: campaign_row, reason: "missing_selected_template_id" }],
    preclaim_scanned_count: 1,
    eligible_claim_count: 0,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.eligible_claim_count, 0);
  assert.equal(result.first_10_excluded[0].reason, "missing_selected_template_id");
});

test("normal campaign row missing candidate_snapshot is still excluded", async () => {
  const { deps } = makeStubs();
  
  const campaign_row = {
    id: 2002,
    queue_key: "campaign:123",
    message_body: "Hello",
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    template_id: 555,
    metadata: {} // Missing candidate_snapshot
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [],
    raw_rows: [campaign_row],
    skipped: [{ row: campaign_row, reason: "missing_candidate_snapshot" }],
    preclaim_scanned_count: 1,
    eligible_claim_count: 0,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.eligible_claim_count, 0);
  assert.equal(result.first_10_excluded[0].reason, "missing_candidate_snapshot");
});
