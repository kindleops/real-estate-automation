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

test("manual inbox send outside local window is still eligible", async () => {
  const { deps } = makeStubs();
  
  const manual_row = {
    id: 1005,
    queue_key: "inbox:send_now:phone:+12146072916:123",
    message_body: "Hello world",
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    metadata: {}
  };

  // Mock contact window as NOT allowed
  deps.evaluateContactWindow = () => ({ allowed: false, reason: "outside_local_send_window" });

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [manual_row], // Even if skipped in real loadRunnable, runSendQueue secondary loop should handle it
    raw_rows: [manual_row],
    skipped: [],
    preclaim_scanned_count: 1,
    eligible_claim_count: 1,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.processed_count, 1, "Manual send should bypass contact window");
  assert.equal(result.sent_count, 1);
});

test("normal campaign row outside local window is still excluded", async () => {
  const { deps } = makeStubs();
  
  const campaign_row = {
    id: 2005,
    queue_key: "campaign:123",
    message_body: "Hello",
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    template_id: 555,
    metadata: { candidate_snapshot: { id: 1 } }
  };

  // Mock contact window as NOT allowed
  deps.evaluateContactWindow = () => ({ allowed: false, reason: "outside_local_send_window" });

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [],
    raw_rows: [campaign_row],
    skipped: [{ row: campaign_row, reason: "outside_local_send_window" }],
    preclaim_scanned_count: 1,
    eligible_claim_count: 0,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.processed_count, 0, "Normal campaign should NOT bypass contact window");
});

test("unknown inbound auto reply without template is eligible", async () => {
  const { deps } = makeStubs();
  
  const unknown_reply = {
    id: 3001,
    use_case_template: "unknown_inbound_auto_reply",
    message_body: "Sorry I am busy",
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued",
    metadata: {}
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [unknown_reply],
    raw_rows: [unknown_reply],
    skipped: [],
    preclaim_scanned_count: 1,
    eligible_claim_count: 1,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.processed_count, 1);
});

test("manual inbox row missing body still fails", async () => {
  const { deps } = makeStubs();
  
  const manual_row = {
    id: 1003,
    metadata: { action: "send_now", source: "inbox" },
    message_body: "", // Empty
    to_phone_number: "+12146072916",
    from_phone_number: "+18885551212",
    queue_status: "queued"
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [],
    raw_rows: [manual_row],
    skipped: [{ row: manual_row, reason: "missing_message_body" }],
    preclaim_scanned_count: 1,
    eligible_claim_count: 0,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.processed_count, 0);
  assert.equal(result.first_10_excluded[0].reason, "missing_message_body");
});

test("manual inbox row missing to/from still fails", async () => {
  const { deps } = makeStubs();
  
  const manual_row = {
    id: 1004,
    metadata: { created_from: "leadcommand_inbox" },
    message_body: "Hello",
    to_phone_number: "", // Empty
    from_phone_number: "+18885551212",
    queue_status: "queued"
  };

  deps.loadRunnableSendQueueRows = async () => ({
    rows: [],
    raw_rows: [manual_row],
    skipped: [{ row: manual_row, reason: "missing_to_phone_number" }],
    preclaim_scanned_count: 1,
    eligible_claim_count: 0,
  });

  const result = await runSendQueue({ limit: 10, now: NOW }, deps);
  
  assert.equal(result.processed_count, 0);
  assert.equal(result.first_10_excluded[0].reason, "missing_to_phone_number");
});
