import test from "node:test";
import assert from "node:assert/strict";

import { runLivePodioRoundtripVerification } from "@/lib/verification/live-podio.js";
import { runLiveTextgridSendVerification } from "@/lib/verification/live-textgrid.js";
import { runLiveDocusignVerification } from "@/lib/verification/live-docusign.js";

test("live Podio verification requires explicit confirm_live", async () => {
  const result = await runLivePodioRoundtripVerification({
    confirm_live: false,
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "confirm_live_required");
});

test("live TextGrid verification requires explicit confirm_live", async () => {
  const result = await runLiveTextgridSendVerification({
    to: "+15550000001",
    from: "+15550000002",
    confirm_live: false,
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "confirm_live_required");
});

test("live TextGrid verification send is skipped with 423 when gate is disabled", async () => {
  const checked_flags = [];
  let send_calls = 0;

  const result = await runLiveTextgridSendVerification(
    {
      to: "+15550000001",
      from: "+15550000002",
      body: "verification test",
      confirm_live: true,
    },
    {
      env: {},
      getSystemFlag: async (key) => {
        checked_flags.push(key);
        return false;
      },
      sendTextgridSMS: async () => {
        send_calls += 1;
        throw new Error("sendTextgridSMS must not run when verification gate is disabled");
      },
    }
  );

  assert.equal(result.ok, false);
  assert.equal(result.skipped, true);
  assert.equal(result.status, 423);
  assert.equal(result.reason, "verification_textgrid_send_disabled");
  assert.deepEqual(checked_flags, ["verification_textgrid_send_enabled"]);
  assert.equal(send_calls, 0);
});

test("live DocuSign verification enforces confirm_live and tiny caps", async () => {
  const missing_confirm = await runLiveDocusignVerification({
    action: "create_send",
    dry_run: false,
    confirm_live: false,
  });

  assert.equal(missing_confirm.ok, false);
  assert.equal(missing_confirm.reason, "confirm_live_required");

  const too_many_signers = await runLiveDocusignVerification({
    action: "create_send",
    dry_run: true,
    confirm_live: true,
    signers: [{}, {}, {}],
  });

  assert.equal(too_many_signers.ok, false);
  assert.equal(too_many_signers.reason, "signers_limit_exceeded");
});
