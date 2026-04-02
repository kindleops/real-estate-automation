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
