/**
 * scan-owner-statuses.js
 * Scans all owners in the SMS / TIER #1 / ALL view and reports
 * contact-status-2 distribution.
 */

import {
  findMasterOwnerItems,
  listMasterOwnerViews,
  MASTER_OWNER_FIELDS,
  updateMasterOwnerItem,
} from "@/lib/podio/apps/master-owners.js";
import { fetchAllItems } from "@/lib/providers/podio.js";
import APP_IDS from "@/lib/config/app-ids.js";

const DRY_RUN = process.env.DRY_RUN !== "false";
const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 50000;
const AUTO_SET = new Set(["sent", "follow-up scheduled", "received"]);

function getCat(item, ext_id) {
  const f = item?.fields?.find((f) => f.external_id === ext_id);
  if (!f?.values?.length) return null;
  return String(f.values[0]?.value?.text ?? "").toLowerCase().trim() || null;
}

async function run() {
  console.log("=== scan-owner-statuses ===");
  console.log(`DRY_RUN: ${DRY_RUN}`);
  console.log("");

  // Fetch owners with contact-status-2 = "Sent" directly (much faster than scanning all)
  console.log('Fetching owners with contact-status-2 = "Sent"...');
  const sent_owners = await fetchAllItems(
    APP_IDS.master_owners,
    { [MASTER_OWNER_FIELDS.contact_status_2]: ["Sent"] },
    { page_size: 500 }
  );
  console.log(`Found ${sent_owners.length} owners with contact-status-2="Sent"`);

  // Also check "Follow-Up Scheduled"
  console.log('Fetching owners with contact-status-2 = "Follow-Up Scheduled"...');
  const fus_owners = await fetchAllItems(
    APP_IDS.master_owners,
    { [MASTER_OWNER_FIELDS.contact_status_2]: ["Follow-Up Scheduled"] },
    { page_size: 500 }
  );
  console.log(`Found ${fus_owners.length} owners with contact-status-2="Follow-Up Scheduled"`);

  const owners = [...sent_owners, ...fus_owners];
  console.log(`Total fetched: ${owners.length} owners`);
  console.log("");

  // Analyze
  const status_dist = {};
  let blank_count = 0;
  const to_reset = [];

  for (const o of owners) {
    const cs = getCat(o, "contact-status");
    const cs2 = getCat(o, "contact-status-2");
    const key = cs2 || "(blank)";
    status_dist[key] = (status_dist[key] || 0) + 1;

    if (!cs && !cs2) {
      blank_count++;
    } else if (!cs && cs2 && AUTO_SET.has(cs2)) {
      to_reset.push({ id: o.item_id, cs2 });
    }
  }

  console.log("contact-status-2 distribution:");
  for (const [k, v] of Object.entries(status_dist).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${k}: ${v}`);
  }
  console.log("");
  console.log(`Already first-touch (both blank): ${blank_count}`);
  console.log(`Need reset (auto-set, no contact-status): ${to_reset.length}`);
  console.log("");

  if (to_reset.length === 0) {
    console.log("Nothing to reset.");
    return;
  }

  // Reset
  console.log("--- Owners to reset ---");
  for (const { id, cs2 } of to_reset) {
    console.log(`  Owner ${id}: contact_status_2="${cs2}"`);
  }
  console.log("");

  if (DRY_RUN) {
    console.log("[DRY RUN] Would reset these owners. Re-run with DRY_RUN=false to apply.");
    return;
  }

  console.log("--- Resetting ---");
  let ok = 0;
  let fail = 0;
  for (const { id } of to_reset) {
    try {
      await updateMasterOwnerItem(id, {
        [MASTER_OWNER_FIELDS.contact_status_2]: null,
        [MASTER_OWNER_FIELDS.next_follow_up_at]: null,
        [MASTER_OWNER_FIELDS.last_contacted_at]: null,
        [MASTER_OWNER_FIELDS.last_outbound]: null,
      });
      ok++;
      console.log(`  OK ${id}`);
    } catch (e) {
      fail++;
      console.log(`  FAIL ${id}: ${e.message}`);
    }
  }
  console.log("");
  console.log(`Done. ${ok} reset, ${fail} failed.`);
}

run().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
