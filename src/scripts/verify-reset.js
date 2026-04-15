/**
 * verify-reset.js - Quick check: how many owners still have contact-status-2 set?
 */
import { fetchAllItems } from "@/lib/providers/podio.js";
import APP_IDS from "@/lib/config/app-ids.js";

const sent = await fetchAllItems(APP_IDS.master_owners, { "contact-status-2": ["Sent"] }, { page_size: 500 });
console.log(`contact-status-2="Sent": ${sent.length}`);

const fus = await fetchAllItems(APP_IDS.master_owners, { "contact-status-2": ["Follow-Up Scheduled"] }, { page_size: 500 });
console.log(`contact-status-2="Follow-Up Scheduled": ${fus.length}`);

const received = await fetchAllItems(APP_IDS.master_owners, { "contact-status-2": ["Received"] }, { page_size: 500 });
console.log(`contact-status-2="Received": ${received.length}`);

console.log(`Total remaining: ${sent.length + fus.length + received.length}`);
