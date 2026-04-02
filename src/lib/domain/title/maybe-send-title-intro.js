import {
  TITLE_ROUTING_FIELDS,
  getTitleRoutingItem,
  updateTitleRoutingItem,
} from "@/lib/podio/apps/title-routing.js";
import {
  TITLE_COMPANY_FIELDS,
  getTitleCompanyItem,
} from "@/lib/podio/apps/title-companies.js";
import {
  CONTRACT_FIELDS,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import {
  CLOSING_FIELDS,
  getClosingItem,
  updateClosingItem,
} from "@/lib/podio/apps/closings.js";
import {
  buildTitleIntroPackageFiles,
  createStoredDocumentPackage,
} from "@/lib/domain/documents/document-packages.js";
import { sendEmail } from "@/lib/providers/email.js";
import {
  getDateValue,
  getMoneyValue,
  getTextValue,
} from "@/lib/providers/podio.js";
import { syncPipelineState } from "@/lib/domain/pipelines/sync-pipeline-state.js";

function clean(value) {
  return String(value ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function getFieldValue(item, external_id) {
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const field = fields.find((entry) => entry?.external_id === external_id);

  if (!field?.values?.length) return null;

  const first = field.values[0];

  if (first?.value?.item_id) return first.value.item_id;
  if (typeof first?.value === "string") return first.value;
  if (typeof first?.value === "number") return first.value;
  if (first?.value?.text) return first.value.text;
  if (first?.start) return first.start;

  return null;
}

function appendNote(existing_notes, new_note) {
  const prior = clean(existing_notes);
  const next = clean(new_note);

  if (!next) return prior || undefined;
  if (!prior) return next;

  return `${prior}\n${next}`;
}

function extractTitleCompanyContact(title_company_item) {
  return {
    company_name: clean(getTextValue(title_company_item, TITLE_COMPANY_FIELDS.title, "")),
    contact_name: clean(
      getTextValue(title_company_item, TITLE_COMPANY_FIELDS.contact_manager, "")
    ),
    email: clean(
      getTextValue(title_company_item, TITLE_COMPANY_FIELDS.new_order_email, "")
    ),
    phone: clean(getFieldValue(title_company_item, TITLE_COMPANY_FIELDS.phone)),
  };
}

function extractRoutingTitleContact(title_routing_item) {
  return {
    contact_name: clean(
      getTextValue(title_routing_item, TITLE_ROUTING_FIELDS.primary_title_contact, "")
    ),
    email: clean(
      getTextValue(title_routing_item, TITLE_ROUTING_FIELDS.title_contact_email, "")
    ),
    phone: clean(getFieldValue(title_routing_item, TITLE_ROUTING_FIELDS.title_contact_phone)),
  };
}

function extractContractContext(contract_item) {
  return {
    contract_id: clean(getTextValue(contract_item, CONTRACT_FIELDS.contract_id, "")),
    contract_title: clean(getTextValue(contract_item, CONTRACT_FIELDS.title, "")),
    purchase_price:
      getMoneyValue(contract_item, CONTRACT_FIELDS.purchase_price_final, null),
    closing_date_target:
      getDateValue(contract_item, CONTRACT_FIELDS.closing_date_target, null) || "",
    creative_terms: clean(
      getTextValue(contract_item, CONTRACT_FIELDS.creative_terms, "")
    ),
  };
}

function resolveTitleContact({
  title_routing_item = null,
  title_company_item = null,
} = {}) {
  const routing_contact = extractRoutingTitleContact(title_routing_item);
  const title_company_contact = extractTitleCompanyContact(title_company_item);

  return {
    company_name: title_company_contact.company_name,
    contact_name: routing_contact.contact_name || title_company_contact.contact_name,
    email: routing_contact.email || title_company_contact.email,
    phone: routing_contact.phone || title_company_contact.phone,
  };
}

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") return "";
  const amount = Number(value);
  if (!Number.isFinite(amount)) return "";

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);
}

function buildTitleIntroEmail({
  title_company = {},
  contract_context = {},
  title_routing_item_id = null,
  closing_item_id = null,
  package_url = null,
} = {}) {
  const subject = `Open Title - ${
    contract_context.contract_title ||
    contract_context.contract_id ||
    `TR ${title_routing_item_id}`
  }`;

  const greeting = title_company.contact_name
    ? `Hi ${title_company.contact_name},`
    : "Hi,";

  const body = [
    greeting,
    "",
    "Please open title on the following file.",
    "",
    `Contract ID: ${contract_context.contract_id || "N/A"}`,
    `Reference: ${contract_context.contract_title || "N/A"}`,
    `Purchase Price: ${formatCurrency(contract_context.purchase_price) || "N/A"}`,
    `Target Close Date: ${contract_context.closing_date_target || "N/A"}`,
    title_company.phone ? `Title Company Phone: ${title_company.phone}` : "",
    contract_context.creative_terms
      ? `Notes: ${contract_context.creative_terms}`
      : "",
    "",
    `Internal Title Routing ID: ${title_routing_item_id || "N/A"}`,
    `Internal Closing ID: ${closing_item_id || "N/A"}`,
    package_url ? `Package Link: ${package_url}` : "",
    "",
    "Please confirm receipt and let us know what you need from our side to move forward.",
    "",
    "Thank you,",
    "Acquisitions Team",
  ].join("\n");

  return {
    subject,
    body,
  };
}

function shouldSendTitleIntro({
  title_routing_item = null,
  title_company = null,
  contract_item = null,
} = {}) {
  if (!title_routing_item?.item_id) {
    return {
      should_send: false,
      reason: "missing_title_routing_item",
    };
  }

  if (!contract_item?.item_id) {
    return {
      should_send: false,
      reason: "missing_contract_item",
    };
  }

  if (!title_company?.email) {
    return {
      should_send: false,
      reason: "missing_title_company_email",
    };
  }

  const routing_status = clean(
    getFieldValue(title_routing_item, TITLE_ROUTING_FIELDS.routing_status)
  ).toLowerCase();

  if (routing_status && routing_status !== "not routed") {
    return {
      should_send: false,
      reason: "title_intro_already_in_progress",
    };
  }

  return {
    should_send: true,
    reason: "ready_to_send_title_intro",
  };
}

export async function maybeSendTitleIntro({
  title_routing_item_id = null,
  title_routing_item = null,
  closing_item_id = null,
  closing_item = null,
  contract_item_id = null,
  contract_item = null,
  dry_run = false,
} = {}) {
  let resolved_title_routing_item = title_routing_item || null;

  if (!resolved_title_routing_item && title_routing_item_id) {
    resolved_title_routing_item = await getTitleRoutingItem(title_routing_item_id);
  }

  const resolved_title_routing_item_id =
    resolved_title_routing_item?.item_id ||
    title_routing_item_id ||
    null;

  if (!resolved_title_routing_item_id) {
    return {
      ok: false,
      sent: false,
      reason: "missing_title_routing_item_id",
    };
  }

  let resolved_closing_item = closing_item || null;
  if (!resolved_closing_item && closing_item_id) {
    resolved_closing_item = await getClosingItem(closing_item_id);
  }

  const linked_contract_item_id =
    contract_item_id ||
    getFieldValue(resolved_title_routing_item, TITLE_ROUTING_FIELDS.contract) ||
    null;

  let resolved_contract_item = contract_item || null;
  if (!resolved_contract_item && linked_contract_item_id) {
    resolved_contract_item = await getContractItem(linked_contract_item_id);
  }

  const linked_title_company_item_id =
    getFieldValue(resolved_title_routing_item, TITLE_ROUTING_FIELDS.title_company) ||
    null;

  const title_company_item = linked_title_company_item_id
    ? await getTitleCompanyItem(linked_title_company_item_id)
    : null;

  const title_company = resolveTitleContact({
    title_routing_item: resolved_title_routing_item,
    title_company_item,
  });
  const contract_context = extractContractContext(resolved_contract_item);

  const decision = shouldSendTitleIntro({
    title_routing_item: resolved_title_routing_item,
    title_company,
    contract_item: resolved_contract_item,
  });

  if (!decision.should_send) {
    return {
      ok: true,
      sent: false,
      reason: decision.reason,
      title_routing_item_id: resolved_title_routing_item_id,
    };
  }

  const email = buildTitleIntroEmail({
    title_company,
    contract_context,
    title_routing_item_id: resolved_title_routing_item_id,
    closing_item_id: resolved_closing_item?.item_id || closing_item_id || null,
    package_url: null,
  });
  const package_record = await createStoredDocumentPackage({
    namespace: "title-packages",
    entity_type: "title-routing",
    entity_id: resolved_title_routing_item_id,
    label: "title-intro-package",
    metadata: {
      title_routing_item_id: resolved_title_routing_item_id,
      closing_item_id: resolved_closing_item?.item_id || closing_item_id || null,
      contract_item_id: linked_contract_item_id || null,
      title_company_email: title_company.email || null,
    },
    files: buildTitleIntroPackageFiles({
      title_company,
      contract_context,
      title_routing_item_id: resolved_title_routing_item_id,
      closing_item_id: resolved_closing_item?.item_id || closing_item_id || null,
      email,
    }),
    dry_run,
  });
  const email_with_package = buildTitleIntroEmail({
    title_company,
    contract_context,
    title_routing_item_id: resolved_title_routing_item_id,
    closing_item_id: resolved_closing_item?.item_id || closing_item_id || null,
    package_url: package_record?.primary_file?.access_url || null,
  });

  let send_result = null;
  let pipeline = null;

  if (resolved_closing_item?.item_id) {
    resolved_closing_item = await getClosingItem(resolved_closing_item.item_id);
  }

  if (!dry_run) {
    send_result = await sendEmail({
      to: title_company.email,
      subject: email_with_package.subject,
      text: email_with_package.body,
      html: `<pre>${email_with_package.body}</pre>`,
    });

    if (!send_result?.ok) {
      return {
        ok: false,
        sent: false,
        drafted: false,
        reason: send_result?.error_message || "title_intro_send_failed",
        title_routing_item_id: resolved_title_routing_item_id,
        closing_item_id: resolved_closing_item?.item_id || null,
        title_company_email: title_company.email || null,
        subject: email_with_package.subject,
        body: email_with_package.body,
        send_result,
        draft_result: send_result,
        package_record,
      };
    }

    await updateTitleRoutingItem(resolved_title_routing_item_id, {
      [TITLE_ROUTING_FIELDS.routing_status]: "Routed",
      [TITLE_ROUTING_FIELDS.file_routed_date]: { start: nowIso() },
      [TITLE_ROUTING_FIELDS.primary_title_contact]:
        title_company.contact_name || undefined,
      [TITLE_ROUTING_FIELDS.title_contact_email]:
        title_company.email || undefined,
      [TITLE_ROUTING_FIELDS.title_contact_phone]:
        title_company.phone || undefined,
      [TITLE_ROUTING_FIELDS.title_notes]: appendNote(
        clean(getFieldValue(resolved_title_routing_item, TITLE_ROUTING_FIELDS.title_notes)),
        `[${nowIso()}] Title intro sent to ${title_company.email}. Subject: ${email_with_package.subject}`
      ),
      [TITLE_ROUTING_FIELDS.internal_notes]: appendNote(
        clean(
          getFieldValue(resolved_title_routing_item, TITLE_ROUTING_FIELDS.internal_notes)
        ),
        `[${nowIso()}] Routed to title company${title_company.company_name ? ` (${title_company.company_name})` : ""}. Sent to ${title_company.email}.`,
        package_record?.ok
          ? `[${nowIso()}] Title package archived at ${package_record.manifest_key}${package_record.manifest_access_url ? ` (${package_record.manifest_access_url})` : ""}.`
          : `[${nowIso()}] Title package archive unavailable: ${clean(package_record?.reason) || "not_configured"}.`
      ),
    });

    if (resolved_closing_item?.item_id) {
      await updateClosingItem(resolved_closing_item.item_id, {
        [CLOSING_FIELDS.pre_close_notes]: appendNote(
          clean(getFieldValue(resolved_closing_item, CLOSING_FIELDS.pre_close_notes)),
          `[${nowIso()}] Title intro sent to ${title_company.email}.${package_record?.primary_file?.access_url ? ` Package: ${package_record.primary_file.access_url}` : ""}`
        ),
      });
    }

    pipeline = await syncPipelineState({
      contract_item_id: linked_contract_item_id || null,
      title_routing_item_id: resolved_title_routing_item_id,
      closing_item_id: resolved_closing_item?.item_id || null,
      notes: `Title intro sent to ${title_company.email}.`,
    });
  }

  return {
    ok: true,
    sent: !dry_run,
    drafted: false,
    reason: dry_run ? "title_intro_dry_run_ready" : "title_intro_sent",
    title_routing_item_id: resolved_title_routing_item_id,
    closing_item_id: resolved_closing_item?.item_id || null,
    title_company_email: title_company.email || null,
    subject: email_with_package.subject,
    body: email_with_package.body,
    pipeline,
    send_result,
    draft_result: send_result,
    package_record,
  };
}

export default maybeSendTitleIntro;
