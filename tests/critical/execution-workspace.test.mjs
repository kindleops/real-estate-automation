import test, { afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  CONTRACT_TEMPLATE_FIELDS,
} from "@/lib/podio/apps/contract-templates.js";
import {
  CONTRACT_FIELDS,
} from "@/lib/podio/apps/contracts.js";
import {
  TITLE_ROUTING_FIELDS,
} from "@/lib/podio/apps/title-routing.js";
import {
  CLOSING_FIELDS,
} from "@/lib/podio/apps/closings.js";
import {
  BUYER_MATCH_FIELDS,
} from "@/lib/podio/apps/buyer-match.js";
import {
  DEAL_REVENUE_FIELDS,
} from "@/lib/podio/apps/deal-revenue.js";
import {
  PIPELINE_FIELDS,
} from "@/lib/podio/apps/pipelines.js";
import {
  __resetContractTemplateResolverTestDeps,
  __setContractTemplateResolverTestDeps,
  resolveContractTemplate,
} from "@/lib/domain/contracts/resolve-contract-template.js";
import {
  __resetMaybeSendContractForSigningTestDeps,
  __setMaybeSendContractForSigningTestDeps,
  maybeSendContractForSigning,
} from "@/lib/domain/contracts/maybe-send-contract-for-signing.js";
import {
  __resetCreateTitleRoutingFromContractTestDeps,
  __setCreateTitleRoutingFromContractTestDeps,
  createTitleRoutingFromContract,
} from "@/lib/domain/title/create-title-routing-from-contract.js";
import {
  __resetTitleRoutingStatusTestDeps,
  __setTitleRoutingStatusTestDeps,
  updateTitleRoutingStatus,
} from "@/lib/domain/title/update-title-routing-status.js";
import {
  __resetClosingStatusTestDeps,
  __setClosingStatusTestDeps,
  updateClosingStatus,
} from "@/lib/domain/closings/update-closing-status.js";
import {
  __resetDealRevenueFromClosingTestDeps,
  __setDealRevenueFromClosingTestDeps,
  createDealRevenueFromClosedClosing,
} from "@/lib/domain/revenue/create-deal-revenue-from-closed-closing.js";
import { buildPipelinePayload } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import {
  appRefField,
  categoryField,
  createPodioItem,
  dateField,
  numberField,
  phoneField,
  textField,
} from "../helpers/test-helpers.js";

afterEach(() => {
  __resetContractTemplateResolverTestDeps();
  __resetMaybeSendContractForSigningTestDeps();
  __resetCreateTitleRoutingFromContractTestDeps();
  __resetTitleRoutingStatusTestDeps();
  __resetClosingStatusTestDeps();
  __resetDealRevenueFromClosingTestDeps();
});

test("contract template resolution prefers live active auto-generation defaults with an exact state/type match", async () => {
  __setContractTemplateResolverTestDeps({
    findContractTemplates: async () => [
      createPodioItem(101, {
        [CONTRACT_TEMPLATE_FIELDS.title]: textField("TX Cash Default"),
        [CONTRACT_TEMPLATE_FIELDS.state]: categoryField("TX"),
        [CONTRACT_TEMPLATE_FIELDS.contract_type]: categoryField("Cash"),
        [CONTRACT_TEMPLATE_FIELDS.template_type]: categoryField("Standard Purchase"),
        [CONTRACT_TEMPLATE_FIELDS.active]: categoryField("Yes"),
        [CONTRACT_TEMPLATE_FIELDS.use_for_auto_generation]: categoryField("Yes"),
        [CONTRACT_TEMPLATE_FIELDS.template_status]: categoryField("Active"),
        [CONTRACT_TEMPLATE_FIELDS.default_for_state_type]: categoryField("Yes"),
        [CONTRACT_TEMPLATE_FIELDS.priority]: numberField(90),
        [CONTRACT_TEMPLATE_FIELDS.docusign_template_id]: textField("tmpl-tx-cash"),
      }),
      createPodioItem(102, {
        [CONTRACT_TEMPLATE_FIELDS.title]: textField("Fallback Wildcard"),
        [CONTRACT_TEMPLATE_FIELDS.contract_type]: categoryField("Cash"),
        [CONTRACT_TEMPLATE_FIELDS.template_type]: categoryField("Standard Purchase"),
        [CONTRACT_TEMPLATE_FIELDS.active]: categoryField("Yes"),
        [CONTRACT_TEMPLATE_FIELDS.use_for_auto_generation]: categoryField("Yes"),
        [CONTRACT_TEMPLATE_FIELDS.template_status]: categoryField("Active"),
        [CONTRACT_TEMPLATE_FIELDS.priority]: numberField(40),
        [CONTRACT_TEMPLATE_FIELDS.docusign_template_id]: textField("tmpl-fallback"),
      }),
    ],
  });

  const result = await resolveContractTemplate({
    contract_item: createPodioItem(5001, {
      [CONTRACT_FIELDS.state]: categoryField("TX"),
      [CONTRACT_FIELDS.contract_type]: categoryField("Cash"),
      [CONTRACT_FIELDS.template_type]: categoryField("Standard Purchase"),
      [CONTRACT_FIELDS.assignment_allowed]: categoryField("No"),
    }),
  });

  assert.equal(result.ok, true);
  assert.equal(result.template_item_id, 101);
  assert.equal(result.docusign_template_id, "tmpl-tx-cash");
  assert.equal(result.diagnostics.chosen.used_state_fallback, false);
});

test("contract send writes a deterministic pipeline blocker when no usable template exists", async () => {
  let pipelinePayload = null;

  __setMaybeSendContractForSigningTestDeps({
    resolveContractTemplate: async () => ({
      ok: false,
      reason: "no_usable_contract_template_found",
    }),
    syncPipelineState: async (payload) => {
      pipelinePayload = payload;
      return { ok: true, pipeline_item_id: 9101 };
    },
  });

  const result = await maybeSendContractForSigning({
    contract: createPodioItem(6001, {
      [CONTRACT_FIELDS.contract_status]: categoryField("Draft"),
    }),
    signers: [
      {
        name: "Seller One",
        email: "seller@example.com",
        role_name: "Seller",
      },
    ],
    documents: [],
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "missing_documents_or_template");
  assert.equal(pipelinePayload.contract_item_id, 6001);
  assert.equal(pipelinePayload.current_engine, "Contracts");
  assert.equal(pipelinePayload.blocked, "Yes");
  assert.equal(pipelinePayload.next_system_action, "resolve_contract_template");
});

test("title routing creation backfills title-company contact fields and writes the contract linkback", async () => {
  let createdPayload = null;
  let contractUpdatePayload = null;
  let contractSyncArgs = null;

  __setCreateTitleRoutingFromContractTestDeps({
    createTitleRoutingItem: async (payload) => {
      createdPayload = payload;
      return { item_id: 7101 };
    },
    getTitleCompanyItem: async () =>
      createPodioItem(801, {
        "contact-manager": textField("Jane Closer"),
        "new-order-email": textField("title@example.com"),
        phone: phoneField("2145551212"),
      }),
    updateContractItem: async (_item_id, payload) => {
      contractUpdatePayload = payload;
      return { ok: true };
    },
    syncContractStatus: async (args) => {
      contractSyncArgs = args;
      return { ok: true, updated: true, contract_status: "Sent To Title" };
    },
    syncPipelineState: async () => ({ ok: true, current_stage: "Routed to Title" }),
  });

  const result = await createTitleRoutingFromContract({
    contract_item: createPodioItem(7001, {
      [CONTRACT_FIELDS.master_owner]: appRefField(11),
      [CONTRACT_FIELDS.prospect]: appRefField(12),
      [CONTRACT_FIELDS.property]: appRefField(13),
      [CONTRACT_FIELDS.assigned_agent]: appRefField(14),
      [CONTRACT_FIELDS.market]: appRefField(15),
      [CONTRACT_FIELDS.title_company]: appRefField(801),
      [CONTRACT_FIELDS.closing_date_target]: dateField("2026-05-10T00:00:00.000Z"),
    }),
    routing_status: "Routed",
  });

  assert.equal(result.ok, true);
  assert.equal(createdPayload[TITLE_ROUTING_FIELDS.primary_title_contact], "Jane Closer");
  assert.equal(createdPayload[TITLE_ROUTING_FIELDS.title_contact_email], "title@example.com");
  assert.equal(createdPayload[TITLE_ROUTING_FIELDS.title_contact_phone], "2145551212");
  assert.ok(createdPayload[TITLE_ROUTING_FIELDS.file_routed_date]?.start);
  assert.deepEqual(contractUpdatePayload[TITLE_ROUTING_FIELDS.contract], undefined);
  assert.deepEqual(contractUpdatePayload[CONTRACT_FIELDS.title_routing], [7101]);
  assert.equal(contractSyncArgs.status, "Sent To Title");
});

test("title routing creation surfaces a deterministic blocker when the title company is missing", async () => {
  let pipelinePayload = null;

  __setCreateTitleRoutingFromContractTestDeps({
    createTitleRoutingItem: async () => ({ item_id: 7201 }),
    updateContractItem: async () => ({ ok: true }),
    syncContractStatus: async () => ({ ok: true, updated: true }),
    syncPipelineState: async (payload) => {
      pipelinePayload = payload;
      return { ok: true, current_stage: "Routed to Title" };
    },
  });

  const result = await createTitleRoutingFromContract({
    contract_item: createPodioItem(7002, {
      [CONTRACT_FIELDS.property]: appRefField(22),
      [CONTRACT_FIELDS.master_owner]: appRefField(23),
    }),
    routing_status: "Routed",
  });

  assert.equal(result.ok, true);
  assert.equal(pipelinePayload.blocked, "Yes");
  assert.equal(pipelinePayload.next_system_action, "assign_title_company");
});

test("title routing clear-to-close syncs the contract and ensures the closing bridge", async () => {
  let closingArgs = null;
  let contractArgs = null;

  __setTitleRoutingStatusTestDeps({
    updateTitleRoutingItem: async () => ({ ok: true }),
    syncContractStatus: async (args) => {
      contractArgs = args;
      return { ok: true, updated: true };
    },
    maybeCreateClosingFromTitleRouting: async (args) => {
      closingArgs = args;
      return { ok: true, created: true, closing_item_id: 8301 };
    },
    syncPipelineState: async () => ({ ok: true, current_stage: "Clear to Close" }),
    updateBrainFromExecution: async () => ({ ok: true, updated: true }),
  });

  const result = await updateTitleRoutingStatus({
    title_routing_item: createPodioItem(8001, {
      [TITLE_ROUTING_FIELDS.contract]: appRefField(9001),
      [TITLE_ROUTING_FIELDS.expected_closing_date]: dateField("2026-05-20T00:00:00.000Z"),
    }),
    status: "Clear to Close",
  });

  assert.equal(result.ok, true);
  assert.equal(contractArgs.status, "Clear To Close");
  assert.equal(closingArgs.closing_status, "Scheduled");
  assert.equal(result.closing_sync.closing_item_id, 8301);
});

test("closing completion syncs contract/title and triggers deal revenue creation", async () => {
  let contractArgs = null;
  let titleArgs = null;
  let revenueArgs = null;

  __setClosingStatusTestDeps({
    updateClosingItem: async () => ({ ok: true }),
    syncContractStatus: async (args) => {
      contractArgs = args;
      return { ok: true, updated: true };
    },
    updateTitleRoutingStatus: async (args) => {
      titleArgs = args;
      return { ok: true, updated: true };
    },
    createDealRevenueFromClosedClosing: async (args) => {
      revenueArgs = args;
      return { ok: true, created: true, deal_revenue_item_id: 9401 };
    },
    syncPipelineState: async () => ({ ok: true, current_stage: "Closed" }),
    updateBrainFromExecution: async () => ({ ok: true, updated: true }),
  });

  const result = await updateClosingStatus({
    closing_item: createPodioItem(9002, {
      [CLOSING_FIELDS.contract]: appRefField(9003),
      [CLOSING_FIELDS.title_routing]: appRefField(9004),
    }),
    status: "Completed",
  });

  assert.equal(result.ok, true);
  assert.equal(contractArgs.status, "Closed");
  assert.equal(titleArgs.status, "Closed");
  assert.equal(revenueArgs.closing_item_id, 9002);
});

test("deal revenue creation derives pricing and selected buyer from the linked contract and buyer match", async () => {
  let createPayload = null;
  let buyerMatchUpdate = null;

  __setDealRevenueFromClosingTestDeps({
    findDealRevenueItems: async () => [],
    getContractItem: async () =>
      createPodioItem(1001, {
        [CONTRACT_FIELDS.purchase_price_final]: { value: { value: 140000 } },
      }),
    getBuyerMatchItem: async () =>
      createPodioItem(2001, {
        [BUYER_MATCH_FIELDS.final_disposition_price]: numberField(165000),
        [BUYER_MATCH_FIELDS.assignment_fee]: numberField(25000),
        [BUYER_MATCH_FIELDS.selected_buyer]: appRefField(777),
      }),
    createDealRevenueItem: async (payload) => {
      createPayload = payload;
      return { item_id: 9501 };
    },
    updateBuyerMatchItem: async (_item_id, payload) => {
      buyerMatchUpdate = payload;
      return { ok: true };
    },
    syncPipelineState: async () => ({ ok: true, current_stage: "Closed" }),
    updateBrainFromExecution: async () => ({ ok: true, updated: true }),
  });

  const result = await createDealRevenueFromClosedClosing({
    closing_item: createPodioItem(9001, {
      [CLOSING_FIELDS.closing_status]: categoryField("Completed"),
      [CLOSING_FIELDS.contract]: appRefField(1001),
      [CLOSING_FIELDS.buyer_match]: appRefField(2001),
      [CLOSING_FIELDS.master_owner]: appRefField(3001),
      [CLOSING_FIELDS.property]: appRefField(4001),
      [CLOSING_FIELDS.title_company]: appRefField(5001),
      [CLOSING_FIELDS.market]: appRefField(6001),
      [CLOSING_FIELDS.actual_closing_date]: dateField("2026-05-11T00:00:00.000Z"),
    }),
  });

  assert.equal(result.ok, true);
  assert.equal(createPayload[DEAL_REVENUE_FIELDS.purchase_price], 140000);
  assert.equal(createPayload[DEAL_REVENUE_FIELDS.sold_price], 165000);
  assert.equal(createPayload[DEAL_REVENUE_FIELDS.assignment_fee], 25000);
  assert.deepEqual(createPayload[DEAL_REVENUE_FIELDS.buyer], [777]);
  assert.deepEqual(buyerMatchUpdate[BUYER_MATCH_FIELDS.deal_revenue], [9501]);
});

test("pipeline payload honors live contract execution statuses without regressing a closed deal", () => {
  const routed = buildPipelinePayload({
    records: {
      contract_item: createPodioItem(1, {
        [CONTRACT_FIELDS.contract_status]: categoryField("Sent To Title"),
      }),
    },
    identifiers: {},
  });
  const opened = buildPipelinePayload({
    records: {
      contract_item: createPodioItem(2, {
        [CONTRACT_FIELDS.contract_status]: categoryField("Opened"),
      }),
    },
    identifiers: {},
  });
  const clearToClose = buildPipelinePayload({
    records: {
      contract_item: createPodioItem(3, {
        [CONTRACT_FIELDS.contract_status]: categoryField("Clear To Close"),
      }),
    },
    identifiers: {},
  });
  const closed = buildPipelinePayload({
    records: {
      contract_item: createPodioItem(4, {
        [CONTRACT_FIELDS.contract_status]: categoryField("Closed"),
      }),
      title_routing_item: createPodioItem(5, {
        [TITLE_ROUTING_FIELDS.routing_status]: categoryField("Title Reviewing"),
      }),
    },
    identifiers: {},
  });

  assert.equal(routed.current_stage, "Routed to Title");
  assert.equal(opened.current_stage, "Title Reviewing");
  assert.equal(clearToClose.current_stage, "Clear to Close");
  assert.equal(closed.current_stage, "Closed");
  assert.equal(closed.payload[PIPELINE_FIELDS.pipeline_status], "Closed Won");
});
