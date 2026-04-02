import ENV from "@/lib/config/env.js";
import FEATURE_FLAGS from "@/lib/config/feature-flags.js";
import {
  CONTRACT_FIELDS,
  findContractItems,
  getContractItem,
} from "@/lib/podio/apps/contracts.js";
import { getEmailItem } from "@/lib/podio/apps/emails.js";
import {
  MASTER_OWNER_FIELDS,
  getMasterOwnerItem,
} from "@/lib/podio/apps/master-owners.js";
import {
  OFFER_FIELDS,
  findOfferItems,
} from "@/lib/podio/apps/offers.js";
import {
  TITLE_ROUTING_FIELDS,
  findTitleRoutingItems,
} from "@/lib/podio/apps/title-routing.js";
import { CLOSING_FIELDS, findClosingItems, getClosingItem } from "@/lib/podio/apps/closings.js";
import {
  getCategoryValue,
  getFirstAppReferenceId,
  getTextValue,
} from "@/lib/providers/podio.js";
import { child } from "@/lib/logging/logger.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { maybeCreateContractFromAcceptedOffer } from "@/lib/domain/contracts/maybe-create-contract-from-accepted-offer.js";
import { maybeSendContractForSigning } from "@/lib/domain/contracts/maybe-send-contract-for-signing.js";
import { maybeCreateTitleRoutingFromSignedContract } from "@/lib/domain/title/maybe-create-title-routing-from-signed-contract.js";
import { selectTitleCompany } from "@/lib/domain/title/select-title-company.js";
import { maybeSendTitleIntro } from "@/lib/domain/title/maybe-send-title-intro.js";
import { maybeCreateClosingFromTitleRouting } from "@/lib/domain/closings/maybe-create-closing-from-title-routing.js";
import { createDealRevenueFromClosedClosing } from "@/lib/domain/revenue/create-deal-revenue-from-closed-closing.js";
import { createBuyerMatchFlow } from "@/lib/flows/create-buyer-match-flow.js";
import { sendBuyerBlast } from "@/lib/domain/buyers/send-buyer-blast.js";

const logger = child({
  module: "domain.autopilot.run_deals_autopilot",
});

function clean(value) {
  return String(value ?? "").trim();
}

function sortNewestFirst(items = []) {
  return [...items].sort((a, b) => Number(b?.item_id || 0) - Number(a?.item_id || 0));
}

function takeNewest(items = [], limit = 25) {
  return sortNewestFirst(items).slice(0, limit);
}

function countReasons(results = []) {
  const counts = new Map();

  for (const result of results) {
    const reason = clean(result?.reason) || "unknown";
    counts.set(reason, (counts.get(reason) || 0) + 1);
  }

  return [...counts.entries()]
    .map(([reason, count]) => ({ reason, count }))
    .sort((left, right) => right.count - left.count);
}

function summarizeStage({
  stage,
  enabled = true,
  results = [],
  scanned_count = 0,
  fatal_error = null,
}) {
  const processed_count = results.length;

  return {
    stage,
    enabled,
    ok: !fatal_error,
    scanned_count,
    processed_count,
    created_count: results.filter((result) => Boolean(result?.created)).length,
    updated_count: results.filter((result) => Boolean(result?.updated)).length,
    sent_count: results.filter((result) => Boolean(result?.sent)).length,
    error_count: results.filter((result) => result?.ok === false).length,
    reason_counts: countReasons(results),
    fatal_error: fatal_error
      ? {
          message: fatal_error?.message || "unknown_error",
        }
      : null,
    results,
  };
}

function disabledStage(stage, reason) {
  return summarizeStage({
    stage,
    enabled: false,
    results: [
      {
        ok: true,
        reason,
      },
    ],
  });
}

function isBuyerDispositionReady(contract_item = null) {
  const status = clean(
    getCategoryValue(contract_item, CONTRACT_FIELDS.contract_status, "")
  ).toLowerCase();

  return [
    "fully executed",
    "sent to title",
    "opened",
    "clear to close",
  ].includes(status);
}

async function maybeAutoSendContract({
  contract_item = null,
  dry_run = false,
} = {}) {
  if (!FEATURE_FLAGS.ENABLE_AUTO_CONTRACT_SEND) {
    return {
      ok: true,
      attempted: false,
      sent: false,
      reason: "auto_contract_send_disabled",
    };
  }

  const template_id = clean(ENV.DOCUSIGN_DEFAULT_TEMPLATE_ID);
  if (!template_id) {
    return {
      ok: false,
      attempted: false,
      sent: false,
      reason: "missing_docusign_default_template_id",
    };
  }

  const email_item_id = getFirstAppReferenceId(contract_item, CONTRACT_FIELDS.email, null);
  const master_owner_id = getFirstAppReferenceId(
    contract_item,
    CONTRACT_FIELDS.master_owner,
    null
  );

  const [email_item, master_owner_item] = await Promise.all([
    email_item_id ? getEmailItem(email_item_id) : Promise.resolve(null),
    master_owner_id ? getMasterOwnerItem(master_owner_id) : Promise.resolve(null),
  ]);

  const seller_email =
    clean(getTextValue(email_item, "email", "")) ||
    clean(getTextValue(email_item, "title", ""));
  const seller_name =
    clean(getTextValue(master_owner_item, MASTER_OWNER_FIELDS.owner_full_name, "")) ||
    clean(master_owner_item?.title) ||
    "Seller";

  if (!seller_email) {
    return {
      ok: false,
      attempted: false,
      sent: false,
      reason: "missing_seller_email",
    };
  }

  const signers = [
    {
      name: seller_name,
      email: seller_email,
      role_name: clean(ENV.DOCUSIGN_SELLER_ROLE_NAME) || "Seller",
    },
  ];

  if (clean(ENV.DOCUSIGN_BUYER_EMAIL) && clean(ENV.DOCUSIGN_BUYER_NAME)) {
    signers.push({
      name: clean(ENV.DOCUSIGN_BUYER_NAME),
      email: clean(ENV.DOCUSIGN_BUYER_EMAIL),
      role_name: clean(ENV.DOCUSIGN_BUYER_ROLE_NAME) || "Buyer",
    });
  }

  return maybeSendContractForSigning({
    contract: contract_item,
    template_id,
    signers,
    auto_send: true,
    dry_run,
  });
}

async function runOfferStage({ scan_limit, dry_run }) {
  if (!FEATURE_FLAGS.ENABLE_AUTO_CONTRACT_FLOW) {
    return disabledStage("offers_to_contracts", "auto_contract_flow_disabled");
  }

  try {
    const offer_items = takeNewest(await findOfferItems({}, scan_limit, 0), scan_limit);
    const results = [];

    for (const offer_item of offer_items) {
      try {
        const contract_result = await maybeCreateContractFromAcceptedOffer({
          offer_item,
          offer_item_id: offer_item.item_id,
          auto_send: false,
          dry_run,
        });

        const contract_item_id = contract_result?.contract_item_id || null;
        let send_result = null;

        if (contract_item_id) {
          const contract_item = await getContractItem(contract_item_id);
          if (contract_item?.item_id) {
            send_result = await maybeAutoSendContract({
              contract_item,
              dry_run,
            });
          }
        }

        results.push({
          ok: contract_result?.ok !== false && (send_result?.ok !== false || !send_result),
          offer_item_id: offer_item.item_id,
          offer_status: getCategoryValue(offer_item, OFFER_FIELDS.offer_status, null),
          contract_item_id,
          created: Boolean(contract_result?.created),
          sent: Boolean(send_result?.sent),
          reason: clean(contract_result?.reason) || "offer_processed",
          send_reason: clean(send_result?.reason) || null,
        });
      } catch (error) {
        logger.warn("autopilot.offer_failed", {
          offer_item_id: offer_item?.item_id || null,
          error: error?.message || "unknown_error",
        });

        results.push({
          ok: false,
          offer_item_id: offer_item?.item_id || null,
          reason: error?.message || "offer_stage_failed",
        });
      }
    }

    return summarizeStage({
      stage: "offers_to_contracts",
      scanned_count: offer_items.length,
      results,
    });
  } catch (fatal_error) {
    logger.warn("autopilot.offer_stage_failed", {
      error: fatal_error?.message || "unknown_error",
    });

    return summarizeStage({
      stage: "offers_to_contracts",
      scanned_count: 0,
      results: [],
      fatal_error,
    });
  }
}

async function runContractStage({ scan_limit, contract_item_id = null }) {
  if (!FEATURE_FLAGS.ENABLE_AUTO_TITLE_ROUTING) {
    return disabledStage("contracts_to_title", "auto_title_routing_disabled");
  }

  try {
    const contract_items = contract_item_id
      ? [await getContractItem(contract_item_id)].filter((item) => item?.item_id)
      : takeNewest(await findContractItems({}, scan_limit, 0), scan_limit);
    const results = [];

    for (const contract_item of contract_items) {
      try {
        const title_routing = await maybeCreateTitleRoutingFromSignedContract({
          contract_item,
          contract_item_id: contract_item.item_id,
          source: "Deals Autopilot Sweep",
        });

        results.push({
          ok: title_routing?.ok !== false,
          contract_item_id: contract_item.item_id,
          contract_status: getCategoryValue(
            contract_item,
            CONTRACT_FIELDS.contract_status,
            null
          ),
          title_routing_item_id:
            title_routing?.title_routing_item_id ||
            title_routing?.existing_title_routing?.item_id ||
            null,
          created: Boolean(title_routing?.created),
          reason: clean(title_routing?.reason) || "contract_processed",
        });
      } catch (error) {
        logger.warn("autopilot.contract_failed", {
          contract_item_id: contract_item?.item_id || null,
          error: error?.message || "unknown_error",
        });

        results.push({
          ok: false,
          contract_item_id: contract_item?.item_id || null,
          reason: error?.message || "contract_stage_failed",
        });
      }
    }

    return summarizeStage({
      stage: "contracts_to_title",
      scanned_count: contract_items.length,
      results,
    });
  } catch (fatal_error) {
    logger.warn("autopilot.contract_stage_failed", {
      error: fatal_error?.message || "unknown_error",
    });

    return summarizeStage({
      stage: "contracts_to_title",
      scanned_count: 0,
      results: [],
      fatal_error,
    });
  }
}

async function runTitleRoutingStage({
  scan_limit,
  dry_run,
  contract_item_id = null,
}) {
  const company_assignment_enabled = FEATURE_FLAGS.ENABLE_AUTO_TITLE_COMPANY_ASSIGNMENT;
  const closing_enabled = FEATURE_FLAGS.ENABLE_AUTO_CLOSING_FLOW;
  const intro_enabled = FEATURE_FLAGS.ENABLE_AUTO_TITLE_INTRO;

  if (!company_assignment_enabled && !closing_enabled && !intro_enabled) {
    return disabledStage("title_to_closing", "title_stage_automation_disabled");
  }

  try {
    const title_routing_items = contract_item_id
      ? takeNewest(
          await findTitleRoutingItems(
            { [TITLE_ROUTING_FIELDS.contract]: contract_item_id },
            scan_limit,
            0
          ),
          scan_limit
        )
      : takeNewest(await findTitleRoutingItems({}, scan_limit, 0), scan_limit);
    const results = [];

    for (const title_routing_item of title_routing_items) {
      try {
        const assign_result = company_assignment_enabled
          ? await selectTitleCompany({
              title_routing_id: title_routing_item.item_id,
            })
          : {
              ok: true,
              selected: false,
              reason: "auto_title_company_assignment_disabled",
            };

        const closing_result = closing_enabled
          ? await maybeCreateClosingFromTitleRouting({
              title_routing_item,
              title_routing_item_id: title_routing_item.item_id,
              source: "Deals Autopilot Sweep",
            })
          : {
              ok: true,
              created: false,
              reason: "auto_closing_flow_disabled",
            };

        const closing_item_id =
          closing_result?.closing_item_id ||
          closing_result?.existing_closing?.item_id ||
          null;

        const title_intro_result = intro_enabled
          ? await maybeSendTitleIntro({
              title_routing_item_id: title_routing_item.item_id,
              closing_item_id,
              dry_run,
            })
          : {
              ok: true,
              sent: false,
              reason: "auto_title_intro_disabled",
            };

        results.push({
          ok:
            assign_result?.ok !== false &&
            closing_result?.ok !== false &&
            title_intro_result?.ok !== false,
          title_routing_item_id: title_routing_item.item_id,
          routing_status: getCategoryValue(
            title_routing_item,
            TITLE_ROUTING_FIELDS.routing_status,
            null
          ),
          title_company_item_id: assign_result?.title_company_item_id || null,
          closing_item_id,
          created: Boolean(closing_result?.created),
          sent: Boolean(title_intro_result?.sent),
          reason: clean(title_intro_result?.reason || closing_result?.reason) || "title_processed",
          assignment_reason: clean(assign_result?.reason) || null,
          closing_reason: clean(closing_result?.reason) || null,
          title_intro_reason: clean(title_intro_result?.reason) || null,
        });
      } catch (error) {
        logger.warn("autopilot.title_routing_failed", {
          title_routing_item_id: title_routing_item?.item_id || null,
          error: error?.message || "unknown_error",
        });

        results.push({
          ok: false,
          title_routing_item_id: title_routing_item?.item_id || null,
          reason: error?.message || "title_routing_stage_failed",
        });
      }
    }

    return summarizeStage({
      stage: "title_to_closing",
      scanned_count: title_routing_items.length,
      results,
    });
  } catch (fatal_error) {
    logger.warn("autopilot.title_routing_stage_failed", {
      error: fatal_error?.message || "unknown_error",
    });

    return summarizeStage({
      stage: "title_to_closing",
      scanned_count: 0,
      results: [],
      fatal_error,
    });
  }
}

async function runBuyerStage({
  scan_limit,
  dry_run,
  contract_item_id = null,
}) {
  if (!FEATURE_FLAGS.ENABLE_AUTO_BUYER_MATCH) {
    return disabledStage("contracts_to_buyers", "auto_buyer_match_disabled");
  }

  try {
    const contract_items = contract_item_id
      ? [await getContractItem(contract_item_id)].filter((item) => item?.item_id)
      : takeNewest(await findContractItems({}, scan_limit, 0), scan_limit);
    const results = [];

    for (const contract_item of contract_items) {
      const contract_status = clean(
        getCategoryValue(contract_item, CONTRACT_FIELDS.contract_status, null)
      );

      if (!isBuyerDispositionReady(contract_item)) {
        results.push({
          ok: true,
          contract_item_id: contract_item?.item_id || null,
          contract_status,
          created: false,
          updated: false,
          sent: false,
          reason: "contract_not_dispo_ready",
        });
        continue;
      }

      try {
        const buyer_match_result = await createBuyerMatchFlow({
          contract_id: contract_item.item_id,
          dry_run,
          candidate_limit: 10,
        });

        let blast_result = {
          ok: true,
          sent: false,
          reason: "auto_buyer_blast_disabled",
        };

        if (
          buyer_match_result?.ok !== false &&
          buyer_match_result?.buyer_match_item_id &&
          FEATURE_FLAGS.ENABLE_AUTO_BUYER_BLAST &&
          FEATURE_FLAGS.ENABLE_LIVE_SENDING &&
          !dry_run
        ) {
          blast_result = await sendBuyerBlast({
            buyer_match_id: buyer_match_result.buyer_match_item_id,
            dry_run: false,
            max_buyers: 5,
            force: false,
          });
        } else if (
          buyer_match_result?.ok !== false &&
          buyer_match_result?.buyer_match_item_id &&
          dry_run
        ) {
          blast_result = await sendBuyerBlast({
            buyer_match_id: buyer_match_result.buyer_match_item_id,
            dry_run: true,
            max_buyers: 5,
            force: false,
          });
        }

        results.push({
          ok: buyer_match_result?.ok !== false && blast_result?.ok !== false,
          contract_item_id: contract_item.item_id,
          contract_status,
          buyer_match_item_id: buyer_match_result?.buyer_match_item_id || null,
          created: Boolean(buyer_match_result?.created),
          updated: Boolean(buyer_match_result?.updated),
          sent: Boolean(blast_result?.sent),
          reason: clean(buyer_match_result?.reason) || "buyer_match_processed",
          blast_reason: clean(blast_result?.reason) || null,
        });
      } catch (error) {
        logger.warn("autopilot.buyer_match_failed", {
          contract_item_id: contract_item?.item_id || null,
          error: error?.message || "unknown_error",
        });

        results.push({
          ok: false,
          contract_item_id: contract_item?.item_id || null,
          contract_status,
          reason: error?.message || "buyer_stage_failed",
        });
      }
    }

    return summarizeStage({
      stage: "contracts_to_buyers",
      scanned_count: contract_items.length,
      results,
    });
  } catch (fatal_error) {
    logger.warn("autopilot.buyer_stage_failed", {
      error: fatal_error?.message || "unknown_error",
    });

    return summarizeStage({
      stage: "contracts_to_buyers",
      scanned_count: 0,
      results: [],
      fatal_error,
    });
  }
}

async function runClosingStage({ scan_limit, contract_item_id = null }) {
  if (!FEATURE_FLAGS.ENABLE_AUTO_REVENUE_SYNC) {
    return disabledStage("closings_to_revenue", "auto_revenue_sync_disabled");
  }

  try {
    const closing_items = contract_item_id
      ? takeNewest(
          await findClosingItems(
            { [CLOSING_FIELDS.contract]: contract_item_id },
            scan_limit,
            0
          ),
          scan_limit
        )
      : takeNewest(await findClosingItems({}, scan_limit, 0), scan_limit);
    const results = [];

    for (const closing_item of closing_items) {
      try {
        const revenue_result = await createDealRevenueFromClosedClosing({
          closing_item,
          closing_item_id: closing_item.item_id,
        });

        results.push({
          ok: revenue_result?.ok !== false,
          closing_item_id: closing_item.item_id,
          created: Boolean(revenue_result?.created),
          deal_revenue_item_id: revenue_result?.deal_revenue_item_id || null,
          reason: clean(revenue_result?.reason) || "closing_processed",
        });
      } catch (error) {
        logger.warn("autopilot.closing_failed", {
          closing_item_id: closing_item?.item_id || null,
          error: error?.message || "unknown_error",
        });

        results.push({
          ok: false,
          closing_item_id: closing_item?.item_id || null,
          reason: error?.message || "closing_stage_failed",
        });
      }
    }

    return summarizeStage({
      stage: "closings_to_revenue",
      scanned_count: closing_items.length,
      results,
    });
  } catch (fatal_error) {
    logger.warn("autopilot.closing_stage_failed", {
      error: fatal_error?.message || "unknown_error",
    });

    return summarizeStage({
      stage: "closings_to_revenue",
      scanned_count: 0,
      results: [],
      fatal_error,
    });
  }
}

export async function runDealsAutopilot({
  scan_limit = Number(ENV.DEALS_AUTOPILOT_SCAN_LIMIT || 25) || 25,
  dry_run = false,
  contract_item_id = null,
} = {}) {
  const started_at = new Date().toISOString();
  const scoped_contract_item_id = Number(contract_item_id || 0) || null;

  logger.info("autopilot.run_started", {
    scan_limit,
    dry_run,
    contract_item_id: scoped_contract_item_id,
  });

  const offers = scoped_contract_item_id
    ? disabledStage("offers_to_contracts", "contract_scope_active_offers_stage_skipped")
    : await runOfferStage({
        scan_limit,
        dry_run,
      });
  const contracts = await runContractStage({
    scan_limit,
    contract_item_id: scoped_contract_item_id,
  });
  const buyers = await runBuyerStage({
    scan_limit,
    dry_run,
    contract_item_id: scoped_contract_item_id,
  });
  const title_routing = await runTitleRoutingStage({
    scan_limit,
    dry_run,
    contract_item_id: scoped_contract_item_id,
  });
  const closings = await runClosingStage({
    scan_limit,
    contract_item_id: scoped_contract_item_id,
  });

  const stages = [offers, contracts, buyers, title_routing, closings];
  const completed_at = new Date().toISOString();

  const result = {
    ok: stages.every((stage) => stage.ok),
    started_at,
    completed_at,
    dry_run,
    scan_limit,
    contract_item_id: scoped_contract_item_id,
    summary: {
      stages_total: stages.length,
      stages_ok: stages.filter((stage) => stage.ok).length,
      scanned_count: stages.reduce((sum, stage) => sum + (stage.scanned_count || 0), 0),
      processed_count: stages.reduce((sum, stage) => sum + (stage.processed_count || 0), 0),
      created_count: stages.reduce((sum, stage) => sum + (stage.created_count || 0), 0),
      updated_count: stages.reduce((sum, stage) => sum + (stage.updated_count || 0), 0),
      sent_count: stages.reduce((sum, stage) => sum + (stage.sent_count || 0), 0),
      error_count: stages.reduce((sum, stage) => sum + (stage.error_count || 0), 0),
    },
    stages: {
      offers,
      contracts,
      buyers,
      title_routing,
      closings,
    },
  };

  if (!dry_run && result.summary.error_count > 0) {
    await recordSystemAlert({
      subsystem: "autopilot",
      code: "stage_errors",
      severity: "high",
      retryable: true,
      summary: `Deals autopilot completed with ${result.summary.error_count} stage error(s).`,
      dedupe_key: scoped_contract_item_id
        ? `autopilot:${scoped_contract_item_id}`
        : "autopilot",
      metadata: {
        scan_limit,
        contract_item_id: scoped_contract_item_id,
        error_count: result.summary.error_count,
        processed_count: result.summary.processed_count,
      },
    });
  } else if (!dry_run) {
    await resolveSystemAlert({
      subsystem: "autopilot",
      code: "stage_errors",
      dedupe_key: scoped_contract_item_id
        ? `autopilot:${scoped_contract_item_id}`
        : "autopilot",
      resolution_message: "Deals autopilot completed without stage errors.",
    });
  }

  logger.info("autopilot.run_completed", result.summary);

  return result;
}

export default runDealsAutopilot;
