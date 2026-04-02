import test from "node:test";
import assert from "node:assert/strict";

import {
  buildBuyerBlastContent,
  inferDispositionStrategy,
  inferTargetBuyerTypes,
  rankBuyerCandidates,
} from "@/lib/domain/buyers/match-engine.js";
import {
  buildBuyerSmsBlastText,
  buildPreviewRecipients,
  pickBuyerBlastChannel,
} from "@/lib/domain/buyers/send-buyer-blast.js";
import { buildPipelinePayload } from "@/lib/domain/pipelines/sync-pipeline-state.js";
import {
  BUYER_MATCH_FIELDS,
} from "@/lib/podio/apps/buyer-match.js";
import {
  PIPELINE_FIELDS,
} from "@/lib/podio/apps/pipelines.js";
import {
  TITLE_ROUTING_FIELDS,
} from "@/lib/podio/apps/title-routing.js";
import {
  categoryField,
  createPodioItem,
  numberField,
  textField,
} from "../helpers/test-helpers.js";

test("buyer ranking prefers same-market multifamily buyers for multifamily dispo", () => {
  const context = {
    property_type: "Multi-Family",
    units: 12,
    purchase_price: 925000,
    market_name: "Dallas",
    zip_code: "75201",
    target_buyer_types: inferTargetBuyerTypes({
      property_type: "Multi-Family",
      units: 12,
    }),
    primary_target_type: "Multifamily Buyer",
    market_signals: {
      mf_buyer_density_score: 82,
      hedge_fund_density_score: 71,
      cash_buyer_density_score: 44,
    },
  };

  const ranked = rankBuyerCandidates({
    context,
    limit: 2,
    candidates: [
      {
        item_id: 101,
        company_name: "Metro MF Capital",
        owner_type: "Hedge Fund",
        total_properties_owned: 88,
        estimated_portfolio_value: 12000000,
        contact_summary: {
          email_count: 2,
          phone_count: 1,
          officer_count: 1,
        },
        history: {
          sale_count: 9,
          same_market_count: 5,
          same_zip_count: 2,
          recent_sale_count: 4,
          avg_sale_price: 1300000,
          avg_flip_spread: 12000,
          property_types: ["Multi-Family"],
          property_styles: ["Apartment"],
        },
      },
      {
        item_id: 102,
        company_name: "Starter House Flips",
        owner_type: "Individual",
        total_properties_owned: 3,
        estimated_portfolio_value: 350000,
        contact_summary: {
          email_count: 1,
          phone_count: 0,
          officer_count: 0,
        },
        history: {
          sale_count: 2,
          same_market_count: 0,
          same_zip_count: 0,
          recent_sale_count: 1,
          avg_sale_price: 240000,
          avg_flip_spread: 45000,
          property_types: ["Single Family"],
          property_styles: ["Ranch"],
        },
      },
    ],
  });

  assert.equal(ranked[0]?.company_name, "Metro MF Capital");
  assert.ok(ranked[0]?.score > ranked[1]?.score);
  assert.ok(ranked[0]?.reasons.some((reason) => reason.includes("zip 75201")));
});

test("blast recipient preview can filter to live-eligible buyers only", () => {
  const diagnostics = {
    diagnostics: {
      top_candidates: [
        {
          item_id: 201,
          company_name: "Live Eligible Buyer",
          score: 72,
          reasons: ["recent purchases in Dallas"],
          emails: ["buyer@example.com"],
          phones: ["5551112222"],
        },
        {
          item_id: 202,
          company_name: "Weak No Email Buyer",
          score: 32,
          reasons: ["email contact unavailable"],
          emails: [],
          phones: ["5553334444"],
        },
      ],
    },
  };

  const preview = buildPreviewRecipients(diagnostics, 5);
  const eligible = buildPreviewRecipients(diagnostics, 5, {
    min_score: 45,
    require_email: true,
  });

  assert.equal(preview.length, 2);
  assert.equal(eligible.length, 1);
  assert.equal(eligible[0]?.company_name, "Live Eligible Buyer");
});

test("buyer blast can prefer SMS when the recipient wants texts or only has a phone", () => {
  assert.equal(
    pickBuyerBlastChannel(
      {
        preferred_contact_method: "SMS / Text",
        emails: ["buyer@example.com"],
        phones: ["5551112222"],
      },
      { sms_enabled: true }
    ),
    "sms"
  );

  assert.equal(
    pickBuyerBlastChannel(
      {
        preferred_contact_method: "Email",
        emails: [],
        phones: ["5553334444"],
      },
      { sms_enabled: true }
    ),
    "sms"
  );

  assert.equal(
    pickBuyerBlastChannel(
      {
        preferred_contact_method: "Email",
        emails: ["buyer@example.com"],
        phones: ["5551112222"],
      },
      { sms_enabled: false }
    ),
    "email"
  );
});

test("pipeline payload surfaces buyer match stage from active disposition work", () => {
  const records = {
    buyer_match_item: createPodioItem(301, {
      [BUYER_MATCH_FIELDS.match_status]: categoryField("Sent to Buyers"),
      [BUYER_MATCH_FIELDS.buyer_response_status]: categoryField("Sent"),
      [BUYER_MATCH_FIELDS.buyer_match_score]: numberField(68),
      [BUYER_MATCH_FIELDS.internal_notes]: textField("Top buyers ranked and package ready."),
    }),
  };

  const result = buildPipelinePayload({
    records,
    identifiers: {},
  });

  assert.equal(result.current_stage, "Buyer Match");
  assert.equal(result.current_engine, "Buyer Match");
  assert.equal(result.payload[PIPELINE_FIELDS.current_stage], "Buyer Match");
});

test("pipeline payload lets clear-to-close override stale buyer-match stage", () => {
  const records = {
    buyer_match_item: createPodioItem(302, {
      [BUYER_MATCH_FIELDS.match_status]: categoryField("Assigned"),
      [BUYER_MATCH_FIELDS.assignment_status]: categoryField("Buyer Confirmed"),
    }),
    title_routing_item: createPodioItem(402, {
      [TITLE_ROUTING_FIELDS.routing_status]: categoryField("Clear to Close"),
    }),
  };

  const result = buildPipelinePayload({
    records,
    identifiers: {},
  });

  assert.equal(result.current_stage, "Clear to Close");
  assert.equal(result.current_engine, "Closings");
});

test("buyer blast content stays honest about plain-text-only package delivery", () => {
  const strategy = inferDispositionStrategy({
    offer_type: "Cash",
    property_type: "Single Family",
    units: 1,
  });
  const content = buildBuyerBlastContent({
    context: {
      disposition_strategy: strategy,
      property_type: "Single Family",
      property_address: "123 Main St, Dallas, TX 75201",
      market_name: "Dallas",
      zip_code: "75201",
      purchase_price: 215000,
      estimated_value: 295000,
      closing_date_target: "2026-04-15",
    },
    candidate: {
      company_name: "Buyer One",
      reasons: ["active in zip 75201", "email contact available"],
    },
  });

  assert.match(content.subject, /Assignment|Disposition|Single Family/i);
  assert.match(content.text, /123 Main St/);
  assert.match(content.text, /Acquisition Price: \$215,000/);
  assert.match(content.text, /plain-text opportunity summary only/i);
});

test("buyer SMS blast text stays concise and keeps the package link in-band", () => {
  const sms = buildBuyerSmsBlastText({
    context: {
      property_address: "123 Main St, Dallas, TX 75201",
      property_type: "Single Family",
      purchase_price: 215000,
      closing_date_target: "2026-04-15",
    },
    package_summary_url: "https://example.test/package",
  });

  assert.match(sms, /123 Main St/);
  assert.match(sms, /Buy \$215,000/);
  assert.match(sms, /Reply interested, pass, or questions/i);
  assert.ok(sms.length <= 320);
});
