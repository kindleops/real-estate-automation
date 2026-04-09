import APP_IDS from "@/lib/config/app-ids.js";
import PODIO_ATTACHED_BASE_SCHEMA from "@/lib/podio/schema-attached.generated.js";

const BASE_BRAIN_SCHEMA =
  PODIO_ATTACHED_BASE_SCHEMA[String(APP_IDS.ai_conversation_brain)] || null;

const BASE_SEND_QUEUE_SCHEMA =
  PODIO_ATTACHED_BASE_SCHEMA[String(APP_IDS.send_queue)] || null;

export const PODIO_ATTACHED_SCHEMA_SUPPLEMENT = Object.freeze({
  // Send Queue — extends base schema with enrichment fields added after the
  // initial schema snapshot, plus corrected/annotated overrides for existing
  // fields whose snapshot data is stale or wrong.
  //
  // ── Template field mismatch (REQUIRES PODIO SCHEMA CHANGE) ────────────────
  // The base snapshot records Send Queue.template.referenced_app_ids = [29488989]
  // (an old, inactive Templates app).  The active Templates app is
  // APP_IDS.templates = 30647181.  Until the Podio relationship field is updated
  // to reference 30647181, every attempt to attach a template item will be
  // rejected by Podio with 400.  The code correctly retries without template on
  // 400, but the template is never linked.
  // PODIO ACTION: In the Send Queue app settings, open the "Template" relationship
  // field and change its referenced app from 29488989 to 30647181.
  // CODE ACTION: The supplement below already declares the correct app ID so that
  // once Podio is updated the code path works without further changes.
  //
  // ── Queue Status — Delivered option (REQUIRES PODIO SCHEMA CHANGE) ────────
  // "Delivered" is added here so the schema check in handle-textgrid-delivery.js
  // can detect the option.  The placeholder id:7 MUST match the actual Podio
  // option id once the option is added.  Run the schema refresh script after
  // adding the Podio option to obtain the correct id.
  //   node --import ./tests/register-aliases.mjs scripts/refresh-send-queue-schema.mjs
  //
  // ── Failed Reason — missing options (REQUIRES PODIO SCHEMA CHANGE) ────────
  // "Content Filter", "Destination Unreachable", "Unknown Error", and
  // "Delivery Rejected" are used in code but absent from the snapshot.
  // Placeholder ids 6–9 must be confirmed after adding them to Podio.
  //
  // ── Personalization Tags Used — single-select (REQUIRES PODIO SCHEMA CHANGE) ─
  // The field is declared multiple:true below to reflect intent.  Until the
  // Podio field is changed from single-select to multi-select, the write path
  // will only persist the first tag.  Code logs a warning when multiple tags
  // are detected.
  //
  // ── property-address: declared as type "location" (updated from "text"). ──
  // ── property-type/owner-type/category/use-case-template: see options below. ─
  //   Run the schema refresh script if option ids ever diverge from Podio.
  [String(APP_IDS.send_queue)]: {
    ...(BASE_SEND_QUEUE_SCHEMA || {
      app_id: APP_IDS.send_queue,
      app_name: "Send Queue",
      item_name: "Message",
      fields: {},
    }),
    fields: {
      ...(BASE_SEND_QUEUE_SCHEMA?.fields || {}),
      // ── Template relationship — correct referenced app id ─────────────────
      // Base snapshot has referenced_app_ids: [29488989] (stale inactive app).
      // This override declares the active Templates app so resolveTemplateFieldReference
      // correctly identifies direct-attachment candidates.  The write will still
      // fail with Podio 400 until the Podio field itself is pointed at 30647181.
      "template": {
        ...(BASE_SEND_QUEUE_SCHEMA?.fields?.["template"] || {}),
        referenced_app_ids: [APP_IDS.templates],
      },

      // ── Queue Status — adds "Delivered" option ─────────────────────────────
      // Placeholder id:7 — verify against actual Podio option id after adding
      // the option via the Podio admin UI.
      "queue-status": {
        ...(BASE_SEND_QUEUE_SCHEMA?.fields?.["queue-status"] || {}),
        options: [
          ...(BASE_SEND_QUEUE_SCHEMA?.fields?.["queue-status"]?.options || []),
          { id: 7, text: "Delivered" },
        ],
      },

      // ── Failed Reason — adds missing options used in code ─────────────────
      // Placeholder ids 6–9 — verify against Podio after adding.
      "failed-reason": {
        ...(BASE_SEND_QUEUE_SCHEMA?.fields?.["failed-reason"] || {}),
        options: [
          ...(BASE_SEND_QUEUE_SCHEMA?.fields?.["failed-reason"]?.options || []),
          { id: 6, text: "Content Filter" },
          { id: 7, text: "Destination Unreachable" },
          { id: 8, text: "Unknown Error" },
          { id: 9, text: "Delivery Rejected" },
        ],
      },

      // ── Personalization Tags Used — multi-select intent ───────────────────
      // The Podio field is currently single-select (multiple:false).  This
      // override reflects the intended post-schema-change behaviour.  Until
      // Podio is updated, normalizeCategoryValue will only persist the first
      // tag.  Code in build-send-queue-item.js logs a warning when > 1 tag is
      // detected but the field is still single-select.
      "personalization-tags-used": {
        ...(BASE_SEND_QUEUE_SCHEMA?.fields?.["personalization-tags-used"] || {}),
        multiple: true,
      },

      "queue-id-2": {
        label: "Queue ID",
        type: "text",
        multiple: false,
        allowed_currencies: null,
        referenced_app_ids: [],
        options: [],
      },
      "queue-sequence": {
        label: "Queue Sequence",
        type: "number",
        multiple: false,
        allowed_currencies: null,
        referenced_app_ids: [],
        options: [],
      },
      "property-address": {
        label: "Property Address",
        type: "location",
        multiple: false,
        allowed_currencies: null,
        referenced_app_ids: [],
        options: [],
      },
      "property-type": {
        label: "Property Type",
        type: "category",
        multiple: false,
        allowed_currencies: null,
        referenced_app_ids: [],
        options: [
        { id: 1, text: "Single Family" },
        { id: 2, text: "Multi-Family" },
        { id: 3, text: "Vacant Land" },
        { id: 4, text: "Apartment" },
        { id: 5, text: "Other" },
        { id: 6, text: "Mobile Home" },
      ],
      },
      "owner-type": {
        label: "Owner Type",
        type: "category",
        multiple: false,
        allowed_currencies: null,
        referenced_app_ids: [],
        options: [
        { id: 1, text: "Corporate" },
        { id: 2, text: "Individual" },
        { id: 3, text: "Trust / Estate" },
        { id: 4, text: "Bank / Lender" },
        { id: 6, text: "Government" },
      ],
      },
      "category": {
        label: "Category",
        type: "category",
        multiple: false,
        allowed_currencies: null,
        referenced_app_ids: [],
        options: [
        { id: 1, text: "Corporate" },
        { id: 2, text: "Individual" },
        { id: 3, text: "Trust / Estate" },
        { id: 4, text: "Bank / Lender" },
        { id: 5, text: "Government" },
      ],
      },
      "use-case-template": {
        label: "Use Case / Template",
        type: "category",
        multiple: false,
        allowed_currencies: null,
        referenced_app_ids: [],
        options: [
        { id: 1, text: "ownership_check" },
        { id: 2, text: "ownership_check_follow_up" },
        { id: 3, text: "consider_selling" },
        { id: 4, text: "consider_selling_follow_up" },
        { id: 5, text: "followup_hard" },
        { id: 6, text: "followup_soft" },
        { id: 7, text: "offer_no_response_followup" },
        { id: 8, text: "persona_empathetic_followup" },
        { id: 9, text: "persona_investor_direct_followup" },
        { id: 10, text: "persona_neighborly_followup" },
        { id: 11, text: "persona_no-nonsense_closer_followup" },
        { id: 12, text: "persona_warm_professional_followup" },
        { id: 13, text: "send_info" },
        { id: 14, text: "asking_price" },
        { id: 15, text: "asking_price_follow_up" },
        { id: 16, text: "price_works_confirm_basics" },
        { id: 17, text: "price_high_condition_probe" },
        { id: 18, text: "creative_followup" },
        { id: 19, text: "creative_probe" },
        { id: 20, text: "offer_reveal_cash" },
        { id: 21, text: "offer_reveal_lease_option" },
        { id: 22, text: "offer_reveal_subject_to" },
        { id: 23, text: "offer_reveal_novation" },
        { id: 24, text: "mf_confirm_units" },
        { id: 25, text: "mf_occupancy" },
        { id: 26, text: "mf_rents" },
        { id: 27, text: "mf_expenses" },
        { id: 28, text: "mf_underwriting_ack" },
        { id: 29, text: "justify_price" },
        { id: 30, text: "close_handoff" },
        { id: 31, text: "how_got_number" },
        { id: 32, text: "not_interested" },
        { id: 33, text: "reengagement" },
        { id: 34, text: "who_is_this" },
        { id: 35, text: "wrong_person" },
        { id: 36, text: "already_have_someone" },
        { id: 37, text: "already_listed" },
        { id: 38, text: "asks_contract" },
        { id: 39, text: "bankruptcy_sensitivity" },
        { id: 40, text: "best_price" },
        { id: 41, text: "buyer_referral_transition" },
        { id: 42, text: "call_me_later_redirect" },
        { id: 43, text: "can_you_do_better" },
        { id: 44, text: "clear_to_close" },
        { id: 45, text: "close_ask_casual" },
        { id: 46, text: "close_ask_hard" },
        { id: 47, text: "close_ask_soft" },
        { id: 48, text: "closing_date_locked" },
        { id: 49, text: "closing_date_moved" },
        { id: 50, text: "closing_timeline" },
        { id: 51, text: "code_violation_probe" },
        { id: 52, text: "condition_question_set" },
        { id: 53, text: "contract_not_signed_followup" },
        { id: 54, text: "contract_nudge_ultrashort" },
        { id: 55, text: "contract_revision" },
        { id: 56, text: "contract_sent" },
        { id: 57, text: "day_before_close" },
        { id: 58, text: "death_sensitivity" },
        { id: 59, text: "divorce_sensitivity" },
        { id: 60, text: "earnest_money" },
        { id: 61, text: "earnest_pending" },
        { id: 62, text: "earnest_sent" },
        { id: 63, text: "email_for_docs" },
        { id: 64, text: "email_me_instead" },
        { id: 65, text: "emotion_neighborly_calm" },
        { id: 66, text: "emotion_neighborly_curious" },
        { id: 67, text: "emotion_neighborly_frustrated" },
        { id: 68, text: "emotion_neighborly_guarded" },
        { id: 69, text: "emotion_neighborly_motivated" },
        { id: 70, text: "emotion_neighborly_overwhelmed" },
        { id: 71, text: "emotion_neighborly_skeptical" },
        { id: 72, text: "emotion_neighborly_tired_landlord" },
        { id: 73, text: "emotion_no-nonsense_closer_calm" },
        { id: 74, text: "emotion_no-nonsense_closer_curious" },
        { id: 75, text: "emotion_no-nonsense_closer_frustrated" },
        { id: 76, text: "emotion_no-nonsense_closer_guarded" },
        { id: 77, text: "emotion_no-nonsense_closer_motivated" },
        { id: 78, text: "emotion_no-nonsense_closer_overwhelmed" },
        { id: 79, text: "emotion_no-nonsense_closer_skeptical" },
        { id: 80, text: "emotion_no-nonsense_closer_tired_landlord" },
        { id: 81, text: "emotion_warm_professional_calm" },
        { id: 82, text: "emotion_warm_professional_curious" },
        { id: 83, text: "emotion_warm_professional_frustrated" },
        { id: 84, text: "emotion_warm_professional_guarded" },
        { id: 85, text: "emotion_warm_professional_motivated" },
        { id: 86, text: "emotion_warm_professional_overwhelmed" },
        { id: 87, text: "emotion_warm_professional_skeptical" },
        { id: 88, text: "emotion_warm_professional_tired_landlord" },
        { id: 89, text: "esign_help" },
        { id: 90, text: "esign_link_sent" },
        { id: 91, text: "family_discussion" },
        { id: 92, text: "foreclosure_pressure" },
        { id: 93, text: "ghost_after_contract" },
        { id: 94, text: "has_tenants" },
        { id: 95, text: "hostile_reply_defuse" },
        { id: 96, text: "inspection_schedule" },
        { id: 97, text: "lien_issue_detected" },
        { id: 98, text: "lowball_accusation" },
        { id: 99, text: "mf_occupancy_rents" },
        { id: 100, text: "monthly_payment_followup" },
        { id: 101, text: "need_spouse_signoff" },
        { id: 102, text: "no_call_reassurance" },
        { id: 103, text: "not_ready" },
        { id: 104, text: "obj_empathetic_already_listed" },
        { id: 105, text: "obj_empathetic_condition_bad" },
        { id: 106, text: "obj_empathetic_need_family_ok" },
        { id: 107, text: "obj_empathetic_need_more_money" },
        { id: 108, text: "obj_empathetic_need_time" },
        { id: 109, text: "obj_empathetic_not_interested" },
        { id: 110, text: "obj_empathetic_send_offer_first" },
        { id: 111, text: "obj_empathetic_stop_texting" },
        { id: 112, text: "obj_empathetic_tenant_issue" },
        { id: 113, text: "obj_empathetic_who_is_this" },
        { id: 114, text: "obj_neighborly_condition_bad" },
        { id: 115, text: "obj_neighborly_need_family_ok" },
        { id: 116, text: "obj_neighborly_send_offer_first" },
        { id: 117, text: "obj_neighborly_stop_texting" },
        { id: 118, text: "obj_neighborly_tenant_issue" },
        { id: 119, text: "obj_neighborly_who_is_this" },
        { id: 120, text: "obj_warm_professional_need_more_money" },
        { id: 121, text: "obj_warm_professional_not_interested" },
        { id: 122, text: "obj_warm_professional_send_offer_first" },
        { id: 123, text: "obj_warm_professional_stop_texting" },
        { id: 124, text: "obj_warm_professional_who_is_this" },
        { id: 125, text: "occupied_asset" },
        { id: 126, text: "offer_reveal_casual" },
        { id: 127, text: "offer_reveal_hard" },
        { id: 128, text: "offer_reveal_soft" },
        { id: 129, text: "offer_reveal_ultrashort" },
        { id: 130, text: "pain_probe" },
        { id: 131, text: "persona_empathetic_close_ask" },
        { id: 132, text: "persona_empathetic_offer_reveal" },
        { id: 133, text: "persona_empathetic_price_pushback" },
        { id: 134, text: "persona_investor_direct_close_ask" },
        { id: 135, text: "persona_investor_direct_offer_reveal" },
        { id: 136, text: "persona_investor_direct_price_pushback" },
        { id: 137, text: "persona_neighborly_close_ask" },
        { id: 138, text: "persona_neighborly_offer_reveal" },
        { id: 139, text: "persona_neighborly_price_pushback" },
        { id: 140, text: "persona_no-nonsense_closer_close_ask" },
        { id: 141, text: "persona_no-nonsense_closer_offer_reveal" },
        { id: 142, text: "persona_no-nonsense_closer_price_pushback" },
        { id: 143, text: "persona_warm_professional_close_ask" },
        { id: 144, text: "persona_warm_professional_offer_reveal" },
        { id: 145, text: "persona_warm_professional_price_pushback" },
        { id: 146, text: "photo_request" },
        { id: 147, text: "post_close_referral" },
        { id: 148, text: "price_low_casual" },
        { id: 149, text: "price_low_hard" },
        { id: 150, text: "price_low_soft" },
        { id: 151, text: "price_too_low" },
        { id: 152, text: "probate_doc_needed" },
        { id: 153, text: "proof_of_funds" },
        { id: 154, text: "retrade_pushback" },
        { id: 155, text: "seller_asking_price" },
        { id: 156, text: "seller_asks_legit" },
        { id: 157, text: "seller_docs_needed" },
        { id: 158, text: "seller_finance_casual" },
        { id: 159, text: "seller_finance_interest" },
        { id: 160, text: "seller_stalling_after_yes" },
        { id: 161, text: "send_package" },
        { id: 162, text: "sibling_conflict" },
        { id: 163, text: "sms_only_preference" },
        { id: 164, text: "tenants_ok" },
        { id: 165, text: "text_me_later_specific" },
        { id: 166, text: "title_by_text_update" },
        { id: 167, text: "title_company" },
        { id: 168, text: "title_delay_followup" },
        { id: 169, text: "title_intro" },
        { id: 170, text: "title_issue_discovered" },
        { id: 171, text: "title_issue_soft" },
        { id: 172, text: "vacant_boarded_probe" },
        { id: 173, text: "walkthrough_confirmed" },
        { id: 174, text: "walkthrough_or_condition" },
        { id: 175, text: "website_reviews_request" },
        { id: 176, text: "wrong_number_knows_owner" },
      ],
      },
    },
  },
  [String(APP_IDS.ai_conversation_brain)]: {
    ...(BASE_BRAIN_SCHEMA || {
      app_id: APP_IDS.ai_conversation_brain,
      app_name: "AI Conversation Brain",
      item_name: "Message",
      fields: {},
    }),
    fields: {
      ...(BASE_BRAIN_SCHEMA?.fields || {}),
      "linked-message-events": {
        label: "Linked Message Events",
        type: "app",
        multiple: true,
        allowed_currencies: null,
        referenced_app_ids: [APP_IDS.message_events],
        options: [],
      },
    },
  },
  "30644077": {
    "app_id": 30644077,
    "app_name": "Buyers (Hedge funds, flippers, institutions)",
    "item_name": "Buyer",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644094": {
    "app_id": 30644094,
    "app_name": "Buyer Preferences",
    "item_name": "Preference",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644095": {
    "app_id": 30644095,
    "app_name": "Buyer Activity",
    "item_name": "Activity",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644096": {
    "app_id": 30644096,
    "app_name": "ZIP Buyer Scoring",
    "item_name": "ZIP",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644097": {
    "app_id": 30644097,
    "app_name": "Auto-Match Engine",
    "item_name": "Engine",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644237": {
    "app_id": 30644237,
    "app_name": "Buyer Officers",
    "item_name": "Officer",
    "fields": {
      "seller-id": {
        "label": "Seller ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-full-name": {
        "label": "Owner Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-type": {
        "label": "Owner Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Individual"
          },
          {
            "id": 2,
            "text": "Corporate"
          },
          {
            "id": 3,
            "text": "Trust / Estate"
          },
          {
            "id": 4,
            "text": "Hedge Fund"
          },
          {
            "id": 5,
            "text": "Government"
          },
          {
            "id": 6,
            "text": "Bank / Lender"
          },
          {
            "id": 7,
            "text": "Needs Review"
          },
          {
            "id": 8,
            "text": "Hedgefund"
          }
        ]
      },
      "file": {
        "label": "File",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "#6"
          },
          {
            "id": 2,
            "text": "#6 (2)"
          },
          {
            "id": 3,
            "text": "#6 (1)"
          },
          {
            "id": 4,
            "text": "#6 (0)"
          },
          {
            "id": 5,
            "text": "#1 (0)"
          },
          {
            "id": 6,
            "text": "#1 (1)"
          },
          {
            "id": 7,
            "text": "#1 (2)"
          },
          {
            "id": 8,
            "text": "#2 (0)"
          },
          {
            "id": 9,
            "text": "#2 (1)"
          }
        ]
      },
      "owner-1-full-name": {
        "label": "Owner #1 Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title": {
        "label": "Owner #1 First Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-last-name": {
        "label": "Owner #1 Last Name / Company Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-2-full-name": {
        "label": "Owner #2 Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-2-first-name": {
        "label": "Owner #2 First Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-2-last-name": {
        "label": "Owner #2 Last Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field": {
        "label": ">",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "name-of-contact": {
        "label": "Name of Contact",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-mailing-address": {
        "label": "Contact Address",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contact-order-score": {
        "label": "Contact Order Score",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "3"
          },
          {
            "id": 2,
            "text": "2"
          },
          {
            "id": 3,
            "text": "1"
          },
          {
            "id": 4,
            "text": "$3"
          },
          {
            "id": 5,
            "text": "$2"
          },
          {
            "id": 6,
            "text": "0"
          },
          {
            "id": 7,
            "text": "$1"
          },
          {
            "id": 8,
            "text": "$0"
          },
          {
            "id": 9,
            "text": "Unknown"
          }
        ]
      },
      "contact-matching-tags": {
        "label": "Contact Matching Tags",
        "type": "category",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Likely Owner"
          },
          {
            "id": 2,
            "text": "Resident"
          },
          {
            "id": 3,
            "text": "Family"
          },
          {
            "id": 4,
            "text": "Likely Renting"
          },
          {
            "id": 5,
            "text": "Linked To Company"
          },
          {
            "id": 6,
            "text": "Potentially Linked To Company"
          },
          {
            "id": 7,
            "text": "Potential Owner"
          },
          {
            "id": 8,
            "text": "Likely Owner, Family, Resident"
          },
          {
            "id": 9,
            "text": "Likely Owner, Family"
          },
          {
            "id": 10,
            "text": "Resident, Likely Renting"
          },
          {
            "id": 11,
            "text": "Potential Owner, Family"
          },
          {
            "id": 12,
            "text": "Family, Resident"
          },
          {
            "id": 13,
            "text": "Likely Owner, Resident"
          },
          {
            "id": 14,
            "text": "Potential Owner, Resident"
          },
          {
            "id": 15,
            "text": "Linked To Company, Family"
          },
          {
            "id": 16,
            "text": "Potentially Linked To Company, Family"
          }
        ]
      },
      "contact-matching-type": {
        "label": "Contact Matching Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "mailing_address"
          },
          {
            "id": 2,
            "text": "property_address"
          },
          {
            "id": 3,
            "text": "company_auto_match"
          },
          {
            "id": 4,
            "text": "pi_auto_match"
          },
          {
            "id": 5,
            "text": "company_tiebreaker"
          },
          {
            "id": 6,
            "text": "pi_tiebreaker"
          },
          {
            "id": 7,
            "text": "trust_tiebreaker"
          },
          {
            "id": 8,
            "text": "trust_auto_match"
          },
          {
            "id": 9,
            "text": "company_level2_tiebreaker"
          },
          {
            "id": 10,
            "text": "company_level2_auto_match"
          }
        ]
      },
      "age-of-contact": {
        "label": "Age of Contact",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "marital-status": {
        "label": "Marital Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Married - Likely"
          },
          {
            "id": 2,
            "text": "Single - Likely"
          }
        ]
      },
      "gender": {
        "label": "Gender",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Male"
          },
          {
            "id": 2,
            "text": "Female"
          }
        ]
      },
      "language": {
        "label": "Language",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "English"
          },
          {
            "id": 2,
            "text": "Spanish"
          },
          {
            "id": 3,
            "text": "Portuguese"
          },
          {
            "id": 4,
            "text": "Italian"
          },
          {
            "id": 5,
            "text": "Vietnamese"
          },
          {
            "id": 6,
            "text": "Asian Indian (Hindi or Other)"
          },
          {
            "id": 7,
            "text": "Mandarin"
          },
          {
            "id": 8,
            "text": "Arabic"
          },
          {
            "id": 9,
            "text": "Polish"
          },
          {
            "id": 10,
            "text": "Japanese"
          },
          {
            "id": 11,
            "text": "Korean"
          },
          {
            "id": 12,
            "text": "French"
          },
          {
            "id": 13,
            "text": "Hebrew"
          },
          {
            "id": 14,
            "text": "Russian"
          },
          {
            "id": 15,
            "text": "Greek"
          },
          {
            "id": 16,
            "text": "German"
          },
          {
            "id": 17,
            "text": "Pashtu/Pashto"
          },
          {
            "id": 18,
            "text": "Thai"
          },
          {
            "id": 19,
            "text": "Farsi"
          }
        ]
      },
      "education-level": {
        "label": "Education Level",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Grad Degree - Likely"
          },
          {
            "id": 2,
            "text": "Some College - Likely"
          },
          {
            "id": 3,
            "text": "HS Diploma - Likely"
          },
          {
            "id": 4,
            "text": "Bach Degree - Likely"
          },
          {
            "id": 5,
            "text": "Doctorate Degree - Likely"
          }
        ]
      },
      "household-income": {
        "label": "Household Income",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "$145,000-$149,999"
          },
          {
            "id": 2,
            "text": "$30,000-$34,999"
          },
          {
            "id": 3,
            "text": "$70,000-$74,999"
          },
          {
            "id": 4,
            "text": "$25,000-$29,999"
          },
          {
            "id": 5,
            "text": "$75,000-$79,999"
          },
          {
            "id": 6,
            "text": "$140,000-$144,999"
          },
          {
            "id": 7,
            "text": "$65,000-$69,999"
          },
          {
            "id": 8,
            "text": "$90,000-$94,999"
          },
          {
            "id": 9,
            "text": "$135,000-$139,999"
          },
          {
            "id": 10,
            "text": "$190,000-$199,999"
          },
          {
            "id": 11,
            "text": "$55,000-$59,999"
          },
          {
            "id": 12,
            "text": "$250,000 or More"
          },
          {
            "id": 13,
            "text": "$0-$14,999"
          },
          {
            "id": 14,
            "text": "$170,000-$174,999"
          },
          {
            "id": 15,
            "text": "$45,000-$49,999"
          },
          {
            "id": 16,
            "text": "$80,000-$84,999"
          },
          {
            "id": 17,
            "text": "$115,000-$119,999"
          },
          {
            "id": 18,
            "text": "$50,000-$54,999"
          },
          {
            "id": 19,
            "text": "$40,000-$44,999"
          },
          {
            "id": 20,
            "text": "$60,000-$64,999"
          },
          {
            "id": 21,
            "text": "$20,000-$24,999"
          },
          {
            "id": 22,
            "text": "$225,000-$249,999"
          },
          {
            "id": 23,
            "text": "$15,000-$19,999"
          },
          {
            "id": 24,
            "text": "$160,000-$169,999"
          },
          {
            "id": 25,
            "text": "$35,000-$39,999"
          },
          {
            "id": 26,
            "text": "$95,000-$99,999"
          },
          {
            "id": 27,
            "text": "$120,000-$124,999"
          },
          {
            "id": 28,
            "text": "$130,000-$134,999"
          },
          {
            "id": 29,
            "text": "$85,000-$89,999"
          },
          {
            "id": 30,
            "text": "$175,000-$189,999"
          },
          {
            "id": 31,
            "text": "$100,000-$104,999"
          },
          {
            "id": 32,
            "text": "$105,000-$109,999"
          },
          {
            "id": 33,
            "text": "$110,000-$114,999"
          },
          {
            "id": 34,
            "text": "$200,000-$224,999"
          },
          {
            "id": 35,
            "text": "$0"
          },
          {
            "id": 36,
            "text": "$125,000-$129,999"
          },
          {
            "id": 37,
            "text": "$150,000-$159,999"
          },
          {
            "id": 38,
            "text": "Likely Owner"
          },
          {
            "id": 39,
            "text": "Resident"
          },
          {
            "id": 40,
            "text": "Linked To Company"
          },
          {
            "id": 41,
            "text": "Potentially Linked To Company"
          },
          {
            "id": 42,
            "text": "Family"
          },
          {
            "id": 43,
            "text": "Potential Owner"
          }
        ]
      },
      "buyer-power": {
        "label": "Buyer Power",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Moderate and Emerging Buyers"
          },
          {
            "id": 2,
            "text": "Very High Risk"
          },
          {
            "id": 3,
            "text": "Emerging with Potential"
          },
          {
            "id": 4,
            "text": "Potential but High Risk"
          },
          {
            "id": 5,
            "text": "Stable and Reliable Buyers"
          },
          {
            "id": 6,
            "text": "High-Tier Buyers"
          },
          {
            "id": 7,
            "text": "Top-Tier Buyers"
          },
          {
            "id": 8,
            "text": "High Risk"
          },
          {
            "id": 9,
            "text": "Caution Buyers"
          }
        ]
      },
      "net-asset-value": {
        "label": "Net Asset Value",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "$2,000,000 or more"
          },
          {
            "id": 2,
            "text": "$0-24,999"
          },
          {
            "id": 3,
            "text": "$100,000-249,999"
          },
          {
            "id": 4,
            "text": "$75,000-99,999"
          },
          {
            "id": 5,
            "text": "$50,000-74,999"
          },
          {
            "id": 6,
            "text": "$250,000-499,000"
          },
          {
            "id": 7,
            "text": "$750,000-999,999"
          },
          {
            "id": 8,
            "text": "$500,000-749,999"
          },
          {
            "id": 9,
            "text": "$1,000,000-1,999,999"
          },
          {
            "id": 10,
            "text": "$25,000-49,999"
          }
        ]
      },
      "occupation": {
        "label": "Occupation",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Restricted"
          },
          {
            "id": 2,
            "text": "Teacher/Educator"
          },
          {
            "id": 3,
            "text": "Clerical/Office"
          },
          {
            "id": 4,
            "text": "Upper Management/Executive"
          },
          {
            "id": 5,
            "text": "Professional/Technical"
          },
          {
            "id": 6,
            "text": "Nurse"
          },
          {
            "id": 7,
            "text": "Real Estate"
          },
          {
            "id": 8,
            "text": "Skilled Trade/Machine/Laborer"
          },
          {
            "id": 9,
            "text": "Sales/Marketing"
          },
          {
            "id": 10,
            "text": "Homemaker"
          },
          {
            "id": 11,
            "text": "Military"
          },
          {
            "id": 12,
            "text": "Middle Management"
          },
          {
            "id": 13,
            "text": "Self Employed"
          },
          {
            "id": 14,
            "text": "Executive/Administrator"
          },
          {
            "id": 15,
            "text": "Doctors/Physicians/Surgeons"
          },
          {
            "id": 16,
            "text": "Health Services"
          },
          {
            "id": 17,
            "text": "Retail Sales"
          },
          {
            "id": 18,
            "text": "Computer Professional"
          },
          {
            "id": 19,
            "text": "Services/Creative"
          },
          {
            "id": 20,
            "text": "Financial Services"
          },
          {
            "id": 21,
            "text": "Engineers"
          },
          {
            "id": 22,
            "text": "Beauty"
          },
          {
            "id": 23,
            "text": "Attorneys"
          },
          {
            "id": 24,
            "text": "Farming/Agriculture"
          },
          {
            "id": 25,
            "text": "Insurance/Underwriters"
          },
          {
            "id": 26,
            "text": "Occup Therapist/Physical Therapist"
          },
          {
            "id": 27,
            "text": "Pharmacist"
          },
          {
            "id": 28,
            "text": "Civil Servant"
          },
          {
            "id": 29,
            "text": "Architects"
          },
          {
            "id": 30,
            "text": "Dentist/Dental Hygienist"
          },
          {
            "id": 31,
            "text": "Professional Driver"
          },
          {
            "id": 32,
            "text": "Accountants/CPA"
          },
          {
            "id": 33,
            "text": "Speech Path./Audiologist"
          },
          {
            "id": 34,
            "text": "Work From Home"
          },
          {
            "id": 35,
            "text": "Social Worker"
          },
          {
            "id": 36,
            "text": "Counselors"
          },
          {
            "id": 37,
            "text": "Clergy"
          },
          {
            "id": 38,
            "text": "Psychologist"
          },
          {
            "id": 39,
            "text": "Veterinarian"
          },
          {
            "id": 40,
            "text": "Landscape Architects"
          },
          {
            "id": 41,
            "text": "Opticians/Optometrist"
          },
          {
            "id": 42,
            "text": "Interior Designers"
          },
          {
            "id": 43,
            "text": "Chiropractors"
          },
          {
            "id": 44,
            "text": "Electricians"
          },
          {
            "id": 45,
            "text": "Surveyors"
          }
        ]
      },
      "occupation-group": {
        "label": "Occupation Group",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Restricted"
          },
          {
            "id": 2,
            "text": "Professional: Legal/Education and Health Practitioner"
          },
          {
            "id": 3,
            "text": "Office and Administrative Support"
          },
          {
            "id": 4,
            "text": "Management/Business and Financial Operations"
          },
          {
            "id": 5,
            "text": "Sales"
          },
          {
            "id": 6,
            "text": "Blue Collar"
          },
          {
            "id": 7,
            "text": "Other"
          },
          {
            "id": 8,
            "text": "Technical: Computers/Math and Architect/Engineering"
          },
          {
            "id": 9,
            "text": "Farming/Fish/Forestry"
          }
        ]
      },
      "owner-tags": {
        "label": "Owner Tags",
        "type": "category",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Real Estate Investor"
          },
          {
            "id": 2,
            "text": "Primary Decision Maker"
          },
          {
            "id": 3,
            "text": "Senior"
          },
          {
            "id": 4,
            "text": "Home Business"
          },
          {
            "id": 5,
            "text": "High Earner"
          },
          {
            "id": 6,
            "text": "High Net Worth"
          },
          {
            "id": 7,
            "text": "Property Owner"
          },
          {
            "id": 8,
            "text": "High Spender"
          },
          {
            "id": 9,
            "text": "Empty Nester"
          },
          {
            "id": 10,
            "text": "Veteran"
          },
          {
            "id": 11,
            "text": "Renter"
          },
          {
            "id": 12,
            "text": "Potential First Time Home Buyer"
          },
          {
            "id": 13,
            "text": "Cash Buyer"
          },
          {
            "id": 14,
            "text": "Business Owner"
          },
          {
            "id": 15,
            "text": "Elderly Parent"
          },
          {
            "id": 16,
            "text": "Likely To Move"
          },
          {
            "id": 17,
            "text": "Young Adult"
          },
          {
            "id": 18,
            "text": "Real Estate Agent"
          },
          {
            "id": 19,
            "text": "New Mover"
          },
          {
            "id": 20,
            "text": "House Flipper"
          },
          {
            "id": 21,
            "text": "likely_to_move"
          },
          {
            "id": 22,
            "text": "empty_nester"
          },
          {
            "id": 23,
            "text": "property_owner"
          },
          {
            "id": 24,
            "text": "high_spender"
          },
          {
            "id": 25,
            "text": "potential_first_time_home_buyer"
          }
        ]
      },
      "likely-owner": {
        "label": "Likely Owner",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "TRUE"
          },
          {
            "id": 2,
            "text": "FALSE"
          }
        ]
      },
      "in-owner-family": {
        "label": "In Owner Family",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "FALSE"
          },
          {
            "id": 2,
            "text": "TRUE"
          }
        ]
      },
      "likely-renter": {
        "label": "Likely Renter",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "FALSE"
          },
          {
            "id": 2,
            "text": "TRUE"
          }
        ]
      },
      "likely-resident": {
        "label": "Likely Resident",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "FALSE"
          },
          {
            "id": 2,
            "text": "TRUE"
          },
          {
            "id": 3,
            "text": "Potential First Time Home Buyer"
          },
          {
            "id": 4,
            "text": "Property Owner"
          },
          {
            "id": 5,
            "text": "High Spender"
          }
        ]
      },
      "linked-phone-number": {
        "label": "Linked Phone Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637174
        ],
        "options": []
      },
      "linked-email-addresses": {
        "label": "Linked Email Addresses",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637242
        ],
        "options": []
      },
      "linked-owner": {
        "label": "Linked Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637059
        ],
        "options": []
      }
    }
  },
  "30644239": {
    "app_id": 30644239,
    "app_name": "Sold Properties",
    "item_name": "Prospect",
    "fields": {
      "property-id": {
        "label": "Property ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "full-name": {
        "label": "Company Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-type-2": {
        "label": "Company Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Corporate"
          },
          {
            "id": 2,
            "text": "Individual"
          },
          {
            "id": 3,
            "text": "Trust / Estate"
          },
          {
            "id": 4,
            "text": "Bank / Lender"
          },
          {
            "id": 5,
            "text": "Hedgefund"
          },
          {
            "id": 6,
            "text": "Government"
          }
        ]
      },
      "file": {
        "label": "File",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "#1"
          },
          {
            "id": 2,
            "text": "#2"
          },
          {
            "id": 3,
            "text": "#3"
          },
          {
            "id": 4,
            "text": "#4"
          },
          {
            "id": 5,
            "text": "#5"
          },
          {
            "id": 6,
            "text": "#6"
          }
        ]
      },
      "comp-search-profile-hash": {
        "label": "Comp Search Profile Hash",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30657385
        ],
        "options": []
      },
      "property-address": {
        "label": "Comp Address",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation": {
        "label": ">>",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field": {
        "label": ">",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Miami, FL"
          },
          {
            "id": 2,
            "text": "Houston,. TX"
          },
          {
            "id": 3,
            "text": "Orlando, FL"
          },
          {
            "id": 4,
            "text": "Tampa, FL"
          },
          {
            "id": 5,
            "text": "Dallas, TX"
          },
          {
            "id": 6,
            "text": "Jacksonville, FL"
          },
          {
            "id": 7,
            "text": "Charlotte, NC"
          },
          {
            "id": 8,
            "text": "Minneapolis, MN"
          },
          {
            "id": 9,
            "text": "Nashville, TN"
          },
          {
            "id": 10,
            "text": "Phoenix, AZ"
          },
          {
            "id": 11,
            "text": "Saint Louis, MO"
          },
          {
            "id": 12,
            "text": "Indianapolis, IN"
          },
          {
            "id": 13,
            "text": "Memphis, TN"
          },
          {
            "id": 14,
            "text": "Rochester, NY"
          },
          {
            "id": 15,
            "text": "Atlanta, GA"
          },
          {
            "id": 16,
            "text": "Lakeland, FL"
          },
          {
            "id": 17,
            "text": "Fresno, CA"
          },
          {
            "id": 18,
            "text": "Bakersfield, CA"
          },
          {
            "id": 19,
            "text": "Tuscon, AZ"
          },
          {
            "id": 20,
            "text": "Sacramento, CA"
          },
          {
            "id": 21,
            "text": "Oklahoma City, OK"
          },
          {
            "id": 22,
            "text": "Birmingham, AL"
          },
          {
            "id": 23,
            "text": "New Orleans, LA"
          },
          {
            "id": 24,
            "text": "Inland Emprie, CA"
          },
          {
            "id": 25,
            "text": "Stockton, CA"
          },
          {
            "id": 26,
            "text": "Modesto, CA"
          },
          {
            "id": 27,
            "text": "Hartford, CT"
          },
          {
            "id": 28,
            "text": "Boise, ID"
          },
          {
            "id": 29,
            "text": "Raleigh, NC"
          },
          {
            "id": 30,
            "text": "Tulsa, OK"
          },
          {
            "id": 31,
            "text": "Providence, RI"
          },
          {
            "id": 32,
            "text": "Austin, TX"
          },
          {
            "id": 33,
            "text": "Albuquerque, NM"
          },
          {
            "id": 34,
            "text": "Norfolk, VA"
          },
          {
            "id": 35,
            "text": "Columbus, OH"
          },
          {
            "id": 36,
            "text": "Des Moines, IA"
          },
          {
            "id": 37,
            "text": "Louisville, KY"
          },
          {
            "id": 38,
            "text": "El Paso, TX"
          },
          {
            "id": 39,
            "text": "Cincinnati, OH"
          },
          {
            "id": 40,
            "text": "Portsmouth, VA"
          },
          {
            "id": 41,
            "text": "San Antonio, TX"
          },
          {
            "id": 42,
            "text": "Pittsburg, PA"
          },
          {
            "id": 43,
            "text": "Wichita, KS"
          },
          {
            "id": 44,
            "text": "Salt Lake City, UT"
          },
          {
            "id": 45,
            "text": "Richmond, VA"
          },
          {
            "id": 46,
            "text": "Omaha, NE"
          },
          {
            "id": 47,
            "text": "Cleveland, OH"
          },
          {
            "id": 48,
            "text": "Detroit, MI"
          },
          {
            "id": 49,
            "text": "Baltimore, MD"
          },
          {
            "id": 50,
            "text": "Philadelphia, PA"
          },
          {
            "id": 51,
            "text": "Chicago, IL"
          },
          {
            "id": 52,
            "text": "Milwaukee, WI"
          },
          {
            "id": 53,
            "text": "Kansas City, MO"
          },
          {
            "id": 54,
            "text": "Clayton, GA"
          },
          {
            "id": 55,
            "text": "Houston, TX"
          },
          {
            "id": 56,
            "text": "Las Vegas, NV"
          },
          {
            "id": 57,
            "text": "Los Angeles, CA"
          },
          {
            "id": 58,
            "text": "Spokane, WA"
          },
          {
            "id": 59,
            "text": "Inland Empire, CA"
          }
        ]
      },
      "county": {
        "label": "County",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30657386
        ],
        "options": []
      },
      "property-county": {
        "label": "Property County",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Palm Beach"
          },
          {
            "id": 2,
            "text": "Broward"
          },
          {
            "id": 3,
            "text": "Miami-Dade"
          },
          {
            "id": 4,
            "text": "Harris"
          },
          {
            "id": 5,
            "text": "Orange"
          },
          {
            "id": 6,
            "text": "Hillsborough"
          },
          {
            "id": 7,
            "text": "Dallas"
          },
          {
            "id": 8,
            "text": "Duval"
          },
          {
            "id": 9,
            "text": "Mecklenburg"
          },
          {
            "id": 10,
            "text": "Ramsey"
          },
          {
            "id": 11,
            "text": "Davidson"
          },
          {
            "id": 12,
            "text": "Hennepin"
          },
          {
            "id": 13,
            "text": "Maricopa"
          },
          {
            "id": 14,
            "text": "Johnson"
          },
          {
            "id": 15,
            "text": "Saint Louis"
          },
          {
            "id": 16,
            "text": "Cabarrus"
          },
          {
            "id": 17,
            "text": "Lake"
          },
          {
            "id": 18,
            "text": "Polk"
          },
          {
            "id": 19,
            "text": "Pasco"
          },
          {
            "id": 20,
            "text": "Hernando"
          },
          {
            "id": 21,
            "text": "Osceola"
          },
          {
            "id": 22,
            "text": "De Kalb"
          },
          {
            "id": 23,
            "text": "Fulton"
          },
          {
            "id": 24,
            "text": "Seminole"
          },
          {
            "id": 25,
            "text": "Union"
          },
          {
            "id": 26,
            "text": "Jackson"
          },
          {
            "id": 27,
            "text": "Marion"
          },
          {
            "id": 28,
            "text": "Shelby"
          },
          {
            "id": 29,
            "text": "Monroe"
          },
          {
            "id": 30,
            "text": "Stanly"
          },
          {
            "id": 31,
            "text": "Rowan"
          },
          {
            "id": 32,
            "text": "Iredell"
          },
          {
            "id": 33,
            "text": "Fresno"
          },
          {
            "id": 34,
            "text": "Kern"
          },
          {
            "id": 35,
            "text": "Tarrant"
          },
          {
            "id": 36,
            "text": "Pima"
          },
          {
            "id": 37,
            "text": "Sacramento"
          },
          {
            "id": 38,
            "text": "Oklahoma"
          },
          {
            "id": 39,
            "text": "Jefferson"
          },
          {
            "id": 40,
            "text": "Orleans"
          },
          {
            "id": 41,
            "text": "Riverside"
          },
          {
            "id": 42,
            "text": "San Bernardino"
          },
          {
            "id": 43,
            "text": "San Joaquin"
          },
          {
            "id": 44,
            "text": "Stanislaus"
          },
          {
            "id": 45,
            "text": "Hartford"
          },
          {
            "id": 46,
            "text": "Canyon"
          },
          {
            "id": 47,
            "text": "Bladen"
          },
          {
            "id": 48,
            "text": "Cumberland"
          },
          {
            "id": 49,
            "text": "Durham"
          },
          {
            "id": 50,
            "text": "Edgecombe"
          },
          {
            "id": 51,
            "text": "Nash"
          },
          {
            "id": 52,
            "text": "Wilson"
          },
          {
            "id": 53,
            "text": "Creek"
          },
          {
            "id": 54,
            "text": "Osage"
          },
          {
            "id": 55,
            "text": "Tulsa"
          },
          {
            "id": 56,
            "text": "Wagoner"
          },
          {
            "id": 57,
            "text": "Beaver"
          },
          {
            "id": 58,
            "text": "Providence"
          },
          {
            "id": 59,
            "text": "Bastrop"
          },
          {
            "id": 60,
            "text": "Hays"
          },
          {
            "id": 61,
            "text": "Travis"
          },
          {
            "id": 62,
            "text": "Bernalillo"
          },
          {
            "id": 63,
            "text": "Norfolk City"
          },
          {
            "id": 64,
            "text": "Franklin"
          },
          {
            "id": 65,
            "text": "El Paso"
          },
          {
            "id": 66,
            "text": "Hamilton"
          },
          {
            "id": 67,
            "text": "Portsmouth City"
          },
          {
            "id": 68,
            "text": "Bexar"
          },
          {
            "id": 69,
            "text": "Hampton City"
          },
          {
            "id": 70,
            "text": "Weber"
          },
          {
            "id": 71,
            "text": "Allegheny"
          },
          {
            "id": 72,
            "text": "Sedgwick"
          },
          {
            "id": 73,
            "text": "Salt Lake"
          },
          {
            "id": 74,
            "text": "Richmond City"
          },
          {
            "id": 75,
            "text": "Cleveland"
          },
          {
            "id": 76,
            "text": "Douglas"
          },
          {
            "id": 77,
            "text": "Newport News City"
          },
          {
            "id": 78,
            "text": "Henrico"
          },
          {
            "id": 79,
            "text": "Chesterfield"
          },
          {
            "id": 80,
            "text": "Suffolk City"
          },
          {
            "id": 81,
            "text": "Saint Louis City"
          },
          {
            "id": 82,
            "text": "Cuyahoga"
          },
          {
            "id": 83,
            "text": "Portage"
          },
          {
            "id": 84,
            "text": "Wayne"
          },
          {
            "id": 85,
            "text": "Baltimore City"
          },
          {
            "id": 86,
            "text": "Baltimore"
          },
          {
            "id": 87,
            "text": "Philadelphia"
          },
          {
            "id": 88,
            "text": "Cook"
          },
          {
            "id": 89,
            "text": "Anoka"
          },
          {
            "id": 90,
            "text": "Washington"
          },
          {
            "id": 91,
            "text": "Dakota"
          },
          {
            "id": 92,
            "text": "Milwaukee"
          },
          {
            "id": 93,
            "text": "Wyandotte"
          },
          {
            "id": 94,
            "text": "Clayton"
          },
          {
            "id": 95,
            "text": "Henry"
          },
          {
            "id": 96,
            "text": "Rockdale"
          },
          {
            "id": 97,
            "text": "Fayette"
          },
          {
            "id": 98,
            "text": "Gwinnett"
          },
          {
            "id": 99,
            "text": "Fort Bend"
          },
          {
            "id": 100,
            "text": "Galveston"
          },
          {
            "id": 101,
            "text": "Clark"
          },
          {
            "id": 102,
            "text": "Los Angeles"
          },
          {
            "id": 103,
            "text": "Ashtabula"
          },
          {
            "id": 104,
            "text": "Spokane"
          },
          {
            "id": 105,
            "text": "Williamson"
          }
        ]
      },
      "market-2": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "zip-code-2": {
        "label": "Zip Code",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "93702"
          },
          {
            "id": 2,
            "text": "93309"
          },
          {
            "id": 3,
            "text": "76105"
          },
          {
            "id": 4,
            "text": "85712"
          },
          {
            "id": 5,
            "text": "93706"
          },
          {
            "id": 6,
            "text": "75061"
          },
          {
            "id": 7,
            "text": "95822"
          },
          {
            "id": 8,
            "text": "32805"
          },
          {
            "id": 9,
            "text": "76110"
          },
          {
            "id": 10,
            "text": "75060"
          },
          {
            "id": 11,
            "text": "95823"
          },
          {
            "id": 12,
            "text": "93305"
          },
          {
            "id": 13,
            "text": "93306"
          },
          {
            "id": 14,
            "text": "73119"
          },
          {
            "id": 15,
            "text": "76112"
          },
          {
            "id": 16,
            "text": "95824"
          },
          {
            "id": 17,
            "text": "93308"
          },
          {
            "id": 18,
            "text": "95758"
          },
          {
            "id": 19,
            "text": "35206"
          },
          {
            "id": 20,
            "text": "75051"
          },
          {
            "id": 21,
            "text": "76106"
          },
          {
            "id": 22,
            "text": "76133"
          },
          {
            "id": 23,
            "text": "95660"
          },
          {
            "id": 24,
            "text": "95838"
          },
          {
            "id": 25,
            "text": "95820"
          },
          {
            "id": 26,
            "text": "93307"
          },
          {
            "id": 27,
            "text": "32206"
          },
          {
            "id": 28,
            "text": "73109"
          },
          {
            "id": 29,
            "text": "76164"
          },
          {
            "id": 30,
            "text": "95815"
          },
          {
            "id": 31,
            "text": "95828"
          },
          {
            "id": 32,
            "text": "32808"
          },
          {
            "id": 33,
            "text": "35208"
          },
          {
            "id": 34,
            "text": "76119"
          },
          {
            "id": 35,
            "text": "32209"
          },
          {
            "id": 36,
            "text": "73114"
          },
          {
            "id": 37,
            "text": "95610"
          },
          {
            "id": 38,
            "text": "93304"
          },
          {
            "id": 39,
            "text": "93727"
          },
          {
            "id": 40,
            "text": "93722"
          },
          {
            "id": 41,
            "text": "32205"
          },
          {
            "id": 42,
            "text": "32811"
          },
          {
            "id": 43,
            "text": "95621"
          },
          {
            "id": 44,
            "text": "93705"
          },
          {
            "id": 45,
            "text": "73107"
          },
          {
            "id": 46,
            "text": "93710"
          },
          {
            "id": 47,
            "text": "85713"
          },
          {
            "id": 48,
            "text": "75042"
          },
          {
            "id": 49,
            "text": "75227"
          },
          {
            "id": 50,
            "text": "32210"
          },
          {
            "id": 51,
            "text": "85705"
          },
          {
            "id": 52,
            "text": "75216"
          },
          {
            "id": 53,
            "text": "75203"
          },
          {
            "id": 54,
            "text": "34787"
          },
          {
            "id": 55,
            "text": "32818"
          },
          {
            "id": 56,
            "text": "75228"
          },
          {
            "id": 57,
            "text": "32839"
          },
          {
            "id": 58,
            "text": "35211"
          },
          {
            "id": 59,
            "text": "93726"
          },
          {
            "id": 60,
            "text": "93612"
          },
          {
            "id": 61,
            "text": "75217"
          },
          {
            "id": 62,
            "text": "32208"
          },
          {
            "id": 63,
            "text": "32822"
          },
          {
            "id": 64,
            "text": "75211"
          },
          {
            "id": 65,
            "text": "32825"
          },
          {
            "id": 66,
            "text": "93206"
          },
          {
            "id": 67,
            "text": "35218"
          },
          {
            "id": 68,
            "text": "85706"
          },
          {
            "id": 69,
            "text": "85711"
          },
          {
            "id": 70,
            "text": "75224"
          },
          {
            "id": 71,
            "text": "32703"
          },
          {
            "id": 72,
            "text": "95624"
          },
          {
            "id": 73,
            "text": "35217"
          },
          {
            "id": 74,
            "text": "32211"
          },
          {
            "id": 75,
            "text": "34761"
          },
          {
            "id": 76,
            "text": "95670"
          },
          {
            "id": 77,
            "text": "93662"
          },
          {
            "id": 78,
            "text": "75210"
          },
          {
            "id": 79,
            "text": "32792"
          },
          {
            "id": 80,
            "text": "70118"
          },
          {
            "id": 81,
            "text": "70116"
          },
          {
            "id": 82,
            "text": "70115"
          },
          {
            "id": 83,
            "text": "70117"
          },
          {
            "id": 84,
            "text": "73112"
          },
          {
            "id": 85,
            "text": "35214"
          },
          {
            "id": 86,
            "text": "35207"
          },
          {
            "id": 87,
            "text": "32254"
          },
          {
            "id": 88,
            "text": "75150"
          },
          {
            "id": 89,
            "text": "32789"
          },
          {
            "id": 90,
            "text": "32244"
          },
          {
            "id": 91,
            "text": "35212"
          },
          {
            "id": 92,
            "text": "85756"
          },
          {
            "id": 93,
            "text": "32712"
          },
          {
            "id": 94,
            "text": "85747"
          },
          {
            "id": 95,
            "text": "70119"
          },
          {
            "id": 96,
            "text": "75040"
          },
          {
            "id": 97,
            "text": "75212"
          },
          {
            "id": 98,
            "text": "32218"
          },
          {
            "id": 99,
            "text": "85710"
          },
          {
            "id": 100,
            "text": "75231"
          },
          {
            "id": 101,
            "text": "70127"
          },
          {
            "id": 102,
            "text": "93711"
          },
          {
            "id": 103,
            "text": "70114"
          },
          {
            "id": 104,
            "text": "35215"
          },
          {
            "id": 105,
            "text": "75241"
          },
          {
            "id": 106,
            "text": "75232"
          },
          {
            "id": 107,
            "text": "70126"
          },
          {
            "id": 108,
            "text": "70122"
          },
          {
            "id": 109,
            "text": "32277"
          },
          {
            "id": 110,
            "text": "85746"
          },
          {
            "id": 111,
            "text": "85653"
          },
          {
            "id": 112,
            "text": "85321"
          },
          {
            "id": 113,
            "text": "85743"
          },
          {
            "id": 114,
            "text": "35224"
          },
          {
            "id": 115,
            "text": "85303"
          },
          {
            "id": 116,
            "text": "85037"
          },
          {
            "id": 117,
            "text": "85033"
          },
          {
            "id": 118,
            "text": "85035"
          },
          {
            "id": 119,
            "text": "85009"
          },
          {
            "id": 120,
            "text": "85017"
          },
          {
            "id": 121,
            "text": "85019"
          },
          {
            "id": 122,
            "text": "85210"
          },
          {
            "id": 123,
            "text": "85382"
          },
          {
            "id": 124,
            "text": "85201"
          },
          {
            "id": 125,
            "text": "85204"
          },
          {
            "id": 126,
            "text": "85205"
          },
          {
            "id": 127,
            "text": "85345"
          },
          {
            "id": 128,
            "text": "85304"
          },
          {
            "id": 129,
            "text": "85302"
          },
          {
            "id": 130,
            "text": "85301"
          },
          {
            "id": 131,
            "text": "85029"
          },
          {
            "id": 132,
            "text": "85032"
          },
          {
            "id": 133,
            "text": "92503"
          },
          {
            "id": 134,
            "text": "92507"
          },
          {
            "id": 135,
            "text": "92557"
          },
          {
            "id": 136,
            "text": "92553"
          },
          {
            "id": 137,
            "text": "92509"
          },
          {
            "id": 138,
            "text": "92570"
          },
          {
            "id": 139,
            "text": "92583"
          },
          {
            "id": 140,
            "text": "92544"
          },
          {
            "id": 141,
            "text": "92543"
          },
          {
            "id": 142,
            "text": "92324"
          },
          {
            "id": 143,
            "text": "92201"
          },
          {
            "id": 144,
            "text": "92376"
          },
          {
            "id": 145,
            "text": "92410"
          },
          {
            "id": 146,
            "text": "92407"
          },
          {
            "id": 147,
            "text": "92373"
          },
          {
            "id": 148,
            "text": "92335"
          },
          {
            "id": 149,
            "text": "92336"
          },
          {
            "id": 150,
            "text": "92418"
          },
          {
            "id": 151,
            "text": "92345"
          },
          {
            "id": 152,
            "text": "92394"
          },
          {
            "id": 153,
            "text": "95209"
          },
          {
            "id": 154,
            "text": "95207"
          },
          {
            "id": 155,
            "text": "95210"
          },
          {
            "id": 156,
            "text": "95205"
          },
          {
            "id": 157,
            "text": "95206"
          },
          {
            "id": 158,
            "text": "95336"
          },
          {
            "id": 159,
            "text": "95337"
          },
          {
            "id": 160,
            "text": "95376"
          },
          {
            "id": 161,
            "text": "95377"
          },
          {
            "id": 162,
            "text": "95350"
          },
          {
            "id": 163,
            "text": "95351"
          },
          {
            "id": 164,
            "text": "95355"
          },
          {
            "id": 165,
            "text": "95354"
          },
          {
            "id": 166,
            "text": "6108"
          },
          {
            "id": 167,
            "text": "6118"
          },
          {
            "id": 168,
            "text": "6106"
          },
          {
            "id": 169,
            "text": "6112"
          },
          {
            "id": 170,
            "text": "6114"
          },
          {
            "id": 171,
            "text": "6120"
          },
          {
            "id": 172,
            "text": "6042"
          },
          {
            "id": 173,
            "text": "6040"
          },
          {
            "id": 174,
            "text": "6053"
          },
          {
            "id": 175,
            "text": "6051"
          },
          {
            "id": 176,
            "text": "6052"
          },
          {
            "id": 177,
            "text": "83605"
          },
          {
            "id": 178,
            "text": "83651"
          },
          {
            "id": 179,
            "text": "83686"
          },
          {
            "id": 180,
            "text": "83687"
          },
          {
            "id": 181,
            "text": "83607"
          },
          {
            "id": 182,
            "text": "28306"
          },
          {
            "id": 183,
            "text": "28314"
          },
          {
            "id": 184,
            "text": "28311"
          },
          {
            "id": 185,
            "text": "28301"
          },
          {
            "id": 186,
            "text": "27701"
          },
          {
            "id": 187,
            "text": "27703"
          },
          {
            "id": 188,
            "text": "27707"
          },
          {
            "id": 189,
            "text": "27704"
          },
          {
            "id": 190,
            "text": "27801"
          },
          {
            "id": 191,
            "text": "27896"
          },
          {
            "id": 192,
            "text": "27804"
          },
          {
            "id": 193,
            "text": "27893"
          },
          {
            "id": 194,
            "text": "74126"
          },
          {
            "id": 195,
            "text": "74127"
          },
          {
            "id": 196,
            "text": "74056"
          },
          {
            "id": 197,
            "text": "74110"
          },
          {
            "id": 198,
            "text": "74070"
          },
          {
            "id": 199,
            "text": "74637"
          },
          {
            "id": 200,
            "text": "74063"
          },
          {
            "id": 201,
            "text": "74115"
          },
          {
            "id": 202,
            "text": "74112"
          },
          {
            "id": 203,
            "text": "74106"
          },
          {
            "id": 204,
            "text": "74128"
          },
          {
            "id": 205,
            "text": "74108"
          },
          {
            "id": 206,
            "text": "74012"
          },
          {
            "id": 207,
            "text": "74014"
          },
          {
            "id": 208,
            "text": "15059"
          },
          {
            "id": 209,
            "text": "2863"
          },
          {
            "id": 210,
            "text": "2905"
          },
          {
            "id": 211,
            "text": "2920"
          },
          {
            "id": 212,
            "text": "2910"
          },
          {
            "id": 213,
            "text": "2904"
          },
          {
            "id": 214,
            "text": "2860"
          },
          {
            "id": 215,
            "text": "2861"
          },
          {
            "id": 216,
            "text": "2908"
          },
          {
            "id": 217,
            "text": "2907"
          },
          {
            "id": 218,
            "text": "2909"
          },
          {
            "id": 219,
            "text": "78621"
          },
          {
            "id": 220,
            "text": "78610"
          },
          {
            "id": 221,
            "text": "78640"
          }
        ]
      },
      "relationship": {
        "label": "Zip Code",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644725
        ],
        "options": []
      },
      "section-separator": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market-status": {
        "label": "Market Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Off Market"
          },
          {
            "id": 2,
            "text": "Sold"
          },
          {
            "id": 3,
            "text": "Fail"
          },
          {
            "id": 4,
            "text": "Active"
          },
          {
            "id": 5,
            "text": "Pending"
          },
          {
            "id": 6,
            "text": "Unknown"
          },
          {
            "id": 7,
            "text": "Contingent"
          }
        ]
      },
      "purchase-info": {
        "label": "Purchase Info",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-label": {
        "label": "MLS Label",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-sale-price-2": {
        "label": "Last Sale Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-value-2": {
        "label": "Estimated Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "price-off-value": {
        "label": "Price off Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-listed-price": {
        "label": "MLS Listed Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-sold-date": {
        "label": "MLS Sold Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-sold-price": {
        "label": "MLS Sold Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-score": {
        "label": "AI Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "percent-off": {
        "label": "Percent Off",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "smart-cash-offer-2": {
        "label": "Smart Cash Offer",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "potential-flip-spread": {
        "label": "Potential Flip Spread",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ppsf": {
        "label": "PPSF",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ppu": {
        "label": "PPU",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "initial-contact-date": {
        "label": "Initial Contact Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ppbd-bed": {
        "label": "PPBD (Bed)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "send-sms": {
        "label": "❌ SEND SMS",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Stage #1"
          },
          {
            "id": 2,
            "text": "Stage #2"
          },
          {
            "id": 3,
            "text": "Stage #3"
          },
          {
            "id": 4,
            "text": "Stage #4"
          }
        ]
      },
      "property-class": {
        "label": "Property Class",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Residential"
          },
          {
            "id": 2,
            "text": "Vacant"
          },
          {
            "id": 3,
            "text": "Exempt"
          },
          {
            "id": 4,
            "text": "Commercial"
          }
        ]
      },
      "section-separator-2": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property-type": {
        "label": "Property Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Single Family"
          },
          {
            "id": 2,
            "text": "Multi-Family"
          },
          {
            "id": 3,
            "text": "Vacant Land"
          },
          {
            "id": 4,
            "text": "Apartment"
          },
          {
            "id": 5,
            "text": "Other"
          },
          {
            "id": 6,
            "text": "Townhouse"
          },
          {
            "id": 7,
            "text": "Mobile Home"
          },
          {
            "id": 8,
            "text": "Condominium"
          }
        ]
      },
      "offer-status": {
        "label": "Offer Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Offer Sent"
          },
          {
            "id": 2,
            "text": "Offer Accepted"
          },
          {
            "id": 3,
            "text": "Counter Offer"
          },
          {
            "id": 4,
            "text": "Offer Rejected"
          },
          {
            "id": 5,
            "text": "Offer Follow Up"
          }
        ]
      },
      "property-style": {
        "label": "Property Style",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Duplex"
          },
          {
            "id": 2,
            "text": "Custom"
          },
          {
            "id": 3,
            "text": "MultiFamily"
          },
          {
            "id": 4,
            "text": "Ranch\\Rambler"
          },
          {
            "id": 5,
            "text": "Triplex"
          },
          {
            "id": 6,
            "text": "Quadplex"
          },
          {
            "id": 7,
            "text": "unknown"
          },
          {
            "id": 8,
            "text": "Conventional"
          },
          {
            "id": 9,
            "text": "TownHouse"
          },
          {
            "id": 10,
            "text": "Traditional"
          },
          {
            "id": 11,
            "text": "CONDO"
          },
          {
            "id": 12,
            "text": "Mediterranean"
          },
          {
            "id": 13,
            "text": "Mobile Home"
          },
          {
            "id": 14,
            "text": "Contemporary"
          },
          {
            "id": 15,
            "text": "Bungalow"
          },
          {
            "id": 16,
            "text": "Modern"
          },
          {
            "id": 17,
            "text": "Colonial"
          },
          {
            "id": 18,
            "text": "Tudor"
          },
          {
            "id": 19,
            "text": "Other"
          },
          {
            "id": 20,
            "text": "Cape Cod"
          },
          {
            "id": 21,
            "text": "Split Level"
          },
          {
            "id": 22,
            "text": "Raised Ranch"
          },
          {
            "id": 23,
            "text": "Historical"
          },
          {
            "id": 24,
            "text": "Bi-Level"
          },
          {
            "id": 25,
            "text": "Log Cabin/Rustic"
          },
          {
            "id": 26,
            "text": "Tri-Level"
          },
          {
            "id": 27,
            "text": "Prefab, Modular"
          },
          {
            "id": 28,
            "text": "Cottage"
          },
          {
            "id": 29,
            "text": "Victorian"
          },
          {
            "id": 30,
            "text": "High-rise"
          },
          {
            "id": 31,
            "text": "Split Foyer"
          },
          {
            "id": 32,
            "text": "Row Home"
          },
          {
            "id": 33,
            "text": "Unfinished\\Under Construction"
          },
          {
            "id": 34,
            "text": "English"
          },
          {
            "id": 35,
            "text": "Patio Home"
          },
          {
            "id": 36,
            "text": "Spanish"
          },
          {
            "id": 37,
            "text": "Mansion"
          },
          {
            "id": 38,
            "text": "French Provincial"
          },
          {
            "id": 39,
            "text": "Cluster"
          }
        ]
      },
      "smart-cash-offer": {
        "label": "Smart Cash Offer",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "stories": {
        "label": "Stories",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "1 Story"
          },
          {
            "id": 2,
            "text": "2 Stories"
          },
          {
            "id": 3,
            "text": "1.5 Stories"
          },
          {
            "id": 4,
            "text": "3 Stories"
          },
          {
            "id": 5,
            "text": "2.5 Stories"
          },
          {
            "id": 6,
            "text": "10 Stories"
          },
          {
            "id": 7,
            "text": "1.75 Stories"
          },
          {
            "id": 8,
            "text": "4 Stories"
          },
          {
            "id": 9,
            "text": "1.25 Stories"
          },
          {
            "id": 10,
            "text": "6 Stories"
          },
          {
            "id": 11,
            "text": "2.75 Stories"
          },
          {
            "id": 12,
            "text": "2.25 Stories"
          },
          {
            "id": 13,
            "text": "5 Stories"
          },
          {
            "id": 14,
            "text": "19 Stories"
          },
          {
            "id": 15,
            "text": "11 Stories"
          },
          {
            "id": 16,
            "text": "13 Stories"
          },
          {
            "id": 17,
            "text": "8 Stories"
          },
          {
            "id": 18,
            "text": "12 Stories"
          },
          {
            "id": 19,
            "text": "7 Stories"
          },
          {
            "id": 20,
            "text": "4.5 Stories"
          },
          {
            "id": 21,
            "text": "9 Stories"
          },
          {
            "id": 22,
            "text": "18 Stories"
          },
          {
            "id": 23,
            "text": "22 Stories"
          },
          {
            "id": 24,
            "text": "31 Stories"
          },
          {
            "id": 25,
            "text": "16 Stories"
          },
          {
            "id": 26,
            "text": "17 Stories"
          }
        ]
      },
      "offer-vs-loan": {
        "label": "Offer VS Loan",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Free and Clear"
          },
          {
            "id": 2,
            "text": "Offer < Loan"
          },
          {
            "id": 3,
            "text": "Offer > Loan (Clear)"
          },
          {
            "id": 4,
            "text": "Offer ≈ Loan"
          }
        ]
      },
      "number-of-units": {
        "label": "Number of Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "offer-vs-last-purchase-price": {
        "label": "Offer VS Last Purchase Price",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No Purchase Data"
          },
          {
            "id": 2,
            "text": "Offer < Purchase"
          },
          {
            "id": 3,
            "text": "Offer > Purchase (Win)"
          },
          {
            "id": 4,
            "text": "Offer ≈ Purchase"
          }
        ]
      },
      "number-of-commercial-units": {
        "label": "Number of Commercial Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "purchase-options": {
        "label": "Purchase Options",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "CASH"
          },
          {
            "id": 2,
            "text": "SF"
          },
          {
            "id": 3,
            "text": "SUBTO"
          },
          {
            "id": 4,
            "text": "LO"
          }
        ]
      },
      "number-of-buildings": {
        "label": "Number of Buildings",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sq-ft-per-unit": {
        "label": "Sq Ft per Unit",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contact-made": {
        "label": "Contact Made",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No Contact"
          },
          {
            "id": 2,
            "text": "\"Called, No Answer + Left VM\", Sent SMS"
          },
          {
            "id": 3,
            "text": "\"Called, No Interest/Motivation\""
          },
          {
            "id": 4,
            "text": "\"Called, No Answer + Left VM\""
          },
          {
            "id": 5,
            "text": "SMS Sent"
          }
        ]
      },
      "beds-per-unit": {
        "label": "Beds per Unit",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-outbound": {
        "label": "Last Outbound",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "motivation-layers": {
        "label": "Motivation Layers",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-inbound": {
        "label": "Last Inbound",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-9": {
        "label": ">",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sms-template": {
        "label": "SMS Templates",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "linked-number": {
        "label": "SMS Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30541677
        ],
        "options": []
      },
      "field-8": {
        "label": ">",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sms-messages": {
        "label": "SMS Messages",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30541680
        ],
        "options": []
      },
      "field-20": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "lead-promotion-date": {
        "label": "Lead Promotion Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "bedrooms": {
        "label": "Bedrooms",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "3"
          },
          {
            "id": 2,
            "text": "2"
          },
          {
            "id": 3,
            "text": "4"
          },
          {
            "id": 4,
            "text": "0"
          },
          {
            "id": 5,
            "text": "8"
          },
          {
            "id": 6,
            "text": "6"
          },
          {
            "id": 7,
            "text": "5"
          },
          {
            "id": 8,
            "text": "1"
          },
          {
            "id": 9,
            "text": "7"
          },
          {
            "id": 10,
            "text": "10"
          },
          {
            "id": 11,
            "text": "9"
          },
          {
            "id": 12,
            "text": "12"
          },
          {
            "id": 13,
            "text": "15"
          },
          {
            "id": 14,
            "text": "24"
          },
          {
            "id": 15,
            "text": "26"
          },
          {
            "id": 16,
            "text": "16"
          },
          {
            "id": 17,
            "text": "11"
          },
          {
            "id": 18,
            "text": "18"
          },
          {
            "id": 19,
            "text": "20"
          },
          {
            "id": 20,
            "text": "14"
          },
          {
            "id": 21,
            "text": "21"
          },
          {
            "id": 22,
            "text": "13"
          },
          {
            "id": 23,
            "text": "41"
          },
          {
            "id": 24,
            "text": "40"
          },
          {
            "id": 25,
            "text": "17"
          },
          {
            "id": 26,
            "text": "33"
          },
          {
            "id": 27,
            "text": "49"
          },
          {
            "id": 28,
            "text": "32"
          },
          {
            "id": 29,
            "text": "66"
          },
          {
            "id": 30,
            "text": "45"
          },
          {
            "id": 31,
            "text": "39"
          },
          {
            "id": 32,
            "text": "68"
          },
          {
            "id": 33,
            "text": "27"
          },
          {
            "id": 34,
            "text": "30"
          },
          {
            "id": 35,
            "text": "29"
          },
          {
            "id": 36,
            "text": "51"
          },
          {
            "id": 37,
            "text": "34"
          },
          {
            "id": 38,
            "text": "23"
          },
          {
            "id": 39,
            "text": "52"
          },
          {
            "id": 40,
            "text": "47"
          },
          {
            "id": 41,
            "text": "99"
          },
          {
            "id": 42,
            "text": "69"
          },
          {
            "id": 43,
            "text": "54"
          },
          {
            "id": 44,
            "text": "22"
          },
          {
            "id": 45,
            "text": "28"
          },
          {
            "id": 46,
            "text": "60"
          },
          {
            "id": 47,
            "text": "36"
          },
          {
            "id": 48,
            "text": "48"
          },
          {
            "id": 49,
            "text": "88"
          },
          {
            "id": 50,
            "text": "42"
          },
          {
            "id": 51,
            "text": "108"
          },
          {
            "id": 52,
            "text": "53"
          },
          {
            "id": 53,
            "text": "38"
          },
          {
            "id": 54,
            "text": "59"
          },
          {
            "id": 55,
            "text": "50"
          },
          {
            "id": 56,
            "text": "67"
          },
          {
            "id": 57,
            "text": "82"
          },
          {
            "id": 58,
            "text": "104"
          },
          {
            "id": 59,
            "text": "63"
          },
          {
            "id": 60,
            "text": "62"
          },
          {
            "id": 61,
            "text": "44"
          },
          {
            "id": 62,
            "text": "46"
          },
          {
            "id": 63,
            "text": "98"
          },
          {
            "id": 64,
            "text": "35"
          },
          {
            "id": 65,
            "text": "76"
          },
          {
            "id": 66,
            "text": "56"
          },
          {
            "id": 67,
            "text": "19"
          },
          {
            "id": 68,
            "text": "93"
          },
          {
            "id": 69,
            "text": "72"
          },
          {
            "id": 70,
            "text": "57"
          },
          {
            "id": 71,
            "text": "86"
          },
          {
            "id": 72,
            "text": "31"
          },
          {
            "id": 73,
            "text": "65"
          },
          {
            "id": 74,
            "text": "80"
          },
          {
            "id": 75,
            "text": "216"
          },
          {
            "id": 76,
            "text": "260"
          },
          {
            "id": 77,
            "text": "275"
          },
          {
            "id": 78,
            "text": "87"
          },
          {
            "id": 79,
            "text": "25"
          },
          {
            "id": 80,
            "text": "360"
          },
          {
            "id": 81,
            "text": "152"
          },
          {
            "id": 82,
            "text": "105"
          },
          {
            "id": 83,
            "text": "111"
          },
          {
            "id": 84,
            "text": "96"
          },
          {
            "id": 85,
            "text": "114"
          },
          {
            "id": 86,
            "text": "155"
          },
          {
            "id": 87,
            "text": "245"
          },
          {
            "id": 88,
            "text": "146"
          },
          {
            "id": 89,
            "text": "161"
          },
          {
            "id": 90,
            "text": "117"
          },
          {
            "id": 91,
            "text": "204"
          },
          {
            "id": 92,
            "text": "394"
          },
          {
            "id": 93,
            "text": "348"
          },
          {
            "id": 94,
            "text": "264"
          },
          {
            "id": 95,
            "text": "324"
          },
          {
            "id": 96,
            "text": "120"
          },
          {
            "id": 97,
            "text": "189"
          },
          {
            "id": 98,
            "text": "350"
          },
          {
            "id": 99,
            "text": "382"
          },
          {
            "id": 100,
            "text": "121"
          },
          {
            "id": 101,
            "text": "78"
          },
          {
            "id": 102,
            "text": "244"
          },
          {
            "id": 103,
            "text": "270"
          },
          {
            "id": 104,
            "text": "272"
          },
          {
            "id": 105,
            "text": "100"
          },
          {
            "id": 106,
            "text": "384"
          },
          {
            "id": 107,
            "text": "422"
          },
          {
            "id": 108,
            "text": "160"
          },
          {
            "id": 109,
            "text": "240"
          },
          {
            "id": 110,
            "text": "326"
          },
          {
            "id": 111,
            "text": "233"
          },
          {
            "id": 112,
            "text": "232"
          },
          {
            "id": 113,
            "text": "130"
          },
          {
            "id": 114,
            "text": "220"
          },
          {
            "id": 115,
            "text": "374"
          },
          {
            "id": 116,
            "text": "288"
          },
          {
            "id": 117,
            "text": "327"
          },
          {
            "id": 118,
            "text": "375"
          },
          {
            "id": 119,
            "text": "283"
          },
          {
            "id": 120,
            "text": "287"
          },
          {
            "id": 121,
            "text": "408"
          },
          {
            "id": 122,
            "text": "200"
          },
          {
            "id": 123,
            "text": "176"
          },
          {
            "id": 124,
            "text": "336"
          },
          {
            "id": 125,
            "text": "286"
          },
          {
            "id": 126,
            "text": "538"
          },
          {
            "id": 127,
            "text": "647"
          },
          {
            "id": 128,
            "text": "372"
          },
          {
            "id": 129,
            "text": "84"
          },
          {
            "id": 130,
            "text": "113"
          },
          {
            "id": 131,
            "text": "258"
          },
          {
            "id": 132,
            "text": "223"
          },
          {
            "id": 133,
            "text": "251"
          },
          {
            "id": 134,
            "text": "210"
          },
          {
            "id": 135,
            "text": "156"
          },
          {
            "id": 136,
            "text": "248"
          },
          {
            "id": 137,
            "text": "423"
          },
          {
            "id": 138,
            "text": "122"
          },
          {
            "id": 139,
            "text": "368"
          },
          {
            "id": 140,
            "text": "199"
          },
          {
            "id": 141,
            "text": "119"
          },
          {
            "id": 142,
            "text": "230"
          },
          {
            "id": 143,
            "text": "196"
          },
          {
            "id": 144,
            "text": "228"
          },
          {
            "id": 145,
            "text": "106"
          },
          {
            "id": 146,
            "text": "134"
          },
          {
            "id": 147,
            "text": "180"
          },
          {
            "id": 148,
            "text": "110"
          },
          {
            "id": 149,
            "text": "192"
          },
          {
            "id": 150,
            "text": "208"
          },
          {
            "id": 151,
            "text": "458"
          },
          {
            "id": 152,
            "text": "330"
          },
          {
            "id": 153,
            "text": "135"
          },
          {
            "id": 154,
            "text": "454"
          },
          {
            "id": 155,
            "text": "344"
          },
          {
            "id": 156,
            "text": "404"
          },
          {
            "id": 157,
            "text": "181"
          },
          {
            "id": 158,
            "text": "528"
          },
          {
            "id": 159,
            "text": "246"
          },
          {
            "id": 160,
            "text": "229"
          },
          {
            "id": 161,
            "text": "305"
          },
          {
            "id": 162,
            "text": "90"
          },
          {
            "id": 163,
            "text": "696"
          },
          {
            "id": 164,
            "text": "190"
          },
          {
            "id": 165,
            "text": "300"
          },
          {
            "id": 166,
            "text": "352"
          },
          {
            "id": 167,
            "text": "370"
          },
          {
            "id": 168,
            "text": "280"
          },
          {
            "id": 169,
            "text": "128"
          },
          {
            "id": 170,
            "text": "94"
          },
          {
            "id": 171,
            "text": "592"
          },
          {
            "id": 172,
            "text": "127"
          },
          {
            "id": 173,
            "text": "224"
          },
          {
            "id": 174,
            "text": "396"
          },
          {
            "id": 175,
            "text": "268"
          },
          {
            "id": 176,
            "text": "444"
          },
          {
            "id": 177,
            "text": "291"
          },
          {
            "id": 178,
            "text": "397"
          },
          {
            "id": 179,
            "text": "187"
          },
          {
            "id": 180,
            "text": "118"
          },
          {
            "id": 181,
            "text": "79"
          },
          {
            "id": 182,
            "text": "450"
          },
          {
            "id": 183,
            "text": "217"
          },
          {
            "id": 184,
            "text": "307"
          }
        ]
      },
      "seller-lead": {
        "label": "Seller Lead",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "bathrooms": {
        "label": "Bathrooms",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "1"
          },
          {
            "id": 2,
            "text": "2"
          },
          {
            "id": 3,
            "text": "8"
          },
          {
            "id": 4,
            "text": "4"
          },
          {
            "id": 5,
            "text": "3"
          },
          {
            "id": 6,
            "text": "1.5"
          },
          {
            "id": 7,
            "text": "2.5"
          },
          {
            "id": 8,
            "text": "4.5"
          },
          {
            "id": 9,
            "text": "7"
          },
          {
            "id": 10,
            "text": "5"
          },
          {
            "id": 11,
            "text": "6"
          },
          {
            "id": 12,
            "text": "3.5"
          },
          {
            "id": 13,
            "text": "1.75"
          },
          {
            "id": 14,
            "text": "17.25"
          },
          {
            "id": 15,
            "text": "11"
          },
          {
            "id": 16,
            "text": "17"
          },
          {
            "id": 17,
            "text": "9"
          },
          {
            "id": 18,
            "text": "15"
          },
          {
            "id": 19,
            "text": "10"
          },
          {
            "id": 20,
            "text": "16"
          },
          {
            "id": 21,
            "text": "24"
          },
          {
            "id": 22,
            "text": "12"
          },
          {
            "id": 23,
            "text": "18"
          },
          {
            "id": 24,
            "text": "8.5"
          },
          {
            "id": 25,
            "text": "5.5"
          },
          {
            "id": 26,
            "text": "4.25"
          },
          {
            "id": 27,
            "text": "2.75"
          },
          {
            "id": 28,
            "text": "13"
          },
          {
            "id": 29,
            "text": "20"
          },
          {
            "id": 30,
            "text": "2.25"
          },
          {
            "id": 31,
            "text": "14"
          },
          {
            "id": 32,
            "text": "6.5"
          },
          {
            "id": 33,
            "text": "1.25"
          },
          {
            "id": 34,
            "text": "21"
          },
          {
            "id": 35,
            "text": "3.25"
          },
          {
            "id": 36,
            "text": "0"
          },
          {
            "id": 37,
            "text": "6.25"
          },
          {
            "id": 38,
            "text": "3.75"
          },
          {
            "id": 39,
            "text": "5.75"
          },
          {
            "id": 40,
            "text": "5.25"
          },
          {
            "id": 41,
            "text": "47"
          },
          {
            "id": 42,
            "text": "45"
          },
          {
            "id": 43,
            "text": "30"
          },
          {
            "id": 44,
            "text": "13.5"
          },
          {
            "id": 45,
            "text": "10.5"
          },
          {
            "id": 46,
            "text": "22"
          },
          {
            "id": 47,
            "text": "19.5"
          },
          {
            "id": 48,
            "text": "0.5"
          },
          {
            "id": 49,
            "text": "4.75"
          },
          {
            "id": 50,
            "text": "7.5"
          },
          {
            "id": 51,
            "text": "19"
          },
          {
            "id": 52,
            "text": "68"
          },
          {
            "id": 53,
            "text": "9.5"
          },
          {
            "id": 54,
            "text": "32"
          },
          {
            "id": 55,
            "text": "28"
          },
          {
            "id": 56,
            "text": "85"
          },
          {
            "id": 57,
            "text": "87"
          },
          {
            "id": 58,
            "text": "99"
          },
          {
            "id": 59,
            "text": "35"
          },
          {
            "id": 60,
            "text": "50"
          },
          {
            "id": 61,
            "text": "40"
          },
          {
            "id": 62,
            "text": "21.75"
          },
          {
            "id": 63,
            "text": "31"
          },
          {
            "id": 64,
            "text": "42"
          },
          {
            "id": 65,
            "text": "12.5"
          },
          {
            "id": 66,
            "text": "26"
          },
          {
            "id": 67,
            "text": "65"
          },
          {
            "id": 68,
            "text": "152"
          },
          {
            "id": 69,
            "text": "72"
          },
          {
            "id": 70,
            "text": "54"
          },
          {
            "id": 71,
            "text": "105"
          },
          {
            "id": 72,
            "text": "143"
          },
          {
            "id": 73,
            "text": "93"
          },
          {
            "id": 74,
            "text": "90"
          },
          {
            "id": 75,
            "text": "48"
          },
          {
            "id": 76,
            "text": "52"
          },
          {
            "id": 77,
            "text": "51"
          },
          {
            "id": 78,
            "text": "60"
          },
          {
            "id": 79,
            "text": "66"
          },
          {
            "id": 80,
            "text": "114"
          },
          {
            "id": 81,
            "text": "141"
          },
          {
            "id": 82,
            "text": "245"
          },
          {
            "id": 83,
            "text": "133"
          },
          {
            "id": 84,
            "text": "44"
          },
          {
            "id": 85,
            "text": "57"
          },
          {
            "id": 86,
            "text": "56"
          }
        ]
      },
      "square-feet": {
        "label": "Square Feet",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "stage": {
        "label": "Stage",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Future Follow Up"
          },
          {
            "id": 2,
            "text": "Stage #2 - Interest Filter"
          },
          {
            "id": 3,
            "text": "Opt-Out"
          },
          {
            "id": 4,
            "text": "Stage #6 - Make Offer"
          }
        ]
      },
      "sq-ft-range": {
        "label": "Sq Ft Range",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "0–1000"
          },
          {
            "id": 2,
            "text": "Non-SFR"
          }
        ]
      },
      "status": {
        "label": "Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Not Interested"
          },
          {
            "id": 2,
            "text": "Owner Verified"
          },
          {
            "id": 3,
            "text": "Opt-Out"
          },
          {
            "id": 4,
            "text": "Not Owner"
          },
          {
            "id": 5,
            "text": "Messaged"
          },
          {
            "id": 6,
            "text": "Offer Sent"
          },
          {
            "id": 7,
            "text": "New"
          },
          {
            "id": 8,
            "text": "Follow Up"
          },
          {
            "id": 9,
            "text": "Dead"
          },
          {
            "id": 10,
            "text": "Make Offer"
          }
        ]
      },
      "year-build": {
        "label": "Year Build",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "effective-year-build": {
        "label": "Effective Year Build",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "construction-type": {
        "label": "Construction Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Frame"
          },
          {
            "id": 2,
            "text": "Masonry"
          },
          {
            "id": 3,
            "text": "Wood"
          },
          {
            "id": 4,
            "text": "Brick"
          },
          {
            "id": 5,
            "text": "Concrete"
          },
          {
            "id": 6,
            "text": "Steel"
          },
          {
            "id": 7,
            "text": "Other"
          },
          {
            "id": 8,
            "text": "Manufactured"
          },
          {
            "id": 9,
            "text": "Concrete Block"
          },
          {
            "id": 10,
            "text": "Stone"
          },
          {
            "id": 11,
            "text": "Tilt-up (pre-cast concrete)"
          },
          {
            "id": 12,
            "text": "Metal"
          },
          {
            "id": 13,
            "text": "Adobe"
          }
        ]
      },
      "exterior-walls": {
        "label": "Exterior Walls",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Stucco"
          },
          {
            "id": 2,
            "text": "Concrete Block"
          },
          {
            "id": 3,
            "text": "Other"
          },
          {
            "id": 4,
            "text": "Wood"
          },
          {
            "id": 5,
            "text": "Brick veneer"
          },
          {
            "id": 6,
            "text": "Brick"
          },
          {
            "id": 7,
            "text": "Asbestos shingle"
          },
          {
            "id": 8,
            "text": "Wood Shingle"
          },
          {
            "id": 9,
            "text": "Combination"
          },
          {
            "id": 10,
            "text": "Concrete"
          },
          {
            "id": 11,
            "text": "Siding (Alum/Vinyl)"
          },
          {
            "id": 12,
            "text": "Composition/Composite"
          },
          {
            "id": 13,
            "text": "Block"
          },
          {
            "id": 14,
            "text": "Wood Siding"
          },
          {
            "id": 15,
            "text": "Shingle (Not Wood)"
          },
          {
            "id": 16,
            "text": "Metal"
          },
          {
            "id": 17,
            "text": "Rock, Stone"
          },
          {
            "id": 18,
            "text": "Siding Not (aluminum, vinyl, etc.)"
          },
          {
            "id": 19,
            "text": "Adobe"
          },
          {
            "id": 20,
            "text": "Fiber cement siding (Hardi-board/Hardi-plank)"
          },
          {
            "id": 21,
            "text": "Masonry"
          },
          {
            "id": 22,
            "text": "Log"
          },
          {
            "id": 23,
            "text": "Vinyl siding"
          },
          {
            "id": 24,
            "text": "Tile"
          },
          {
            "id": 25,
            "text": "Glass"
          },
          {
            "id": 26,
            "text": "Aluminum siding"
          },
          {
            "id": 27,
            "text": "Tilt-up (pre-cast concrete)"
          }
        ]
      },
      "floor-cover": {
        "label": "Floor Cover",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Carpet"
          },
          {
            "id": 2,
            "text": "Wood"
          },
          {
            "id": 3,
            "text": "Tile"
          },
          {
            "id": 4,
            "text": "Cork"
          },
          {
            "id": 5,
            "text": "Vinyl"
          },
          {
            "id": 6,
            "text": "Concrete"
          },
          {
            "id": 7,
            "text": "Plywood"
          },
          {
            "id": 8,
            "text": "Ceramic"
          },
          {
            "id": 9,
            "text": "Terrazzo"
          },
          {
            "id": 10,
            "text": "Parquet"
          },
          {
            "id": 11,
            "text": "Linoleum"
          },
          {
            "id": 12,
            "text": "Covered"
          },
          {
            "id": 13,
            "text": "Floating Floor/laminate"
          },
          {
            "id": 14,
            "text": "Slate"
          },
          {
            "id": 15,
            "text": "Marble"
          }
        ]
      },
      "basement": {
        "label": "Basement",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No Basement"
          },
          {
            "id": 2,
            "text": "Unspecified Basement"
          },
          {
            "id": 3,
            "text": "Unfinished Basement"
          },
          {
            "id": 4,
            "text": "Partial Basement"
          },
          {
            "id": 5,
            "text": "Full Basement"
          },
          {
            "id": 6,
            "text": "Improved Basement (Finished)"
          },
          {
            "id": 7,
            "text": "Daylight, Full"
          },
          {
            "id": 8,
            "text": "Daylight, Partial"
          }
        ]
      },
      "other-rooms": {
        "label": "Other Rooms",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Family Room/Den"
          },
          {
            "id": 2,
            "text": "Utility room"
          },
          {
            "id": 3,
            "text": "Bonus Room"
          },
          {
            "id": 4,
            "text": "Sun, Solarium, Florida room"
          },
          {
            "id": 5,
            "text": "Game / Recreation room"
          },
          {
            "id": 6,
            "text": "Laundry Room"
          }
        ]
      },
      "number-of-fireplaces": {
        "label": "Number of Fireplaces",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "patio": {
        "label": "Patio",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Patio - Screened"
          },
          {
            "id": 2,
            "text": "Patio - Unknown"
          }
        ]
      },
      "last-sale-price": {
        "label": "Last Sale Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "porch": {
        "label": "Porch",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Porch"
          },
          {
            "id": 2,
            "text": "Porch - Open"
          },
          {
            "id": 3,
            "text": "Porch screened"
          },
          {
            "id": 4,
            "text": "Portico (drive under)"
          },
          {
            "id": 5,
            "text": "Porch covered"
          }
        ]
      },
      "deck": {
        "label": "Deck",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No"
          },
          {
            "id": 2,
            "text": "Yes"
          }
        ]
      },
      "driveway": {
        "label": "Driveway",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Gravel"
          },
          {
            "id": 2,
            "text": "Unknown"
          },
          {
            "id": 3,
            "text": "Asphalt"
          },
          {
            "id": 4,
            "text": "Concrete"
          },
          {
            "id": 5,
            "text": "Paver"
          },
          {
            "id": 6,
            "text": "Bomanite"
          }
        ]
      },
      "garage": {
        "label": "Garage",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Garage"
          },
          {
            "id": 2,
            "text": "Attached Garage"
          },
          {
            "id": 3,
            "text": "Carport"
          },
          {
            "id": 4,
            "text": "Detached Garage"
          },
          {
            "id": 5,
            "text": "Covered"
          },
          {
            "id": 6,
            "text": "None"
          },
          {
            "id": 7,
            "text": "Mixed"
          },
          {
            "id": 8,
            "text": "Underground/Basement"
          },
          {
            "id": 9,
            "text": "Paved/Surfaced"
          },
          {
            "id": 10,
            "text": "Finished - Detached"
          },
          {
            "id": 11,
            "text": "Built-in"
          },
          {
            "id": 12,
            "text": "Open"
          },
          {
            "id": 13,
            "text": "Tuckunder"
          }
        ]
      },
      "garage-square-feet": {
        "label": "Garage Square Feet",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "air-conditioning": {
        "label": "Air Conditioning",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Evaporative Cooler"
          },
          {
            "id": 2,
            "text": "Central"
          },
          {
            "id": 3,
            "text": "Yes"
          },
          {
            "id": 4,
            "text": "Wall"
          },
          {
            "id": 5,
            "text": "Window/Unit"
          },
          {
            "id": 6,
            "text": "Packaged Unit"
          },
          {
            "id": 7,
            "text": "Refrigeration"
          },
          {
            "id": 8,
            "text": "None"
          },
          {
            "id": 9,
            "text": "Partial"
          },
          {
            "id": 10,
            "text": "Chilled Water"
          },
          {
            "id": 11,
            "text": "Other"
          }
        ]
      },
      "heating-type": {
        "label": "Heating Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Electric"
          },
          {
            "id": 2,
            "text": "Floor/Wall"
          },
          {
            "id": 3,
            "text": "Central"
          },
          {
            "id": 4,
            "text": "Yes"
          },
          {
            "id": 5,
            "text": "Convection"
          },
          {
            "id": 6,
            "text": "Space/Suspended"
          },
          {
            "id": 7,
            "text": "Forced air unit"
          },
          {
            "id": 8,
            "text": "None"
          },
          {
            "id": 9,
            "text": "Gravity"
          },
          {
            "id": 10,
            "text": "Solar"
          },
          {
            "id": 11,
            "text": "Radiant"
          },
          {
            "id": 12,
            "text": "Gas"
          },
          {
            "id": 13,
            "text": "Heat Pump"
          },
          {
            "id": 14,
            "text": "Oil"
          },
          {
            "id": 15,
            "text": "Steam"
          },
          {
            "id": 16,
            "text": "Hot Water"
          },
          {
            "id": 17,
            "text": "Zone"
          },
          {
            "id": 18,
            "text": "Baseboard"
          },
          {
            "id": 19,
            "text": "Vent"
          },
          {
            "id": 20,
            "text": "Other"
          },
          {
            "id": 21,
            "text": "Wood Burning"
          },
          {
            "id": 22,
            "text": "Partial"
          }
        ]
      },
      "heating-fuel-type": {
        "label": "Heating Fuel Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Electric"
          },
          {
            "id": 2,
            "text": "Gas"
          },
          {
            "id": 3,
            "text": "Solar"
          },
          {
            "id": 4,
            "text": "Oil"
          },
          {
            "id": 5,
            "text": "None"
          },
          {
            "id": 6,
            "text": "Coal"
          },
          {
            "id": 7,
            "text": "Wood"
          }
        ]
      },
      "interior-walls": {
        "label": "Interior Walls",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Plaster"
          },
          {
            "id": 2,
            "text": "Gypsum Board/Drywall/Sheetrock/Wallboard"
          },
          {
            "id": 3,
            "text": "Plywood/Minimum"
          },
          {
            "id": 4,
            "text": "Wood"
          },
          {
            "id": 5,
            "text": "Paneling"
          },
          {
            "id": 6,
            "text": "Other"
          },
          {
            "id": 7,
            "text": "Masonry"
          },
          {
            "id": 8,
            "text": "Finished/Painted"
          },
          {
            "id": 9,
            "text": "Unfinished"
          },
          {
            "id": 10,
            "text": "Vinyl"
          },
          {
            "id": 11,
            "text": "Decorative\\Custom"
          }
        ]
      },
      "roof-cover": {
        "label": "Roof Cover",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Wood Shake/ Shingles"
          },
          {
            "id": 2,
            "text": "Built-up"
          },
          {
            "id": 3,
            "text": "Composition Shingle"
          },
          {
            "id": 4,
            "text": "Other"
          },
          {
            "id": 5,
            "text": "Asphalt"
          },
          {
            "id": 6,
            "text": "Tar & Gravel"
          },
          {
            "id": 7,
            "text": "Metal"
          },
          {
            "id": 8,
            "text": "Concrete"
          },
          {
            "id": 9,
            "text": "Asbestos"
          },
          {
            "id": 10,
            "text": "Tile"
          },
          {
            "id": 11,
            "text": "Wood"
          },
          {
            "id": 12,
            "text": "Rock / Gravel"
          },
          {
            "id": 13,
            "text": "Aluminum"
          },
          {
            "id": 14,
            "text": "Slate"
          },
          {
            "id": 15,
            "text": "Steel"
          },
          {
            "id": 16,
            "text": "Shingle (Not Wood)"
          },
          {
            "id": 17,
            "text": "Roll Composition"
          },
          {
            "id": 18,
            "text": "Clay tile"
          },
          {
            "id": 19,
            "text": "Fiberglass"
          }
        ]
      },
      "roof-type": {
        "label": "Roof Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Hip"
          },
          {
            "id": 2,
            "text": "Mansard"
          },
          {
            "id": 3,
            "text": "Gable or Hip"
          },
          {
            "id": 4,
            "text": "Gable"
          },
          {
            "id": 5,
            "text": "Flat"
          },
          {
            "id": 6,
            "text": "Irr/Cathedral"
          },
          {
            "id": 7,
            "text": "Gambrel"
          },
          {
            "id": 8,
            "text": "Dome"
          },
          {
            "id": 9,
            "text": "Sawtooth"
          },
          {
            "id": 10,
            "text": "Wood Truss"
          },
          {
            "id": 11,
            "text": "Shed"
          },
          {
            "id": 12,
            "text": "Rigid Frm Bar Jt"
          },
          {
            "id": 13,
            "text": "Bowstring Truss"
          },
          {
            "id": 14,
            "text": "Steel Frame/Truss"
          },
          {
            "id": 15,
            "text": "Prestress Concrete"
          }
        ]
      },
      "pool": {
        "label": "Pool",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No"
          },
          {
            "id": 2,
            "text": "Pool (yes)"
          },
          {
            "id": 3,
            "text": "Spa or Hot Tub (only)"
          },
          {
            "id": 4,
            "text": "Above ground pool"
          },
          {
            "id": 5,
            "text": "Pool & Spa (both)"
          },
          {
            "id": 6,
            "text": "Solar Heated"
          },
          {
            "id": 7,
            "text": "Heated Pool"
          },
          {
            "id": 8,
            "text": "In-Ground Pool"
          },
          {
            "id": 9,
            "text": "Vinyl In-ground Pool"
          },
          {
            "id": 10,
            "text": "Community Pool or Spa"
          },
          {
            "id": 11,
            "text": "Indoor Swimming Pool"
          },
          {
            "id": 12,
            "text": "Enclosed"
          }
        ]
      },
      "field-10": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-sale-date": {
        "label": "Last Sale Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "years-since-last-sale": {
        "label": "Ownership Years",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-sale-document": {
        "label": "Last Sale Document",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Grant Deed"
          },
          {
            "id": 2,
            "text": "Warranty Deed"
          },
          {
            "id": 3,
            "text": "Special Warranty Deed"
          },
          {
            "id": 4,
            "text": "Executor’s Deed"
          },
          {
            "id": 5,
            "text": "Vendor’s Lien Warranty Deed"
          },
          {
            "id": 6,
            "text": "Deed"
          },
          {
            "id": 7,
            "text": "Public Action"
          },
          {
            "id": 8,
            "text": "Intrafamily Transfer"
          },
          {
            "id": 9,
            "text": "Corporation Deed"
          },
          {
            "id": 10,
            "text": "Joint Tenancy Deed"
          },
          {
            "id": 11,
            "text": "Cash Sale Deed"
          },
          {
            "id": 12,
            "text": "Correction Document"
          },
          {
            "id": 13,
            "text": "Quit Claim Deed"
          },
          {
            "id": 14,
            "text": "Individual Deed"
          },
          {
            "id": 15,
            "text": "Trustee’s Deed"
          },
          {
            "id": 16,
            "text": "Sheriff’s Deed"
          },
          {
            "id": 17,
            "text": "Foreclosure"
          },
          {
            "id": 18,
            "text": "Administrator’s Deed"
          },
          {
            "id": 19,
            "text": "Conservator’s Deed"
          },
          {
            "id": 20,
            "text": "Re-recorded Document"
          },
          {
            "id": 21,
            "text": "Partnership Deed"
          },
          {
            "id": 22,
            "text": "Other"
          },
          {
            "id": 23,
            "text": "Personal Representatives Deed"
          },
          {
            "id": 24,
            "text": "Survivorship Deed/Survivor Property Agreement"
          },
          {
            "id": 25,
            "text": "Deed in Lieu of Foreclosure"
          },
          {
            "id": 26,
            "text": "Contract of Sale"
          },
          {
            "id": 27,
            "text": "Deed of Distribution"
          },
          {
            "id": 28,
            "text": "Limited Warranty Deed"
          },
          {
            "id": 29,
            "text": "Land Contract"
          },
          {
            "id": 30,
            "text": "Agreement of Sale"
          },
          {
            "id": 31,
            "text": "Beneficiary Deed"
          },
          {
            "id": 32,
            "text": "Legal Action/Court Order"
          },
          {
            "id": 33,
            "text": "Deed of Guardian"
          },
          {
            "id": 34,
            "text": "Bargain and Sale Deed"
          },
          {
            "id": 35,
            "text": "Affidavit of Death of Joint Tenant"
          },
          {
            "id": 36,
            "text": "Redemption Deed"
          },
          {
            "id": 37,
            "text": "Commissioner’s Deed"
          },
          {
            "id": 38,
            "text": "Gift Deed"
          },
          {
            "id": 39,
            "text": "Transaction History Record"
          },
          {
            "id": 40,
            "text": "Quit Claim Deed (arms length)"
          },
          {
            "id": 41,
            "text": "Fiduciary Deed"
          },
          {
            "id": 42,
            "text": "Receiver’s Deed"
          },
          {
            "id": 43,
            "text": "Certificate of Transfer"
          },
          {
            "id": 44,
            "text": "Transfer on Death Deed"
          },
          {
            "id": 45,
            "text": "Special Master Deed"
          },
          {
            "id": 46,
            "text": "Assignment Deed"
          },
          {
            "id": 47,
            "text": "Affidavit"
          },
          {
            "id": 48,
            "text": "Referee’s Deed"
          },
          {
            "id": 49,
            "text": "Affidavit of Death of Life Tenant"
          },
          {
            "id": 50,
            "text": "Distress Sale"
          },
          {
            "id": 51,
            "text": "Assignment of Lease"
          },
          {
            "id": 52,
            "text": "Ground Lease"
          },
          {
            "id": 53,
            "text": "Exchange"
          },
          {
            "id": 54,
            "text": "Condominium Deed"
          },
          {
            "id": 55,
            "text": "Lease"
          }
        ]
      },
      "estimated-value": {
        "label": "Estimated Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-equity-amount": {
        "label": "Estimated Equity Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-equity-percent": {
        "label": "Estimated Equity Percent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-11": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-delinquent-2": {
        "label": "Tax Delinquent",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No"
          },
          {
            "id": 2,
            "text": "Yes"
          }
        ]
      },
      "tax-delinquent-year": {
        "label": "Tax Delinquent Year",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-amount": {
        "label": "Tax Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-assessment-year": {
        "label": "Tax Assessment Year",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "accessed-total-value": {
        "label": "Accessed Total Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculated-total-value": {
        "label": "Calculated Total Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "accessed-land-value": {
        "label": "Accessed Land Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculated-land-value": {
        "label": "Calculated Land Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "accessed-improvement-value": {
        "label": "Accessed Improvement Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculated-improvement-value": {
        "label": "Calculated Improvement Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-13": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-loan-amount": {
        "label": "Total Loan Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-loan-balance": {
        "label": "Total Loan Balance",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-loan-payment": {
        "label": "Total Loan Payment",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-6": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-repair-cost": {
        "label": "Estimated Repair Cost",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-34": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "building-quality": {
        "label": "Building Quality",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "C"
          },
          {
            "id": 2,
            "text": "D"
          },
          {
            "id": 3,
            "text": "E"
          },
          {
            "id": 4,
            "text": "E-"
          },
          {
            "id": 5,
            "text": "B"
          },
          {
            "id": 6,
            "text": "A"
          },
          {
            "id": 7,
            "text": "C+"
          },
          {
            "id": 8,
            "text": "B+"
          },
          {
            "id": 9,
            "text": "D+"
          },
          {
            "id": 10,
            "text": "C-"
          },
          {
            "id": 11,
            "text": "D-"
          },
          {
            "id": 12,
            "text": "B-"
          },
          {
            "id": 13,
            "text": "E+"
          }
        ]
      },
      "hoa-name": {
        "label": "HOA Name",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Brentwood Homeowners Association"
          },
          {
            "id": 2,
            "text": "OAKS AT HILLTOP RANCH HOA"
          },
          {
            "id": 3,
            "text": "Lincoln Crossing Community Association"
          },
          {
            "id": 4,
            "text": "Arbor Ridge Homeowners' Association of Apopka, Inc."
          },
          {
            "id": 5,
            "text": "TRACT NO. 3545"
          },
          {
            "id": 6,
            "text": "PINON SPRINGS VILLAGE HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 7,
            "text": "Legacy Lane Home Owner's Association"
          },
          {
            "id": 8,
            "text": "SUNSET VILLAS ASSOCIATION"
          },
          {
            "id": 9,
            "text": "LYNN CREEK HILLS HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 10,
            "text": "The Home Owners Association of"
          },
          {
            "id": 11,
            "text": "Valencia Homeowners Association"
          },
          {
            "id": 12,
            "text": "Laguna Pointe Owners Association"
          },
          {
            "id": 13,
            "text": "Laguna Park Plaza Owner Association"
          },
          {
            "id": 14,
            "text": "Laguna West Association"
          },
          {
            "id": 15,
            "text": "Tyner Ranch II Homeowners Association"
          },
          {
            "id": 16,
            "text": "The Willows Homeowners Association of Orlando, Inc."
          },
          {
            "id": 17,
            "text": "Hiawassee Landings Owner's Association, Inc."
          },
          {
            "id": 18,
            "text": "SOUTHWOOD DUPLEX"
          },
          {
            "id": 19,
            "text": "Westwood Village"
          },
          {
            "id": 20,
            "text": "DAYSTAR II"
          },
          {
            "id": 21,
            "text": "KERN CITY CIVIC"
          },
          {
            "id": 22,
            "text": "CAL MUNICIPAL FINANCE AUTHORITY"
          },
          {
            "id": 23,
            "text": "PRATAP"
          },
          {
            "id": 24,
            "text": "SUMMERSET AT BRENTWOOD II ASSOCIATION"
          },
          {
            "id": 25,
            "text": "Natoma Meadows HOA"
          },
          {
            "id": 26,
            "text": "REGENCY PLACE CONDOMINIUM OWNERS ASSOCIATION"
          },
          {
            "id": 27,
            "text": "Morningstar Drive"
          },
          {
            "id": 28,
            "text": "Thomas Kelly Management, Inc."
          },
          {
            "id": 29,
            "text": "Stonelake Master Association"
          },
          {
            "id": 30,
            "text": "Cripple Creek Condominium Association"
          },
          {
            "id": 31,
            "text": "CITY IN THE HILLS"
          },
          {
            "id": 32,
            "text": "CANDLERIDGE FORT WORTH HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 33,
            "text": "FLOWER CONDO LLC"
          },
          {
            "id": 34,
            "text": "HERITAGE HOMEOWNERS ASSOCIATIO"
          },
          {
            "id": 35,
            "text": "SPORTLAND COURTS OWNERS ASSOCIATION"
          },
          {
            "id": 36,
            "text": "NORTHBOROUGH"
          },
          {
            "id": 37,
            "text": "RIDGEPOINT"
          },
          {
            "id": 38,
            "text": "CYPRESS GLEN MASTER HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 39,
            "text": "SILVERADO HILLS HOA"
          },
          {
            "id": 40,
            "text": "Sunrise Meadows Homeowners Assoc"
          },
          {
            "id": 41,
            "text": "CRISTO PARA TODOS MINISTRIES"
          },
          {
            "id": 42,
            "text": "The Aspens at Laguna"
          },
          {
            "id": 43,
            "text": "IRON BLOSAM OWNERS ASSOCIATION"
          },
          {
            "id": 44,
            "text": "THE LOMA VERDE"
          },
          {
            "id": 45,
            "text": "SAUNDERS PARK VILLA HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 46,
            "text": "Carriage Crossing II Association, Inc."
          },
          {
            "id": 47,
            "text": "Seven Oaks HomeOwners Association"
          },
          {
            "id": 48,
            "text": "PARKS OF DEER CREEK HOA INC"
          },
          {
            "id": 49,
            "text": "ENCHANTED BAY HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 50,
            "text": "Rancho Murieta Association"
          },
          {
            "id": 51,
            "text": "TILLERMAN HILLS HOMEOWNERS ASSCOCIATION"
          },
          {
            "id": 52,
            "text": "Diamond Property Management"
          },
          {
            "id": 53,
            "text": "TYNER RANCH"
          },
          {
            "id": 54,
            "text": "The La Reserve Community Association"
          },
          {
            "id": 55,
            "text": "Aloma Park Homeowners Association"
          },
          {
            "id": 56,
            "text": "North Lawne Villas Homeowners Association, Inc."
          },
          {
            "id": 57,
            "text": "Anatolia Units 1, 2, and 4"
          },
          {
            "id": 58,
            "text": "River City Commons Association"
          },
          {
            "id": 59,
            "text": "LE MY NGOC/HOA THE NGUYEN"
          },
          {
            "id": 60,
            "text": "CALIFORNIA GARDENS PARCEL MAP 10700"
          },
          {
            "id": 61,
            "text": "Seven Palms HOA"
          },
          {
            "id": 62,
            "text": "Oakland Village"
          },
          {
            "id": 63,
            "text": "MARKET CONDOMINIUM ASSOCIATION INC"
          },
          {
            "id": 64,
            "text": "Sherbrooke Townhomes Homeowners Association, Inc."
          },
          {
            "id": 65,
            "text": "Whispering Pines Homeowners Association of Jacksonville, Inc."
          },
          {
            "id": 66,
            "text": "CALIFORNIA MUNICIPAL FINANCE AUTHORITY"
          },
          {
            "id": 67,
            "text": "LE MY N/HOA THE NGUYEN"
          },
          {
            "id": 68,
            "text": "HOA if applicable - need info"
          },
          {
            "id": 69,
            "text": "Wildwood Homeowners Association"
          },
          {
            "id": 70,
            "text": "PRIMAVERA"
          },
          {
            "id": 71,
            "text": "Parkway Estates Neighborhood Association"
          },
          {
            "id": 72,
            "text": "Beverly Lotts Inc HOA"
          },
          {
            "id": 73,
            "text": "FRUITRIDGE VISTA UNIT NO 19 LOT OWNERS ASSOCIATION"
          },
          {
            "id": 74,
            "text": "SCOTTSDALE VILLAGE"
          },
          {
            "id": 75,
            "text": "ELO RESTORATION INC"
          },
          {
            "id": 76,
            "text": "VISTA WEST HOMEOWNERS ASSOCIAT"
          },
          {
            "id": 77,
            "text": "Atriums Civic Improvement Association, Inc."
          },
          {
            "id": 78,
            "text": "CLARCONA RESORT CONDOMINIUM ASSN INC"
          },
          {
            "id": 79,
            "text": "Rose Point Homeowners Association, Inc."
          },
          {
            "id": 80,
            "text": "Colby Management, Inc"
          },
          {
            "id": 81,
            "text": "Hidden Valley Community Association"
          },
          {
            "id": 82,
            "text": "Vierra Moore,Inc."
          },
          {
            "id": 83,
            "text": "GOLD RIVER COMMUNITY"
          },
          {
            "id": 84,
            "text": "Westridge-Fair Oaks HOA"
          },
          {
            "id": 85,
            "text": "LEMON HILL ESTATES"
          },
          {
            "id": 86,
            "text": "FirstService Residential"
          },
          {
            "id": 87,
            "text": "Gold Ridge Forest Property Owners Asso"
          },
          {
            "id": 88,
            "text": "GOOSE LAKE WATER PROPERTY OWNERS ASSOCIATION"
          },
          {
            "id": 89,
            "text": "Spectrum Property Services"
          },
          {
            "id": 90,
            "text": "The Parcel Map No. 81-59 property owners"
          },
          {
            "id": 91,
            "text": "I & I PROPERTY C/O"
          },
          {
            "id": 92,
            "text": "LA RESERVE COMMUNITY ASSN"
          },
          {
            "id": 93,
            "text": "First American Property & Casualty"
          },
          {
            "id": 94,
            "text": "Serrano El Dorado Owners Association"
          },
          {
            "id": 95,
            "text": "North Country Village"
          },
          {
            "id": 96,
            "text": "ALPINE FOREST PARK PROPERTY OWNERS ASSOCIATION INC"
          },
          {
            "id": 97,
            "text": "FT WORTH & WOODHAVEN CONDO ASSN"
          },
          {
            "id": 98,
            "text": "Sutton Ridge Homeowners Association, Inc."
          },
          {
            "id": 99,
            "text": "LOVELL TERRACE PROPERTY OWNERS ASSN INC"
          },
          {
            "id": 100,
            "text": "Villages of Bartram Springs Owners Association, Inc."
          },
          {
            "id": 101,
            "text": "L &amp"
          },
          {
            "id": 102,
            "text": "L Demolition &amp"
          },
          {
            "id": 103,
            "text": "Salvage, Inc."
          },
          {
            "id": 104,
            "text": "SYCAMORE CREEK COMMUNITY ASSOCIATION"
          },
          {
            "id": 105,
            "text": "Center Mall Association"
          },
          {
            "id": 106,
            "text": "SUNRISE"
          },
          {
            "id": 107,
            "text": "TRACT 3545 WESTWOOD ASSOCIATION"
          },
          {
            "id": 108,
            "text": "THE 1421 CHARTRES ST CONDOMINIUM"
          },
          {
            "id": 109,
            "text": "Emerald Greens Homeowners Association"
          },
          {
            "id": 110,
            "text": "Easton Homeowners Association, Inc."
          },
          {
            "id": 111,
            "text": "Residences at Wynnfield Lakes Owners Ass"
          },
          {
            "id": 112,
            "text": "The Willows First Addition Homeowners Association, Inc."
          },
          {
            "id": 113,
            "text": "Diable Grande Residential"
          },
          {
            "id": 114,
            "text": "Pinewood Villas Homeowners' Association, Inc."
          },
          {
            "id": 115,
            "text": "Twin Rivers Homeowners Association"
          },
          {
            "id": 116,
            "text": "Mabury Manor HOA"
          },
          {
            "id": 117,
            "text": "Morgan Creek Community Association"
          },
          {
            "id": 118,
            "text": "Liberty Mutual"
          },
          {
            "id": 119,
            "text": "NGUYEN TRI MINH/HOA THI PHAM"
          },
          {
            "id": 120,
            "text": "The Hillside of Oakwood Villa Estates Owners Association Inc."
          },
          {
            "id": 121,
            "text": "COVERED BRIDGE AT CURRY FORD WOODS ASSOCIATION INC"
          },
          {
            "id": 122,
            "text": "Pueblo Gardens HOA"
          },
          {
            "id": 123,
            "text": "Blue Stem Ridge HOA"
          },
          {
            "id": 124,
            "text": "FORECLOSURE COMMISIONER"
          },
          {
            "id": 125,
            "text": "Woodside Condominiums Woodside Associati"
          },
          {
            "id": 126,
            "text": "OF THE CONDOMINIUM"
          },
          {
            "id": 127,
            "text": "PATRIOT VILLAGE HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 128,
            "text": "Lace Fern Village Homeowners' Association, Inc."
          },
          {
            "id": 129,
            "text": "Ventura Country Club Community Homeowners Association, Inc."
          },
          {
            "id": 130,
            "text": "BELLAS CATALINAS HOMEOWNERS ASSN"
          },
          {
            "id": 131,
            "text": "WELLINGTON HOMEOWNERS ASSOCIAT"
          },
          {
            "id": 132,
            "text": "FOREST WEST OWNERS ASSN INC"
          },
          {
            "id": 133,
            "text": "Northpointe"
          },
          {
            "id": 134,
            "text": "Natomas Park"
          },
          {
            "id": 135,
            "text": "NATIONWIDE RECONVEYANCE LLC"
          },
          {
            "id": 136,
            "text": "VILLAGE STINE"
          },
          {
            "id": 137,
            "text": "Crestview 1 @ Anaverde"
          },
          {
            "id": 138,
            "text": "Recromax, LLC"
          },
          {
            "id": 139,
            "text": "UNKNOWN"
          },
          {
            "id": 140,
            "text": "Midvale Park Master Review Board, Inc"
          },
          {
            "id": 141,
            "text": "Globolink Management"
          },
          {
            "id": 142,
            "text": "ORLANDO CITY"
          },
          {
            "id": 143,
            "text": "University Garden Community Association, Inc"
          },
          {
            "id": 144,
            "text": "POYNTER CROSSING HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 145,
            "text": "US BANK NATIONAL ASSOCIATION"
          },
          {
            "id": 146,
            "text": "CROSSWOODS"
          },
          {
            "id": 147,
            "text": "SUMMERPLACE"
          },
          {
            "id": 148,
            "text": "Rudy & Mary H. Hinojosa"
          },
          {
            "id": 149,
            "text": "Habitat for Humanity of Jacksonville"
          },
          {
            "id": 150,
            "text": "Campbell Improvement Association"
          },
          {
            "id": 151,
            "text": "Cowell HOA Inc."
          },
          {
            "id": 152,
            "text": "COLOMA ROAD - MILLS RANCH"
          },
          {
            "id": 153,
            "text": "EDEN VILLAS GARDEN COURT TOWNHOUSES ASSOCIATION"
          },
          {
            "id": 154,
            "text": "Richwood Homeowners Association, Inc."
          },
          {
            "id": 155,
            "text": "CREEKSIDE CIRCLE"
          },
          {
            "id": 156,
            "text": "KIRKWOOD PLACE"
          },
          {
            "id": 157,
            "text": "Oasis Property Owners Association"
          },
          {
            "id": 158,
            "text": "BAKERSFIELD FRENCH QUARTER"
          },
          {
            "id": 159,
            "text": "STATE FARM"
          },
          {
            "id": 160,
            "text": "HUNTINGTON PARK CONDOMINIUM VILLAGE COMMUNITY ASSOCIATION"
          },
          {
            "id": 161,
            "text": "Cypress Glen II"
          },
          {
            "id": 162,
            "text": "Orchard Park HOA"
          },
          {
            "id": 163,
            "text": "SYCAMORE LANDING HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 164,
            "text": "Errol Estate Property Owners' Association, Inc."
          },
          {
            "id": 165,
            "text": "LEXINGTON SQUARE MAINERANCE ASSOCIATION"
          },
          {
            "id": 166,
            "text": "FOREST LAKES ESTATES HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 167,
            "text": "CMA Management"
          },
          {
            "id": 168,
            "text": "HARVEST RIDGE HOME OWNERS ASSOCIATION"
          },
          {
            "id": 169,
            "text": "Pueblo Inc."
          },
          {
            "id": 170,
            "text": "SUNSET HILLS HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 171,
            "text": "CAMBRICK PLACE CONDO"
          },
          {
            "id": 172,
            "text": "LOS ENCINOS HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 173,
            "text": "VILLAGES OF RUNYON SPRINGS HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 174,
            "text": "Principal Management Group"
          },
          {
            "id": 175,
            "text": "PRAIRIE CREEK DALLAS HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 176,
            "text": "QUARTER RESIDENCES CONDO"
          },
          {
            "id": 177,
            "text": "HOA of Sandyland Estates"
          },
          {
            "id": 178,
            "text": "Wheatland Meadows HOA"
          },
          {
            "id": 179,
            "text": "GLEN OAKS TOWNHOMES CONDO"
          },
          {
            "id": 180,
            "text": "PV OF CARROLLTON HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 181,
            "text": "NORTHCREST HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 182,
            "text": "GRAND PRAIRIE LAKEWOOD HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 183,
            "text": "HARBOR POINTE HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 184,
            "text": "TRINITY FOREST DALLAS HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 185,
            "text": "Light Pointe Place HOA"
          },
          {
            "id": 186,
            "text": "Villa Del Mar HOA"
          },
          {
            "id": 187,
            "text": "Wheatland Hills Estates HOA"
          },
          {
            "id": 188,
            "text": "WATERVIEW COMMUNITY ASSOCIATION INC"
          },
          {
            "id": 189,
            "text": "ROYAL CENTRAL CONDOMINIUMS"
          },
          {
            "id": 190,
            "text": "Highport Estates HOA"
          },
          {
            "id": 191,
            "text": "1811 EUCLID HOMEOWNERS ASSOCIATION INC MANAGEMENT CERTIFICATE"
          },
          {
            "id": 192,
            "text": "The Belvedere Condos at State-Thomas Inc"
          },
          {
            "id": 193,
            "text": "SNL Associates, Inc."
          },
          {
            "id": 194,
            "text": "HEARTHSTONE ADDITION PHASES 1A 1B 2 3 HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 195,
            "text": "Lake Parks HOA"
          },
          {
            "id": 196,
            "text": "CAMBRIDGE CONDO OWNERS ASSN"
          },
          {
            "id": 197,
            "text": "COLLEGE PARK HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 198,
            "text": "VILLAGES OF ELDORADO II"
          },
          {
            "id": 199,
            "text": "JOBSON EAST HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 200,
            "text": "LAS COLINAS ASSOCIATION"
          },
          {
            "id": 201,
            "text": "Northview Place HOA"
          },
          {
            "id": 202,
            "text": "COUNTRY CREEK ASSOCIATION"
          },
          {
            "id": 203,
            "text": "SHERWOOD VILLAGE PROPERTY OWNERS ASSOCIATION"
          },
          {
            "id": 204,
            "text": "BRISTOL ON THE PARK HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 205,
            "text": "PALOS VERDES TOWNHOMES OWNERS ASSN INC"
          },
          {
            "id": 206,
            "text": "IMPRESSIONS PROPERTY OWNERS ASSOCIATION"
          },
          {
            "id": 207,
            "text": "CHISHOLM SPRINGS HOMEOWNERS ASSOCIATION INC"
          },
          {
            "id": 208,
            "text": "FOREST WEST OWNERS ASSOCIATION INC"
          },
          {
            "id": 209,
            "text": "Curtiss Wright Village HOA"
          },
          {
            "id": 210,
            "text": "GRAND PRAIRIE TOWNHOMES HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 211,
            "text": "STONEY CREEK MASTER COMMUNITY HOA INC"
          },
          {
            "id": 212,
            "text": "CARROLL AVE CONDOMINIUMS ASSOCIATION INC"
          },
          {
            "id": 213,
            "text": "ST JOSEPH CONDOS"
          },
          {
            "id": 214,
            "text": "CHISHOLM VILLAGE HOMEOWNERS ASSOCIATION CVHOA"
          },
          {
            "id": 215,
            "text": "LAKE WILLOW HOMEOWNERS ASSOC INC"
          },
          {
            "id": 216,
            "text": "FISHERMANS PARADISE PROPERTY OWNERS ASSOCIATION"
          },
          {
            "id": 217,
            "text": "COMMUNITY ASSOCIATES INC"
          },
          {
            "id": 218,
            "text": "ACORN COMMUNITY LAND ASSN OF LA"
          },
          {
            "id": 219,
            "text": "BACH HOA LLC"
          },
          {
            "id": 220,
            "text": "Colonial Lakes Homeowners Association, Inc."
          },
          {
            "id": 221,
            "text": "Southpointe Condominium Association, Inc."
          },
          {
            "id": 222,
            "text": "Lake Mann estates Neighborhood Assn."
          },
          {
            "id": 223,
            "text": "EDGAR QUINTIN INC"
          },
          {
            "id": 224,
            "text": "Las Alamedas Community Association, Inc."
          },
          {
            "id": 225,
            "text": "Internal Revenue Service"
          },
          {
            "id": 226,
            "text": "TOOLS ON WHEELS, INC."
          },
          {
            "id": 227,
            "text": "BALDWIN PARK RESIDENTIAL OWNERS ASSOCIATION INC"
          },
          {
            "id": 228,
            "text": "Hiawassee Point Homeowners Association, Inc."
          },
          {
            "id": 229,
            "text": "DEUTSCHE BANK NATIONAL TRUST COMPANY"
          },
          {
            "id": 230,
            "text": "WESTGATE LAKES OWNERS ASSN INC"
          },
          {
            "id": 231,
            "text": "Park Avenue Estates Homeowners' Association of Winter garden, Inc."
          },
          {
            "id": 232,
            "text": "Residences at Villa Medici Condominium Association, Inc."
          },
          {
            "id": 233,
            "text": "DEVONWOOD COMMUNITY ASSOCIATION INC"
          },
          {
            "id": 234,
            "text": "CARTER GLEN"
          },
          {
            "id": 235,
            "text": "Langdale Woods Homeowners Association, Inc."
          },
          {
            "id": 236,
            "text": "East Bay Homeowners, Inc"
          },
          {
            "id": 237,
            "text": "The HOA of Avalon Village, Inc."
          },
          {
            "id": 238,
            "text": "Sweetwater Country Club Homeowners Association, Inc."
          },
          {
            "id": 239,
            "text": "Quail Trail/Eastwood Terrace Community"
          },
          {
            "id": 240,
            "text": "Pine Ridge Hollow East Homeowners' Association, Inc."
          },
          {
            "id": 241,
            "text": "ISLANDS OF VALENCIA HOMEOWNERS ASSOCIATION"
          },
          {
            "id": 242,
            "text": "The Islands Homeowners Association, Inc."
          },
          {
            "id": 243,
            "text": "Valencia Greens Homeowners Association, Inc."
          },
          {
            "id": 244,
            "text": "Timberleaf Village Lot 2 - Phase 1 Homeowners Association, Inc."
          },
          {
            "id": 245,
            "text": "Clovercrest Village Homeowners Association, Inc."
          },
          {
            "id": 246,
            "text": "Park Lake Towers Condominium Association, Inc."
          },
          {
            "id": 247,
            "text": "Wintergreen at Winter Park Homeowners' Association, Inc."
          },
          {
            "id": 248,
            "text": "Springview Homeowners Association"
          },
          {
            "id": 249,
            "text": "Robinswood Community Improvement Association,"
          },
          {
            "id": 250,
            "text": "Piedmont Park Homeowners' Association, Inc."
          },
          {
            "id": 251,
            "text": "Brandywine Dubsdread East Home Owners Association, Inc."
          },
          {
            "id": 252,
            "text": "Lake Doe Estates Homeowners Association, Inc."
          },
          {
            "id": 253,
            "text": "Cedar Village Homeowners' Association, Inc."
          },
          {
            "id": 254,
            "text": "Timberleaf Master Association, Inc."
          },
          {
            "id": 255,
            "text": "SKY LAKE SOUTH HOMEOWNERS ASSN INC"
          },
          {
            "id": 256,
            "text": "SPRING RIDGE HOME OWNERS ASSOCIATION OF ORANGE COUNTY INC"
          },
          {
            "id": 257,
            "text": "Woodfield Oaks Community Association, Inc."
          },
          {
            "id": 258,
            "text": "Sheeler Oaks Community Association, Inc."
          }
        ]
      },
      "estimated-repair-cost-per-sq-ft": {
        "label": "Estimated Repair Cost Per Sq Ft",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "$20-$50"
          },
          {
            "id": 2,
            "text": "$50-$100"
          },
          {
            "id": 3,
            "text": "$10-$20"
          }
        ]
      },
      "hoa-type": {
        "label": "HOA Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "PUD"
          },
          {
            "id": 2,
            "text": "HOA"
          },
          {
            "id": 3,
            "text": "COA"
          }
        ]
      },
      "building-condition": {
        "label": "Building Condition",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Unsound"
          },
          {
            "id": 2,
            "text": "Very Good"
          },
          {
            "id": 3,
            "text": "Excellent"
          },
          {
            "id": 4,
            "text": "Good"
          },
          {
            "id": 5,
            "text": "Average"
          },
          {
            "id": 6,
            "text": "Unknown"
          },
          {
            "id": 7,
            "text": "Fair"
          },
          {
            "id": 8,
            "text": "Poor"
          }
        ]
      },
      "hoa-fee-amount": {
        "label": "HOA Fee Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "renovation-level": {
        "label": "Renovation Level",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Structural"
          },
          {
            "id": 2,
            "text": "Full Rehab"
          }
        ]
      },
      "field-15": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-7": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "auction-date": {
        "label": "Auction Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "legal-description": {
        "label": "Legal Description",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "recording-date": {
        "label": "Recording Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "apn-number": {
        "label": "APN Number",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "past-due-amount": {
        "label": "Past Due Amount",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "lot-size-acres": {
        "label": "Lot Size (Acres)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "default-date": {
        "label": "Default Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "lot-size-square-feet": {
        "label": "Lot Size (Square Feet)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "document-type": {
        "label": "Document Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sewer": {
        "label": "Sewer",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Municipal"
          },
          {
            "id": 2,
            "text": "Yes"
          },
          {
            "id": 3,
            "text": "Septic"
          },
          {
            "id": 4,
            "text": "Storm"
          },
          {
            "id": 5,
            "text": "None"
          }
        ]
      },
      "field-17": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "water": {
        "label": "Water",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Municipal"
          },
          {
            "id": 2,
            "text": "None"
          },
          {
            "id": 3,
            "text": "Yes"
          },
          {
            "id": 4,
            "text": "Cistern"
          },
          {
            "id": 5,
            "text": "Well"
          }
        ]
      },
      "property-tags": {
        "label": "Property Tags",
        "type": "category",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "High Equity"
          },
          {
            "id": 2,
            "text": "Tax Delinquent"
          },
          {
            "id": 3,
            "text": "Cash Buyer"
          },
          {
            "id": 4,
            "text": "Senior Owner"
          },
          {
            "id": 5,
            "text": "Tired Landlord"
          },
          {
            "id": 6,
            "text": "Out Of State Owner"
          },
          {
            "id": 7,
            "text": "Absentee Owner"
          },
          {
            "id": 8,
            "text": "Free And Clear"
          },
          {
            "id": 9,
            "text": "Adjustable Loan"
          },
          {
            "id": 10,
            "text": "Likely To Move"
          },
          {
            "id": 11,
            "text": "Vacant Home"
          },
          {
            "id": 12,
            "text": "Low Equity"
          },
          {
            "id": 13,
            "text": "Empty Nester"
          },
          {
            "id": 14,
            "text": "Corporate Owner"
          },
          {
            "id": 15,
            "text": "Probate"
          },
          {
            "id": 16,
            "text": "No Updates"
          },
          {
            "id": 17,
            "text": "Heavily Dated"
          },
          {
            "id": 18,
            "text": "Moderate Repairs"
          },
          {
            "id": 19,
            "text": "Major Repairs Needed"
          },
          {
            "id": 20,
            "text": "Minor Cosmetic Only"
          },
          {
            "id": 21,
            "text": "Long Term Owner"
          },
          {
            "id": 22,
            "text": "Mid-Term Owner"
          },
          {
            "id": 23,
            "text": "New Owner"
          },
          {
            "id": 24,
            "text": "Active Lien"
          },
          {
            "id": 25,
            "text": "Preforeclosure"
          },
          {
            "id": 26,
            "text": "Foreclosure"
          },
          {
            "id": 27,
            "text": "Bank Owned"
          },
          {
            "id": 28,
            "text": "Upcoming Auction"
          },
          {
            "id": 29,
            "text": "Off Market"
          },
          {
            "id": 30,
            "text": "Zombie Property"
          },
          {
            "id": 31,
            "text": "Phone 1: Wireless: Direct Match"
          },
          {
            "id": 32,
            "text": "Phone 1: Wireless: Last Name Match"
          },
          {
            "id": 33,
            "text": "Phone 1: Wireless: No Match"
          },
          {
            "id": 34,
            "text": "Phone 1: Landline: Direct Match"
          },
          {
            "id": 35,
            "text": "Phone 1: Landline: Last Name Match"
          },
          {
            "id": 36,
            "text": "Phone 1: Landline: No Match"
          },
          {
            "id": 37,
            "text": "Phone 2: Wireless: Direct Match"
          },
          {
            "id": 38,
            "text": "Phone 2: Wireless: Last Name Match"
          },
          {
            "id": 39,
            "text": "Phone 2: Wireless: No Match"
          },
          {
            "id": 40,
            "text": "Hoa Lien"
          },
          {
            "id": 41,
            "text": "Recently Sold"
          },
          {
            "id": 42,
            "text": "#VALUE!"
          }
        ]
      },
      "topography": {
        "label": "Topography",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "ROLLING"
          },
          {
            "id": 2,
            "text": "Level grade"
          },
          {
            "id": 3,
            "text": "Low Elevation"
          },
          {
            "id": 4,
            "text": "STEEP"
          },
          {
            "id": 5,
            "text": "Below street level"
          },
          {
            "id": 6,
            "text": "Above street level"
          },
          {
            "id": 7,
            "text": "High elevation"
          },
          {
            "id": 8,
            "text": "SWAMPY"
          },
          {
            "id": 9,
            "text": "ROCKY"
          },
          {
            "id": 10,
            "text": "WOODED"
          },
          {
            "id": 11,
            "text": "MIXED"
          }
        ]
      },
      "zoning": {
        "label": "Zoning",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "R1"
          },
          {
            "id": 2,
            "text": "R-1"
          },
          {
            "id": 3,
            "text": "R2"
          },
          {
            "id": 4,
            "text": "Z67"
          },
          {
            "id": 5,
            "text": "R-2A/T/AN"
          },
          {
            "id": 6,
            "text": "Z177"
          },
          {
            "id": 7,
            "text": "RD-5"
          },
          {
            "id": 8,
            "text": "R-4"
          },
          {
            "id": 9,
            "text": "R-1-EA-4 R"
          },
          {
            "id": 10,
            "text": "R5"
          },
          {
            "id": 11,
            "text": "Z190"
          },
          {
            "id": 12,
            "text": "RS"
          },
          {
            "id": 13,
            "text": "R-2"
          },
          {
            "id": 14,
            "text": "RMD-B"
          },
          {
            "id": 15,
            "text": "C-CBD"
          },
          {
            "id": 16,
            "text": "R-1-EA-4"
          },
          {
            "id": 17,
            "text": "R-3A/W"
          },
          {
            "id": 18,
            "text": "R4"
          },
          {
            "id": 19,
            "text": "R-2B"
          },
          {
            "id": 20,
            "text": "RD5"
          },
          {
            "id": 21,
            "text": "R1C"
          },
          {
            "id": 22,
            "text": "R-3"
          },
          {
            "id": 23,
            "text": "RMD-A"
          },
          {
            "id": 24,
            "text": "RLD-60"
          },
          {
            "id": 25,
            "text": "M-1"
          },
          {
            "id": 26,
            "text": "R-1A-SPD"
          },
          {
            "id": 27,
            "text": "RD-10"
          },
          {
            "id": 28,
            "text": "E"
          },
          {
            "id": 29,
            "text": "R-S"
          },
          {
            "id": 30,
            "text": "R3"
          },
          {
            "id": 31,
            "text": "RMX"
          },
          {
            "id": 32,
            "text": "R2A"
          },
          {
            "id": 33,
            "text": "SR1"
          },
          {
            "id": 34,
            "text": "D"
          },
          {
            "id": 35,
            "text": "Z69"
          },
          {
            "id": 36,
            "text": "R-3B"
          },
          {
            "id": 37,
            "text": "CRO"
          },
          {
            "id": 38,
            "text": "MU"
          },
          {
            "id": 39,
            "text": "I-G"
          },
          {
            "id": 40,
            "text": "RD-12"
          },
          {
            "id": 41,
            "text": "RD-7"
          },
          {
            "id": 42,
            "text": "RD10"
          },
          {
            "id": 43,
            "text": "M1"
          },
          {
            "id": 44,
            "text": "AE5"
          },
          {
            "id": 45,
            "text": "C1"
          },
          {
            "id": 46,
            "text": "Z164"
          },
          {
            "id": 47,
            "text": "Z239"
          },
          {
            "id": 48,
            "text": "R-NC"
          },
          {
            "id": 49,
            "text": "RMD-S"
          },
          {
            "id": 50,
            "text": "CCG-2"
          },
          {
            "id": 51,
            "text": "C-2"
          },
          {
            "id": 52,
            "text": "RD-6"
          },
          {
            "id": 53,
            "text": "R-1A"
          },
          {
            "id": 54,
            "text": "R-1-SC"
          },
          {
            "id": 55,
            "text": "SP"
          },
          {
            "id": 56,
            "text": "Z414"
          },
          {
            "id": 57,
            "text": "Z191"
          },
          {
            "id": 58,
            "text": "RD-4"
          },
          {
            "id": 59,
            "text": "R-1A-R"
          },
          {
            "id": 60,
            "text": "R-2-MH"
          },
          {
            "id": 61,
            "text": "RD-20"
          },
          {
            "id": 62,
            "text": "RF"
          },
          {
            "id": 63,
            "text": "RR"
          },
          {
            "id": 64,
            "text": "AE20"
          },
          {
            "id": 65,
            "text": "C-1"
          },
          {
            "id": 66,
            "text": "C-O"
          },
          {
            "id": 67,
            "text": "R-3B/AN"
          },
          {
            "id": 68,
            "text": "MUL"
          },
          {
            "id": 69,
            "text": "CP"
          },
          {
            "id": 70,
            "text": "Z413"
          },
          {
            "id": 71,
            "text": "B2"
          },
          {
            "id": 72,
            "text": "R-2A"
          },
          {
            "id": 73,
            "text": "SPA"
          },
          {
            "id": 74,
            "text": "A-10"
          },
          {
            "id": 75,
            "text": "P.U.D."
          },
          {
            "id": 76,
            "text": "RA"
          },
          {
            "id": 77,
            "text": "O3"
          },
          {
            "id": 78,
            "text": "Z59"
          },
          {
            "id": 79,
            "text": "C2"
          },
          {
            "id": 80,
            "text": "Z298"
          },
          {
            "id": 81,
            "text": "A"
          },
          {
            "id": 82,
            "text": "RD2"
          },
          {
            "id": 83,
            "text": "102"
          },
          {
            "id": 84,
            "text": "C5"
          },
          {
            "id": 85,
            "text": "R1B"
          },
          {
            "id": 86,
            "text": "OCR2"
          },
          {
            "id": 87,
            "text": "CRO-S"
          },
          {
            "id": 88,
            "text": "Z65"
          },
          {
            "id": 89,
            "text": "R-2A/T/PH"
          },
          {
            "id": 90,
            "text": "P-D"
          },
          {
            "id": 91,
            "text": "NR1"
          },
          {
            "id": 92,
            "text": "RD 10"
          },
          {
            "id": 93,
            "text": "RD 5"
          },
          {
            "id": 94,
            "text": "M-2"
          },
          {
            "id": 95,
            "text": "R17"
          },
          {
            "id": 96,
            "text": "Z392"
          },
          {
            "id": 97,
            "text": "MF2"
          },
          {
            "id": 98,
            "text": "PUD"
          },
          {
            "id": 99,
            "text": "Z46"
          },
          {
            "id": 100,
            "text": "HU-RM1"
          },
          {
            "id": 101,
            "text": "RMD-D"
          },
          {
            "id": 102,
            "text": "VCC-2"
          },
          {
            "id": 103,
            "text": "HU-RD2"
          },
          {
            "id": 104,
            "text": "Z297"
          },
          {
            "id": 105,
            "text": "R-1-C"
          },
          {
            "id": 106,
            "text": "A1"
          },
          {
            "id": 107,
            "text": "HC3"
          },
          {
            "id": 108,
            "text": "Z14"
          },
          {
            "id": 109,
            "text": "Z83"
          },
          {
            "id": 110,
            "text": "MH1"
          },
          {
            "id": 111,
            "text": "CA2"
          },
          {
            "id": 112,
            "text": "SPA-OT"
          },
          {
            "id": 113,
            "text": "R-1-EA-2"
          },
          {
            "id": 114,
            "text": "R-1A-EA-4"
          },
          {
            "id": 115,
            "text": "Z163"
          },
          {
            "id": 116,
            "text": "Z198"
          },
          {
            "id": 117,
            "text": "R-2B/T/PH"
          },
          {
            "id": 118,
            "text": "R-2B/T/SP/"
          },
          {
            "id": 119,
            "text": "R-3A"
          },
          {
            "id": 120,
            "text": "RSTD R-2"
          },
          {
            "id": 121,
            "text": "R-1/T/AN"
          },
          {
            "id": 122,
            "text": "C-2-EA-4"
          },
          {
            "id": 123,
            "text": "RMF"
          },
          {
            "id": 124,
            "text": "RD3"
          },
          {
            "id": 125,
            "text": "R1AH"
          },
          {
            "id": 126,
            "text": "C6"
          },
          {
            "id": 127,
            "text": "R-1 MH"
          },
          {
            "id": 128,
            "text": "Z165"
          },
          {
            "id": 129,
            "text": "I1"
          },
          {
            "id": 130,
            "text": "R-3A/AN"
          },
          {
            "id": 131,
            "text": "R-3A/W/RP"
          },
          {
            "id": 132,
            "text": "Z324"
          },
          {
            "id": 133,
            "text": "RTF"
          },
          {
            "id": 134,
            "text": "R-1-PUD"
          },
          {
            "id": 135,
            "text": "RD20"
          },
          {
            "id": 136,
            "text": "RD-5 (NPA)"
          },
          {
            "id": 137,
            "text": "R=-1"
          },
          {
            "id": 138,
            "text": "SPLIT"
          },
          {
            "id": 139,
            "text": "R-1 R-3"
          },
          {
            "id": 140,
            "text": "HU-RS"
          },
          {
            "id": 141,
            "text": "Z134"
          },
          {
            "id": 142,
            "text": "R3A"
          },
          {
            "id": 143,
            "text": "Z415"
          },
          {
            "id": 144,
            "text": "RHD-A"
          },
          {
            "id": 145,
            "text": "BP"
          },
          {
            "id": 146,
            "text": "RD1"
          },
          {
            "id": 147,
            "text": "M-1-R"
          },
          {
            "id": 148,
            "text": "HMR-2"
          },
          {
            "id": 149,
            "text": "M-1S-R"
          },
          {
            "id": 150,
            "text": "R-1A-PUD"
          },
          {
            "id": 151,
            "text": "CR5"
          },
          {
            "id": 152,
            "text": "HU-MU"
          },
          {
            "id": 153,
            "text": "CCG-1"
          },
          {
            "id": 154,
            "text": "Z115"
          },
          {
            "id": 155,
            "text": "RD-3"
          },
          {
            "id": 156,
            "text": "R-S-1A"
          },
          {
            "id": 157,
            "text": "Z31"
          },
          {
            "id": 158,
            "text": "R-1-SPD"
          },
          {
            "id": 159,
            "text": "RMX-SPD"
          },
          {
            "id": 160,
            "text": "S-RM2"
          },
          {
            "id": 161,
            "text": "RD"
          },
          {
            "id": 162,
            "text": "AR-2"
          },
          {
            "id": 163,
            "text": "RD7"
          },
          {
            "id": 164,
            "text": "RP"
          },
          {
            "id": 165,
            "text": "B1"
          },
          {
            "id": 166,
            "text": "PD"
          },
          {
            "id": 167,
            "text": "MU-1"
          },
          {
            "id": 168,
            "text": "PD/AN"
          },
          {
            "id": 169,
            "text": "AL20"
          },
          {
            "id": 170,
            "text": "R-3B/T/PH"
          },
          {
            "id": 171,
            "text": "S-RM1"
          },
          {
            "id": 172,
            "text": "Z314"
          },
          {
            "id": 173,
            "text": "Z325"
          },
          {
            "id": 174,
            "text": "Z202"
          },
          {
            "id": 175,
            "text": "CO"
          },
          {
            "id": 176,
            "text": "M-1-SPD"
          },
          {
            "id": 177,
            "text": "R1-MH"
          },
          {
            "id": 178,
            "text": "R2MH"
          },
          {
            "id": 179,
            "text": "CM"
          },
          {
            "id": 180,
            "text": "Z315"
          },
          {
            "id": 181,
            "text": "R-1-EA-3 R"
          },
          {
            "id": 182,
            "text": "Z160"
          },
          {
            "id": 183,
            "text": "R-2A/T"
          },
          {
            "id": 184,
            "text": "RD 7"
          },
          {
            "id": 185,
            "text": "RD-2"
          },
          {
            "id": 186,
            "text": "C-2-SPD"
          },
          {
            "id": 187,
            "text": "R-1-R"
          },
          {
            "id": 188,
            "text": "CS"
          },
          {
            "id": 189,
            "text": "Z390"
          },
          {
            "id": 190,
            "text": "CN"
          },
          {
            "id": 191,
            "text": "SPA (WRSPA"
          },
          {
            "id": 192,
            "text": "RD-10 (NPA"
          },
          {
            "id": 193,
            "text": "R6"
          },
          {
            "id": 194,
            "text": "MU-D"
          },
          {
            "id": 195,
            "text": "LC"
          },
          {
            "id": 196,
            "text": "R-S-2.5A"
          },
          {
            "id": 197,
            "text": "E (1/2) R-"
          },
          {
            "id": 198,
            "text": "RO"
          },
          {
            "id": 199,
            "text": "R-3-EA-4"
          },
          {
            "id": 200,
            "text": "RLD-120"
          },
          {
            "id": 201,
            "text": "RMD-C"
          },
          {
            "id": 202,
            "text": "TH3A"
          },
          {
            "id": 203,
            "text": "Z06"
          },
          {
            "id": 204,
            "text": "Z412"
          },
          {
            "id": 205,
            "text": "Z116"
          },
          {
            "id": 206,
            "text": "Z294"
          },
          {
            "id": 207,
            "text": "R5A"
          },
          {
            "id": 208,
            "text": "I2"
          },
          {
            "id": 209,
            "text": "Z248"
          },
          {
            "id": 210,
            "text": "Z149"
          },
          {
            "id": 211,
            "text": "Z411"
          },
          {
            "id": 212,
            "text": "Z372"
          },
          {
            "id": 213,
            "text": "Z424"
          },
          {
            "id": 214,
            "text": "Z409"
          },
          {
            "id": 215,
            "text": "SF"
          },
          {
            "id": 216,
            "text": "Z128"
          },
          {
            "id": 217,
            "text": "LI"
          },
          {
            "id": 218,
            "text": "Z268"
          },
          {
            "id": 219,
            "text": "Z287"
          },
          {
            "id": 220,
            "text": "Z237"
          },
          {
            "id": 221,
            "text": "Z374"
          },
          {
            "id": 222,
            "text": "NZ"
          },
          {
            "id": 223,
            "text": "Z200"
          },
          {
            "id": 224,
            "text": "0"
          },
          {
            "id": 225,
            "text": "Z386"
          },
          {
            "id": 226,
            "text": "Z236"
          },
          {
            "id": 227,
            "text": "Z97"
          },
          {
            "id": 228,
            "text": "Z20"
          },
          {
            "id": 229,
            "text": "HU-RD1"
          },
          {
            "id": 230,
            "text": "Z24"
          },
          {
            "id": 231,
            "text": "Z313"
          },
          {
            "id": 232,
            "text": "S-RS"
          },
          {
            "id": 233,
            "text": "S-RD"
          },
          {
            "id": 234,
            "text": "HU-B1"
          },
          {
            "id": 235,
            "text": "BIP"
          },
          {
            "id": 236,
            "text": "S-B1"
          },
          {
            "id": 237,
            "text": "HMR-3"
          },
          {
            "id": 238,
            "text": "HMC-2"
          },
          {
            "id": 239,
            "text": "MU-2"
          },
          {
            "id": 240,
            "text": "A-2"
          },
          {
            "id": 241,
            "text": "R-1AA"
          },
          {
            "id": 242,
            "text": "R-1/W"
          },
          {
            "id": 243,
            "text": "R-1/W/RP"
          },
          {
            "id": 244,
            "text": "A-1"
          },
          {
            "id": 245,
            "text": "R-1AA/T"
          },
          {
            "id": 246,
            "text": "P-O"
          },
          {
            "id": 247,
            "text": "RNC-2"
          },
          {
            "id": 248,
            "text": "R-5"
          },
          {
            "id": 249,
            "text": "PD/RP"
          },
          {
            "id": 250,
            "text": "PRD"
          },
          {
            "id": 251,
            "text": "R1A"
          },
          {
            "id": 252,
            "text": "NR"
          },
          {
            "id": 253,
            "text": "I-G/T"
          },
          {
            "id": 254,
            "text": "R-2A/SP"
          },
          {
            "id": 255,
            "text": "I-2"
          },
          {
            "id": 256,
            "text": "R-1/T/PH"
          },
          {
            "id": 257,
            "text": "R-CE"
          },
          {
            "id": 258,
            "text": "R-1A/SP"
          },
          {
            "id": 259,
            "text": "O-1/SP"
          },
          {
            "id": 260,
            "text": "R-T-1"
          }
        ]
      },
      "flood-zone": {
        "label": "Flood Zone",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "X"
          },
          {
            "id": 2,
            "text": "A"
          },
          {
            "id": 3,
            "text": "AH"
          },
          {
            "id": 4,
            "text": "AE"
          },
          {
            "id": 5,
            "text": "AO"
          },
          {
            "id": 6,
            "text": "D"
          },
          {
            "id": 7,
            "text": "VE"
          }
        ]
      },
      "subdivision-name": {
        "label": "Subdivision Name",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "HAZELWOOD"
          },
          {
            "id": 2,
            "text": "BELMONT CENTER"
          },
          {
            "id": 3,
            "text": "ENGLEWOOD HEIGHTS ADDITION"
          },
          {
            "id": 4,
            "text": "NULL"
          },
          {
            "id": 5,
            "text": "SUNSET GARDENS #2"
          },
          {
            "id": 6,
            "text": "OAKWOOD"
          },
          {
            "id": 7,
            "text": "WEST LAND PARK"
          },
          {
            "id": 8,
            "text": "LUCERNE PARK"
          },
          {
            "id": 9,
            "text": "WORTH HEIGHTS ADDITION"
          },
          {
            "id": 10,
            "text": "MEYER ACRES ANNEX"
          },
          {
            "id": 11,
            "text": "COUNTRY SCENE 02 EXC M/R"
          },
          {
            "id": 12,
            "text": "PARKWAY ESTATES 15"
          },
          {
            "id": 13,
            "text": "SIERRA VISTA ADD 4"
          },
          {
            "id": 14,
            "text": "TRACT NO. 1160"
          },
          {
            "id": 15,
            "text": "ROGER GIVENS SOUTHWEST ADD"
          },
          {
            "id": 16,
            "text": "SHELTON SUBDIVISION LOT"
          },
          {
            "id": 17,
            "text": "CITY FARMS 06"
          },
          {
            "id": 18,
            "text": "HOMECREST"
          },
          {
            "id": 19,
            "text": "SUNRISE LAGUNA WEST"
          },
          {
            "id": 20,
            "text": "EAST LAKE"
          },
          {
            "id": 21,
            "text": "BELTLINE ADDN & BELTLINE SHP VLG"
          },
          {
            "id": 22,
            "text": "WESTHAVEN"
          },
          {
            "id": 23,
            "text": "SOUTH CREEK ADDITION"
          },
          {
            "id": 24,
            "text": "HIGHLAND TERRACE 01"
          },
          {
            "id": 25,
            "text": "OAK RDG ACRES"
          },
          {
            "id": 26,
            "text": "CITY FARMS 03"
          },
          {
            "id": 27,
            "text": "TRACT NO 1604"
          },
          {
            "id": 28,
            "text": "KEITH ADDITION"
          },
          {
            "id": 29,
            "text": "GERALD TRACT"
          },
          {
            "id": 30,
            "text": "CARVER MANOR #2"
          },
          {
            "id": 31,
            "text": "SPEEDWAY NO 1"
          },
          {
            "id": 32,
            "text": "BRENTWOOD"
          },
          {
            "id": 33,
            "text": "CAPITOL HILL ADD"
          },
          {
            "id": 34,
            "text": "COUNTRYSIDE ADDITION-FT WORTH"
          },
          {
            "id": 35,
            "text": "NORTH FORT WORTH"
          },
          {
            "id": 36,
            "text": "HILLSDALE 01"
          },
          {
            "id": 37,
            "text": "N SACTO SUB 3"
          },
          {
            "id": 38,
            "text": "VINTAGE PARK 04"
          },
          {
            "id": 39,
            "text": "GOLF COURSE VILLAGE 03"
          },
          {
            "id": 40,
            "text": "CORDOVA TOWNSITE"
          },
          {
            "id": 41,
            "text": "FISHERS VILLA ADD"
          },
          {
            "id": 42,
            "text": "WILLOWS SEC 5"
          },
          {
            "id": 43,
            "text": "FAIRVIEW PARK"
          },
          {
            "id": 44,
            "text": "WILKES ESTATES ADDITION"
          },
          {
            "id": 45,
            "text": "GRAND BOULEVARD"
          },
          {
            "id": 46,
            "text": "ALTAVUE ADDITION"
          },
          {
            "id": 47,
            "text": "NORTH SACRAMENTO SUB 8"
          },
          {
            "id": 48,
            "text": "LARCHMONT VILLAGE 20 EXC M/R"
          },
          {
            "id": 49,
            "text": "EAST DEL PASO HEIGHTS"
          },
          {
            "id": 50,
            "text": "LAGUNA CREEK WEST 06"
          },
          {
            "id": 51,
            "text": "COUNTRY PARK SOUTH 01"
          },
          {
            "id": 52,
            "text": "PARCEL MAP"
          },
          {
            "id": 53,
            "text": "CAMELIA ACRES"
          },
          {
            "id": 54,
            "text": "SLAWSONS 01"
          },
          {
            "id": 55,
            "text": "GOLF COURSE VILLAGE 07"
          },
          {
            "id": 56,
            "text": "MAYFLOWER ADD TO THE CITY OF BAKERSFIELD"
          },
          {
            "id": 57,
            "text": "SUNSET PARK"
          },
          {
            "id": 58,
            "text": "BELMONT GARDENS 2 EXT E61 FT"
          },
          {
            "id": 59,
            "text": "LOWELL ADDITION"
          },
          {
            "id": 60,
            "text": "LARCHMONT VALLEY HI 07"
          },
          {
            "id": 61,
            "text": "DESCANO PARK"
          },
          {
            "id": 62,
            "text": "HIGHLAND PARK"
          },
          {
            "id": 63,
            "text": "INGLESIDE PARK"
          },
          {
            "id": 64,
            "text": "ALTOS ACRES"
          },
          {
            "id": 65,
            "text": "LAKE MANN SHORES"
          },
          {
            "id": 66,
            "text": "OAKLAND"
          },
          {
            "id": 67,
            "text": "SPRINGFIELD, N.W. PORTION"
          },
          {
            "id": 68,
            "text": "GLENWOOD PARK 04"
          },
          {
            "id": 69,
            "text": "BRINKMEYER SUBDIVISION"
          },
          {
            "id": 70,
            "text": "E DEL PASO HEIGHTS ADD 01"
          },
          {
            "id": 71,
            "text": "DEL PASO HTS ADD"
          },
          {
            "id": 72,
            "text": "SWANSTON ESTATES 02"
          },
          {
            "id": 73,
            "text": "NORTH SACTO SUB 9"
          },
          {
            "id": 74,
            "text": "HACIENDAS TRACT 01"
          },
          {
            "id": 75,
            "text": "MURPHYS ORCHARD"
          },
          {
            "id": 76,
            "text": "PARKER HOMES TERRACE"
          },
          {
            "id": 77,
            "text": "PETERSON TRACT 01"
          },
          {
            "id": 78,
            "text": "KERN BOULEVARD HEIGHTS"
          },
          {
            "id": 79,
            "text": "RIVER VIEW"
          },
          {
            "id": 80,
            "text": "MOUNT DIABLO MERIDI"
          },
          {
            "id": 81,
            "text": "SOUTHERN ADDITION"
          },
          {
            "id": 82,
            "text": "BETTER HOMES 04 1220"
          },
          {
            "id": 83,
            "text": "PINKHAM"
          },
          {
            "id": 84,
            "text": "BAKERSFIELD"
          },
          {
            "id": 85,
            "text": "SIERRA VISTA ADD"
          },
          {
            "id": 86,
            "text": "STRAWBERRY MANOR 02"
          },
          {
            "id": 87,
            "text": "NORTH SACRAMENTO 08"
          },
          {
            "id": 88,
            "text": "MILLERS BOULEVARD"
          },
          {
            "id": 89,
            "text": "WILLIAMS R/P PT LOT5 BK E"
          },
          {
            "id": 90,
            "text": "HALLMARK HOMES #15"
          },
          {
            "id": 91,
            "text": "SUNSET VILLA"
          },
          {
            "id": 92,
            "text": "FORTY OAKS ADDITION"
          },
          {
            "id": 93,
            "text": "COLLEGE MANORS"
          },
          {
            "id": 94,
            "text": "PARKDALE HEIGHTS"
          },
          {
            "id": 95,
            "text": "MARKLAND HEIGHTS ADD"
          },
          {
            "id": 96,
            "text": "DOLLINS L J SUNSET PARK"
          },
          {
            "id": 97,
            "text": "LAKE SIDE PARK"
          },
          {
            "id": 98,
            "text": "SECTION LAND"
          },
          {
            "id": 99,
            "text": "PARKMORE"
          },
          {
            "id": 100,
            "text": "CRESTWOOD ADDITION"
          },
          {
            "id": 101,
            "text": "KING GROVE SUB"
          },
          {
            "id": 102,
            "text": "LAGUNA CREEK RANCH EAST 05"
          },
          {
            "id": 103,
            "text": "FOULKS RANCH 04A"
          },
          {
            "id": 104,
            "text": "GRAND OAKS 04"
          },
          {
            "id": 105,
            "text": "LAGUNA PARK 06"
          },
          {
            "id": 106,
            "text": "LAGUNA PARK VILLAGE 02A"
          },
          {
            "id": 107,
            "text": "LAGUNA CREEK VILLAGE 05"
          },
          {
            "id": 108,
            "text": "LAGUNA WEST 20"
          },
          {
            "id": 109,
            "text": "LAGUNA VISTA 15"
          },
          {
            "id": 110,
            "text": "SUNRISE RANCH"
          },
          {
            "id": 111,
            "text": "LAGUNA CROSSING"
          },
          {
            "id": 112,
            "text": "VICTORIA"
          },
          {
            "id": 113,
            "text": "FLORIN VISTA 01 EXC M/R"
          },
          {
            "id": 114,
            "text": "VILLAGE PARK 05"
          },
          {
            "id": 115,
            "text": "GOLF COURSE TERRACE 04"
          },
          {
            "id": 116,
            "text": "TRACT 3366"
          },
          {
            "id": 117,
            "text": "MEADOWVIEW GARDENS"
          },
          {
            "id": 118,
            "text": "CITRUS TERRACE VILLA TRACT"
          },
          {
            "id": 119,
            "text": "MAYFLOWER ADDITION"
          },
          {
            "id": 120,
            "text": "ROEDING NURSERY ACRS"
          },
          {
            "id": 121,
            "text": "OAK PARK AVE"
          },
          {
            "id": 122,
            "text": "WILLOWS"
          },
          {
            "id": 123,
            "text": "SPHINX AT REESE COURT"
          },
          {
            "id": 124,
            "text": "BUCKNER TERRACE APTS"
          },
          {
            "id": 125,
            "text": "OAK CLIFF ORIGINAL"
          },
          {
            "id": 126,
            "text": "MONCRIEF PARK"
          },
          {
            "id": 127,
            "text": "MANN SUB"
          },
          {
            "id": 128,
            "text": "HIAWASSEE LANDINGS UT 1"
          },
          {
            "id": 129,
            "text": "SRINGFELD S/D BLK 3,5,9 ,"
          },
          {
            "id": 130,
            "text": "MCKENZIES D.P. S/D"
          },
          {
            "id": 131,
            "text": "HACIENDAS TR"
          },
          {
            "id": 132,
            "text": "LARCHMONT VILLAGE 27 EXC M/R"
          },
          {
            "id": 133,
            "text": "VINTAGE PARK 03"
          },
          {
            "id": 134,
            "text": "TRENHOLM VILLAGE 02"
          },
          {
            "id": 135,
            "text": "RED FOX VILLAGE 01"
          },
          {
            "id": 136,
            "text": "COLLEGE VIEW ESTATES 03"
          },
          {
            "id": 137,
            "text": "SOUTHWOODS 04"
          },
          {
            "id": 138,
            "text": "COUNTRY PARK SOUTH 02"
          },
          {
            "id": 139,
            "text": "W & K WILLOW RANCHO 04"
          },
          {
            "id": 140,
            "text": "DAYSTAR 02"
          },
          {
            "id": 141,
            "text": "TALLAC VILLAGE 05"
          },
          {
            "id": 142,
            "text": "VIRGINIA COLONY"
          },
          {
            "id": 143,
            "text": "FIFTH AVENUE TRACT 02"
          },
          {
            "id": 144,
            "text": "AIRPORT ACRES"
          },
          {
            "id": 145,
            "text": "NORTH PARK"
          },
          {
            "id": 146,
            "text": "GOLDEN STATE TRACT TRACT #1139"
          },
          {
            "id": 147,
            "text": "TRACT NO. 3129"
          },
          {
            "id": 148,
            "text": "UNINCORPORATED"
          },
          {
            "id": 149,
            "text": "TRACT #1153 EL CAMINO PARK"
          },
          {
            "id": 150,
            "text": "BETTER HOMES #13"
          },
          {
            "id": 151,
            "text": "CENTRAL CALIFORNIA COLONY"
          },
          {
            "id": 152,
            "text": "MAYFLOWER ADD"
          },
          {
            "id": 153,
            "text": "SOMERSET HEIGHTS"
          },
          {
            "id": 154,
            "text": "CORONADO HEIGHTS"
          },
          {
            "id": 155,
            "text": "ACREAGE & UNREC"
          },
          {
            "id": 156,
            "text": "EDWARD COYLE"
          },
          {
            "id": 157,
            "text": "YOUNGS ENGLEWOOD ADD"
          },
          {
            "id": 158,
            "text": "COLLEGE HILL ADD"
          },
          {
            "id": 159,
            "text": "LINDSEY J. H. S/D"
          },
          {
            "id": 160,
            "text": "PARK LAWN"
          },
          {
            "id": 161,
            "text": "ANGEBILT ADD 2"
          },
          {
            "id": 162,
            "text": "ADAMS S/D"
          },
          {
            "id": 163,
            "text": "AIRPORT SUBDIVISION"
          },
          {
            "id": 164,
            "text": "SOUTH WOODS 02 EXC M/R"
          },
          {
            "id": 165,
            "text": "VALLEY HIGH VILLAGE"
          },
          {
            "id": 166,
            "text": "FRUITRIDGE MANOR 10"
          },
          {
            "id": 167,
            "text": "SANDRA HEIGHTS"
          },
          {
            "id": 168,
            "text": "PARKWAY NORTH"
          },
          {
            "id": 169,
            "text": "COLONIAL HEIGHTS"
          },
          {
            "id": 170,
            "text": "CLOVERDALE VILLAGE"
          },
          {
            "id": 171,
            "text": "STEBBINS PLAT/TOULA"
          },
          {
            "id": 172,
            "text": "CITY FARMS 02"
          },
          {
            "id": 173,
            "text": "MONTE VISTA TERRACE"
          },
          {
            "id": 174,
            "text": "MANCHESTER PARK #1251"
          },
          {
            "id": 175,
            "text": "STATE COLLEGE TRACT #1"
          },
          {
            "id": 176,
            "text": "ARLINGTON HEIGHTS"
          },
          {
            "id": 177,
            "text": "SARA PERRY SURVEY ABSTRACT #1164"
          },
          {
            "id": 178,
            "text": "POLYTECHNIC HEIGHTS ADDITION"
          },
          {
            "id": 179,
            "text": "NORTH SACRAMENTO 10"
          },
          {
            "id": 180,
            "text": "ARLINGTON HEIGHTS TRACT"
          },
          {
            "id": 181,
            "text": "BOWERS ADDITION"
          },
          {
            "id": 182,
            "text": "MURRAY HILLS HEIGHTS"
          },
          {
            "id": 183,
            "text": "INDIAN LANDING"
          },
          {
            "id": 184,
            "text": "E W DALLAS"
          },
          {
            "id": 185,
            "text": "OAKDALE VILLAGE"
          },
          {
            "id": 186,
            "text": "CINDY WOODS"
          },
          {
            "id": 187,
            "text": "N SAC SUB 8"
          },
          {
            "id": 188,
            "text": "RICHARDSON VILLAGE 01"
          },
          {
            "id": 189,
            "text": "LARCHMONT VILLAGE 07 EXC M/R"
          },
          {
            "id": 190,
            "text": "SUNRISE OAKS 02"
          },
          {
            "id": 191,
            "text": "CHEVIOT HILLS"
          },
          {
            "id": 192,
            "text": "OAKRIDGE ACRES"
          },
          {
            "id": 193,
            "text": "VALLEY HI 05"
          },
          {
            "id": 194,
            "text": "SUNRISE WILLOWOOD 03 REVISED"
          },
          {
            "id": 195,
            "text": "LARCHMONT VALLEY HI 13A"
          },
          {
            "id": 196,
            "text": "VILLA ROYALE #3"
          },
          {
            "id": 197,
            "text": "ARROYO VISTA ESTATES"
          },
          {
            "id": 198,
            "text": "LARCHMONT VALLEY HI 14"
          },
          {
            "id": 199,
            "text": "COUNTRY PARK SOUTH 03"
          },
          {
            "id": 200,
            "text": "STONEWOOD 02"
          },
          {
            "id": 201,
            "text": "CITRUS HEIGHTS ADD 05"
          },
          {
            "id": 202,
            "text": "FRUITRIDGE VISTA 16"
          },
          {
            "id": 203,
            "text": "CITY FARMS 04"
          },
          {
            "id": 204,
            "text": "SOUTH SACRAMENTO GARDENS"
          },
          {
            "id": 205,
            "text": "FRUITRIDGE VISTA 03"
          },
          {
            "id": 206,
            "text": "SCOTTSDALE GREENS 01"
          },
          {
            "id": 207,
            "text": "COUNTRY PLACE REVISED"
          },
          {
            "id": 208,
            "text": "CITY OF BAKERSFIELD"
          },
          {
            "id": 209,
            "text": "KERN COUNTY SALES MAP 01"
          },
          {
            "id": 210,
            "text": "DESCANSO PARK"
          },
          {
            "id": 211,
            "text": "REDDING AVENUE SUBDIVISION"
          },
          {
            "id": 212,
            "text": "SACRAMENTO HEIGHTS"
          },
          {
            "id": 213,
            "text": "SCOTTSDALE EAST 02 EXC M/R"
          },
          {
            "id": 214,
            "text": "FRUITRIDGE OAKS 08"
          },
          {
            "id": 215,
            "text": "3867 UN B"
          },
          {
            "id": 216,
            "text": "FREEPORT VILLAGE 01"
          },
          {
            "id": 217,
            "text": "SOUTHFIELD 01"
          },
          {
            "id": 218,
            "text": "GARDEN ACRES"
          },
          {
            "id": 219,
            "text": "TRACT NO 1655"
          },
          {
            "id": 220,
            "text": "JAMES ARP"
          },
          {
            "id": 221,
            "text": "CLOVERDALE"
          },
          {
            "id": 222,
            "text": "CASA LOMA ACRES"
          },
          {
            "id": 223,
            "text": "UNION AVENUE TRACT"
          },
          {
            "id": 224,
            "text": "EL CAMINO PARK"
          },
          {
            "id": 225,
            "text": "EDISON MANOR"
          },
          {
            "id": 226,
            "text": "MAPLE PLACE"
          },
          {
            "id": 227,
            "text": "KEARNEY BLVD HEIGHTS"
          },
          {
            "id": 228,
            "text": "CHAPARRAL COUNTRY AMD"
          },
          {
            "id": 229,
            "text": "MEADOWS AT INDEPENDENCE LOT 1-297"
          },
          {
            "id": 230,
            "text": "MEADOWS 2"
          },
          {
            "id": 231,
            "text": "VILLA DE PAZ 1"
          },
          {
            "id": 232,
            "text": "EMERALD POINT AMD LOT 1-291 TR A-M P"
          },
          {
            "id": 233,
            "text": "VILLA DE PAZ UNIT 2"
          },
          {
            "id": 234,
            "text": "SUNRISE TERRACE UNIT 5"
          },
          {
            "id": 235,
            "text": "SUNRISE VILLAGE"
          },
          {
            "id": 236,
            "text": "MARYVALE TERRACE NO. 49"
          },
          {
            "id": 237,
            "text": "COLLEGE PARK 21"
          },
          {
            "id": 238,
            "text": "ARIZONA HOMES"
          },
          {
            "id": 239,
            "text": "PONDEROSA HOMES WEST UNIT ONE"
          },
          {
            "id": 240,
            "text": "WILLOWS WEST"
          },
          {
            "id": 241,
            "text": "ARIZONA HOMES NO. 2"
          },
          {
            "id": 242,
            "text": "LEVITT HOMES WEST UNIT 1"
          },
          {
            "id": 243,
            "text": "VILLA OASIS 2 AMD"
          },
          {
            "id": 244,
            "text": "LAURELWOOD UNIT 1"
          },
          {
            "id": 245,
            "text": "LAURELWOOD 2"
          },
          {
            "id": 246,
            "text": "BRAEWOOD PARK UNIT 4"
          },
          {
            "id": 247,
            "text": "BRAEWOOD PARK UNIT 6"
          },
          {
            "id": 248,
            "text": "CHAPARRAL VILLAGE"
          },
          {
            "id": 249,
            "text": "TERRACITA"
          },
          {
            "id": 250,
            "text": "SILVERTHORN ESTATES"
          },
          {
            "id": 251,
            "text": "WESTBRIAR"
          },
          {
            "id": 252,
            "text": "WEST PLAZA 29 & 30 LOTS 1-147"
          },
          {
            "id": 253,
            "text": "NATIONAL EMBLEM WEST UNIT 1"
          },
          {
            "id": 254,
            "text": "NATIONAL EMBLEM WEST UNIT 2"
          },
          {
            "id": 255,
            "text": "WESTRIDGE SHADOWS"
          },
          {
            "id": 256,
            "text": "WESTFIELD 1 LOT 1-136 TR A-E"
          },
          {
            "id": 257,
            "text": "SKYVIEW NORTH UNIT 4"
          },
          {
            "id": 258,
            "text": "VILLA DE PAZ UNIT 3"
          },
          {
            "id": 259,
            "text": "VILLA DE PAZ UNIT 4"
          },
          {
            "id": 260,
            "text": "YOUNG AMERICA WEST"
          },
          {
            "id": 261,
            "text": "MARYVALE TERRACE 47"
          },
          {
            "id": 262,
            "text": "VILLA DE PAZ UNIT 6 AMD"
          },
          {
            "id": 263,
            "text": "BOLERO COURT"
          },
          {
            "id": 264,
            "text": "SOLACE SUBDIVISION"
          },
          {
            "id": 265,
            "text": "VILLA DE PAZ UNIT 9 AMD"
          },
          {
            "id": 266,
            "text": "BRAEWOOD PARK UNIT 1"
          },
          {
            "id": 267,
            "text": "BRAEWOOD PARK UNIT 2"
          },
          {
            "id": 268,
            "text": "SUNRISE TERRACE"
          },
          {
            "id": 269,
            "text": "SUNRISE TERRACE UNIT 2"
          },
          {
            "id": 270,
            "text": "SUNRISE TERRACE UNIT 3"
          },
          {
            "id": 271,
            "text": "SUNRISE TERRACE UNIT 4"
          },
          {
            "id": 272,
            "text": "PONDEROSA HOMES WEST UNIT TWO"
          },
          {
            "id": 273,
            "text": "VILLA OASIS 3 AMD"
          },
          {
            "id": 274,
            "text": "CASA REAL PHOENIX 1A LOTS 1 THROUGH 29"
          },
          {
            "id": 275,
            "text": "CASA REAL PHOENIX 1B"
          },
          {
            "id": 276,
            "text": "WESTRIDGE GLEN 4 LOT 188-254"
          },
          {
            "id": 277,
            "text": "WESTRIDGE GLEN 5 LOT 255-290"
          },
          {
            "id": 278,
            "text": "CASA REAL PHOENIX 2 LOTS 187 & 188"
          },
          {
            "id": 279,
            "text": "CASA REAL PHOENIX 3"
          },
          {
            "id": 280,
            "text": "GATEWAY CROSSING 1"
          },
          {
            "id": 281,
            "text": "SHEFFIELD PLACE UNIT 1"
          },
          {
            "id": 282,
            "text": "NATIONAL EMBLEM WEST UNIT 3"
          },
          {
            "id": 283,
            "text": "GATEWAY CROSSING 2"
          },
          {
            "id": 284,
            "text": "VILLA OASIS 1"
          },
          {
            "id": 285,
            "text": "WESTPOINT LOT 1-107 TR A"
          },
          {
            "id": 286,
            "text": "MARYVALE TERRACE 29 LOTS 212-352 & TR A"
          },
          {
            "id": 287,
            "text": "LAURELWOOD UNIT 3"
          },
          {
            "id": 288,
            "text": "PALM RIDGE UNIT ONE"
          },
          {
            "id": 289,
            "text": "LAURELWOOD UNIT 4"
          },
          {
            "id": 290,
            "text": "SUNRISE TERRACE 6"
          },
          {
            "id": 291,
            "text": "SKYVIEW NORTH UNIT FIVE"
          },
          {
            "id": 292,
            "text": "MARYVALE TERRACE NO. 58"
          },
          {
            "id": 293,
            "text": "CHAPARRAL VILLAGE 2 LOT 97-196"
          },
          {
            "id": 294,
            "text": "RYANS RIDGE LT 1-162 TR A-C"
          },
          {
            "id": 295,
            "text": "SUNRISE TERRACE UNIT 8"
          },
          {
            "id": 296,
            "text": "VISTA DE OESTE 2 PHASE 2"
          },
          {
            "id": 297,
            "text": "MARLBOROUGH COUNTRY UNIT 10"
          },
          {
            "id": 298,
            "text": "MARLBOROUGH COUNTRY UNIT 11"
          },
          {
            "id": 299,
            "text": "MARYVALE TERRACE 28 LOTS 10999-11084"
          },
          {
            "id": 300,
            "text": "SUNRISE TERRACE UNIT 9"
          },
          {
            "id": 301,
            "text": "MARYVALE TERRACE 28A LOTS 11505-11600"
          }
        ]
      },
      "school-district": {
        "label": "School District",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Fresno Unified School District"
          },
          {
            "id": 2,
            "text": "Kern High School District"
          },
          {
            "id": 3,
            "text": "Fort Worth Independent School District"
          },
          {
            "id": 4,
            "text": "Tucson Unified District"
          },
          {
            "id": 5,
            "text": "Irving Independent School District"
          },
          {
            "id": 6,
            "text": "Sacramento City Unified School District"
          },
          {
            "id": 7,
            "text": "Orange County School District"
          },
          {
            "id": 8,
            "text": "Elk Grove Unified School District"
          },
          {
            "id": 9,
            "text": "Oklahoma City Public Schools"
          },
          {
            "id": 10,
            "text": "Birmingham City School District"
          },
          {
            "id": 11,
            "text": "Grand Prairie Independent School District"
          },
          {
            "id": 12,
            "text": "Twin Rivers Unified School District"
          },
          {
            "id": 13,
            "text": "Washington Unified School District"
          },
          {
            "id": 14,
            "text": "Duval County School District"
          },
          {
            "id": 15,
            "text": "Crowley Independent School District"
          },
          {
            "id": 16,
            "text": "San Juan Unified School District"
          },
          {
            "id": 17,
            "text": "Central Unified School District"
          },
          {
            "id": 18,
            "text": "Twin Rivers Unified School District (7-12)"
          },
          {
            "id": 19,
            "text": "Garland Independent School District"
          },
          {
            "id": 20,
            "text": "Dallas Independent School District"
          },
          {
            "id": 21,
            "text": "Flowing Wells Unified District"
          },
          {
            "id": 22,
            "text": "Washington Unified School District (9-12)"
          },
          {
            "id": 23,
            "text": "Amphitheater Unified District"
          },
          {
            "id": 24,
            "text": "Clovis Unified School District"
          },
          {
            "id": 25,
            "text": "Sunnyside Unified District"
          },
          {
            "id": 26,
            "text": "Folsom-Cordova Unified School District"
          },
          {
            "id": 27,
            "text": "Selma Unified School District"
          },
          {
            "id": 28,
            "text": "Seminole County School District"
          },
          {
            "id": 29,
            "text": "Orleans Parish School District"
          },
          {
            "id": 30,
            "text": "Jefferson County School District"
          },
          {
            "id": 31,
            "text": "Mesquite Independent School District"
          },
          {
            "id": 32,
            "text": "Tarrant City School District"
          },
          {
            "id": 33,
            "text": "Lake Worth Independent School District"
          },
          {
            "id": 34,
            "text": "Vail Unified District"
          },
          {
            "id": 35,
            "text": "Putnam City Public Schools"
          },
          {
            "id": 36,
            "text": "Sierra Sands Unified School District"
          },
          {
            "id": 37,
            "text": "Fowler Unified School District"
          },
          {
            "id": 38,
            "text": "Arlington Independent School District"
          },
          {
            "id": 39,
            "text": "Richardson Independent School District"
          },
          {
            "id": 40,
            "text": "Marana Unified District"
          },
          {
            "id": 41,
            "text": "Ajo Unified District"
          },
          {
            "id": 42,
            "text": "Castleberry Independent School District"
          },
          {
            "id": 43,
            "text": "Beardsley Elementary School District"
          },
          {
            "id": 44,
            "text": "Standard Elementary School District"
          },
          {
            "id": 45,
            "text": "Fairfield City School District"
          },
          {
            "id": 46,
            "text": "Glendale Union High School District"
          },
          {
            "id": 47,
            "text": "Tolleson Union High School District"
          },
          {
            "id": 48,
            "text": "Phoenix Union High School District"
          },
          {
            "id": 49,
            "text": "Mesa Unified District"
          },
          {
            "id": 50,
            "text": "Peoria Unified School District"
          },
          {
            "id": 51,
            "text": "Tempe Union High School District"
          },
          {
            "id": 52,
            "text": "Gilbert Unified District"
          },
          {
            "id": 53,
            "text": "Paradise Valley Unified District"
          },
          {
            "id": 54,
            "text": "Saddle Mountain Unified School District"
          },
          {
            "id": 55,
            "text": "Alvord Unified School District"
          },
          {
            "id": 56,
            "text": "Riverside Unified School District"
          },
          {
            "id": 57,
            "text": "Moreno Valley Unified School District"
          },
          {
            "id": 58,
            "text": "Jurupa Unified School District"
          },
          {
            "id": 59,
            "text": "Perris Union High School District"
          },
          {
            "id": 60,
            "text": "Val Verde Unified School District"
          },
          {
            "id": 61,
            "text": "Corona-Norco Unified School District"
          },
          {
            "id": 62,
            "text": "San Jacinto Unified School District"
          },
          {
            "id": 63,
            "text": "Hemet Unified School District"
          },
          {
            "id": 64,
            "text": "Colton Joint Unified School District"
          },
          {
            "id": 65,
            "text": "Lake Elsinore Unified School District"
          },
          {
            "id": 66,
            "text": "Desert Sands Unified School District"
          },
          {
            "id": 67,
            "text": "Coachella Valley Unified School District"
          },
          {
            "id": 68,
            "text": "Rialto Unified School District"
          },
          {
            "id": 69,
            "text": "San Bernardino City Unified School District"
          },
          {
            "id": 70,
            "text": "Redlands Unified School District"
          },
          {
            "id": 71,
            "text": "Fontana Unified School District"
          },
          {
            "id": 72,
            "text": "Hesperia Unified School District"
          },
          {
            "id": 73,
            "text": "Victor Valley Union High School District"
          },
          {
            "id": 74,
            "text": "Lodi Unified School District"
          },
          {
            "id": 75,
            "text": "Lincoln Unified School District"
          },
          {
            "id": 76,
            "text": "Stockton Unified School District"
          },
          {
            "id": 77,
            "text": "Manteca Unified School District"
          },
          {
            "id": 78,
            "text": "Tracy Unified School District"
          },
          {
            "id": 79,
            "text": "Tracy Unified School District (9-12)"
          },
          {
            "id": 80,
            "text": "Modesto City High School District"
          },
          {
            "id": 81,
            "text": "Ceres Unified School District"
          },
          {
            "id": 82,
            "text": "East Hartford School District"
          },
          {
            "id": 83,
            "text": "Bristol School District"
          },
          {
            "id": 84,
            "text": "Glastonbury School District"
          },
          {
            "id": 85,
            "text": "Hartford School District"
          },
          {
            "id": 86,
            "text": "Manchester School District"
          },
          {
            "id": 87,
            "text": "New Britain School District"
          },
          {
            "id": 88,
            "text": "West Hartford School District"
          },
          {
            "id": 89,
            "text": "Caldwell School District 132"
          },
          {
            "id": 90,
            "text": "Nampa School District 131"
          },
          {
            "id": 91,
            "text": "Vallivue School District 139"
          },
          {
            "id": 92,
            "text": "Kuna Joint School District 3"
          },
          {
            "id": 93,
            "text": "Notus School District 135"
          },
          {
            "id": 94,
            "text": "Middleton School District 134"
          },
          {
            "id": 95,
            "text": "Meridian Joint School District 2"
          },
          {
            "id": 96,
            "text": "Bladen County Schools"
          },
          {
            "id": 97,
            "text": "Cumberland County Schools"
          },
          {
            "id": 98,
            "text": "Durham Public Schools"
          },
          {
            "id": 99,
            "text": "Edgecombe County Schools"
          },
          {
            "id": 100,
            "text": "Nash-Rocky Mount Schools"
          },
          {
            "id": 101,
            "text": "Wilson County Schools"
          },
          {
            "id": 102,
            "text": "Tulsa Public Schools"
          },
          {
            "id": 103,
            "text": "Sperry Public Schools"
          },
          {
            "id": 104,
            "text": "Shidler Public Schools"
          },
          {
            "id": 105,
            "text": "Cleveland Public Schools"
          },
          {
            "id": 106,
            "text": "Bowring Public School"
          },
          {
            "id": 107,
            "text": "Woodland Public Schools"
          },
          {
            "id": 108,
            "text": "Sand Springs Public Schools"
          },
          {
            "id": 109,
            "text": "Broken Arrow Public Schools"
          },
          {
            "id": 110,
            "text": "Union Public Schools"
          },
          {
            "id": 111,
            "text": "Catoosa Public Schools"
          },
          {
            "id": 112,
            "text": "Coweta Public Schools"
          },
          {
            "id": 113,
            "text": "Midland Borough School District"
          },
          {
            "id": 114,
            "text": "Central Falls School District"
          },
          {
            "id": 115,
            "text": "Cranston School District"
          },
          {
            "id": 116,
            "text": "Lincoln School District"
          },
          {
            "id": 117,
            "text": "North Providence School District"
          },
          {
            "id": 118,
            "text": "Pawtucket School District"
          },
          {
            "id": 119,
            "text": "Providence School District"
          },
          {
            "id": 120,
            "text": "Elgin Independent School District"
          },
          {
            "id": 121,
            "text": "Hays Consolidated Independent School District"
          },
          {
            "id": 122,
            "text": "Austin Independent School District"
          },
          {
            "id": 123,
            "text": "Albuquerque Public Schools"
          },
          {
            "id": 124,
            "text": "Midwest City-Del City Schools"
          },
          {
            "id": 125,
            "text": "Norfolk City Public Schools"
          },
          {
            "id": 126,
            "text": "Columbus City School District"
          },
          {
            "id": 127,
            "text": "Des Moines Independent Community School District"
          },
          {
            "id": 128,
            "text": "El Paso Independent School District"
          },
          {
            "id": 129,
            "text": "Cincinnati City School District"
          },
          {
            "id": 130,
            "text": "Ysleta Independent School District"
          },
          {
            "id": 131,
            "text": "Portsmouth City Public Schools"
          },
          {
            "id": 132,
            "text": "Northside Independent School District"
          },
          {
            "id": 133,
            "text": "Hampton City Public Schools"
          },
          {
            "id": 134,
            "text": "San Antonio Independent School District"
          },
          {
            "id": 135,
            "text": "Ogden School District"
          },
          {
            "id": 136,
            "text": "Whitehall City School District"
          },
          {
            "id": 137,
            "text": "Duquesne City School District"
          },
          {
            "id": 138,
            "text": "Colorado Springs School District 11"
          },
          {
            "id": 139,
            "text": "Wichita Unified School District 259"
          },
          {
            "id": 140,
            "text": "Harlandale Independent School District"
          },
          {
            "id": 141,
            "text": "Salt Lake City School District"
          },
          {
            "id": 142,
            "text": "Pittsburgh School District"
          },
          {
            "id": 143,
            "text": "Richmond City Public Schools"
          },
          {
            "id": 144,
            "text": "Edgewood Independent School District"
          },
          {
            "id": 145,
            "text": "Moore Public Schools"
          },
          {
            "id": 146,
            "text": "Wilkinsburg Borough School District"
          },
          {
            "id": 147,
            "text": "Rochester City School District"
          },
          {
            "id": 148,
            "text": "Omaha Public Schools"
          },
          {
            "id": 149,
            "text": "Woodland Hills School District"
          },
          {
            "id": 150,
            "text": "Steel Valley School District"
          },
          {
            "id": 151,
            "text": "Clairton City School District"
          },
          {
            "id": 152,
            "text": "McKeesport Area School District"
          },
          {
            "id": 153,
            "text": "Reading Community City School District"
          },
          {
            "id": 154,
            "text": "Socorro Independent School District"
          },
          {
            "id": 155,
            "text": "Clint Independent School District"
          },
          {
            "id": 156,
            "text": "Harrison School District 2"
          },
          {
            "id": 157,
            "text": "Academy School District 20"
          },
          {
            "id": 158,
            "text": "Granite School District"
          },
          {
            "id": 159,
            "text": "Weber School District"
          },
          {
            "id": 160,
            "text": "Hamilton Local School District"
          },
          {
            "id": 161,
            "text": "Edmond Public Schools"
          },
          {
            "id": 162,
            "text": "Crutcho Public School"
          },
          {
            "id": 163,
            "text": "South-Western City School District"
          },
          {
            "id": 164,
            "text": "Baldwin-Whitehall School District"
          },
          {
            "id": 165,
            "text": "West Mifflin Area School District"
          },
          {
            "id": 166,
            "text": "Newport News City Public Schools"
          },
          {
            "id": 167,
            "text": "Cheyenne Mountain School District 12"
          },
          {
            "id": 168,
            "text": "Widefield School District 3"
          },
          {
            "id": 169,
            "text": "Saydel Community School District"
          },
          {
            "id": 170,
            "text": "Johnston Community School District"
          },
          {
            "id": 171,
            "text": "North East Independent School District"
          },
          {
            "id": 172,
            "text": "Western Heights Public Schools"
          },
          {
            "id": 173,
            "text": "Groveport Madison Local School District"
          },
          {
            "id": 174,
            "text": "Hilliard City School District"
          },
          {
            "id": 175,
            "text": "Penn Hills School District"
          },
          {
            "id": 176,
            "text": "North Hills School District"
          },
          {
            "id": 177,
            "text": "Shaler Area School District"
          },
          {
            "id": 178,
            "text": "West Jefferson Hills School District"
          },
          {
            "id": 179,
            "text": "East Allegheny School District"
          },
          {
            "id": 180,
            "text": "Oak Hills Local School District"
          },
          {
            "id": 181,
            "text": "Northwest Local School District"
          },
          {
            "id": 182,
            "text": "Mariemont City School District"
          },
          {
            "id": 183,
            "text": "Westside Community Schools"
          },
          {
            "id": 184,
            "text": "Haysville Unified School District 261"
          },
          {
            "id": 185,
            "text": "Southeast Polk Community School District"
          },
          {
            "id": 186,
            "text": "Southside Independent School District"
          },
          {
            "id": 187,
            "text": "East Central Independent School District"
          },
          {
            "id": 188,
            "text": "Alamo Heights Independent School District"
          },
          {
            "id": 189,
            "text": "Murray School District"
          },
          {
            "id": 190,
            "text": "Ambridge Area School District"
          },
          {
            "id": 191,
            "text": "Henrico County Public Schools"
          },
          {
            "id": 192,
            "text": "Chesterfield County Public Schools"
          },
          {
            "id": 193,
            "text": "Suffolk City Public Schools"
          },
          {
            "id": 194,
            "text": "St. Louis City School District"
          },
          {
            "id": 195,
            "text": "Jennings School District"
          },
          {
            "id": 196,
            "text": "Riverview Gardens School District"
          },
          {
            "id": 197,
            "text": "Hazelwood School District"
          },
          {
            "id": 198,
            "text": "Normandy Schools Collaborative"
          },
          {
            "id": 199,
            "text": "Cleveland Municipal School District"
          },
          {
            "id": 200,
            "text": "Garfield Heights City School District"
          },
          {
            "id": 201,
            "text": "Cleveland Heights-University Heights City School District"
          },
          {
            "id": 202,
            "text": "East Cleveland City School District"
          },
          {
            "id": 203,
            "text": "Aurora City School District"
          },
          {
            "id": 204,
            "text": "Euclid City School District"
          },
          {
            "id": 205,
            "text": "Shaker Heights City School District"
          },
          {
            "id": 206,
            "text": "South Euclid-Lyndhurst City School District"
          }
        ]
      },
      "linked-owners": {
        "label": "Linked Owners",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637059
        ],
        "options": []
      },
      "number": {
        "label": "Number",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644240": {
    "app_id": 30644240,
    "app_name": "Companies",
    "item_name": "Seller",
    "fields": {
      "seller-id": {
        "label": "Owner ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-full-name": {
        "label": "Owner Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-type": {
        "label": "Owner Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Individual"
          },
          {
            "id": 2,
            "text": "Corporate"
          },
          {
            "id": 3,
            "text": "Trust / Estate"
          },
          {
            "id": 4,
            "text": "Hedge Fund"
          },
          {
            "id": 5,
            "text": "Government"
          },
          {
            "id": 6,
            "text": "Bank / Lender"
          },
          {
            "id": 7,
            "text": "Needs Review"
          },
          {
            "id": 8,
            "text": "Hedgefund"
          }
        ]
      },
      "calculation": {
        "label": ">",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-1-full-name": {
        "label": "Owner #1 Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-mailing-address": {
        "label": "Owner Address",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title": {
        "label": "Owner #1 First Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "file": {
        "label": "File",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "#6"
          },
          {
            "id": 2,
            "text": "#1"
          },
          {
            "id": 3,
            "text": "#2"
          },
          {
            "id": 4,
            "text": "#5"
          }
        ]
      },
      "owner-last-name": {
        "label": "Owner #1 Last Name / Company Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property-profile": {
        "label": "Property Profile",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30657385
        ],
        "options": []
      },
      "owner-2-full-name": {
        "label": "Owner #2 Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ein-number": {
        "label": "EIN Number",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-2-first-name": {
        "label": "Owner #2 First Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "entity-age": {
        "label": "Entity Age",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-2-last-name": {
        "label": "Owner #2 Last Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contact-phones": {
        "label": "Contact Phones",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644241
        ],
        "options": []
      },
      "out-of-state-owner": {
        "label": "Out Of State Owner",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "FALSE"
          },
          {
            "id": 2,
            "text": "TRUE"
          }
        ]
      },
      "contact-emails": {
        "label": "Contact Emails",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644242
        ],
        "options": []
      },
      "primary-officers": {
        "label": "Primary Officer(s)",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644237
        ],
        "options": []
      },
      "preferred-contact-method": {
        "label": "Preferred Contact Method",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "SMS"
          },
          {
            "id": 2,
            "text": "Email"
          },
          {
            "id": 3,
            "text": "Phone"
          }
        ]
      },
      "total-properties-owned": {
        "label": "Total Properties Owned",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation-6": {
        "label": "Calculation",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation-7": {
        "label": "Calculation",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation-5": {
        "label": "Calculation",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation-4": {
        "label": "Calculation",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation-3": {
        "label": "Calculation",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation-2": {
        "label": "Calculation",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "category": {
        "label": "Category",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-portfolio-value": {
        "label": "Estimated Portfolio Value",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644725": {
    "app_id": 30644725,
    "app_name": "Zip Codes",
    "item_name": "Zip Code",
    "fields": {
      "title": {
        "label": "Zip Code",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30550863
        ],
        "options": []
      },
      "field-9": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market-temperature": {
        "label": "Market Temperature",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Sellers Market (Hot)"
          },
          {
            "id": 2,
            "text": "Balanced"
          }
        ]
      },
      "price-trend": {
        "label": "Price Trend",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Rising"
          },
          {
            "id": 2,
            "text": "Stable"
          },
          {
            "id": 3,
            "text": "Declining"
          }
        ]
      },
      "location": {
        "label": "Location",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sfr": {
        "label": "SFR Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "multifamily": {
        "label": "Multifamily Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rental-units": {
        "label": "Rental Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-occup": {
        "label": "Owner-Occupied Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "vacancy-rate": {
        "label": "Vacancy Rate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rental-vacancy-rate": {
        "label": "Rental Vacancy Rate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-year-built": {
        "label": "Median Year Built",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "older-homes": {
        "label": "Older Homes (Pre-1980) ",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "new-construction-vo": {
        "label": "New Construction Volume",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "average-sq-ft": {
        "label": "Average Sq Ft",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "manufactured-home-percent": {
        "label": "Manufactured Home Percent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "crime-index": {
        "label": "Crime Index",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "violent-crime-index": {
        "label": "Violent Crime Index",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property-crime-rate": {
        "label": "Property Crime Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "flood-risk-score": {
        "label": "Flood Risk Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "fire-risk-score": {
        "label": "Fire Risk Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tornado-risk-scor": {
        "label": "Tornado Risk Scor",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "wind-risk-score": {
        "label": "Wind Risk Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "earthquake-risk-score": {
        "label": "Earthquake Risk Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "climate-risk-score": {
        "label": "Climate Risk Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "insurance-cost-multiplier": {
        "label": "Insurance Cost Multiplier",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-rent": {
        "label": "Median Rent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rent-per-sq-ft": {
        "label": "Rent Per Sq Ft",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rent-growth-1-year": {
        "label": "Rent Growth: 1 Year %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rent-growth-5-year": {
        "label": "Rent Growth: 5 Year %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rent-1bd": {
        "label": "Rent 1BD",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rent-2br": {
        "label": "Rent 2BR",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rent-3br": {
        "label": "Rent 3BR",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "price-to-rent": {
        "label": "Price-To-Rent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "gross-yield": {
        "label": "Gross Yield %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "cap-rate-estimate": {
        "label": "Cap Rate Estimate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "investor-owned-percent": {
        "label": "Investor Owned Percent ",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "cash-buyer-activity-score": {
        "label": "Cash Buyer Activity Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "flip-volume-score": {
        "label": "Flip Volume Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "landlord-density-score": {
        "label": "Landlord Density Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-8": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-sales-price": {
        "label": "Median Sales Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-list-price": {
        "label": "Median List Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "price-per-sqft": {
        "label": "Price Per SqFt",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "appreciation-1-year": {
        "label": "Appreciation (1 Year)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "appreciation-5-year": {
        "label": "Appreciation (5 Year)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "active-listings-count": {
        "label": "Active Listings Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sold-listings-count": {
        "label": "Sold Listings Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "months-of-inventory": {
        "label": "Months of Inventory",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "days-on-market-median": {
        "label": "Days on Market Median",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sale-to-list-ratio": {
        "label": "Sale-To-List Ratio",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-4": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "job-growth-rate": {
        "label": "Job Growth Rate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "unemployment-rate": {
        "label": "Unemployment Rate ",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "major-employers-index": {
        "label": "Major Employers Index",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "business-openings-count": {
        "label": "Business Openings Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "business-closures-count": {
        "label": "Business Closures Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "economic-stability-score": {
        "label": "Economic Stability Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "gdp-per-capita": {
        "label": "GDP Per Capita",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-11": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "migration-inflow-rate": {
        "label": "Migration Inflow Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "net-migration-rate": {
        "label": "Net Migration Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "migration-outflow-rate": {
        "label": "Migration Outflow Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "population-total": {
        "label": "Population Total",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "population-growth-1-year": {
        "label": "Population Growth: 1 Year %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "population-growth-5-year": {
        "label": "Population Growth: 5 Year %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-age": {
        "label": "Median Age",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "household-county": {
        "label": "Household Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "household-growth-5-year": {
        "label": "Household Growth: 5 Year %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-household-income": {
        "label": "Median Household Income",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "income-growth-5-year": {
        "label": "Income Growth: 5 Year ",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "poverty-rate": {
        "label": "Poverty Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-21": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "education-level-index": {
        "label": "Education Level Index",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "married-percent": {
        "label": "Married Percent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "single-percent": {
        "label": "Single Percent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "average-household-size": {
        "label": "Average Household Size",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "occupation-distribution-score": {
        "label": "Occupation Distribution Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "commute-time-average": {
        "label": "Commute Time Average",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-10": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "pre-foreclosure-rate": {
        "label": "Pre-Foreclosure Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "pre-foreclosure-filings": {
        "label": "Pre-Foreclosure Filings",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-delinquency-rate": {
        "label": "Tax Delinquency Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "eviction-filings": {
        "label": "Eviction Filings",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "bankruptcy-rate": {
        "label": "Bankruptcy Rate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "code-violations-per-1000": {
        "label": "Code Violations Per 1000",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "reo-inventory-count": {
        "label": "REO Inventory Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "divorce-rate-indicator": {
        "label": "Divorce Rate Indicator ",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "distress-score": {
        "label": "Distress Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "motivation-index": {
        "label": "Motivation Index",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-6": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-market-summary": {
        "label": "AI Market Summary",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation": {
        "label": "Calculation",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "smart-offer-floor": {
        "label": "Smart Offer Floor %",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "smart-offer-ceiling": {
        "label": "Smart Offer Ceiling %",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "dispo-strategy": {
        "label": "Dispo Strategy",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "distress-probability-score": {
        "label": "Distress Probability Score ",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "buyer-targeting-profile": {
        "label": "Buyer Targeting Profile",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "suggested-follow-up-intensity": {
        "label": "Suggested Follow-Up Intensity",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "zip-temperature-summary": {
        "label": "Zip Temperature Summary",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-7": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-data-refresh": {
        "label": "Last Data Refresh",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "data-confidence-score": {
        "label": "Data Confidence Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "notes": {
        "label": "Notes",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644726": {
    "app_id": 30644726,
    "app_name": "Markets",
    "item_name": "Market",
    "fields": {
      "title": {
        "label": "Market",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "zip-codes": {
        "label": "Zip Codes",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644725
        ],
        "options": []
      },
      "population": {
        "label": "Population",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "5-year-population-growth": {
        "label": "5-Year Population Growth %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-hh-income": {
        "label": "Median HH Income",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "employment-rate": {
        "label": "Employment Rate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rent-to-value-ratio": {
        "label": "Rent-to-Value Ratio %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "12-month-appreciation": {
        "label": "12-Month Appreciation %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-home-value": {
        "label": "Median Home Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "price-per-sq-ft": {
        "label": "Price Per Sq Ft",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "median-rent": {
        "label": "Median Rent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "vacancy-rate": {
        "label": "Vacancy Rate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "days-on-market": {
        "label": "Days on Market",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "active-listings": {
        "label": "Active Listings",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "cash-buyer-density-score": {
        "label": "Cash Buyer Density Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "avg-price-per-unit": {
        "label": "Avg Price Per Unit",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "avg-cap-rate": {
        "label": "Avg Cap Rate %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "hedge-fund-density-score": {
        "label": "Hedge Fund Density Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mf-buyer-density-score": {
        "label": "MF Buyer Density Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "smart-offer-floor": {
        "label": "Smart Offer Floor %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "smart-offer-ceiling": {
        "label": "Smart Offer Ceiling %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rehab-multiplier": {
        "label": "Rehab Multiplier %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "best-strategy": {
        "label": "Best Strategy",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Hold"
          },
          {
            "id": 2,
            "text": "Creative"
          },
          {
            "id": 3,
            "text": "Novation"
          },
          {
            "id": 4,
            "text": "Flip"
          }
        ]
      },
      "market-hotness-score": {
        "label": "Market Hotness Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "follow-up-intensity": {
        "label": "Follow-Up Intensity",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Medium"
          },
          {
            "id": 2,
            "text": "Low"
          },
          {
            "id": 3,
            "text": "High"
          }
        ]
      },
      "crime-index": {
        "label": "Crime Index",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "flood-zone": {
        "label": "Flood Zone %",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market-volatility-score": {
        "label": "Market Volatility Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "regulatory-risk-score": {
        "label": "Regulatory Risk Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-data-refresh": {
        "label": "Last Data Refresh",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644728": {
    "app_id": 30644728,
    "app_name": "Institutional Activity",
    "item_name": "Activity",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644731": {
    "app_id": 30644731,
    "app_name": "Market Trends",
    "item_name": "Trend",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30644734": {
    "app_id": 30644734,
    "app_name": "AI Market Insights",
    "item_name": "Insight",
    "fields": {
      "title": {
        "label": "Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30657385": {
    "app_id": 30657385,
    "app_name": "Property Profile",
    "item_name": "Profile",
    "fields": {
      "title": {
        "label": "Hash Label",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644070
        ],
        "options": []
      }
    }
  },
  "30657386": {
    "app_id": 30657386,
    "app_name": "County",
    "item_name": "County",
    "fields": {
      "title": {
        "label": "County Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30657958": {
    "app_id": 30657958,
    "app_name": "Sold Properties",
    "item_name": "Prospect",
    "fields": {
      "property-id": {
        "label": "Property ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "full-name": {
        "label": "Company Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "file": {
        "label": "File",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "#1"
          },
          {
            "id": 2,
            "text": "#2"
          },
          {
            "id": 3,
            "text": "#3"
          },
          {
            "id": 4,
            "text": "#4"
          },
          {
            "id": 5,
            "text": "#5"
          },
          {
            "id": 6,
            "text": "#6"
          }
        ]
      },
      "comp-search-profile-hash": {
        "label": "Comp Search Profile Hash",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30657385
        ],
        "options": []
      },
      "property-address": {
        "label": "Comp Address",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation": {
        "label": ">>",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Miami, FL"
          },
          {
            "id": 2,
            "text": "Houston,. TX"
          },
          {
            "id": 3,
            "text": "Orlando, FL"
          },
          {
            "id": 4,
            "text": "Tampa, FL"
          },
          {
            "id": 5,
            "text": "Dallas, TX"
          },
          {
            "id": 6,
            "text": "Jacksonville, FL"
          },
          {
            "id": 7,
            "text": "Charlotte, NC"
          },
          {
            "id": 8,
            "text": "Minneapolis, MN"
          },
          {
            "id": 9,
            "text": "Nashville, TN"
          },
          {
            "id": 10,
            "text": "Phoenix, AZ"
          },
          {
            "id": 11,
            "text": "Saint Louis, MO"
          },
          {
            "id": 12,
            "text": "Indianapolis, IN"
          },
          {
            "id": 13,
            "text": "Memphis, TN"
          },
          {
            "id": 14,
            "text": "Rochester, NY"
          },
          {
            "id": 15,
            "text": "Atlanta, GA"
          },
          {
            "id": 16,
            "text": "Lakeland, FL"
          },
          {
            "id": 17,
            "text": "Fresno, CA"
          },
          {
            "id": 18,
            "text": "Bakersfield, CA"
          },
          {
            "id": 19,
            "text": "Tuscon, AZ"
          },
          {
            "id": 20,
            "text": "Sacramento, CA"
          },
          {
            "id": 21,
            "text": "Oklahoma City, OK"
          },
          {
            "id": 22,
            "text": "Birmingham, AL"
          },
          {
            "id": 23,
            "text": "New Orleans, LA"
          },
          {
            "id": 24,
            "text": "Inland Emprie, CA"
          },
          {
            "id": 25,
            "text": "Stockton, CA"
          },
          {
            "id": 26,
            "text": "Modesto, CA"
          },
          {
            "id": 27,
            "text": "Hartford, CT"
          },
          {
            "id": 28,
            "text": "Boise, ID"
          },
          {
            "id": 29,
            "text": "Raleigh, NC"
          },
          {
            "id": 30,
            "text": "Tulsa, OK"
          },
          {
            "id": 31,
            "text": "Providence, RI"
          },
          {
            "id": 32,
            "text": "Austin, TX"
          },
          {
            "id": 33,
            "text": "Albuquerque, NM"
          },
          {
            "id": 34,
            "text": "Norfolk, VA"
          },
          {
            "id": 35,
            "text": "Columbus, OH"
          },
          {
            "id": 36,
            "text": "Des Moines, IA"
          },
          {
            "id": 37,
            "text": "Louisville, KY"
          },
          {
            "id": 38,
            "text": "El Paso, TX"
          },
          {
            "id": 39,
            "text": "Cincinnati, OH"
          },
          {
            "id": 40,
            "text": "Portsmouth, VA"
          },
          {
            "id": 41,
            "text": "San Antonio, TX"
          },
          {
            "id": 42,
            "text": "Pittsburg, PA"
          },
          {
            "id": 43,
            "text": "Wichita, KS"
          },
          {
            "id": 44,
            "text": "Salt Lake City, UT"
          },
          {
            "id": 45,
            "text": "Richmond, VA"
          },
          {
            "id": 46,
            "text": "Omaha, NE"
          },
          {
            "id": 47,
            "text": "Cleveland, OH"
          },
          {
            "id": 48,
            "text": "Detroit, MI"
          },
          {
            "id": 49,
            "text": "Baltimore, MD"
          },
          {
            "id": 50,
            "text": "Philadelphia, PA"
          },
          {
            "id": 51,
            "text": "Chicago, IL"
          },
          {
            "id": 52,
            "text": "Milwaukee, WI"
          },
          {
            "id": 53,
            "text": "Kansas City, MO"
          },
          {
            "id": 54,
            "text": "Clayton, GA"
          },
          {
            "id": 55,
            "text": "Houston, TX"
          },
          {
            "id": 56,
            "text": "Las Vegas, NV"
          },
          {
            "id": 57,
            "text": "Los Angeles, CA"
          },
          {
            "id": 58,
            "text": "Spokane, WA"
          },
          {
            "id": 59,
            "text": "Inland Empire, CA"
          },
          {
            "id": 60,
            "text": "High Desert / Antelope Valley, CA"
          },
          {
            "id": 61,
            "text": "Other"
          },
          {
            "id": 62,
            "text": "Austin / San Antonio, TX"
          }
        ]
      },
      "county": {
        "label": "County",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30657386
        ],
        "options": []
      },
      "market-2": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "relationship": {
        "label": "Zip Code",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644725
        ],
        "options": []
      },
      "section-separator": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "market-status": {
        "label": "Market Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Off Market"
          },
          {
            "id": 2,
            "text": "Sold"
          },
          {
            "id": 3,
            "text": "Fail"
          },
          {
            "id": 4,
            "text": "Active"
          },
          {
            "id": 5,
            "text": "Pending"
          },
          {
            "id": 6,
            "text": "Unknown"
          },
          {
            "id": 7,
            "text": "Contingent"
          },
          {
            "id": 8,
            "text": "Under Contract"
          },
          {
            "id": 9,
            "text": "Expired"
          }
        ]
      },
      "purchase-info": {
        "label": "Purchase Info",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-label": {
        "label": "MLS Label",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Off Market"
          },
          {
            "id": 2,
            "text": "Active"
          },
          {
            "id": 3,
            "text": "Fail"
          },
          {
            "id": 4,
            "text": "Sold"
          },
          {
            "id": 5,
            "text": "Pending"
          },
          {
            "id": 6,
            "text": "Unknown"
          },
          {
            "id": 7,
            "text": "Contingent"
          }
        ]
      },
      "last-sale-price-2": {
        "label": "Last Sale Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-value-2": {
        "label": "Estimated Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "price-off-value": {
        "label": "Price off Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-listed-price": {
        "label": "MLS Listed Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-sold-date": {
        "label": "MLS Sold Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-sold-price": {
        "label": "MLS Sold Price",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "percent-off": {
        "label": "Percent Off",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "potential-flip-spread": {
        "label": "Potential Flip Spread",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ppsf": {
        "label": "PPSF",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ppu": {
        "label": "PPU",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ppbd-bed": {
        "label": "PPBD (Bed)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property-class": {
        "label": "Property Class",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Residential"
          },
          {
            "id": 2,
            "text": "Vacant"
          },
          {
            "id": 3,
            "text": "Exempt"
          },
          {
            "id": 4,
            "text": "Commercial"
          }
        ]
      },
      "property-type": {
        "label": "Property Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Single Family"
          },
          {
            "id": 2,
            "text": "Multi-Family"
          },
          {
            "id": 3,
            "text": "Vacant Land"
          },
          {
            "id": 4,
            "text": "Apartment"
          },
          {
            "id": 5,
            "text": "Other"
          },
          {
            "id": 6,
            "text": "Townhouse"
          },
          {
            "id": 7,
            "text": "Mobile Home"
          },
          {
            "id": 8,
            "text": "Condominium"
          }
        ]
      },
      "property-style": {
        "label": "Property Style",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Duplex"
          },
          {
            "id": 2,
            "text": "Custom"
          },
          {
            "id": 3,
            "text": "MultiFamily"
          },
          {
            "id": 4,
            "text": "Ranch\\Rambler"
          },
          {
            "id": 5,
            "text": "Triplex"
          },
          {
            "id": 6,
            "text": "Quadplex"
          },
          {
            "id": 7,
            "text": "unknown"
          },
          {
            "id": 8,
            "text": "Conventional"
          },
          {
            "id": 9,
            "text": "TownHouse"
          },
          {
            "id": 10,
            "text": "Traditional"
          },
          {
            "id": 11,
            "text": "CONDO"
          },
          {
            "id": 12,
            "text": "Mediterranean"
          },
          {
            "id": 13,
            "text": "Mobile Home"
          },
          {
            "id": 14,
            "text": "Contemporary"
          },
          {
            "id": 15,
            "text": "Bungalow"
          },
          {
            "id": 16,
            "text": "Modern"
          },
          {
            "id": 17,
            "text": "Colonial"
          },
          {
            "id": 18,
            "text": "Tudor"
          },
          {
            "id": 19,
            "text": "Other"
          },
          {
            "id": 20,
            "text": "Cape Cod"
          },
          {
            "id": 21,
            "text": "Split Level"
          },
          {
            "id": 22,
            "text": "Raised Ranch"
          },
          {
            "id": 23,
            "text": "Historical"
          },
          {
            "id": 24,
            "text": "Bi-Level"
          },
          {
            "id": 25,
            "text": "Log Cabin/Rustic"
          },
          {
            "id": 26,
            "text": "Tri-Level"
          },
          {
            "id": 27,
            "text": "Prefab, Modular"
          },
          {
            "id": 28,
            "text": "Cottage"
          },
          {
            "id": 29,
            "text": "Victorian"
          },
          {
            "id": 30,
            "text": "High-rise"
          },
          {
            "id": 31,
            "text": "Split Foyer"
          },
          {
            "id": 32,
            "text": "Row Home"
          },
          {
            "id": 33,
            "text": "Unfinished\\Under Construction"
          },
          {
            "id": 34,
            "text": "English"
          },
          {
            "id": 35,
            "text": "Patio Home"
          },
          {
            "id": 36,
            "text": "Spanish"
          },
          {
            "id": 37,
            "text": "Mansion"
          },
          {
            "id": 38,
            "text": "French Provincial"
          },
          {
            "id": 39,
            "text": "Cluster"
          }
        ]
      },
      "stories": {
        "label": "Stories",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "1 Story"
          },
          {
            "id": 2,
            "text": "2 Stories"
          },
          {
            "id": 3,
            "text": "1.5 Stories"
          },
          {
            "id": 4,
            "text": "3 Stories"
          },
          {
            "id": 5,
            "text": "2.5 Stories"
          },
          {
            "id": 6,
            "text": "10 Stories"
          },
          {
            "id": 7,
            "text": "1.75 Stories"
          },
          {
            "id": 8,
            "text": "4 Stories"
          },
          {
            "id": 9,
            "text": "1.25 Stories"
          },
          {
            "id": 10,
            "text": "6 Stories"
          },
          {
            "id": 11,
            "text": "2.75 Stories"
          },
          {
            "id": 12,
            "text": "2.25 Stories"
          },
          {
            "id": 13,
            "text": "5 Stories"
          },
          {
            "id": 14,
            "text": "19 Stories"
          },
          {
            "id": 15,
            "text": "11 Stories"
          },
          {
            "id": 16,
            "text": "13 Stories"
          },
          {
            "id": 17,
            "text": "8 Stories"
          },
          {
            "id": 18,
            "text": "12 Stories"
          },
          {
            "id": 19,
            "text": "7 Stories"
          },
          {
            "id": 20,
            "text": "4.5 Stories"
          },
          {
            "id": 21,
            "text": "9 Stories"
          },
          {
            "id": 22,
            "text": "18 Stories"
          },
          {
            "id": 23,
            "text": "22 Stories"
          },
          {
            "id": 24,
            "text": "31 Stories"
          },
          {
            "id": 25,
            "text": "16 Stories"
          },
          {
            "id": 26,
            "text": "17 Stories"
          },
          {
            "id": 27,
            "text": "26 Stories"
          },
          {
            "id": 28,
            "text": "25 Stories"
          }
        ]
      },
      "number-of-units": {
        "label": "Number of Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-of-commercial-units": {
        "label": "Number of Commercial Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-of-buildings": {
        "label": "Number of Buildings",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sq-ft-per-unit": {
        "label": "Sq Ft per Unit",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "beds-per-unit": {
        "label": "Beds per Unit",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-9": {
        "label": ">",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-20": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "bedrooms": {
        "label": "Bedrooms",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "3"
          },
          {
            "id": 2,
            "text": "2"
          },
          {
            "id": 3,
            "text": "4"
          },
          {
            "id": 4,
            "text": "0"
          },
          {
            "id": 5,
            "text": "8"
          },
          {
            "id": 6,
            "text": "6"
          },
          {
            "id": 7,
            "text": "5"
          },
          {
            "id": 8,
            "text": "1"
          },
          {
            "id": 9,
            "text": "7"
          },
          {
            "id": 10,
            "text": "10"
          },
          {
            "id": 11,
            "text": "9"
          },
          {
            "id": 12,
            "text": "12"
          },
          {
            "id": 13,
            "text": "15"
          },
          {
            "id": 14,
            "text": "24"
          },
          {
            "id": 15,
            "text": "26"
          },
          {
            "id": 16,
            "text": "16"
          },
          {
            "id": 17,
            "text": "11"
          },
          {
            "id": 18,
            "text": "18"
          },
          {
            "id": 19,
            "text": "20"
          },
          {
            "id": 20,
            "text": "14"
          },
          {
            "id": 21,
            "text": "21"
          },
          {
            "id": 22,
            "text": "13"
          },
          {
            "id": 23,
            "text": "41"
          },
          {
            "id": 24,
            "text": "40"
          },
          {
            "id": 25,
            "text": "17"
          },
          {
            "id": 26,
            "text": "33"
          },
          {
            "id": 27,
            "text": "49"
          },
          {
            "id": 28,
            "text": "32"
          },
          {
            "id": 29,
            "text": "66"
          },
          {
            "id": 30,
            "text": "45"
          },
          {
            "id": 31,
            "text": "39"
          },
          {
            "id": 32,
            "text": "68"
          },
          {
            "id": 33,
            "text": "27"
          },
          {
            "id": 34,
            "text": "30"
          },
          {
            "id": 35,
            "text": "29"
          },
          {
            "id": 36,
            "text": "51"
          },
          {
            "id": 37,
            "text": "34"
          },
          {
            "id": 38,
            "text": "23"
          },
          {
            "id": 39,
            "text": "52"
          },
          {
            "id": 40,
            "text": "47"
          },
          {
            "id": 41,
            "text": "99"
          },
          {
            "id": 42,
            "text": "69"
          },
          {
            "id": 43,
            "text": "54"
          },
          {
            "id": 44,
            "text": "22"
          },
          {
            "id": 45,
            "text": "28"
          },
          {
            "id": 46,
            "text": "60"
          },
          {
            "id": 47,
            "text": "36"
          },
          {
            "id": 48,
            "text": "48"
          },
          {
            "id": 49,
            "text": "88"
          },
          {
            "id": 50,
            "text": "42"
          },
          {
            "id": 51,
            "text": "108"
          },
          {
            "id": 52,
            "text": "53"
          },
          {
            "id": 53,
            "text": "38"
          },
          {
            "id": 54,
            "text": "59"
          },
          {
            "id": 55,
            "text": "50"
          },
          {
            "id": 56,
            "text": "67"
          },
          {
            "id": 57,
            "text": "82"
          },
          {
            "id": 58,
            "text": "104"
          },
          {
            "id": 59,
            "text": "63"
          },
          {
            "id": 60,
            "text": "62"
          },
          {
            "id": 61,
            "text": "44"
          },
          {
            "id": 62,
            "text": "46"
          },
          {
            "id": 63,
            "text": "98"
          },
          {
            "id": 64,
            "text": "35"
          },
          {
            "id": 65,
            "text": "76"
          },
          {
            "id": 66,
            "text": "56"
          },
          {
            "id": 67,
            "text": "19"
          },
          {
            "id": 68,
            "text": "93"
          },
          {
            "id": 69,
            "text": "72"
          },
          {
            "id": 70,
            "text": "57"
          },
          {
            "id": 71,
            "text": "86"
          },
          {
            "id": 72,
            "text": "31"
          },
          {
            "id": 73,
            "text": "65"
          },
          {
            "id": 74,
            "text": "80"
          },
          {
            "id": 75,
            "text": "216"
          },
          {
            "id": 76,
            "text": "260"
          },
          {
            "id": 77,
            "text": "275"
          },
          {
            "id": 78,
            "text": "87"
          },
          {
            "id": 79,
            "text": "25"
          },
          {
            "id": 80,
            "text": "360"
          },
          {
            "id": 81,
            "text": "152"
          },
          {
            "id": 82,
            "text": "105"
          },
          {
            "id": 83,
            "text": "111"
          },
          {
            "id": 84,
            "text": "96"
          },
          {
            "id": 85,
            "text": "114"
          },
          {
            "id": 86,
            "text": "155"
          },
          {
            "id": 87,
            "text": "245"
          },
          {
            "id": 88,
            "text": "146"
          },
          {
            "id": 89,
            "text": "161"
          },
          {
            "id": 90,
            "text": "117"
          },
          {
            "id": 91,
            "text": "204"
          },
          {
            "id": 92,
            "text": "394"
          },
          {
            "id": 93,
            "text": "348"
          },
          {
            "id": 94,
            "text": "264"
          },
          {
            "id": 95,
            "text": "324"
          },
          {
            "id": 96,
            "text": "120"
          },
          {
            "id": 97,
            "text": "189"
          },
          {
            "id": 98,
            "text": "350"
          },
          {
            "id": 99,
            "text": "382"
          },
          {
            "id": 100,
            "text": "121"
          },
          {
            "id": 101,
            "text": "78"
          },
          {
            "id": 102,
            "text": "244"
          },
          {
            "id": 103,
            "text": "270"
          },
          {
            "id": 104,
            "text": "272"
          },
          {
            "id": 105,
            "text": "100"
          },
          {
            "id": 106,
            "text": "384"
          },
          {
            "id": 107,
            "text": "422"
          },
          {
            "id": 108,
            "text": "160"
          },
          {
            "id": 109,
            "text": "240"
          },
          {
            "id": 110,
            "text": "326"
          },
          {
            "id": 111,
            "text": "233"
          },
          {
            "id": 112,
            "text": "232"
          },
          {
            "id": 113,
            "text": "130"
          },
          {
            "id": 114,
            "text": "220"
          },
          {
            "id": 115,
            "text": "374"
          },
          {
            "id": 116,
            "text": "288"
          },
          {
            "id": 117,
            "text": "327"
          },
          {
            "id": 118,
            "text": "375"
          },
          {
            "id": 119,
            "text": "283"
          },
          {
            "id": 120,
            "text": "287"
          },
          {
            "id": 121,
            "text": "408"
          },
          {
            "id": 122,
            "text": "200"
          },
          {
            "id": 123,
            "text": "176"
          },
          {
            "id": 124,
            "text": "336"
          },
          {
            "id": 125,
            "text": "286"
          },
          {
            "id": 126,
            "text": "538"
          },
          {
            "id": 127,
            "text": "647"
          },
          {
            "id": 128,
            "text": "372"
          },
          {
            "id": 129,
            "text": "84"
          },
          {
            "id": 130,
            "text": "113"
          },
          {
            "id": 131,
            "text": "258"
          },
          {
            "id": 132,
            "text": "223"
          },
          {
            "id": 133,
            "text": "251"
          },
          {
            "id": 134,
            "text": "210"
          },
          {
            "id": 135,
            "text": "156"
          },
          {
            "id": 136,
            "text": "248"
          },
          {
            "id": 137,
            "text": "423"
          },
          {
            "id": 138,
            "text": "122"
          },
          {
            "id": 139,
            "text": "368"
          },
          {
            "id": 140,
            "text": "199"
          },
          {
            "id": 141,
            "text": "119"
          },
          {
            "id": 142,
            "text": "230"
          },
          {
            "id": 143,
            "text": "196"
          },
          {
            "id": 144,
            "text": "228"
          },
          {
            "id": 145,
            "text": "106"
          },
          {
            "id": 146,
            "text": "134"
          },
          {
            "id": 147,
            "text": "180"
          },
          {
            "id": 148,
            "text": "110"
          },
          {
            "id": 149,
            "text": "192"
          },
          {
            "id": 150,
            "text": "208"
          },
          {
            "id": 151,
            "text": "458"
          },
          {
            "id": 152,
            "text": "330"
          },
          {
            "id": 153,
            "text": "135"
          },
          {
            "id": 154,
            "text": "454"
          },
          {
            "id": 155,
            "text": "344"
          },
          {
            "id": 156,
            "text": "404"
          },
          {
            "id": 157,
            "text": "181"
          },
          {
            "id": 158,
            "text": "528"
          },
          {
            "id": 159,
            "text": "246"
          },
          {
            "id": 160,
            "text": "229"
          },
          {
            "id": 161,
            "text": "305"
          },
          {
            "id": 162,
            "text": "90"
          },
          {
            "id": 163,
            "text": "696"
          },
          {
            "id": 164,
            "text": "190"
          },
          {
            "id": 165,
            "text": "300"
          },
          {
            "id": 166,
            "text": "352"
          },
          {
            "id": 167,
            "text": "370"
          },
          {
            "id": 168,
            "text": "280"
          },
          {
            "id": 169,
            "text": "128"
          },
          {
            "id": 170,
            "text": "94"
          },
          {
            "id": 171,
            "text": "592"
          },
          {
            "id": 172,
            "text": "127"
          },
          {
            "id": 173,
            "text": "224"
          },
          {
            "id": 174,
            "text": "396"
          },
          {
            "id": 175,
            "text": "268"
          },
          {
            "id": 176,
            "text": "444"
          },
          {
            "id": 177,
            "text": "291"
          },
          {
            "id": 178,
            "text": "397"
          },
          {
            "id": 179,
            "text": "187"
          },
          {
            "id": 180,
            "text": "118"
          },
          {
            "id": 181,
            "text": "79"
          },
          {
            "id": 182,
            "text": "450"
          },
          {
            "id": 183,
            "text": "217"
          },
          {
            "id": 184,
            "text": "307"
          },
          {
            "id": 185,
            "text": "353"
          },
          {
            "id": 186,
            "text": "166"
          },
          {
            "id": 187,
            "text": "310"
          },
          {
            "id": 188,
            "text": "387"
          },
          {
            "id": 189,
            "text": "303"
          },
          {
            "id": 190,
            "text": "186"
          },
          {
            "id": 191,
            "text": "58"
          },
          {
            "id": 192,
            "text": "64"
          },
          {
            "id": 193,
            "text": "74"
          },
          {
            "id": 194,
            "text": "81"
          },
          {
            "id": 195,
            "text": "55"
          },
          {
            "id": 196,
            "text": "43"
          },
          {
            "id": 197,
            "text": "37"
          },
          {
            "id": 198,
            "text": "71"
          },
          {
            "id": 199,
            "text": "73"
          },
          {
            "id": 200,
            "text": "195"
          },
          {
            "id": 201,
            "text": "70"
          },
          {
            "id": 202,
            "text": "89"
          },
          {
            "id": 203,
            "text": "75"
          }
        ]
      },
      "bathrooms": {
        "label": "Bathrooms",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "1"
          },
          {
            "id": 2,
            "text": "2"
          },
          {
            "id": 3,
            "text": "8"
          },
          {
            "id": 4,
            "text": "4"
          },
          {
            "id": 5,
            "text": "3"
          },
          {
            "id": 6,
            "text": "1.5"
          },
          {
            "id": 7,
            "text": "2.5"
          },
          {
            "id": 8,
            "text": "4.5"
          },
          {
            "id": 9,
            "text": "7"
          },
          {
            "id": 10,
            "text": "5"
          },
          {
            "id": 11,
            "text": "6"
          },
          {
            "id": 12,
            "text": "3.5"
          },
          {
            "id": 13,
            "text": "1.75"
          },
          {
            "id": 14,
            "text": "17.25"
          },
          {
            "id": 15,
            "text": "11"
          },
          {
            "id": 16,
            "text": "17"
          },
          {
            "id": 17,
            "text": "9"
          },
          {
            "id": 18,
            "text": "15"
          },
          {
            "id": 19,
            "text": "10"
          },
          {
            "id": 20,
            "text": "16"
          },
          {
            "id": 21,
            "text": "24"
          },
          {
            "id": 22,
            "text": "12"
          },
          {
            "id": 23,
            "text": "18"
          },
          {
            "id": 24,
            "text": "8.5"
          },
          {
            "id": 25,
            "text": "5.5"
          },
          {
            "id": 26,
            "text": "4.25"
          },
          {
            "id": 27,
            "text": "2.75"
          },
          {
            "id": 28,
            "text": "13"
          },
          {
            "id": 29,
            "text": "20"
          },
          {
            "id": 30,
            "text": "2.25"
          },
          {
            "id": 31,
            "text": "14"
          },
          {
            "id": 32,
            "text": "6.5"
          },
          {
            "id": 33,
            "text": "1.25"
          },
          {
            "id": 34,
            "text": "21"
          },
          {
            "id": 35,
            "text": "3.25"
          },
          {
            "id": 36,
            "text": "0"
          },
          {
            "id": 37,
            "text": "6.25"
          },
          {
            "id": 38,
            "text": "3.75"
          },
          {
            "id": 39,
            "text": "5.75"
          },
          {
            "id": 40,
            "text": "5.25"
          },
          {
            "id": 41,
            "text": "47"
          },
          {
            "id": 42,
            "text": "45"
          },
          {
            "id": 43,
            "text": "30"
          },
          {
            "id": 44,
            "text": "13.5"
          },
          {
            "id": 45,
            "text": "10.5"
          },
          {
            "id": 46,
            "text": "22"
          },
          {
            "id": 47,
            "text": "19.5"
          },
          {
            "id": 48,
            "text": "0.5"
          },
          {
            "id": 49,
            "text": "4.75"
          },
          {
            "id": 50,
            "text": "7.5"
          },
          {
            "id": 51,
            "text": "19"
          },
          {
            "id": 52,
            "text": "68"
          },
          {
            "id": 53,
            "text": "9.5"
          },
          {
            "id": 54,
            "text": "32"
          },
          {
            "id": 55,
            "text": "28"
          },
          {
            "id": 56,
            "text": "85"
          },
          {
            "id": 57,
            "text": "87"
          },
          {
            "id": 58,
            "text": "99"
          },
          {
            "id": 59,
            "text": "35"
          },
          {
            "id": 60,
            "text": "50"
          },
          {
            "id": 61,
            "text": "40"
          },
          {
            "id": 62,
            "text": "21.75"
          },
          {
            "id": 63,
            "text": "31"
          },
          {
            "id": 64,
            "text": "42"
          },
          {
            "id": 65,
            "text": "12.5"
          },
          {
            "id": 66,
            "text": "26"
          },
          {
            "id": 67,
            "text": "65"
          },
          {
            "id": 68,
            "text": "152"
          },
          {
            "id": 69,
            "text": "72"
          },
          {
            "id": 70,
            "text": "54"
          },
          {
            "id": 71,
            "text": "105"
          },
          {
            "id": 72,
            "text": "143"
          },
          {
            "id": 73,
            "text": "93"
          },
          {
            "id": 74,
            "text": "90"
          },
          {
            "id": 75,
            "text": "48"
          },
          {
            "id": 76,
            "text": "52"
          },
          {
            "id": 77,
            "text": "51"
          },
          {
            "id": 78,
            "text": "60"
          },
          {
            "id": 79,
            "text": "66"
          },
          {
            "id": 80,
            "text": "114"
          },
          {
            "id": 81,
            "text": "141"
          },
          {
            "id": 82,
            "text": "245"
          },
          {
            "id": 83,
            "text": "133"
          },
          {
            "id": 84,
            "text": "44"
          },
          {
            "id": 85,
            "text": "57"
          },
          {
            "id": 86,
            "text": "56"
          },
          {
            "id": 87,
            "text": "88"
          },
          {
            "id": 88,
            "text": "37"
          },
          {
            "id": 89,
            "text": "58"
          },
          {
            "id": 90,
            "text": "29"
          },
          {
            "id": 91,
            "text": "33"
          },
          {
            "id": 92,
            "text": "36"
          },
          {
            "id": 93,
            "text": "80"
          },
          {
            "id": 94,
            "text": "117"
          },
          {
            "id": 95,
            "text": "78"
          },
          {
            "id": 96,
            "text": "27"
          },
          {
            "id": 97,
            "text": "83"
          },
          {
            "id": 98,
            "text": "75"
          },
          {
            "id": 99,
            "text": "79"
          },
          {
            "id": 100,
            "text": "34"
          },
          {
            "id": 101,
            "text": "55"
          },
          {
            "id": 102,
            "text": "41"
          },
          {
            "id": 103,
            "text": "97"
          },
          {
            "id": 104,
            "text": "39"
          },
          {
            "id": 105,
            "text": "46"
          },
          {
            "id": 106,
            "text": "38"
          },
          {
            "id": 107,
            "text": "25"
          },
          {
            "id": 108,
            "text": "76"
          },
          {
            "id": 109,
            "text": "69"
          },
          {
            "id": 110,
            "text": "23"
          },
          {
            "id": 111,
            "text": "64"
          },
          {
            "id": 112,
            "text": "81"
          },
          {
            "id": 113,
            "text": "86"
          },
          {
            "id": 114,
            "text": "195"
          },
          {
            "id": 115,
            "text": "70"
          },
          {
            "id": 116,
            "text": "95"
          },
          {
            "id": 117,
            "text": "74"
          },
          {
            "id": 118,
            "text": "63"
          },
          {
            "id": 119,
            "text": "96"
          },
          {
            "id": 120,
            "text": "53"
          },
          {
            "id": 121,
            "text": "91"
          },
          {
            "id": 122,
            "text": "49"
          },
          {
            "id": 123,
            "text": "219"
          },
          {
            "id": 124,
            "text": "43"
          },
          {
            "id": 125,
            "text": "198"
          },
          {
            "id": 126,
            "text": "84"
          },
          {
            "id": 127,
            "text": "77"
          },
          {
            "id": 128,
            "text": "109"
          },
          {
            "id": 129,
            "text": "139"
          },
          {
            "id": 130,
            "text": "196"
          },
          {
            "id": 131,
            "text": "112"
          },
          {
            "id": 132,
            "text": "144"
          },
          {
            "id": 133,
            "text": "62"
          }
        ]
      },
      "square-feet": {
        "label": "Square Feet",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sq-ft-range": {
        "label": "Sq Ft Range",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "0–1000"
          },
          {
            "id": 2,
            "text": "Non-SFR"
          },
          {
            "id": 3,
            "text": "2000–2500"
          },
          {
            "id": 4,
            "text": "1750–2000"
          },
          {
            "id": 5,
            "text": "1500–1750"
          },
          {
            "id": 6,
            "text": "1250–1500"
          },
          {
            "id": 7,
            "text": "2500–3000"
          },
          {
            "id": 8,
            "text": "3000+"
          },
          {
            "id": 9,
            "text": "1000–1250"
          }
        ]
      },
      "year-build": {
        "label": "Year Build",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "effective-year-build": {
        "label": "Effective Year Build",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "construction-type": {
        "label": "Construction Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Frame"
          },
          {
            "id": 2,
            "text": "Masonry"
          },
          {
            "id": 3,
            "text": "Wood"
          },
          {
            "id": 4,
            "text": "Brick"
          },
          {
            "id": 5,
            "text": "Concrete"
          },
          {
            "id": 6,
            "text": "Steel"
          },
          {
            "id": 7,
            "text": "Other"
          },
          {
            "id": 8,
            "text": "Manufactured"
          },
          {
            "id": 9,
            "text": "Concrete Block"
          },
          {
            "id": 10,
            "text": "Stone"
          },
          {
            "id": 11,
            "text": "Tilt-up (pre-cast concrete)"
          },
          {
            "id": 12,
            "text": "Metal"
          },
          {
            "id": 13,
            "text": "Adobe"
          }
        ]
      },
      "exterior-walls": {
        "label": "Exterior Walls",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Stucco"
          },
          {
            "id": 2,
            "text": "Concrete Block"
          },
          {
            "id": 3,
            "text": "Other"
          },
          {
            "id": 4,
            "text": "Wood"
          },
          {
            "id": 5,
            "text": "Brick veneer"
          },
          {
            "id": 6,
            "text": "Brick"
          },
          {
            "id": 7,
            "text": "Asbestos shingle"
          },
          {
            "id": 8,
            "text": "Wood Shingle"
          },
          {
            "id": 9,
            "text": "Combination"
          },
          {
            "id": 10,
            "text": "Concrete"
          },
          {
            "id": 11,
            "text": "Siding (Alum/Vinyl)"
          },
          {
            "id": 12,
            "text": "Composition/Composite"
          },
          {
            "id": 13,
            "text": "Block"
          },
          {
            "id": 14,
            "text": "Wood Siding"
          },
          {
            "id": 15,
            "text": "Shingle (Not Wood)"
          },
          {
            "id": 16,
            "text": "Metal"
          },
          {
            "id": 17,
            "text": "Rock, Stone"
          },
          {
            "id": 18,
            "text": "Siding Not (aluminum, vinyl, etc.)"
          },
          {
            "id": 19,
            "text": "Adobe"
          },
          {
            "id": 20,
            "text": "Fiber cement siding (Hardi-board/Hardi-plank)"
          },
          {
            "id": 21,
            "text": "Masonry"
          },
          {
            "id": 22,
            "text": "Log"
          },
          {
            "id": 23,
            "text": "Vinyl siding"
          },
          {
            "id": 24,
            "text": "Tile"
          },
          {
            "id": 25,
            "text": "Glass"
          },
          {
            "id": 26,
            "text": "Aluminum siding"
          },
          {
            "id": 27,
            "text": "Tilt-up (pre-cast concrete)"
          }
        ]
      },
      "floor-cover": {
        "label": "Floor Cover",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Carpet"
          },
          {
            "id": 2,
            "text": "Wood"
          },
          {
            "id": 3,
            "text": "Tile"
          },
          {
            "id": 4,
            "text": "Cork"
          },
          {
            "id": 5,
            "text": "Vinyl"
          },
          {
            "id": 6,
            "text": "Concrete"
          },
          {
            "id": 7,
            "text": "Plywood"
          },
          {
            "id": 8,
            "text": "Ceramic"
          },
          {
            "id": 9,
            "text": "Terrazzo"
          },
          {
            "id": 10,
            "text": "Parquet"
          },
          {
            "id": 11,
            "text": "Linoleum"
          },
          {
            "id": 12,
            "text": "Covered"
          },
          {
            "id": 13,
            "text": "Floating Floor/laminate"
          },
          {
            "id": 14,
            "text": "Slate"
          },
          {
            "id": 15,
            "text": "Marble"
          }
        ]
      },
      "basement": {
        "label": "Basement",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No Basement"
          },
          {
            "id": 2,
            "text": "Unspecified Basement"
          },
          {
            "id": 3,
            "text": "Unfinished Basement"
          },
          {
            "id": 4,
            "text": "Partial Basement"
          },
          {
            "id": 5,
            "text": "Full Basement"
          },
          {
            "id": 6,
            "text": "Improved Basement (Finished)"
          },
          {
            "id": 7,
            "text": "Daylight, Full"
          },
          {
            "id": 8,
            "text": "Daylight, Partial"
          }
        ]
      },
      "other-rooms": {
        "label": "Other Rooms",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Family Room/Den"
          },
          {
            "id": 2,
            "text": "Utility room"
          },
          {
            "id": 3,
            "text": "Bonus Room"
          },
          {
            "id": 4,
            "text": "Sun, Solarium, Florida room"
          },
          {
            "id": 5,
            "text": "Game / Recreation room"
          },
          {
            "id": 6,
            "text": "Laundry Room"
          },
          {
            "id": 7,
            "text": "Media room/Home theater"
          }
        ]
      },
      "number-of-fireplaces": {
        "label": "Number of Fireplaces",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "patio": {
        "label": "Patio",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Patio - Screened"
          },
          {
            "id": 2,
            "text": "Patio - Unknown"
          }
        ]
      },
      "porch": {
        "label": "Porch",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Porch"
          },
          {
            "id": 2,
            "text": "Porch - Open"
          },
          {
            "id": 3,
            "text": "Porch screened"
          },
          {
            "id": 4,
            "text": "Portico (drive under)"
          },
          {
            "id": 5,
            "text": "Porch covered"
          }
        ]
      },
      "deck": {
        "label": "Deck",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No"
          },
          {
            "id": 2,
            "text": "Yes"
          }
        ]
      },
      "driveway": {
        "label": "Driveway",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Gravel"
          },
          {
            "id": 2,
            "text": "Unknown"
          },
          {
            "id": 3,
            "text": "Asphalt"
          },
          {
            "id": 4,
            "text": "Concrete"
          },
          {
            "id": 5,
            "text": "Paver"
          },
          {
            "id": 6,
            "text": "Bomanite"
          }
        ]
      },
      "garage": {
        "label": "Garage",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Garage"
          },
          {
            "id": 2,
            "text": "Attached Garage"
          },
          {
            "id": 3,
            "text": "Carport"
          },
          {
            "id": 4,
            "text": "Detached Garage"
          },
          {
            "id": 5,
            "text": "Covered"
          },
          {
            "id": 6,
            "text": "None"
          },
          {
            "id": 7,
            "text": "Mixed"
          },
          {
            "id": 8,
            "text": "Underground/Basement"
          },
          {
            "id": 9,
            "text": "Paved/Surfaced"
          },
          {
            "id": 10,
            "text": "Finished - Detached"
          },
          {
            "id": 11,
            "text": "Built-in"
          },
          {
            "id": 12,
            "text": "Open"
          },
          {
            "id": 13,
            "text": "Tuckunder"
          }
        ]
      },
      "garage-square-feet": {
        "label": "Garage Square Feet",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "air-conditioning": {
        "label": "Air Conditioning",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Evaporative Cooler"
          },
          {
            "id": 2,
            "text": "Central"
          },
          {
            "id": 3,
            "text": "Yes"
          },
          {
            "id": 4,
            "text": "Wall"
          },
          {
            "id": 5,
            "text": "Window/Unit"
          },
          {
            "id": 6,
            "text": "Packaged Unit"
          },
          {
            "id": 7,
            "text": "Refrigeration"
          },
          {
            "id": 8,
            "text": "None"
          },
          {
            "id": 9,
            "text": "Partial"
          },
          {
            "id": 10,
            "text": "Chilled Water"
          },
          {
            "id": 11,
            "text": "Other"
          }
        ]
      },
      "heating-type": {
        "label": "Heating Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Electric"
          },
          {
            "id": 2,
            "text": "Floor/Wall"
          },
          {
            "id": 3,
            "text": "Central"
          },
          {
            "id": 4,
            "text": "Yes"
          },
          {
            "id": 5,
            "text": "Convection"
          },
          {
            "id": 6,
            "text": "Space/Suspended"
          },
          {
            "id": 7,
            "text": "Forced air unit"
          },
          {
            "id": 8,
            "text": "None"
          },
          {
            "id": 9,
            "text": "Gravity"
          },
          {
            "id": 10,
            "text": "Solar"
          },
          {
            "id": 11,
            "text": "Radiant"
          },
          {
            "id": 12,
            "text": "Gas"
          },
          {
            "id": 13,
            "text": "Heat Pump"
          },
          {
            "id": 14,
            "text": "Oil"
          },
          {
            "id": 15,
            "text": "Steam"
          },
          {
            "id": 16,
            "text": "Hot Water"
          },
          {
            "id": 17,
            "text": "Zone"
          },
          {
            "id": 18,
            "text": "Baseboard"
          },
          {
            "id": 19,
            "text": "Vent"
          },
          {
            "id": 20,
            "text": "Other"
          },
          {
            "id": 21,
            "text": "Wood Burning"
          },
          {
            "id": 22,
            "text": "Partial"
          }
        ]
      },
      "heating-fuel-type": {
        "label": "Heating Fuel Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Electric"
          },
          {
            "id": 2,
            "text": "Gas"
          },
          {
            "id": 3,
            "text": "Solar"
          },
          {
            "id": 4,
            "text": "Oil"
          },
          {
            "id": 5,
            "text": "None"
          },
          {
            "id": 6,
            "text": "Coal"
          },
          {
            "id": 7,
            "text": "Wood"
          }
        ]
      },
      "interior-walls": {
        "label": "Interior Walls",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Plaster"
          },
          {
            "id": 2,
            "text": "Gypsum Board/Drywall/Sheetrock/Wallboard"
          },
          {
            "id": 3,
            "text": "Plywood/Minimum"
          },
          {
            "id": 4,
            "text": "Wood"
          },
          {
            "id": 5,
            "text": "Paneling"
          },
          {
            "id": 6,
            "text": "Other"
          },
          {
            "id": 7,
            "text": "Masonry"
          },
          {
            "id": 8,
            "text": "Finished/Painted"
          },
          {
            "id": 9,
            "text": "Unfinished"
          },
          {
            "id": 10,
            "text": "Vinyl"
          },
          {
            "id": 11,
            "text": "Decorative\\Custom"
          }
        ]
      },
      "roof-cover": {
        "label": "Roof Cover",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Wood Shake/ Shingles"
          },
          {
            "id": 2,
            "text": "Built-up"
          },
          {
            "id": 3,
            "text": "Composition Shingle"
          },
          {
            "id": 4,
            "text": "Other"
          },
          {
            "id": 5,
            "text": "Asphalt"
          },
          {
            "id": 6,
            "text": "Tar & Gravel"
          },
          {
            "id": 7,
            "text": "Metal"
          },
          {
            "id": 8,
            "text": "Concrete"
          },
          {
            "id": 9,
            "text": "Asbestos"
          },
          {
            "id": 10,
            "text": "Tile"
          },
          {
            "id": 11,
            "text": "Wood"
          },
          {
            "id": 12,
            "text": "Rock / Gravel"
          },
          {
            "id": 13,
            "text": "Aluminum"
          },
          {
            "id": 14,
            "text": "Slate"
          },
          {
            "id": 15,
            "text": "Steel"
          },
          {
            "id": 16,
            "text": "Shingle (Not Wood)"
          },
          {
            "id": 17,
            "text": "Roll Composition"
          },
          {
            "id": 18,
            "text": "Clay tile"
          },
          {
            "id": 19,
            "text": "Fiberglass"
          }
        ]
      },
      "roof-type": {
        "label": "Roof Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Hip"
          },
          {
            "id": 2,
            "text": "Mansard"
          },
          {
            "id": 3,
            "text": "Gable or Hip"
          },
          {
            "id": 4,
            "text": "Gable"
          },
          {
            "id": 5,
            "text": "Flat"
          },
          {
            "id": 6,
            "text": "Irr/Cathedral"
          },
          {
            "id": 7,
            "text": "Gambrel"
          },
          {
            "id": 8,
            "text": "Dome"
          },
          {
            "id": 9,
            "text": "Sawtooth"
          },
          {
            "id": 10,
            "text": "Wood Truss"
          },
          {
            "id": 11,
            "text": "Shed"
          },
          {
            "id": 12,
            "text": "Rigid Frm Bar Jt"
          },
          {
            "id": 13,
            "text": "Bowstring Truss"
          },
          {
            "id": 14,
            "text": "Steel Frame/Truss"
          },
          {
            "id": 15,
            "text": "Prestress Concrete"
          }
        ]
      },
      "pool": {
        "label": "Pool",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No"
          },
          {
            "id": 2,
            "text": "Pool (yes)"
          },
          {
            "id": 3,
            "text": "Spa or Hot Tub (only)"
          },
          {
            "id": 4,
            "text": "Above ground pool"
          },
          {
            "id": 5,
            "text": "Pool & Spa (both)"
          },
          {
            "id": 6,
            "text": "Solar Heated"
          },
          {
            "id": 7,
            "text": "Heated Pool"
          },
          {
            "id": 8,
            "text": "In-Ground Pool"
          },
          {
            "id": 9,
            "text": "Vinyl In-ground Pool"
          },
          {
            "id": 10,
            "text": "Community Pool or Spa"
          },
          {
            "id": 11,
            "text": "Indoor Swimming Pool"
          },
          {
            "id": 12,
            "text": "Enclosed"
          }
        ]
      },
      "field-10": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-sale-date": {
        "label": "Last Sale Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "years-since-last-sale": {
        "label": "Ownership Years",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-sale-document": {
        "label": "Last Sale Document",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Grant Deed"
          },
          {
            "id": 2,
            "text": "Warranty Deed"
          },
          {
            "id": 3,
            "text": "Special Warranty Deed"
          },
          {
            "id": 4,
            "text": "Executor’s Deed"
          },
          {
            "id": 5,
            "text": "Vendor’s Lien Warranty Deed"
          },
          {
            "id": 6,
            "text": "Deed"
          },
          {
            "id": 7,
            "text": "Public Action"
          },
          {
            "id": 8,
            "text": "Intrafamily Transfer"
          },
          {
            "id": 9,
            "text": "Corporation Deed"
          },
          {
            "id": 10,
            "text": "Joint Tenancy Deed"
          },
          {
            "id": 11,
            "text": "Cash Sale Deed"
          },
          {
            "id": 12,
            "text": "Correction Document"
          },
          {
            "id": 13,
            "text": "Quit Claim Deed"
          },
          {
            "id": 14,
            "text": "Individual Deed"
          },
          {
            "id": 15,
            "text": "Trustee’s Deed"
          },
          {
            "id": 16,
            "text": "Sheriff’s Deed"
          },
          {
            "id": 17,
            "text": "Foreclosure"
          },
          {
            "id": 18,
            "text": "Administrator’s Deed"
          },
          {
            "id": 19,
            "text": "Conservator’s Deed"
          },
          {
            "id": 20,
            "text": "Re-recorded Document"
          },
          {
            "id": 21,
            "text": "Partnership Deed"
          },
          {
            "id": 22,
            "text": "Other"
          },
          {
            "id": 23,
            "text": "Personal Representatives Deed"
          },
          {
            "id": 24,
            "text": "Survivorship Deed/Survivor Property Agreement"
          },
          {
            "id": 25,
            "text": "Deed in Lieu of Foreclosure"
          },
          {
            "id": 26,
            "text": "Contract of Sale"
          },
          {
            "id": 27,
            "text": "Deed of Distribution"
          },
          {
            "id": 28,
            "text": "Limited Warranty Deed"
          },
          {
            "id": 29,
            "text": "Land Contract"
          },
          {
            "id": 30,
            "text": "Agreement of Sale"
          },
          {
            "id": 31,
            "text": "Beneficiary Deed"
          },
          {
            "id": 32,
            "text": "Legal Action/Court Order"
          },
          {
            "id": 33,
            "text": "Deed of Guardian"
          },
          {
            "id": 34,
            "text": "Bargain and Sale Deed"
          },
          {
            "id": 35,
            "text": "Affidavit of Death of Joint Tenant"
          },
          {
            "id": 36,
            "text": "Redemption Deed"
          },
          {
            "id": 37,
            "text": "Commissioner’s Deed"
          },
          {
            "id": 38,
            "text": "Gift Deed"
          },
          {
            "id": 39,
            "text": "Transaction History Record"
          },
          {
            "id": 40,
            "text": "Quit Claim Deed (arms length)"
          },
          {
            "id": 41,
            "text": "Fiduciary Deed"
          },
          {
            "id": 42,
            "text": "Receiver’s Deed"
          },
          {
            "id": 43,
            "text": "Certificate of Transfer"
          },
          {
            "id": 44,
            "text": "Transfer on Death Deed"
          },
          {
            "id": 45,
            "text": "Special Master Deed"
          },
          {
            "id": 46,
            "text": "Assignment Deed"
          },
          {
            "id": 47,
            "text": "Affidavit"
          },
          {
            "id": 48,
            "text": "Referee’s Deed"
          },
          {
            "id": 49,
            "text": "Affidavit of Death of Life Tenant"
          },
          {
            "id": 50,
            "text": "Distress Sale"
          },
          {
            "id": 51,
            "text": "Assignment of Lease"
          },
          {
            "id": 52,
            "text": "Ground Lease"
          },
          {
            "id": 53,
            "text": "Exchange"
          },
          {
            "id": 54,
            "text": "Condominium Deed"
          },
          {
            "id": 55,
            "text": "Lease"
          },
          {
            "id": 56,
            "text": "Mortgage"
          }
        ]
      },
      "estimated-value": {
        "label": "Estimated Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-equity-amount": {
        "label": "Estimated Equity Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-equity-percent": {
        "label": "Estimated Equity Percent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-11": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-delinquent-2": {
        "label": "Tax Delinquent",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "No"
          },
          {
            "id": 2,
            "text": "Yes"
          }
        ]
      },
      "tax-delinquent-year": {
        "label": "Tax Delinquent Year",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-amount": {
        "label": "Tax Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-assessment-year": {
        "label": "Tax Assessment Year",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "accessed-total-value": {
        "label": "Accessed Total Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculated-total-value": {
        "label": "Calculated Total Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "accessed-land-value": {
        "label": "Accessed Land Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculated-land-value": {
        "label": "Calculated Land Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "accessed-improvement-value": {
        "label": "Accessed Improvement Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculated-improvement-value": {
        "label": "Calculated Improvement Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-13": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-loan-amount": {
        "label": "Total Loan Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-loan-balance": {
        "label": "Total Loan Balance",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-loan-payment": {
        "label": "Total Loan Payment",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-6": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-repair-cost": {
        "label": "Estimated Repair Cost",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "building-quality": {
        "label": "Building Quality",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "C"
          },
          {
            "id": 2,
            "text": "D"
          },
          {
            "id": 3,
            "text": "E"
          },
          {
            "id": 4,
            "text": "E-"
          },
          {
            "id": 5,
            "text": "B"
          },
          {
            "id": 6,
            "text": "A"
          },
          {
            "id": 7,
            "text": "C+"
          },
          {
            "id": 8,
            "text": "B+"
          },
          {
            "id": 9,
            "text": "D+"
          },
          {
            "id": 10,
            "text": "C-"
          },
          {
            "id": 11,
            "text": "D-"
          },
          {
            "id": 12,
            "text": "B-"
          },
          {
            "id": 13,
            "text": "E+"
          },
          {
            "id": 14,
            "text": "A+"
          },
          {
            "id": 15,
            "text": "A-"
          }
        ]
      },
      "estimated-repair-cost-per-sq-ft": {
        "label": "Estimated Repair Cost Per Sq Ft",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "$20-$50"
          },
          {
            "id": 2,
            "text": "$50-$100"
          },
          {
            "id": 3,
            "text": "$10-$20"
          }
        ]
      },
      "building-condition": {
        "label": "Building Condition",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Unsound"
          },
          {
            "id": 2,
            "text": "Very Good"
          },
          {
            "id": 3,
            "text": "Excellent"
          },
          {
            "id": 4,
            "text": "Good"
          },
          {
            "id": 5,
            "text": "Average"
          },
          {
            "id": 6,
            "text": "Unknown"
          },
          {
            "id": 7,
            "text": "Fair"
          },
          {
            "id": 8,
            "text": "Poor"
          }
        ]
      },
      "renovation-level": {
        "label": "Renovation Level",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Structural"
          },
          {
            "id": 2,
            "text": "Full Rehab"
          },
          {
            "id": 3,
            "text": "Cosmetic"
          },
          {
            "id": 4,
            "text": "Moderate"
          }
        ]
      },
      "field-7": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "legal-description": {
        "label": "Legal Description",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "apn-number": {
        "label": "APN Number",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "lot-size-acres": {
        "label": "Lot Size (Acres)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "lot-size-square-feet": {
        "label": "Lot Size (Square Feet)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sewer": {
        "label": "Sewer",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Municipal"
          },
          {
            "id": 2,
            "text": "Yes"
          },
          {
            "id": 3,
            "text": "Septic"
          },
          {
            "id": 4,
            "text": "Storm"
          },
          {
            "id": 5,
            "text": "None"
          }
        ]
      },
      "water": {
        "label": "Water",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Municipal"
          },
          {
            "id": 2,
            "text": "None"
          },
          {
            "id": 3,
            "text": "Yes"
          },
          {
            "id": 4,
            "text": "Cistern"
          },
          {
            "id": 5,
            "text": "Well"
          }
        ]
      },
      "topography": {
        "label": "Topography",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "ROLLING"
          },
          {
            "id": 2,
            "text": "Level grade"
          },
          {
            "id": 3,
            "text": "Low Elevation"
          },
          {
            "id": 4,
            "text": "STEEP"
          },
          {
            "id": 5,
            "text": "Below street level"
          },
          {
            "id": 6,
            "text": "Above street level"
          },
          {
            "id": 7,
            "text": "High elevation"
          },
          {
            "id": 8,
            "text": "SWAMPY"
          },
          {
            "id": 9,
            "text": "ROCKY"
          },
          {
            "id": 10,
            "text": "WOODED"
          },
          {
            "id": 11,
            "text": "MIXED"
          }
        ]
      },
      "zoning": {
        "label": "Zoning",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "R1"
          },
          {
            "id": 2,
            "text": "R-1"
          },
          {
            "id": 3,
            "text": "R2"
          },
          {
            "id": 4,
            "text": "Z67"
          },
          {
            "id": 5,
            "text": "R-2A/T/AN"
          },
          {
            "id": 6,
            "text": "Z177"
          },
          {
            "id": 7,
            "text": "RD-5"
          },
          {
            "id": 8,
            "text": "R-4"
          },
          {
            "id": 9,
            "text": "R-1-EA-4 R"
          },
          {
            "id": 10,
            "text": "R5"
          },
          {
            "id": 11,
            "text": "Z190"
          },
          {
            "id": 12,
            "text": "RS"
          },
          {
            "id": 13,
            "text": "R-2"
          },
          {
            "id": 14,
            "text": "RMD-B"
          },
          {
            "id": 15,
            "text": "C-CBD"
          },
          {
            "id": 16,
            "text": "R-1-EA-4"
          },
          {
            "id": 17,
            "text": "R-3A/W"
          },
          {
            "id": 18,
            "text": "R4"
          },
          {
            "id": 19,
            "text": "R-2B"
          },
          {
            "id": 20,
            "text": "RD5"
          },
          {
            "id": 21,
            "text": "R1C"
          },
          {
            "id": 22,
            "text": "R-3"
          },
          {
            "id": 23,
            "text": "RMD-A"
          },
          {
            "id": 24,
            "text": "RLD-60"
          },
          {
            "id": 25,
            "text": "M-1"
          },
          {
            "id": 26,
            "text": "R-1A-SPD"
          },
          {
            "id": 27,
            "text": "RD-10"
          },
          {
            "id": 28,
            "text": "E"
          },
          {
            "id": 29,
            "text": "R-S"
          },
          {
            "id": 30,
            "text": "R3"
          },
          {
            "id": 31,
            "text": "RMX"
          },
          {
            "id": 32,
            "text": "R2A"
          },
          {
            "id": 33,
            "text": "SR1"
          },
          {
            "id": 34,
            "text": "D"
          },
          {
            "id": 35,
            "text": "Z69"
          },
          {
            "id": 36,
            "text": "R-3B"
          },
          {
            "id": 37,
            "text": "CRO"
          },
          {
            "id": 38,
            "text": "MU"
          },
          {
            "id": 39,
            "text": "I-G"
          },
          {
            "id": 40,
            "text": "RD-12"
          },
          {
            "id": 41,
            "text": "RD-7"
          },
          {
            "id": 42,
            "text": "RD10"
          },
          {
            "id": 43,
            "text": "M1"
          },
          {
            "id": 44,
            "text": "AE5"
          },
          {
            "id": 45,
            "text": "C1"
          },
          {
            "id": 46,
            "text": "Z164"
          },
          {
            "id": 47,
            "text": "Z239"
          },
          {
            "id": 48,
            "text": "R-NC"
          },
          {
            "id": 49,
            "text": "RMD-S"
          },
          {
            "id": 50,
            "text": "CCG-2"
          },
          {
            "id": 51,
            "text": "C-2"
          },
          {
            "id": 52,
            "text": "RD-6"
          },
          {
            "id": 53,
            "text": "R-1A"
          },
          {
            "id": 54,
            "text": "R-1-SC"
          },
          {
            "id": 55,
            "text": "SP"
          },
          {
            "id": 56,
            "text": "Z414"
          },
          {
            "id": 57,
            "text": "Z191"
          },
          {
            "id": 58,
            "text": "RD-4"
          },
          {
            "id": 59,
            "text": "R-1A-R"
          },
          {
            "id": 60,
            "text": "R-2-MH"
          },
          {
            "id": 61,
            "text": "RD-20"
          },
          {
            "id": 62,
            "text": "RF"
          },
          {
            "id": 63,
            "text": "RR"
          },
          {
            "id": 64,
            "text": "AE20"
          },
          {
            "id": 65,
            "text": "C-1"
          },
          {
            "id": 66,
            "text": "C-O"
          },
          {
            "id": 67,
            "text": "R-3B/AN"
          },
          {
            "id": 68,
            "text": "MUL"
          },
          {
            "id": 69,
            "text": "CP"
          },
          {
            "id": 70,
            "text": "Z413"
          },
          {
            "id": 71,
            "text": "B2"
          },
          {
            "id": 72,
            "text": "R-2A"
          },
          {
            "id": 73,
            "text": "SPA"
          },
          {
            "id": 74,
            "text": "A-10"
          },
          {
            "id": 75,
            "text": "P.U.D."
          },
          {
            "id": 76,
            "text": "RA"
          },
          {
            "id": 77,
            "text": "O3"
          },
          {
            "id": 78,
            "text": "Z59"
          },
          {
            "id": 79,
            "text": "C2"
          },
          {
            "id": 80,
            "text": "Z298"
          },
          {
            "id": 81,
            "text": "A"
          },
          {
            "id": 82,
            "text": "RD2"
          },
          {
            "id": 83,
            "text": "102"
          },
          {
            "id": 84,
            "text": "C5"
          },
          {
            "id": 85,
            "text": "R1B"
          },
          {
            "id": 86,
            "text": "OCR2"
          },
          {
            "id": 87,
            "text": "CRO-S"
          },
          {
            "id": 88,
            "text": "Z65"
          },
          {
            "id": 89,
            "text": "R-2A/T/PH"
          },
          {
            "id": 90,
            "text": "P-D"
          },
          {
            "id": 91,
            "text": "NR1"
          },
          {
            "id": 92,
            "text": "RD 10"
          },
          {
            "id": 93,
            "text": "RD 5"
          },
          {
            "id": 94,
            "text": "M-2"
          },
          {
            "id": 95,
            "text": "R17"
          },
          {
            "id": 96,
            "text": "Z392"
          },
          {
            "id": 97,
            "text": "MF2"
          },
          {
            "id": 98,
            "text": "PUD"
          },
          {
            "id": 99,
            "text": "Z46"
          },
          {
            "id": 100,
            "text": "HU-RM1"
          },
          {
            "id": 101,
            "text": "RMD-D"
          },
          {
            "id": 102,
            "text": "VCC-2"
          },
          {
            "id": 103,
            "text": "HU-RD2"
          },
          {
            "id": 104,
            "text": "Z297"
          },
          {
            "id": 105,
            "text": "R-1-C"
          },
          {
            "id": 106,
            "text": "A1"
          },
          {
            "id": 107,
            "text": "HC3"
          },
          {
            "id": 108,
            "text": "Z14"
          },
          {
            "id": 109,
            "text": "Z83"
          },
          {
            "id": 110,
            "text": "MH1"
          },
          {
            "id": 111,
            "text": "CA2"
          },
          {
            "id": 112,
            "text": "SPA-OT"
          },
          {
            "id": 113,
            "text": "R-1-EA-2"
          },
          {
            "id": 114,
            "text": "R-1A-EA-4"
          },
          {
            "id": 115,
            "text": "Z163"
          },
          {
            "id": 116,
            "text": "Z198"
          },
          {
            "id": 117,
            "text": "R-2B/T/PH"
          },
          {
            "id": 118,
            "text": "R-2B/T/SP/"
          },
          {
            "id": 119,
            "text": "R-3A"
          },
          {
            "id": 120,
            "text": "RSTD R-2"
          },
          {
            "id": 121,
            "text": "R-1/T/AN"
          },
          {
            "id": 122,
            "text": "C-2-EA-4"
          },
          {
            "id": 123,
            "text": "RMF"
          },
          {
            "id": 124,
            "text": "RD3"
          },
          {
            "id": 125,
            "text": "R1AH"
          },
          {
            "id": 126,
            "text": "C6"
          },
          {
            "id": 127,
            "text": "R-1 MH"
          },
          {
            "id": 128,
            "text": "Z165"
          },
          {
            "id": 129,
            "text": "I1"
          },
          {
            "id": 130,
            "text": "R-3A/AN"
          },
          {
            "id": 131,
            "text": "R-3A/W/RP"
          },
          {
            "id": 132,
            "text": "Z324"
          },
          {
            "id": 133,
            "text": "RTF"
          },
          {
            "id": 134,
            "text": "R-1-PUD"
          },
          {
            "id": 135,
            "text": "RD20"
          },
          {
            "id": 136,
            "text": "RD-5 (NPA)"
          },
          {
            "id": 137,
            "text": "R=-1"
          },
          {
            "id": 138,
            "text": "SPLIT"
          },
          {
            "id": 139,
            "text": "R-1 R-3"
          },
          {
            "id": 140,
            "text": "HU-RS"
          },
          {
            "id": 141,
            "text": "Z134"
          },
          {
            "id": 142,
            "text": "R3A"
          },
          {
            "id": 143,
            "text": "Z415"
          },
          {
            "id": 144,
            "text": "RHD-A"
          },
          {
            "id": 145,
            "text": "BP"
          },
          {
            "id": 146,
            "text": "RD1"
          },
          {
            "id": 147,
            "text": "M-1-R"
          },
          {
            "id": 148,
            "text": "HMR-2"
          },
          {
            "id": 149,
            "text": "M-1S-R"
          },
          {
            "id": 150,
            "text": "R-1A-PUD"
          },
          {
            "id": 151,
            "text": "CR5"
          },
          {
            "id": 152,
            "text": "HU-MU"
          },
          {
            "id": 153,
            "text": "CCG-1"
          },
          {
            "id": 154,
            "text": "Z115"
          },
          {
            "id": 155,
            "text": "RD-3"
          },
          {
            "id": 156,
            "text": "R-S-1A"
          },
          {
            "id": 157,
            "text": "Z31"
          },
          {
            "id": 158,
            "text": "R-1-SPD"
          },
          {
            "id": 159,
            "text": "RMX-SPD"
          },
          {
            "id": 160,
            "text": "S-RM2"
          },
          {
            "id": 161,
            "text": "RD"
          },
          {
            "id": 162,
            "text": "AR-2"
          },
          {
            "id": 163,
            "text": "RD7"
          },
          {
            "id": 164,
            "text": "RP"
          },
          {
            "id": 165,
            "text": "B1"
          },
          {
            "id": 166,
            "text": "PD"
          },
          {
            "id": 167,
            "text": "MU-1"
          },
          {
            "id": 168,
            "text": "PD/AN"
          },
          {
            "id": 169,
            "text": "AL20"
          },
          {
            "id": 170,
            "text": "R-3B/T/PH"
          },
          {
            "id": 171,
            "text": "S-RM1"
          },
          {
            "id": 172,
            "text": "Z314"
          },
          {
            "id": 173,
            "text": "Z325"
          },
          {
            "id": 174,
            "text": "Z202"
          },
          {
            "id": 175,
            "text": "CO"
          },
          {
            "id": 176,
            "text": "M-1-SPD"
          },
          {
            "id": 177,
            "text": "R1-MH"
          },
          {
            "id": 178,
            "text": "R2MH"
          },
          {
            "id": 179,
            "text": "CM"
          },
          {
            "id": 180,
            "text": "Z315"
          },
          {
            "id": 181,
            "text": "R-1-EA-3 R"
          },
          {
            "id": 182,
            "text": "Z160"
          },
          {
            "id": 183,
            "text": "R-2A/T"
          },
          {
            "id": 184,
            "text": "RD 7"
          },
          {
            "id": 185,
            "text": "RD-2"
          },
          {
            "id": 186,
            "text": "C-2-SPD"
          },
          {
            "id": 187,
            "text": "R-1-R"
          },
          {
            "id": 188,
            "text": "CS"
          },
          {
            "id": 189,
            "text": "Z390"
          },
          {
            "id": 190,
            "text": "CN"
          },
          {
            "id": 191,
            "text": "SPA (WRSPA"
          },
          {
            "id": 192,
            "text": "RD-10 (NPA"
          },
          {
            "id": 193,
            "text": "R6"
          },
          {
            "id": 194,
            "text": "MU-D"
          },
          {
            "id": 195,
            "text": "LC"
          },
          {
            "id": 196,
            "text": "R-S-2.5A"
          },
          {
            "id": 197,
            "text": "E (1/2) R-"
          },
          {
            "id": 198,
            "text": "RO"
          },
          {
            "id": 199,
            "text": "R-3-EA-4"
          },
          {
            "id": 200,
            "text": "RLD-120"
          },
          {
            "id": 201,
            "text": "RMD-C"
          },
          {
            "id": 202,
            "text": "TH3A"
          },
          {
            "id": 203,
            "text": "Z06"
          },
          {
            "id": 204,
            "text": "Z412"
          },
          {
            "id": 205,
            "text": "Z116"
          },
          {
            "id": 206,
            "text": "Z294"
          },
          {
            "id": 207,
            "text": "R5A"
          },
          {
            "id": 208,
            "text": "I2"
          },
          {
            "id": 209,
            "text": "Z248"
          },
          {
            "id": 210,
            "text": "Z149"
          },
          {
            "id": 211,
            "text": "Z411"
          },
          {
            "id": 212,
            "text": "Z372"
          },
          {
            "id": 213,
            "text": "Z424"
          },
          {
            "id": 214,
            "text": "Z409"
          },
          {
            "id": 215,
            "text": "SF"
          },
          {
            "id": 216,
            "text": "Z128"
          },
          {
            "id": 217,
            "text": "LI"
          },
          {
            "id": 218,
            "text": "Z268"
          },
          {
            "id": 219,
            "text": "Z287"
          },
          {
            "id": 220,
            "text": "Z237"
          },
          {
            "id": 221,
            "text": "Z374"
          },
          {
            "id": 222,
            "text": "NZ"
          },
          {
            "id": 223,
            "text": "Z200"
          },
          {
            "id": 224,
            "text": "0"
          },
          {
            "id": 225,
            "text": "Z386"
          },
          {
            "id": 226,
            "text": "Z236"
          },
          {
            "id": 227,
            "text": "Z97"
          },
          {
            "id": 228,
            "text": "Z20"
          },
          {
            "id": 229,
            "text": "HU-RD1"
          },
          {
            "id": 230,
            "text": "Z24"
          },
          {
            "id": 231,
            "text": "Z313"
          },
          {
            "id": 232,
            "text": "S-RS"
          },
          {
            "id": 233,
            "text": "S-RD"
          },
          {
            "id": 234,
            "text": "HU-B1"
          },
          {
            "id": 235,
            "text": "BIP"
          },
          {
            "id": 236,
            "text": "S-B1"
          },
          {
            "id": 237,
            "text": "HMR-3"
          },
          {
            "id": 238,
            "text": "HMC-2"
          },
          {
            "id": 239,
            "text": "MU-2"
          },
          {
            "id": 240,
            "text": "A-2"
          },
          {
            "id": 241,
            "text": "R-1AA"
          },
          {
            "id": 242,
            "text": "R-1/W"
          },
          {
            "id": 243,
            "text": "R-1/W/RP"
          },
          {
            "id": 244,
            "text": "A-1"
          },
          {
            "id": 245,
            "text": "R-1AA/T"
          },
          {
            "id": 246,
            "text": "P-O"
          },
          {
            "id": 247,
            "text": "RNC-2"
          },
          {
            "id": 248,
            "text": "R-5"
          },
          {
            "id": 249,
            "text": "PD/RP"
          },
          {
            "id": 250,
            "text": "PRD"
          },
          {
            "id": 251,
            "text": "R1A"
          },
          {
            "id": 252,
            "text": "NR"
          },
          {
            "id": 253,
            "text": "I-G/T"
          },
          {
            "id": 254,
            "text": "R-2A/SP"
          },
          {
            "id": 255,
            "text": "I-2"
          },
          {
            "id": 256,
            "text": "R-1/T/PH"
          },
          {
            "id": 257,
            "text": "R-CE"
          },
          {
            "id": 258,
            "text": "R-1A/SP"
          },
          {
            "id": 259,
            "text": "O-1/SP"
          },
          {
            "id": 260,
            "text": "R-T-1"
          }
        ]
      },
      "flood-zone": {
        "label": "Flood Zone",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "X"
          },
          {
            "id": 2,
            "text": "A"
          },
          {
            "id": 3,
            "text": "AH"
          },
          {
            "id": 4,
            "text": "AE"
          },
          {
            "id": 5,
            "text": "AO"
          },
          {
            "id": 6,
            "text": "D"
          },
          {
            "id": 7,
            "text": "VE"
          }
        ]
      },
      "subdivision-name": {
        "label": "Subdivision Name",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "HAZELWOOD"
          },
          {
            "id": 2,
            "text": "BELMONT CENTER"
          },
          {
            "id": 3,
            "text": "ENGLEWOOD HEIGHTS ADDITION"
          },
          {
            "id": 4,
            "text": "NULL"
          },
          {
            "id": 5,
            "text": "SUNSET GARDENS #2"
          },
          {
            "id": 6,
            "text": "OAKWOOD"
          },
          {
            "id": 7,
            "text": "WEST LAND PARK"
          },
          {
            "id": 8,
            "text": "LUCERNE PARK"
          },
          {
            "id": 9,
            "text": "WORTH HEIGHTS ADDITION"
          },
          {
            "id": 10,
            "text": "MEYER ACRES ANNEX"
          },
          {
            "id": 11,
            "text": "COUNTRY SCENE 02 EXC M/R"
          },
          {
            "id": 12,
            "text": "PARKWAY ESTATES 15"
          },
          {
            "id": 13,
            "text": "SIERRA VISTA ADD 4"
          },
          {
            "id": 14,
            "text": "TRACT NO. 1160"
          },
          {
            "id": 15,
            "text": "ROGER GIVENS SOUTHWEST ADD"
          },
          {
            "id": 16,
            "text": "SHELTON SUBDIVISION LOT"
          },
          {
            "id": 17,
            "text": "CITY FARMS 06"
          },
          {
            "id": 18,
            "text": "HOMECREST"
          },
          {
            "id": 19,
            "text": "SUNRISE LAGUNA WEST"
          },
          {
            "id": 20,
            "text": "EAST LAKE"
          },
          {
            "id": 21,
            "text": "BELTLINE ADDN & BELTLINE SHP VLG"
          },
          {
            "id": 22,
            "text": "WESTHAVEN"
          },
          {
            "id": 23,
            "text": "SOUTH CREEK ADDITION"
          },
          {
            "id": 24,
            "text": "HIGHLAND TERRACE 01"
          },
          {
            "id": 25,
            "text": "OAK RDG ACRES"
          },
          {
            "id": 26,
            "text": "CITY FARMS 03"
          },
          {
            "id": 27,
            "text": "TRACT NO 1604"
          },
          {
            "id": 28,
            "text": "KEITH ADDITION"
          },
          {
            "id": 29,
            "text": "GERALD TRACT"
          },
          {
            "id": 30,
            "text": "CARVER MANOR #2"
          },
          {
            "id": 31,
            "text": "SPEEDWAY NO 1"
          },
          {
            "id": 32,
            "text": "BRENTWOOD"
          },
          {
            "id": 33,
            "text": "CAPITOL HILL ADD"
          },
          {
            "id": 34,
            "text": "COUNTRYSIDE ADDITION-FT WORTH"
          },
          {
            "id": 35,
            "text": "NORTH FORT WORTH"
          },
          {
            "id": 36,
            "text": "HILLSDALE 01"
          },
          {
            "id": 37,
            "text": "N SACTO SUB 3"
          },
          {
            "id": 38,
            "text": "VINTAGE PARK 04"
          },
          {
            "id": 39,
            "text": "GOLF COURSE VILLAGE 03"
          },
          {
            "id": 40,
            "text": "CORDOVA TOWNSITE"
          },
          {
            "id": 41,
            "text": "FISHERS VILLA ADD"
          },
          {
            "id": 42,
            "text": "WILLOWS SEC 5"
          },
          {
            "id": 43,
            "text": "FAIRVIEW PARK"
          },
          {
            "id": 44,
            "text": "WILKES ESTATES ADDITION"
          },
          {
            "id": 45,
            "text": "GRAND BOULEVARD"
          },
          {
            "id": 46,
            "text": "ALTAVUE ADDITION"
          },
          {
            "id": 47,
            "text": "NORTH SACRAMENTO SUB 8"
          },
          {
            "id": 48,
            "text": "LARCHMONT VILLAGE 20 EXC M/R"
          },
          {
            "id": 49,
            "text": "EAST DEL PASO HEIGHTS"
          },
          {
            "id": 50,
            "text": "LAGUNA CREEK WEST 06"
          },
          {
            "id": 51,
            "text": "COUNTRY PARK SOUTH 01"
          },
          {
            "id": 52,
            "text": "PARCEL MAP"
          },
          {
            "id": 53,
            "text": "CAMELIA ACRES"
          },
          {
            "id": 54,
            "text": "SLAWSONS 01"
          },
          {
            "id": 55,
            "text": "GOLF COURSE VILLAGE 07"
          },
          {
            "id": 56,
            "text": "MAYFLOWER ADD TO THE CITY OF BAKERSFIELD"
          },
          {
            "id": 57,
            "text": "SUNSET PARK"
          },
          {
            "id": 58,
            "text": "BELMONT GARDENS 2 EXT E61 FT"
          },
          {
            "id": 59,
            "text": "LOWELL ADDITION"
          },
          {
            "id": 60,
            "text": "LARCHMONT VALLEY HI 07"
          },
          {
            "id": 61,
            "text": "DESCANO PARK"
          },
          {
            "id": 62,
            "text": "HIGHLAND PARK"
          },
          {
            "id": 63,
            "text": "INGLESIDE PARK"
          },
          {
            "id": 64,
            "text": "ALTOS ACRES"
          },
          {
            "id": 65,
            "text": "LAKE MANN SHORES"
          },
          {
            "id": 66,
            "text": "OAKLAND"
          },
          {
            "id": 67,
            "text": "SPRINGFIELD, N.W. PORTION"
          },
          {
            "id": 68,
            "text": "GLENWOOD PARK 04"
          },
          {
            "id": 69,
            "text": "BRINKMEYER SUBDIVISION"
          },
          {
            "id": 70,
            "text": "E DEL PASO HEIGHTS ADD 01"
          },
          {
            "id": 71,
            "text": "DEL PASO HTS ADD"
          },
          {
            "id": 72,
            "text": "SWANSTON ESTATES 02"
          },
          {
            "id": 73,
            "text": "NORTH SACTO SUB 9"
          },
          {
            "id": 74,
            "text": "HACIENDAS TRACT 01"
          },
          {
            "id": 75,
            "text": "MURPHYS ORCHARD"
          },
          {
            "id": 76,
            "text": "PARKER HOMES TERRACE"
          },
          {
            "id": 77,
            "text": "PETERSON TRACT 01"
          },
          {
            "id": 78,
            "text": "KERN BOULEVARD HEIGHTS"
          },
          {
            "id": 79,
            "text": "RIVER VIEW"
          },
          {
            "id": 80,
            "text": "MOUNT DIABLO MERIDI"
          },
          {
            "id": 81,
            "text": "SOUTHERN ADDITION"
          },
          {
            "id": 82,
            "text": "BETTER HOMES 04 1220"
          },
          {
            "id": 83,
            "text": "PINKHAM"
          },
          {
            "id": 84,
            "text": "BAKERSFIELD"
          },
          {
            "id": 85,
            "text": "SIERRA VISTA ADD"
          },
          {
            "id": 86,
            "text": "STRAWBERRY MANOR 02"
          },
          {
            "id": 87,
            "text": "NORTH SACRAMENTO 08"
          },
          {
            "id": 88,
            "text": "MILLERS BOULEVARD"
          },
          {
            "id": 89,
            "text": "WILLIAMS R/P PT LOT5 BK E"
          },
          {
            "id": 90,
            "text": "HALLMARK HOMES #15"
          },
          {
            "id": 91,
            "text": "SUNSET VILLA"
          },
          {
            "id": 92,
            "text": "FORTY OAKS ADDITION"
          },
          {
            "id": 93,
            "text": "COLLEGE MANORS"
          },
          {
            "id": 94,
            "text": "PARKDALE HEIGHTS"
          },
          {
            "id": 95,
            "text": "MARKLAND HEIGHTS ADD"
          },
          {
            "id": 96,
            "text": "DOLLINS L J SUNSET PARK"
          },
          {
            "id": 97,
            "text": "LAKE SIDE PARK"
          },
          {
            "id": 98,
            "text": "SECTION LAND"
          },
          {
            "id": 99,
            "text": "PARKMORE"
          },
          {
            "id": 100,
            "text": "CRESTWOOD ADDITION"
          },
          {
            "id": 101,
            "text": "KING GROVE SUB"
          },
          {
            "id": 102,
            "text": "LAGUNA CREEK RANCH EAST 05"
          },
          {
            "id": 103,
            "text": "FOULKS RANCH 04A"
          },
          {
            "id": 104,
            "text": "GRAND OAKS 04"
          },
          {
            "id": 105,
            "text": "LAGUNA PARK 06"
          },
          {
            "id": 106,
            "text": "LAGUNA PARK VILLAGE 02A"
          },
          {
            "id": 107,
            "text": "LAGUNA CREEK VILLAGE 05"
          },
          {
            "id": 108,
            "text": "LAGUNA WEST 20"
          },
          {
            "id": 109,
            "text": "LAGUNA VISTA 15"
          },
          {
            "id": 110,
            "text": "SUNRISE RANCH"
          },
          {
            "id": 111,
            "text": "LAGUNA CROSSING"
          },
          {
            "id": 112,
            "text": "VICTORIA"
          },
          {
            "id": 113,
            "text": "FLORIN VISTA 01 EXC M/R"
          },
          {
            "id": 114,
            "text": "VILLAGE PARK 05"
          },
          {
            "id": 115,
            "text": "GOLF COURSE TERRACE 04"
          },
          {
            "id": 116,
            "text": "TRACT 3366"
          },
          {
            "id": 117,
            "text": "MEADOWVIEW GARDENS"
          },
          {
            "id": 118,
            "text": "CITRUS TERRACE VILLA TRACT"
          },
          {
            "id": 119,
            "text": "MAYFLOWER ADDITION"
          },
          {
            "id": 120,
            "text": "ROEDING NURSERY ACRS"
          },
          {
            "id": 121,
            "text": "OAK PARK AVE"
          },
          {
            "id": 122,
            "text": "WILLOWS"
          },
          {
            "id": 123,
            "text": "SPHINX AT REESE COURT"
          },
          {
            "id": 124,
            "text": "BUCKNER TERRACE APTS"
          },
          {
            "id": 125,
            "text": "OAK CLIFF ORIGINAL"
          },
          {
            "id": 126,
            "text": "MONCRIEF PARK"
          },
          {
            "id": 127,
            "text": "MANN SUB"
          },
          {
            "id": 128,
            "text": "HIAWASSEE LANDINGS UT 1"
          },
          {
            "id": 129,
            "text": "SRINGFELD S/D BLK 3,5,9 ,"
          },
          {
            "id": 130,
            "text": "MCKENZIES D.P. S/D"
          },
          {
            "id": 131,
            "text": "HACIENDAS TR"
          },
          {
            "id": 132,
            "text": "LARCHMONT VILLAGE 27 EXC M/R"
          },
          {
            "id": 133,
            "text": "VINTAGE PARK 03"
          },
          {
            "id": 134,
            "text": "TRENHOLM VILLAGE 02"
          },
          {
            "id": 135,
            "text": "RED FOX VILLAGE 01"
          },
          {
            "id": 136,
            "text": "COLLEGE VIEW ESTATES 03"
          },
          {
            "id": 137,
            "text": "SOUTHWOODS 04"
          },
          {
            "id": 138,
            "text": "COUNTRY PARK SOUTH 02"
          },
          {
            "id": 139,
            "text": "W & K WILLOW RANCHO 04"
          },
          {
            "id": 140,
            "text": "DAYSTAR 02"
          },
          {
            "id": 141,
            "text": "TALLAC VILLAGE 05"
          },
          {
            "id": 142,
            "text": "VIRGINIA COLONY"
          },
          {
            "id": 143,
            "text": "FIFTH AVENUE TRACT 02"
          },
          {
            "id": 144,
            "text": "AIRPORT ACRES"
          },
          {
            "id": 145,
            "text": "NORTH PARK"
          },
          {
            "id": 146,
            "text": "GOLDEN STATE TRACT TRACT #1139"
          },
          {
            "id": 147,
            "text": "TRACT NO. 3129"
          },
          {
            "id": 148,
            "text": "UNINCORPORATED"
          },
          {
            "id": 149,
            "text": "TRACT #1153 EL CAMINO PARK"
          },
          {
            "id": 150,
            "text": "BETTER HOMES #13"
          },
          {
            "id": 151,
            "text": "CENTRAL CALIFORNIA COLONY"
          },
          {
            "id": 152,
            "text": "MAYFLOWER ADD"
          },
          {
            "id": 153,
            "text": "SOMERSET HEIGHTS"
          },
          {
            "id": 154,
            "text": "CORONADO HEIGHTS"
          },
          {
            "id": 155,
            "text": "ACREAGE & UNREC"
          },
          {
            "id": 156,
            "text": "EDWARD COYLE"
          },
          {
            "id": 157,
            "text": "YOUNGS ENGLEWOOD ADD"
          },
          {
            "id": 158,
            "text": "COLLEGE HILL ADD"
          },
          {
            "id": 159,
            "text": "LINDSEY J. H. S/D"
          },
          {
            "id": 160,
            "text": "PARK LAWN"
          },
          {
            "id": 161,
            "text": "ANGEBILT ADD 2"
          },
          {
            "id": 162,
            "text": "ADAMS S/D"
          },
          {
            "id": 163,
            "text": "AIRPORT SUBDIVISION"
          },
          {
            "id": 164,
            "text": "SOUTH WOODS 02 EXC M/R"
          },
          {
            "id": 165,
            "text": "VALLEY HIGH VILLAGE"
          },
          {
            "id": 166,
            "text": "FRUITRIDGE MANOR 10"
          },
          {
            "id": 167,
            "text": "SANDRA HEIGHTS"
          },
          {
            "id": 168,
            "text": "PARKWAY NORTH"
          },
          {
            "id": 169,
            "text": "COLONIAL HEIGHTS"
          },
          {
            "id": 170,
            "text": "CLOVERDALE VILLAGE"
          },
          {
            "id": 171,
            "text": "STEBBINS PLAT/TOULA"
          },
          {
            "id": 172,
            "text": "CITY FARMS 02"
          },
          {
            "id": 173,
            "text": "MONTE VISTA TERRACE"
          },
          {
            "id": 174,
            "text": "MANCHESTER PARK #1251"
          },
          {
            "id": 175,
            "text": "STATE COLLEGE TRACT #1"
          },
          {
            "id": 176,
            "text": "ARLINGTON HEIGHTS"
          },
          {
            "id": 177,
            "text": "SARA PERRY SURVEY ABSTRACT #1164"
          },
          {
            "id": 178,
            "text": "POLYTECHNIC HEIGHTS ADDITION"
          },
          {
            "id": 179,
            "text": "NORTH SACRAMENTO 10"
          },
          {
            "id": 180,
            "text": "ARLINGTON HEIGHTS TRACT"
          },
          {
            "id": 181,
            "text": "BOWERS ADDITION"
          },
          {
            "id": 182,
            "text": "MURRAY HILLS HEIGHTS"
          },
          {
            "id": 183,
            "text": "INDIAN LANDING"
          },
          {
            "id": 184,
            "text": "E W DALLAS"
          },
          {
            "id": 185,
            "text": "OAKDALE VILLAGE"
          },
          {
            "id": 186,
            "text": "CINDY WOODS"
          },
          {
            "id": 187,
            "text": "N SAC SUB 8"
          },
          {
            "id": 188,
            "text": "RICHARDSON VILLAGE 01"
          },
          {
            "id": 189,
            "text": "LARCHMONT VILLAGE 07 EXC M/R"
          },
          {
            "id": 190,
            "text": "SUNRISE OAKS 02"
          },
          {
            "id": 191,
            "text": "CHEVIOT HILLS"
          },
          {
            "id": 192,
            "text": "OAKRIDGE ACRES"
          },
          {
            "id": 193,
            "text": "VALLEY HI 05"
          },
          {
            "id": 194,
            "text": "SUNRISE WILLOWOOD 03 REVISED"
          },
          {
            "id": 195,
            "text": "LARCHMONT VALLEY HI 13A"
          },
          {
            "id": 196,
            "text": "VILLA ROYALE #3"
          },
          {
            "id": 197,
            "text": "ARROYO VISTA ESTATES"
          },
          {
            "id": 198,
            "text": "LARCHMONT VALLEY HI 14"
          },
          {
            "id": 199,
            "text": "COUNTRY PARK SOUTH 03"
          },
          {
            "id": 200,
            "text": "STONEWOOD 02"
          },
          {
            "id": 201,
            "text": "CITRUS HEIGHTS ADD 05"
          },
          {
            "id": 202,
            "text": "FRUITRIDGE VISTA 16"
          },
          {
            "id": 203,
            "text": "CITY FARMS 04"
          },
          {
            "id": 204,
            "text": "SOUTH SACRAMENTO GARDENS"
          },
          {
            "id": 205,
            "text": "FRUITRIDGE VISTA 03"
          },
          {
            "id": 206,
            "text": "SCOTTSDALE GREENS 01"
          },
          {
            "id": 207,
            "text": "COUNTRY PLACE REVISED"
          },
          {
            "id": 208,
            "text": "CITY OF BAKERSFIELD"
          },
          {
            "id": 209,
            "text": "KERN COUNTY SALES MAP 01"
          },
          {
            "id": 210,
            "text": "DESCANSO PARK"
          },
          {
            "id": 211,
            "text": "REDDING AVENUE SUBDIVISION"
          },
          {
            "id": 212,
            "text": "SACRAMENTO HEIGHTS"
          },
          {
            "id": 213,
            "text": "SCOTTSDALE EAST 02 EXC M/R"
          },
          {
            "id": 214,
            "text": "FRUITRIDGE OAKS 08"
          },
          {
            "id": 215,
            "text": "3867 UN B"
          },
          {
            "id": 216,
            "text": "FREEPORT VILLAGE 01"
          },
          {
            "id": 217,
            "text": "SOUTHFIELD 01"
          },
          {
            "id": 218,
            "text": "GARDEN ACRES"
          },
          {
            "id": 219,
            "text": "TRACT NO 1655"
          },
          {
            "id": 220,
            "text": "JAMES ARP"
          },
          {
            "id": 221,
            "text": "CLOVERDALE"
          },
          {
            "id": 222,
            "text": "CASA LOMA ACRES"
          },
          {
            "id": 223,
            "text": "UNION AVENUE TRACT"
          },
          {
            "id": 224,
            "text": "EL CAMINO PARK"
          },
          {
            "id": 225,
            "text": "EDISON MANOR"
          },
          {
            "id": 226,
            "text": "MAPLE PLACE"
          },
          {
            "id": 227,
            "text": "KEARNEY BLVD HEIGHTS"
          },
          {
            "id": 228,
            "text": "CHAPARRAL COUNTRY AMD"
          },
          {
            "id": 229,
            "text": "MEADOWS AT INDEPENDENCE LOT 1-297"
          },
          {
            "id": 230,
            "text": "MEADOWS 2"
          },
          {
            "id": 231,
            "text": "VILLA DE PAZ 1"
          },
          {
            "id": 232,
            "text": "EMERALD POINT AMD LOT 1-291 TR A-M P"
          },
          {
            "id": 233,
            "text": "VILLA DE PAZ UNIT 2"
          },
          {
            "id": 234,
            "text": "SUNRISE TERRACE UNIT 5"
          },
          {
            "id": 235,
            "text": "SUNRISE VILLAGE"
          },
          {
            "id": 236,
            "text": "MARYVALE TERRACE NO. 49"
          },
          {
            "id": 237,
            "text": "COLLEGE PARK 21"
          },
          {
            "id": 238,
            "text": "ARIZONA HOMES"
          },
          {
            "id": 239,
            "text": "PONDEROSA HOMES WEST UNIT ONE"
          },
          {
            "id": 240,
            "text": "WILLOWS WEST"
          },
          {
            "id": 241,
            "text": "ARIZONA HOMES NO. 2"
          },
          {
            "id": 242,
            "text": "LEVITT HOMES WEST UNIT 1"
          },
          {
            "id": 243,
            "text": "VILLA OASIS 2 AMD"
          },
          {
            "id": 244,
            "text": "LAURELWOOD UNIT 1"
          },
          {
            "id": 245,
            "text": "LAURELWOOD 2"
          },
          {
            "id": 246,
            "text": "BRAEWOOD PARK UNIT 4"
          },
          {
            "id": 247,
            "text": "BRAEWOOD PARK UNIT 6"
          },
          {
            "id": 248,
            "text": "CHAPARRAL VILLAGE"
          },
          {
            "id": 249,
            "text": "TERRACITA"
          },
          {
            "id": 250,
            "text": "SILVERTHORN ESTATES"
          },
          {
            "id": 251,
            "text": "WESTBRIAR"
          },
          {
            "id": 252,
            "text": "WEST PLAZA 29 & 30 LOTS 1-147"
          },
          {
            "id": 253,
            "text": "NATIONAL EMBLEM WEST UNIT 1"
          },
          {
            "id": 254,
            "text": "NATIONAL EMBLEM WEST UNIT 2"
          },
          {
            "id": 255,
            "text": "WESTRIDGE SHADOWS"
          },
          {
            "id": 256,
            "text": "WESTFIELD 1 LOT 1-136 TR A-E"
          },
          {
            "id": 257,
            "text": "SKYVIEW NORTH UNIT 4"
          },
          {
            "id": 258,
            "text": "VILLA DE PAZ UNIT 3"
          },
          {
            "id": 259,
            "text": "VILLA DE PAZ UNIT 4"
          },
          {
            "id": 260,
            "text": "YOUNG AMERICA WEST"
          },
          {
            "id": 261,
            "text": "MARYVALE TERRACE 47"
          },
          {
            "id": 262,
            "text": "VILLA DE PAZ UNIT 6 AMD"
          },
          {
            "id": 263,
            "text": "BOLERO COURT"
          },
          {
            "id": 264,
            "text": "SOLACE SUBDIVISION"
          },
          {
            "id": 265,
            "text": "VILLA DE PAZ UNIT 9 AMD"
          },
          {
            "id": 266,
            "text": "BRAEWOOD PARK UNIT 1"
          },
          {
            "id": 267,
            "text": "BRAEWOOD PARK UNIT 2"
          },
          {
            "id": 268,
            "text": "SUNRISE TERRACE"
          },
          {
            "id": 269,
            "text": "SUNRISE TERRACE UNIT 2"
          },
          {
            "id": 270,
            "text": "SUNRISE TERRACE UNIT 3"
          },
          {
            "id": 271,
            "text": "SUNRISE TERRACE UNIT 4"
          },
          {
            "id": 272,
            "text": "PONDEROSA HOMES WEST UNIT TWO"
          },
          {
            "id": 273,
            "text": "VILLA OASIS 3 AMD"
          },
          {
            "id": 274,
            "text": "CASA REAL PHOENIX 1A LOTS 1 THROUGH 29"
          },
          {
            "id": 275,
            "text": "CASA REAL PHOENIX 1B"
          },
          {
            "id": 276,
            "text": "WESTRIDGE GLEN 4 LOT 188-254"
          },
          {
            "id": 277,
            "text": "WESTRIDGE GLEN 5 LOT 255-290"
          },
          {
            "id": 278,
            "text": "CASA REAL PHOENIX 2 LOTS 187 & 188"
          },
          {
            "id": 279,
            "text": "CASA REAL PHOENIX 3"
          },
          {
            "id": 280,
            "text": "GATEWAY CROSSING 1"
          },
          {
            "id": 281,
            "text": "SHEFFIELD PLACE UNIT 1"
          },
          {
            "id": 282,
            "text": "NATIONAL EMBLEM WEST UNIT 3"
          },
          {
            "id": 283,
            "text": "GATEWAY CROSSING 2"
          },
          {
            "id": 284,
            "text": "VILLA OASIS 1"
          },
          {
            "id": 285,
            "text": "WESTPOINT LOT 1-107 TR A"
          },
          {
            "id": 286,
            "text": "MARYVALE TERRACE 29 LOTS 212-352 & TR A"
          },
          {
            "id": 287,
            "text": "LAURELWOOD UNIT 3"
          },
          {
            "id": 288,
            "text": "PALM RIDGE UNIT ONE"
          },
          {
            "id": 289,
            "text": "LAURELWOOD UNIT 4"
          },
          {
            "id": 290,
            "text": "SUNRISE TERRACE 6"
          },
          {
            "id": 291,
            "text": "SKYVIEW NORTH UNIT FIVE"
          },
          {
            "id": 292,
            "text": "MARYVALE TERRACE NO. 58"
          },
          {
            "id": 293,
            "text": "CHAPARRAL VILLAGE 2 LOT 97-196"
          },
          {
            "id": 294,
            "text": "RYANS RIDGE LT 1-162 TR A-C"
          },
          {
            "id": 295,
            "text": "SUNRISE TERRACE UNIT 8"
          },
          {
            "id": 296,
            "text": "VISTA DE OESTE 2 PHASE 2"
          },
          {
            "id": 297,
            "text": "MARLBOROUGH COUNTRY UNIT 10"
          },
          {
            "id": 298,
            "text": "MARLBOROUGH COUNTRY UNIT 11"
          },
          {
            "id": 299,
            "text": "MARYVALE TERRACE 28 LOTS 10999-11084"
          },
          {
            "id": 300,
            "text": "SUNRISE TERRACE UNIT 9"
          },
          {
            "id": 301,
            "text": "MARYVALE TERRACE 28A LOTS 11505-11600"
          }
        ]
      },
      "school-district": {
        "label": "School District",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Fresno Unified School District"
          },
          {
            "id": 2,
            "text": "Kern High School District"
          },
          {
            "id": 3,
            "text": "Fort Worth Independent School District"
          },
          {
            "id": 4,
            "text": "Tucson Unified District"
          },
          {
            "id": 5,
            "text": "Irving Independent School District"
          },
          {
            "id": 6,
            "text": "Sacramento City Unified School District"
          },
          {
            "id": 7,
            "text": "Orange County School District"
          },
          {
            "id": 8,
            "text": "Elk Grove Unified School District"
          },
          {
            "id": 9,
            "text": "Oklahoma City Public Schools"
          },
          {
            "id": 10,
            "text": "Birmingham City School District"
          },
          {
            "id": 11,
            "text": "Grand Prairie Independent School District"
          },
          {
            "id": 12,
            "text": "Twin Rivers Unified School District"
          },
          {
            "id": 13,
            "text": "Washington Unified School District"
          },
          {
            "id": 14,
            "text": "Duval County School District"
          },
          {
            "id": 15,
            "text": "Crowley Independent School District"
          },
          {
            "id": 16,
            "text": "San Juan Unified School District"
          },
          {
            "id": 17,
            "text": "Central Unified School District"
          },
          {
            "id": 18,
            "text": "Twin Rivers Unified School District (7-12)"
          },
          {
            "id": 19,
            "text": "Garland Independent School District"
          },
          {
            "id": 20,
            "text": "Dallas Independent School District"
          },
          {
            "id": 21,
            "text": "Flowing Wells Unified District"
          },
          {
            "id": 22,
            "text": "Washington Unified School District (9-12)"
          },
          {
            "id": 23,
            "text": "Amphitheater Unified District"
          },
          {
            "id": 24,
            "text": "Clovis Unified School District"
          },
          {
            "id": 25,
            "text": "Sunnyside Unified District"
          },
          {
            "id": 26,
            "text": "Folsom-Cordova Unified School District"
          },
          {
            "id": 27,
            "text": "Selma Unified School District"
          },
          {
            "id": 28,
            "text": "Seminole County School District"
          },
          {
            "id": 29,
            "text": "Orleans Parish School District"
          },
          {
            "id": 30,
            "text": "Jefferson County School District"
          },
          {
            "id": 31,
            "text": "Mesquite Independent School District"
          },
          {
            "id": 32,
            "text": "Tarrant City School District"
          },
          {
            "id": 33,
            "text": "Lake Worth Independent School District"
          },
          {
            "id": 34,
            "text": "Vail Unified District"
          },
          {
            "id": 35,
            "text": "Putnam City Public Schools"
          },
          {
            "id": 36,
            "text": "Sierra Sands Unified School District"
          },
          {
            "id": 37,
            "text": "Fowler Unified School District"
          },
          {
            "id": 38,
            "text": "Arlington Independent School District"
          },
          {
            "id": 39,
            "text": "Richardson Independent School District"
          },
          {
            "id": 40,
            "text": "Marana Unified District"
          },
          {
            "id": 41,
            "text": "Ajo Unified District"
          },
          {
            "id": 42,
            "text": "Castleberry Independent School District"
          },
          {
            "id": 43,
            "text": "Beardsley Elementary School District"
          },
          {
            "id": 44,
            "text": "Standard Elementary School District"
          },
          {
            "id": 45,
            "text": "Fairfield City School District"
          },
          {
            "id": 46,
            "text": "Glendale Union High School District"
          },
          {
            "id": 47,
            "text": "Tolleson Union High School District"
          },
          {
            "id": 48,
            "text": "Phoenix Union High School District"
          },
          {
            "id": 49,
            "text": "Mesa Unified District"
          },
          {
            "id": 50,
            "text": "Peoria Unified School District"
          },
          {
            "id": 51,
            "text": "Tempe Union High School District"
          },
          {
            "id": 52,
            "text": "Gilbert Unified District"
          },
          {
            "id": 53,
            "text": "Paradise Valley Unified District"
          },
          {
            "id": 54,
            "text": "Saddle Mountain Unified School District"
          },
          {
            "id": 55,
            "text": "Alvord Unified School District"
          },
          {
            "id": 56,
            "text": "Riverside Unified School District"
          },
          {
            "id": 57,
            "text": "Moreno Valley Unified School District"
          },
          {
            "id": 58,
            "text": "Jurupa Unified School District"
          },
          {
            "id": 59,
            "text": "Perris Union High School District"
          },
          {
            "id": 60,
            "text": "Val Verde Unified School District"
          },
          {
            "id": 61,
            "text": "Corona-Norco Unified School District"
          },
          {
            "id": 62,
            "text": "San Jacinto Unified School District"
          },
          {
            "id": 63,
            "text": "Hemet Unified School District"
          },
          {
            "id": 64,
            "text": "Colton Joint Unified School District"
          },
          {
            "id": 65,
            "text": "Lake Elsinore Unified School District"
          },
          {
            "id": 66,
            "text": "Desert Sands Unified School District"
          },
          {
            "id": 67,
            "text": "Coachella Valley Unified School District"
          },
          {
            "id": 68,
            "text": "Rialto Unified School District"
          },
          {
            "id": 69,
            "text": "San Bernardino City Unified School District"
          },
          {
            "id": 70,
            "text": "Redlands Unified School District"
          },
          {
            "id": 71,
            "text": "Fontana Unified School District"
          },
          {
            "id": 72,
            "text": "Hesperia Unified School District"
          },
          {
            "id": 73,
            "text": "Victor Valley Union High School District"
          },
          {
            "id": 74,
            "text": "Lodi Unified School District"
          },
          {
            "id": 75,
            "text": "Lincoln Unified School District"
          },
          {
            "id": 76,
            "text": "Stockton Unified School District"
          },
          {
            "id": 77,
            "text": "Manteca Unified School District"
          },
          {
            "id": 78,
            "text": "Tracy Unified School District"
          },
          {
            "id": 79,
            "text": "Tracy Unified School District (9-12)"
          },
          {
            "id": 80,
            "text": "Modesto City High School District"
          },
          {
            "id": 81,
            "text": "Ceres Unified School District"
          },
          {
            "id": 82,
            "text": "East Hartford School District"
          },
          {
            "id": 83,
            "text": "Bristol School District"
          },
          {
            "id": 84,
            "text": "Glastonbury School District"
          },
          {
            "id": 85,
            "text": "Hartford School District"
          },
          {
            "id": 86,
            "text": "Manchester School District"
          },
          {
            "id": 87,
            "text": "New Britain School District"
          },
          {
            "id": 88,
            "text": "West Hartford School District"
          },
          {
            "id": 89,
            "text": "Caldwell School District 132"
          },
          {
            "id": 90,
            "text": "Nampa School District 131"
          },
          {
            "id": 91,
            "text": "Vallivue School District 139"
          },
          {
            "id": 92,
            "text": "Kuna Joint School District 3"
          },
          {
            "id": 93,
            "text": "Notus School District 135"
          },
          {
            "id": 94,
            "text": "Middleton School District 134"
          },
          {
            "id": 95,
            "text": "Meridian Joint School District 2"
          },
          {
            "id": 96,
            "text": "Bladen County Schools"
          },
          {
            "id": 97,
            "text": "Cumberland County Schools"
          },
          {
            "id": 98,
            "text": "Durham Public Schools"
          },
          {
            "id": 99,
            "text": "Edgecombe County Schools"
          },
          {
            "id": 100,
            "text": "Nash-Rocky Mount Schools"
          },
          {
            "id": 101,
            "text": "Wilson County Schools"
          },
          {
            "id": 102,
            "text": "Tulsa Public Schools"
          },
          {
            "id": 103,
            "text": "Sperry Public Schools"
          },
          {
            "id": 104,
            "text": "Shidler Public Schools"
          },
          {
            "id": 105,
            "text": "Cleveland Public Schools"
          },
          {
            "id": 106,
            "text": "Bowring Public School"
          },
          {
            "id": 107,
            "text": "Woodland Public Schools"
          },
          {
            "id": 108,
            "text": "Sand Springs Public Schools"
          },
          {
            "id": 109,
            "text": "Broken Arrow Public Schools"
          },
          {
            "id": 110,
            "text": "Union Public Schools"
          },
          {
            "id": 111,
            "text": "Catoosa Public Schools"
          },
          {
            "id": 112,
            "text": "Coweta Public Schools"
          },
          {
            "id": 113,
            "text": "Midland Borough School District"
          },
          {
            "id": 114,
            "text": "Central Falls School District"
          },
          {
            "id": 115,
            "text": "Cranston School District"
          },
          {
            "id": 116,
            "text": "Lincoln School District"
          },
          {
            "id": 117,
            "text": "North Providence School District"
          },
          {
            "id": 118,
            "text": "Pawtucket School District"
          },
          {
            "id": 119,
            "text": "Providence School District"
          },
          {
            "id": 120,
            "text": "Elgin Independent School District"
          },
          {
            "id": 121,
            "text": "Hays Consolidated Independent School District"
          },
          {
            "id": 122,
            "text": "Austin Independent School District"
          },
          {
            "id": 123,
            "text": "Albuquerque Public Schools"
          },
          {
            "id": 124,
            "text": "Midwest City-Del City Schools"
          },
          {
            "id": 125,
            "text": "Norfolk City Public Schools"
          },
          {
            "id": 126,
            "text": "Columbus City School District"
          },
          {
            "id": 127,
            "text": "Des Moines Independent Community School District"
          },
          {
            "id": 128,
            "text": "El Paso Independent School District"
          },
          {
            "id": 129,
            "text": "Cincinnati City School District"
          },
          {
            "id": 130,
            "text": "Ysleta Independent School District"
          },
          {
            "id": 131,
            "text": "Portsmouth City Public Schools"
          },
          {
            "id": 132,
            "text": "Northside Independent School District"
          },
          {
            "id": 133,
            "text": "Hampton City Public Schools"
          },
          {
            "id": 134,
            "text": "San Antonio Independent School District"
          },
          {
            "id": 135,
            "text": "Ogden School District"
          },
          {
            "id": 136,
            "text": "Whitehall City School District"
          },
          {
            "id": 137,
            "text": "Duquesne City School District"
          },
          {
            "id": 138,
            "text": "Colorado Springs School District 11"
          },
          {
            "id": 139,
            "text": "Wichita Unified School District 259"
          },
          {
            "id": 140,
            "text": "Harlandale Independent School District"
          },
          {
            "id": 141,
            "text": "Salt Lake City School District"
          },
          {
            "id": 142,
            "text": "Pittsburgh School District"
          },
          {
            "id": 143,
            "text": "Richmond City Public Schools"
          },
          {
            "id": 144,
            "text": "Edgewood Independent School District"
          },
          {
            "id": 145,
            "text": "Moore Public Schools"
          },
          {
            "id": 146,
            "text": "Wilkinsburg Borough School District"
          },
          {
            "id": 147,
            "text": "Rochester City School District"
          },
          {
            "id": 148,
            "text": "Omaha Public Schools"
          },
          {
            "id": 149,
            "text": "Woodland Hills School District"
          },
          {
            "id": 150,
            "text": "Steel Valley School District"
          },
          {
            "id": 151,
            "text": "Clairton City School District"
          },
          {
            "id": 152,
            "text": "McKeesport Area School District"
          },
          {
            "id": 153,
            "text": "Reading Community City School District"
          },
          {
            "id": 154,
            "text": "Socorro Independent School District"
          },
          {
            "id": 155,
            "text": "Clint Independent School District"
          },
          {
            "id": 156,
            "text": "Harrison School District 2"
          },
          {
            "id": 157,
            "text": "Academy School District 20"
          },
          {
            "id": 158,
            "text": "Granite School District"
          },
          {
            "id": 159,
            "text": "Weber School District"
          },
          {
            "id": 160,
            "text": "Hamilton Local School District"
          },
          {
            "id": 161,
            "text": "Edmond Public Schools"
          },
          {
            "id": 162,
            "text": "Crutcho Public School"
          },
          {
            "id": 163,
            "text": "South-Western City School District"
          },
          {
            "id": 164,
            "text": "Baldwin-Whitehall School District"
          },
          {
            "id": 165,
            "text": "West Mifflin Area School District"
          },
          {
            "id": 166,
            "text": "Newport News City Public Schools"
          },
          {
            "id": 167,
            "text": "Cheyenne Mountain School District 12"
          },
          {
            "id": 168,
            "text": "Widefield School District 3"
          },
          {
            "id": 169,
            "text": "Saydel Community School District"
          },
          {
            "id": 170,
            "text": "Johnston Community School District"
          },
          {
            "id": 171,
            "text": "North East Independent School District"
          },
          {
            "id": 172,
            "text": "Western Heights Public Schools"
          },
          {
            "id": 173,
            "text": "Groveport Madison Local School District"
          },
          {
            "id": 174,
            "text": "Hilliard City School District"
          },
          {
            "id": 175,
            "text": "Penn Hills School District"
          },
          {
            "id": 176,
            "text": "North Hills School District"
          },
          {
            "id": 177,
            "text": "Shaler Area School District"
          },
          {
            "id": 178,
            "text": "West Jefferson Hills School District"
          },
          {
            "id": 179,
            "text": "East Allegheny School District"
          },
          {
            "id": 180,
            "text": "Oak Hills Local School District"
          },
          {
            "id": 181,
            "text": "Northwest Local School District"
          },
          {
            "id": 182,
            "text": "Mariemont City School District"
          },
          {
            "id": 183,
            "text": "Westside Community Schools"
          },
          {
            "id": 184,
            "text": "Haysville Unified School District 261"
          },
          {
            "id": 185,
            "text": "Southeast Polk Community School District"
          },
          {
            "id": 186,
            "text": "Southside Independent School District"
          },
          {
            "id": 187,
            "text": "East Central Independent School District"
          },
          {
            "id": 188,
            "text": "Alamo Heights Independent School District"
          },
          {
            "id": 189,
            "text": "Murray School District"
          },
          {
            "id": 190,
            "text": "Ambridge Area School District"
          },
          {
            "id": 191,
            "text": "Henrico County Public Schools"
          },
          {
            "id": 192,
            "text": "Chesterfield County Public Schools"
          },
          {
            "id": 193,
            "text": "Suffolk City Public Schools"
          },
          {
            "id": 194,
            "text": "St. Louis City School District"
          },
          {
            "id": 195,
            "text": "Jennings School District"
          },
          {
            "id": 196,
            "text": "Riverview Gardens School District"
          },
          {
            "id": 197,
            "text": "Hazelwood School District"
          },
          {
            "id": 198,
            "text": "Normandy Schools Collaborative"
          },
          {
            "id": 199,
            "text": "Cleveland Municipal School District"
          },
          {
            "id": 200,
            "text": "Garfield Heights City School District"
          },
          {
            "id": 201,
            "text": "Cleveland Heights-University Heights City School District"
          },
          {
            "id": 202,
            "text": "East Cleveland City School District"
          },
          {
            "id": 203,
            "text": "Aurora City School District"
          },
          {
            "id": 204,
            "text": "Euclid City School District"
          },
          {
            "id": 205,
            "text": "Shaker Heights City School District"
          },
          {
            "id": 206,
            "text": "South Euclid-Lyndhurst City School District"
          }
        ]
      },
      "linked-owners": {
        "label": "Linked Owners",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644240
        ],
        "options": []
      },
      "number": {
        "label": "Number",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  }
});

export default PODIO_ATTACHED_SCHEMA_SUPPLEMENT;
