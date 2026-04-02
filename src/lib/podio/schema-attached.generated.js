export const PODIO_ATTACHED_SCHEMA = Object.freeze({
  "30541677": {
    "app_id": 30541677,
    "app_name": "TextGrid Numbers",
    "item_name": "Number",
    "fields": {
      "title": {
        "label": "Phone Number",
        "type": "text",
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
      "friendly-name": {
        "label": "Friendly Name",
        "type": "text",
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
            "text": "Los Angeles, CA"
          },
          {
            "id": 2,
            "text": "Tampa, FL"
          },
          {
            "id": 3,
            "text": "Charlotte, NC"
          },
          {
            "id": 4,
            "text": "Miami, FL"
          },
          {
            "id": 5,
            "text": "Minneapolis, MN"
          },
          {
            "id": 6,
            "text": "Jacksonville, FL"
          },
          {
            "id": 7,
            "text": "Houston, TX"
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
            "text": "_ Active"
          },
          {
            "id": 2,
            "text": "_ Warming Up"
          },
          {
            "id": 3,
            "text": "_ Paused"
          },
          {
            "id": 4,
            "text": "_ Flagged"
          },
          {
            "id": 5,
            "text": "⚫ Retired"
          }
        ]
      },
      "ai-risk-level": {
        "label": "AI Risk Level",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Low"
          },
          {
            "id": 2,
            "text": "Medium"
          },
          {
            "id": 3,
            "text": "High"
          },
          {
            "id": 4,
            "text": "Critical"
          }
        ]
      },
      "ai-recommendation": {
        "label": "AI Recommendation",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Keep Active"
          },
          {
            "id": 2,
            "text": "Rotate Soon"
          },
          {
            "id": 3,
            "text": "Pause"
          },
          {
            "id": 4,
            "text": "Replace"
          }
        ]
      },
      "rotation-weight-1-10": {
        "label": "Rotation Weight (1–10)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "allowed-send-window-start-local": {
        "label": "Allowed Send Window Start (local)",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "allowed-send-window-end-local": {
        "label": "Allowed Send Window End (local",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "quiet-hours-enabled": {
        "label": "Quiet Hours Enabled",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "hard-pause": {
        "label": "Hard Pause",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "pause-reason": {
        "label": "Pause Reason",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "pause-until": {
        "label": "Pause Until",
        "type": "date",
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
      "sent-today": {
        "label": "Sent Today",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "delivered-today": {
        "label": "Delivered Today",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "replies-today": {
        "label": "Replies Today",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "failed-today": {
        "label": "Failed Today",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "remaining-today": {
        "label": "Remaining Today",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "hourly-send-cap": {
        "label": "Hourly Send Cap",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "daily-send-cap": {
        "label": "Daily Send Cap",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "risk-spike-flag": {
        "label": "Risk Spike Flag",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "last-used": {
        "label": "Last Used",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-sends": {
        "label": "Total Sends",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-deliveries": {
        "label": "Total Deliveries",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-replies": {
        "label": "Total Replies",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-failed": {
        "label": "Total Failed",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-leads": {
        "label": "Total Leads",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-opt-outs": {
        "label": "Total Opt-Outs",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "opt-out-rate": {
        "label": "Opt-Out Rate",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "delivery-rate": {
        "label": "Delivery Rate",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-risk-score": {
        "label": "Number Risk Score",
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
      "markets": {
        "label": "Markets",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "linked-messages": {
        "label": "Linked Messages",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30541680
        ],
        "options": []
      },
      "linked-conversation": {
        "label": "Linked Conversation",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646484
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
  "30541680": {
    "app_id": 30541680,
    "app_name": "Message Events",
    "item_name": "SMS",
    "fields": {
      "message-id": {
        "label": "Message ID",
        "type": "text",
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
      "timestamp": {
        "label": "Timestamp",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "direction": {
        "label": "Direction",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Inbound"
          },
          {
            "id": 2,
            "text": "Outbound"
          }
        ]
      },
      "message-variant": {
        "label": "Message Variant",
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
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "linked-seller": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "property": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "textgrid-number": {
        "label": "TextGrid Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30541677
        ],
        "options": []
      },
      "phone-number": {
        "label": "Phone Number",
        "type": "app",
        "multiple": true,
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
          30644726
        ],
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
      "ai-route": {
        "label": "AI Route",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Ownership Check"
          },
          {
            "id": 2,
            "text": "Identity"
          },
          {
            "id": 3,
            "text": "Offer"
          },
          {
            "id": 4,
            "text": "Objection Handling"
          },
          {
            "id": 5,
            "text": "Wrong Number"
          },
          {
            "id": 6,
            "text": "Follow-Up"
          },
          {
            "id": 7,
            "text": "Re-Engagement"
          },
          {
            "id": 8,
            "text": "Dispo Buyer"
          },
          {
            "id": 9,
            "text": "Spanish Route"
          },
          {
            "id": 10,
            "text": "Legal Opt-Out"
          }
        ]
      },
      "processed-by": {
        "label": "Processed By",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Manual Sender"
          },
          {
            "id": 2,
            "text": "GPT-4o (AI)"
          },
          {
            "id": 3,
            "text": "Mistral-7B (AI)"
          },
          {
            "id": 4,
            "text": "Autoresponder"
          },
          {
            "id": 5,
            "text": "Drip Campaign"
          },
          {
            "id": 6,
            "text": "Scheduled Campaign"
          }
        ]
      },
      "source-app": {
        "label": "Source App",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Send Queue"
          },
          {
            "id": 2,
            "text": "Agents"
          },
          {
            "id": 3,
            "text": "External API"
          }
        ]
      },
      "trigger-name": {
        "label": "Trigger Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "message": {
        "label": "Message Body",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "template-selected": {
        "label": "Template Selected",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "ai-output": {
        "label": "AI Output",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "character-count": {
        "label": "Character Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "status-3": {
        "label": "Delivery Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 5,
            "text": "Pending"
          },
          {
            "id": 1,
            "text": "Sent"
          },
          {
            "id": 2,
            "text": "Delivered"
          },
          {
            "id": 3,
            "text": "Failed"
          },
          {
            "id": 4,
            "text": "Received"
          }
        ]
      },
      "status-2": {
        "label": "Raw Carrier Status",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-difficulty-score": {
        "label": "AI Difficulty Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "is-final-failure": {
        "label": "Is Final Failure?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "failure-bucket": {
        "label": "Failure Bucket",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Hard Bounce"
          },
          {
            "id": 2,
            "text": "Soft Bounce"
          },
          {
            "id": 3,
            "text": "Spam"
          },
          {
            "id": 4,
            "text": "DNC"
          },
          {
            "id": 5,
            "text": "Other"
          }
        ]
      },
      "latency-ms": {
        "label": "Latency (ms)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30636444": {
    "app_id": 30636444,
    "app_name": "Properties",
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
        "label": "Owner Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-type-2": {
        "label": "Owner Type",
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
          },
          {
            "id": 7,
            "text": "#7"
          }
        ]
      },
      "property-address": {
        "label": "Property Address",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "latitude": {
        "label": "Latitude",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "longitude": {
        "label": "Longitude",
        "type": "text",
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
      "market-3": {
        "label": "Market",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "San Bernardino, CA"
          },
          {
            "id": 2,
            "text": "Riverside, CA"
          },
          {
            "id": 3,
            "text": "Hartford, CT"
          },
          {
            "id": 4,
            "text": "Providence, RI"
          },
          {
            "id": 5,
            "text": "Durham, NC"
          },
          {
            "id": 6,
            "text": "Boise, ID"
          },
          {
            "id": 7,
            "text": "Fayetteville, NC"
          },
          {
            "id": 8,
            "text": "Rocky Mount, NC"
          },
          {
            "id": 9,
            "text": "Oklahoma City, OK"
          },
          {
            "id": 10,
            "text": "Columbus, OH"
          },
          {
            "id": 11,
            "text": "Pittsburgh, PA"
          },
          {
            "id": 12,
            "text": "Cincinnati, OH"
          },
          {
            "id": 13,
            "text": "Louisville, KY"
          },
          {
            "id": 14,
            "text": "Richmond, VA"
          },
          {
            "id": 15,
            "text": "Hampton Roads, VA"
          },
          {
            "id": 16,
            "text": "Rochester, NY"
          },
          {
            "id": 17,
            "text": "Albuquerque, NM"
          },
          {
            "id": 18,
            "text": "El Paso, TX"
          },
          {
            "id": 19,
            "text": "Colorado Springs, CO"
          },
          {
            "id": 20,
            "text": "Omaha, NE"
          },
          {
            "id": 21,
            "text": "Wichita, KS"
          },
          {
            "id": 22,
            "text": "Des Moines, IA"
          },
          {
            "id": 23,
            "text": "San Antonio, TX"
          },
          {
            "id": 24,
            "text": "Ogden, UT"
          },
          {
            "id": 25,
            "text": "Salt Lake City, UT"
          },
          {
            "id": 26,
            "text": "St. Louis, MO"
          },
          {
            "id": 27,
            "text": "Cleveland, OH"
          },
          {
            "id": 28,
            "text": "Detroit, MI"
          },
          {
            "id": 29,
            "text": "Baltimore, MD"
          },
          {
            "id": 30,
            "text": "Philadelphia, PA"
          },
          {
            "id": 31,
            "text": "Chicago, IL"
          },
          {
            "id": 32,
            "text": "Minneapolis, MN"
          },
          {
            "id": 33,
            "text": "St. Paul, MN"
          },
          {
            "id": 34,
            "text": "Milwaukee, WI"
          },
          {
            "id": 35,
            "text": "Indianapolis, IN"
          },
          {
            "id": 36,
            "text": "Kansas City, MO"
          },
          {
            "id": 37,
            "text": "Kansas City, KS"
          },
          {
            "id": 38,
            "text": "Atlanta, GA"
          },
          {
            "id": 39,
            "text": "Unmapped"
          },
          {
            "id": 40,
            "text": "Houston, TX"
          },
          {
            "id": 41,
            "text": "Memphis, TN"
          },
          {
            "id": 42,
            "text": "Las Vegas, NV"
          },
          {
            "id": 43,
            "text": "Charlotte, NC"
          },
          {
            "id": 44,
            "text": "New Orleans, LA"
          },
          {
            "id": 45,
            "text": "Dallas, TX"
          },
          {
            "id": 46,
            "text": "Fort Worth, TX"
          },
          {
            "id": 47,
            "text": "Birmingham, AL"
          },
          {
            "id": 48,
            "text": "Bakersfield, CA"
          },
          {
            "id": 49,
            "text": "Jacksonville, FL"
          },
          {
            "id": 50,
            "text": "Orlando, FL"
          },
          {
            "id": 51,
            "text": "Tucson, AZ"
          },
          {
            "id": 52,
            "text": "Fresno, CA"
          },
          {
            "id": 53,
            "text": "Sacramento, CA"
          },
          {
            "id": 54,
            "text": "Los Angeles, CA"
          },
          {
            "id": 55,
            "text": "Fort Lauderdale, FL"
          },
          {
            "id": 56,
            "text": "Tampa, FL"
          },
          {
            "id": 57,
            "text": "Miami, FL"
          },
          {
            "id": 58,
            "text": "West Palm Beach, FL"
          },
          {
            "id": 59,
            "text": "Stockton, CA"
          },
          {
            "id": 60,
            "text": "Stockton/Modesto, CA"
          },
          {
            "id": 61,
            "text": "Spokane, WA"
          },
          {
            "id": 62,
            "text": "Tulsa, OK"
          },
          {
            "id": 63,
            "text": "Austin, TX"
          },
          {
            "id": 64,
            "text": "Phoenix, AZ"
          },
          {
            "id": 65,
            "text": "Palm Springs, CA"
          },
          {
            "id": 66,
            "text": "Inland Empire, CA"
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
      "occupancy-status": {
        "label": "Occupancy Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Absentee Owner"
          },
          {
            "id": 2,
            "text": "Owner Occupied"
          }
        ]
      },
      "smart-cash-offer-2": {
        "label": "Smart Cash Offer",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "offer-ppsf": {
        "label": "Offer PPSF",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "offer-ppu": {
        "label": "Offer PPU",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "offer-ppls": {
        "label": "Offer PPLS",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "offer-ppbd": {
        "label": "Offer PPBD",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
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
            "id": 7,
            "text": "Mobile Home"
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
            "id": 7,
            "text": "unknown"
          },
          {
            "id": 8,
            "text": "Conventional"
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
          },
          {
            "id": 40,
            "text": "A-Frame"
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
      "avg-sqft-per-unit": {
        "label": "Avg SqFt Per Unit",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "avg-beds-per-unit": {
        "label": "Avg Beds Per Unit",
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
      "motivation-layers": {
        "label": "Motivation Layers",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "final-aquisition-score": {
        "label": "FINAL Aquisition Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tag-distress-score": {
        "label": "Tag Distress Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "deal-strength-score": {
        "label": "Deal Strength Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "structured-motivation-score": {
        "label": "Structured Motivation Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
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
          },
          {
            "id": 5,
            "text": "No Purchase Data"
          }
        ]
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
            "text": "1.5"
          },
          {
            "id": 96,
            "text": "2.5"
          },
          {
            "id": 97,
            "text": "13.5"
          },
          {
            "id": 98,
            "text": "10.5"
          },
          {
            "id": 99,
            "text": "3.5"
          },
          {
            "id": 100,
            "text": "19.5"
          },
          {
            "id": 101,
            "text": "4.5"
          },
          {
            "id": 102,
            "text": "3.25"
          },
          {
            "id": 103,
            "text": "2.25"
          },
          {
            "id": 104,
            "text": "1.75"
          },
          {
            "id": 105,
            "text": "7.5"
          },
          {
            "id": 106,
            "text": "5.75"
          },
          {
            "id": 107,
            "text": "3.75"
          },
          {
            "id": 108,
            "text": "2.75"
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
            "text": "49"
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
            "text": "1250–1500"
          },
          {
            "id": 3,
            "text": "1000–1250"
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
            "text": "2000–2500"
          },
          {
            "id": 7,
            "text": "2500–3000"
          },
          {
            "id": 8,
            "text": "Non-SFR"
          },
          {
            "id": 9,
            "text": "3000+"
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
            "id": 17,
            "text": "Rock, Stone"
          },
          {
            "id": 18,
            "text": "Siding Not (aluminum, vinyl, etc.)"
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
          },
          {
            "id": 28,
            "text": "ExteriorWalls"
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
          },
          {
            "id": 16,
            "text": "Brick"
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
          },
          {
            "id": 14,
            "text": "Offsite"
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
          },
          {
            "id": 12,
            "text": "Stone"
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
          },
          {
            "id": 20,
            "text": "RoofCover"
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
            "id": 15,
            "text": "Trustee’s Deed"
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
      "rehab-level": {
        "label": "Rehab Level",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Full Rehab"
          },
          {
            "id": 2,
            "text": "Structural"
          },
          {
            "id": 3,
            "text": "Moderate"
          },
          {
            "id": 4,
            "text": "Cosmetic"
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
          },
          {
            "id": 6,
            "text": "Sewer"
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
          },
          {
            "id": 6,
            "text": "Water"
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
            "id": 3,
            "text": "Low Elevation"
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
            "id": 32,
            "text": "R2A"
          },
          {
            "id": 33,
            "text": "SR1"
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
            "id": 54,
            "text": "R-1-SC"
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
          },
          {
            "id": 261,
            "text": "R"
          },
          {
            "id": 262,
            "text": "RS10M"
          },
          {
            "id": 263,
            "text": "JTRS"
          },
          {
            "id": 264,
            "text": "LCRS"
          },
          {
            "id": 265,
            "text": "LARS14M"
          },
          {
            "id": 266,
            "text": "RM"
          },
          {
            "id": 267,
            "text": "BLRS"
          },
          {
            "id": 268,
            "text": "926"
          },
          {
            "id": 269,
            "text": "MSRS10M"
          },
          {
            "id": 270,
            "text": "R1510"
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
      "field-34": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
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
            "id": 95,
            "text": "North Country Village"
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
      "hoa-fee-amount": {
        "label": "HOA Fee Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-15": {
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
      "recording-date": {
        "label": "Recording Date",
        "type": "date",
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
      "default-date": {
        "label": "Default Date",
        "type": "date",
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
      "field-17": {
        "label": ">",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
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
            "id": 40,
            "text": "Hoa Lien"
          },
          {
            "id": 41,
            "text": "Recently Sold"
          },
          {
            "id": 43,
            "text": "Moderately Updated"
          },
          {
            "id": 44,
            "text": "Cosmetic Updates Only"
          },
          {
            "id": 45,
            "text": "Corner Lot"
          }
        ]
      },
      "ownerlinkstatus": {
        "label": "owner_link_status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owneridtext": {
        "label": "owner_id_text",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "linked-master-owner": {
        "label": "Linked Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
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
      }
    }
  },
  "30637059": {
    "app_id": 30637059,
    "app_name": "Owners",
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
          },
          {
            "id": 9,
            "text": "OWNER TYPE"
          }
        ]
      },
      "master-owner": {
        "label": "MASTER OWNER",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "YES"
          },
          {
            "id": 2,
            "text": "NO"
          }
        ]
      },
      "corrected-file": {
        "label": "CORRECTED FILE",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "YES"
          },
          {
            "id": 2,
            "text": "CORRECTED FILE"
          },
          {
            "id": 3,
            "text": "MAIN FILE"
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
            "text": "#1"
          },
          {
            "id": 3,
            "text": "#2"
          },
          {
            "id": 4,
            "text": "#5"
          },
          {
            "id": 5,
            "text": "#3"
          },
          {
            "id": 6,
            "text": "#4"
          },
          {
            "id": 7,
            "text": "#7"
          },
          {
            "id": 8,
            "text": "Joseph Hughes"
          },
          {
            "id": 9,
            "text": "Karl Hughes"
          },
          {
            "id": 10,
            "text": "Sean Hughes"
          },
          {
            "id": 11,
            "text": "Shirley Hughes"
          },
          {
            "id": 12,
            "text": "Trevor Hughes"
          },
          {
            "id": 13,
            "text": "Tanja Hughley"
          },
          {
            "id": 14,
            "text": "Saul O Huiracocha & Maria I Chaca"
          },
          {
            "id": 15,
            "text": "Ian W Huisken"
          },
          {
            "id": 16,
            "text": "Human Service Housing Com"
          },
          {
            "id": 17,
            "text": "Maria U Humberto"
          },
          {
            "id": 18,
            "text": "Ashton Humphrey"
          },
          {
            "id": 19,
            "text": "Earnest Humphrey"
          },
          {
            "id": 20,
            "text": "Stanley & Pearlie Humphrey"
          },
          {
            "id": 21,
            "text": "Cortez Humphries"
          },
          {
            "id": 22,
            "text": "Nhu Hung"
          },
          {
            "id": 23,
            "text": "Jannie & Einestine Hunley"
          },
          {
            "id": 24,
            "text": "Cleavon Hunt & Adam Clayton"
          },
          {
            "id": 25,
            "text": "Adam Hunt"
          },
          {
            "id": 26,
            "text": "Angelique Hunt"
          },
          {
            "id": 27,
            "text": "John & Bertha Hunt"
          },
          {
            "id": 28,
            "text": "Cassondra Hunt"
          },
          {
            "id": 29,
            "text": "Donald E Hunt"
          },
          {
            "id": 30,
            "text": "Elbert Hunt"
          },
          {
            "id": 31,
            "text": "James & Lizzie Hunt"
          },
          {
            "id": 32,
            "text": "Jerry Hunt"
          },
          {
            "id": 33,
            "text": "John Hunt"
          },
          {
            "id": 34,
            "text": "Karen R Hunt & Kenyatta Robinson"
          },
          {
            "id": 35,
            "text": "Larry W Hunt"
          },
          {
            "id": 36,
            "text": "Maliq Q Hunt"
          },
          {
            "id": 37,
            "text": "Shirley Hunt"
          },
          {
            "id": 38,
            "text": "Thelma H Hunt"
          },
          {
            "id": 39,
            "text": "Arthur D Hunter"
          },
          {
            "id": 40,
            "text": "Carl Hunter"
          },
          {
            "id": 41,
            "text": "Carolyn Hunter"
          },
          {
            "id": 42,
            "text": "Clarence Hunter"
          },
          {
            "id": 43,
            "text": "Dion Hunter"
          },
          {
            "id": 44,
            "text": "Gregory Hunter"
          },
          {
            "id": 45,
            "text": "Jeanett Hunter"
          },
          {
            "id": 46,
            "text": "Joseph Hunter III"
          },
          {
            "id": 47,
            "text": "Michelle D Hunter"
          },
          {
            "id": 48,
            "text": "Willie & Pat Hunter"
          },
          {
            "id": 49,
            "text": "Patricia A Hunter"
          },
          {
            "id": 50,
            "text": "Solomon Hunter Jr"
          },
          {
            "id": 51,
            "text": "Terence Hunter"
          },
          {
            "id": 52,
            "text": "Vivian Hunter"
          },
          {
            "id": 53,
            "text": "Huntington National Bank"
          },
          {
            "id": 54,
            "text": "Barbara J Hurd"
          },
          {
            "id": 55,
            "text": "Ezekiel & Sandra Hurst"
          },
          {
            "id": 56,
            "text": "Stanley & Kim Hurst"
          },
          {
            "id": 57,
            "text": "Murel H Hurt Sr"
          },
          {
            "id": 58,
            "text": "Alejandro Hurtado"
          },
          {
            "id": 59,
            "text": "Daisy Hurtado"
          },
          {
            "id": 60,
            "text": "Otto Hurtado"
          },
          {
            "id": 61,
            "text": "R A Hurtado"
          },
          {
            "id": 62,
            "text": "Teofilo Hurtado"
          },
          {
            "id": 63,
            "text": "Richard L Husarick"
          },
          {
            "id": 64,
            "text": "David & Addie Husband"
          },
          {
            "id": 65,
            "text": "Brett Huseby"
          },
          {
            "id": 66,
            "text": "Ahmed Hussain"
          },
          {
            "id": 67,
            "text": "Hasina Hussain"
          },
          {
            "id": 68,
            "text": "Hutchins"
          },
          {
            "id": 69,
            "text": "Geraldine Hutchins"
          },
          {
            "id": 70,
            "text": "Hannah & Scharad Hutchins"
          },
          {
            "id": 71,
            "text": "Scharad Hutchins"
          },
          {
            "id": 72,
            "text": "Bernard Hutchinson"
          },
          {
            "id": 73,
            "text": "Joan Hutchinson"
          },
          {
            "id": 74,
            "text": "Michelle & Liam Hutchinson"
          },
          {
            "id": 75,
            "text": "Charles D Huth"
          },
          {
            "id": 76,
            "text": "Michael Hutson"
          },
          {
            "id": 77,
            "text": "Thomas T Huynh & Anh Luu"
          },
          {
            "id": 78,
            "text": "Tan Huynh & Bich-ngoc Nguyen"
          },
          {
            "id": 79,
            "text": "Nam Huynh & Ha Nu"
          },
          {
            "id": 80,
            "text": "Ivane Q Huynh"
          },
          {
            "id": 81,
            "text": "Thuy X Huynh & Theresa Nguyen"
          },
          {
            "id": 82,
            "text": "Michael Hwang"
          },
          {
            "id": 83,
            "text": "Tracy & Babette Hykes"
          },
          {
            "id": 84,
            "text": "Charles Hyman"
          },
          {
            "id": 85,
            "text": "Sharon M Hynes & David Roswurm"
          },
          {
            "id": 86,
            "text": "Betty C Iacona"
          },
          {
            "id": 87,
            "text": "Celina A Iancu"
          },
          {
            "id": 88,
            "text": "Patrizia Iasiello"
          },
          {
            "id": 89,
            "text": "Gisell Ibarburu"
          },
          {
            "id": 90,
            "text": "Adolfo Ibarra & Maria D Delibarra"
          },
          {
            "id": 91,
            "text": "Maria Y Ibarra"
          },
          {
            "id": 92,
            "text": "Nasreldin H Ibrahim"
          },
          {
            "id": 93,
            "text": "Jeffrey Ibsen"
          },
          {
            "id": 94,
            "text": "W D W & Frances Idleburg"
          },
          {
            "id": 95,
            "text": "Cie Idrizi"
          },
          {
            "id": 96,
            "text": "Jorge M Idrovo"
          },
          {
            "id": 97,
            "text": "Margie B Igbinosun"
          },
          {
            "id": 98,
            "text": "Mitchell E Igess"
          },
          {
            "id": 99,
            "text": "Iglesia Evangelica Ch"
          },
          {
            "id": 100,
            "text": "Krzysztof & Bozena Ignasiak"
          },
          {
            "id": 101,
            "text": "Douglas & Dorothy Ikeh"
          },
          {
            "id": 102,
            "text": "3316 C Illino"
          },
          {
            "id": 103,
            "text": "Rapid O Illinoi"
          },
          {
            "id": 104,
            "text": "Illinoid Land Invest"
          },
          {
            "id": 105,
            "text": "Illinois Hoising Auth"
          },
          {
            "id": 106,
            "text": "Illinois Housing Auth"
          },
          {
            "id": 107,
            "text": "Illinois Housing Ayth"
          },
          {
            "id": 108,
            "text": "Claire A Iltis & Joy A Feasley"
          },
          {
            "id": 109,
            "text": "Nataliya Ilto"
          },
          {
            "id": 110,
            "text": "Keith Immken"
          },
          {
            "id": 111,
            "text": "Infinite Real Est"
          },
          {
            "id": 112,
            "text": "Billy & Cheryl Ingraham"
          },
          {
            "id": 113,
            "text": "Micah G Ingram"
          },
          {
            "id": 114,
            "text": "Aaron Ingrum"
          },
          {
            "id": 115,
            "text": "Sharontoya Inkton"
          },
          {
            "id": 116,
            "text": "Kerry & Tanisha Innis"
          },
          {
            "id": 117,
            "text": "Innovation Prop Serie"
          },
          {
            "id": 118,
            "text": "David & Mamie Irace"
          },
          {
            "id": 119,
            "text": "David M Irace"
          },
          {
            "id": 120,
            "text": "Liza Irazoque"
          },
          {
            "id": 121,
            "text": "Eugene & Toni Irby"
          },
          {
            "id": 122,
            "text": "Dramaine Irions"
          },
          {
            "id": 123,
            "text": "Adnan Iriskic"
          },
          {
            "id": 124,
            "text": "Obioma Iro-nwokeukwu"
          },
          {
            "id": 125,
            "text": "Iron Stone Enterprises"
          },
          {
            "id": 126,
            "text": "Ella & James Irons"
          },
          {
            "id": 127,
            "text": "D J Irpino"
          },
          {
            "id": 128,
            "text": "Dominic J Irpino"
          },
          {
            "id": 129,
            "text": "Irrevocable Special Needs"
          },
          {
            "id": 130,
            "text": "Julie E Irvin"
          },
          {
            "id": 131,
            "text": "Corey A Irving"
          },
          {
            "id": 132,
            "text": "Elizabeth Irwin"
          },
          {
            "id": 133,
            "text": "Patricia Irwin & Mary Taylor"
          },
          {
            "id": 134,
            "text": "Jamal R Isaac"
          },
          {
            "id": 135,
            "text": "Sargon Isaac"
          },
          {
            "id": 136,
            "text": "Patricia & Tomas Isakowitz"
          },
          {
            "id": 137,
            "text": "Sara Isbell"
          },
          {
            "id": 138,
            "text": "Kashif Ishaq & Sabeen Zia"
          },
          {
            "id": 139,
            "text": "Md S Islam"
          },
          {
            "id": 140,
            "text": "Safi Islam"
          },
          {
            "id": 141,
            "text": "Island Jennie"
          },
          {
            "id": 142,
            "text": "Joseph M Isom"
          },
          {
            "id": 143,
            "text": "Richard & William Isom"
          },
          {
            "id": 144,
            "text": "Perry Ison"
          },
          {
            "id": 145,
            "text": "Ronald R Israel"
          },
          {
            "id": 146,
            "text": "Mohamed A Issa"
          },
          {
            "id": 147,
            "text": "Nafe Issa"
          },
          {
            "id": 148,
            "text": "Andrew IV Lockhart"
          },
          {
            "id": 149,
            "text": "Kimberly M IVerson"
          },
          {
            "id": 150,
            "text": "Bobby IVory"
          },
          {
            "id": 151,
            "text": "Darlene IVory"
          },
          {
            "id": 152,
            "text": "Margaret IVory"
          },
          {
            "id": 153,
            "text": "Jason T IVy"
          },
          {
            "id": 154,
            "text": "Kaeyava T IVy"
          },
          {
            "id": 155,
            "text": "Patricia L IVy"
          },
          {
            "id": 156,
            "text": "Rachel IVy"
          },
          {
            "id": 157,
            "text": "Helen C Iwanowski"
          },
          {
            "id": 158,
            "text": "Fernando Izaguirre"
          },
          {
            "id": 159,
            "text": "Lauro Izaguirre"
          },
          {
            "id": 160,
            "text": "J & J Heating Cooling"
          },
          {
            "id": 161,
            "text": "J A M Enterprise"
          },
          {
            "id": 162,
            "text": "Yalisa Jabbie"
          },
          {
            "id": 163,
            "text": "Larry D Jablonowski"
          },
          {
            "id": 164,
            "text": "John E Jablonski"
          },
          {
            "id": 165,
            "text": "Robert Jablonski"
          },
          {
            "id": 166,
            "text": "Jack Gamboa Enterprise"
          },
          {
            "id": 167,
            "text": "Jackson Marilyn D Rev Trst"
          },
          {
            "id": 168,
            "text": "Pamela Jackson-petty"
          },
          {
            "id": 169,
            "text": "Alan C Jackson"
          },
          {
            "id": 170,
            "text": "Allen F Jackson"
          },
          {
            "id": 171,
            "text": "Alphonso Jackson"
          },
          {
            "id": 172,
            "text": "Alphonzo Jackson"
          },
          {
            "id": 173,
            "text": "Alysia Jackson"
          },
          {
            "id": 174,
            "text": "Angelene Jackson"
          },
          {
            "id": 175,
            "text": "Arthur L Jackson"
          },
          {
            "id": 176,
            "text": "Barbara A Jackson"
          },
          {
            "id": 177,
            "text": "Bianca Jackson"
          },
          {
            "id": 178,
            "text": "James & Brenda Jackson"
          },
          {
            "id": 179,
            "text": "Carla Jackson"
          },
          {
            "id": 180,
            "text": "Carolyn Jackson"
          },
          {
            "id": 181,
            "text": "Charles Jackson"
          },
          {
            "id": 182,
            "text": "Chavone Jackson"
          },
          {
            "id": 183,
            "text": "Claudell & Sandra Jackson"
          },
          {
            "id": 184,
            "text": "Constance Jackson & Erica Harris"
          },
          {
            "id": 185,
            "text": "Darlean Jackson"
          },
          {
            "id": 186,
            "text": "Darnell Jackson"
          },
          {
            "id": 187,
            "text": "David N Jackson"
          },
          {
            "id": 188,
            "text": "David Jackson"
          },
          {
            "id": 189,
            "text": "Deborah L Jackson"
          },
          {
            "id": 190,
            "text": "Desiree Jackson"
          },
          {
            "id": 191,
            "text": "Eric & Diara Jackson"
          },
          {
            "id": 192,
            "text": "Willie & Donna Jackson"
          },
          {
            "id": 193,
            "text": "Doreen K Jackson & Tyres D Mosby"
          },
          {
            "id": 194,
            "text": "Earsell Jackson"
          },
          {
            "id": 195,
            "text": "Eddie F Jackson"
          },
          {
            "id": 196,
            "text": "Edith G Jackson"
          },
          {
            "id": 197,
            "text": "Emery D Jackson"
          },
          {
            "id": 198,
            "text": "Jimmie & Ethel Jackson"
          },
          {
            "id": 199,
            "text": "Evana L Jackson"
          },
          {
            "id": 200,
            "text": "Gwendolyn Jackson"
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
          },
          {
            "id": 3,
            "text": "out_of_state_owner"
          }
        ]
      },
      "tax-mailing-address": {
        "label": "Owner Address",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation": {
        "label": ">",
        "type": "calculation",
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
      },
      "linked-master-owner": {
        "label": "Linked Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      }
    }
  },
  "30637173": {
    "app_id": 30637173,
    "app_name": "Prospects",
    "item_name": "Seller",
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
          },
          {
            "id": 10,
            "text": "#1[0]"
          },
          {
            "id": 11,
            "text": "#1[1]"
          },
          {
            "id": 12,
            "text": "#1[2]"
          },
          {
            "id": 13,
            "text": "#2 (2)"
          },
          {
            "id": 14,
            "text": "#2[0]"
          },
          {
            "id": 15,
            "text": "#2[1]"
          },
          {
            "id": 16,
            "text": "#2[2]"
          },
          {
            "id": 17,
            "text": "#3[0]"
          },
          {
            "id": 18,
            "text": "#3[1]"
          },
          {
            "id": 19,
            "text": "#3[2]"
          },
          {
            "id": 20,
            "text": "#4[0]"
          },
          {
            "id": 21,
            "text": "#5[0]"
          },
          {
            "id": 22,
            "text": "#5[1]"
          },
          {
            "id": 23,
            "text": "#5[2]"
          },
          {
            "id": 24,
            "text": "#6[0]"
          },
          {
            "id": 25,
            "text": "#6[1]"
          },
          {
            "id": 26,
            "text": "#6[2]"
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
            "id": 6,
            "text": "0"
          }
        ]
      },
      "motivation-score-ai": {
        "label": "Motivation Score (AI)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contact-matching-tags": {
        "label": "Contact Matching Tags",
        "type": "category",
        "multiple": false,
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
      "age-bracket": {
        "label": "Age Bracket",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "30–44"
          },
          {
            "id": 2,
            "text": "18–29"
          },
          {
            "id": 3,
            "text": "45–59"
          },
          {
            "id": 4,
            "text": "60–74"
          },
          {
            "id": 5,
            "text": "75+"
          },
          {
            "id": 6,
            "text": "#VALUE!"
          }
        ]
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
          }
        ]
      },
      "conversations-thread": {
        "label": "Conversations Thread",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30643943
        ],
        "options": []
      },
      "latest-ai-summary": {
        "label": "Latest AI Summary",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-contact-date": {
        "label": "Last Contact Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "linked-phone-numbers": {
        "label": "Linked Phone Numbers",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "linked-emails": {
        "label": "Linked Emails",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646486
        ],
        "options": []
      },
      "linked-phone-number": {
        "label": "Primary Phone Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "linked-email-addresses": {
        "label": "Primary Email Address",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646486
        ],
        "options": []
      },
      "linked-master-owner": {
        "label": "Linked Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
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
      },
      "assigned-agent-ai": {
        "label": "Assigned Agent (AI)",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644052
        ],
        "options": []
      }
    }
  },
  "30643944": {
    "app_id": 30643944,
    "app_name": "Offers",
    "item_name": "Offer",
    "fields": {
      "offer-id": {
        "label": "Offer ID",
        "type": "text",
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
      "relationship": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "property": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "offer-type": {
        "label": "Offer Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Cash"
          },
          {
            "id": 2,
            "text": "Subject To"
          },
          {
            "id": 3,
            "text": "Lease Option"
          },
          {
            "id": 4,
            "text": "Multi Family"
          },
          {
            "id": 5,
            "text": "Novation"
          }
        ]
      },
      "assigned-agent": {
        "label": "Assigned Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30678167
        ],
        "options": []
      },
      "offer-date": {
        "label": "Offer Sent Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "offer-expiration-date-2": {
        "label": "Offer Expiration Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
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
            "text": "Viewed"
          },
          {
            "id": 3,
            "text": "Counter Received"
          },
          {
            "id": 4,
            "text": "Revised Offer Sent"
          },
          {
            "id": 5,
            "text": "Negotiating"
          },
          {
            "id": 7,
            "text": "Accepted (Ready for Contract)"
          },
          {
            "id": 8,
            "text": "Rejected"
          },
          {
            "id": 9,
            "text": "Expired"
          }
        ]
      },
      "follow-up-window": {
        "label": "Follow-Up Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "section-separator": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "relationship-2": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "prospect": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "phone-number": {
        "label": "Phone Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30658310
        ],
        "options": []
      },
      "email-address": {
        "label": "Email Address",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646486
        ],
        "options": []
      },
      "conversation": {
        "label": "Conversation",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646484
        ],
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
      "offer-sent-price-2": {
        "label": "Offer Sent Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "seller-asking-price-3": {
        "label": "Seller Asking Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "seller-counter-offer-3": {
        "label": "Seller Counter Offer",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "accepted-date": {
        "label": "Accepted Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rejected-date": {
        "label": "Rejected Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "converted-to-contract": {
        "label": "Converted To Contract?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "under-contract-date": {
        "label": "Under Contract Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "closing-date-target": {
        "label": "Closing Date Target",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract": {
        "label": "Contract",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30643964
        ],
        "options": []
      },
      "deal-killed-reason": {
        "label": "Deal Killed Reason",
        "type": "category",
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
  "30646484": {
    "app_id": 30646484,
    "app_name": "AI Conversation Brain",
    "item_name": "Message",
    "fields": {
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "phone-number": {
        "label": "Phone Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637174
        ],
        "options": []
      },
      "prospect": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "properties": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "ai-agent-assigned": {
        "label": "AI Agent Assigned",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sms-agent": {
        "label": "SMS Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30678167
        ],
        "options": []
      },
      "last-template-sent": {
        "label": "Last Template Sent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "message-token-log": {
        "label": "Message Token Log",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-sent-time": {
        "label": "Last Sent Time",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "conversation-stage": {
        "label": "Conversation Stage",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Ownership"
          },
          {
            "id": 2,
            "text": "Offer"
          },
          {
            "id": 3,
            "text": "Q/A"
          },
          {
            "id": 4,
            "text": "Contract"
          },
          {
            "id": 5,
            "text": "Follow-Up"
          }
        ]
      },
      "ai-route": {
        "label": "AI Route",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Aggressive"
          },
          {
            "id": 2,
            "text": "Soft"
          },
          {
            "id": 3,
            "text": "Quick Offer"
          },
          {
            "id": 4,
            "text": "Deep Motivational"
          }
        ]
      },
      "seller-profile": {
        "label": "Seller Profile",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Probate"
          },
          {
            "id": 2,
            "text": "Tired Landlord"
          },
          {
            "id": 3,
            "text": "Strategic Seller"
          }
        ]
      },
      "language-preference": {
        "label": "Language Preference",
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
            "text": "French"
          },
          {
            "id": 5,
            "text": "Italian"
          },
          {
            "id": 6,
            "text": "Russian"
          },
          {
            "id": 7,
            "text": "Hebrew"
          },
          {
            "id": 8,
            "text": "German"
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
            "text": "Mandarin"
          },
          {
            "id": 13,
            "text": "Asian Indian (Hindi or Other)"
          },
          {
            "id": 14,
            "text": "Vietnamese"
          },
          {
            "id": 15,
            "text": "Arabic"
          },
          {
            "id": 16,
            "text": "Greek"
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
            "text": "Masculine"
          },
          {
            "id": 2,
            "text": "Feminine"
          },
          {
            "id": 3,
            "text": "Neutral"
          }
        ]
      },
      "status-ai-managed": {
        "label": "Status (AI Managed)",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "_ Active Negotiation"
          },
          {
            "id": 2,
            "text": "_ Warm Lead"
          },
          {
            "id": 3,
            "text": "_ Hot Opportunity"
          },
          {
            "id": 4,
            "text": "_ Waiting on Seller"
          },
          {
            "id": 5,
            "text": "⚫️ AI Follow-Up Running"
          },
          {
            "id": 6,
            "text": "⚪️ Cold / No Response"
          },
          {
            "id": 7,
            "text": "_ Under Contract"
          },
          {
            "id": 8,
            "text": "_ Closed"
          }
        ]
      },
      "seller-motivation-score": {
        "label": "Seller Motivation Score",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "deal-prioirty-tag": {
        "label": "Deal Prioirty Tag",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "_ High Priority"
          },
          {
            "id": 2,
            "text": "_ Medium"
          },
          {
            "id": 3,
            "text": "_ Low"
          }
        ]
      },
      "transcript": {
        "label": "Last Message Summary (AI)",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title": {
        "label": "Full Conversation Summary (AI)",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ais-recommended-next-move": {
        "label": "AI’s Recommended Next Move",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "risk-flags-ai": {
        "label": "Risk Flags (AI)",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "_ Seller Hesitation"
          },
          {
            "id": 2,
            "text": "_ Wants too high"
          },
          {
            "id": 3,
            "text": "_ Not decision maker"
          },
          {
            "id": 4,
            "text": "_ Possible scam"
          },
          {
            "id": 5,
            "text": "_ Angry / short replies"
          },
          {
            "id": 6,
            "text": "_ Emotional volatility"
          }
        ]
      },
      "follow-up-trigger-state": {
        "label": "Follow-Up Trigger State",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "AI Running"
          },
          {
            "id": 2,
            "text": "Waiting"
          },
          {
            "id": 3,
            "text": "Paused"
          },
          {
            "id": 4,
            "text": "Manual Override"
          }
        ]
      },
      "ai-next-message": {
        "label": "AI Next Message",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-outbound-message": {
        "label": "Last Outbound Message",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-inbound-message": {
        "label": "Last Inbound Message",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-contact-timestamp": {
        "label": "Last Contact Timestamp",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30646486": {
    "app_id": 30646486,
    "app_name": "Emails",
    "item_name": "Email",
    "fields": {
      "title": {
        "label": "Full Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "first-name": {
        "label": "First Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "email": {
        "label": "Email",
        "type": "email",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "email-hidden": {
        "label": "Email (HIDDEN)",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "email-role": {
        "label": "Email Role",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Primary"
          },
          {
            "id": 2,
            "text": "Secondary"
          },
          {
            "id": 3,
            "text": "Business"
          },
          {
            "id": 4,
            "text": "Unknown"
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
            "text": "Spanish"
          },
          {
            "id": 2,
            "text": "English"
          },
          {
            "id": 3,
            "text": "Vietnamese"
          },
          {
            "id": 4,
            "text": "Russian"
          },
          {
            "id": 5,
            "text": "Italian"
          },
          {
            "id": 6,
            "text": "Portuguese"
          },
          {
            "id": 7,
            "text": "Mandarin"
          },
          {
            "id": 8,
            "text": "Asian Indian (Hindi or Other)"
          },
          {
            "id": 9,
            "text": "Greek"
          },
          {
            "id": 10,
            "text": "French"
          },
          {
            "id": 11,
            "text": "Arabic"
          },
          {
            "id": 12,
            "text": "Japanese"
          },
          {
            "id": 13,
            "text": "Korean"
          },
          {
            "id": 14,
            "text": "German"
          },
          {
            "id": 15,
            "text": "Polish"
          },
          {
            "id": 16,
            "text": "Hebrew"
          },
          {
            "id": 17,
            "text": "Thai"
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
            "text": "[0](1)"
          },
          {
            "id": 2,
            "text": "#1[0](1)"
          },
          {
            "id": 3,
            "text": "#1 [0](2)"
          },
          {
            "id": 4,
            "text": "#5"
          },
          {
            "id": 5,
            "text": "#2"
          },
          {
            "id": 6,
            "text": "#3"
          }
        ]
      },
      "linkage-score": {
        "label": "Linkage Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "percentage": {
        "label": "Percentage",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ranking": {
        "label": "Ranking",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Low"
          },
          {
            "id": 2,
            "text": "Medium"
          },
          {
            "id": 3,
            "text": "Elite"
          },
          {
            "id": 4,
            "text": "Weak"
          },
          {
            "id": 5,
            "text": "Strong"
          }
        ]
      },
      "prospects": {
        "label": "Prospects",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "owners": {
        "label": "Owners",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637059
        ],
        "options": []
      },
      "linked-master-owners": {
        "label": "Linked Master Owners",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      }
    }
  },
  "30647181": {
    "app_id": 30647181,
    "app_name": "Templates",
    "item_name": "Template",
    "fields": {
      "template-id": {
        "label": "Template ID",
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
      "property-type": {
        "label": "Category",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Ownership Verification"
          },
          {
            "id": 2,
            "text": "Follow-Up"
          },
          {
            "id": 3,
            "text": "Heavy Negotiation"
          },
          {
            "id": 4,
            "text": "Residential"
          },
          {
            "id": 5,
            "text": "Probate / Trust"
          },
          {
            "id": 6,
            "text": "Corporate / Institutional"
          },
          {
            "id": 7,
            "text": "Landlord / Multifamily"
          }
        ]
      },
      "use-case": {
        "label": "Use Case",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "First Message"
          },
          {
            "id": 2,
            "text": "Post-Confirm Offer"
          },
          {
            "id": 3,
            "text": "Re-Engagement"
          },
          {
            "id": 4,
            "text": "Cold Open"
          },
          {
            "id": 5,
            "text": "Objection Handle"
          },
          {
            "id": 6,
            "text": "Opt-Out Recovery"
          },
          {
            "id": 7,
            "text": "ownership_check"
          },
          {
            "id": 8,
            "text": "wrong_person"
          },
          {
            "id": 9,
            "text": "who_is_this"
          },
          {
            "id": 10,
            "text": "how_got_number"
          },
          {
            "id": 11,
            "text": "soft_value_drop"
          },
          {
            "id": 12,
            "text": "offer_reveal"
          },
          {
            "id": 13,
            "text": "not_ready"
          },
          {
            "id": 14,
            "text": "price_too_low"
          },
          {
            "id": 15,
            "text": "has_tenants"
          },
          {
            "id": 16,
            "text": "already_listed"
          },
          {
            "id": 17,
            "text": "already_have_someone"
          },
          {
            "id": 18,
            "text": "family_discussion"
          },
          {
            "id": 19,
            "text": "send_info"
          },
          {
            "id": 20,
            "text": "not_interested"
          },
          {
            "id": 21,
            "text": "reengagement"
          },
          {
            "id": 22,
            "text": "close_handoff"
          },
          {
            "id": 23,
            "text": "asks_contract"
          },
          {
            "id": 24,
            "text": "proof_of_funds"
          },
          {
            "id": 25,
            "text": "creative_probe"
          },
          {
            "id": 26,
            "text": "mf_units"
          },
          {
            "id": 27,
            "text": "mf_occupancy_rents"
          },
          {
            "id": 28,
            "text": "occupied_asset"
          },
          {
            "id": 29,
            "text": "send_package"
          },
          {
            "id": 30,
            "text": "pain_probe"
          },
          {
            "id": 31,
            "text": "tenants_ok"
          },
          {
            "id": 32,
            "text": "mf_occupancy"
          },
          {
            "id": 33,
            "text": "mf_rents"
          },
          {
            "id": 34,
            "text": "mf_expenses"
          },
          {
            "id": 35,
            "text": "best_price"
          },
          {
            "id": 36,
            "text": "earnest_money"
          },
          {
            "id": 37,
            "text": "title_company"
          },
          {
            "id": 38,
            "text": "closing_timeline"
          },
          {
            "id": 39,
            "text": "can_you_do_better"
          },
          {
            "id": 40,
            "text": "seller_asking_price"
          },
          {
            "id": 41,
            "text": "creative_followup"
          },
          {
            "id": 42,
            "text": "offer_no_response_followup"
          },
          {
            "id": 43,
            "text": "walkthrough_or_condition"
          },
          {
            "id": 44,
            "text": "contract_sent"
          },
          {
            "id": 45,
            "text": "contract_not_signed_followup"
          },
          {
            "id": 46,
            "text": "contract_revision"
          },
          {
            "id": 47,
            "text": "title_intro"
          },
          {
            "id": 48,
            "text": "earnest_pending"
          },
          {
            "id": 49,
            "text": "earnest_sent"
          },
          {
            "id": 50,
            "text": "inspection_schedule"
          },
          {
            "id": 51,
            "text": "walkthrough_confirmed"
          },
          {
            "id": 52,
            "text": "seller_stalling_after_yes"
          },
          {
            "id": 53,
            "text": "need_spouse_signoff"
          },
          {
            "id": 54,
            "text": "closing_date_locked"
          },
          {
            "id": 55,
            "text": "closing_date_moved"
          },
          {
            "id": 56,
            "text": "retrade_pushback"
          },
          {
            "id": 57,
            "text": "title_delay_followup"
          },
          {
            "id": 58,
            "text": "seller_docs_needed"
          },
          {
            "id": 59,
            "text": "clear_to_close"
          },
          {
            "id": 60,
            "text": "day_before_close"
          },
          {
            "id": 61,
            "text": "post_close_referral"
          },
          {
            "id": 62,
            "text": "buyer_referral_transition"
          },
          {
            "id": 63,
            "text": "sms_only_preference"
          },
          {
            "id": 64,
            "text": "no_call_reassurance"
          },
          {
            "id": 65,
            "text": "photo_request"
          },
          {
            "id": 66,
            "text": "condition_question_set"
          },
          {
            "id": 67,
            "text": "esign_link_sent"
          },
          {
            "id": 68,
            "text": "esign_help"
          },
          {
            "id": 69,
            "text": "email_for_docs"
          },
          {
            "id": 70,
            "text": "title_by_text_update"
          },
          {
            "id": 71,
            "text": "seller_asks_legit"
          },
          {
            "id": 72,
            "text": "lowball_accusation"
          },
          {
            "id": 73,
            "text": "call_me_later_redirect"
          },
          {
            "id": 74,
            "text": "ghost_after_contract"
          },
          {
            "id": 75,
            "text": "title_issue_discovered"
          },
          {
            "id": 76,
            "text": "death_sensitivity"
          },
          {
            "id": 77,
            "text": "divorce_sensitivity"
          },
          {
            "id": 78,
            "text": "sibling_conflict"
          },
          {
            "id": 79,
            "text": "foreclosure_pressure"
          },
          {
            "id": 80,
            "text": "bankruptcy_sensitivity"
          },
          {
            "id": 81,
            "text": "hostile_reply_defuse"
          },
          {
            "id": 82,
            "text": "wrong_number_knows_owner"
          },
          {
            "id": 83,
            "text": "vacant_boarded_probe"
          },
          {
            "id": 84,
            "text": "code_violation_probe"
          },
          {
            "id": 85,
            "text": "seller_finance_interest"
          },
          {
            "id": 86,
            "text": "monthly_payment_followup"
          },
          {
            "id": 87,
            "text": "website_reviews_request"
          },
          {
            "id": 88,
            "text": "text_me_later_specific"
          },
          {
            "id": 89,
            "text": "email_me_instead"
          },
          {
            "id": 90,
            "text": "lien_issue_detected"
          },
          {
            "id": 91,
            "text": "probate_doc_needed"
          },
          {
            "id": 92,
            "text": "offer_reveal_soft"
          },
          {
            "id": 93,
            "text": "offer_reveal_hard"
          },
          {
            "id": 94,
            "text": "offer_reveal_ultrashort"
          },
          {
            "id": 95,
            "text": "price_low_soft"
          },
          {
            "id": 96,
            "text": "price_low_hard"
          },
          {
            "id": 97,
            "text": "followup_soft"
          },
          {
            "id": 98,
            "text": "followup_hard"
          },
          {
            "id": 99,
            "text": "close_ask_soft"
          },
          {
            "id": 100,
            "text": "close_ask_hard"
          },
          {
            "id": 101,
            "text": "contract_nudge_ultrashort"
          },
          {
            "id": 102,
            "text": "title_issue_soft"
          },
          {
            "id": 103,
            "text": "seller_finance_casual"
          },
          {
            "id": 104,
            "text": "offer_reveal_casual"
          },
          {
            "id": 105,
            "text": "price_low_casual"
          },
          {
            "id": 106,
            "text": "close_ask_casual"
          },
          {
            "id": 107,
            "text": "persona_warm_professional_offer_reveal"
          },
          {
            "id": 108,
            "text": "persona_warm_professional_price_pushback"
          },
          {
            "id": 109,
            "text": "persona_warm_professional_followup"
          },
          {
            "id": 110,
            "text": "persona_warm_professional_close_ask"
          },
          {
            "id": 111,
            "text": "persona_no-nonsense_closer_offer_reveal"
          },
          {
            "id": 112,
            "text": "persona_no-nonsense_closer_price_pushback"
          },
          {
            "id": 113,
            "text": "persona_no-nonsense_closer_followup"
          },
          {
            "id": 114,
            "text": "persona_no-nonsense_closer_close_ask"
          },
          {
            "id": 115,
            "text": "persona_neighborly_offer_reveal"
          },
          {
            "id": 116,
            "text": "persona_neighborly_price_pushback"
          },
          {
            "id": 117,
            "text": "persona_neighborly_followup"
          },
          {
            "id": 118,
            "text": "persona_neighborly_close_ask"
          },
          {
            "id": 119,
            "text": "persona_empathetic_offer_reveal"
          },
          {
            "id": 120,
            "text": "persona_empathetic_price_pushback"
          },
          {
            "id": 121,
            "text": "persona_empathetic_followup"
          },
          {
            "id": 122,
            "text": "persona_empathetic_close_ask"
          },
          {
            "id": 123,
            "text": "persona_investor_direct_offer_reveal"
          },
          {
            "id": 124,
            "text": "persona_investor_direct_price_pushback"
          },
          {
            "id": 125,
            "text": "persona_investor_direct_followup"
          },
          {
            "id": 126,
            "text": "persona_investor_direct_close_ask"
          },
          {
            "id": 127,
            "text": "obj_warm_professional_not_interested"
          },
          {
            "id": 128,
            "text": "obj_warm_professional_already_listed"
          },
          {
            "id": 129,
            "text": "obj_warm_professional_need_more_money"
          },
          {
            "id": 130,
            "text": "obj_warm_professional_need_time"
          },
          {
            "id": 131,
            "text": "obj_warm_professional_need_family_ok"
          },
          {
            "id": 132,
            "text": "obj_warm_professional_send_offer_first"
          },
          {
            "id": 133,
            "text": "obj_warm_professional_who_is_this"
          },
          {
            "id": 134,
            "text": "obj_warm_professional_stop_texting"
          },
          {
            "id": 135,
            "text": "obj_warm_professional_tenant_issue"
          },
          {
            "id": 136,
            "text": "obj_warm_professional_condition_bad"
          },
          {
            "id": 137,
            "text": "obj_no-nonsense_closer_not_interested"
          },
          {
            "id": 138,
            "text": "obj_no-nonsense_closer_already_listed"
          },
          {
            "id": 139,
            "text": "obj_no-nonsense_closer_need_more_money"
          },
          {
            "id": 140,
            "text": "obj_no-nonsense_closer_need_time"
          },
          {
            "id": 141,
            "text": "obj_no-nonsense_closer_need_family_ok"
          },
          {
            "id": 142,
            "text": "obj_no-nonsense_closer_send_offer_first"
          },
          {
            "id": 143,
            "text": "obj_no-nonsense_closer_who_is_this"
          },
          {
            "id": 144,
            "text": "obj_no-nonsense_closer_stop_texting"
          },
          {
            "id": 145,
            "text": "obj_no-nonsense_closer_tenant_issue"
          },
          {
            "id": 146,
            "text": "obj_no-nonsense_closer_condition_bad"
          },
          {
            "id": 147,
            "text": "obj_neighborly_not_interested"
          },
          {
            "id": 148,
            "text": "obj_neighborly_already_listed"
          },
          {
            "id": 149,
            "text": "obj_neighborly_need_more_money"
          },
          {
            "id": 150,
            "text": "obj_neighborly_need_time"
          },
          {
            "id": 151,
            "text": "obj_neighborly_need_family_ok"
          },
          {
            "id": 152,
            "text": "obj_neighborly_send_offer_first"
          },
          {
            "id": 153,
            "text": "obj_neighborly_who_is_this"
          },
          {
            "id": 154,
            "text": "obj_neighborly_stop_texting"
          },
          {
            "id": 155,
            "text": "obj_neighborly_tenant_issue"
          },
          {
            "id": 156,
            "text": "obj_neighborly_condition_bad"
          },
          {
            "id": 157,
            "text": "obj_empathetic_not_interested"
          },
          {
            "id": 158,
            "text": "obj_empathetic_already_listed"
          },
          {
            "id": 159,
            "text": "obj_empathetic_need_more_money"
          },
          {
            "id": 160,
            "text": "obj_empathetic_need_time"
          },
          {
            "id": 161,
            "text": "obj_empathetic_need_family_ok"
          },
          {
            "id": 162,
            "text": "obj_empathetic_send_offer_first"
          },
          {
            "id": 163,
            "text": "obj_empathetic_who_is_this"
          },
          {
            "id": 164,
            "text": "obj_empathetic_stop_texting"
          },
          {
            "id": 165,
            "text": "obj_empathetic_tenant_issue"
          },
          {
            "id": 166,
            "text": "obj_empathetic_condition_bad"
          },
          {
            "id": 167,
            "text": "obj_investor_direct_not_interested"
          },
          {
            "id": 168,
            "text": "obj_investor_direct_already_listed"
          },
          {
            "id": 169,
            "text": "obj_investor_direct_need_more_money"
          },
          {
            "id": 170,
            "text": "obj_investor_direct_need_time"
          },
          {
            "id": 171,
            "text": "obj_investor_direct_need_family_ok"
          },
          {
            "id": 172,
            "text": "obj_investor_direct_send_offer_first"
          },
          {
            "id": 173,
            "text": "obj_investor_direct_who_is_this"
          },
          {
            "id": 174,
            "text": "obj_investor_direct_stop_texting"
          },
          {
            "id": 175,
            "text": "obj_investor_direct_tenant_issue"
          },
          {
            "id": 176,
            "text": "obj_investor_direct_condition_bad"
          },
          {
            "id": 177,
            "text": "emotion_warm_professional_calm"
          },
          {
            "id": 178,
            "text": "emotion_warm_professional_skeptical"
          },
          {
            "id": 179,
            "text": "emotion_warm_professional_guarded"
          },
          {
            "id": 180,
            "text": "emotion_warm_professional_frustrated"
          },
          {
            "id": 181,
            "text": "emotion_warm_professional_curious"
          },
          {
            "id": 182,
            "text": "emotion_warm_professional_motivated"
          },
          {
            "id": 183,
            "text": "emotion_warm_professional_tired_landlord"
          },
          {
            "id": 184,
            "text": "emotion_warm_professional_overwhelmed"
          },
          {
            "id": 185,
            "text": "emotion_no-nonsense_closer_calm"
          },
          {
            "id": 186,
            "text": "emotion_no-nonsense_closer_skeptical"
          },
          {
            "id": 187,
            "text": "emotion_no-nonsense_closer_guarded"
          },
          {
            "id": 188,
            "text": "emotion_no-nonsense_closer_frustrated"
          },
          {
            "id": 189,
            "text": "emotion_no-nonsense_closer_curious"
          },
          {
            "id": 190,
            "text": "emotion_no-nonsense_closer_motivated"
          },
          {
            "id": 191,
            "text": "emotion_no-nonsense_closer_tired_landlord"
          },
          {
            "id": 192,
            "text": "emotion_no-nonsense_closer_overwhelmed"
          },
          {
            "id": 193,
            "text": "emotion_neighborly_calm"
          },
          {
            "id": 194,
            "text": "emotion_neighborly_skeptical"
          },
          {
            "id": 195,
            "text": "emotion_neighborly_guarded"
          },
          {
            "id": 196,
            "text": "emotion_neighborly_frustrated"
          },
          {
            "id": 197,
            "text": "emotion_neighborly_curious"
          },
          {
            "id": 198,
            "text": "emotion_neighborly_motivated"
          },
          {
            "id": 199,
            "text": "emotion_neighborly_tired_landlord"
          },
          {
            "id": 200,
            "text": "emotion_neighborly_overwhelmed"
          }
        ]
      },
      "active": {
        "label": "Active?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "stage": {
        "label": "Variant Group",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Short Casual"
          },
          {
            "id": 2,
            "text": "Investor Context"
          },
          {
            "id": 3,
            "text": "Human Soft"
          },
          {
            "id": 6,
            "text": "Urgency"
          },
          {
            "id": 7,
            "text": "Curiosity Hook"
          },
          {
            "id": 8,
            "text": "Authority Play"
          },
          {
            "id": 9,
            "text": "Stage 1 — Ownership Confirmation"
          },
          {
            "id": 10,
            "text": "Stage 1 — Identity / Trust"
          },
          {
            "id": 11,
            "text": "Stage 2 — Soft Value Drop"
          },
          {
            "id": 12,
            "text": "Stage 3 — Offer Reveal"
          },
          {
            "id": 13,
            "text": "Objection — Not Ready"
          },
          {
            "id": 14,
            "text": "Objection — Price Too Low"
          },
          {
            "id": 15,
            "text": "Objection — Has Tenants"
          },
          {
            "id": 16,
            "text": "Objection — Already Listed"
          },
          {
            "id": 17,
            "text": "Objection — Already Have Someone"
          },
          {
            "id": 18,
            "text": "Objection — Need Family Discussion"
          },
          {
            "id": 19,
            "text": "Stage 3 — Offer Follow-Up"
          },
          {
            "id": 20,
            "text": "Soft Close"
          },
          {
            "id": 21,
            "text": "Stage 5 — Re-engagement"
          },
          {
            "id": 22,
            "text": "Stage 6 — Close / Handoff"
          },
          {
            "id": 23,
            "text": "Stage 6 — Contract Request"
          },
          {
            "id": 24,
            "text": "Stage 6 — Proof of Funds"
          },
          {
            "id": 25,
            "text": "Creative Finance Probe"
          },
          {
            "id": 26,
            "text": "Multifamily Underwrite — Units"
          },
          {
            "id": 27,
            "text": "Multifamily Underwrite — Occupancy / Rents"
          },
          {
            "id": 28,
            "text": "Stage 6 — Package Send"
          },
          {
            "id": 29,
            "text": "Landlord Pain Probe"
          },
          {
            "id": 30,
            "text": "Multifamily Underwrite — Occupancy"
          },
          {
            "id": 31,
            "text": "Multifamily Underwrite — Rents"
          },
          {
            "id": 32,
            "text": "Multifamily Underwrite — Expenses"
          },
          {
            "id": 33,
            "text": "Negotiation — Best Price"
          },
          {
            "id": 34,
            "text": "Stage 6 — Earnest Money"
          },
          {
            "id": 35,
            "text": "Stage 6 — Title / Closing"
          },
          {
            "id": 36,
            "text": "Stage 6 — Close Timing"
          },
          {
            "id": 37,
            "text": "Negotiation — Improve Offer"
          },
          {
            "id": 38,
            "text": "Negotiation — Seller Price"
          },
          {
            "id": 39,
            "text": "Creative Finance Follow-Up"
          },
          {
            "id": 40,
            "text": "Stage 4 — Offer No Response"
          },
          {
            "id": 41,
            "text": "Stage 6 — Condition / Walkthrough"
          },
          {
            "id": 42,
            "text": "Stage 6 — Contract Sent"
          },
          {
            "id": 43,
            "text": "Stage 6 — Contract Unsigned Follow-Up"
          },
          {
            "id": 44,
            "text": "Stage 6 — Contract Revision"
          },
          {
            "id": 45,
            "text": "Stage 6 — Title Intro"
          },
          {
            "id": 46,
            "text": "Stage 6 — Earnest Pending"
          },
          {
            "id": 47,
            "text": "Stage 6 — Earnest Sent"
          },
          {
            "id": 48,
            "text": "Stage 6 — Inspection / Walkthrough"
          },
          {
            "id": 49,
            "text": "Negotiation — Stalling After Yes"
          },
          {
            "id": 50,
            "text": "Negotiation — Need Spouse / Partner Signoff"
          },
          {
            "id": 51,
            "text": "Stage 6 — Closing Date Locked"
          },
          {
            "id": 52,
            "text": "Stage 6 — Closing Date Changed"
          },
          {
            "id": 53,
            "text": "Negotiation — Retrade / Repair Pushback"
          },
          {
            "id": 54,
            "text": "Stage 6 — Title Delay"
          },
          {
            "id": 55,
            "text": "Stage 6 — Seller Docs Needed"
          },
          {
            "id": 56,
            "text": "Stage 6 — Clear to Close"
          },
          {
            "id": 57,
            "text": "Stage 6 — Close Reminder"
          },
          {
            "id": 58,
            "text": "Post-Close / Referral"
          },
          {
            "id": 59,
            "text": "Disposition / Referral"
          },
          {
            "id": 60,
            "text": "SMS-Only Preference"
          },
          {
            "id": 61,
            "text": "SMS-Only Underwriting"
          },
          {
            "id": 62,
            "text": "Stage 6 — E-Sign Sent"
          },
          {
            "id": 63,
            "text": "Stage 6 — E-Sign Help"
          },
          {
            "id": 64,
            "text": "SMS-Only Docs Exchange"
          },
          {
            "id": 65,
            "text": "Identity / Trust"
          },
          {
            "id": 66,
            "text": "Negotiation — Lowball Pushback"
          },
          {
            "id": 67,
            "text": "Stage 6 — Title Issue"
          },
          {
            "id": 68,
            "text": "Probate / Sensitivity"
          },
          {
            "id": 69,
            "text": "Sensitive Situation"
          },
          {
            "id": 70,
            "text": "Distress / Timing"
          },
          {
            "id": 71,
            "text": "Hostile Reply"
          },
          {
            "id": 72,
            "text": "Wrong Number / Referral"
          },
          {
            "id": 73,
            "text": "Property Condition / Distress"
          },
          {
            "id": 74,
            "text": "Creative Finance / Terms"
          },
          {
            "id": 75,
            "text": "Re-engagement / Timing"
          },
          {
            "id": 76,
            "text": "Channel Shift"
          },
          {
            "id": 77,
            "text": "Negotiation — Price Too Low"
          },
          {
            "id": 78,
            "text": "Stage 4 — Offer Follow-Up"
          },
          {
            "id": 79,
            "text": "Objection — Not Interested"
          },
          {
            "id": 80,
            "text": "Objection — Need More Money"
          },
          {
            "id": 81,
            "text": "Objection — Need Time"
          },
          {
            "id": 82,
            "text": "Objection — Family Approval"
          },
          {
            "id": 83,
            "text": "Objection — Send Offer First"
          },
          {
            "id": 84,
            "text": "Objection — Stop / Opt Out"
          },
          {
            "id": 85,
            "text": "Objection — Tenant Issue"
          },
          {
            "id": 86,
            "text": "Objection — Bad Condition"
          },
          {
            "id": 87,
            "text": "Emotion — Calm"
          },
          {
            "id": 88,
            "text": "Emotion — Skeptical"
          },
          {
            "id": 89,
            "text": "Emotion — Guarded"
          },
          {
            "id": 90,
            "text": "Emotion — Frustrated"
          },
          {
            "id": 91,
            "text": "Emotion — Curious"
          },
          {
            "id": 92,
            "text": "Emotion — Motivated"
          },
          {
            "id": 93,
            "text": "Emotion — Tired Landlord"
          },
          {
            "id": 94,
            "text": "Emotion — Overwhelmed"
          }
        ]
      },
      "tone": {
        "label": "Tone",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Casual"
          },
          {
            "id": 2,
            "text": "Direct"
          },
          {
            "id": 3,
            "text": "Soft"
          },
          {
            "id": 4,
            "text": "Professional"
          },
          {
            "id": 5,
            "text": "Warm"
          },
          {
            "id": 6,
            "text": "Neutral"
          },
          {
            "id": 7,
            "text": "Calm"
          },
          {
            "id": 8,
            "text": "Corporate"
          }
        ]
      },
      "gender-variant": {
        "label": "Gender Variant",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Masculine"
          },
          {
            "id": 2,
            "text": "Feminine"
          },
          {
            "id": 3,
            "text": "Neutral"
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
          }
        ]
      },
      "sequence-position": {
        "label": "Sequence Position",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "1st Touch"
          },
          {
            "id": 2,
            "text": "2nd Touch"
          },
          {
            "id": 3,
            "text": "3rd Touch"
          },
          {
            "id": 4,
            "text": "4th Touch"
          },
          {
            "id": 5,
            "text": "Final"
          },
          {
            "id": 6,
            "text": "V1"
          },
          {
            "id": 7,
            "text": "V2"
          },
          {
            "id": 8,
            "text": "V3"
          }
        ]
      },
      "paired-with-agent-type": {
        "label": "Paired With Agent Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "High Energy"
          },
          {
            "id": 2,
            "text": "Empathetic"
          },
          {
            "id": 3,
            "text": "Authoritative"
          },
          {
            "id": 4,
            "text": "Friendly"
          },
          {
            "id": 5,
            "text": "Fallback / Market-Local"
          },
          {
            "id": 6,
            "text": "Specialist-Spanish / Market-Local"
          },
          {
            "id": 7,
            "text": "Specialist-Probate"
          },
          {
            "id": 8,
            "text": "Specialist-Probate-Spanish"
          },
          {
            "id": 9,
            "text": "Specialist-Corporate"
          },
          {
            "id": 10,
            "text": "Specialist-Corporate-Spanish"
          },
          {
            "id": 11,
            "text": "Specialist-Landlord / Market-Local"
          },
          {
            "id": 12,
            "text": "Specialist-Landlord / Specialist-Spanish"
          },
          {
            "id": 13,
            "text": "Specialist-Portuguese / Specialist-Portuguese-Corporate"
          },
          {
            "id": 14,
            "text": "Specialist-Italian / Specialist-Italian-Family"
          },
          {
            "id": 15,
            "text": "Specialist-Hebrew"
          },
          {
            "id": 16,
            "text": "Specialist-Mandarin"
          },
          {
            "id": 17,
            "text": "Specialist-Korean"
          },
          {
            "id": 18,
            "text": "Specialist-Vietnamese"
          },
          {
            "id": 19,
            "text": "Specialist-Polish"
          },
          {
            "id": 20,
            "text": "Fallback / Market-Local / Specialist-Close"
          },
          {
            "id": 21,
            "text": "Specialist-Spanish / Specialist-Close"
          },
          {
            "id": 22,
            "text": "Specialist-Portuguese / Specialist-Close"
          },
          {
            "id": 23,
            "text": "Specialist-Italian / Specialist-Close"
          },
          {
            "id": 24,
            "text": "Specialist-Hebrew / Specialist-Close"
          },
          {
            "id": 25,
            "text": "Specialist-Mandarin / Specialist-Close"
          },
          {
            "id": 26,
            "text": "Specialist-Korean / Specialist-Close"
          },
          {
            "id": 27,
            "text": "Specialist-Vietnamese / Specialist-Close"
          },
          {
            "id": 28,
            "text": "Specialist-Polish / Specialist-Close"
          },
          {
            "id": 29,
            "text": "Fallback / Market-Local / Specialist-Close / Specialist-Probate"
          },
          {
            "id": 30,
            "text": "Soft Closer / Hard Closer / Ultra-Short"
          },
          {
            "id": 31,
            "text": "Suave / Directo / Corto"
          },
          {
            "id": 32,
            "text": "Casual / Closer"
          },
          {
            "id": 33,
            "text": "Warm Professional"
          },
          {
            "id": 34,
            "text": "No-Nonsense Closer"
          },
          {
            "id": 35,
            "text": "Neighborly"
          },
          {
            "id": 36,
            "text": "Investor Direct"
          }
        ]
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "text": {
        "label": "Template Text",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "english-translation": {
        "label": "English Translation",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "is-ownership-check": {
        "label": "Is Ownership Check",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "character-count": {
        "label": "Character Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "segment-count": {
        "label": "Segment Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "personalization-tags": {
        "label": "Personalization Tags",
        "type": "category",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "{{owner_name}}"
          },
          {
            "id": 2,
            "text": "{{property_address}}"
          },
          {
            "id": 3,
            "text": "{{agent_name}}"
          },
          {
            "id": 4,
            "text": "{{market}}"
          },
          {
            "id": 5,
            "text": "{{city}}"
          },
          {
            "id": 6,
            "text": "{{first_name}}"
          },
          {
            "id": 7,
            "text": "first_name"
          },
          {
            "id": 8,
            "text": "property_address"
          },
          {
            "id": 9,
            "text": "agent_first_name"
          },
          {
            "id": 10,
            "text": "smart_cash_offer_display"
          },
          {
            "id": 11,
            "text": "none"
          },
          {
            "id": 12,
            "text": "units"
          },
          {
            "id": 13,
            "text": "contact_name"
          },
          {
            "id": 14,
            "text": "closing_date"
          }
        ]
      },
      "category": {
        "label": "Category",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "✅ Yes"
          },
          {
            "id": 2,
            "text": "❌ No"
          },
          {
            "id": 3,
            "text": "Outbound Initial"
          },
          {
            "id": 4,
            "text": "Inbound Reply"
          },
          {
            "id": 5,
            "text": "Outbound Follow-Up"
          },
          {
            "id": 6,
            "text": "Offer"
          },
          {
            "id": 7,
            "text": "Objection Handling"
          },
          {
            "id": 8,
            "text": "Close / Park"
          },
          {
            "id": 9,
            "text": "Re-engagement"
          },
          {
            "id": 10,
            "text": "Close / Handoff"
          },
          {
            "id": 11,
            "text": "Negotiation"
          },
          {
            "id": 12,
            "text": "Underwriting"
          },
          {
            "id": 13,
            "text": "Offer Follow-Up"
          },
          {
            "id": 14,
            "text": "Post-Close"
          },
          {
            "id": 15,
            "text": "Identity / Trust"
          },
          {
            "id": 16,
            "text": "Sensitive Situation"
          },
          {
            "id": 17,
            "text": "Distress"
          },
          {
            "id": 18,
            "text": "Compliance"
          },
          {
            "id": 19,
            "text": "Emotion Routing"
          }
        ]
      },
      "section-separator": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "historical-reply-rate": {
        "label": "Historical Reply Rate",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "deliverability-score": {
        "label": "Deliverability Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "spam-risk": {
        "label": "Spam Risk",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-sends": {
        "label": "Total Sends",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-replies": {
        "label": "Total Replies",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-conversations": {
        "label": "Total Conversations",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "conversation-rate": {
        "label": "Conversation Rate",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ab-test-winner": {
        "label": "A/B Test Winner?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "_ Winner"
          },
          {
            "id": 2,
            "text": "_ Testing"
          },
          {
            "id": 3,
            "text": "❌ Retired"
          }
        ]
      },
      "last-used": {
        "label": "Last Used",
        "type": "date",
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
      "cooldown-days": {
        "label": "Cooldown Days",
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
      },
      "date-created": {
        "label": "Date Created",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "date-modified": {
        "label": "Date Modified",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "version": {
        "label": "Version",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30658310": {
    "app_id": 30658310,
    "app_name": "Phone Numbers",
    "item_name": "Number",
    "fields": {
      "phone-full-name": {
        "label": "Phone Full Name",
        "type": "text",
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
      "phone-first-name": {
        "label": "Phone First Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "phone": {
        "label": "Phone",
        "type": "phone",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "phone-hidden": {
        "label": "Phone (HIDDEN)",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "canonical-e164": {
        "label": "Canonical E.164",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "primary-display-name": {
        "label": "Primary Display Name",
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
      "linked-master-owner": {
        "label": "Linked Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "linked-owner": {
        "label": "Linked Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637059,
          30644240
        ],
        "options": []
      },
      "linked-contact": {
        "label": "Linked Contact",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173,
          30644237,
          30644727
        ],
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
            "text": "#6 [0](1)"
          },
          {
            "id": 2,
            "text": "#6 [0](2)"
          },
          {
            "id": 3,
            "text": "#6 [0](3)"
          },
          {
            "id": 4,
            "text": "#6 [1](1)"
          },
          {
            "id": 5,
            "text": "#6 [1](2)"
          },
          {
            "id": 6,
            "text": "[1](2)"
          },
          {
            "id": 7,
            "text": "[2](1)"
          },
          {
            "id": 8,
            "text": "[2](2)"
          },
          {
            "id": 9,
            "text": "#6 [2](3)"
          },
          {
            "id": 10,
            "text": "#1 [0](1)"
          },
          {
            "id": 11,
            "text": "#1 [0](2)"
          },
          {
            "id": 12,
            "text": "#1 [0](3)"
          },
          {
            "id": 13,
            "text": "#1 [1](1)"
          },
          {
            "id": 14,
            "text": "#1 [1](2)"
          },
          {
            "id": 15,
            "text": "#1[0](1)"
          },
          {
            "id": 16,
            "text": "#1[0](2)"
          },
          {
            "id": 17,
            "text": "#1 [1](3)"
          },
          {
            "id": 18,
            "text": "#1 [2](1)"
          },
          {
            "id": 19,
            "text": "#1 [2](2)"
          },
          {
            "id": 20,
            "text": "#1 [2](3)"
          },
          {
            "id": 21,
            "text": "#2 [0](1)"
          },
          {
            "id": 22,
            "text": "#2 [0](2)"
          },
          {
            "id": 23,
            "text": "#2 [0](3)"
          },
          {
            "id": 24,
            "text": "#2 [1](1)"
          },
          {
            "id": 25,
            "text": "#2 [1](2)"
          },
          {
            "id": 26,
            "text": "#2 [1](3)"
          },
          {
            "id": 27,
            "text": "#2 [2](1)"
          },
          {
            "id": 28,
            "text": "#2 [2](2)"
          },
          {
            "id": 29,
            "text": "#2 [2](3)"
          },
          {
            "id": 30,
            "text": "#3 [0](2)"
          },
          {
            "id": 31,
            "text": "#3 [0](1)"
          },
          {
            "id": 32,
            "text": "#3 [0](3)"
          },
          {
            "id": 33,
            "text": "#3 [1](1)"
          },
          {
            "id": 34,
            "text": "#3 [1](2)"
          },
          {
            "id": 35,
            "text": "#3 [2](1)"
          },
          {
            "id": 36,
            "text": "#3 [2](2)"
          },
          {
            "id": 37,
            "text": "#3 [2](3)"
          },
          {
            "id": 38,
            "text": "#4 [0](1)"
          },
          {
            "id": 39,
            "text": "#4 [0](2)"
          },
          {
            "id": 40,
            "text": "#4 [0](3)"
          },
          {
            "id": 41,
            "text": "#5 [0](1)"
          },
          {
            "id": 42,
            "text": "#5 [0](2)"
          },
          {
            "id": 43,
            "text": "#5 [0](3)"
          },
          {
            "id": 44,
            "text": "#5 [1](1)"
          },
          {
            "id": 45,
            "text": "#5 [1](2)"
          },
          {
            "id": 46,
            "text": "#5 [2](1)"
          },
          {
            "id": 47,
            "text": "#5 [1](3)"
          },
          {
            "id": 48,
            "text": "#5 [2](2)"
          },
          {
            "id": 49,
            "text": "#5 [2](3)"
          }
        ]
      },
      "primary-property": {
        "label": "Primary Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contact-type": {
        "label": "Contact Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Seller"
          },
          {
            "id": 2,
            "text": "Buyer"
          },
          {
            "id": 3,
            "text": "Title Company"
          }
        ]
      },
      "phone-role": {
        "label": "Phone Role",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Primary"
          },
          {
            "id": 2,
            "text": "Secondary"
          },
          {
            "id": 3,
            "text": "Mobile"
          },
          {
            "id": 4,
            "text": "Landline"
          },
          {
            "id": 5,
            "text": "VOIP"
          },
          {
            "id": 6,
            "text": "Spam"
          }
        ]
      },
      "phone-type": {
        "label": "Phone Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 4,
            "text": "Landline"
          },
          {
            "id": 5,
            "text": "Wireless"
          }
        ]
      },
      "phone-carrier": {
        "label": "Phone Carrier",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Frontier Communications"
          },
          {
            "id": 2,
            "text": "BHNIS"
          },
          {
            "id": 3,
            "text": "Verizon Wireless"
          },
          {
            "id": 4,
            "text": "T-Mobile"
          },
          {
            "id": 5,
            "text": "AT&T Mobility"
          },
          {
            "id": 6,
            "text": "Level3"
          },
          {
            "id": 7,
            "text": "Time Warner Cable"
          },
          {
            "id": 8,
            "text": "Metro PCS"
          },
          {
            "id": 9,
            "text": "Embarq Communications"
          },
          {
            "id": 10,
            "text": "Verizon"
          },
          {
            "id": 11,
            "text": "AT&T Local"
          },
          {
            "id": 12,
            "text": "Verizon Business"
          },
          {
            "id": 13,
            "text": "TERRA NOVA TELECOM"
          },
          {
            "id": 14,
            "text": "Comcast"
          },
          {
            "id": 15,
            "text": "IP HORIZON LLC"
          },
          {
            "id": 16,
            "text": "Bristol TN Essential Svcs"
          },
          {
            "id": 17,
            "text": "Cablevision Lightpath"
          },
          {
            "id": 18,
            "text": "Onvoy (Emergency Networks\tLLC)"
          },
          {
            "id": 19,
            "text": "Consolidated Communications Ne"
          },
          {
            "id": 20,
            "text": "Neutral Tandem"
          },
          {
            "id": 21,
            "text": "Charter Fiberlink"
          },
          {
            "id": 29,
            "text": "Xspedius"
          },
          {
            "id": 30,
            "text": "Cincinnati Bell"
          },
          {
            "id": 31,
            "text": "AT&T IP"
          },
          {
            "id": 32,
            "text": "Peerless Network"
          },
          {
            "id": 33,
            "text": "CenturyLink"
          },
          {
            "id": 34,
            "text": "CLARO Puerto Rico"
          },
          {
            "id": 35,
            "text": "COMMIO\tLLC"
          },
          {
            "id": 36,
            "text": "North Pittsburgh Telephone"
          },
          {
            "id": 37,
            "text": "Coretel"
          },
          {
            "id": 38,
            "text": "Massillon Cable TV"
          },
          {
            "id": 39,
            "text": "Troy Cablevision"
          },
          {
            "id": 40,
            "text": "Sonic Telecom"
          },
          {
            "id": 41,
            "text": "RCN Telecom Services"
          },
          {
            "id": 42,
            "text": "TelePacific"
          },
          {
            "id": 43,
            "text": "US Cellular"
          },
          {
            "id": 44,
            "text": "Adir International Export dba"
          },
          {
            "id": 45,
            "text": "Utility Telephone"
          },
          {
            "id": 46,
            "text": "AT&T TCG"
          },
          {
            "id": 47,
            "text": "Silver Strand Enterprises"
          },
          {
            "id": 48,
            "text": "ConnectTo Communications"
          },
          {
            "id": 49,
            "text": "Paetec Communications"
          },
          {
            "id": 50,
            "text": "VOIPSTREET\tINC."
          },
          {
            "id": 51,
            "text": "TELNYX LLC"
          },
          {
            "id": 52,
            "text": "DMR Communications"
          },
          {
            "id": 53,
            "text": "Electric Power Board of Chatta"
          },
          {
            "id": 54,
            "text": "Fractel"
          },
          {
            "id": 55,
            "text": "Smart City Solutions"
          },
          {
            "id": 56,
            "text": "Orlando Telephone Company"
          },
          {
            "id": 57,
            "text": "Armstrong Telecommunications"
          },
          {
            "id": 58,
            "text": "CSC WIRELESS"
          },
          {
            "id": 59,
            "text": "SKYE TELECOM LLC DBA SKYETEL"
          },
          {
            "id": 60,
            "text": "Bluffton Telephone Company"
          },
          {
            "id": 61,
            "text": "Home Town Telephone"
          },
          {
            "id": 62,
            "text": "Vernon Communications"
          },
          {
            "id": 63,
            "text": "IDT"
          },
          {
            "id": 64,
            "text": "Ymax Communications"
          },
          {
            "id": 65,
            "text": "Skyline Telephone Membership C"
          },
          {
            "id": 66,
            "text": "Florida Digital Network"
          },
          {
            "id": 67,
            "text": "HD CARRIER LLC"
          },
          {
            "id": 68,
            "text": "Shenandoah Telephone Company"
          },
          {
            "id": 69,
            "text": "TDS Telecommunications"
          },
          {
            "id": 70,
            "text": "Liberty Puerto Rico"
          },
          {
            "id": 71,
            "text": "Valor Telecommunications CLEC"
          },
          {
            "id": 72,
            "text": "Time Warner Communications"
          },
          {
            "id": 73,
            "text": "PRT Communications"
          },
          {
            "id": 74,
            "text": "Illinois Consolidated Tel Comp"
          },
          {
            "id": 75,
            "text": "Farmers Tel Company"
          },
          {
            "id": 76,
            "text": "PBT Communications"
          },
          {
            "id": 77,
            "text": "GCI Communication Corp. dba Ge"
          },
          {
            "id": 78,
            "text": "C Spire"
          },
          {
            "id": 79,
            "text": "A7"
          },
          {
            "id": 80,
            "text": "A1"
          },
          {
            "id": 81,
            "text": "Sigecom"
          },
          {
            "id": 82,
            "text": "I2"
          },
          {
            "id": 83,
            "text": "DISH WIRELESS Frontier Communications Frontier Communications Verizon Wireless Frontier Communications BHNIS T-Mobile DISH WIRELESS"
          },
          {
            "id": 84,
            "text": "Liberty Cablevision Of Puerto"
          },
          {
            "id": 85,
            "text": "GTC Telecom Corporation"
          },
          {
            "id": 86,
            "text": "I5"
          },
          {
            "id": 87,
            "text": "EXIANT COMMUNICATIONS LLC"
          },
          {
            "id": 88,
            "text": "Lexcom Telco"
          },
          {
            "id": 89,
            "text": "Horry Telephone Cooperative"
          },
          {
            "id": 90,
            "text": "I3"
          },
          {
            "id": 91,
            "text": "I1"
          },
          {
            "id": 92,
            "text": "Network Services"
          },
          {
            "id": 93,
            "text": "Mankato Citizen's Telephone Co"
          },
          {
            "id": 94,
            "text": "Astound Broadband"
          },
          {
            "id": 95,
            "text": "TCA COMMUNICATIONS T-Mobile T-Mobile AT&T Mobility AT&T Mobility T-Mobile T-Mobile AT&T Mobility Verizon Wireless AT&T Mobility Verizon Wireless Verizon Wireless T-Mobile AT&T Local Verizon Frontier Communications AT&T Local AT&T Local T-Mobile T-Mobile T-Mobile AT&T Local Verizon Wireless Skyline Telephone Membership C AT&T Local AT&T Local AT&T Mobility AT&T Local AT&T Local Verizon Wireless AT&T Local AT&T IP AT&T Local COMMIO"
          },
          {
            "id": 96,
            "text": "Bulloch County Rural Telephone"
          },
          {
            "id": 97,
            "text": "Reservation Telephone Cooperat"
          },
          {
            "id": 98,
            "text": "I4"
          },
          {
            "id": 99,
            "text": "Comporium Inc"
          },
          {
            "id": 100,
            "text": "American Messaging (Am)"
          },
          {
            "id": 101,
            "text": "Union Springs Telephone Co."
          },
          {
            "id": 102,
            "text": "Spok T-Mobile Comcast AT&T Mobility AT&T Local T-Mobile AT&T Local Verizon Comcast Metro PCS Metro PCS AT&T Local T-Mobile Frontier Communications T-Mobile AT&T Local Comcast AT&T Local AT&T Local AT&T Local AT&T Local T-Mobile AT&T Local Level3 AT&T Mobility Verizon AT&T Mobility AT&T Local AT&T Local T-Mobile Spok"
          },
          {
            "id": 103,
            "text": "Spok AT&T Local T-Mobile Comcast AT&T Mobility T-Mobile AT&T Local AT&T Local T-Mobile NUSO"
          },
          {
            "id": 104,
            "text": "VOIPSTREET AT&T Local AT&T Local AT&T Local AT&T Local Embarq Communications AT&T Mobility AT&T Local AT&T Mobility Verizon Wireless Verizon T-Mobile AT&T Local T-Mobile T-Mobile AT&T Mobility AT&T Local AT&T Local Metro PCS T-Mobile T-Mobile T-Mobile Neutral Tandem AT&T IP AT&T Local AT&T Local Comcast Comcast AT&T Mobility Level3 AT&T Mobility Comcast AT&T Local AT&T Local Spok"
          },
          {
            "id": 105,
            "text": "Live Wire Networks"
          },
          {
            "id": 106,
            "text": "DISH WIRELESS AT&T Local DISH WIRELESS"
          },
          {
            "id": 107,
            "text": "Spok Comcast AT&T Mobility Comcast Comcast AT&T Mobility Frontier Communications T-Mobile AT&T Mobility AT&T Local T-Mobile AT&T Local AT&T Local Comcast AT&T Local Verizon Wireless Verizon Wireless T-Mobile AT&T Local AT&T Mobility AT&T Local AT&T Mobility AT&T Mobility AT&T Local DISH WIRELESS"
          },
          {
            "id": 108,
            "text": "RT Communications"
          },
          {
            "id": 109,
            "text": "Onvoy (Emergency Networks AT&T Local T-Mobile AT&T Local AT&T Local AT&T Local AT&T Local AT&T IP AT&T Local Verizon Wireless Neutral Tandem T-Mobile AT&T Local Verizon Wireless Neutral Tandem AT&T Local AT&T Local AT&T Local T-Mobile AT&T Mobility Verizon Wireless Level3 AT&T Mobility Embarq Communications AT&T Local Comcast Comcast AT&T Local AT&T Local AT&T Local AT&T Local T-Mobile AT&T Local AT&T Local Spok"
          },
          {
            "id": 110,
            "text": "CEBRIDGE TELECOM OH AT&T Local AT&T Local T-Mobile Time Warner Cable T-Mobile AT&T Local AT&T Mobility T-Mobile Metro PCS AT&T Local AT&T Local Neutral Tandem AT&T Local AT&T Mobility AT&T IP AT&T Local AT&T Local AT&T Local T-Mobile Comcast Comcast AT&T Local Comcast Verizon Wireless AT&T Local T-Mobile AT&T Local T-Mobile Peerless Network AT&T Local Frontier Communications AT&T Mobility T-Mobile AT&T Local AT&T Local AT&T Local Verizon Wireless Onvoy (Emergency Networks"
          },
          {
            "id": 111,
            "text": "Ragland Telephone Company"
          },
          {
            "id": 112,
            "text": "Dish Wireless"
          },
          {
            "id": 113,
            "text": "WOW! INTERNET"
          },
          {
            "id": 114,
            "text": "Onvoy (Emergency Networks"
          },
          {
            "id": 115,
            "text": "Randolph Telephone Membership"
          },
          {
            "id": 116,
            "text": "Brindlee Mountain Telephone Co"
          },
          {
            "id": 117,
            "text": "Saddleback Communications Comp"
          },
          {
            "id": 118,
            "text": "Valley Telephone Cooperative"
          },
          {
            "id": 119,
            "text": "ALASKA COMMUNICATIONS SYSTEMS"
          },
          {
            "id": 120,
            "text": "Interlink Advertising Services"
          },
          {
            "id": 121,
            "text": "Spok"
          },
          {
            "id": 122,
            "text": "Table Top Telephone Co"
          },
          {
            "id": 123,
            "text": "Kerman Telephone Company"
          },
          {
            "id": 124,
            "text": "Central Texas Telephone Cooper"
          },
          {
            "id": 125,
            "text": "Ducor Telephone Company"
          },
          {
            "id": 126,
            "text": "CEBRIDGE TELECOM OH"
          },
          {
            "id": 127,
            "text": "Grande Communications ClearSou"
          },
          {
            "id": 128,
            "text": "Venture Communications Coopera"
          },
          {
            "id": 129,
            "text": "SureWest Telephone"
          },
          {
            "id": 130,
            "text": "Millington Telephone Co"
          },
          {
            "id": 131,
            "text": "Iowa Telecommunications Servic"
          },
          {
            "id": 132,
            "text": "Bresnan Communications"
          },
          {
            "id": 133,
            "text": "Hawaiian Telcom Services Compa"
          },
          {
            "id": 134,
            "text": "City of Ketchikan d.b.a. K P U"
          },
          {
            "id": 135,
            "text": "Plateau Telecommunications"
          },
          {
            "id": 136,
            "text": "North Dakota Telephone"
          },
          {
            "id": 137,
            "text": "Midcontinent Communications"
          },
          {
            "id": 138,
            "text": "Northeast Florida Tele Co"
          },
          {
            "id": 139,
            "text": "Plant Telephone Company"
          },
          {
            "id": 140,
            "text": "ITS Telecommunications Syst"
          },
          {
            "id": 141,
            "text": "Reserve Telecom"
          },
          {
            "id": 142,
            "text": "Eatel"
          },
          {
            "id": 143,
            "text": "Hunt Telecommunications dba Hu"
          },
          {
            "id": 144,
            "text": "CP-Tel Network Services"
          },
          {
            "id": 145,
            "text": "NUSO"
          },
          {
            "id": 146,
            "text": "Lafayette City Parish Consolid"
          },
          {
            "id": 147,
            "text": "Atlantic Telephone Membership"
          },
          {
            "id": 148,
            "text": "USFON"
          },
          {
            "id": 149,
            "text": "Santa Rosa Telephone Cooperati"
          },
          {
            "id": 150,
            "text": "INTEGRATED PATH COMMUNICATION"
          },
          {
            "id": 151,
            "text": "Etex Communications dba Etex W"
          },
          {
            "id": 152,
            "text": "Consolidated Communications of"
          },
          {
            "id": 153,
            "text": "NTS Communications"
          },
          {
            "id": 154,
            "text": "Consolidated Comm of Fort Bend"
          },
          {
            "id": 155,
            "text": "Cellular One of NE Arizona"
          },
          {
            "id": 156,
            "text": "Cumby Telephone Cooperative"
          },
          {
            "id": 157,
            "text": "Brandenburg Telephone Company"
          },
          {
            "id": 158,
            "text": "United Wireless Communications"
          },
          {
            "id": 159,
            "text": "VOIPSTREET"
          },
          {
            "id": 160,
            "text": "Cap Rock Telephone Cooperative"
          },
          {
            "id": 161,
            "text": "Taconic Tel Corporation"
          },
          {
            "id": 162,
            "text": "Otelco Telephone"
          },
          {
            "id": 163,
            "text": "Millry Telephone Company"
          },
          {
            "id": 164,
            "text": "New Hope Telephone Coop"
          },
          {
            "id": 165,
            "text": "COMMIO"
          },
          {
            "id": 166,
            "text": "Integra Telecom"
          },
          {
            "id": 167,
            "text": "Long Ln Siouxland"
          },
          {
            "id": 168,
            "text": "Clear Rate Communications"
          },
          {
            "id": 169,
            "text": "Home Telephone Company"
          },
          {
            "id": 170,
            "text": "Hargray"
          },
          {
            "id": 171,
            "text": "A6"
          },
          {
            "id": 172,
            "text": "Stayton Cooperative Telephone"
          },
          {
            "id": 173,
            "text": "Whidbey Telephone Company"
          },
          {
            "id": 174,
            "text": "Ellijay Tel. Co."
          },
          {
            "id": 175,
            "text": "All West Communications"
          },
          {
            "id": 176,
            "text": "WTC Communications"
          },
          {
            "id": 177,
            "text": "A2"
          },
          {
            "id": 178,
            "text": "Eastex Telephone Cooperative"
          },
          {
            "id": 179,
            "text": "COMMUNICATION"
          },
          {
            "id": 180,
            "text": "Hill Country Tel Coop"
          },
          {
            "id": 181,
            "text": "South Carolina Net"
          },
          {
            "id": 182,
            "text": "Peoples Telephone Coop. Inc"
          },
          {
            "id": 183,
            "text": "ETS Telephone Company"
          },
          {
            "id": 184,
            "text": "Smithville Telephone Company"
          },
          {
            "id": 185,
            "text": "Lipan Telephone Co."
          },
          {
            "id": 186,
            "text": "The Ponderosa Telephone Co."
          },
          {
            "id": 187,
            "text": "A4"
          },
          {
            "id": 188,
            "text": "North State Telephone Company"
          },
          {
            "id": 189,
            "text": "Worldnet Telecommunications"
          },
          {
            "id": 190,
            "text": "Hancock Communications"
          },
          {
            "id": 191,
            "text": "Bixby Telephone Company"
          },
          {
            "id": 192,
            "text": "Comanche County Telephone Comp"
          },
          {
            "id": 193,
            "text": "Interstate"
          },
          {
            "id": 194,
            "text": "Blountsville Telephone Company"
          },
          {
            "id": 195,
            "text": "Pine Belt Telephone"
          },
          {
            "id": 196,
            "text": "Sierra Telephone Company"
          },
          {
            "id": 197,
            "text": "Mountain Rural Tel. Coop. Corp"
          },
          {
            "id": 198,
            "text": "Madison Tel Co"
          },
          {
            "id": 199,
            "text": "Socket Telecom"
          },
          {
            "id": 200,
            "text": "West Central Telephone Associa"
          },
          {
            "id": 201,
            "text": "Dell Tel. Corp."
          },
          {
            "id": 202,
            "text": "Pine Tree Telephone"
          },
          {
            "id": 203,
            "text": "Vantage Telecom"
          },
          {
            "id": 204,
            "text": "Race Telecommunications"
          },
          {
            "id": 205,
            "text": "Cheyenne River Sioux Tribal Te"
          },
          {
            "id": 206,
            "text": "Cox Communications"
          },
          {
            "id": 207,
            "text": "A3"
          }
        ]
      },
      "phone-prepaid-indicator": {
        "label": "Phone Prepaid Indicator",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "False"
          },
          {
            "id": 2,
            "text": "True"
          }
        ]
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "do-not-call": {
        "label": "Do Not Call",
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
      "dnc-source": {
        "label": "DNC Source",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Federal DNC"
          },
          {
            "id": 2,
            "text": "Internal Opt-Out"
          },
          {
            "id": 3,
            "text": "Carrier Flag"
          },
          {
            "id": 4,
            "text": "Litigator Match"
          }
        ]
      },
      "opt-out-date": {
        "label": "Opt-Out Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-compliance-check": {
        "label": "Last Compliance Check",
        "type": "date",
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
      "total-messages-sent": {
        "label": "Total Messages Sent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-replies": {
        "label": "Total Replies",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-reply-date": {
        "label": "Last Reply Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "phone-activity-status": {
        "label": "Phone Activity Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Active for 12 months or longer"
          },
          {
            "id": 2,
            "text": "Inactive monthly for 10-11 months"
          },
          {
            "id": 3,
            "text": "Active monthly for 10-11 months"
          },
          {
            "id": 4,
            "text": "Inactive for 1 month or less"
          },
          {
            "id": 5,
            "text": "Inactive monthly for 7-9 months"
          },
          {
            "id": 6,
            "text": "Inactive monthly for 3 months"
          },
          {
            "id": 7,
            "text": "Inactive monthly for 2 months"
          },
          {
            "id": 8,
            "text": "Active monthly for 4-6 months"
          },
          {
            "id": 9,
            "text": "Active for 1 month or less"
          },
          {
            "id": 10,
            "text": "Unknown"
          },
          {
            "id": 11,
            "text": "Active monthly for 2 months"
          },
          {
            "id": 12,
            "text": "Active monthly for 7-9 months"
          },
          {
            "id": 13,
            "text": "Inactive monthly for 4-6 months"
          },
          {
            "id": 14,
            "text": "Active monthly for 3 months"
          }
        ]
      },
      "phone-usage-2-months": {
        "label": "Phone Usage (2 Months)",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Light Usage"
          },
          {
            "id": 2,
            "text": "No data available or no usage in the last 2 months"
          },
          {
            "id": 3,
            "text": "Minimal Usage"
          },
          {
            "id": 4,
            "text": "Moderate Usage"
          },
          {
            "id": 5,
            "text": "Heavy Usage"
          },
          {
            "id": 6,
            "text": "Very Heavy Usage"
          }
        ]
      },
      "phone-usage-12-months": {
        "label": "Phone Usage (12 Months)",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Moderate Usage"
          },
          {
            "id": 2,
            "text": "No data available or no usage in the last 2 months"
          },
          {
            "id": 3,
            "text": "Light Usage"
          },
          {
            "id": 4,
            "text": "Very Heavy Usage"
          },
          {
            "id": 5,
            "text": "Heavy Usage"
          },
          {
            "id": 6,
            "text": "Minimal Usage"
          }
        ]
      },
      "engagement-tier": {
        "label": "Engagement Tier",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Hot"
          },
          {
            "id": 2,
            "text": "Warm"
          },
          {
            "id": 3,
            "text": "Cold"
          },
          {
            "id": 4,
            "text": "Dead"
          }
        ]
      },
      "tags": {
        "label": "Tags",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Wrong Number"
          },
          {
            "id": 2,
            "text": "Tenant"
          },
          {
            "id": 3,
            "text": "Relative"
          },
          {
            "id": 4,
            "text": "Gatekeeper"
          },
          {
            "id": 5,
            "text": "Business Line"
          }
        ]
      },
      "linked-prospects": {
        "label": "Linked Messages",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30541680
        ],
        "options": []
      },
      "linked-owner-2": {
        "label": "Linked Conversations",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30678167": {
    "app_id": 30678167,
    "app_name": "Agents",
    "item_name": "Agent",
    "fields": {
      "title": {
        "label": "Agent Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "agent-id": {
        "label": "Agent ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "section-separator-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "first-name": {
        "label": "First Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
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
          },
          {
            "id": 3,
            "text": "Neutral"
          },
          {
            "id": 4,
            "text": "M"
          },
          {
            "id": 5,
            "text": "F"
          }
        ]
      },
      "age-range": {
        "label": "Age Range",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "35-44"
          },
          {
            "id": 2,
            "text": "45-54"
          },
          {
            "id": 3,
            "text": "25-34"
          }
        ]
      },
      "backstory": {
        "label": "Backstory",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "category": {
        "label": "Family",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Probate"
          },
          {
            "id": 2,
            "text": "Corporate"
          },
          {
            "id": 3,
            "text": "Spanish Local"
          },
          {
            "id": 4,
            "text": "Mandarin"
          },
          {
            "id": 5,
            "text": "Single"
          },
          {
            "id": 6,
            "text": "Married, 1 kid"
          },
          {
            "id": 7,
            "text": "Married"
          },
          {
            "id": 8,
            "text": "Married, no kids"
          },
          {
            "id": 9,
            "text": "Married, 2 kids"
          },
          {
            "id": 10,
            "text": "Partnered"
          },
          {
            "id": 11,
            "text": "Single, 1 kid"
          }
        ]
      },
      "text": {
        "label": "Archetype",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "category-2": {
        "label": "Active",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Active"
          },
          {
            "id": 2,
            "text": "Paused"
          },
          {
            "id": 3,
            "text": "Retired"
          },
          {
            "id": 4,
            "text": "Yes"
          }
        ]
      },
      "category-3": {
        "label": "Priority Tier",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Specialist"
          },
          {
            "id": 2,
            "text": "Market-Local"
          },
          {
            "id": 3,
            "text": "Fallback"
          },
          {
            "id": 4,
            "text": "Tier 3"
          },
          {
            "id": 5,
            "text": "Tier 2"
          },
          {
            "id": 6,
            "text": "Tier 1"
          }
        ]
      },
      "markets-2": {
        "label": "Markets",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Default"
          },
          {
            "id": 2,
            "text": "Florida"
          },
          {
            "id": 3,
            "text": "Texas"
          },
          {
            "id": 4,
            "text": "California"
          },
          {
            "id": 5,
            "text": "Arizona"
          },
          {
            "id": 6,
            "text": "Nevada"
          },
          {
            "id": 7,
            "text": "Providence, RI"
          },
          {
            "id": 8,
            "text": "New York, NY"
          },
          {
            "id": 9,
            "text": "Chicago, IL"
          },
          {
            "id": 10,
            "text": "Global"
          },
          {
            "id": 11,
            "text": "Los Angeles, CA"
          },
          {
            "id": 12,
            "text": "Sacramento, CA"
          },
          {
            "id": 13,
            "text": "Inland Empire, CA"
          },
          {
            "id": 14,
            "text": "Dallas, TX"
          },
          {
            "id": 15,
            "text": "Atlanta, GA"
          },
          {
            "id": 16,
            "text": "Orange County, CA"
          },
          {
            "id": 17,
            "text": "San Jose, CA"
          },
          {
            "id": 18,
            "text": "Houston, TX"
          },
          {
            "id": 19,
            "text": "Las Vegas, NV"
          },
          {
            "id": 20,
            "text": "Oklahoma City, OK"
          },
          {
            "id": 21,
            "text": "Tulsa, OK"
          },
          {
            "id": 22,
            "text": "Miami, FL"
          },
          {
            "id": 23,
            "text": "Tampa, FL"
          },
          {
            "id": 24,
            "text": "Orlando, FL"
          },
          {
            "id": 25,
            "text": "Phoenix, AZ"
          },
          {
            "id": 26,
            "text": "Stockton, CA"
          },
          {
            "id": 27,
            "text": "Modesto, CA"
          },
          {
            "id": 28,
            "text": "Fresno, CA"
          },
          {
            "id": 29,
            "text": "Bakersfield, CA"
          },
          {
            "id": 30,
            "text": "San Diego, CA"
          },
          {
            "id": 31,
            "text": "Riverside, CA"
          },
          {
            "id": 32,
            "text": "San Bernardino, CA"
          },
          {
            "id": 33,
            "text": "Palm Springs, CA"
          },
          {
            "id": 34,
            "text": "Fort Worth, TX"
          },
          {
            "id": 35,
            "text": "San Antonio, TX"
          },
          {
            "id": 36,
            "text": "Austin, TX"
          },
          {
            "id": 37,
            "text": "El Paso, TX"
          },
          {
            "id": 38,
            "text": "Tucson, AZ"
          },
          {
            "id": 39,
            "text": "Albuquerque, NM"
          },
          {
            "id": 40,
            "text": "Clayton, GA"
          },
          {
            "id": 41,
            "text": "Charlotte, NC"
          },
          {
            "id": 42,
            "text": "Raleigh, NC"
          },
          {
            "id": 43,
            "text": "Durham, NC"
          },
          {
            "id": 44,
            "text": "Fayetteville, NC"
          },
          {
            "id": 45,
            "text": "Rocky Mount, NC"
          },
          {
            "id": 46,
            "text": "Nashville, TN"
          },
          {
            "id": 47,
            "text": "Memphis, TN"
          },
          {
            "id": 48,
            "text": "Birmingham, AL"
          },
          {
            "id": 49,
            "text": "New Orleans, LA"
          },
          {
            "id": 50,
            "text": "Philadelphia, PA"
          },
          {
            "id": 51,
            "text": "Pittsburgh, PA"
          },
          {
            "id": 52,
            "text": "Baltimore, MD"
          },
          {
            "id": 53,
            "text": "Hartford, CT"
          },
          {
            "id": 54,
            "text": "Richmond, VA"
          },
          {
            "id": 55,
            "text": "Hampton Roads, VA"
          },
          {
            "id": 56,
            "text": "Portsmouth, VA"
          },
          {
            "id": 57,
            "text": "Minneapolis, MN"
          },
          {
            "id": 58,
            "text": "St. Paul, MN"
          },
          {
            "id": 59,
            "text": "Milwaukee, WI"
          },
          {
            "id": 60,
            "text": "Des Moines, IA"
          },
          {
            "id": 61,
            "text": "Omaha, NE"
          },
          {
            "id": 62,
            "text": "Denver, CO"
          },
          {
            "id": 63,
            "text": "Colorado Springs, CO"
          },
          {
            "id": 64,
            "text": "Salt Lake City, UT"
          },
          {
            "id": 65,
            "text": "Boise, ID"
          },
          {
            "id": 66,
            "text": "Seattle, WA"
          },
          {
            "id": 67,
            "text": "Portland, OR"
          },
          {
            "id": 68,
            "text": "Spokane, WA"
          },
          {
            "id": 69,
            "text": "Detroit, MI"
          },
          {
            "id": 70,
            "text": "Cleveland, OH"
          },
          {
            "id": 71,
            "text": "Rochester, NY"
          },
          {
            "id": 72,
            "text": "Columbus, OH"
          },
          {
            "id": 73,
            "text": "Cincinnati, OH"
          },
          {
            "id": 74,
            "text": "Indianapolis, IN"
          },
          {
            "id": 75,
            "text": "St. Louis, MO"
          },
          {
            "id": 76,
            "text": "Kansas City, MO"
          },
          {
            "id": 77,
            "text": "Kansas City, KS"
          },
          {
            "id": 78,
            "text": "Wichita, KS"
          },
          {
            "id": 79,
            "text": "Louisville, KY"
          },
          {
            "id": 80,
            "text": "Jacksonville, FL"
          },
          {
            "id": 81,
            "text": "Fort Lauderdale, FL"
          },
          {
            "id": 82,
            "text": "West Palm Beach, FL"
          }
        ]
      },
      "image": {
        "label": "Profile Photo",
        "type": "image",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "link": {
        "label": "Instagram Handle",
        "type": "embed",
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
      "markets": {
        "label": "Markets",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "category-5": {
        "label": "Languages",
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
            "text": "Hebrew"
          },
          {
            "id": 6,
            "text": "Mandarin"
          },
          {
            "id": 7,
            "text": "Korean"
          },
          {
            "id": 8,
            "text": "Vietnamese"
          },
          {
            "id": 9,
            "text": "Polish"
          },
          {
            "id": 10,
            "text": "Arabic"
          },
          {
            "id": 11,
            "text": "Hindi"
          },
          {
            "id": 12,
            "text": "French"
          },
          {
            "id": 13,
            "text": "Russian"
          },
          {
            "id": 14,
            "text": "Japanese"
          },
          {
            "id": 15,
            "text": "Farsi"
          },
          {
            "id": 16,
            "text": "German"
          },
          {
            "id": 17,
            "text": "Greek"
          },
          {
            "id": 18,
            "text": "Thai"
          },
          {
            "id": 19,
            "text": "Pashto"
          }
        ]
      },
      "category-6": {
        "label": "Seed Positions",
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
            "text": "3"
          },
          {
            "id": 3,
            "text": "2"
          },
          {
            "id": 4,
            "text": "4"
          },
          {
            "id": 5,
            "text": "5"
          }
        ]
      },
      "text-2": {
        "label": "Trigger Conditions",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "text-3": {
        "label": "Override Conditions",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "max-daily-contacts": {
        "label": "Max Daily Contacts",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "section-separator": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number": {
        "label": "Formality Level",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-3": {
        "label": "Slang Level",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-2": {
        "label": "Emoji Level",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "preferred-greeting": {
        "label": "Preferred Greeting",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Hey!"
          },
          {
            "id": 2,
            "text": "Hi there"
          },
          {
            "id": 3,
            "text": "Good afternoon"
          },
          {
            "id": 4,
            "text": "Hi {{first_name}}"
          },
          {
            "id": 5,
            "text": "Quick question, {{first_name}}"
          },
          {
            "id": 6,
            "text": "Morning {{first_name}}"
          },
          {
            "id": 7,
            "text": "Hola {{first_name}}"
          },
          {
            "id": 8,
            "text": "Buenos días {{first_name}}"
          },
          {
            "id": 9,
            "text": "Una pregunta, {{first_name}}"
          },
          {
            "id": 10,
            "text": "Oi {{first_name}}"
          },
          {
            "id": 11,
            "text": "Rapidinho, {{first_name}}"
          },
          {
            "id": 12,
            "text": "Ciao {{first_name}}"
          },
          {
            "id": 13,
            "text": "Buongiorno {{first_name}}"
          },
          {
            "id": 14,
            "text": "רק שאלה, {{first_name}}"
          },
          {
            "id": 15,
            "text": "שלום {{first_name}}"
          },
          {
            "id": 16,
            "text": "{{first_name}}，想确认一下"
          },
          {
            "id": 17,
            "text": "한 가지만 여쭤볼게요 {{first_name}}님"
          },
          {
            "id": 18,
            "text": "Chào {{first_name}}"
          },
          {
            "id": 19,
            "text": "Cześć {{first_name}}"
          },
          {
            "id": 20,
            "text": "Hey there {{first_name}}"
          },
          {
            "id": 21,
            "text": "Hey {{first_name}}"
          }
        ]
      },
      "forbidden-words": {
        "label": "Forbidden Words",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "forbidden-moves": {
        "label": "Forbidden Moves",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "response-style-sample": {
        "label": "Response Style Sample",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mirroring-rule": {
        "label": "Mirroring Rule",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "max-chars-sms": {
        "label": "Max Chars (SMS)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "category-9": {
        "label": "Sentence Length",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Short"
          },
          {
            "id": 2,
            "text": "Medium"
          },
          {
            "id": 3,
            "text": "Long"
          }
        ]
      },
      "category-8": {
        "label": "Directness",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Soft"
          },
          {
            "id": 2,
            "text": "Neutral"
          },
          {
            "id": 3,
            "text": "Blunt"
          },
          {
            "id": 4,
            "text": "Very Blunt"
          },
          {
            "id": 5,
            "text": "Medium"
          },
          {
            "id": 6,
            "text": "High"
          }
        ]
      },
      "category-7": {
        "label": "Energy Level",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Calm"
          },
          {
            "id": 2,
            "text": "Measured"
          },
          {
            "id": 3,
            "text": "Confident"
          },
          {
            "id": 4,
            "text": "High Energy"
          },
          {
            "id": 5,
            "text": "High"
          }
        ]
      },
      "latency-hot-min": {
        "label": "Latency Hot Min",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "latency-hot-max": {
        "label": "Latency Hot Max",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "latency-neutral-min": {
        "label": "Latency Neutral Min",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "latency-neutral-max": {
        "label": "Latency Neutral Max",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "latency-cold-min": {
        "label": "Latency Cold Min",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "latency-cold-max": {
        "label": "Latency Cold Max",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-5": {
        "label": "Response Latency Min",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-4": {
        "label": "Response Latency Max",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "text-4": {
        "label": "Signature",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "text-5": {
        "label": "Style Notes",
        "type": "text",
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
      "stage-1-ownership-confirmation": {
        "label": "Stage 1 — Ownership Confirmation",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "stage-2-offer-confirmation": {
        "label": "Stage 2 — Offer Confirmation",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "number-6": {
        "label": "Stage 2 Delay (Days)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "stage-3-offer-reveal": {
        "label": "Stage 3 — Offer Reveal",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "number-7": {
        "label": "Stage 3 Delay (Days)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "objection-not-ready": {
        "label": "Objection: Not Ready",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "objection-price-too-low": {
        "label": "Objection: Price Too Low",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "objection-has-tenants": {
        "label": "Objection: Has Tenants",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "objection-already-listed": {
        "label": "Objection: Already Listed",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "objection-already-have-someone": {
        "label": "Objection: Already Have Someone",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "objection-need-family-discussion": {
        "label": "Objection: Need Family Discussion",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "stage-5-re-engagement": {
        "label": "Stage 5 — Re-engagement",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "number-8": {
        "label": "Stage 5 Delay (Days)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "stage-6-close-handoff": {
        "label": "Stage 6 — Close / Handoff",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "section-separator-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "calculation": {
        "label": "Total Assigned",
        "type": "calculation",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "texts-sent": {
        "label": "Texts Sent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "reply-rate": {
        "label": "Reply Rate (%)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "positive-reply-rate": {
        "label": "Positive Reply Rate (%)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-opt-outs": {
        "label": "Total Opt-Outs",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-conversions": {
        "label": "Total Conversions",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "opt-out-rate": {
        "label": "Opt-Out Rate (%)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "nstage-3-reach-rate-umber": {
        "label": "Stage 3 Reach Rate (%)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "conversion-rate": {
        "label": "Conversion Rate (%)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "performance-tier": {
        "label": "Performance Tier",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "_ Elite"
          },
          {
            "id": 2,
            "text": "✅ Good"
          },
          {
            "id": 3,
            "text": "⚠️ Watch"
          },
          {
            "id": 4,
            "text": "❌ Retire"
          }
        ]
      },
      "last-active-date": {
        "label": "Last Active Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-campaign-date": {
        "label": "Last Campaign Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "lifetime-score": {
        "label": "Lifetime Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "text-22": {
        "label": "Text",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "text-21": {
        "label": "Text",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30679234": {
    "app_id": 30679234,
    "app_name": "MasterOwners",
    "item_name": "Owner",
    "fields": {
      "seller-id": {
        "label": "Master Key",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "owner-full-name": {
        "label": "Display Name",
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
      "calculation": {
        "label": ">",
        "type": "calculation",
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
            "id": 11,
            "text": "LLC/CORP | ABSENTEE"
          },
          {
            "id": 12,
            "text": "TRUST/ESTATE | ABSENTEE"
          },
          {
            "id": 13,
            "text": "INDIVIDUAL | ABSENTEE"
          },
          {
            "id": 14,
            "text": "BANK/INSTITUTION | ABSENTEE"
          },
          {
            "id": 15,
            "text": "INDIVIDUAL | OWNER_OCC"
          },
          {
            "id": 16,
            "text": "TRUST/ESTATE | OWNER_OCC"
          }
        ]
      },
      "markets": {
        "label": "Markets",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Tampa, FL"
          },
          {
            "id": 2,
            "text": "Miami, FL"
          },
          {
            "id": 3,
            "text": "Los Angeles, CA"
          },
          {
            "id": 4,
            "text": "Fort Lauderdale, FL"
          },
          {
            "id": 5,
            "text": "West Palm Beach, FL"
          },
          {
            "id": 6,
            "text": "Cleveland, OH"
          },
          {
            "id": 7,
            "text": "Riverside, CA"
          },
          {
            "id": 8,
            "text": "Phoenix, AZ"
          },
          {
            "id": 9,
            "text": "Providence, RI"
          },
          {
            "id": 10,
            "text": "Unmapped"
          },
          {
            "id": 11,
            "text": "Tulsa, OK"
          },
          {
            "id": 12,
            "text": "Inland Empire, CA"
          },
          {
            "id": 13,
            "text": "Fayetteville, NC"
          },
          {
            "id": 14,
            "text": "Spokane, WA"
          },
          {
            "id": 15,
            "text": "San Bernardino, CA"
          },
          {
            "id": 16,
            "text": "Stockton, CA"
          },
          {
            "id": 17,
            "text": "Durham, NC"
          },
          {
            "id": 18,
            "text": "Austin, TX"
          },
          {
            "id": 19,
            "text": "Modesto, CA"
          },
          {
            "id": 20,
            "text": "Rocky Mount, NC"
          },
          {
            "id": 21,
            "text": "Palm Springs, CA"
          },
          {
            "id": 22,
            "text": "Hartford, CT"
          },
          {
            "id": 23,
            "text": "Pittsburgh, PA"
          },
          {
            "id": 24,
            "text": "Oklahoma City, OK"
          },
          {
            "id": 25,
            "text": "Rochester, NY"
          },
          {
            "id": 26,
            "text": "Hampton Roads, VA"
          },
          {
            "id": 27,
            "text": "San Antonio, TX"
          },
          {
            "id": 28,
            "text": "Wichita, KS"
          },
          {
            "id": 29,
            "text": "Richmond, VA"
          },
          {
            "id": 30,
            "text": "Columbus, OH"
          },
          {
            "id": 31,
            "text": "Louisville, KY"
          },
          {
            "id": 32,
            "text": "Cincinnati, OH"
          },
          {
            "id": 33,
            "text": "Salt Lake City, UT"
          },
          {
            "id": 34,
            "text": "El Paso, TX"
          },
          {
            "id": 35,
            "text": "Omaha, NE"
          },
          {
            "id": 36,
            "text": "Des Moines, IA"
          },
          {
            "id": 37,
            "text": "Colorado Springs, CO"
          },
          {
            "id": 38,
            "text": "Portsmouth, VA"
          },
          {
            "id": 39,
            "text": "Albuquerque, NM"
          },
          {
            "id": 40,
            "text": "Kansas City, MO"
          },
          {
            "id": 41,
            "text": "Atlanta, GA"
          },
          {
            "id": 42,
            "text": "Kansas City, KS"
          },
          {
            "id": 43,
            "text": "Las Vegas, NV"
          },
          {
            "id": 44,
            "text": "Charlotte, NC"
          },
          {
            "id": 45,
            "text": "Indianapolis, IN"
          },
          {
            "id": 46,
            "text": "Chicago, IL"
          },
          {
            "id": 47,
            "text": "Houston, TX"
          },
          {
            "id": 48,
            "text": "Minneapolis, MN"
          },
          {
            "id": 49,
            "text": "Milwaukee, WI"
          },
          {
            "id": 50,
            "text": "St. Paul, MN"
          },
          {
            "id": 51,
            "text": "Memphis, TN"
          },
          {
            "id": 52,
            "text": "Sacramento, CA"
          },
          {
            "id": 53,
            "text": "New Orleans, LA"
          },
          {
            "id": 54,
            "text": "Tucson, AZ"
          },
          {
            "id": 55,
            "text": "Jacksonville, FL"
          },
          {
            "id": 56,
            "text": "Orlando, FL"
          },
          {
            "id": 57,
            "text": "Bakersfield, CA"
          },
          {
            "id": 58,
            "text": "Dallas, TX"
          },
          {
            "id": 59,
            "text": "Fresno, CA"
          },
          {
            "id": 60,
            "text": "Birmingham, AL"
          },
          {
            "id": 61,
            "text": "Fort Worth, TX"
          },
          {
            "id": 62,
            "text": "Philadelphia, PA"
          },
          {
            "id": 63,
            "text": "Detroit, MI"
          },
          {
            "id": 64,
            "text": "Baltimore, MD"
          },
          {
            "id": 65,
            "text": "St. Louis, MO"
          }
        ]
      },
      "field": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "master-owner-priority-score": {
        "label": "Master Owner Priority Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "category-2": {
        "label": "Category",
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
            "text": "None"
          }
        ]
      },
      "contact-status": {
        "label": "Contact Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Not Contacted"
          },
          {
            "id": 2,
            "text": "Contacted"
          },
          {
            "id": 3,
            "text": "Engaged"
          },
          {
            "id": 7,
            "text": "Offer Sent"
          },
          {
            "id": 4,
            "text": "Negotiating"
          },
          {
            "id": 5,
            "text": "Under Contract"
          },
          {
            "id": 6,
            "text": "Dead"
          },
          {
            "id": 8,
            "text": "In Escrow"
          }
        ]
      },
      "sms-elgible": {
        "label": "SMS Elgible?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
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
            "id": 2,
            "text": "#1"
          },
          {
            "id": 3,
            "text": "#2 - MASTER"
          },
          {
            "id": 5,
            "text": "#3"
          },
          {
            "id": 6,
            "text": "#4"
          },
          {
            "id": 4,
            "text": "#5"
          },
          {
            "id": 1,
            "text": "#6"
          },
          {
            "id": 7,
            "text": "#7"
          },
          {
            "id": 8,
            "text": "#1 - MASTER"
          },
          {
            "id": 9,
            "text": "#3 - MASTER"
          },
          {
            "id": 10,
            "text": "#5 - MASTER"
          },
          {
            "id": 11,
            "text": "#6 - MASTER"
          }
        ]
      },
      "priority-tier": {
        "label": "Priority Tier",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 5,
            "text": "TIER_3"
          },
          {
            "id": 6,
            "text": "TIER_2"
          },
          {
            "id": 7,
            "text": "TIER_1"
          }
        ]
      },
      "portfolio-value": {
        "label": "Portfolio Value",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-equty": {
        "label": "Total Equty",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-debt": {
        "label": "Total Debt",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-equity-percent": {
        "label": "Total Equity Percent",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "monthly-debt-payment": {
        "label": "Monthly Debt Payment",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "portfolio-property-count": {
        "label": "Portfolio Property Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "units": {
        "label": "Units",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property-type-majority": {
        "label": "Property Type Majority",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "SINGLE FAMILY"
          },
          {
            "id": 2,
            "text": "MULTI-FAMILY"
          },
          {
            "id": 3,
            "text": "APARTMENT"
          },
          {
            "id": 4,
            "text": "VACANT LAND"
          },
          {
            "id": 5,
            "text": "OTHER"
          },
          {
            "id": 6,
            "text": "TOWNHOUSE"
          }
        ]
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "best-contact-1": {
        "label": "Best Contact #1",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "best-contact-2": {
        "label": "Best Contact #2",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "best-phone-1": {
        "label": "Best Phone #1",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30658310
        ],
        "options": []
      },
      "best-phone-2": {
        "label": "Best Phone #2",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30658310
        ],
        "options": []
      },
      "best-phone-3": {
        "label": "Best Phone #3",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30658310
        ],
        "options": []
      },
      "best-email-1": {
        "label": "Best Email #1",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646486
        ],
        "options": []
      },
      "best-email-2": {
        "label": "Best Email #2",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646486
        ],
        "options": []
      },
      "contactability-score": {
        "label": "Contactability Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contact-confidence": {
        "label": "Contact Confidence",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "MEDIUM"
          },
          {
            "id": 2,
            "text": "HIGH"
          },
          {
            "id": 3,
            "text": "LOW"
          }
        ]
      },
      "phone-quality-bucket": {
        "label": "Phone Quality Bucket",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "A"
          },
          {
            "id": 2,
            "text": "B"
          },
          {
            "id": 3,
            "text": "C"
          },
          {
            "id": 4,
            "text": "D"
          }
        ]
      },
      "section-separator-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "financial-pressure-score": {
        "label": "Financial Pressure Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "urgency-score": {
        "label": "Urgency Score",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "portfolio-tax-delinquent-count": {
        "label": "Portfolio Tax Delinquent Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "tax-delinquent": {
        "label": "Tax Delinquent",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "NO"
          },
          {
            "id": 2,
            "text": "YES"
          }
        ]
      },
      "active-lien": {
        "label": "Active Lien",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "NO"
          },
          {
            "id": 2,
            "text": "YES"
          }
        ]
      },
      "portfolio-lien-count": {
        "label": "Portfolio Lien Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "oldest-tax-delinquent-year": {
        "label": "Oldest Tax Delinquent Year",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "total-tax-amount": {
        "label": "Total Tax Amount",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sfr-count": {
        "label": "SFR Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mf-count": {
        "label": "MF Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property-mix": {
        "label": "Property Mix",
        "type": "category",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "2 SFR"
          },
          {
            "id": 2,
            "text": "1 MF"
          },
          {
            "id": 3,
            "text": "1 SFR"
          },
          {
            "id": 4,
            "text": "3 SFR"
          },
          {
            "id": 5,
            "text": "2 MF"
          },
          {
            "id": 6,
            "text": "10 SFR"
          },
          {
            "id": 7,
            "text": "4 SFR"
          },
          {
            "id": 8,
            "text": "5 SFR"
          },
          {
            "id": 9,
            "text": "17 SFR"
          },
          {
            "id": 10,
            "text": "6 SFR"
          },
          {
            "id": 11,
            "text": "3 MF"
          },
          {
            "id": 12,
            "text": "7 SFR"
          },
          {
            "id": 13,
            "text": "9 SFR"
          },
          {
            "id": 14,
            "text": "13 SFR"
          },
          {
            "id": 15,
            "text": "6 MF"
          },
          {
            "id": 16,
            "text": "11 SFR"
          },
          {
            "id": 17,
            "text": "9 MF"
          },
          {
            "id": 18,
            "text": "22 SFR"
          },
          {
            "id": 19,
            "text": "8 SFR"
          },
          {
            "id": 20,
            "text": "10 MF"
          },
          {
            "id": 21,
            "text": "4 MF"
          },
          {
            "id": 22,
            "text": "5 MF"
          },
          {
            "id": 23,
            "text": "12 SFR"
          },
          {
            "id": 24,
            "text": "14 SFR"
          },
          {
            "id": 25,
            "text": "127 SFR"
          },
          {
            "id": 26,
            "text": "35 MF"
          },
          {
            "id": 27,
            "text": "40 SFR"
          },
          {
            "id": 28,
            "text": "32 MF"
          },
          {
            "id": 29,
            "text": "23 SFR"
          },
          {
            "id": 30,
            "text": "20 SFR"
          },
          {
            "id": 31,
            "text": "18 SFR"
          },
          {
            "id": 32,
            "text": "7 MF"
          },
          {
            "id": 33,
            "text": "1 LAND"
          },
          {
            "id": 34,
            "text": "63 SFR"
          },
          {
            "id": 35,
            "text": "60 SFR"
          },
          {
            "id": 36,
            "text": "19 SFR"
          },
          {
            "id": 37,
            "text": "8 MF"
          },
          {
            "id": 38,
            "text": "15 SFR"
          },
          {
            "id": 39,
            "text": "53 SFR"
          },
          {
            "id": 40,
            "text": "33 SFR"
          },
          {
            "id": 41,
            "text": "29 MF"
          },
          {
            "id": 42,
            "text": "30 SFR"
          },
          {
            "id": 43,
            "text": "44 SFR"
          },
          {
            "id": 44,
            "text": "25 MF"
          },
          {
            "id": 45,
            "text": "20 MF"
          },
          {
            "id": 46,
            "text": "16 SFR"
          },
          {
            "id": 47,
            "text": "31 SFR"
          },
          {
            "id": 48,
            "text": "21 SFR"
          },
          {
            "id": 49,
            "text": "56 SFR"
          },
          {
            "id": 50,
            "text": "119 MF"
          },
          {
            "id": 51,
            "text": "27 SFR"
          },
          {
            "id": 52,
            "text": "34 SFR"
          },
          {
            "id": 53,
            "text": "25 SFR"
          },
          {
            "id": 54,
            "text": "38 SFR"
          },
          {
            "id": 55,
            "text": "46 SFR"
          },
          {
            "id": 56,
            "text": "12 MF"
          },
          {
            "id": 57,
            "text": "26 SFR"
          },
          {
            "id": 58,
            "text": "35 SFR"
          },
          {
            "id": 59,
            "text": "19 MF"
          }
        ]
      },
      "section-separator": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "language-primary": {
        "label": "Language Primary",
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
            "text": "Hebrew"
          },
          {
            "id": 4,
            "text": "Portuguese"
          },
          {
            "id": 5,
            "text": "Italian"
          },
          {
            "id": 6,
            "text": "Arabic"
          },
          {
            "id": 7,
            "text": "Asian Indian (Hindi or Other)"
          },
          {
            "id": 8,
            "text": "French"
          },
          {
            "id": 9,
            "text": "Korean"
          },
          {
            "id": 10,
            "text": "Mandarin"
          },
          {
            "id": 11,
            "text": "Polish"
          },
          {
            "id": 12,
            "text": "Russian"
          },
          {
            "id": 13,
            "text": "Japanese"
          },
          {
            "id": 14,
            "text": "Vietnamese"
          },
          {
            "id": 15,
            "text": "Farsi"
          },
          {
            "id": 16,
            "text": "German"
          },
          {
            "id": 17,
            "text": "Greek"
          },
          {
            "id": 18,
            "text": "Thai"
          },
          {
            "id": 19,
            "text": "Pashtu/Pashto"
          }
        ]
      },
      "owner-cluster-key": {
        "label": "Owner Cluster Key",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "unique-entity-count": {
        "label": "Unique Entity Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "unique-owner-name-count": {
        "label": "Unique Owner Name Count",
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
      "linked-conversations": {
        "label": "Linked Conversations",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646484
        ],
        "options": []
      },
      "outbound-number": {
        "label": "Outbound Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30541677
        ],
        "options": []
      },
      "offer": {
        "label": "Offer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30643944
        ],
        "options": []
      },
      "contract": {
        "label": "Contract",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687663
        ],
        "options": []
      },
      "closing": {
        "label": "Closing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687671
        ],
        "options": []
      },
      "sms-agent": {
        "label": "SMS Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30678167
        ],
        "options": []
      },
      "assigned-agent": {
        "label": "Assigned Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644052
        ],
        "options": []
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "timezone": {
        "label": "Timezone",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Central"
          },
          {
            "id": 2,
            "text": "Eastern"
          },
          {
            "id": 3,
            "text": "Pacific"
          },
          {
            "id": 4,
            "text": "Mountain"
          },
          {
            "id": 5,
            "text": "Hawaii"
          },
          {
            "id": 6,
            "text": "Alaska"
          }
        ]
      },
      "best-contact-window": {
        "label": "Best Contact Window",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "9AM-8PM CT"
          },
          {
            "id": 2,
            "text": "9AM-11AM ET"
          },
          {
            "id": 3,
            "text": "12PM-1PM ET"
          },
          {
            "id": 4,
            "text": "5PM-9PM PT"
          },
          {
            "id": 5,
            "text": "9AM-11AM PT"
          },
          {
            "id": 6,
            "text": "11AM-1PM PT"
          },
          {
            "id": 7,
            "text": "8AM-10AM ET"
          },
          {
            "id": 8,
            "text": "9AM-8PM PT"
          },
          {
            "id": 9,
            "text": "11AM-1PM ET"
          },
          {
            "id": 10,
            "text": "5PM-8PM PT"
          },
          {
            "id": 11,
            "text": "9AM-8PM ET"
          },
          {
            "id": 12,
            "text": "7AM-9AM ET"
          },
          {
            "id": 13,
            "text": "5PM-8PM ET"
          },
          {
            "id": 14,
            "text": "12PM-1PM PT"
          },
          {
            "id": 15,
            "text": "8AM-10AM PT"
          },
          {
            "id": 16,
            "text": "10AM-12PM PT"
          },
          {
            "id": 17,
            "text": "5PM-9PM ET"
          },
          {
            "id": 18,
            "text": "6PM-9PM PT"
          },
          {
            "id": 19,
            "text": "7AM-9AM PT"
          },
          {
            "id": 20,
            "text": "6AM-8AM PT"
          },
          {
            "id": 21,
            "text": "10AM-12PM ET"
          },
          {
            "id": 22,
            "text": "12PM-1PM Local"
          },
          {
            "id": 23,
            "text": "6PM-9PM MT"
          },
          {
            "id": 24,
            "text": "9AM-8PM Local"
          },
          {
            "id": 25,
            "text": "8AM-10AM CT"
          },
          {
            "id": 26,
            "text": "8AM-10AM Local"
          },
          {
            "id": 27,
            "text": "7AM-9AM CT"
          },
          {
            "id": 28,
            "text": "6AM-8AM ET"
          },
          {
            "id": 29,
            "text": "6PM-9PM ET"
          },
          {
            "id": 30,
            "text": "9AM-8PM MT"
          },
          {
            "id": 31,
            "text": "5PM-9PM Local"
          },
          {
            "id": 32,
            "text": "12PM-1PM CT"
          },
          {
            "id": 33,
            "text": "12PM-1PM MT"
          },
          {
            "id": 34,
            "text": "10AM-12PM CT"
          },
          {
            "id": 35,
            "text": "11AM-1PM MT"
          },
          {
            "id": 36,
            "text": "5PM-8PM CT"
          },
          {
            "id": 37,
            "text": "10AM-12PM MT"
          },
          {
            "id": 38,
            "text": "11AM-1PM CT"
          },
          {
            "id": 39,
            "text": "12PM-2PM ET"
          },
          {
            "id": 40,
            "text": "6PM-9PM Local"
          },
          {
            "id": 41,
            "text": "12PM-2PM CT"
          },
          {
            "id": 42,
            "text": "12PM-2PM PT"
          },
          {
            "id": 43,
            "text": "3PM-6PM PT"
          },
          {
            "id": 44,
            "text": "6AM-8AM CT"
          },
          {
            "id": 45,
            "text": "3PM-6PM ET"
          },
          {
            "id": 46,
            "text": "11AM-1PM Local"
          },
          {
            "id": 47,
            "text": "3PM-6PM CT"
          },
          {
            "id": 48,
            "text": "9AM-11AM Local"
          },
          {
            "id": 49,
            "text": "12PM-2PM Local"
          },
          {
            "id": 50,
            "text": "9AM-11AM CT"
          },
          {
            "id": 51,
            "text": "3PM-6PM MT"
          },
          {
            "id": 52,
            "text": "3PM-6PM Local"
          },
          {
            "id": 53,
            "text": "9AM-11AM MT"
          },
          {
            "id": 54,
            "text": "12PM-2PM MT"
          },
          {
            "id": 55,
            "text": "5PM-8PM MT"
          },
          {
            "id": 56,
            "text": "10AM-12PM Local"
          },
          {
            "id": 57,
            "text": "5PM-9PM CT"
          },
          {
            "id": 58,
            "text": "7AM-9AM Local"
          },
          {
            "id": 59,
            "text": "7AM-9AM MT"
          },
          {
            "id": 60,
            "text": "8AM-10AM MT"
          },
          {
            "id": 61,
            "text": "6PM-9PM CT"
          },
          {
            "id": 62,
            "text": "6AM-8AM MT"
          },
          {
            "id": 63,
            "text": "5PM-9PM MT"
          },
          {
            "id": 64,
            "text": "6AM-8AM Local"
          },
          {
            "id": 65,
            "text": "5PM-8PM Local"
          }
        ]
      },
      "follow-up-cadence": {
        "label": "Follow Up Cadence",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "PASSIVE"
          },
          {
            "id": 2,
            "text": "STANDARD"
          },
          {
            "id": 3,
            "text": "AGGRESSIVE"
          }
        ]
      },
      "message-variant-seed": {
        "label": "Message Variant Seed",
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
            "text": "5"
          },
          {
            "id": 5,
            "text": "1"
          }
        ]
      },
      "contact-status-2": {
        "label": "Contact Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Sent"
          },
          {
            "id": 2,
            "text": "Received"
          },
          {
            "id": 3,
            "text": "Follow-Up Scheduled"
          },
          {
            "id": 4,
            "text": "DNC"
          }
        ]
      },
      "last-outbound": {
        "label": "Last Outbound",
        "type": "date",
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
      "last-contacted-at": {
        "label": "Last Contacted At",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "next-follow-up-at": {
        "label": "Next Follow-Up At",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30680653": {
    "app_id": 30680653,
    "app_name": "Send Queue",
    "item_name": "Message",
    "fields": {
      "queue-id": {
        "label": "Queue ID",
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
      "scheduled-for-local": {
        "label": "Scheduled For (Local)",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "scheduled-for-utc": {
        "label": "Scheduled For (UTC)",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "timezone": {
        "label": "Timezone",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Central"
          },
          {
            "id": 2,
            "text": "Eastern"
          },
          {
            "id": 3,
            "text": "Pacific"
          },
          {
            "id": 4,
            "text": "Mountain"
          },
          {
            "id": 5,
            "text": "Hawaii"
          },
          {
            "id": 6,
            "text": "Alaska"
          }
        ]
      },
      "contact-window": {
        "label": "Contact Window",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "9AM-8PM CT"
          }
        ]
      },
      "send-priority": {
        "label": "Send Priority",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "_ Urgent"
          },
          {
            "id": 2,
            "text": "_ Normal"
          },
          {
            "id": 3,
            "text": "_ Low"
          }
        ]
      },
      "retry-count": {
        "label": "Retry Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "max-retries": {
        "label": "Max Retries",
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
      "queue-status": {
        "label": "Queue Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Queued"
          },
          {
            "id": 2,
            "text": "Sending"
          },
          {
            "id": 3,
            "text": "Sent"
          },
          {
            "id": 4,
            "text": "Cancelled"
          },
          {
            "id": 5,
            "text": "Blocked"
          },
          {
            "id": 6,
            "text": "Failed"
          }
        ]
      },
      "sent-at": {
        "label": "Sent At",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "delivered-at": {
        "label": "Delivered At",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "failed-reason": {
        "label": "Failed Reason",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Carrier Block"
          },
          {
            "id": 2,
            "text": "Opt-Out"
          },
          {
            "id": 3,
            "text": "Invalid Number"
          },
          {
            "id": 4,
            "text": "Daily Limit Hit"
          },
          {
            "id": 5,
            "text": "Network Error"
          }
        ]
      },
      "delivery-confirmed": {
        "label": "Delivery Confirmed",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "✅ Confirmed"
          },
          {
            "id": 2,
            "text": "❌ Failed"
          },
          {
            "id": 3,
            "text": "⏳ Pending"
          }
        ]
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "prospects": {
        "label": "Prospects",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "properties": {
        "label": "Properties",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "phone-number": {
        "label": "Phone Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30658310
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "touch-number": {
        "label": "Touch Number",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "dnc-check": {
        "label": "DNC Check",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "✅ Cleared"
          },
          {
            "id": 2,
            "text": "_ Blocked"
          }
        ]
      },
      "field-4": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sms-agent": {
        "label": "SMS Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30678167
        ],
        "options": []
      },
      "textgrid-number": {
        "label": "Textgrid Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30541677
        ],
        "options": []
      },
      "template": {
        "label": "Template",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          29488989
        ],
        "options": []
      },
      "message-type": {
        "label": "Message Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Cold Outbound"
          },
          {
            "id": 2,
            "text": "Follow-Up"
          },
          {
            "id": 3,
            "text": "Re-Engagement"
          },
          {
            "id": 4,
            "text": "Opt-Out Confirm"
          }
        ]
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "message-text": {
        "label": "Message Text",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "personalization-tags-used": {
        "label": "Personalization Tags Used",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "{{owner_name}}"
          },
          {
            "id": 2,
            "text": "{{property_address}}"
          },
          {
            "id": 3,
            "text": "{{agent_name}}"
          },
          {
            "id": 4,
            "text": "{{market}}"
          }
        ]
      },
      "character-count": {
        "label": "Character Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30682508": {
    "app_id": 30682508,
    "app_name": "Underwriting",
    "item_name": "Numbers",
    "fields": {
      "title": {
        "label": "Underwriter Title",
        "type": "text",
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
      "underwriting-id": {
        "label": "Underwriting ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "underwriting-type": {
        "label": "Underwriting Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Creative"
          },
          {
            "id": 2,
            "text": "Multifamily"
          },
          {
            "id": 3,
            "text": "Novation"
          }
        ]
      },
      "underwriting-status": {
        "label": "Underwriting Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Queued"
          },
          {
            "id": 2,
            "text": "Running"
          },
          {
            "id": 3,
            "text": "Completed"
          },
          {
            "id": 4,
            "text": "Failed"
          },
          {
            "id": 5,
            "text": "Sent to Offers"
          },
          {
            "id": 6,
            "text": "Dead"
          }
        ]
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property-2": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "prospect": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "conversation": {
        "label": "Conversation",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646484
        ],
        "options": []
      },
      "phone-number": {
        "label": "Phone Number",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30658310
        ],
        "options": []
      },
      "offer": {
        "label": "Offer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30643944
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "reason-sent-to-underwriting": {
        "label": "Reason Sent To Underwriting",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Wants More Than Cash"
          },
          {
            "id": 2,
            "text": "Asked for Terms"
          },
          {
            "id": 3,
            "text": "Multifamily Deal"
          },
          {
            "id": 4,
            "text": "Novation Opportunity"
          },
          {
            "id": 5,
            "text": "Complex Condition"
          },
          {
            "id": 6,
            "text": "AI Escalation"
          }
        ]
      },
      "seller-asking-price": {
        "label": "Seller Asking Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "seller-counter-offer": {
        "label": "Seller Counter Offer",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "escalation-summary": {
        "label": "Escalation Summary",
        "type": "text",
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
      "creative-strategy": {
        "label": "Creative Strategy",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Subject To"
          },
          {
            "id": 2,
            "text": "Seller Finance"
          },
          {
            "id": 3,
            "text": "Lease Option"
          },
          {
            "id": 4,
            "text": "Wrap"
          },
          {
            "id": 5,
            "text": "Hybrid"
          }
        ]
      },
      "purchase-price": {
        "label": "Purchase Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "down-payment": {
        "label": "Down Payment",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "monthly-payment": {
        "label": "Monthly Payment",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "interest-rate": {
        "label": "Interest Rate",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "loan-terms-months": {
        "label": "Loan Terms (Months",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "balloon-payment": {
        "label": "Balloon Payment",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "existing-mortgage-balance": {
        "label": "Existing Mortgage Balance",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "existing-mortgage-payment": {
        "label": "Existing Mortgage Payment",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-payoff": {
        "label": "Estimated Payoff",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "creative-terms-summary": {
        "label": "Creative Terms Summary",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-of-units-snapshot": {
        "label": "Number of Units Snapshot",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "occupancy-at-underwriting": {
        "label": "Occupancy at Underwriting",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "current-gross-rents": {
        "label": "Current Gross Rents",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-expenses": {
        "label": "Estimated Expenses",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "noi": {
        "label": "NOI",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "cap-rate": {
        "label": "Cap Rate",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mf-exit-strategy": {
        "label": "MF Exit Strategy",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Wholesale"
          },
          {
            "id": 2,
            "text": "Hold"
          },
          {
            "id": 3,
            "text": "Refinance"
          },
          {
            "id": 4,
            "text": "Seller Finance Out"
          }
        ]
      },
      "mf-summary": {
        "label": "MF Summary",
        "type": "text",
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
      "novation-list-price": {
        "label": "Novation List Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "target-net-to-seller": {
        "label": "Target Net To Seller",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "our-estimated-spread": {
        "label": "Our Estimated Spread",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-repair-scope": {
        "label": "Estimated Repair Scope",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "None"
          },
          {
            "id": 2,
            "text": "Light"
          },
          {
            "id": 3,
            "text": "Medium"
          },
          {
            "id": 4,
            "text": "Heavy"
          }
        ]
      },
      "estimated-repair-cost": {
        "label": "Estimated Repair Cost",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "estimated-days-to-sell": {
        "label": "Estimated Days To Sell",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "mls-target-date": {
        "label": "MLS Target Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "novation-summary": {
        "label": "Novation Summary",
        "type": "text",
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
      "ai-recommeneded-strategy": {
        "label": "AI Recommeneded Strategy",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Creative"
          },
          {
            "id": 2,
            "text": "Multifamily"
          },
          {
            "id": 3,
            "text": "Novation"
          },
          {
            "id": 4,
            "text": "Dead"
          }
        ]
      },
      "ai-recommended-next-move": {
        "label": "AI Recommended Next Move",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-risk-summary": {
        "label": "AI Risk Summary",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-offer-terms-justification": {
        "label": "AI Offer / Terms Justification",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-confidence-score": {
        "label": "AI Confidence Score",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "automation-result": {
        "label": "Automation Result",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Approved"
          },
          {
            "id": 2,
            "text": "Rejected"
          },
          {
            "id": 3,
            "text": "Needs Retry"
          },
          {
            "id": 4,
            "text": "Needs Alternative Strategy"
          }
        ]
      },
      "rejection-failure-reason": {
        "label": "Rejection / Failure Reason",
        "type": "text",
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
      "automation-status": {
        "label": "Automation Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Queued"
          },
          {
            "id": 2,
            "text": "Running"
          },
          {
            "id": 3,
            "text": "Waiting on Input"
          },
          {
            "id": 4,
            "text": "Completed"
          },
          {
            "id": 5,
            "text": "Failed"
          }
        ]
      },
      "current-engine-step": {
        "label": "Current Engine Step",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Triggered"
          },
          {
            "id": 2,
            "text": "Evaluatiing"
          },
          {
            "id": 3,
            "text": "Structuring Terms"
          },
          {
            "id": 4,
            "text": "Finalizing Output"
          },
          {
            "id": 5,
            "text": "Sent to Offers"
          }
        ]
      },
      "triggered-at": {
        "label": "Triggered At",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "completed-at": {
        "label": "Completed At",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "sent-to-offers-date": {
        "label": "Sent to Offers Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "retry-count": {
        "label": "Retry Count",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30687663": {
    "app_id": 30687663,
    "app_name": "Contracts",
    "item_name": "Contract",
    "fields": {
      "title": {
        "label": "Contract Title",
        "type": "text",
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
      "contract-id": {
        "label": "Contract ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract-version": {
        "label": "Contract Version",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "category": {
        "label": "Contract Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Draft"
          },
          {
            "id": 2,
            "text": "Sent"
          },
          {
            "id": 3,
            "text": "Viewed"
          },
          {
            "id": 4,
            "text": "Seller Signed"
          },
          {
            "id": 5,
            "text": "Buyer Signed"
          },
          {
            "id": 6,
            "text": "Fully Executed"
          },
          {
            "id": 7,
            "text": "Sent To Title"
          },
          {
            "id": 8,
            "text": "Opened"
          },
          {
            "id": 9,
            "text": "Clear To Close"
          },
          {
            "id": 10,
            "text": "Closed"
          },
          {
            "id": 11,
            "text": "Cancelled"
          }
        ]
      },
      "contract-type": {
        "label": "Contract Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Cash"
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
            "text": "Multifamily"
          }
        ]
      },
      "state": {
        "label": "State",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "template-type": {
        "label": "Template Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Standard Purchase"
          },
          {
            "id": 2,
            "text": "Assignment"
          },
          {
            "id": 3,
            "text": "Novation"
          },
          {
            "id": 4,
            "text": "Creative"
          },
          {
            "id": 5,
            "text": "Mutlifamily"
          }
        ]
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "offer": {
        "label": "Offer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30643944
        ],
        "options": []
      },
      "property": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "prospect": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "phone": {
        "label": "Phone",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "email": {
        "label": "Email",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "conversation": {
        "label": "Conversation",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646484
        ],
        "options": []
      },
      "assigned-agent": {
        "label": "Assigned Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30678167
        ],
        "options": []
      },
      "title-company-2": {
        "label": "Title Company",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644727
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "purchase-price-final": {
        "label": "Purchase Price (Final)",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "money": {
        "label": "EMD Amount",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "date": {
        "label": "Closing Date Target",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number": {
        "label": "Closing Timeline",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "assignment-allowed": {
        "label": "Assignment Allowed",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "inspection-period-days": {
        "label": "Inspection Period (Days)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "creative-terms": {
        "label": "Creative Terms",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-company": {
        "label": "Title Company",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "section-separator": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract-document": {
        "label": "Contract Document",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "docusign-envelope-id": {
        "label": "DocuSign Envelope ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "docusign-signing-link": {
        "label": "DocuSign Signing Link",
        "type": "embed",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract-sent-timestamp": {
        "label": "Contract Sent Timestamp",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract-viewed-timestamp": {
        "label": "Contract Viewed Timestamp",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "seller-signed-timestamp": {
        "label": "Seller Signed Timestamp",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "buyer-signed-timestamp": {
        "label": "Buyer Signed Timestamp",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-opened-timestamp": {
        "label": "Fully Executed Timestamp",
        "type": "date",
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
      "title-routing": {
        "label": "Title Routing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644041
        ],
        "options": []
      },
      "buyer-match": {
        "label": "Buyer Match",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644050
        ],
        "options": []
      },
      "pipeline": {
        "label": "Pipeline",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644051
        ],
        "options": []
      }
    }
  },
  "30687664": {
    "app_id": 30687664,
    "app_name": "Title Routing (Closing Engine)",
    "item_name": "Closing",
    "fields": {
      "title": {
        "label": "Title Routing Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "section-separator": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-routing-id": {
        "label": "Title Routing ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-file-status": {
        "label": "Routing Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Not Routed"
          },
          {
            "id": 2,
            "text": "Routed"
          },
          {
            "id": 3,
            "text": "Title Reviewing"
          },
          {
            "id": 4,
            "text": "Opened"
          },
          {
            "id": 5,
            "text": "Waiting on Docs"
          },
          {
            "id": 6,
            "text": "Waiting on Payoff"
          },
          {
            "id": 7,
            "text": "Waiting on Probate"
          },
          {
            "id": 8,
            "text": "Waiting on Seller"
          },
          {
            "id": 9,
            "text": "Waiting on Buyer"
          },
          {
            "id": 10,
            "text": "Clear to Close"
          },
          {
            "id": 11,
            "text": "Closed"
          },
          {
            "id": 12,
            "text": "Cancelled"
          }
        ]
      },
      "field": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract-2": {
        "label": "Contract",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687663
        ],
        "options": []
      },
      "closing": {
        "label": "Closing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687671
        ],
        "options": []
      },
      "property-2": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "prospect-2": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "title-company": {
        "label": "Title Company",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687667
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "assigned-agent": {
        "label": "Assigned Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30678167
        ],
        "options": []
      },
      "section-separator-2": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "file-routed-date": {
        "label": "File Routed Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-opened-date": {
        "label": "TItle Opened Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "commitment-received-date": {
        "label": "Commitment Received Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "clear-to-close-date": {
        "label": "Clear To Close Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "expected-closing-date": {
        "label": "Expected Closing Date",
        "type": "date",
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
      "preliminary-title-issues": {
        "label": "Preliminary Title Issues",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "seller-docs-needed": {
        "label": "Seller Docs Needed",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "payoff-needed": {
        "label": "Payoff Needed",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "probate-issue": {
        "label": "Probate Issue?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "lien-issue": {
        "label": "Lien Issue?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "open-permit-issue": {
        "label": "Open Permit Issue?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "boundary-legal-issue": {
        "label": "Boundary / Legal Issue?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "entity-signing-issue": {
        "label": "Entity / Signing Issue?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "primary-title-contact": {
        "label": "Primary Title Contact",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-contact-email": {
        "label": "Title Contact Email",
        "type": "email",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-contact-phone": {
        "label": "Title Contact Phone",
        "type": "phone",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-title-update": {
        "label": "Last Title Update",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "next-title-follow-up": {
        "label": "Next Title Follow-Up",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title-notes": {
        "label": "Title Notes",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "internal-notes": {
        "label": "Internal Notes",
        "type": "text",
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
      "resolved": {
        "label": "Resolved?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "cancelled-reason": {
        "label": "Cancelled Reason",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Seller Backed Out"
          },
          {
            "id": 2,
            "text": "Buyer Backed Out"
          },
          {
            "id": 3,
            "text": "Title Issue"
          },
          {
            "id": 4,
            "text": "Probate Delay"
          },
          {
            "id": 5,
            "text": "Docs Missing"
          },
          {
            "id": 6,
            "text": "Funding Issue"
          },
          {
            "id": 7,
            "text": "Other"
          }
        ]
      },
      "final-outcome-notes": {
        "label": "Final Outcome Notes",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30687665": {
    "app_id": 30687665,
    "app_name": "Pipelines (Automation Layer)",
    "item_name": "Flow",
    "fields": {
      "title": {
        "label": "Pipeline Title",
        "type": "text",
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
      "pipeline-id": {
        "label": "Pipeline ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "pipeline-status": {
        "label": "Pipeline Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Active"
          },
          {
            "id": 2,
            "text": "Stalled"
          },
          {
            "id": 3,
            "text": "Closed Won"
          },
          {
            "id": 4,
            "text": "Closed Lost"
          },
          {
            "id": 5,
            "text": "Archived"
          }
        ]
      },
      "current-stage": {
        "label": "Current Stage",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "New Lead"
          },
          {
            "id": 2,
            "text": "Contacted"
          },
          {
            "id": 3,
            "text": "Negotiating"
          },
          {
            "id": 4,
            "text": "Offer Sent"
          },
          {
            "id": 5,
            "text": "Offer Accepted"
          },
          {
            "id": 6,
            "text": "Contract Sent"
          },
          {
            "id": 7,
            "text": "Fully Executed"
          },
          {
            "id": 8,
            "text": "Routed to Title"
          },
          {
            "id": 9,
            "text": "Title Reviewing"
          },
          {
            "id": 10,
            "text": "Clear to Close"
          },
          {
            "id": 11,
            "text": "Closing Scheduled"
          },
          {
            "id": 12,
            "text": "Closed"
          },
          {
            "id": 13,
            "text": "Dead"
          }
        ]
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "property": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "prospect": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "conversation": {
        "label": "Conversation",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30646484
        ],
        "options": []
      },
      "offer": {
        "label": "Offer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30643944
        ],
        "options": []
      },
      "contract": {
        "label": "Contract",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687663
        ],
        "options": []
      },
      "title-routing": {
        "label": "Title Routing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687664
        ],
        "options": []
      },
      "closing": {
        "label": "Closing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687671
        ],
        "options": []
      },
      "buyer-match": {
        "label": "Buyer Match",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687666
        ],
        "options": []
      },
      "deal-revenue": {
        "label": "Deal Revenue",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687673
        ],
        "options": []
      },
      "assigned-agent": {
        "label": "Assigned Agent",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30678167
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "automation-status": {
        "label": "Automation Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Running"
          },
          {
            "id": 2,
            "text": "Waiting"
          },
          {
            "id": 3,
            "text": "Paused"
          },
          {
            "id": 4,
            "text": "Escalated"
          },
          {
            "id": 5,
            "text": "Complete"
          }
        ]
      },
      "current-engine": {
        "label": "Current Engine",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Acquisitions "
          },
          {
            "id": 2,
            "text": "Underwriting"
          },
          {
            "id": 3,
            "text": "Offers"
          },
          {
            "id": 4,
            "text": "Contracts"
          },
          {
            "id": 5,
            "text": "Title Routing"
          },
          {
            "id": 6,
            "text": "Closings"
          },
          {
            "id": 7,
            "text": "Buyer Match"
          },
          {
            "id": 8,
            "text": "Deal Revenue"
          }
        ]
      },
      "next-system-action": {
        "label": "Next System Action",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "next-action-date": {
        "label": "Next Action Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-automation-update": {
        "label": "Last Automation Update",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "section-separator": {
        "label": "Section Separator",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "deal-created-date": {
        "label": "Deal Created Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "last-stage-change": {
        "label": "Last Stage Change",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "expected-close-date": {
        "label": "Expected Close Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "actual-close-date": {
        "label": "Actual Close Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "number-of-days-in-current-stage": {
        "label": "Number of Days in Current Stage",
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
      "blocked": {
        "label": "Blocked?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "blocker-type": {
        "label": "Blocker Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Seller Delay"
          },
          {
            "id": 2,
            "text": "Buyer Delay"
          },
          {
            "id": 3,
            "text": "Title Issue"
          },
          {
            "id": 4,
            "text": "Probate"
          },
          {
            "id": 5,
            "text": "Missing Docs"
          },
          {
            "id": 6,
            "text": "Pricing Gap"
          },
          {
            "id": 7,
            "text": "Funding"
          },
          {
            "id": 8,
            "text": "Legal"
          },
          {
            "id": 9,
            "text": "No Response"
          },
          {
            "id": 10,
            "text": "Other"
          }
        ]
      },
      "blocker-summary": {
        "label": "Blocker Summary",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "escalation-needed": {
        "label": "Escalation Needed?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "won-lost-reason": {
        "label": "Won / Lost Reason",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Closed"
          },
          {
            "id": 2,
            "text": "Seller Backed Out"
          },
          {
            "id": 3,
            "text": "Buyer Backed Out"
          },
          {
            "id": 4,
            "text": "Price"
          },
          {
            "id": 5,
            "text": "Terms"
          },
          {
            "id": 6,
            "text": "Title"
          },
          {
            "id": 7,
            "text": "Probate"
          },
          {
            "id": 8,
            "text": "No Response"
          },
          {
            "id": 9,
            "text": "Competition"
          },
          {
            "id": 10,
            "text": "Other"
          }
        ]
      },
      "outcome-notes": {
        "label": "Outcome Notes",
        "type": "text",
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
      "pipeline-summary": {
        "label": "Pipeline Summary",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "internal-notes": {
        "label": "Internal Notes",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-next-move-summary": {
        "label": "AI Next Move Summary",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30687666": {
    "app_id": 30687666,
    "app_name": "Buyer Match",
    "item_name": "Buyer",
    "fields": {
      "title": {
        "label": "Buyer Match Title",
        "type": "text",
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
      "buyer-match-id": {
        "label": "Buyer Match ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "match-status": {
        "label": "Match Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Not Started"
          },
          {
            "id": 2,
            "text": "Matching"
          },
          {
            "id": 3,
            "text": "Buyers Selected"
          },
          {
            "id": 4,
            "text": "Sent to Buyers"
          },
          {
            "id": 5,
            "text": "Buyers Interested"
          },
          {
            "id": 6,
            "text": "Buyers Chosen"
          },
          {
            "id": 7,
            "text": "Assigned"
          },
          {
            "id": 8,
            "text": "Closed"
          },
          {
            "id": 9,
            "text": "Dead"
          }
        ]
      },
      "disposition-strategy": {
        "label": "Disposition Strategy",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Assignment"
          },
          {
            "id": 2,
            "text": "Double Close"
          },
          {
            "id": 3,
            "text": "Novation"
          },
          {
            "id": 4,
            "text": "Hold"
          },
          {
            "id": 5,
            "text": "Hybrid"
          }
        ]
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "pipe": {
        "label": "Pipe",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687665
        ],
        "options": []
      },
      "contract": {
        "label": "Contract",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687663
        ],
        "options": []
      },
      "property": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "offer": {
        "label": "Offer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30643944
        ],
        "options": []
      },
      "closing": {
        "label": "Closing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687671
        ],
        "options": []
      },
      "deal-revenue": {
        "label": "Deal Revenue",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687673
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
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "primary-buyer": {
        "label": "Primary Buyer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644240
        ],
        "options": []
      },
      "backup-buyer-1": {
        "label": "Backup Buyer #1",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644240
        ],
        "options": []
      },
      "backup-buyer-2": {
        "label": "Backup Buyer #2",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644240
        ],
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
      "buyer-type-match": {
        "label": "Buyer Type Match",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Cash Buyer"
          },
          {
            "id": 2,
            "text": "Landlord"
          },
          {
            "id": 3,
            "text": "Rehabber"
          },
          {
            "id": 4,
            "text": "Multifamily Buyer"
          },
          {
            "id": 5,
            "text": "Hedge Fund"
          },
          {
            "id": 6,
            "text": "Hotel / Commercial"
          },
          {
            "id": 7,
            "text": "Unknown"
          }
        ]
      },
      "buyer-match-score": {
        "label": "Buyer Match Score",
        "type": "progress",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "reason-for-match": {
        "label": "Reason For Match",
        "type": "text",
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
      "package-sent-date": {
        "label": "Package Sent Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "buyer-response-status": {
        "label": "Buyer Response Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Not Sent"
          },
          {
            "id": 2,
            "text": "Sent"
          },
          {
            "id": 3,
            "text": "Opened"
          },
          {
            "id": 4,
            "text": "Interested"
          },
          {
            "id": 5,
            "text": "Passed"
          },
          {
            "id": 6,
            "text": "Needs More Info"
          },
          {
            "id": 7,
            "text": "Offer Submitted"
          },
          {
            "id": 8,
            "text": "Selected"
          }
        ]
      },
      "buyer-offered-price": {
        "label": "Buyer Offered Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "buyer-notes": {
        "label": "Buyer Notes",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "buyer-proof-of-funds-received": {
        "label": "Buyer Proof of Funds Received?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "buyer-emd-ready": {
        "label": "Buyer EMD Ready?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "field-5": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "assignment-fee": {
        "label": "Assignment Fee",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "final-acquisition-price": {
        "label": "Final Acquisition Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "final-disposition-price": {
        "label": "Final Disposition Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "assignment-status": {
        "label": "Assignment Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Not Started"
          },
          {
            "id": 2,
            "text": "In Progress"
          },
          {
            "id": 3,
            "text": "Buyer Confirmed"
          },
          {
            "id": 4,
            "text": "Assigned"
          },
          {
            "id": 5,
            "text": "Closed"
          },
          {
            "id": 6,
            "text": "Cancelled"
          }
        ]
      },
      "selected-buyer": {
        "label": "Selected Buyer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644240
        ],
        "options": []
      },
      "buyer-assigned-date": {
        "label": "Buyer Assigned Date",
        "type": "date",
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
      "buyer-match-start-date": {
        "label": "Buyer Match Start Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "next-buyer-follow-up": {
        "label": "Next Buyer Follow Up",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "urgency-level": {
        "label": "Urgency Level",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Low"
          },
          {
            "id": 2,
            "text": "Medium"
          },
          {
            "id": 3,
            "text": "High"
          },
          {
            "id": 4,
            "text": "Urgent"
          }
        ]
      },
      "automation-status": {
        "label": "Automation Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Running"
          },
          {
            "id": 2,
            "text": "Waiting"
          },
          {
            "id": 3,
            "text": "Paused"
          },
          {
            "id": 4,
            "text": "Complete"
          }
        ]
      },
      "field-7": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "dispo-outcome": {
        "label": "Dispo Outcome",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Buyer Secured"
          },
          {
            "id": 2,
            "text": "Buyer Passed"
          },
          {
            "id": 3,
            "text": "No Buyer Found"
          },
          {
            "id": 4,
            "text": "Deal Cancelled"
          },
          {
            "id": 5,
            "text": "Seller Backed Out"
          },
          {
            "id": 6,
            "text": "Closed"
          }
        ]
      },
      "internal-notes": {
        "label": "Internal Notes",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ai-buyer-match-summary": {
        "label": "AI Buyer Match Summary",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30687667": {
    "app_id": 30687667,
    "app_name": "Title Companies",
    "item_name": "Company",
    "fields": {
      "title": {
        "label": "Company Name",
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
          30644726
        ],
        "options": []
      },
      "rating": {
        "label": "Rating",
        "type": "number",
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
      "address": {
        "label": "Address",
        "type": "location",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contact-manager": {
        "label": "Contact Manager",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "new-order-email": {
        "label": "New Order Email",
        "type": "email",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "phone": {
        "label": "Phone",
        "type": "phone",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "underwriter": {
        "label": "Underwriter",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "WFG National"
          },
          {
            "id": 2,
            "text": "Williston Financial"
          },
          {
            "id": 3,
            "text": "Old Republic"
          },
          {
            "id": 4,
            "text": "Westcor"
          },
          {
            "id": 5,
            "text": "First American"
          },
          {
            "id": 6,
            "text": "Fidelity National"
          },
          {
            "id": 7,
            "text": "Chicago Title"
          },
          {
            "id": 8,
            "text": "Stewart Title"
          },
          {
            "id": 9,
            "text": "North American"
          },
          {
            "id": 10,
            "text": "Shaddock National"
          },
          {
            "id": 11,
            "text": "Pioneer Holding"
          },
          {
            "id": 12,
            "text": "Independent"
          },
          {
            "id": 13,
            "text": "Investors Title Ins"
          }
        ]
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
  "30687669": {
    "app_id": 30687669,
    "app_name": "Contract Templates",
    "item_name": "Contract",
    "fields": {
      "title": {
        "label": "Template Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "template-id": {
        "label": "Template ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "state": {
        "label": "State",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract-type": {
        "label": "Contract Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Cash"
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
            "text": "Multifamily"
          }
        ]
      },
      "template-type": {
        "label": "Template Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Standard Purchase"
          },
          {
            "id": 2,
            "text": "Assignment"
          },
          {
            "id": 3,
            "text": "Creative"
          },
          {
            "id": 4,
            "text": "Multifamily"
          },
          {
            "id": 5,
            "text": "Addendum"
          },
          {
            "id": 6,
            "text": "Disclosure"
          }
        ]
      },
      "version": {
        "label": "Version",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "active": {
        "label": "Active",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "field": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "docusign-template-id": {
        "label": "DocuSign Template ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "docusign-template-name": {
        "label": "DocuSign Template Name",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "template-source": {
        "label": "Template Source",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "File Upload"
          },
          {
            "id": 2,
            "text": "DocuSign Template"
          },
          {
            "id": 3,
            "text": "Google Drive"
          },
          {
            "id": 4,
            "text": "External Link"
          }
        ]
      },
      "field-2": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "use-for-auto-generation": {
        "label": "Use for Auto Generation?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "priority": {
        "label": "Priority",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "default-for-state-type": {
        "label": "Default for State + Type",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "assignment-allowed": {
        "label": "Assignment Allowed",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "default-closing-timeline-days": {
        "label": "Default Closing Timeline (Days)",
        "type": "number",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "special-conditions": {
        "label": "Special Conditions",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "template-status": {
        "label": "Template Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Draft"
          },
          {
            "id": 2,
            "text": "Active"
          },
          {
            "id": 3,
            "text": "Deprecated"
          },
          {
            "id": 4,
            "text": "Archived"
          }
        ]
      },
      "last-updated": {
        "label": "Last Updated",
        "type": "date",
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
  "30687671": {
    "app_id": 30687671,
    "app_name": "Closings",
    "item_name": "Closing",
    "fields": {
      "closing-title": {
        "label": "Closing Title",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "title": {
        "label": "Closing ID",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "closing-status": {
        "label": "Closing Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Not Scheduled"
          },
          {
            "id": 2,
            "text": "Scheduled"
          },
          {
            "id": 3,
            "text": "Confirmed"
          },
          {
            "id": 4,
            "text": "Rescheduled"
          },
          {
            "id": 5,
            "text": "Completed"
          },
          {
            "id": 6,
            "text": "Cancelled"
          }
        ]
      },
      "field": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "contract": {
        "label": "Contract",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687663
        ],
        "options": []
      },
      "property": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "prospect": {
        "label": "Prospect",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30637173
        ],
        "options": []
      },
      "title-routing": {
        "label": "Title Routing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687664
        ],
        "options": []
      },
      "buyer-match": {
        "label": "Buyer Match",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687666
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
        "options": []
      },
      "title-company": {
        "label": "Title Company",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687667
        ],
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
      "closing-date-time": {
        "label": "Closing Date / Time",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "confirmed-date": {
        "label": "Confirmed Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "rescheduled-date": {
        "label": "Rescheduled Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "timezone": {
        "label": "Timezone",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "ready-to-close": {
        "label": "Ready To Close?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "docs-complete": {
        "label": "Docs Complete?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "funds-ready": {
        "label": "Funds Ready?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "outstanding-items": {
        "label": "Outstanding Items",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "pre-close-notes": {
        "label": "Pre-Close Notes",
        "type": "text",
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
      "actual-closing-date": {
        "label": "Actual Closing Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "closed-successfully": {
        "label": "Closed Successfully?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "closer-delay-reasons": {
        "label": "Closer Delay Reasons",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Seller Delay"
          },
          {
            "id": 2,
            "text": "Buyer Delay"
          },
          {
            "id": 3,
            "text": "Title Delay"
          },
          {
            "id": 4,
            "text": "Docs Missing"
          },
          {
            "id": 5,
            "text": "Funding Delay"
          },
          {
            "id": 6,
            "text": "Rescheduled"
          },
          {
            "id": 7,
            "text": "Cancelled"
          }
        ]
      },
      "post-close-notes": {
        "label": "Post-Close Notes",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  },
  "30687673": {
    "app_id": 30687673,
    "app_name": "Deal Revenue",
    "item_name": "Cash Flow",
    "fields": {
      "title": {
        "label": "Revenue ID",
        "type": "text",
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
      "contract": {
        "label": "Contract",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687663
        ],
        "options": []
      },
      "closing": {
        "label": "Closing",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687671
        ],
        "options": []
      },
      "property": {
        "label": "Property",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30636444
        ],
        "options": []
      },
      "master-owner": {
        "label": "Master Owner",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30679234
        ],
        "options": []
      },
      "buyer": {
        "label": "Buyer",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687666
        ],
        "options": []
      },
      "title-company": {
        "label": "Title Company",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30687667
        ],
        "options": []
      },
      "market": {
        "label": "Market",
        "type": "app",
        "multiple": true,
        "allowed_currencies": null,
        "referenced_app_ids": [
          30644726
        ],
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
      "purchase-price": {
        "label": "Purchase Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "sold-price": {
        "label": "Sold Price",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "assignment-fee": {
        "label": "Assignment Fee",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "expected-wire-date": {
        "label": "Expected Wire Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "field-3": {
        "label": "-",
        "type": "separator",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "wire-received": {
        "label": "Wire Received?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "wire-received-date": {
        "label": "Wire Received Date",
        "type": "date",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "wire-received-amount": {
        "label": "Wire Received Amount",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
        "referenced_app_ids": [],
        "options": []
      },
      "partial-payment": {
        "label": "Partial Payment?",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Yes"
          },
          {
            "id": 2,
            "text": "No"
          }
        ]
      },
      "remaining-balance": {
        "label": "Remaining Balance",
        "type": "money",
        "multiple": false,
        "allowed_currencies": [
          "USD",
          "EUR"
        ],
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
      "revenue-status": {
        "label": "Revenue Status",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": [
          {
            "id": 1,
            "text": "Pending"
          },
          {
            "id": 2,
            "text": "Expected Soon"
          },
          {
            "id": 3,
            "text": "Partially Received"
          },
          {
            "id": 4,
            "text": "Received"
          },
          {
            "id": 5,
            "text": "Short Paid"
          },
          {
            "id": 6,
            "text": "Exception"
          }
        ]
      },
      "account-wired-to": {
        "label": "Account Wired To",
        "type": "category",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      },
      "wire-confirmation-number": {
        "label": "Wire Confirmation Number",
        "type": "text",
        "multiple": false,
        "allowed_currencies": null,
        "referenced_app_ids": [],
        "options": []
      }
    }
  }
});

export default PODIO_ATTACHED_SCHEMA;
