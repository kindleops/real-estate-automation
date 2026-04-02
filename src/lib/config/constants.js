export const QUEUE_STATUS = {
  QUEUED: "Queued",
  PROCESSING: "Processing",
  SENT: "Sent",
  DELIVERED: "Delivered",
  FAILED: "Failed",
  BLOCKED: "Blocked",
};

export const DELIVERY_CONFIRMED = {
  PENDING: "⏳ Pending",
  CONFIRMED: "✅ Confirmed",
  FAILED: "❌ Failed",
};

export const MESSAGE_TYPE = {
  COLD_OUTBOUND: "Cold Outbound",
  FOLLOW_UP: "Follow-Up",
  REENGAGEMENT: "Re-Engagement",
  OPT_OUT_CONFIRM: "Opt-Out Confirm",
};

export const SEND_PRIORITY = {
  URGENT: "_ Urgent",
  NORMAL: "_ Normal",
  LOW: "_ Low",
};

export const DNC_CHECK = {
  CLEARED: "✅ Cleared",
  BLOCKED: "_ Blocked",
};

export const DIRECTION = {
  INBOUND: "Inbound",
  OUTBOUND: "Outbound",
};

export const FAILURE_BUCKET = {
  CARRIER: "Carrier Failure",
  DELIVERABILITY: "Deliverability Failure",
  COMPLIANCE: "Compliance Block",
  INVALID_NUMBER: "Invalid Number",
  PROVIDER: "Provider Failure",
  UNKNOWN: "Unknown Failure",
};

export const DEFAULTS = {
  QUEUE_RUN_LIMIT: 25,
  RETRY_RUN_LIMIT: 25,
  MAX_RETRIES: 3,
  TOUCH_NUMBER_START: 1,
  DEFAULT_CONTACT_WINDOW: "9AM-8PM CT",
  DEFAULT_TIMEZONE: "Central",
  DEFAULT_LANGUAGE: "English",
  DEFAULT_STAGE: "Ownership",
  DEFAULT_PERSONA: "Warm Professional",
  DEFAULT_TONE: "Warm",
  DEFAULT_VARIANT_GROUP: "Human Soft",
  AI_CONFIDENCE_THRESHOLD: 0.82,
};

export const TEXTGRID_WEBHOOK_EVENT = {
  INBOUND: "inbound",
  DELIVERY: "delivery",
};

export const SELLER_PROFILE = {
  PROBATE: "Probate",
  TIRED_LANDLORD: "Tired Landlord",
  STRATEGIC_SELLER: "Strategic Seller",
};