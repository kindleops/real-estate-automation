export const STAGES = {
  OWNERSHIP: "Ownership",
  OFFER: "Offer",
  QA: "Q/A",
  CONTRACT: "Contract",
  FOLLOW_UP: "Follow-Up",
};

export const LIFECYCLE_STAGES = {
  OWNERSHIP: STAGES.OWNERSHIP,
  OFFER: STAGES.OFFER,
  QA: STAGES.QA,
  CONTRACT: STAGES.CONTRACT,
  TITLE: "Title",
  CLOSING: "Closing",
  DISPOSITION: "Disposition",
  FOLLOW_UP: STAGES.FOLLOW_UP,
  POST_CLOSE: "Post-Close",
};

export const STAGE_LIST = [
  STAGES.OWNERSHIP,
  STAGES.OFFER,
  STAGES.QA,
  STAGES.CONTRACT,
  STAGES.FOLLOW_UP,
];

export const LIFECYCLE_STAGE_LIST = [
  LIFECYCLE_STAGES.OWNERSHIP,
  LIFECYCLE_STAGES.OFFER,
  LIFECYCLE_STAGES.QA,
  LIFECYCLE_STAGES.CONTRACT,
  LIFECYCLE_STAGES.TITLE,
  LIFECYCLE_STAGES.CLOSING,
  LIFECYCLE_STAGES.DISPOSITION,
  LIFECYCLE_STAGES.FOLLOW_UP,
  LIFECYCLE_STAGES.POST_CLOSE,
];

export function isValidStage(value) {
  return STAGE_LIST.includes(String(value || "").trim());
}

export function isValidLifecycleStage(value) {
  return LIFECYCLE_STAGE_LIST.includes(String(value || "").trim());
}

export function normalizeStage(value, fallback = STAGES.OWNERSHIP) {
  const raw = String(value || "").trim();

  if (!raw) return fallback;

  const normalized = raw.toLowerCase();

  if (normalized === "ownership") return STAGES.OWNERSHIP;
  if (normalized === "offer") return STAGES.OFFER;
  if (normalized === "q/a" || normalized === "qa" || normalized === "q & a") {
    return STAGES.QA;
  }
  if (normalized === "contract") return STAGES.CONTRACT;
  if (normalized === "follow-up" || normalized === "follow up" || normalized === "followup") {
    return STAGES.FOLLOW_UP;
  }

  return fallback;
}

export function normalizeLifecycleStage(
  value,
  fallback = LIFECYCLE_STAGES.OWNERSHIP
) {
  const raw = String(value || "").trim();

  if (!raw) return fallback;

  const normalized = raw.toLowerCase();

  if (normalized === "ownership") return LIFECYCLE_STAGES.OWNERSHIP;
  if (normalized === "offer") return LIFECYCLE_STAGES.OFFER;
  if (normalized === "q/a" || normalized === "qa" || normalized === "q & a") {
    return LIFECYCLE_STAGES.QA;
  }
  if (normalized === "contract") return LIFECYCLE_STAGES.CONTRACT;
  if (normalized === "title") return LIFECYCLE_STAGES.TITLE;
  if (normalized === "closing" || normalized === "close") {
    return LIFECYCLE_STAGES.CLOSING;
  }
  if (normalized === "disposition" || normalized === "dispo") {
    return LIFECYCLE_STAGES.DISPOSITION;
  }
  if (normalized === "post-close" || normalized === "post close" || normalized === "postclose") {
    return LIFECYCLE_STAGES.POST_CLOSE;
  }
  if (normalized === "follow-up" || normalized === "follow up" || normalized === "followup") {
    return LIFECYCLE_STAGES.FOLLOW_UP;
  }

  return fallback;
}

export function collapseLifecycleStage(
  lifecycle_stage,
  fallback = STAGES.OWNERSHIP
) {
  const normalized = normalizeLifecycleStage(lifecycle_stage, fallback);

  if (normalized === LIFECYCLE_STAGES.TITLE) return STAGES.CONTRACT;
  if (normalized === LIFECYCLE_STAGES.CLOSING) return STAGES.CONTRACT;
  if (normalized === LIFECYCLE_STAGES.DISPOSITION) return STAGES.CONTRACT;
  if (normalized === LIFECYCLE_STAGES.POST_CLOSE) return STAGES.FOLLOW_UP;

  return normalizeStage(normalized, fallback);
}

export default STAGES;
