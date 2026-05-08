function clean(value) {
  return String(value ?? "").trim();
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const text = clean(value);
    if (text) return text;
  }
  return "";
}

function firstNumber(...values) {
  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;
    const n = Number(String(value).replace(/[,$\s]/g, ""));
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function asIso(value) {
  const text = clean(value);
  if (!text) return null;
  const ts = new Date(text).toISOString();
  return ts && ts !== "Invalid Date" ? ts : null;
}

function parseIds(rows = [], ...keys) {
  const ids = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    for (const key of keys) {
      const value = clean(row?.[key]);
      if (value) ids.add(value);
    }
  }
  return [...ids];
}

function omitSensitiveFields(input, mode = "internal") {
  if (!input || typeof input !== "object") return input;

  const out = Array.isArray(input) ? [] : {};
  const blocked = ["mao", "walkaway", "internal_valuation", "arv_internal", "max_offer_internal"];

  for (const [key, value] of Object.entries(input)) {
    const normalized = clean(key).toLowerCase();
    const should_block = mode === "ai_safe" && blocked.some((needle) => normalized.includes(needle));
    if (should_block) continue;
    out[key] = typeof value === "object" && value !== null
      ? omitSensitiveFields(value, mode)
      : value;
  }

  return out;
}

async function safeSelect(supabase, table, queryBuilder) {
  try {
    const query = supabase.from(table);
    const result = await queryBuilder(query);
    if (result?.error) {
      return { ok: false, table, rows: [], error: result.error.message };
    }
    return { ok: true, table, rows: Array.isArray(result?.data) ? result.data : [] };
  } catch (error) {
    return { ok: false, table, rows: [], error: error?.message || "query_failed" };
  }
}

export async function loadThreadContext({ thread_key, supabase }) {
  const [messageEventsRes, sendQueueRes, brainRes] = await Promise.all([
    safeSelect(supabase, "inbox_chat_timeline_hydrated", (q) =>
      q.select("*").eq("thread_key", thread_key).order("event_timestamp", { ascending: false }).limit(200)
    ).then(async (res) => {
      // Fallback if the view is missing or empty
      if (!res.ok || res.rows.length === 0) {
        return safeSelect(supabase, "inbox_messages_hydrated", (q) =>
          q.select("*").eq("thread_key", thread_key).order("created_at", { ascending: false }).limit(200)
        );
      }
      return res;
    }),
    safeSelect(supabase, "send_queue", (q) =>
      q.select("*").eq("thread_key", thread_key).order("created_at", { ascending: false }).limit(200)
    ),
    safeSelect(supabase, "ai_conversation_brain", (q) =>
      q.select("*").eq("thread_key", thread_key).order("updated_at", { ascending: false }).limit(20)
    ),
  ]);

  const seedRows = [...messageEventsRes.rows, ...sendQueueRes.rows, ...brainRes.rows];
  const masterOwnerIds = parseIds(seedRows, "master_owner_id");
  const propertyIds = parseIds(seedRows, "property_id");
  const ownerIds = parseIds(seedRows, "owner_id", "prospect_id");
  const marketIds = parseIds(seedRows, "market_id");
  const phoneIds = parseIds(seedRows, "phone_number_id");

  const [
    propertiesRes,
    masterOwnersRes,
    ownersRes,
    prospectsRes,
    phoneNumbersRes,
    emailsRes,
    marketsRes,
    zipCodesRes,
    offersRes,
    underwritingRes,
    contractsRes,
    titleRoutingRes,
    closingsRes,
    buyerMatchRes,
    agentsRes,
    templatesRes,
    stateRes,
  ] = await Promise.all([
    safeSelect(supabase, "properties", (q) => (propertyIds.length ? q.select("*").in("id", propertyIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "master_owners", (q) => (masterOwnerIds.length ? q.select("*").in("id", masterOwnerIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "owners", (q) => (ownerIds.length ? q.select("*").in("id", ownerIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "prospects", (q) => (ownerIds.length ? q.select("*").in("id", ownerIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "phone_numbers", (q) => (phoneIds.length ? q.select("*").in("id", phoneIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "emails", (q) => (ownerIds.length ? q.select("*").in("owner_id", ownerIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "markets", (q) => (marketIds.length ? q.select("*").in("id", marketIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "zip_codes", (q) => (marketIds.length ? q.select("*").in("market_id", marketIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "offers", (q) => (masterOwnerIds.length ? q.select("*").in("master_owner_id", masterOwnerIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "underwriting", (q) => (propertyIds.length ? q.select("*").in("property_id", propertyIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "contracts", (q) => (masterOwnerIds.length ? q.select("*").in("master_owner_id", masterOwnerIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "title_routing_closing_engine", (q) => (propertyIds.length ? q.select("*").in("property_id", propertyIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "closings", (q) => (propertyIds.length ? q.select("*").in("property_id", propertyIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "buyer_match", (q) => (propertyIds.length ? q.select("*").in("property_id", propertyIds) : Promise.resolve({ data: [] }))),
    safeSelect(supabase, "agents", (q) => q.select("*").limit(100)),
    safeSelect(supabase, "templates", (q) => q.select("*").limit(200)),
    safeSelect(supabase, "inbox_thread_state", (q) => q.select("*").eq("thread_key", thread_key).limit(1)),
  ]);

  const unified = {
    thread_key,
    selected_thread: {
      message_events: messageEventsRes.rows,
      send_queue: sendQueueRes.rows,
      ai_conversation_brain: brainRes.rows,
      properties: propertiesRes.rows,
      master_owners: masterOwnersRes.rows,
      owners: ownersRes.rows,
      prospects: prospectsRes.rows,
      phone_numbers: phoneNumbersRes.rows,
      emails: emailsRes.rows,
      markets: marketsRes.rows,
      zip_codes: zipCodesRes.rows,
      offers: offersRes.rows,
      underwriting: underwritingRes.rows,
      contracts: contractsRes.rows,
      title_routing_closing_engine: titleRoutingRes.rows,
      closings: closingsRes.rows,
      buyer_match: buyerMatchRes.rows,
      agents: agentsRes.rows,
      templates: templatesRes.rows,
      inbox_thread_state: stateRes.rows[0] || null,
    },
  };

  const primaryMasterOwner = masterOwnersRes.rows[0] || {};
  const primaryOwner = ownersRes.rows[0] || {};
  const primaryProspect = prospectsRes.rows[0] || {};
  const primaryPhone = phoneNumbersRes.rows[0] || {};
  const primaryProperty = propertiesRes.rows[0] || {};
  const primaryState = stateRes.rows[0] || {};
  const latestMessage = messageEventsRes.rows[0] || {};

  const ownerFullName = firstNonEmpty(
    primaryMasterOwner.owner_full_name,
    primaryMasterOwner.full_name,
    primaryMasterOwner.owner_name,
    primaryOwner.owner_full_name,
    primaryOwner.full_name,
    primaryOwner.name,
    primaryProspect.full_name,
    primaryProspect.name
  );

  const ownerType = firstNonEmpty(
    primaryMasterOwner.owner_type,
    primaryOwner.owner_type,
    primaryOwner.type,
    primaryProspect.owner_type
  );

  const sellerOwnerIntelligence = {
    contact_identity: {
      name_of_contact: firstNonEmpty(primaryProspect.name, primaryProspect.full_name, primaryOwner.name),
      owner_full_name: ownerFullName || null,
      owner_type: ownerType || null,
      contact_matching_tags: primaryProspect.contact_matching_tags ?? primaryMasterOwner.contact_matching_tags ?? [],
      gender: firstNonEmpty(primaryProspect.gender, primaryOwner.gender) || null,
      language: firstNonEmpty(primaryProspect.language, primaryOwner.language, primaryMasterOwner.language) || null,
      age_bracket: firstNonEmpty(primaryProspect.age_bracket, primaryOwner.age_bracket) || null,
      age: firstNumber(primaryProspect.age, primaryOwner.age),
    },
    demographics_capacity: {
      household_income: firstNumber(primaryProspect.household_income, primaryOwner.household_income),
      education_level: firstNonEmpty(primaryProspect.education_level, primaryOwner.education_level) || null,
      buyer_power: firstNonEmpty(primaryProspect.buyer_power, primaryOwner.buyer_power) || null,
      net_asset_value: firstNumber(primaryProspect.net_asset_value, primaryOwner.net_asset_value, primaryMasterOwner.net_asset_value),
    },
    prospect_intelligence: {
      prospect_tags: primaryProspect.tags ?? primaryProspect.prospect_tags ?? [],
      last_intent: firstNonEmpty(primaryState.ui_intent, latestMessage.ai_route, latestMessage.event_type) || null,
      last_inbound: firstNonEmpty(latestMessage.received_at, latestMessage.event_timestamp, latestMessage.created_at) || null,
      best_contact_window: firstNonEmpty(primaryProspect.best_contact_window, primaryOwner.best_contact_window, primaryMasterOwner.best_contact_window) || null,
      timezone: firstNonEmpty(primaryProspect.timezone, primaryOwner.timezone, primaryMasterOwner.timezone) || null,
    },
    portfolio_intelligence: {
      portfolio_value: firstNumber(primaryMasterOwner.portfolio_value, primaryOwner.portfolio_value),
      total_equity: firstNumber(primaryMasterOwner.total_equity, primaryOwner.total_equity),
      total_debt: firstNumber(primaryMasterOwner.total_debt, primaryOwner.total_debt),
      monthly_debt_payment: firstNumber(primaryMasterOwner.monthly_debt_payment, primaryOwner.monthly_debt_payment),
      portfolio_property_count: firstNumber(primaryMasterOwner.portfolio_property_count, primaryOwner.portfolio_property_count),
      units: firstNumber(primaryMasterOwner.units, primaryOwner.units, primaryProperty.unit_count),
      financial_pressure_score: firstNumber(primaryMasterOwner.financial_pressure_score, primaryOwner.financial_pressure_score),
      urgency_score: firstNumber(primaryMasterOwner.urgency_score, primaryOwner.urgency_score),
    },
    risk_distress: {
      portfolio_tax_delinquent_count: firstNumber(primaryMasterOwner.portfolio_tax_delinquent_count, primaryOwner.portfolio_tax_delinquent_count),
      portfolio_lien_count: firstNumber(primaryMasterOwner.portfolio_lien_count, primaryOwner.portfolio_lien_count),
      total_tax_amount: firstNumber(primaryMasterOwner.total_tax_amount, primaryOwner.total_tax_amount),
      sfr_count: firstNumber(primaryMasterOwner.sfr_count, primaryOwner.sfr_count),
      mf_count: firstNumber(primaryMasterOwner.mf_count, primaryOwner.mf_count),
    },
    phone_intelligence: {
      best_phone: firstNonEmpty(primaryPhone.canonical_e164, primaryPhone.phone_number, latestMessage.from_phone_number, latestMessage.to_phone_number) || null,
      carrier: firstNonEmpty(primaryPhone.carrier, primaryPhone.provider_name) || null,
      activity_status: firstNonEmpty(primaryPhone.activity_status, primaryPhone.status) || null,
      two_month_usage: firstNumber(primaryPhone.two_month_usage, primaryPhone.usage_2m),
      twelve_month_usage: firstNumber(primaryPhone.twelve_month_usage, primaryPhone.usage_12m),
    },
  };

  const latestInbound = messageEventsRes.rows.find((row) => clean(row.direction).toLowerCase() === "inbound") || {};
  const latestQueue = sendQueueRes.rows[0] || {};
  const latestBrain = brainRes.rows[0] || {};
  const selectedTemplate = templatesRes.rows.find((row) =>
    clean(row.id) && clean(latestQueue.selected_template_id) && clean(row.id) === clean(latestQueue.selected_template_id)
  ) || templatesRes.rows[0] || {};
  const selectedAgent = agentsRes.rows.find((row) =>
    clean(row.id) && clean(latestQueue.sms_agent_id) && clean(row.id) === clean(latestQueue.sms_agent_id)
  ) || agentsRes.rows[0] || {};

  const automationDecision = {
    inbound_detection: {
      reply_detected: Boolean(clean(latestInbound.id)),
      latest_inbound_text: firstNonEmpty(latestInbound.message_body, latestInbound.redacted_body) || null,
      latest_inbound_at: asIso(firstNonEmpty(latestInbound.received_at, latestInbound.event_timestamp, latestInbound.created_at)),
      from_phone: firstNonEmpty(latestInbound.from_phone_number) || null,
      to_textgrid_number: firstNonEmpty(latestInbound.to_phone_number) || null,
    },
    classification: {
      detected_intent: firstNonEmpty(latestBrain.intent, latestBrain.route, primaryState.ui_intent) || null,
      confidence: firstNumber(latestBrain.confidence, latestBrain.intent_confidence),
      language: firstNonEmpty(latestBrain.language, primaryProspect.language, primaryOwner.language) || null,
      sentiment: firstNonEmpty(latestBrain.sentiment, latestBrain.emotion) || null,
      objection_type: firstNonEmpty(latestBrain.objection, latestBrain.primary_objection_type) || null,
      seller_stage_before: firstNonEmpty(latestInbound.stage_before, latestBrain.stage_before) || null,
      seller_stage_after: firstNonEmpty(latestInbound.stage_after, latestBrain.stage_after, primaryState.stage) || null,
      source: firstNonEmpty(latestBrain.source, "ai_conversation_brain") || null,
      classified_at: asIso(firstNonEmpty(latestBrain.updated_at, latestInbound.created_at)),
    },
    template_selection: {
      template_name: firstNonEmpty(selectedTemplate.name, selectedTemplate.template_name, latestQueue.use_case_template) || null,
      template_id: firstNonEmpty(selectedTemplate.id, latestQueue.selected_template_id) || null,
      use_case: firstNonEmpty(latestQueue.use_case_template, selectedTemplate.use_case) || null,
      stage: firstNonEmpty(latestQueue.current_stage, selectedTemplate.stage) || null,
      agent_name: firstNonEmpty(selectedAgent.name, selectedAgent.agent_name) || null,
      agent_persona: firstNonEmpty(selectedAgent.persona, selectedTemplate.agent_style) || null,
      template_source: firstNonEmpty(selectedTemplate.source, latestQueue.template_source, "Podio") || null,
      rendered_reply_preview: firstNonEmpty(latestQueue.message_body, latestQueue.message_text) || null,
    },
    decision: {
      automation_status: firstNonEmpty(latestQueue.queue_status, primaryState.status, "WAITING") || null,
      action_taken: firstNonEmpty(latestQueue.message_type, latestQueue.action_taken, "queued_for_review") || null,
      safety_gate_result: firstNonEmpty(latestQueue.safety_gate_result, latestQueue.compliance_status, "unknown") || null,
      blocked_reason: firstNonEmpty(latestQueue.blocked_reason, latestQueue.failure_reason, latestQueue.error_message) || null,
      queue_id: firstNonEmpty(latestQueue.id, latestQueue.queue_id) || null,
      scheduled_for: asIso(firstNonEmpty(latestQueue.scheduled_for, latestQueue.scheduled_for_utc, latestQueue.scheduled_for_local)),
      sent_at: asIso(firstNonEmpty(latestQueue.sent_at, latestQueue.completed_at)),
      delivery_status: firstNonEmpty(latestQueue.delivery_status, latestInbound.delivery_status) || null,
      next_follow_up_at: asIso(firstNonEmpty(latestBrain.next_follow_up_due_at, latestQueue.next_follow_up_at)),
      active_stage: firstNonEmpty(primaryState.stage, latestQueue.current_stage) || null,
      next_stage: firstNonEmpty(latestBrain.next_stage, latestQueue.next_stage) || null,
    },
    raw_debug: {
      latest_inbound_id: firstNonEmpty(latestInbound.id) || null,
      latest_queue_id: firstNonEmpty(latestQueue.id) || null,
      latest_brain_id: firstNonEmpty(latestBrain.id) || null,
      template_row_id: firstNonEmpty(selectedTemplate.id) || null,
      agent_row_id: firstNonEmpty(selectedAgent.id) || null,
    },
  };

  const automationTimeline = [
    ...messageEventsRes.rows.map((row) => ({
      timestamp: asIso(firstNonEmpty(row.event_timestamp, row.created_at)),
      event_type: clean(row.direction).toLowerCase() === "inbound" ? "inbound_received" : "outbound_sent",
      status: firstNonEmpty(row.delivery_status, row.event_type, "ok"),
      actor: clean(row.direction).toLowerCase() === "inbound" ? "TextGrid" : "system",
      detail: firstNonEmpty(row.message_body, row.event_type, "message_event"),
      queue_id: firstNonEmpty(row.queue_id) || null,
      message_id: firstNonEmpty(row.id, row.message_id) || null,
      template_id: firstNonEmpty(row.template_id) || null,
    })),
    ...sendQueueRes.rows.map((row) => ({
      timestamp: asIso(firstNonEmpty(row.scheduled_for, row.created_at)),
      event_type: firstNonEmpty(row.queue_status) === "queued" ? "outbound_queued" : "auto_reply_queued",
      status: firstNonEmpty(row.queue_status, "queued"),
      actor: "system",
      detail: firstNonEmpty(row.message_type, row.use_case_template, "queue_event"),
      queue_id: firstNonEmpty(row.id, row.queue_id) || null,
      message_id: null,
      template_id: firstNonEmpty(row.selected_template_id) || null,
    })),
    ...brainRes.rows.map((row) => ({
      timestamp: asIso(firstNonEmpty(row.updated_at, row.created_at)),
      event_type: "reply_classified",
      status: "classified",
      actor: "AI",
      detail: firstNonEmpty(row.intent, row.route, "classification"),
      queue_id: null,
      message_id: null,
      template_id: null,
    })),
  ]
    .filter((event) => Boolean(event.timestamp))
    .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());

  const normalizedDossier = {
    ownerFullName: ownerFullName || null,
    ownerType: ownerType || null,
    sellerOwnerIntelligence,
    seller_owner_intelligence: sellerOwnerIntelligence,
    automationDecision,
    automationTimeline,
    properties: unified.selected_thread.properties,
    seller_profile: unified.selected_thread.master_owners,
    offer_summary: unified.selected_thread.offers,
    underwriting: unified.selected_thread.underwriting,
    contracts: unified.selected_thread.contracts,
    closing: unified.selected_thread.closings,
  };

  const unifiedWithIntel = {
    ...unified,
    seller_owner_intelligence: sellerOwnerIntelligence,
    automation_decision: automationDecision,
    automation_timeline: automationTimeline,
    dossier: normalizedDossier,
  };

  const internalOnlyContext = omitSensitiveFields(unifiedWithIntel, "internal");
  const aiSafeContext = omitSensitiveFields(unifiedWithIntel, "ai_safe");
  const seller_facing_context = aiSafeContext;

  const source_health = [
    messageEventsRes,
    sendQueueRes,
    brainRes,
    propertiesRes,
    masterOwnersRes,
    ownersRes,
    prospectsRes,
    phoneNumbersRes,
    emailsRes,
    marketsRes,
    zipCodesRes,
    offersRes,
    underwritingRes,
    contractsRes,
    titleRoutingRes,
    closingsRes,
    buyerMatchRes,
    agentsRes,
    templatesRes,
    stateRes,
  ].map((entry) => {
    const count = entry.rows.length;
    const status = entry.ok === false ? "failed" : (count === 0 ? "degraded" : "ok");
    return { table: entry.table, ok: entry.ok, status, error: entry.error || null, count };
  });

  const missingData = source_health
    .filter((entry) =>
      [
        "offers",
        "underwriting",
        "contracts",
        "buyer_match",
        "agents",
        "templates",
        "send_queue",
        "ai_conversation_brain",
      ].includes(entry.table) && entry.status !== "ok"
    )
    .map((entry) => ({
      block: entry.table,
      status: entry.status,
      reason: entry.error ? "source_failed" : "no_rows",
      message: entry.error || `${entry.table} unavailable or empty`,
    }));

  return {
    thread_key,
    context: unifiedWithIntel,
    linkedRecords: {
      propertyIds,
      ownerIds,
      masterOwnerIds,
      phoneIds,
      marketIds,
    },
    copilot_context: {
      internalOnlyContext,
      aiSafeContext,
    },
    seller_facing_context,
    seller_owner_intelligence: sellerOwnerIntelligence,
    automation_decision: automationDecision,
    automation_timeline: automationTimeline,
    dossier: normalizedDossier,
    source_health,
    missingData,
  };
}
