-- PHASE 3: AUTONOMOUS ACQUISITIONS OPERATING SYSTEM
-- NEXUS DASHBOARD + SUPABASE-FIRST MIGRATION

-- ==========================================
-- 1. EXTENSIONS & TYPES
-- ==========================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==========================================
-- 2. TABLES
-- ==========================================

-- 2.1 conversation_threads
-- Represents an ongoing relationship/conversation with a seller regarding a specific property or in general.
CREATE TABLE IF NOT EXISTS conversation_threads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    seller_id UUID NOT NULL, -- Assuming relation to a sellers table or contact id
    property_id UUID, -- Optional, if linked to a specific property
    status VARCHAR(50) NOT NULL DEFAULT 'active', -- active, paused, closed, escalated
    priority INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_message_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- 2.2 conversation_turns
-- Individual messages (inbound/outbound) or system actions within a thread.
CREATE TABLE IF NOT EXISTS conversation_turns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id UUID NOT NULL REFERENCES conversation_threads(id) ON DELETE CASCADE,
    direction VARCHAR(20) NOT NULL, -- inbound, outbound, system
    channel VARCHAR(50) NOT NULL, -- sms, email, voice, etc.
    content TEXT,
    intent_detected VARCHAR(100),
    confidence_score FLOAT,
    handled_by VARCHAR(50) NOT NULL DEFAULT 'ai', -- ai, human, system
    created_at TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);

-- 2.3 seller_state_snapshots
-- Periodic captures of the seller's state for timeline replay and observability.
CREATE TABLE IF NOT EXISTS seller_state_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    seller_id UUID NOT NULL,
    thread_id UUID REFERENCES conversation_threads(id) ON DELETE CASCADE,
    state_data JSONB NOT NULL,
    capture_reason VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.4 negotiation_events
-- Specific events during a negotiation (e.g., offer made, objection raised, counter-offer).
CREATE TABLE IF NOT EXISTS negotiation_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id UUID NOT NULL REFERENCES conversation_threads(id) ON DELETE CASCADE,
    event_type VARCHAR(100) NOT NULL, -- offer_made, objection_handled, counter_offer
    event_payload JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.5 follow_up_queue
-- Autonomous queue for scheduling follow-ups based on seller state and timeline.
CREATE TABLE IF NOT EXISTS follow_up_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id UUID NOT NULL REFERENCES conversation_threads(id) ON DELETE CASCADE,
    seller_id UUID NOT NULL,
    scheduled_for TIMESTAMPTZ NOT NULL,
    follow_up_type VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, processed, cancelled
    context JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- 2.6 agent_actions
-- Actions taken by the AI agent during the orchestration of acquisitions.
CREATE TABLE IF NOT EXISTS agent_actions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id UUID NOT NULL REFERENCES conversation_threads(id) ON DELETE CASCADE,
    action_type VARCHAR(100) NOT NULL, -- pause_thread, escalate, switch_tone
    reasoning TEXT,
    action_payload JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.7 routing_decisions
-- Tracks how AI routed conversations (e.g., to templates, human agents, fallbacks).
CREATE TABLE IF NOT EXISTS routing_decisions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    turn_id UUID REFERENCES conversation_turns(id) ON DELETE CASCADE,
    thread_id UUID NOT NULL REFERENCES conversation_threads(id) ON DELETE CASCADE,
    decision_type VARCHAR(100) NOT NULL,
    routed_to VARCHAR(100) NOT NULL,
    confidence FLOAT,
    rules_triggered JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.8 seller_heat_scores
-- Tracks the acquisition probability and distress score dynamically.
CREATE TABLE IF NOT EXISTS seller_heat_scores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    seller_id UUID NOT NULL,
    base_score FLOAT NOT NULL DEFAULT 0,
    momentum_score FLOAT NOT NULL DEFAULT 0,
    distress_indicator FLOAT NOT NULL DEFAULT 0,
    last_calculated_at TIMESTAMPTZ DEFAULT NOW(),
    factors JSONB DEFAULT '[]'::jsonb
);

-- 2.9 conversation_memory
-- Persistent memory accumulation for the seller (objections, motivations, timeline).
CREATE TABLE IF NOT EXISTS conversation_memory (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    seller_id UUID NOT NULL UNIQUE,
    objections JSONB DEFAULT '[]'::jsonb,
    motivations JSONB DEFAULT '[]'::jsonb,
    price_anchors JSONB DEFAULT '[]'::jsonb,
    timeline_signals JSONB DEFAULT '[]'::jsonb,
    compliance_history JSONB DEFAULT '[]'::jsonb,
    confidence_metrics JSONB DEFAULT '{}'::jsonb,
    last_updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.10 active_negotiations
-- Live state of an ongoing negotiation, linked to a thread.
CREATE TABLE IF NOT EXISTS active_negotiations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id UUID NOT NULL UNIQUE REFERENCES conversation_threads(id) ON DELETE CASCADE,
    seller_id UUID NOT NULL,
    current_offer FLOAT,
    seller_target FLOAT,
    negotiation_posture VARCHAR(100), -- aggressive, defensive, cooperative
    stage VARCHAR(50) NOT NULL, -- discovery, offer, counter, accepted, rejected
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.11 human_escalations
-- Queue and management of conversations requiring human intervention.
CREATE TABLE IF NOT EXISTS human_escalations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id UUID NOT NULL REFERENCES conversation_threads(id) ON DELETE CASCADE,
    reason VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'open', -- open, claimed, resolved
    assigned_to UUID, -- human agent ID
    escalated_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    context JSONB DEFAULT '{}'::jsonb
);

-- 2.12 ai_decisions
-- General log of major AI determinations (e.g., tone shift, opt-out preservation).
CREATE TABLE IF NOT EXISTS ai_decisions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id UUID REFERENCES conversation_threads(id) ON DELETE CASCADE,
    decision_category VARCHAR(100) NOT NULL,
    decision_value TEXT NOT NULL,
    confidence FLOAT,
    alternatives JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.13 live_conversation_metrics
-- Rollup metrics for the live dashboard (denormalized operational snapshots).
CREATE TABLE IF NOT EXISTS live_conversation_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    active_threads_count INT DEFAULT 0,
    escalation_rate FLOAT DEFAULT 0,
    unclear_classification_rate FLOAT DEFAULT 0,
    avg_confidence_score FLOAT DEFAULT 0,
    queue_depth INT DEFAULT 0,
    metrics_payload JSONB DEFAULT '{}'::jsonb
);

-- ==========================================
-- 3. INDEXES
-- ==========================================

CREATE INDEX idx_conv_threads_seller_id ON conversation_threads(seller_id);
CREATE INDEX idx_conv_threads_status ON conversation_threads(status);
CREATE INDEX idx_conv_threads_last_message ON conversation_threads(last_message_at);

CREATE INDEX idx_conv_turns_thread_id ON conversation_turns(thread_id);
CREATE INDEX idx_conv_turns_created_at ON conversation_turns(created_at);

CREATE INDEX idx_seller_snapshots_seller_id ON seller_state_snapshots(seller_id);

CREATE INDEX idx_neg_events_thread_id ON negotiation_events(thread_id);

CREATE INDEX idx_follow_up_queue_scheduled_for ON follow_up_queue(scheduled_for) WHERE status = 'pending';
CREATE INDEX idx_follow_up_queue_seller_id ON follow_up_queue(seller_id);

CREATE INDEX idx_agent_actions_thread_id ON agent_actions(thread_id);

CREATE INDEX idx_routing_decisions_thread_id ON routing_decisions(thread_id);

CREATE INDEX idx_seller_heat_seller_id ON seller_heat_scores(seller_id);
CREATE INDEX idx_seller_heat_score ON seller_heat_scores(base_score DESC);

CREATE INDEX idx_active_neg_seller_id ON active_negotiations(seller_id);

CREATE INDEX idx_human_esc_status ON human_escalations(status);

-- ==========================================
-- 4. RLS POLICIES (Row Level Security)
-- ==========================================

ALTER TABLE conversation_threads ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow service role full access on threads" ON conversation_threads FOR ALL USING (true);
CREATE POLICY "Allow authenticated read on threads" ON conversation_threads FOR SELECT TO authenticated USING (true);

ALTER TABLE conversation_turns ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow service role full access on turns" ON conversation_turns FOR ALL USING (true);
CREATE POLICY "Allow authenticated read on turns" ON conversation_turns FOR SELECT TO authenticated USING (true);

ALTER TABLE follow_up_queue ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow service role full access on queue" ON follow_up_queue FOR ALL USING (true);

ALTER TABLE human_escalations ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow service role full access on escalations" ON human_escalations FOR ALL USING (true);
CREATE POLICY "Allow authenticated full access on escalations" ON human_escalations FOR ALL TO authenticated USING (true);

-- (Extend RLS to other tables similarly based on app requirements)

-- ==========================================
-- 5. VIEWS & MATERIALIZED VIEWS
-- ==========================================

-- Operational Snapshot: Active Dashboard View
CREATE OR REPLACE VIEW v_nexus_active_dashboard AS
SELECT
    ct.id AS thread_id,
    ct.seller_id,
    ct.status,
    ct.last_message_at,
    sh.base_score AS heat_score,
    an.stage AS negotiation_stage,
    he.status AS escalation_status
FROM conversation_threads ct
LEFT JOIN seller_heat_scores sh ON ct.seller_id = sh.seller_id
LEFT JOIN active_negotiations an ON ct.id = an.thread_id
LEFT JOIN human_escalations he ON ct.id = he.thread_id
WHERE ct.status = 'active';

-- Denormalized recommendations:
-- Consider periodic CRON jobs or triggers to update `live_conversation_metrics`
-- every 5 minutes rather than computing on the fly for heavy dashboard loads.
