/**
 * Phase 3: Autonomous Acquisitions Operating System
 * TypeScript Interfaces for Nexus Dashboard & Conversational Memory Engine
 */

export type UUID = string;

// ==========================================
// 1. CONVERSATIONAL MEMORY ENGINE
// ==========================================

export interface SellerProfile {
  name?: string;
  contact_info?: {
    phone?: string;
    email?: string;
  };
  demographics?: Record<string, any>;
}

export interface BehavioralProfile {
  responsiveness: 'high' | 'medium' | 'low';
  preferred_channel: 'sms' | 'email' | 'voice';
  communication_style: 'direct' | 'storyteller' | 'guarded';
}

export interface NegotiationProfile {
  posture: 'aggressive' | 'defensive' | 'cooperative' | 'neutral';
  flexibility: number; // 0.0 to 1.0
  key_objections: string[];
}

export interface EmotionalProfile {
  current_state: 'angry' | 'anxious' | 'neutral' | 'happy' | 'distressed';
  volatility: number; // 0.0 to 1.0
}

export interface PricingProfile {
  price_anchors: number[];
  target_price?: number;
  perceived_value?: number;
  flexibility: 'rigid' | 'flexible' | 'unknown';
}

export interface ComplianceProfile {
  opt_out_risk: number; // 0.0 to 1.0
  prior_complaints: boolean;
  dnc_status: boolean;
}

export interface SellerMemoryState {
  seller_id: UUID;
  seller_profile: SellerProfile;
  behavioral_profile: BehavioralProfile;
  negotiation_profile: NegotiationProfile;
  emotional_profile: EmotionalProfile;
  pricing_profile: PricingProfile;
  compliance_profile: ComplianceProfile;

  acquisition_probability: number; // 0.0 to 1.0
  distress_probability: number;    // 0.0 to 1.0

  next_best_action: string;
  ai_confidence: number;           // 0.0 to 1.0
}

// ==========================================
// 2. NEXUS DASHBOARD TYPES
// ==========================================

export interface Thread {
  id: UUID;
  seller_id: UUID;
  property_id?: UUID;
  status: 'active' | 'paused' | 'closed' | 'escalated';
  priority: number;
  created_at: string;
  updated_at: string;
  last_message_at: string;
  metadata: Record<string, any>;
}

export interface Turn {
  id: UUID;
  thread_id: UUID;
  direction: 'inbound' | 'outbound' | 'system';
  channel: string;
  content: string;
  intent_detected?: string;
  confidence_score?: number;
  handled_by: 'ai' | 'human' | 'system';
  created_at: string;
}

export interface ActiveNegotiation {
  id: UUID;
  thread_id: UUID;
  seller_id: UUID;
  current_offer?: number;
  seller_target?: number;
  negotiation_posture: string;
  stage: 'discovery' | 'offer' | 'counter' | 'accepted' | 'rejected';
  updated_at: string;
}

export interface HumanEscalation {
  id: UUID;
  thread_id: UUID;
  reason: string;
  status: 'open' | 'claimed' | 'resolved';
  assigned_to?: UUID;
  escalated_at: string;
  resolved_at?: string;
}

export interface LiveDashboardMetrics {
  active_threads_count: number;
  escalation_rate: number;
  unclear_classification_rate: number;
  avg_confidence_score: number;
  queue_depth: number;
  timestamp: string;
}

// ==========================================
// 3. AUTONOMOUS NEGOTIATION ENGINE
// ==========================================

export interface AutonomousActionDecision {
  action: 'push' | 'pause' | 'follow_up' | 'escalate' | 'nurture' | 'make_offer' | 'shift_tone' | 'switch_agent' | 'stop_outreach';
  reasoning: string;
  confidence: number;
  payload?: any;
}
