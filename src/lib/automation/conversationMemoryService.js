// ─── conversationMemoryService.js ─────────────────────────────────────────
import { supabase } from "@/lib/supabase/client.js";
import { info, warn, error as logError } from "@/lib/logging/logger.js";
import crypto from "node:crypto";

/**
 * Ensures a string is a valid UUID, or hashes it into a deterministic UUID.
 */
function ensureUuid(input) {
  if (!input) return null;
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (uuidRegex.test(input)) return input;

  // Generate deterministic UUID v4-formatted hash from string
  const hash = crypto.createHash('sha256').update(String(input)).digest('hex');
  return [
    hash.substring(0, 8),
    hash.substring(8, 12),
    '4' + hash.substring(12, 15), // version 4 flag + 3 chars = 4 chars
    ((parseInt(hash.substring(15, 16), 16) & 0x3) | 0x8).toString(16) + hash.substring(16, 19), // 4 chars
    hash.substring(19, 31) // 12 chars
  ].join('-');
}

/**
 * ConversationMemoryService
 *
 * Provides a clean interface to Phase 3 conversational memory tables.
 * Used for dual-writing during transition and for the Nexus Dashboard.
 */

/**
 * Upserts a conversation thread.
 * @param {object} thread - Thread data { seller_id, property_id, status, metadata }
 * @returns {Promise<string|null>} thread_id
 */
export async function upsertThread(thread) {
  const { seller_id: raw_seller_id, property_id: raw_property_id, status = 'active', metadata = {} } = thread;
  
  const seller_id = ensureUuid(raw_seller_id);
  const property_id = ensureUuid(raw_property_id);

  if (!seller_id) {
    warn("memory.upsert_thread_missing_seller_id");
    return null;
  }

  const { data, error } = await supabase
    .from("conversation_threads")
    .upsert({
      seller_id,
      property_id,
      status,
      metadata,
      updated_at: new Date().toISOString(),
    }, {
      onConflict: 'seller_id, property_id',
    })
    .select("id")
    .single();

  if (error) {
    logError("memory.upsert_thread_failed", { error: error.message, seller_id, raw_seller_id });
    return null;
  }

  return data.id;
}

/**
 * Appends a conversation turn with idempotency.
 * @param {object} turn - Turn data { thread_id, direction, content, intent_detected, confidence_score, metadata }
 * @returns {Promise<string|null>} turn_id
 */
export async function appendTurn(turn) {
  const { thread_id, direction, channel = 'sms', content, intent_detected, confidence_score, metadata = {} } = turn;
  
  if (!thread_id) {
    warn("memory.append_turn_missing_thread_id");
    return null;
  }

  // Idempotency check using metadata (e.g., inbound_message_id)
  if (metadata.inbound_message_id) {
    const { data: existing } = await supabase
      .from("conversation_turns")
      .select("id")
      .eq("metadata->>inbound_message_id", metadata.inbound_message_id)
      .maybeSingle();

    if (existing) {
      info("memory.turn_duplicate_prevented", { turn_id: existing.id, inbound_message_id: metadata.inbound_message_id });
      return existing.id;
    }
  }

  const { data, error } = await supabase
    .from("conversation_turns")
    .insert({
      thread_id,
      direction,
      channel,
      content,
      intent_detected,
      confidence_score,
      metadata,
    })
    .select("id")
    .single();

  if (error) {
    logError("memory.append_turn_failed", { error: error.message, thread_id });
    return null;
  }

  // Update thread last_message_at
  await supabase
    .from("conversation_threads")
    .update({ last_message_at: new Date().toISOString() })
    .eq("id", thread_id);

  return data.id;
}

/**
 * Stores a seller state snapshot.
 */
export async function storeSellerStateSnapshot({ seller_id: raw_seller_id, thread_id, state_data, capture_reason }) {
  const seller_id = ensureUuid(raw_seller_id);
  const { error } = await supabase
    .from("seller_state_snapshots")
    .insert({
      seller_id,
      thread_id,
      state_data,
      capture_reason,
    });

  if (error) {
    logError("memory.store_snapshot_failed", { error: error.message, seller_id });
  }
}

/**
 * Stores a negotiation event.
 */
export async function storeNegotiationEvent({ thread_id, event_type, event_payload }) {
  const { error } = await supabase
    .from("negotiation_events")
    .insert({
      thread_id,
      event_type,
      event_payload,
    });

  if (error) {
    logError("memory.store_negotiation_event_failed", { error: error.message, thread_id });
  }
}

/**
 * Stores a routing decision with idempotency.
 */
export async function storeRoutingDecision(decision) {
  const { turn_id, thread_id, decision_type, routed_to, confidence, rules_triggered = [] } = decision;

  if (turn_id) {
    const { data: existing } = await supabase
      .from("routing_decisions")
      .select("id")
      .eq("turn_id", turn_id)
      .maybeSingle();

    if (existing) {
      info("memory.routing_decision_duplicate_prevented", { turn_id });
      return existing.id;
    }
  }

  const { data, error } = await supabase
    .from("routing_decisions")
    .insert({
      turn_id,
      thread_id,
      decision_type,
      routed_to,
      confidence,
      rules_triggered,
    })
    .select("id")
    .single();

  if (error) {
    logError("memory.store_routing_decision_failed", { error: error.message, thread_id });
    return null;
  }

  return data.id;
}

/**
 * Loads the full conversation memory for a thread.
 * @param {string} thread_key - The key used to identify the thread (or thread_id)
 * @returns {Promise<object>} Full memory object
 */
export async function loadConversationMemory(thread_key) {
  // Try to find thread by thread_key in metadata or by ID
  let threadQuery = supabase.from("conversation_threads").select("*");
  
  if (thread_key.includes('-')) { // UUID check
    threadQuery = threadQuery.eq("id", thread_key);
  } else {
    threadQuery = threadQuery.eq("metadata->>thread_key", thread_key);
  }

  const { data: thread, error: threadError } = await threadQuery.maybeSingle();

  if (threadError || !thread) {
    return { found: false, thread: null, turns: [], snapshots: [], memory: null };
  }

  // Load turns
  const { data: turns } = await supabase
    .from("conversation_turns")
    .select("*")
    .eq("thread_id", thread.id)
    .order("created_at", { ascending: false });

  // Load snapshots
  const { data: snapshots } = await supabase
    .from("seller_state_snapshots")
    .select("*")
    .eq("thread_id", thread.id)
    .order("created_at", { ascending: false });

  // Load persistent memory record
  const { data: memory } = await supabase
    .from("conversation_memory")
    .select("*")
    .eq("seller_id", thread.seller_id)
    .maybeSingle();

  return {
    found: true,
    thread,
    turns: turns || [],
    snapshots: snapshots || [],
    memory,
    // Helper to get latest state
    latest_state: snapshots?.[0]?.state_data || null,
  };
}

export default {
  upsertThread,
  appendTurn,
  storeSellerStateSnapshot,
  storeNegotiationEvent,
  storeRoutingDecision,
  loadConversationMemory,
};

