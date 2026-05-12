# Phase 3 Architecture: Autonomous Acquisitions Operating System

## 1. Executive Summary
Phase 3 shifts the operational center of gravity from Podio to Supabase, establishing the "Nexus Dashboard" as the command center for the AI Acquisitions Engine. Podio becomes a legacy CRM mirror and human visibility layer.

## 2. Supabase Architecture
The new Supabase schema introduces a normalized relational model optimized for queue processing, replay analysis, and live dashboards.

### Core Tables
*   `conversation_threads` & `conversation_turns`: Track ongoing relationships and individual messages.
*   `seller_state_snapshots`: Periodic captures of the seller's state for timeline replay.
*   `negotiation_events`: Specific events during a negotiation.
*   `follow_up_queue`: Autonomous queue for scheduling follow-ups.
*   `agent_actions` & `routing_decisions`: Track AI actions and routing logic.
*   `seller_heat_scores`: Tracks acquisition probability and distress dynamically.
*   `conversation_memory`: Persistent memory accumulation (objections, motivations).
*   `active_negotiations`: Live state of ongoing negotiations.
*   `human_escalations`: Queue for human intervention.
*   `ai_decisions` & `live_conversation_metrics`: Observability and dashboards.

*See `supabase/migrations/20260510000000_phase3_nexus_architecture.sql` for full schema.*

## 3. Conversational Memory Engine
A persistent memory layer that accumulates:
*   Objections
*   Motivations
*   Emotional patterns
*   Price anchors
*   Timeline signals

Outputs a comprehensive seller profile used by the Autonomous Negotiation Engine to determine the next best action.

## 4. Autonomous Negotiation Engine
Orchestrates AI-driven seller negotiations, determining when to push, pause, follow up, escalate, nurture, or make offers based on seller state, emotional posture, and historical interactions.

## 5. Nexus Dashboard Architecture (React/Next.js)
An elite operational UI system resembling a command center.

### Modules:
1.  **Live Negotiation Radar**: Active conversations and intent transitions.
2.  **Seller Intelligence Panel**: Seller profile, distress indicators, negotiation posture.
3.  **Conversation Timeline**: Inbound/outbound turns, detected intents, AI actions.
4.  **Autonomous Routing Center**: Queue status, routing decisions, blocked sends.
5.  **Acquisitions Heat Map**: Market activity, seller density, response rates.
6.  **AI Operations Monitor**: Replay metrics, classification regressions, confidence drift.

*See `src/components/nexus/` for module stubs.*

## 6. Migration Strategy from Podio
1.  **Dual-Write (Shadow Mode)**: Keep existing Podio logic intact. Introduce Supabase dual-writes for all new events.
2.  **Data Hydration**: Backfill historical Podio data into Supabase `conversation_threads` and `conversation_memory`.
3.  **Read Transition**: Shift dashboard reads from Podio to Supabase.
4.  **Logic Transition**: Shift queue runners and classification logic to rely on Supabase as the source of truth.
5.  **Podio Degradation**: Convert Podio into a one-way sync target (legacy mirror).

## 7. Queue Orchestration & Live Events
*   Use Supabase Realtime (websockets) to power the Nexus Dashboard.
*   `follow_up_queue` table acts as the source of truth for the autonomous orchestration engine.
*   Dedicated worker processes poll the queue and execute actions, logging to `agent_actions` and `routing_decisions`.

## 8. Rollout Plan
*   **Step 1: Foundation**: Deploy Supabase schema and React stubs (Current Step).
*   **Step 2: Shadow Mode**: Implement dual-writes without modifying live send behavior.
*   **Step 3: Observability**: Connect Nexus Dashboard to live Supabase data.
*   **Step 4: Memory Engine**: Activate persistent memory accumulation.
*   **Step 5: Autonomous Routing**: Transition routing logic to rely on the new engine.
*   **Step 6: Podio Deprecation**: Demote Podio to a legacy mirror.
