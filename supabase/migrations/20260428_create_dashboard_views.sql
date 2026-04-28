-- Migration: Create lightweight dashboard views
-- Date: 2026-04-28
-- Purpose: Provide fast, column-scoped views for dashboard loads.
--          Avoids giant raw table scans from the frontend.

-- ---------------------------------------------------------------------------
-- set_updated_at helper (idempotent)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- inbox_thread_state — persists archive/read status per conversation thread
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.inbox_thread_state (
  id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  thread_key     text        NOT NULL UNIQUE,
  master_owner_id text,
  property_id    text,
  is_read        boolean     NOT NULL DEFAULT false,
  is_archived    boolean     NOT NULL DEFAULT false,
  archived_at    timestamptz,
  read_at        timestamptz,
  updated_by     text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inbox_thread_state_master_owner
  ON public.inbox_thread_state (master_owner_id);

CREATE INDEX IF NOT EXISTS idx_inbox_thread_state_thread_key
  ON public.inbox_thread_state (thread_key);

DROP TRIGGER IF EXISTS trg_inbox_thread_state_updated_at ON public.inbox_thread_state;
CREATE TRIGGER trg_inbox_thread_state_updated_at
  BEFORE UPDATE ON public.inbox_thread_state
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE public.inbox_thread_state ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS inbox_thread_state_service   ON public.inbox_thread_state;
DROP POLICY IF EXISTS inbox_thread_state_authed_rw ON public.inbox_thread_state;
DROP POLICY IF EXISTS inbox_thread_state_anon_r    ON public.inbox_thread_state;

CREATE POLICY inbox_thread_state_service ON public.inbox_thread_state
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY inbox_thread_state_authed_rw ON public.inbox_thread_state
  FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY inbox_thread_state_anon_r ON public.inbox_thread_state
  FOR SELECT TO anon USING (true);

-- ---------------------------------------------------------------------------
-- v_dashboard_message_events — scoped view for the inbox
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_dashboard_message_events AS
SELECT
  me.id,
  me.master_owner_id,
  me.property_id,
  me.direction,
  me.message_body,
  me.from_phone,
  me.to_phone,
  me.event_type,
  me.queue_status,
  me.created_at,
  -- Stable thread_key that matches what the application computes.
  coalesce(me.master_owner_id, '') || ':' ||
    coalesce(me.property_id, '') || ':' ||
    least(coalesce(me.from_phone, ''), coalesce(me.to_phone, '')) || ':' ||
    greatest(coalesce(me.from_phone, ''), coalesce(me.to_phone, ''))
    AS thread_key,
  its.is_read,
  its.is_archived
FROM public.message_events me
LEFT JOIN public.inbox_thread_state its
  ON its.thread_key = (
    coalesce(me.master_owner_id, '') || ':' ||
    coalesce(me.property_id, '') || ':' ||
    least(coalesce(me.from_phone, ''), coalesce(me.to_phone, '')) || ':' ||
    greatest(coalesce(me.from_phone, ''), coalesce(me.to_phone, ''))
  );

-- ---------------------------------------------------------------------------
-- v_dashboard_inbox_threads — one row per conversation thread
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_dashboard_inbox_threads AS
SELECT
  thread_key,
  master_owner_id,
  property_id,
  count(*)                                       AS message_count,
  max(created_at)                                AS last_message_at,
  bool_or(direction = 'inbound')                 AS has_inbound,
  coalesce(bool_or(its.is_read), false)          AS is_read,
  coalesce(bool_or(its.is_archived), false)      AS is_archived
FROM (
  SELECT
    me.master_owner_id,
    me.property_id,
    me.direction,
    me.created_at,
    coalesce(me.master_owner_id, '') || ':' ||
      coalesce(me.property_id, '') || ':' ||
      least(coalesce(me.from_phone, ''), coalesce(me.to_phone, '')) || ':' ||
      greatest(coalesce(me.from_phone, ''), coalesce(me.to_phone, ''))
      AS thread_key
  FROM public.message_events me
) t
LEFT JOIN public.inbox_thread_state its USING (thread_key)
GROUP BY thread_key, master_owner_id, property_id, its.is_read, its.is_archived;

-- ---------------------------------------------------------------------------
-- v_property_map_points — lightweight map tile data
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_property_map_points AS
SELECT
  p.id          AS property_id,
  p.address     AS address,
  p.city        AS city,
  p.state       AS state,
  p.zip         AS zip,
  p.market      AS market,
  p.lat         AS lat,
  p.lng         AS lng,
  p.status      AS status,
  p.score       AS score,
  p.tier        AS tier,
  p.created_at  AS created_at
FROM public.properties p
WHERE p.lat IS NOT NULL
  AND p.lng IS NOT NULL;
