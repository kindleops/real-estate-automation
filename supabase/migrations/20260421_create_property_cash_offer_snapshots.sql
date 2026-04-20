-- ─── 20260421_create_property_cash_offer_snapshots.sql ────────────────────────
-- Single-family cash offer snapshot cache.
--
-- Design decisions:
--
--   1. Only single-family cash offers are cached here.  Multifamily, commercial,
--      and creative-finance deals are handled by the Podio Underwriting app (see
--      transfer-to-underwriting.js) and must NOT appear in this table.
--
--   2. One active offer per property_id at any time.  When a new snapshot is
--      upserted the previous active row is superseded (status → 'superseded')
--      before the new row is inserted / the version counter is incremented.
--
--   3. The unique partial index on (property_id) WHERE status = 'active'
--      enforces the single-active-offer rule at the database level.
--
--   4. cash_offer is a single number — not min/max.  Do not add range columns.

-- ── Table ──────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS property_cash_offer_snapshots (
  -- Identity
  id                         uuid         PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Property anchors
  property_id                text         NOT NULL,
  podio_property_item_id     bigint,
  master_owner_id            bigint,
  owner_id                   text,

  -- Location / attributes
  property_address           text,
  property_city              text,
  property_state             text,
  property_zip               text,
  market                     text,
  property_type              text,
  property_class             text,

  -- Offer financials  (single-family cash, no min/max range)
  cash_offer                 numeric,
  repair_estimate            numeric,
  estimated_value            numeric,
  calculated_value           numeric,
  estimated_equity           numeric,
  estimated_mortgage_balance numeric,
  estimated_mortgage_payment numeric,

  -- Provenance
  offer_source               text         NOT NULL DEFAULT 'podio',
  valuation_source           text,
  confidence_score           numeric,
  motivation_score           numeric,

  -- Lifecycle
  status                     text         NOT NULL DEFAULT 'active',
  version                    int          NOT NULL DEFAULT 1,

  -- Podio sync
  podio_offer_item_id        bigint,
  podio_synced_at            timestamptz,

  -- Extra context
  metadata                   jsonb        NOT NULL DEFAULT '{}'::jsonb,

  -- Timestamps
  generated_at               timestamptz  NOT NULL DEFAULT now(),
  expires_at                 timestamptz,
  created_at                 timestamptz  NOT NULL DEFAULT now(),
  updated_at                 timestamptz  NOT NULL DEFAULT now()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

-- Enforce one active snapshot per property.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_property_cash_offer_active
  ON property_cash_offer_snapshots (property_id)
  WHERE status = 'active';

-- Fast lookup by Podio property item id (nullable).
CREATE INDEX IF NOT EXISTS idx_property_cash_offer_podio_property
  ON property_cash_offer_snapshots (podio_property_item_id)
  WHERE podio_property_item_id IS NOT NULL;

-- Fast lookup by master owner.
CREATE INDEX IF NOT EXISTS idx_property_cash_offer_master_owner
  ON property_cash_offer_snapshots (master_owner_id)
  WHERE master_owner_id IS NOT NULL;

-- Time-series / audit queries.
CREATE INDEX IF NOT EXISTS idx_property_cash_offer_created_at
  ON property_cash_offer_snapshots (created_at DESC);

-- ── Row-level security ────────────────────────────────────────────────────────

ALTER TABLE property_cash_offer_snapshots ENABLE ROW LEVEL SECURITY;

-- Only the service role (backend) may read or write this table.
CREATE POLICY "service_role_only"
  ON property_cash_offer_snapshots
  FOR ALL
  USING     (auth.role() = 'service_role')
  WITH CHECK (auth.role() = 'service_role');

-- ── updated_at trigger ────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_property_cash_offer_snapshots_updated_at'
  ) THEN
    CREATE TRIGGER trg_property_cash_offer_snapshots_updated_at
      BEFORE UPDATE ON property_cash_offer_snapshots
      FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END;
$$;
