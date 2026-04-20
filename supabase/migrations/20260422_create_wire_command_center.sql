-- Wire Command Center tables
-- Tracks expected, received, and cleared wire events for closings and deal revenue tracking

-- wire_accounts: Master list of bank accounts receiving wires
CREATE TABLE IF NOT EXISTS wire_accounts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  account_key text UNIQUE NOT NULL,
  display_name text NOT NULL,
  institution_name text,
  account_last4 text,
  country text,
  currency text DEFAULT 'USD',
  is_active boolean DEFAULT true,
  metadata jsonb DEFAULT '{}'::jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX idx_wire_accounts_active ON wire_accounts(is_active);
CREATE INDEX idx_wire_accounts_key ON wire_accounts(account_key);

-- wire_events: Individual wire transaction records
CREATE TABLE IF NOT EXISTS wire_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  wire_key text UNIQUE NOT NULL,
  deal_key text,
  closing_id bigint,
  deal_revenue_id bigint,
  property_id bigint,
  buyer_id bigint,
  title_company_id bigint,
  account_key text,
  amount numeric NOT NULL,
  currency text DEFAULT 'USD',
  direction text DEFAULT 'incoming',
  status text DEFAULT 'expected',
  status_note text,
  expected_at timestamptz,
  received_at timestamptz,
  cleared_at timestamptz,
  source text,
  metadata jsonb DEFAULT '{}'::jsonb,
  created_by_discord_user_id text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX idx_wire_events_status ON wire_events(status);
CREATE INDEX idx_wire_events_expected_at ON wire_events(expected_at);
CREATE INDEX idx_wire_events_received_at ON wire_events(received_at);
CREATE INDEX idx_wire_events_account_key ON wire_events(account_key);
CREATE INDEX idx_wire_events_property_id ON wire_events(property_id);
CREATE INDEX idx_wire_events_closing_id ON wire_events(closing_id);
CREATE INDEX idx_wire_events_deal_revenue_id ON wire_events(deal_revenue_id);
CREATE INDEX idx_wire_events_wire_key ON wire_events(wire_key);

-- Add comments for clarity
COMMENT ON TABLE wire_accounts IS 'Master list of bank accounts that receive wire transfers';
COMMENT ON TABLE wire_events IS 'Records of wire transactions — expected, received, or cleared';
COMMENT ON COLUMN wire_events.wire_key IS 'Unique identifier for tracking a wire transfer';
COMMENT ON COLUMN wire_events.status IS 'expected | pending | received | cleared | cancelled | disputed';
COMMENT ON COLUMN wire_events.direction IS 'incoming | outgoing | internal_transfer';
COMMENT ON COLUMN wire_events.created_by_discord_user_id IS 'Discord user ID of who created/updated the record';
