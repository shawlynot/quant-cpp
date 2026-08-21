-- security_master: instrument reference data, class-table-inheritance style.
--
-- `instrument` is the one core table — venue_id and tick_size live only
-- here, never duplicated on a subtype. Subtype tables (`option`,
-- `future`, `currencies`) share their primary key with the parent instrument row
-- (PRIMARY KEY REFERENCES instrument(instrument_id)), so a subtype row
-- cannot exist without a venue.

CREATE SCHEMA IF NOT EXISTS security_master;

-- ── venue ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS security_master.venue (
    venue_id    bigserial PRIMARY KEY,
    code        text NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO security_master.venue (code)
VALUES ('DERIBIT')
ON CONFLICT (code) DO NOTHING;

-- ── instrument (core table) ────────────────────────────────────────
CREATE TABLE IF NOT
  EXISTS security_master.instrument (
    instrument_id         bigserial PRIMARY KEY,
    venue_id              bigint NOT NULL REFERENCES security_master.venue (venue_id),
    tick_size             numeric(20, 8) NOT NULL,
    instrument_type       text NOT NULL
        CHECK (instrument_type IN ('option', 'future', 'perpetual', 'currency')),
    symbol text NOT NULL,
    UNIQUE (venue_id, symbol)
);

-- ── option (inherits instrument) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS security_master.option (
    instrument_id  bigint PRIMARY KEY
        REFERENCES security_master.instrument (instrument_id) ON DELETE CASCADE,
    option_type    text NOT NULL CHECK (option_type IN ('call', 'put')),
    strike         numeric(20, 8) NOT NULL,
    expiration_timestamp  timestamptz,
    contract_size int,
    creation_timestamp    timestamptz
);

-- ── currencies (inherits instrument) ──────────────────────────────
-- Identity row only. Actual BTC/USD quote values are ingested by a
-- separate, later process and are not part of this table.
CREATE TABLE IF NOT EXISTS security_master.currencies (
    instrument_id  bigint PRIMARY KEY
        REFERENCES security_master.instrument (instrument_id) ON DELETE CASCADE
);

-- ── future (inherits instrument) ──────────────────────────────────
-- Dated futures only. `settlement_period` is retained because it is the
-- discriminator that kept the perpetual out at ingest time. Deribit lists
-- dated futures as 'day', 'week' and 'month', so the constraint excludes
-- the one value that must never land here rather than allowlisting the
-- rest -- a new period name should not break the ingest.
CREATE TABLE IF NOT EXISTS security_master.future (
    instrument_id  bigint PRIMARY KEY
        REFERENCES security_master.instrument (instrument_id) ON DELETE CASCADE,
    settlement_period     text NOT NULL CHECK (settlement_period <> 'perpetual'),
    expiration_timestamp  timestamptz,
    contract_size int,
    creation_timestamp    timestamptz
);
