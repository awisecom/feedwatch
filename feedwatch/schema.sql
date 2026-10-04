-- feedwatch schema. The sink applies it on every start; every statement is idempotent.

CREATE TABLE IF NOT EXISTS ticks (
    symbol      text             NOT NULL,
    seq         bigint           NOT NULL,
    price       double precision NOT NULL,  -- fine for monitoring; money columns would be numeric
    qty         double precision NOT NULL,
    source_ts   timestamptz      NOT NULL,  -- when the event happened upstream
    applied_ts  timestamptz      NOT NULL,  -- when it entered confirmed state here
    via         text             NOT NULL CHECK (via IN ('stream', 'backfill')),
    PRIMARY KEY (symbol, seq)
);

-- Append-only and written in time order: a BRIN index stays a few pages
-- instead of growing into a B-tree the size of the table.
CREATE INDEX IF NOT EXISTS ticks_applied_brin ON ticks USING brin (applied_ts);

CREATE TABLE IF NOT EXISTS revisions (
    symbol             text             NOT NULL,
    seq                bigint           NOT NULL,
    provisional_price  double precision NOT NULL,
    confirmed_price    double precision NOT NULL,
    provisional_qty    double precision NOT NULL,
    confirmed_qty      double precision NOT NULL,
    seen_at            timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, seq)
);

CREATE TABLE IF NOT EXISTS freshness_changes (
    symbol      text        NOT NULL,
    from_state  text        NOT NULL,
    to_state    text        NOT NULL,
    age_s       real,
    at          timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS freshness_changes_symbol_at ON freshness_changes (symbol, at);

-- Time from the event upstream to confirmed state here, per symbol and minute.
CREATE OR REPLACE VIEW lag_per_minute AS
SELECT symbol,
       date_trunc('minute', applied_ts) AS minute,
       count(*) AS ticks,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM applied_ts - source_ts)) AS p50_s,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM applied_ts - source_ts)) AS p99_s,
       count(*) FILTER (WHERE via = 'backfill') AS backfilled
FROM ticks
GROUP BY symbol, date_trunc('minute', applied_ts);
