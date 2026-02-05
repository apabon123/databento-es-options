-- Canonical Futures-Six contract series mapping and view
-- Maps each root to the canonical front-month contract_series.
-- v_canonical_continuous_bar_daily selects only canonical series from g_continuous_bar_daily.
--
-- IDEMPOTENT: Safe to rerun (e.g. rebuild DB, reapply migrations).
-- Pattern: CREATE TABLE IF NOT EXISTS; DELETE; INSERT; CREATE OR REPLACE VIEW.

create table if not exists dim_canonical_series (
  root varchar primary key,
  contract_series varchar not null,
  description varchar,
  optional boolean default false
);

-- Populate from configs/canonical_series.yaml (kept in sync manually)
delete from dim_canonical_series;
insert into dim_canonical_series (root, contract_series, description, optional) values
  ('ES', 'ES_FRONT_CALENDAR', 'S&P 500 e-mini', false),
  ('NQ', 'NQ_FRONT_CALENDAR', 'Nasdaq-100 e-mini', false),
  ('RTY', 'RTY_FRONT_CALENDAR', 'Russell 2000 e-mini', false),
  ('ZT', 'ZT_FRONT_VOLUME', '2Y Treasury note', false),
  ('ZF', 'ZF_FRONT_VOLUME', '5Y Treasury note', false),
  ('ZN', 'ZN_FRONT_VOLUME', '10Y Treasury note', false),
  ('UB', 'UB_FRONT_VOLUME', 'Ultra-bond (30Y+)', false),
  ('CL', 'CL_FRONT_VOLUME', 'WTI crude oil', false),
  ('GC', 'GC_FRONT_VOLUME', 'COMEX gold', false),
  ('6E', '6E_FRONT_CALENDAR', 'EUR/USD', false),
  ('6J', '6J_FRONT_CALENDAR', 'JPY/USD', false),
  ('6B', '6B_FRONT_CALENDAR', 'GBP/USD', false),
  ('SR3', 'SR3_FRONT_CALENDAR', 'SOFR 3M front month', false),
  ('VX', 'VX_FRONT_CALENDAR', 'VIX futures', true);

create or replace view v_canonical_continuous_bar_daily as
select
  b.trading_date,
  c.root,
  b.contract_series,
  b.underlying_instrument_id,
  b.open,
  b.high,
  b.low,
  b.close,
  b.volume
from g_continuous_bar_daily b
join dim_canonical_series c on b.contract_series = c.contract_series;
