-- ES Continuous Futures Daily Bars Table
-- Stores pre-aggregated daily OHLCV data from DataBento ohlcv-daily schema

create table if not exists g_continuous_bar_daily (
  trading_date date,
  contract_series varchar,
  underlying_instrument_id bigint,
  open double,
  high double,
  low double,
  close double,
  volume bigint,
  primary key (trading_date, contract_series)
);

