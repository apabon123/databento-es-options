-- ES Continuous Futures Tables
-- Supports front month, back-adjusted series, custom roll dates

create table if not exists dim_continuous_contract (
  contract_series varchar primary key,
  root varchar,
  roll_rule varchar,
  adjustment_method varchar,
  description varchar
);


create table if not exists f_continuous_quote_l1 (
  ts_event timestamp,
  ts_rcv   timestamp,
  contract_series varchar,
  underlying_instrument_id bigint,
  bid_px double, bid_sz double,
  ask_px double, ask_sz double
);


create table if not exists f_continuous_trade (
  ts_event timestamp,
  ts_rcv   timestamp,
  contract_series varchar,
  underlying_instrument_id bigint,
  last_px double, last_sz double,
  aggressor varchar
);


create table if not exists g_continuous_bar_1m (
  ts_minute timestamp,
  contract_series varchar,
  underlying_instrument_id bigint,
  o_mid double, h_mid double, l_mid double, c_mid double,
  v_trades double, v_notional double,
  primary key (ts_minute, contract_series)
);



