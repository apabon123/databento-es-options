create table if not exists dim_fut_instrument (
  instrument_id bigint primary key,
  root varchar,
  expiry date,
  symbol_feed varchar,
  symbol_canonical varchar,
  tick_size double,
  multiplier int
);


create table if not exists f_fut_quote_l1 (
  ts_event timestamp,
  ts_rcv   timestamp,
  instrument_id bigint,
  bid_px double, bid_sz double,
  ask_px double, ask_sz double
);


create table if not exists f_fut_trade (
  ts_event timestamp,
  ts_rcv   timestamp,
  instrument_id bigint,
  last_px double, last_sz double,
  aggressor varchar
);


create table if not exists g_fut_bar_1m (
  ts_minute timestamp,
  instrument_id bigint,
  o_mid double, h_mid double, l_mid double, c_mid double,
  v_trades double, v_notional double
);


