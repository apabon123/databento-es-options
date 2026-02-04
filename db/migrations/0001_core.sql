create table if not exists _migrations (
  id varchar primary key,
  applied_at timestamp default current_timestamp
);


create table if not exists dim_session (
  trade_date date primary key,
  week int,
  month int,
  quarter int,
  is_holiday boolean
);


