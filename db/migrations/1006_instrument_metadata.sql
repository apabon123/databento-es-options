-- Instrument Metadata Tables
-- Stores native symbols, expiry dates, and roll dates for continuous contracts

-- Instrument metadata: maps instrument_id to native symbol and expiry date
create table if not exists dim_instrument_metadata (
  instrument_id bigint primary key,
  native_symbol varchar,
  root varchar,
  month integer,
  year integer,
  expiry_date date,  -- IMM date for SOFR/Treasury futures
  date_range_start date,  -- When this instrument first appears
  date_range_end date,    -- When this instrument last appears
  last_updated timestamp default current_timestamp
);

-- Roll dates: tracks when continuous contracts roll from one instrument to another
create table if not exists dim_roll_dates (
  contract_series varchar,
  rank integer,
  roll_date date,
  old_instrument_id bigint,
  new_instrument_id bigint,
  old_native_symbol varchar,
  new_native_symbol varchar,
  old_expiry_date date,
  new_expiry_date date,
  primary key (contract_series, rank, roll_date)
);

-- Index for efficient lookups
create index if not exists idx_instrument_metadata_root on dim_instrument_metadata(root);
create index if not exists idx_instrument_metadata_expiry on dim_instrument_metadata(expiry_date);
create index if not exists idx_roll_dates_contract on dim_roll_dates(contract_series, rank);

