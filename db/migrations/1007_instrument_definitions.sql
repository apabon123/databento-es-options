-- Instrument Definitions Table
-- Stores full contract specifications from DataBento's definition schema
-- This includes tick size, expiry dates, multipliers, and all other contract specs

create table if not exists dim_instrument_definition (
  instrument_id bigint,
  native_symbol varchar,
  definition_date date,  -- Date when this definition was active
  ts_event timestamp,    -- Timestamp from DataBento definition record
  
  -- Contract specifications
  min_price_increment double,          -- Tick size (e.g., 0.0025 for SR3, 0.2500 for ES)
  min_price_increment_amount double,   -- Dollar value per tick
  contract_multiplier double,          -- Contract multiplier
  original_contract_size double,       -- Original contract size
  contract_multiplier_unit integer,    -- Contract multiplier unit
  
  -- Expiration and maturity
  expiration timestamp,                -- Expiration date/time
  maturity_year integer,               -- Maturity year
  maturity_month integer,              -- Maturity month
  maturity_day integer,                -- Maturity day
  maturity_week integer,               -- Maturity week
  
  -- Price limits and trading
  high_limit_price double,             -- High limit price
  low_limit_price double,              -- Low limit price
  trading_reference_price double,      -- Trading reference price
  max_price_variation double,          -- Max price variation
  
  -- Trading volumes
  min_trade_vol integer,               -- Minimum trade volume
  max_trade_vol integer,               -- Maximum trade volume
  min_lot_size integer,                -- Minimum lot size
  min_lot_size_block integer,          -- Minimum lot size for block trades
  min_lot_size_round_lot integer,      -- Minimum lot size for round lots
  
  -- Currency and units
  currency varchar,                    -- Currency code (e.g., USD)
  settl_currency varchar,              -- Settlement currency
  unit_of_measure varchar,             -- Unit of measure (e.g., USD, IPNT)
  unit_of_measure_qty double,          -- Unit of measure quantity
  
  -- Underlying and asset information
  underlying_id bigint,                -- Underlying instrument ID
  underlying varchar,                  -- Underlying symbol
  underlying_product varchar,          -- Underlying product
  asset varchar,                       -- Asset code (e.g., SR3, ES)
  exchange varchar,                    -- Exchange code (e.g., XCME)
  "group" varchar,                     -- Group code (quoted because 'group' is a reserved keyword)
  secsubtype varchar,                  -- Security subtype
  
  -- Instrument classification
  instrument_class varchar,            -- Instrument class (e.g., F for futures)
  security_type varchar,               -- Security type (e.g., FUT)
  cfi varchar,                         -- Classification of Financial Instruments code
  user_defined_instrument varchar,     -- User defined instrument flag
  
  -- Trading and matching
  match_algorithm varchar,             -- Matching algorithm
  tick_rule integer,                   -- Tick rule
  flow_schedule_type integer,          -- Flow schedule type
  
  -- Market data
  market_depth integer,                -- Market depth
  market_depth_implied integer,        -- Market depth implied
  market_segment_id integer,           -- Market segment ID
  md_security_trading_status integer,  -- Market data security trading status
  
  -- Display and formatting
  display_factor double,               -- Display factor
  price_display_format integer,        -- Price display format
  main_fraction integer,               -- Main fraction
  sub_fraction integer,                -- Sub fraction
  
  -- Dates
  activation timestamp,                -- Activation date
  trading_reference_date date,         -- Trading reference date
  decay_start_date date,               -- Decay start date
  decay_quantity integer,              -- Decay quantity
  
  -- Other fields
  security_update_action varchar,      -- Security update action (A=Add, M=Modify, D=Delete)
  price_ratio double,                  -- Price ratio
  inst_attrib_value integer,           -- Instrument attribute value
  raw_instrument_id bigint,            -- Raw instrument ID
  strike_price double,                 -- Strike price (for options)
  strike_price_currency varchar,       -- Strike price currency
  settl_price_type integer,            -- Settlement price type
  appl_id integer,                     -- Application ID
  maturity_year_appl integer,          -- Maturity year (application)
  channel_id integer,                  -- Channel ID
  
  -- Metadata
  last_updated timestamp default current_timestamp,
  
  primary key (instrument_id)
);

-- Indexes for efficient lookups
create index if not exists idx_instrument_def_symbol on dim_instrument_definition(native_symbol);
create index if not exists idx_instrument_def_expiration on dim_instrument_definition(expiration);
create index if not exists idx_instrument_def_date on dim_instrument_definition(definition_date);
create index if not exists idx_instrument_def_asset on dim_instrument_definition(asset);
create index if not exists idx_instrument_def_exchange on dim_instrument_definition(exchange);

-- View for latest definitions per instrument
-- Since we now have only one definition per instrument (primary key is instrument_id),
-- this view is just a pass-through, but kept for compatibility
create or replace view v_instrument_definition_latest as
select 
  instrument_id,
  native_symbol,
  definition_date,
  ts_event,
  min_price_increment,
  min_price_increment_amount,
  contract_multiplier,
  expiration,
  maturity_year,
  maturity_month,
  maturity_day,
  currency,
  unit_of_measure,
  unit_of_measure_qty,
  asset,
  exchange,
  instrument_class,
  security_type,
  high_limit_price,
  low_limit_price,
  min_trade_vol,
  max_trade_vol,
  activation,
  last_updated
from dim_instrument_definition;

