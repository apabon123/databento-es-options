-- FRED Macroeconomic Series Tables
-- Stores daily macroeconomic data from FRED API

-- FRED series metadata (dimension table)
create table if not exists dim_fred_series (
  series_id varchar primary key,
  name varchar,
  units varchar,
  frequency varchar,
  source varchar default 'FRED',
  last_updated timestamp,
  description varchar
);

-- FRED series observations (fact table)
create table if not exists f_fred_observations (
  date date,
  series_id varchar,
  value double,
  source varchar default 'FRED',
  last_updated timestamp,
  primary key (date, series_id)
);

-- Index for efficient date range queries
create index if not exists idx_fred_observations_date on f_fred_observations(date);
create index if not exists idx_fred_observations_series on f_fred_observations(series_id);

