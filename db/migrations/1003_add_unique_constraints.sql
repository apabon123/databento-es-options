-- Add unique constraints to prevent duplicate rows in quote and trade tables
-- Using INSERT OR IGNORE will silently skip duplicates
-- Note: This migration removes duplicates before creating the unique index

-- ES Options: Remove duplicates first, then create unique index
-- Keep only the first occurrence of each (ts_event, instrument_id) pair
delete from f_quote_l1 
where rowid not in (
    select min(rowid) 
    from f_quote_l1 
    group by ts_event, instrument_id
);

create unique index if not exists idx_f_quote_l1_unique on f_quote_l1 (ts_event, instrument_id);

-- ES Futures: Remove duplicates first, then create unique index
delete from f_fut_quote_l1 
where rowid not in (
    select min(rowid) 
    from f_fut_quote_l1 
    group by ts_event, instrument_id
);

create unique index if not exists idx_f_fut_quote_l1_unique on f_fut_quote_l1 (ts_event, instrument_id);

-- ES Continuous Futures: Remove duplicates first, then create unique index
delete from f_continuous_quote_l1 
where rowid not in (
    select min(rowid) 
    from f_continuous_quote_l1 
    group by ts_event, contract_series, underlying_instrument_id
);

create unique index if not exists idx_f_continuous_quote_l1_unique on f_continuous_quote_l1 (ts_event, contract_series, underlying_instrument_id);

