"""Check what definitions we have vs what we should have."""
from pipelines.common import get_paths, connect_duckdb
import pandas as pd

_, _, dbpath = get_paths()
con = connect_duckdb(dbpath)

# Check what instruments we have in daily bars
print("=" * 80)
print("INSTRUMENTS IN DATABASE (from daily bars)")
print("=" * 80)
result = con.execute("""
    SELECT 
        c.root,
        COUNT(DISTINCT b.underlying_instrument_id) as instrument_count
    FROM g_continuous_bar_daily b
    JOIN dim_continuous_contract c ON b.contract_series = c.contract_series
    GROUP BY c.root
    ORDER BY c.root
""").fetchdf()
print(result.to_string(index=False))
print()

# Check what definitions we have
print("=" * 80)
print("DEFINITIONS IN DATABASE")
print("=" * 80)
defs_result = con.execute("""
    SELECT 
        asset,
        COUNT(*) as definition_count
    FROM dim_instrument_definition
    GROUP BY asset
    ORDER BY asset
""").fetchdf()
print(defs_result.to_string(index=False))
print()

# Check ES specifically
print("=" * 80)
print("ES DEFINITIONS")
print("=" * 80)
es_defs = con.execute("""
    SELECT 
        instrument_id,
        native_symbol,
        expiration,
        maturity_year,
        maturity_month
    FROM dim_instrument_definition
    WHERE asset = 'ES'
    ORDER BY expiration
""").fetchdf()
print(f"Count: {len(es_defs)}")
if not es_defs.empty:
    print(es_defs.to_string(index=False))
print()

# Check what ES instruments we have but don't have definitions for
print("=" * 80)
print("ES INSTRUMENTS MISSING DEFINITIONS")
print("=" * 80)
missing = con.execute("""
    SELECT DISTINCT
        b.underlying_instrument_id,
        MIN(b.trading_date) as first_date,
        MAX(b.trading_date) as last_date
    FROM g_continuous_bar_daily b
    JOIN dim_continuous_contract c ON b.contract_series = c.contract_series
    WHERE c.root = 'ES'
      AND b.underlying_instrument_id NOT IN (
          SELECT instrument_id FROM dim_instrument_definition
      )
    GROUP BY b.underlying_instrument_id
    ORDER BY b.underlying_instrument_id
""").fetchdf()
print(f"Missing: {len(missing)} ES instrument definitions")
if not missing.empty:
    print(missing.head(20).to_string(index=False))
print()

# Check SR3 specifically
print("=" * 80)
print("SR3 CHECK")
print("=" * 80)
sr3_check = con.execute("""
    SELECT 
        COUNT(DISTINCT b.underlying_instrument_id) as instrument_count
    FROM g_continuous_bar_daily b
    JOIN dim_continuous_contract c ON b.contract_series = c.contract_series
    WHERE c.root = 'SR3'
""").fetchdf()
print(f"SR3 instruments in database: {sr3_check['instrument_count'].iloc[0] if not sr3_check.empty else 0}")

sr3_contracts = con.execute("""
    SELECT DISTINCT contract_series
    FROM dim_continuous_contract
    WHERE root = 'SR3'
""").fetchdf()
print(f"SR3 contract series in database: {len(sr3_contracts)}")
if not sr3_contracts.empty:
    print(sr3_contracts.to_string(index=False))

con.close()


