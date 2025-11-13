"""Check and clean up instrument definitions that aren't in daily bars."""
from pipelines.common import get_paths, connect_duckdb
import pandas as pd

_, _, dbpath = get_paths()
con = connect_duckdb(dbpath)

print("=" * 80)
print("CHECKING INSTRUMENT DEFINITIONS")
print("=" * 80)
print()

# Get all instrument IDs from daily bars
daily_bar_instruments = con.execute("""
    SELECT DISTINCT underlying_instrument_id
    FROM g_continuous_bar_daily
    WHERE underlying_instrument_id IS NOT NULL
""").fetchdf()['underlying_instrument_id'].tolist()

print(f"Instruments in daily bars: {len(daily_bar_instruments)}")
print()

# Get all definitions
all_defs = con.execute("""
    SELECT COUNT(*) as count
    FROM dim_instrument_definition
""").fetchdf()['count'].iloc[0]

print(f"Total definitions in database: {all_defs}")
print()

# Get definitions that match daily bars
matching_defs = con.execute("""
    SELECT COUNT(*) as count
    FROM dim_instrument_definition
    WHERE instrument_id IN (
        SELECT DISTINCT underlying_instrument_id
        FROM g_continuous_bar_daily
        WHERE underlying_instrument_id IS NOT NULL
    )
""").fetchdf()['count'].iloc[0]

print(f"Definitions matching daily bars: {matching_defs}")
print(f"Definitions NOT in daily bars: {all_defs - matching_defs}")
print()

# Show which assets have definitions not in daily bars
orphaned = con.execute("""
    SELECT 
        asset,
        COUNT(*) as count,
        STRING_AGG(DISTINCT native_symbol, ', ') as symbols
    FROM dim_instrument_definition
    WHERE instrument_id NOT IN (
        SELECT DISTINCT underlying_instrument_id
        FROM g_continuous_bar_daily
        WHERE underlying_instrument_id IS NOT NULL
    )
    GROUP BY asset
    ORDER BY count DESC
""").fetchdf()

if not orphaned.empty:
    print("Definitions NOT in daily bars (by asset):")
    print(orphaned.to_string(index=False))
    print()

# Check ES specifically
es_in_daily = con.execute("""
    SELECT COUNT(DISTINCT underlying_instrument_id) as count
    FROM g_continuous_bar_daily
    JOIN dim_continuous_contract ON g_continuous_bar_daily.contract_series = dim_continuous_contract.contract_series
    WHERE dim_continuous_contract.root = 'ES'
""").fetchdf()['count'].iloc[0]

es_defs_matching = con.execute("""
    SELECT COUNT(*) as count
    FROM dim_instrument_definition
    WHERE asset = 'ES'
    AND instrument_id IN (
        SELECT DISTINCT underlying_instrument_id
        FROM g_continuous_bar_daily
        JOIN dim_continuous_contract ON g_continuous_bar_daily.contract_series = dim_continuous_contract.contract_series
        WHERE dim_continuous_contract.root = 'ES'
    )
""").fetchdf()['count'].iloc[0]

es_defs_total = con.execute("""
    SELECT COUNT(*) as count
    FROM dim_instrument_definition
    WHERE asset = 'ES'
""").fetchdf()['count'].iloc[0]

print("=" * 80)
print("ES FUTURES ANALYSIS")
print("=" * 80)
print(f"ES instruments in daily bars: {es_in_daily}")
print(f"ES definitions total: {es_defs_total}")
print(f"ES definitions matching daily bars: {es_defs_matching}")
print(f"ES definitions NOT in daily bars: {es_defs_total - es_defs_matching}")
print(f"ES instruments missing definitions: {es_in_daily - es_defs_matching}")
print()

# Show missing ES instruments
missing_es = con.execute("""
    SELECT DISTINCT
        underlying_instrument_id,
        MIN(trading_date) as first_date,
        MAX(trading_date) as last_date
    FROM g_continuous_bar_daily
    JOIN dim_continuous_contract ON g_continuous_bar_daily.contract_series = dim_continuous_contract.contract_series
    WHERE dim_continuous_contract.root = 'ES'
    AND underlying_instrument_id NOT IN (
        SELECT instrument_id FROM dim_instrument_definition
    )
    GROUP BY underlying_instrument_id
    ORDER BY underlying_instrument_id
""").fetchdf()

if not missing_es.empty:
    print("ES instruments in daily bars but missing definitions:")
    print(missing_es.to_string(index=False))
    print()

con.close()

