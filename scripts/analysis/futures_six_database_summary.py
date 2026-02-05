"""Focused database summary for Futures-Six integration."""
import duckdb
from pathlib import Path
from pipelines.common import get_paths, connect_duckdb

def main():
    """Generate focused summary for Futures-Six."""
    db_path = get_database_path()
    print(f"\n{'='*80}")
    print(f"DATABASE LOCATION")
    print(f"{'='*80}")
    print(f"Path: {db_path}")
    print(f"Absolute: {db_path.resolve()}")
    print(f"Exists: {db_path.exists()}")
    
    if not db_path.exists():
        print("\nERROR: Database file not found!")
        return
    
    con = connect_duckdb(db_path)
    
    try:
        print(f"\n{'='*80}")
        print("SPOT INDICES (for implied dividends/equity carry)")
        print(f"{'='*80}\n")
        
        spot_indices = con.execute("""
            SELECT 
                series_id,
                COUNT(*) as rows,
                MIN(date) as first_date,
                MAX(date) as last_date
            FROM f_fred_observations
            WHERE series_id IN ('SP500', 'NASDAQ100', 'RUT_SPOT')
            GROUP BY series_id
            ORDER BY series_id
        """).fetchdf()
        
        print(spot_indices.to_string(index=False))
        
        print(f"\n{'='*80}")
        print("CONTINUOUS CONTRACT SERIES (for futures data)")
        print(f"{'='*80}\n")
        
        contracts = con.execute("""
            SELECT 
                contract_series,
                root,
                roll_rule,
                description
            FROM dim_continuous_contract
            ORDER BY root, contract_series
        """).fetchdf()
        
        print(contracts.to_string(index=False))
        
        print(f"\n{'='*80}")
        print("CONTINUOUS DAILY BARS COVERAGE")
        print(f"{'='*80}\n")
        
        bars_coverage = con.execute("""
            SELECT 
                contract_series,
                COUNT(*) as rows,
                MIN(trading_date) as first_date,
                MAX(trading_date) as last_date
            FROM g_continuous_bar_daily
            GROUP BY contract_series
            ORDER BY contract_series
        """).fetchdf()
        
        print(bars_coverage.to_string(index=False))
        
        print(f"\n{'='*80}")
        print("ALL FRED SERIES (macro data)")
        print(f"{'='*80}\n")
        
        fred_series = con.execute("""
            SELECT 
                series_id,
                COUNT(*) as rows,
                MIN(date) as first_date,
                MAX(date) as last_date
            FROM f_fred_observations
            GROUP BY series_id
            ORDER BY series_id
        """).fetchdf()
        
        print(fred_series.to_string(index=False))
        
        print(f"\n{'='*80}")
        print("QUERY EXAMPLES FOR FUTURES-SIX")
        print(f"{'='*80}\n")
        
        print("1. Get SPX spot index:")
        print("   SELECT date, value as spx_close")
        print("   FROM f_fred_observations")
        print("   WHERE series_id = 'SP500'")
        print("     AND date BETWEEN '2020-01-01' AND '2026-01-20'")
        print("   ORDER BY date;")
        print()
        
        print("2. Get NDX spot index:")
        print("   SELECT date, value as ndx_close")
        print("   FROM f_fred_observations")
        print("   WHERE series_id = 'NASDAQ100'")
        print("     AND date BETWEEN '2020-01-01' AND '2026-01-20'")
        print("   ORDER BY date;")
        print()
        
        print("3. Get RUT spot index:")
        print("   SELECT date, value as rut_close")
        print("   FROM f_fred_observations")
        print("   WHERE series_id = 'RUT_SPOT'")
        print("     AND date BETWEEN '2020-01-01' AND '2026-01-20'")
        print("   ORDER BY date;")
        print()
        
        print("4. Get ES continuous daily bars:")
        print("   SELECT trading_date, open, high, low, close, volume")
        print("   FROM g_continuous_bar_daily")
        print("   WHERE contract_series = 'ES_FRONT_CALENDAR_2D'")
        print("     AND trading_date >= '2020-01-01'")
        print("   ORDER BY trading_date;")
        print()
        
        print("5. Get all available contract series:")
        print("   SELECT contract_series, root, roll_rule")
        print("   FROM dim_continuous_contract")
        print("   WHERE root = 'ES'")
        print("   ORDER BY contract_series;")
        
    finally:
        con.close()

def get_database_path():
    """Get the database path."""
    _, _, db_path = get_paths()
    return db_path

if __name__ == "__main__":
    main()
