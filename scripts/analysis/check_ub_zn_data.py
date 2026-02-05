"""Check UB and ZN contract series data in the database."""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
import pandas as pd

def check_ub_zn_data():
    """Check what UB and ZN contract series data exists in the database."""
    _, _, dbpath = get_paths()
    
    if not dbpath.exists():
        print("Database not found at:", dbpath)
        return
    
    con = connect_duckdb(dbpath)
    
    try:
        # Check if table exists
        table_exists = con.execute("""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables 
            WHERE table_name = 'g_continuous_bar_daily'
        """).fetchone()[0]
        
        if table_exists == 0:
            print("Table 'g_continuous_bar_daily' does not exist.")
            return
        
        # Check for UB data
        print("=" * 80)
        print("UB (ULTRA-BOND) DATA")
        print("=" * 80)
        
        ub_result = con.execute("""
            SELECT 
                contract_series,
                COUNT(*) as bar_count,
                MIN(trading_date) as first_date,
                MAX(trading_date) as last_date,
                COUNT(DISTINCT trading_date) as trading_days,
                ROUND(AVG(volume), 0) as avg_volume,
                SUM(volume) as total_volume,
                ROUND(AVG(close), 4) as avg_close
            FROM g_continuous_bar_daily
            WHERE contract_series LIKE 'UB_%'
            GROUP BY contract_series
            ORDER BY contract_series
        """).fetchdf()
        
        if ub_result.empty:
            print("\nNo UB data found in the database.")
        else:
            print(f"\nFound {len(ub_result)} UB contract series:")
            print()
            print(ub_result.to_string(index=False))
            
            print("\nSummary:")
            print(f"  Total bars: {ub_result['bar_count'].sum():,}")
            print(f"  Date range: {ub_result['first_date'].min()} to {ub_result['last_date'].max()}")
            print(f"  Total volume: {ub_result['total_volume'].sum():,}")
        
        # Check for ZN data
        print("\n" + "=" * 80)
        print("ZN (10Y TREASURY NOTE) DATA")
        print("=" * 80)
        
        zn_result = con.execute("""
            SELECT 
                contract_series,
                COUNT(*) as bar_count,
                MIN(trading_date) as first_date,
                MAX(trading_date) as last_date,
                COUNT(DISTINCT trading_date) as trading_days,
                ROUND(AVG(volume), 0) as avg_volume,
                SUM(volume) as total_volume,
                ROUND(AVG(close), 4) as avg_close
            FROM g_continuous_bar_daily
            WHERE contract_series LIKE 'ZN_%'
            GROUP BY contract_series
            ORDER BY contract_series
        """).fetchdf()
        
        if zn_result.empty:
            print("\nNo ZN data found in the database.")
        else:
            print(f"\nFound {len(zn_result)} ZN contract series:")
            print()
            print(zn_result.to_string(index=False))
            
            print("\nSummary:")
            print(f"  Total bars: {zn_result['bar_count'].sum():,}")
            print(f"  Date range: {zn_result['first_date'].min()} to {zn_result['last_date'].max()}")
            print(f"  Total volume: {zn_result['total_volume'].sum():,}")
        
        # Check contract definitions
        print("\n" + "=" * 80)
        print("CONTRACT SERIES DEFINITIONS")
        print("=" * 80)
        
        # Check what columns exist in the table
        try:
            definitions = con.execute("""
                SELECT 
                    contract_series,
                    root,
                    roll_rule,
                    description
                FROM dim_continuous_contract
                WHERE root IN ('UB', 'ZN')
                ORDER BY root, contract_series
            """).fetchdf()
            
            if definitions.empty:
                print("\nNo contract definitions found for UB or ZN.")
            else:
                print(f"\nFound {len(definitions)} contract series definitions:")
                print()
                print(definitions.to_string(index=False))
        except Exception as e:
            print(f"\nCould not query contract definitions: {e}")
            
    finally:
        con.close()

if __name__ == "__main__":
    check_ub_zn_data()
