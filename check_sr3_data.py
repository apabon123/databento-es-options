"""Quick script to check SR3 contract series data in the database."""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
import pandas as pd

def check_sr3_data():
    """Check what SR3 contract series data exists in the database."""
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
        
        # Get SR3 contract series coverage
        result = con.execute("""
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
            WHERE contract_series LIKE 'SR3_%'
            GROUP BY contract_series
            ORDER BY contract_series
        """).fetchdf()
        
        if result.empty:
            print("=" * 80)
            print("SR3 CONTRACT SERIES DATA")
            print("=" * 80)
            print("\nNo SR3 data found in the database.")
            print("\nTo download SR3 data, run:")
            print("  python scripts/download/download_universe_daily_ohlcv.py --roots SR3 --start 2015-01-01 --end 2025-12-31")
        else:
            print("=" * 80)
            print("SR3 CONTRACT SERIES DATA")
            print("=" * 80)
            print(f"\nFound {len(result)} SR3 contract series:")
            print()
            
            # Add rank column for sorting
            def extract_rank(series):
                if 'FRONT' in series:
                    return 0
                elif 'RANK_' in series:
                    import re
                    match = re.search(r'RANK_(\d+)', series)
                    if match:
                        return int(match.group(1))
                return 999
            
            result['rank'] = result['contract_series'].apply(extract_rank)
            result_sorted = result.sort_values('rank')
            
            # Display with rank column
            display_cols = ['rank', 'contract_series', 'bar_count', 'first_date', 'last_date', 'trading_days', 'avg_volume', 'avg_close']
            print(result_sorted[display_cols].to_string(index=False))
            
            # Summary statistics
            print("\n" + "=" * 80)
            print("SUMMARY")
            print("=" * 80)
            print(f"Total contract series: {len(result)}")
            print(f"Total bars: {result['bar_count'].sum():,}")
            print(f"Date range: {result['first_date'].min()} to {result['last_date'].max()}")
            print(f"Total trading days covered: {result['trading_days'].sum():,}")
            print(f"Total volume: {result['total_volume'].sum():,}")
            
            # Check for missing ranks (expected 0-12)
            print("\n" + "=" * 80)
            print("RANK COVERAGE")
            print("=" * 80)
            ranks_found = []
            for series in result['contract_series']:
                # Extract rank from series name
                # Format: SR3_FRONT_CALENDAR (rank 0) or SR3_RANK_N_CALENDAR (rank N)
                if 'FRONT' in series:
                    ranks_found.append(0)
                elif 'RANK_' in series:
                    # Extract number after RANK_
                    import re
                    match = re.search(r'RANK_(\d+)', series)
                    if match:
                        rank = int(match.group(1))
                        ranks_found.append(rank)
            
            ranks_found = sorted(set(ranks_found))
            expected_ranks = list(range(13))  # 0-12
            missing_ranks = [r for r in expected_ranks if r not in ranks_found]
            
            print(f"Ranks found: {ranks_found}")
            if missing_ranks:
                print(f"Missing ranks: {missing_ranks}")
            else:
                print("All expected ranks (0-12) are present!")
            
    finally:
        con.close()

if __name__ == "__main__":
    check_sr3_data()

