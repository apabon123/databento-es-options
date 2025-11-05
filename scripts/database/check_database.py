"""
Database integrity check script.
Checks for duplicate rows and reports database statistics.

Usage:
    python scripts/database/check_database.py
    python scripts/database/check_database.py --product ES_CONTINUOUS_MDP3
"""

import sys
import argparse
from pathlib import Path
from datetime import date

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
import duckdb
from datetime import timedelta


def check_duplicates(con, table_name: str, unique_columns: list) -> dict:
    """
    Check for duplicate rows in a table based on unique columns.
    
    Returns:
        dict with 'total_rows', 'unique_rows', 'duplicate_count', 'duplicates' list
    """
    # Build column list for GROUP BY
    cols_str = ", ".join(unique_columns)
    
    # Count total rows
    total_query = f"SELECT COUNT(*) as cnt FROM {table_name}"
    total_rows = con.execute(total_query).fetchone()[0]
    
    # Count unique combinations
    unique_query = f"SELECT COUNT(DISTINCT ({cols_str})) as cnt FROM {table_name}"
    unique_rows = con.execute(unique_query).fetchone()[0]
    
    duplicate_count = total_rows - unique_rows
    
    # Find actual duplicate rows
    duplicates = []
    if duplicate_count > 0:
        dup_query = f"""
        SELECT {cols_str}, COUNT(*) as dup_count
        FROM {table_name}
        GROUP BY {cols_str}
        HAVING COUNT(*) > 1
        ORDER BY dup_count DESC
        LIMIT 10
        """
        duplicates = con.execute(dup_query).fetchdf()
    
    return {
        'total_rows': total_rows,
        'unique_rows': unique_rows,
        'duplicate_count': duplicate_count,
        'duplicates': duplicates
    }


def check_table_exists(con, table_name: str) -> bool:
    """Check if a table exists in the database."""
    result = con.execute("""
        SELECT COUNT(*) as cnt
        FROM information_schema.tables 
        WHERE table_name = ?
    """, [table_name]).fetchone()[0]
    return result > 0


def check_all_products(con):
    """Check all products for duplicates."""
    print("=" * 80)
    print("DATABASE INTEGRITY CHECK")
    print("=" * 80)
    print()
    
    # Check ES Options
    if check_table_exists(con, "f_quote_l1"):
        print("Checking ES Options...")
        result = check_duplicates(con, "f_quote_l1", ["ts_event", "instrument_id"])
        print(f"  Table: f_quote_l1")
        print(f"  Total rows: {result['total_rows']:,}")
        print(f"  Unique rows: {result['unique_rows']:,}")
        print(f"  Duplicates: {result['duplicate_count']:,}")
        if result['duplicate_count'] > 0:
            print(f"  [WARNING] Found {result['duplicate_count']:,} duplicate rows!")
            print(f"  Sample duplicates:")
            print(result['duplicates'].to_string(index=False))
        else:
            print(f"  [OK] No duplicates found")
        print()
    
    # Check ES Futures
    if check_table_exists(con, "f_fut_quote_l1"):
        print("Checking ES Futures...")
        result = check_duplicates(con, "f_fut_quote_l1", ["ts_event", "instrument_id"])
        print(f"  Table: f_fut_quote_l1")
        print(f"  Total rows: {result['total_rows']:,}")
        print(f"  Unique rows: {result['unique_rows']:,}")
        print(f"  Duplicates: {result['duplicate_count']:,}")
        if result['duplicate_count'] > 0:
            print(f"  [WARNING] Found {result['duplicate_count']:,} duplicate rows!")
            print(f"  Sample duplicates:")
            print(result['duplicates'].to_string(index=False))
        else:
            print(f"  [OK] No duplicates found")
        print()
    
    # Check ES Continuous Futures
    if check_table_exists(con, "f_continuous_quote_l1"):
        print("Checking ES Continuous Futures...")
        result = check_duplicates(con, "f_continuous_quote_l1", 
                                  ["ts_event", "contract_series", "underlying_instrument_id"])
        print(f"  Table: f_continuous_quote_l1")
        print(f"  Total rows: {result['total_rows']:,}")
        print(f"  Unique rows: {result['unique_rows']:,}")
        print(f"  Duplicates: {result['duplicate_count']:,}")
        if result['duplicate_count'] > 0:
            print(f"  [WARNING] Found {result['duplicate_count']:,} duplicate rows!")
            print(f"  Sample duplicates:")
            print(result['duplicates'].to_string(index=False))
        else:
            print(f"  [OK] No duplicates found")
        print()
    
    print("=" * 80)
    print("INTEGRITY CHECK COMPLETE")
    print("=" * 80)


def check_specific_product(con, product: str):
    """Check a specific product for duplicates."""
    print("=" * 80)
    print(f"DATABASE INTEGRITY CHECK - {product}")
    print("=" * 80)
    print()
    
    if product == "ES_OPTIONS_MDP3":
        table = "f_quote_l1"
        cols = ["ts_event", "instrument_id"]
    elif product == "ES_FUTURES_MDP3":
        table = "f_fut_quote_l1"
        cols = ["ts_event", "instrument_id"]
    elif product == "ES_CONTINUOUS_MDP3":
        table = "f_continuous_quote_l1"
        cols = ["ts_event", "contract_series", "underlying_instrument_id"]
    else:
        print(f"Unknown product: {product}")
        return
    
    if not check_table_exists(con, table):
        print(f"Table {table} does not exist in database.")
        return
    
    result = check_duplicates(con, table, cols)
    print(f"Table: {table}")
    print(f"Total rows: {result['total_rows']:,}")
    print(f"Unique rows: {result['unique_rows']:,}")
    print(f"Duplicates: {result['duplicate_count']:,}")
    
    if result['duplicate_count'] > 0:
        print(f"\n[WARNING] Found {result['duplicate_count']:,} duplicate rows!")
        print(f"\nSample duplicates (showing first 10):")
        print(result['duplicates'].to_string(index=False))
    else:
        print(f"\n[OK] No duplicates found - database is clean!")
    
    print()
    print("=" * 80)


def show_summary_stats(con):
    """Show summary statistics for all tables."""
    print("=" * 80)
    print("DATABASE SUMMARY STATISTICS")
    print("=" * 80)
    print()
    
    # ES Options
    if check_table_exists(con, "f_quote_l1"):
        stats = con.execute("""
            SELECT 
                COUNT(*) as total_quotes,
                COUNT(DISTINCT instrument_id) as unique_instruments,
                COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days,
                MIN(ts_event) as first_quote,
                MAX(ts_event) as last_quote
            FROM f_quote_l1
        """).fetchone()
        print("ES Options:")
        print(f"  Total quotes: {stats[0]:,}")
        print(f"  Unique instruments: {stats[1]:,}")
        print(f"  Trading days: {stats[2]}")
        print(f"  Date range: {stats[3]} to {stats[4]}")
        print()
    
    # ES Futures
    if check_table_exists(con, "f_fut_quote_l1"):
        stats = con.execute("""
            SELECT 
                COUNT(*) as total_quotes,
                COUNT(DISTINCT instrument_id) as unique_instruments,
                COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days,
                MIN(ts_event) as first_quote,
                MAX(ts_event) as last_quote
            FROM f_fut_quote_l1
        """).fetchone()
        print("ES Futures:")
        print(f"  Total quotes: {stats[0]:,}")
        print(f"  Unique instruments: {stats[1]:,}")
        print(f"  Trading days: {stats[2]}")
        print(f"  Date range: {stats[3]} to {stats[4]}")
        print()
    
    # ES Continuous Futures
    if check_table_exists(con, "f_continuous_quote_l1"):
        stats = con.execute("""
            SELECT 
                COUNT(*) as total_quotes,
                COUNT(DISTINCT contract_series) as unique_series,
                COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days,
                MIN(ts_event) as first_quote,
                MAX(ts_event) as last_quote
            FROM f_continuous_quote_l1
        """).fetchone()
        print("ES Continuous Futures:")
        print(f"  Total quotes: {stats[0]:,}")
        print(f"  Unique contract series: {stats[1]:,}")
        print(f"  Trading days: {stats[2]}")
        print(f"  Date range: {stats[3]} to {stats[4]}")
        print()
    
    print("=" * 80)


def verify_continuous_coverage(con, year: int = 2025):
    """Verify complete coverage of continuous futures data for a given year."""
    from datetime import date, datetime
    
    def get_trading_days(y):
        """Get all trading days (Mon-Fri) for a year."""
        start = date(y, 1, 1)
        end = date(y, 12, 31)
        trading_days = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # Monday = 0, Friday = 4
                trading_days.append(current)
            current += timedelta(days=1)
        return trading_days
    
    print("=" * 80)
    print(f"CONTINUOUS FUTURES DATA COVERAGE VERIFICATION - {year}")
    print("=" * 80)
    print()
    
    # Get all trading days for the year
    expected_days = get_trading_days(year)
    print(f"Expected trading days in {year}: {len(expected_days)}")
    print(f"Date range: {min(expected_days)} to {max(expected_days)}")
    print()
    
    # Get dates in database
    db_data = con.execute('''
        SELECT 
            CAST(ts_event AS DATE) as date,
            COUNT(*) as quote_count
        FROM f_continuous_quote_l1
        WHERE CAST(ts_event AS DATE) IS NOT NULL
          AND EXTRACT(YEAR FROM ts_event) = ?
        GROUP BY CAST(ts_event AS DATE)
        ORDER BY date
    ''', [year]).fetchdf()
    
    # Convert database dates to date objects for comparison
    db_date_objects = [d.date() if isinstance(d, datetime) else d for d in db_data['date'].tolist()]
    db_dates = set(db_date_objects)
    print(f"Dates in database: {len(db_dates)}")
    
    # Find missing dates
    missing_dates = sorted([d for d in expected_days if d not in db_dates])
    
    if missing_dates:
        print(f"\nMISSING DATES: {len(missing_dates)}")
        print("-" * 80)
        # Group by month
        by_month = {}
        for d in missing_dates:
            month_key = (d.year, d.month)
            if month_key not in by_month:
                by_month[month_key] = []
            by_month[month_key].append(d)
        
        for (y, m), days in sorted(by_month.items()):
            print(f"{y}-{m:02d}: {len(days)} missing days")
            if len(days) <= 10:
                print(f"  Dates: {[str(d) for d in days]}")
            else:
                print(f"  Dates: {[str(d) for d in days[:5]]} ... and {len(days)-5} more")
    else:
        print("\n✓ All expected trading days are present!")
    print()
    
    # Check quote counts per day
    print("QUOTE COUNT ANALYSIS:")
    print("-" * 80)
    
    full_days = db_data[db_data['quote_count'] >= 1200]
    partial_days = db_data[(db_data['quote_count'] >= 200) & (db_data['quote_count'] < 1200)]
    test_days = db_data[db_data['quote_count'] < 50]
    very_partial = db_data[(db_data['quote_count'] >= 50) & (db_data['quote_count'] < 200)]
    
    print(f"Full days (~1,300 quotes): {len(full_days)} days")
    if len(full_days) > 0:
        print(f"  Range: {full_days['quote_count'].min():.0f} - {full_days['quote_count'].max():.0f} quotes")
        print(f"  Average: {full_days['quote_count'].mean():.0f} quotes/day")
    
    print(f"\nPartial days (200-1,200 quotes): {len(partial_days)} days")
    if len(partial_days) > 0:
        print(f"  Range: {partial_days['quote_count'].min():.0f} - {partial_days['quote_count'].max():.0f} quotes")
    
    print(f"\nVery partial days (50-200 quotes): {len(very_partial)} days")
    if len(very_partial) > 0:
        print(f"  Dates: {[str(d) for d in very_partial['date'].tolist()[:10]]}")
    
    print(f"\nTest data (< 50 quotes): {len(test_days)} days")
    if len(test_days) > 0:
        print(f"  Dates: {[str(d) for d in test_days['date'].tolist()]}")
    
    print()
    
    # Monthly summary
    print("MONTHLY SUMMARY:")
    print("-" * 80)
    monthly = con.execute('''
        SELECT 
            EXTRACT(YEAR FROM ts_event) as year,
            EXTRACT(MONTH FROM ts_event) as month,
            COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days,
            COUNT(*) as total_quotes,
            AVG(daily_quotes) as avg_quotes_per_day
        FROM (
            SELECT 
                ts_event,
                COUNT(*) OVER (PARTITION BY CAST(ts_event AS DATE)) as daily_quotes
            FROM f_continuous_quote_l1
            WHERE CAST(ts_event AS DATE) IS NOT NULL
              AND EXTRACT(YEAR FROM ts_event) = ?
        ) sub
        GROUP BY EXTRACT(YEAR FROM ts_event), EXTRACT(MONTH FROM ts_event)
        ORDER BY year, month
    ''', [year]).fetchdf()
    
    print(monthly.to_string(index=False))
    print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Expected trading days: {len(expected_days)}")
    print(f"Dates in database: {len(db_dates)}")
    print(f"Missing dates: {len(missing_dates)}")
    print(f"Full days: {len(full_days)}")
    print(f"Partial days: {len(partial_days)}")
    print(f"Test data days: {len(test_days)}")
    print()
    
    if len(missing_dates) == 0 and len(full_days) == len(expected_days):
        print("[SUCCESS] PERFECT COVERAGE: All expected trading days have full data!")
    elif len(missing_dates) == 0:
        print("[WARNING] ALL DAYS PRESENT: But some days have partial data")
    else:
        print("[INCOMPLETE] Missing dates or partial data")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Check database for duplicates and show statistics"
    )
    parser.add_argument(
        '--product',
        type=str,
        choices=['ES_OPTIONS_MDP3', 'ES_FUTURES_MDP3', 'ES_CONTINUOUS_MDP3'],
        help='Check specific product only'
    )
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Show statistics only, skip duplicate check'
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default=None,
        help='Path to DuckDB database (default: data/silver/market.duckdb)'
    )
    parser.add_argument(
        '--verify-coverage',
        action='store_true',
        help='Verify complete coverage for continuous futures (checks for missing dates and data quality)'
    )
    parser.add_argument(
        '--year',
        type=int,
        default=2025,
        help='Year to verify coverage for (default: 2025)'
    )
    
    args = parser.parse_args()
    
    # Get database path
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()
    
    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        print("Please run migrations first: python orchestrator.py migrate")
        return 1
    
    # Connect to database
    con = connect_duckdb(db_path)
    
    try:
        if args.verify_coverage:
            verify_continuous_coverage(con, args.year)
        elif args.stats_only:
            show_summary_stats(con)
        elif args.product:
            check_specific_product(con, args.product)
        else:
            check_all_products(con)
            print()
            show_summary_stats(con)
    finally:
        con.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

