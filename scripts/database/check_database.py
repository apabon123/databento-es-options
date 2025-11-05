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
        if args.stats_only:
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

