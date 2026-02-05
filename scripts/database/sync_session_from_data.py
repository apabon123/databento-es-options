"""
Sync dim_session from actual trading dates in g_continuous_bar_daily.

This derives the trading calendar from actual ingested data rather than 
assuming any particular schedule (e.g., equity Mon-Fri or CME holidays).
Futures trade ~314 days/year including many Sundays.

The source of truth is whatever days have data in the database.

Usage:
    python scripts/database/sync_session_from_data.py
    python scripts/database/sync_session_from_data.py --dry-run
    python scripts/database/sync_session_from_data.py --db-path ./data/silver/market.duckdb
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
from src.utils.calendar import sync_dim_session_from_data, get_dim_session_count


def check_tables_exist(con) -> tuple[bool, bool]:
    """Check if required tables exist."""
    dim_session_exists = (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'dim_session'"
        ).fetchone()[0]
        > 0
    )
    g_continuous_exists = (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'g_continuous_bar_daily'"
        ).fetchone()[0]
        > 0
    )
    return dim_session_exists, g_continuous_exists


def main():
    parser = argparse.ArgumentParser(
        description="Sync dim_session from actual trading dates in g_continuous_bar_daily"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be inserted without actually inserting",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to DuckDB database (default: from env/config)",
    )
    args = parser.parse_args()

    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        return 1

    con = connect_duckdb(db_path)
    try:
        dim_session_exists, g_continuous_exists = check_tables_exist(con)

        if not dim_session_exists:
            print("ERROR: Table dim_session does not exist. Run migrations first.")
            return 1

        if not g_continuous_exists:
            print("ERROR: Table g_continuous_bar_daily does not exist.")
            print("       Ingest continuous daily data first before syncing the calendar.")
            return 1

        # Check if g_continuous_bar_daily has data
        row_count = con.execute(
            "SELECT COUNT(*) FROM g_continuous_bar_daily"
        ).fetchone()[0]

        if row_count == 0:
            print("WARNING: g_continuous_bar_daily is empty. No trading dates to sync.")
            return 0

        # Get current dim_session count
        current_count = get_dim_session_count(con)

        if args.dry_run:
            would_insert = sync_dim_session_from_data(con, dry_run=True)
            print("=" * 60)
            print("DRY RUN: dim_session sync from g_continuous_bar_daily")
            print("=" * 60)
            print(f"Current dim_session rows: {current_count}")
            print(f"Would insert: {would_insert} new trading dates")
            
            if would_insert > 0:
                # Show date range of new dates
                new_dates_info = con.execute(
                    """
                    SELECT 
                        MIN(trading_date) as first_date,
                        MAX(trading_date) as last_date,
                        COUNT(DISTINCT trading_date) as count
                    FROM g_continuous_bar_daily
                    WHERE trading_date NOT IN (SELECT trade_date FROM dim_session)
                    """
                ).fetchone()
                print(f"Date range: {new_dates_info[0]} to {new_dates_info[1]}")
            print("=" * 60)
        else:
            inserted = sync_dim_session_from_data(con, dry_run=False)
            new_count = get_dim_session_count(con)
            print(f"Synced {inserted} trading dates to dim_session")
            print(f"Total dim_session rows: {new_count}")

        return 0

    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
