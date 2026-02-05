"""
Show coverage for canonical Futures-Six continuous series.

Queries v_canonical_continuous_bar_daily (or g_continuous_bar_daily filtered by
dim_canonical_series) and reports bar counts, date ranges, and coverage summary
per canonical root.

IMPORTANT: Coverage percentage requires dim_session to be populated from actual
trading data. Futures trade ~314 days/year including many Sundays.
Do NOT assume equity-style Mon-Fri trading.

Usage:
    python scripts/analysis/coverage_canonical.py
    python scripts/analysis/coverage_canonical.py --year 2025
    python scripts/analysis/coverage_canonical.py --start 2024-01-01 --end 2024-12-31
"""

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
from src.utils.calendar import get_dim_session_count, get_trading_days_from_dim_session


def main():
    parser = argparse.ArgumentParser(
        description="Show coverage for canonical Futures-Six continuous series"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Report coverage for this calendar year",
    )
    parser.add_argument(
        "--start",
        type=str,
        metavar="YYYY-MM-DD",
        help="Start date (with --end)",
    )
    parser.add_argument(
        "--end",
        type=str,
        metavar="YYYY-MM-DD",
        help="End date (with --start)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        help="Path to DuckDB database (default: from env/config)",
    )
    args = parser.parse_args()

    if args.year:
        start = date(args.year, 1, 1)
        end = date(args.year, 12, 31)
    elif args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    else:
        start = None
        end = None

    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        return 1

    con = connect_duckdb(db_path)

    # Check for v_canonical_continuous_bar_daily
    views = con.execute(
        "SELECT COUNT(*) FROM information_schema.views WHERE view_name = 'v_canonical_continuous_bar_daily'"
    ).fetchone()[0]

    if views == 0:
        print("ERROR: View v_canonical_continuous_bar_daily does not exist. Run migrations.")
        return 1

    # Check if dim_session is populated (needed for coverage percentage)
    dim_session_count = get_dim_session_count(con)
    can_compute_coverage = dim_session_count > 0

    print("=" * 80)
    print("CANONICAL SERIES COVERAGE (v_canonical_continuous_bar_daily)")
    print("=" * 80)

    if not can_compute_coverage and (start and end):
        print()
        print("WARNING: dim_session is empty. Cannot compute coverage percentage.")
        print("         Run: python scripts/database/sync_session_from_data.py")
        print()

    # Overall coverage from view
    if start and end:
        if can_compute_coverage:
            trading_days = get_trading_days_from_dim_session(con, start, end)
            expected = len(trading_days)
        else:
            expected = None
        df = con.execute(
            """
            SELECT root, contract_series,
                   COUNT(*) as bar_count,
                   MIN(trading_date) as first_date,
                   MAX(trading_date) as last_date
            FROM v_canonical_continuous_bar_daily
            WHERE trading_date >= ? AND trading_date <= ?
            GROUP BY root, contract_series
            ORDER BY root
            """,
            [start.isoformat(), end.isoformat()],
        ).fetchdf()
        if expected is not None:
            print(f"Date range: {start} to {end} ({expected} trading days from dim_session)")
        else:
            print(f"Date range: {start} to {end} (coverage % unavailable - dim_session empty)")
    else:
        df = con.execute(
            """
            SELECT root, contract_series,
                   COUNT(*) as bar_count,
                   MIN(trading_date) as first_date,
                   MAX(trading_date) as last_date
            FROM v_canonical_continuous_bar_daily
            GROUP BY root, contract_series
            ORDER BY root
            """
        ).fetchdf()
        expected = None

    if df.empty:
        print("\nNo data in v_canonical_continuous_bar_daily.")
        return 0

    if expected is not None and expected > 0:
        df["expected"] = expected
        df["coverage_pct"] = (df["bar_count"] / expected * 100).round(1)
        cols = ["root", "contract_series", "bar_count", "expected", "coverage_pct", "first_date", "last_date"]
    else:
        cols = ["root", "contract_series", "bar_count", "first_date", "last_date"]

    print()
    print(df[cols].to_string(index=False))
    print()

    # Canonical roots with no data
    canonical_roots = con.execute(
        "SELECT root FROM dim_canonical_series WHERE optional = false"
    ).fetchdf()["root"].tolist()
    roots_with_data = set(df["root"].tolist())
    missing = [r for r in canonical_roots if r not in roots_with_data]
    if missing:
        print("Canonical roots with NO data:", ", ".join(missing))
    else:
        print("All non-optional canonical roots have data.")

    print()
    print("Query to fetch canonical bars:")
    print("  SELECT * FROM v_canonical_continuous_bar_daily")
    print("  WHERE trading_date BETWEEN '2024-01-01' AND '2024-12-31'")
    print("  ORDER BY root, trading_date;")
    print("=" * 80)

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
