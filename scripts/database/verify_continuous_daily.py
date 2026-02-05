"""
Verify g_continuous_bar_daily coverage by contract_series and trading_date.

Expected trading days come from dim_session, which is populated from actual
trading data (not assumed schedules). The calendar is DATA-DERIVED.

IMPORTANT: Futures trade ~314 days/year including many Sundays.
Do NOT assume equity-style Mon-Fri trading or CME holiday schedules.

Reports expected vs actual trading days per contract series, a summary table,
and worst offenders (series with most missing days).

Usage:
    python scripts/database/verify_continuous_daily.py --year 2025
    python scripts/database/verify_continuous_daily.py --start 2024-01-01 --end 2024-12-31
    python scripts/database/verify_continuous_daily.py --range 2024-01-01 2024-12-31
    python scripts/database/verify_continuous_daily.py --year 2025 --gap-threshold 5
"""

import sys
import argparse
from pathlib import Path
from datetime import date

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
from src.utils.calendar import get_trading_days_from_dim_session, get_dim_session_count


def parse_date(s: str) -> date:
    """Parse YYYY-MM-DD string to date."""
    return date.fromisoformat(s)


def dim_session_exists(con) -> bool:
    """Return True if dim_session table exists."""
    return (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'dim_session'"
        ).fetchone()[0]
        > 0
    )


def dim_session_is_empty(con) -> bool:
    """Return True if dim_session exists but has no rows."""
    if not dim_session_exists(con):
        return True
    return get_dim_session_count(con) == 0


def verify_coverage(con, start: date, end: date, expected_days: int, gap_threshold: int):
    """Verify g_continuous_bar_daily coverage; return summary df and worst offenders."""

    # Per-series counts in the date range
    series_df = con.execute("""
        SELECT
            contract_series,
            COUNT(DISTINCT trading_date) AS actual_days,
            MIN(trading_date) AS first_date,
            MAX(trading_date) AS last_date
        FROM g_continuous_bar_daily
        WHERE trading_date >= ? AND trading_date <= ?
        GROUP BY contract_series
        ORDER BY contract_series
    """, [start.isoformat(), end.isoformat()]).fetchdf()

    if series_df.empty:
        return None, None, expected_days, 0

    series_df["expected_days"] = expected_days
    series_df["missing_days"] = series_df["expected_days"] - series_df["actual_days"]
    if expected_days > 0:
        series_df["coverage_pct"] = (
            (series_df["actual_days"] / series_df["expected_days"] * 100).round(2)
        )
    else:
        series_df["coverage_pct"] = 0

    # Worst offenders: series with missing_days > gap_threshold, sorted by missing_days desc
    offenders = series_df[series_df["missing_days"] > gap_threshold].copy()
    offenders = offenders.sort_values("missing_days", ascending=False)

    return series_df, offenders, expected_days, len(series_df)


def check_table_exists(con) -> bool:
    """Return True if g_continuous_bar_daily exists."""
    return (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'g_continuous_bar_daily'"
        ).fetchone()[0]
        > 0
    )


def main():
    parser = argparse.ArgumentParser(
        description="Verify g_continuous_bar_daily coverage by contract_series and trading_date"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--year",
        type=int,
        metavar="YEAR",
        help="Verify coverage for this calendar year (all trading days)",
    )
    group.add_argument(
        "--range",
        nargs=2,
        metavar=("START", "END"),
        help="Verify coverage for date range (YYYY-MM-DD YYYY-MM-DD)",
    )
    parser.add_argument(
        "--start",
        type=str,
        metavar="YYYY-MM-DD",
        help="Start date (use with --end; ignored if --year or --range is set)",
    )
    parser.add_argument(
        "--end",
        type=str,
        metavar="YYYY-MM-DD",
        help="End date (use with --start)",
    )
    parser.add_argument(
        "--gap-threshold",
        type=int,
        default=0,
        metavar="N",
        help="Only show worst offenders with more than N missing days (default: 0)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to DuckDB database (default: from env/config)",
    )
    args = parser.parse_args()

    if args.year:
        start = date(args.year, 1, 1)
        end = date(args.year, 12, 31)
    elif args.range:
        start = parse_date(args.range[0])
        end = parse_date(args.range[1])
    else:
        if not args.start or not args.end:
            parser.error("--start and --end are required when not using --year or --range")
        start = parse_date(args.start)
        end = parse_date(args.end)

    if start > end:
        parser.error("Start date must be before or equal to end date")

    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        return 1

    con = connect_duckdb(db_path)
    try:
        if not check_table_exists(con):
            print("ERROR: Table g_continuous_bar_daily does not exist.")
            return 1
        if not dim_session_exists(con):
            print("ERROR: Table dim_session does not exist. Run migrations first.")
            return 1

        # Check if dim_session is empty and warn user
        if dim_session_is_empty(con):
            print("=" * 80)
            print("WARNING: dim_session is empty. Cannot determine expected trading days.")
            print()
            print("The trading calendar must be derived from actual data.")
            print("Run the following command to populate dim_session from g_continuous_bar_daily:")
            print()
            print("    python scripts/database/sync_session_from_data.py")
            print()
            print("Or use --dry-run first to see what would be inserted:")
            print()
            print("    python scripts/database/sync_session_from_data.py --dry-run")
            print("=" * 80)
            return 1

        calendar_days = get_trading_days_from_dim_session(con, start, end)
        expected_days = len(calendar_days)
        if expected_days == 0:
            print(f"WARNING: dim_session has no trade_date rows in [{start}, {end}].")
            print("         Run: python scripts/database/sync_session_from_data.py")
        summary_df, offenders_df, expected_days, num_series = verify_coverage(
            con, start, end, expected_days, args.gap_threshold
        )

        print("=" * 80)
        print("g_continuous_bar_daily COVERAGE VERIFICATION")
        print("=" * 80)
        print(f"Date range:    {start} to {end}")
        print(f"Trading days:  {expected_days} (from dim_session, data-derived calendar)")
        print(f"Gap threshold: {args.gap_threshold} (show offenders with > N missing days)")
        print()

        if summary_df is None:
            print("No rows in g_continuous_bar_daily for this date range.")
            return 0

        # Summary table (all series)
        print("SUMMARY (all contract series)")
        print("-" * 80)
        display = summary_df[
            [
                "contract_series",
                "expected_days",
                "actual_days",
                "missing_days",
                "coverage_pct",
                "first_date",
                "last_date",
            ]
        ]
        print(display.to_string(index=False))
        print()

        # Totals
        total_expected = num_series * expected_days
        total_actual = int(summary_df["actual_days"].sum())
        total_missing = int(summary_df["missing_days"].sum())
        overall_pct = (total_actual / total_expected * 100) if total_expected else 0
        print(f"Total series:     {num_series}")
        print(f"Total bar rows:   {total_actual} (expected {total_expected})")
        print(f"Total missing:    {int(total_missing)}")
        print(f"Overall coverage: {overall_pct:.2f}%")
        print()

        # Worst offenders
        print("WORST OFFENDERS (most missing days)")
        print("-" * 80)
        if offenders_df.empty:
            if args.gap_threshold > 0:
                print(f"No series with more than {args.gap_threshold} missing days.")
            else:
                print("No missing days; coverage is complete for this range.")
        else:
            offender_cols = [
                "contract_series",
                "missing_days",
                "actual_days",
                "expected_days",
                "coverage_pct",
            ]
            print(offenders_df[offender_cols].to_string(index=False))
        print()
        print("=" * 80)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
