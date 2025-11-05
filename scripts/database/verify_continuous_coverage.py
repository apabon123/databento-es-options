"""
Verify complete coverage of continuous futures data for 2025.

Checks:
1. Every trading day in 2025 has data
2. Each day has ~1,300 quotes (full trading day)
3. Identifies missing days or partial days
"""
import sys
from pathlib import Path
from datetime import date, datetime, timedelta

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb

def get_trading_days(year):
    """Get all trading days (Mon-Fri) for a year."""
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    trading_days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Monday = 0, Friday = 4
            trading_days.append(current)
        current += timedelta(days=1)
    return trading_days

def main():
    _, _, db_path = get_paths()
    con = connect_duckdb(db_path)
    
    print("=" * 80)
    print("CONTINUOUS FUTURES DATA COVERAGE VERIFICATION - 2025")
    print("=" * 80)
    print()
    
    # Get all trading days in 2025
    expected_days = get_trading_days(2025)
    print(f"Expected trading days in 2025: {len(expected_days)}")
    print(f"Date range: {min(expected_days)} to {max(expected_days)}")
    print()
    
    # Get dates in database
    db_data = con.execute('''
        SELECT 
            CAST(ts_event AS DATE) as date,
            COUNT(*) as quote_count
        FROM f_continuous_quote_l1
        WHERE CAST(ts_event AS DATE) IS NOT NULL
          AND EXTRACT(YEAR FROM ts_event) = 2025
        GROUP BY CAST(ts_event AS DATE)
        ORDER BY date
    ''').fetchdf()
    
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
        
        for (year, month), days in sorted(by_month.items()):
            print(f"{year}-{month:02d}: {len(days)} missing days")
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
    
    # Expected quote counts
    # Full trading day: ~1,300 quotes (23 hours * 60 minutes = 1,380 possible, but actual is ~1,300)
    # Partial days: < 1,000
    # Test data: < 50
    
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
              AND EXTRACT(YEAR FROM ts_event) = 2025
        ) sub
        GROUP BY EXTRACT(YEAR FROM ts_event), EXTRACT(MONTH FROM ts_event)
        ORDER BY year, month
    ''').fetchdf()
    
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
    
    con.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
