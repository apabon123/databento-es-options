"""Verify newly added FRED series."""
import os
import duckdb
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent
os.chdir(PROJECT_ROOT)
load_dotenv()

db_path = os.getenv('DUCKDB_PATH')
con = duckdb.connect(db_path)

# New series that were requested
new_series = {
    'DGS5': '5-Year Treasury Constant Maturity',
    'DGS30': '30-Year Treasury Constant Maturity',
    'T10Y2Y': '10y–2y spread',
    'T10YIE': '10y Breakeven Inflation',
    'T5YIFR': '5-year Inflation Forward',
    'BAMLC0A0CM': 'High yield OAS',
}

# Get all FRED series
result = con.execute("""
    SELECT 
        series_id,
        COUNT(*) as row_count,
        MIN(date) as first_date,
        MAX(date) as last_date
    FROM f_fred_observations 
    GROUP BY series_id 
    ORDER BY series_id
""").fetchdf()

print("=" * 80)
print("FRED SERIES STATUS")
print("=" * 80)
print()

print("NEWLY ADDED SERIES (Phase 2, 4-5):")
print("-" * 80)
for series_id, desc in new_series.items():
    series_data = result[result['series_id'] == series_id]
    if len(series_data) > 0:
        row = series_data.iloc[0]
        print(f"[OK] {series_id:15} {desc:45} {row['row_count']:5} rows  {row['first_date']} to {row['last_date']}")
    else:
        print(f"[FAIL] {series_id:15} {desc:45} NOT FOUND")

print()
print("NOT AVAILABLE VIA FRED API:")
print("-" * 80)
print("[FAIL] VIX9D         9-Day VIX (CBOE) - Not available via FRED API")
print("[FAIL] VVIX          Vol-of-Vol (CBOE) - Not available via FRED API")
print()
print("  NOTE: These may need to be downloaded directly from CBOE or")
print("        may have different series IDs in FRED.")
print()

print("ALL FRED SERIES IN DATABASE:")
print("-" * 80)
for _, row in result.iterrows():
    print(f"  {row['series_id']:15} {row['row_count']:5} rows  {row['first_date']} to {row['last_date']}")

print()
print(f"Total series: {len(result)}")

