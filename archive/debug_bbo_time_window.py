"""
Debug script to investigate why bbo-1m returns data outside the requested time window.

This script requests ONLY the last 5 minutes of 2025-10-20 and shows:
1. What time range we requested
2. What time range we actually received
3. How many rows vs. expected (should be ~5 minutes * symbols)
"""
from pathlib import Path
from datetime import datetime, date
import sys
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
import databento as db
import pandas as pd

try:
    from zoneinfo import ZoneInfo
    CHI = ZoneInfo("America/Chicago")
    UTC = ZoneInfo("UTC")
except:
    import pytz
    CHI = pytz.timezone("America/Chicago")
    UTC = pytz.UTC

# Load API key
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
api_key = os.getenv("DATABENTO_API_KEY")

if not api_key:
    print("ERROR: No API key found in .env")
    sys.exit(1)

client = db.Historical(key=api_key)

# Request last 5 minutes of 2025-10-20 (3:00 PM CT = 20:00 UTC to 19:55 UTC)
test_date = date(2025, 10, 20)
end_ct = datetime(2025, 10, 20, 15, 0, 0, tzinfo=CHI)  # 3:00 PM CT
start_ct = datetime(2025, 10, 20, 14, 55, 0, tzinfo=CHI)  # 2:55 PM CT
start_utc = start_ct.astimezone(UTC)
end_utc = end_ct.astimezone(UTC)

print(f"=" * 80)
print(f"Test: Requesting last 5 minutes of {test_date}")
print(f"Requested time window (CT): {start_ct} to {end_ct}")
print(f"Requested time window (UTC): {start_utc} to {end_utc}")
print(f"=" * 80)

# Make the request
print("\nFetching data from DataBento API...")
data = client.timeseries.get_range(
    dataset="GLBX.MDP3",
    schema="bbo-1m",
    symbols=["ES.OPT"],
    stype_in="parent",
    start=start_utc,
    end=end_utc,
)

# Analyze what we got
df = data.to_df()
print(f"\n✓ Received {len(df)} rows")

if not df.empty and 'ts_event' in df.columns:
    df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True)
    df['ts_event_ct'] = df['ts_event'].dt.tz_convert(CHI)
    
    actual_start = df['ts_event'].min()
    actual_end = df['ts_event'].max()
    actual_span = actual_end - actual_start
    
    print(f"\nActual time range in data:")
    print(f"  Start (UTC): {actual_start}")
    print(f"  End (UTC):   {actual_end}")
    print(f"  Span:        {actual_span}")
    
    # Check how many rows are actually in our requested window
    in_window = df[(df['ts_event'] >= start_utc) & (df['ts_event'] <= end_utc)]
    outside_window = df[(df['ts_event'] < start_utc) | (df['ts_event'] > end_utc)]
    
    print(f"\nData distribution:")
    print(f"  Rows INSIDE requested 5min window:  {len(in_window):,} ({len(in_window)/len(df)*100:.1f}%)")
    print(f"  Rows OUTSIDE requested window:      {len(outside_window):,} ({len(outside_window)/len(df)*100:.1f}%)")
    
    if len(df) > 0:
        print(f"\nUnique symbols: {df['symbol'].nunique():,}")
        print(f"Unique timestamps: {df['ts_event'].nunique():,}")
        
        # Show time distribution
        print(f"\nTime distribution (sample):")
        time_counts = df['ts_event_ct'].value_counts().sort_index()
        print(f"  First 5 timestamps:")
        for ts, count in list(time_counts.head(5).items()):
            print(f"    {ts}: {count} rows")
        print(f"  ...")
        print(f"  Last 5 timestamps:")
        for ts, count in list(time_counts.tail(5).items()):
            print(f"    {ts}: {count} rows")

print(f"\n" + "=" * 80)
print("CONCLUSION:")
if len(outside_window) > len(in_window):
    print("  ⚠️  WARNING: Most data is OUTSIDE the requested 5-minute window!")
    print("  ⚠️  This means you're being charged for data you didn't request.")
    print("  ⚠️  The bbo-1m schema may not properly respect time range filters.")
else:
    print("  ✓ Data is mostly within the requested window (as expected).")
print(f"=" * 80)

