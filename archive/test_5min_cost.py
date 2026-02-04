"""
Test the actual cost for a 5-minute window vs full day.
"""
from pathlib import Path
from datetime import datetime, date
import sys
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
import databento as db

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

test_date = date(2025, 10, 20)

print("=" * 80)
print("Comparing costs: 5-minute window vs full day")
print(f"Date: {test_date}")
print("=" * 80)

# Test 1: 5-minute window (2:55 PM - 3:00 PM CT)
end_ct = datetime(2025, 10, 20, 15, 0, 0, tzinfo=CHI)
start_ct = datetime(2025, 10, 20, 14, 55, 0, tzinfo=CHI)
start_5m = start_ct.astimezone(UTC)
end_5m = end_ct.astimezone(UTC)

print(f"\n1. 5-minute window:")
print(f"   {start_ct} to {end_ct} (CT)")
try:
    cost_5m = client.metadata.get_cost(
        dataset="GLBX.MDP3",
        schema="bbo-1m",
        symbols=["ES.OPT"],
        stype_in="parent",
        start=start_5m.isoformat(),
        end=end_5m.isoformat(),
    )
    cost_5m = float(cost_5m) if isinstance(cost_5m, (int, float)) else cost_5m
    print(f"   Cost: ${cost_5m:.4f}")
except Exception as e:
    print(f"   Error: {e}")

# Test 2: Full day
start_day = datetime.combine(test_date, datetime.min.time()).replace(tzinfo=UTC)
end_day = datetime.combine(test_date, datetime.max.time()).replace(tzinfo=UTC)

print(f"\n2. Full day:")
print(f"   {start_day} to {end_day} (UTC)")
try:
    cost_day = client.metadata.get_cost(
        dataset="GLBX.MDP3",
        schema="bbo-1m",
        symbols=["ES.OPT"],
        stype_in="parent",
        start=start_day.isoformat(),
        end=end_day.isoformat(),
    )
    cost_day = float(cost_day) if isinstance(cost_day, (int, float)) else cost_day
    print(f"   Cost: ${cost_day:.4f}")
except Exception as e:
    print(f"   Error: {e}")

print(f"\n" + "=" * 80)
print("ANALYSIS:")
if cost_5m and cost_day:
    if abs(cost_5m - cost_day) < 0.01:
        print(f"  ⚠️  SAME COST: 5-min (${cost_5m:.4f}) ≈ Full day (${cost_day:.4f})")
        print(f"  ⚠️  Databento charges the full day rate regardless of time window!")
        print(f"  ⚠️  You can't save money by requesting smaller windows.")
    else:
        print(f"  ✓ Different costs: 5-min=${cost_5m:.4f}, Full day=${cost_day:.4f}")
        print(f"  ✓ Requesting smaller windows saves money.")
print(f"=" * 80)

