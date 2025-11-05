"""
Test what ES.OPT actually resolves to and compare with other symbol patterns.
"""
from pathlib import Path
from datetime import date
import sys
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
import databento as db

# Load API key
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
api_key = os.getenv("DATABENTO_API_KEY")

if not api_key:
    print("ERROR: No API key found in .env")
    sys.exit(1)

client = db.Historical(key=api_key)

test_date_start = date(2025, 10, 20)
test_date_end = date(2025, 10, 24)

print("=" * 80)
print("Testing different symbol patterns for ES options on GLBX.MDP3")
print(f"Date range: {test_date_start} to {test_date_end}")
print("=" * 80)

# Test different patterns
patterns_to_test = [
    ("ES.OPT", "parent"),
    ("ES.FUT", "parent"),
    ("ES", "parent"),
    # Test without stype_in (let API auto-detect)
    ("ES", None),
]

for symbol, stype in patterns_to_test:
    print(f"\n{'='*80}")
    print(f"Testing: symbols=['{symbol}'], stype_in='{stype}'")
    print(f"{'='*80}")
    
    try:
        # Try to get cost estimate
        from datetime import datetime
        try:
            from zoneinfo import ZoneInfo
            UTC = ZoneInfo("UTC")
        except:
            import pytz
            UTC = pytz.UTC
        
        # Use a single day window to test
        st = datetime.combine(test_date_start, datetime.min.time()).replace(tzinfo=UTC)
        en = datetime.combine(test_date_start, datetime.max.time()).replace(tzinfo=UTC)
        
        kwargs = {"symbols": [symbol], "start": st.isoformat(), "end": en.isoformat()}
        if stype:
            kwargs["stype_in"] = stype
            
        cost_info = client.metadata.get_cost(
            dataset="GLBX.MDP3",
            schema="bbo-1m",
            **kwargs,
        )
        
        cost = float(cost_info) if isinstance(cost_info, (int, float)) else cost_info
        print(f"✓ Cost for 1 day: ${cost:.2f}")
        print(f"  Estimated 5-day cost: ${cost * 5:.2f}")
        
    except Exception as e:
        print(f"✗ Error: {e}")

print(f"\n{'='*80}")
print("COMPARISON:")
print(f"  Your portal test (Oct 27-31, 5 days): $10.33")
print(f"  If our pattern matches, we should see similar costs above.")
print(f"  If costs are much lower, we're missing symbols!")
print(f"{'='*80}")

