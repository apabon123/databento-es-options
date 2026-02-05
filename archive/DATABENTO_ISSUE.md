# DataBento API Issue: bbo-1m Schema Time Filtering

## Summary
The `bbo-1m` schema is returning significantly more data than the requested time window, but billing appears to match the requested window (not the data returned).

## Issue Details

### Request Parameters
- **Dataset**: `GLBX.MDP3`
- **Schema**: `bbo-1m`
- **Symbols**: `["ES.OPT"]`
- **Stype**: `parent`
- **Date**: 2025-10-20
- **Requested Time Window**: 19:55:00 to 20:00:00 UTC (5 minutes)

### Expected Behavior
- Receive ~5 minutes worth of 1-minute BBO snapshots
- Estimate: ~63 rows (based on ~2,172 active ES option symbols with 5 snapshots each)
- Cost estimate: $0.027 for 5-minute window

### Actual Behavior
- **Received**: 10,094 rows spanning 21 hours 54 minutes
- **Time range in data**: 2025-10-19 22:04:37 UTC to 2025-10-20 19:58:57 UTC
- **Rows within requested 5-min window**: 63 (0.6% of total)
- **Rows outside requested window**: 10,031 (99.4% of total)
- **Actual cost charged**: $0.04 (matches estimate for 5-minute window)

### Code to Reproduce

```python
import databento as db
from datetime import datetime
from zoneinfo import ZoneInfo

client = db.Historical(key="YOUR_KEY")

# Request last 5 minutes of trading day
start = datetime(2025, 10, 20, 19, 55, 0, tzinfo=ZoneInfo("UTC"))
end = datetime(2025, 10, 20, 20, 0, 0, tzinfo=ZoneInfo("UTC"))

# Get cost estimate
cost = client.metadata.get_cost(
    dataset="GLBX.MDP3",
    schema="bbo-1m",
    symbols=["ES.OPT"],
    stype_in="parent",
    start=start.isoformat(),
    end=end.isoformat(),
)
print(f"Estimated cost: ${cost:.4f}")  # Shows $0.027

# Download data
data = client.timeseries.get_range(
    dataset="GLBX.MDP3",
    schema="bbo-1m",
    symbols=["ES.OPT"],
    stype_in="parent",
    start=start,
    end=end,
)

df = data.to_df()
print(f"Rows received: {len(df)}")  # Shows 10,094
print(f"Time span: {df['ts_event'].min()} to {df['ts_event'].max()}")
# Shows 2025-10-19 22:04:37 to 2025-10-20 19:58:57 (22 hours)

# Count rows actually in requested window
in_window = df[(df['ts_event'] >= start) & (df['ts_event'] <= end)]
print(f"Rows in requested window: {len(in_window)}")  # Shows 63 (0.6%)
```

## Questions for DataBento Support

1. **Is this expected behavior for `bbo-1m`?**
   - Does `bbo-1m` snapshot schema ignore time range filters?
   - Is the billing correct (charging for requested window despite returning more data)?

2. **What's the correct way to get ONLY the last 5 minutes of data?**
   - Should we use a different schema (e.g., `mbp-1`, `trades`)?
   - Should we filter locally after download?

3. **Billing clarification:**
   - Are we being charged $0.027 for the 5-minute request but receiving 22 hours of data?
   - Could this billing discrepancy be corrected later and result in additional charges?
   - Should we expect the actual charge to match the data volume returned ($2.57/day)?

## Concern

We want to download the last 5 minutes of each trading day for backtesting. At current billing ($0.03/day), this is affordable. However, if DataBento corrects this and charges for all data returned ($2.57/day), the cost would be 95x higher and make the project unviable.

**We need clarification before proceeding with larger downloads to avoid unexpected charges.**

## Environment
- Python databento package version: [run `pip show databento` to get version]
- Python version: 3.x
- OS: Windows

## Contact Info
[Your email or DataBento account info]

