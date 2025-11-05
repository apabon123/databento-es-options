# Analysis Scripts

Scripts for analyzing downloaded data.

## Scripts

### `analyze_data.py`
Analyze downloaded ES options data for quality and completeness.

Checks:
- Symbol counts per day
- Unique symbols per minute snapshot
- Strike coverage (how many strikes per expiry)
- Expiry/maturity distribution
- Data quality (bid/ask spreads, missing prices, outliers)
- Time coverage (do we have all expected minutes?)

**Usage:**
```powershell
python scripts/analysis/analyze_data.py
```

This script analyzes all DBN and parquet files in the `data/raw/` directory (or `DATA_BRONZE_ROOT` if configured).

