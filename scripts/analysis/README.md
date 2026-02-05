# Analysis Scripts

Data quality analysis. See [QUICK_REFERENCE.md](../../QUICK_REFERENCE.md) for full documentation.

## Scripts

| Script | Purpose |
|--------|---------|
| `analyze_data.py` | Analyze downloaded data for quality and completeness |

## Usage

```powershell
python scripts/analysis/analyze_data.py
```

Checks: symbol counts, strike coverage, expiry distribution, data quality (spreads, outliers), time coverage.
