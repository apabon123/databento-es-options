# Analysis Scripts

Data quality analysis. See [QUICK_REFERENCE.md](../../docs/QUICK_REFERENCE.md) for full documentation.

## Scripts

| Script | Purpose |
|--------|---------|
| `analyze_data.py` | Analyze downloaded data for quality and completeness |
| `check_sr3_data.py` | Check SR3 contract series coverage in the database |
| `check_ub_zn_data.py` | Check UB and ZN contract series data in the database |
| `compare_sr3_ranks.py` | Compare SR3 rank 0 vs rank 1 data |
| `coverage_canonical.py` | Show coverage for canonical Futures-Six continuous series |

## Usage

```powershell
python scripts/analysis/analyze_data.py
```

Checks: symbol counts, strike coverage, expiry distribution, data quality (spreads, outliers), time coverage.
