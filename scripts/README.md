# Scripts

User-facing scripts organized by function. See [QUICK_REFERENCE.md](../QUICK_REFERENCE.md) for complete command documentation.

## Directories

| Directory | Purpose |
|-----------|---------|
| `download/` | Download & ingest data (options, futures, continuous, FRED) |
| `database/` | Database management (check, inspect, definitions) |
| `analysis/` | Data quality analysis |
| `utils/` | Utilities (folder organization) |

## Quick Examples

```powershell
# Download data
python scripts/download/download_and_ingest_options.py --weeks 3
python scripts/download/download_universe_daily_ohlcv.py --start 2020-01-01

# Check database
python scripts/database/check_database.py

# Analyze data quality
python scripts/analysis/analyze_data.py
```
