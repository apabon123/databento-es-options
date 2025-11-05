# Quick Reference Card

## Most Common Commands

### ES Options

```powershell
# Download last 3 weeks (recommended starting point)
python scripts/download/download_and_ingest_options.py --weeks 3

# Skip cost prompt
python scripts/download/download_and_ingest_options.py --weeks 3 --yes

# View what's in the database
python scripts/download/download_and_ingest_options.py --summary

# Check for un-ingested raw data
python scripts/download/download_and_ingest_options.py --check-missing
```

### ES Futures

```powershell
# Download last 3 weeks
python scripts/download/download_and_ingest_futures.py --weeks 3

# Auto-confirm download and ingest all ES outrights resolved from ES.FUT
python scripts/download/download_and_ingest_futures.py --weeks 3 --yes

# View summary
python scripts/download/download_and_ingest_futures.py --summary
```

## What Each Flag Does

| Flag | Purpose |
|------|---------|
| `--weeks N` | Download last N weeks of data |
| `--start YYYY-MM-DD` | Start date for download |
| `--end YYYY-MM-DD` | End date (use with `--start`) |
| `--summary` | Show database summary and exit |
| `--check-missing` | Find raw data not yet in database |
| `--ingest-only` | Process existing raw files (no download) |
| `--force` | Download even if data exists in DB |
| `--yes` | Auto-confirm cost estimate (skip prompt) |
| `--symbols X,Y,Z` | (Futures only) Specific contracts |

## Typical Workflows

### First Time Setup
```powershell
# 1. Download 3 months of data
python scripts/download/download_and_ingest_options.py --start 2025-08-01 --end 2025-10-31

# 2. Check what you got
python scripts/download/download_and_ingest_options.py --summary
```

### Weekly Update
```powershell
# Just run this - it skips duplicates automatically
python scripts/download/download_and_ingest_options.py --weeks 1
```

### After Vacation (Catch-up)
```powershell
# Download gap (e.g., 2 weeks ago to yesterday)
python scripts/download/download_and_ingest_options.py --weeks 2 --yes
```

### Fix Ingestion Problem
```powershell
# Re-process existing raw files
python scripts/download/download_and_ingest_options.py --ingest-only
```

## Database Management

### Check Database
```powershell
# Check all products for duplicates and show statistics
python scripts/database/check_database.py

# Show statistics only
python scripts/database/check_database.py --stats-only

# Verify continuous futures coverage
python scripts/database/check_database.py --verify-coverage --year 2025

# Inspect futures data
python scripts/database/inspect_futures.py
```

### Database Queries

### Python
```python
import duckdb
con = duckdb.connect("data/silver/market.duckdb")

# Get dates in database
dates = con.execute("""
    SELECT DISTINCT CAST(ts_event AS DATE)
    FROM f_quote_l1
    ORDER BY 1 DESC
""").fetchdf()
print(dates)

con.close()
```

### DuckDB CLI
```sql
-- Open database
duckdb data/silver/market.duckdb

-- Count quotes per day
SELECT CAST(ts_event AS DATE) AS date, COUNT(*)
FROM f_quote_l1
GROUP BY 1 ORDER BY 1 DESC LIMIT 10;

-- View instruments
SELECT * FROM dim_instrument LIMIT 10;
```

## File Locations

| Item | Path |
|------|------|
| Database | `data/silver/market.duckdb` |
| Raw Downloads | `data/raw/*.dbn.zst` |
| Transformed Data | `data/raw/glbx-mdp3-YYYY-MM-DD/` |
| Logs | `logs/downloader.log` |
| Config | `.env` (API key), `config/schema_registry.yml` |

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "All dates already in database" | Normal! Use `--force` to re-download |
| "Cost estimate is $0" | No data for that range (weekends/holidays) |
| Can't find API key | Check `.env` file has `DATABENTO_API_KEY=...` |
| Download succeeded but ingest failed | Run `--ingest-only` to retry |
| Want to see what's missing | Run `--check-missing` |

## Help

```powershell
# Inline help
python scripts/download/download_and_ingest_options.py --help

# Detailed guide
# See: docs/WRAPPER_GUIDE.md

# Implementation details
# See: WRAPPER_SCRIPTS_SUMMARY.md
```

## Cost Estimates

| Timeframe | ES Options | ES Futures |
|-----------|------------|------------|
| 1 day | ~$0.02-0.03 | ~$0.01 |
| 1 week (5 days) | ~$0.10-0.15 | ~$0.05 |
| 1 month (20 days) | ~$0.40-0.60 | ~$0.20 |

*Actual costs shown before each download - requires confirmation!*

