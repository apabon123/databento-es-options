# Wrapper Script Guide

## Overview

The wrapper scripts provide a complete end-to-end pipeline for downloading and storing ES options/futures data. No manual steps required!

## ES Options Wrapper

### Basic Usage

**Download last week:**
```powershell
python scripts/download/download_and_ingest_options.py --weeks 1

# Skip the interactive cost prompt
python scripts/download/download_and_ingest_options.py --weeks 1 --yes
```

**Download last 3 weeks:**
```powershell
python scripts/download/download_and_ingest_options.py --weeks 3
```

**Download specific date range:**
```powershell
python scripts/download/download_and_ingest_options.py --start 2025-09-01 --end 2025-09-30
```

### Maintenance Operations

**Check database summary:**
```powershell
python scripts/download/download_and_ingest_options.py --summary
```

**Check for un-ingested data:**
```powershell
python scripts/download/download_and_ingest_options.py --check-missing
```

**Ingest existing raw files (no download):**
```powershell
python scripts/download/download_and_ingest_options.py --ingest-only
```

**Force re-download (even if data exists):**
```powershell
python scripts/download/download_and_ingest_options.py --weeks 1 --force
```

## ES Futures Wrapper

### Basic Usage

**Download last week (default ES root):**
```powershell
python scripts/download/download_and_ingest_futures.py --weeks 1

# Auto-confirm cost estimate
python scripts/download/download_and_ingest_futures.py --weeks 1 --yes
```

**Download specific contracts:**
```powershell
python scripts/download/download_and_ingest_futures.py --weeks 1 --symbols ESZ5,ESH6
```

**Download all ES futures:**
```powershell
python scripts/download/download_and_ingest_futures.py --weeks 3 --symbols ES
```

### Maintenance Operations

Same flags as options wrapper:
- `--summary` - View database summary
- `--ingest-only` - Process existing raw data
- `--force` - Force re-download
- `--yes` - Auto-confirm download cost

## What Happens Behind the Scenes

### Step 1: Duplicate Detection
- Queries database to find existing dates
- Filters out dates already in DB
- Shows you exactly what will be downloaded

### Step 2: Cost Estimation
```
Estimating cost for 5 trading days...
      Date  Size (MB)  Cost (USD)
2025-10-20        0.0         0.0
...
Estimated total size: 0.000 MB
Estimated total cost: $0.00 USD

Proceed to download 5 days for $0.00? [y/N]
```
Add `--yes` to bypass the prompt when you are comfortable with the quoted cost.

### Step 3: Download
- Downloads BBO-1m data from DataBento API
- Saves as compressed DBN files in `data/raw/`
- Validates each file after download

### Step 4: Transform
```
Transforming glbx-mdp3-2025-10-20.bbo-1m.last5m.parquet for ES_FUTURES_MDP3...
Processing 15 rows, 3 unique symbols
Kept 15 rows with valid ES_FUTURES_MDP3 symbols
Wrote 3 instruments to fut_instruments/2025-10-20.parquet
Wrote 15 quotes to fut_quotes_l1/2025-10-20.parquet
Transformation complete: data/raw/glbx-mdp3-2025-10-20
```

Creates folder structure:
```
data/raw/glbx-mdp3-2025-10-20/
  ├── instruments/2025-10-20.parquet        # options runs
  ├── quotes_l1/2025-10-20.parquet          # options runs
  ├── fut_instruments/2025-10-20.parquet    # futures runs
  ├── fut_quotes_l1/2025-10-20.parquet      # futures runs
  └── trades/ or fut_trades/ (created if trade data exists; skipped otherwise)
```

### Step 5: Ingest
```
Ingesting glbx-mdp3-2025-10-20...
  glbx-mdp3-2025-10-20 ingested
```

Loads data into DuckDB tables:
- `dim_instrument` - Instrument definitions
- `f_quote_l1` - Quote data

### Step 6: Build Gold Layer
```
Building 1-minute bars...
Gold layer built
```

Creates aggregated `g_bar_1m` table with:
- Open/High/Low/Close mid prices
- Open/Close spreads
- Trade volumes and notional

### Step 7: Validate
```
[fut_instruments] -> 3
[fut_quotes] -> 75
[fut_bars] -> 44
Validation complete
```

### Step 8: Summary
```
DATABASE SUMMARY
Product: ES_OPTIONS_MDP3
Total quotes: 151,410
Unique instruments: 2,147
Date range: 2025-09-29 to 2025-10-20
Trading days: 15
Instrument definitions: 2,147
1-minute bars: 148,223
```

## Common Workflows

### Initial Setup (First Time)
```powershell
# 1. Download last 3 months of data
python scripts/download/download_and_ingest_options.py --start 2025-08-01 --end 2025-10-31

# 2. Check what you got
python scripts/download/download_and_ingest_options.py --summary
```

### Daily Update
```powershell
# Download yesterday's data (skips duplicates automatically)
python scripts/download/download_and_ingest_options.py --weeks 1 --yes
```

### After Vacation (Catch-up)
```powershell
# Download gap (e.g., 2 weeks ago to yesterday)
python scripts/download/download_and_ingest_options.py --weeks 2 --yes
```

### Fix Ingestion Issues
```powershell
# If download succeeded but ingestion failed, just re-ingest
python scripts/download/download_and_ingest_options.py --ingest-only
```

## Troubleshooting

### "All requested dates already in database"
This is normal! The wrapper detected you already have that data. Use `--force` to re-download anyway.

### "Cost estimate is $0"
No data available for that date range (likely weekends/holidays or future dates).

### "Failed to ingest"
Check logs in `logs/` directory. Common issues:
- Column name mismatches
- Corrupt DBN files
- Database locked by another process

### Verify Database Contents
```powershell
# Quick summary
python scripts/download/download_and_ingest_options.py --summary

# Or query directly with DuckDB CLI
duckdb data/silver/market.duckdb
> SELECT COUNT(*) FROM f_quote_l1;
> SELECT DISTINCT DATE(ts_event) FROM f_quote_l1 ORDER BY 1;
```

## Advanced: Querying the Database

### Python
```python
import duckdb

con = duckdb.connect("data/silver/market.duckdb")

# Get all dates
dates = con.execute("SELECT DISTINCT DATE(ts_event) FROM f_quote_l1 ORDER BY 1").fetchdf()
print(dates)

# Get summary for a specific date
summary = con.execute("""
    SELECT 
        DATE(ts_event) as date,
        COUNT(*) as quotes,
        COUNT(DISTINCT instrument_id) as instruments,
        AVG(ask_px - bid_px) as avg_spread
    FROM f_quote_l1
    WHERE DATE(ts_event) = '2025-10-20'
    GROUP BY 1
""").fetchdf()
print(summary)

con.close()
```

### DuckDB CLI
```sql
-- Open database
duckdb data/silver/market.duckdb

-- View tables
.tables

-- Count quotes per day
SELECT 
    DATE(ts_event) as date,
    COUNT(*) as quotes,
    COUNT(DISTINCT instrument_id) as instruments
FROM f_quote_l1
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;

-- View instruments
SELECT * FROM dim_instrument LIMIT 10;

-- View gold bars
SELECT * FROM g_bar_1m 
WHERE ts_minute >= '2025-10-20 19:50:00'
ORDER BY ts_minute DESC, instrument_id
LIMIT 20;
```

## Cost Estimation

Typical costs for ES Options (BBO-1m, last 5 minutes per day):
- **Per day**: ~$0.02-0.03
- **Per week** (5 days): ~$0.10-0.15
- **Per month** (20 days): ~$0.40-0.60

ES Futures are typically cheaper (fewer symbols):
- **Per day**: ~$0.01
- **Per week**: ~$0.05
- **Per month**: ~$0.20

The wrapper always shows exact costs before downloading!

