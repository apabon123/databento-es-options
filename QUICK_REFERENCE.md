# Quick Reference

Complete command reference for the DataBento Market Data Pipeline.

---

## Table of Contents

1. [Download Commands](#download-commands)
2. [Database Commands](#database-commands)
3. [All Flags Reference](#all-flags-reference)
4. [Typical Workflows](#typical-workflows)
5. [Database Queries](#database-queries)
6. [File Locations](#file-locations)
7. [Troubleshooting](#troubleshooting)

---

## Download Commands

### ES Options

```powershell
# Download last N weeks
python scripts/download/download_and_ingest_options.py --weeks 3

# Download specific date range
python scripts/download/download_and_ingest_options.py --start 2025-09-01 --end 2025-09-30

# Skip cost confirmation prompt
python scripts/download/download_and_ingest_options.py --weeks 3 --yes

# Just ingest existing raw data (no download)
python scripts/download/download_and_ingest_options.py --ingest-only

# View database summary
python scripts/download/download_and_ingest_options.py --summary

# Check for un-ingested raw data
python scripts/download/download_and_ingest_options.py --check-missing

# Force re-download even if data exists
python scripts/download/download_and_ingest_options.py --weeks 1 --force
```

### ES Futures

```powershell
# Download last N weeks
python scripts/download/download_and_ingest_futures.py --weeks 3

# Download specific contracts
python scripts/download/download_and_ingest_futures.py --weeks 1 --symbols ESZ5,ESH6

# Download all ES futures (resolves ES.FUT universe)
python scripts/download/download_and_ingest_futures.py --weeks 3 --symbols ES

# Auto-confirm and skip prompt
python scripts/download/download_and_ingest_futures.py --weeks 3 --yes

# View summary
python scripts/download/download_and_ingest_futures.py --summary
```

### ES Continuous Futures (Intraday)

```powershell
# Download full day continuous futures
python scripts/download/download_and_ingest_continuous.py --weeks 1

# Download last 5 minutes only
python scripts/download/download_and_ingest_continuous.py --weeks 1 --last-minutes

# View summary
python scripts/download/download_and_ingest_continuous.py --summary
```

### Continuous Daily OHLCV (Universe)

Downloads daily bars for the configured universe: ES, NQ, RTY, ZT, ZF, ZN, UB, CL, GC, 6E, 6J, 6B, SR3, VX.

```powershell
# Download full universe (all roots)
python scripts/download/download_universe_daily_ohlcv.py --start 2015-01-01 --end 2025-12-31

# Download specific roots only
python scripts/download/download_universe_daily_ohlcv.py --roots ES,NQ,ZN --start 2020-01-01 --end 2024-12-31

# Exclude optional symbols (e.g., VX)
python scripts/download/download_universe_daily_ohlcv.py --exclude-optional --weeks 8

# Download last N weeks
python scripts/download/download_universe_daily_ohlcv.py --weeks 4
```

**Universe roots** (configured in `configs/download_universe.yaml`):
| Category | Roots | Roll Rule |
|----------|-------|-----------|
| Equity | ES, NQ, RTY | Calendar |
| Rates | ZT, ZF, ZN, UB | Volume |
| Commodities | CL, GC | Volume |
| FX | 6E, 6J, 6B | Calendar |
| STIR | SR3 (13 ranks) | Calendar |
| Vol | VX (optional) | Calendar |

### FRED Macro Series

```powershell
# Download all configured series
python scripts/download/download_fred_series.py

# Download and ingest in one step
python scripts/download/download_fred_series.py --ingest

# Download specific series
python scripts/download/download_fred_series.py --series VIXCLS,FEDFUNDS --ingest

# Download spot indices (SPX, NDX)
python scripts/download/download_fred_series.py --series SP500,NASDAQ100 --ingest

# Custom date range
python scripts/download/download_fred_series.py --series VIXCLS --start 2000-01-01 --end 2025-12-31

# Ingest parquet files into database (separate step)
python scripts/database/ingest_fred_series.py
```

**Available FRED series** (configured in `configs/fred_series.yaml`):
- `VIXCLS` - VIX Index
- `VXVCLS` - VXV (3-month VIX)
- `FEDFUNDS` - Fed Funds Rate
- `DGS2`, `DGS5`, `DGS10`, `DGS30` - Treasury rates
- `SOFR`, `DTB3` - Risk-free alternatives
- `T10Y2Y` - 10Y-2Y spread
- `T10YIE`, `T5YIFR` - Inflation expectations
- `BAMLH0A0HYM2`, `BAMLC0A0CM` - Credit spreads
- `TEDRATE` - TED spread
- `CPIAUCSL` - CPI
- `UNRATE` - Unemployment
- `DTWEXBGS` - Dollar index
- `ECBDFR`, `IRSTCI01JPM156N`, `IUDSOIA` - Global rates
- `SP500`, `NASDAQ100` - Spot index levels (price-return)

### RUT Spot Index (Yahoo/MarketWatch)

```powershell
# Sanity check providers (RECOMMENDED before downloading)
python scripts/download/download_index_spot.py --probe

# Download RUT and ingest
python scripts/download/download_index_spot.py --ingest

# Force full backfill from specific date
python scripts/download/download_index_spot.py --backfill --start 1990-01-01 --ingest

# Download only (no ingest)
python scripts/download/download_index_spot.py

# Ingest existing files only (no download)
python scripts/download/download_index_spot.py --ingest-only
```

**Features:**
- Yahoo Finance primary, MarketWatch fallback for RUT spot close
- Uses `Close` price (NOT `Adj Close`) for price-return level
- **If both providers fail, the script hard-fails and does not ingest**
- Hard-fails if data is stale (> 10 days old) or insufficient (< 1000 rows on backfill)
- Append-only: hard-fails if historical values change

**Note:** RUT_SPOT was manually seeded for 2020-2026 (2026-01-21). Going forward: append-only updates via providers. Manual CSV import (`--import-csv`) only for extending history earlier than current min date (rare, never overwrites).
- Validates: no duplicates, no negative values, flags >20% daily moves

### Instrument Definitions

```powershell
# Download definitions for all instruments in database
python scripts/database/download_instrument_definitions.py --all

# Download for specific root
python scripts/database/download_instrument_definitions.py --root SR3

# Force re-download
python scripts/database/download_instrument_definitions.py --all --force

# Show summary
python scripts/database/download_instrument_definitions.py --summary
```

---

## Database Commands

### Check Database

```powershell
# Full check (duplicates + statistics)
python scripts/database/check_database.py

# Statistics only (faster)
python scripts/database/check_database.py --stats-only

# Check specific product
python scripts/database/check_database.py --product ES_CONTINUOUS_MDP3

# Verify coverage (check for missing dates)
python scripts/database/check_database.py --verify-coverage --year 2025
```

### Inspect Futures

```powershell
# Full inspection
python scripts/database/inspect_futures.py

# Inspect specific contract
python scripts/database/inspect_futures.py --contract ESH6

# Export sample data
python scripts/database/inspect_futures.py --export
```

### Manual Pipeline Operations

```powershell
# Run database migrations
python orchestrator.py migrate

# Ingest specific data folder
python orchestrator.py ingest --product ES_OPTIONS_MDP3 --source data/raw/glbx-mdp3-2025-10-30

# Build gold layer
python orchestrator.py build --product ES_OPTIONS_MDP3

# Run validation
python orchestrator.py validate --product ES_OPTIONS_MDP3
```

---

## All Flags Reference

### Download Scripts Common Flags

| Flag | Description |
|------|-------------|
| `--weeks N` | Download last N weeks of data |
| `--start YYYY-MM-DD` | Start date for download |
| `--end YYYY-MM-DD` | End date (use with `--start`) |
| `--yes` | Auto-confirm cost estimate (skip prompt) |
| `--force` | Download even if data exists in DB |
| `--ingest-only` | Process existing raw files (no download) |
| `--summary` | Show database summary and exit |
| `--check-missing` | Find raw data not yet in database |

### Futures-Specific Flags

| Flag | Description |
|------|-------------|
| `--symbols X,Y,Z` | Specific contracts (e.g., ESZ5,ESH6) |
| `--symbols ES` | All ES futures (resolves ES.FUT) |

### Continuous-Specific Flags

| Flag | Description |
|------|-------------|
| `--last-minutes` | Download last 5 minutes only (vs full day) |

### Universe-Specific Flags

| Flag | Description |
|------|-------------|
| `--roots ES,NQ,ZN` | Limit to specific roots |
| `--exclude-optional` | Skip optional symbols (e.g., VX) |

---

## Typical Workflows

### First Time Setup

```powershell
# 1. Download historical continuous daily data
python scripts/download/download_universe_daily_ohlcv.py --start 2015-01-01 --end 2025-12-31

# 2. Download recent options/futures data
python scripts/download/download_and_ingest_options.py --start 2025-01-01 --end 2025-12-31

# 3. Download FRED macro data
python scripts/download/download_fred_series.py

# 4. Download instrument definitions
python scripts/database/download_instrument_definitions.py --all

# 5. Check what you got
python scripts/database/check_database.py
```

### Weekly Update (Routine)

```powershell
# Just run this - it skips duplicates automatically
python scripts/download/download_and_ingest_options.py --weeks 1 --yes
python scripts/download/download_and_ingest_futures.py --weeks 1 --yes
python scripts/download/download_universe_daily_ohlcv.py --weeks 2
python scripts/download/download_fred_series.py --ingest
python scripts/download/download_index_spot.py --ingest
```

### After Vacation (Catch-up)

```powershell
# Download gap (e.g., 2 weeks)
python scripts/download/download_and_ingest_options.py --weeks 2 --yes
python scripts/download/download_universe_daily_ohlcv.py --weeks 3
```

### Fix Ingestion Problem

```powershell
# Re-process existing raw files
python scripts/download/download_and_ingest_options.py --ingest-only
```

### Verify Data Quality

```powershell
# Check for missing dates and duplicates
python scripts/database/check_database.py --verify-coverage --year 2025

# Analyze raw data quality
python scripts/analysis/analyze_data.py
```

---

## Database Queries

### Python

```python
import duckdb
con = duckdb.connect("data/silver/market.duckdb", read_only=True)

# Get dates in database
dates = con.execute("""
    SELECT DISTINCT CAST(ts_event AS DATE) as date
    FROM f_quote_l1
    ORDER BY 1 DESC
    LIMIT 10
""").fetchdf()

# Get continuous daily bars
bars = con.execute("""
    SELECT trading_date, contract_series, open, high, low, close, volume
    FROM g_continuous_bar_daily
    WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
      AND trading_date >= '2025-01-01'
    ORDER BY trading_date
""").fetchdf()

# Get FRED data
vix = con.execute("""
    SELECT date, value
    FROM f_fred_observations
    WHERE series_id = 'VIXCLS'
      AND date >= '2020-01-01'
    ORDER BY date
""").fetchdf()

# Get all contract series
series = con.execute("""
    SELECT contract_series, root, roll_rule, description
    FROM dim_continuous_contract
""").fetchdf()

con.close()
```

### DuckDB CLI

```sql
-- Open database
duckdb data/silver/market.duckdb

-- List all tables
.tables

-- Count quotes per day
SELECT CAST(ts_event AS DATE) AS date, COUNT(*)
FROM f_quote_l1
GROUP BY 1 ORDER BY 1 DESC LIMIT 10;

-- View continuous contract definitions
SELECT * FROM dim_continuous_contract;

-- Check FRED series available
SELECT DISTINCT series_id FROM f_fred_observations ORDER BY 1;

-- Get instrument definitions
SELECT native_symbol, asset, expiration, min_price_increment
FROM dim_instrument_definition
WHERE asset = 'ES'
ORDER BY expiration;
```

---

## File Locations

| Item | Path |
|------|------|
| **Database** | `data/silver/market.duckdb` |
| **Raw Downloads** | `data/raw/` |
| **Gold Outputs** | `data/gold/` |
| **FRED Parquets** | `data/external/fred/` |
| **Index Spot Parquets** | `data/external/index_spot/` |
| **Logs** | `logs/downloader.log` |
| **Config - API Keys** | `.env` |
| **Config - Universe** | `configs/download_universe.yaml` |
| **Config - FRED** | `configs/fred_series.yaml` |
| **DB Migrations** | `db/migrations/` |

---

## Troubleshooting

### Common Issues

| Problem | Solution |
|---------|----------|
| "All dates already in database" | Normal! Use `--force` to re-download |
| "Cost estimate is $0" | No data for that range (weekends/holidays) |
| Can't find API key | Check `.env` file has `DATABENTO_API_KEY=...` |
| Download succeeded but ingest failed | Run `--ingest-only` to retry |
| Want to see what's missing | Run `--check-missing` |
| "No API key found" | Create `.env` from `.env.example` |

### API Key Setup

```powershell
# Copy example env file
Copy-Item .env.example .env

# Edit .env and add your keys:
# DATABENTO_API_KEY=your_key_here
# FRED_API_KEY=your_fred_key_here

# Or set for current session only:
$env:DATABENTO_API_KEY = "your_key_here"
```

### Permission Errors

```powershell
# Ensure directories exist
New-Item -ItemType Directory -Force -Path logs, data\raw, data\silver, data\gold | Out-Null
```

### Jupyter Kernel Issues

```powershell
# Install kernel from your environment
python -m pip install ipykernel
python -m ipykernel install --user --name databento-es-options
```

### Re-run Ingestion Only

```powershell
# If download worked but ingestion failed
python scripts/download/download_and_ingest_options.py --ingest-only
```

---

## Cost Estimates

| Data Type | Per Day | Per Week | Per Month |
|-----------|---------|----------|-----------|
| ES Options (BBO-1m, 5min) | ~$0.02-0.03 | ~$0.10-0.15 | ~$0.40-0.60 |
| ES Futures (BBO-1m, 5min) | ~$0.01 | ~$0.05 | ~$0.20 |
| Continuous Daily OHLCV | ~$0.01 | ~$0.05 | ~$0.20 |

*Exact costs shown before each download - requires confirmation (bypass with `--yes`).*
