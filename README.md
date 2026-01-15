# DataBento Market Data Pipeline

Download, transform, and store CME futures, options, and macro data from DataBento + FRED into a DuckDB warehouse.

## What This Project Does

| Data Source | What It Downloads | Storage |
|-------------|-------------------|---------|
| **DataBento** | ES Options (BBO-1m) | DuckDB |
| **DataBento** | ES Futures (BBO-1m) | DuckDB |
| **DataBento** | Continuous Daily OHLCV (ES, NQ, ZN, CL, etc.) | DuckDB |
| **FRED** | Macro series (VIX, rates, spreads, etc.) | DuckDB |

## Quick Setup

```powershell
# 1. Create virtual environment
python -m venv .venv
./.venv/Scripts/Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure API keys
Copy-Item .env.example .env
# Edit .env:
#   DATABENTO_API_KEY=your_key_here
#   FRED_API_KEY=your_fred_key_here (optional)
```

## Most Common Commands

### Download & Ingest Data

```powershell
# ES Options - last 3 weeks
python scripts/download/download_and_ingest_options.py --weeks 3

# ES Futures - last 3 weeks
python scripts/download/download_and_ingest_futures.py --weeks 3

# Continuous Daily OHLCV - all universe (ES, NQ, ZN, CL, 6E, SR3, etc.)
python scripts/download/download_universe_daily_ohlcv.py --start 2020-01-01 --end 2025-12-31

# FRED Macro Series (VIX, rates, spreads)
python scripts/download/download_fred_series.py
```

### Check What You Have

```powershell
# Database summary
python scripts/download/download_and_ingest_options.py --summary

# Database statistics and duplicate check
python scripts/database/check_database.py

# Verify coverage (check for missing dates)
python scripts/database/check_database.py --verify-coverage --year 2025
```

## Data Architecture

```
DataBento API / FRED API
        ↓
data/raw/           (Bronze - raw downloads)
        ↓
data/silver/market.duckdb  (Silver - normalized tables)
        ↓
data/gold/          (Gold - aggregated outputs)
```

### Key Database Tables

| Table | Description |
|-------|-------------|
| `g_continuous_bar_daily` | Daily OHLCV bars for continuous contracts |
| `g_continuous_bar_1m` | 1-minute bars for continuous contracts |
| `g_bar_1m` | 1-minute bars for ES options |
| `g_fut_bar_1m` | 1-minute bars for ES futures |
| `f_fred_observations` | FRED macro data (VIX, rates, etc.) |

### Query Example

```python
import duckdb
con = duckdb.connect("data/silver/market.duckdb", read_only=True)

# Get ES continuous daily bars
df = con.execute("""
    SELECT trading_date, open, high, low, close, volume
    FROM g_continuous_bar_daily
    WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
    ORDER BY trading_date DESC
    LIMIT 10
""").fetchdf()

con.close()
```

## Cost Estimates (DataBento)

| Data Type | Per Day | Per Week | Per Month |
|-----------|---------|----------|-----------|
| ES Options (BBO-1m, 5min) | ~$0.03 | ~$0.15 | ~$0.60 |
| ES Futures (BBO-1m, 5min) | ~$0.01 | ~$0.05 | ~$0.20 |
| Daily OHLCV (all roots) | ~$0.01 | ~$0.05 | ~$0.20 |

*Exact costs shown before each download - requires confirmation!*

## Documentation

| Document | Purpose |
|----------|---------|
| **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** | Complete command reference, all flags, workflows, troubleshooting |
| **[docs/TECHNICAL_REFERENCE.md](docs/TECHNICAL_REFERENCE.md)** | Database schema, architecture, roll strategies, sharing data |
| **[docs/SOT/DATA_SOURCE_POLICY.md](docs/SOT/DATA_SOURCE_POLICY.md)** | Authoritative data source policy for VIX/VX and volatility data |
| **[docs/SOT/INTEROP_CONTRACT.md](docs/SOT/INTEROP_CONTRACT.md)** | Guaranteed tables and series for downstream systems |

## Requirements

- Python 3.9+
- DataBento API key ([free tier available](https://databento.com))
- FRED API key (optional, for macro data - [get one here](https://fred.stlouisfed.org/docs/api/api_key.html))

## Project Structure

```
databento-es-options/
├── scripts/
│   ├── download/          # Download & ingest scripts
│   ├── database/          # Database management
│   └── analysis/          # Data analysis
├── pipelines/             # ETL pipeline logic
├── src/                   # Core utilities
├── db/migrations/         # SQL schema migrations
├── configs/               # Universe configs (symbols, FRED series)
├── data/
│   ├── raw/              # Bronze layer
│   ├── silver/           # DuckDB database
│   └── gold/             # Aggregated outputs
└── docs/                  # Additional documentation
```
