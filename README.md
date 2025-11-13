# DataBento ES Options & Futures Data Pipeline

Download, transform, and store CME futures, options, and continuous daily OHLCV data from DataBento into a DuckDB warehouse.

## Quick Start

### 1. Setup
```powershell
# Create virtual environment
python -m venv .venv
./.venv/Scripts/Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Configure API key and data paths
Copy-Item .env.example .env
# Edit .env and configure:
# - DATABENTO_API_KEY=your_key_here
# - DATA_BRONZE_ROOT (raw data folder - default: ./data/raw)
# - DATA_GOLD_ROOT (processed data folder - default: ./data/gold)
# - DUCKDB_PATH (database file - default: ./data/silver/market.duckdb)
```

**Data Storage Paths** (configured in `.env`):
- **Bronze** (raw): `DATA_BRONZE_ROOT` - Downloaded parquet files and transformed folders
- **Silver** (database): `DUCKDB_PATH` - DuckDB database file
- **Gold** (aggregated): `DATA_GOLD_ROOT` - Final 1-minute bar outputs

### 2. Download & Ingest Data (NEW - Recommended)

**Config-Driven Continuous Daily Universe:**
```powershell
# Download the configured universe (see configs/download_universe.yaml)
python scripts/download/download_universe_daily_ohlcv.py --start 2015-01-01 --end 2025-12-31

# Limit to specific roots
python scripts/download/download_universe_daily_ohlcv.py --roots ES,NQ,ZN --start 2020-01-01 --end 2020-12-31

# Skip optional symbols (e.g., VX)
python scripts/download/download_universe_daily_ohlcv.py --exclude-optional --weeks 8
```

**FRED Macro Series (external data):**
```powershell
# Fetch the default macro set defined in configs/fred_series.yaml
python scripts/download/download_fred_series.py

# Override the series list and date window
python scripts/download/download_fred_series.py --series VIXCLS,FEDFUNDS --start 2000-01-01 --end 2025-12-31
```

**ES Options - All-in-One Wrapper:**
```powershell
# Download last 3 weeks + ingest into database
python scripts/download/download_and_ingest_options.py --weeks 3

# Skip the cost confirmation prompt
python scripts/download/download_and_ingest_options.py --weeks 3 --yes

# Download specific date range
python scripts/download/download_and_ingest_options.py --start 2025-09-01 --end 2025-09-30

# Just ingest existing raw data (no download)
python scripts/download/download_and_ingest_options.py --ingest-only

# Check for missing ingestions
python scripts/download/download_and_ingest_options.py --check-missing

# View database summary
python scripts/download/download_and_ingest_options.py --summary

# Inspect futures data in detail
python scripts/database/inspect_futures.py
python scripts/database/inspect_futures.py --contract ESH6
python scripts/database/inspect_futures.py --export
```

**ES Futures - All-in-One Wrapper:**
```powershell
# Download last 3 weeks + ingest into database
python scripts/download/download_and_ingest_futures.py --weeks 3

# Auto-confirm cost estimate
python scripts/download/download_and_ingest_futures.py --weeks 3 --yes

# Download specific contracts
python scripts/download/download_and_ingest_futures.py --weeks 1 --symbols ESZ5,ESH6

# Download all ES futures (automatically resolves ES -> ES.FUT universe)
python scripts/download/download_and_ingest_futures.py --weeks 3 --symbols ES
```

**ES Continuous Futures - All-in-One Wrapper:**
```powershell
# Download full day continuous futures (default)
python scripts/download/download_and_ingest_continuous.py --weeks 1

# Download last 5 minutes only
python scripts/download/download_and_ingest_continuous.py --weeks 1 --last-minutes

# View database summary
python scripts/download/download_and_ingest_continuous.py --summary
```

### 3. What the Wrappers Do

The wrapper scripts (`download_and_ingest_options.py`, `download_and_ingest_futures.py`, and `download_and_ingest_continuous.py`) handle everything:

1. ✅ **Duplicate Detection** - Checks database for existing dates, only downloads new data
2. 💰 **Cost Estimation** - Shows exact cost before downloading, requires confirmation
3. ⬇️ **Download** - Gets BBO-1m data from DataBento API
4. 🔄 **Transform** - Converts DBN files to proper folder structure
5. 📊 **Ingest** - Loads data into DuckDB database (skips missing glob patterns, handles incremental updates)
6. 🏗️ **Build Gold** - Creates 1-minute aggregated bars
7. ✓ **Validate** - Runs sanity checks
8. 📈 **Summary** - Shows what's in the database

**No more manual steps!** Just run one command and everything happens automatically.

## Project Structure

```
databento-es-options/
├── src/                                    # Core source code
│   ├── download/                           # Data download logic
│   │   └── bbo_downloader.py               # Main downloader
│   ├── validation/                         # Data validation
│   │   ├── integrity_checks.py             # Quality checks
│   │   └── data_analyzer.py                # Analysis tool
│   └── utils/                              # Utilities
│       ├── logging_config.py               # Logging setup
│       ├── db_utils.py                     # Database utilities (NEW)
│       ├── data_transform.py               # Data transformation (NEW)
│       └── continuous_transform.py         # Continuous contract helpers
├── scripts/                                # User-facing scripts
│   ├── download_and_ingest_options.py      # All-in-one options wrapper (NEW)
│   ├── download_and_ingest_futures.py      # All-in-one futures wrapper (NEW)
│   ├── inspect_futures.py                  # Inspect futures data in DB (NEW)
│   ├── download_last_week.py               # Download only (legacy, use download_and_ingest_*.py instead)
│   ├── download_universe_daily_ohlcv.py    # Config-driven OHLCV downloader (NEW)
│   ├── download_fred_series.py             # FRED macro downloader (NEW)
│   └── analyze_data.py                     # Validation runner
├── configs/                                # High-level universe/config metadata (NEW)
│   ├── download_universe.yaml              # Roots, roll rules, and rank ranges
│   ├── fred_series.yaml                    # FRED macro series manifest (NEW)
│   ├── fred_settings.yaml                  # FRED API configuration (NEW)
│   ├── rates_dv01.yaml                     # Duration proxies for curve hedging
│   ├── contracts.yaml                      # Additional symbol aliases
│   └── universe.yaml                       # Market data discovery configuration
├── pipelines/                              # Database pipeline
│   ├── products/                           # Product-specific loaders
│   │   ├── es_options_mdp3.py              # ES options loader
│   │   └── es_futures_mdp3.py              # ES futures loader
│   ├── common.py                           # Common utilities
│   ├── loader.py                           # Generic loader
│   ├── registry.py                         # Product registry
│   └── validators.py                       # Validation logic
├── db/                                     # Database schema
│   └── migrations/                         # SQL migrations
│       ├── 0001_core.sql                   # Core tables
│       ├── 1000_es_options_mdp3.sql        # Options tables
│       └── 1001_es_futures_mdp3.sql        # Futures tables
├── config/                                 # Configuration
│   └── schema_registry.yml                 # Product definitions
├── data/                                   # Data storage (git-ignored)
│   ├── raw/                                # Downloaded DBN files
│   ├── silver/                             # DuckDB database
│   └── gold/                               # Final outputs
├── orchestrator.py                         # Pipeline orchestrator
├── docs/                                   # Documentation
├── logs/                                   # Log files
└── notebooks/                              # Jupyter notebooks
```

## What It Does

- **Downloads**: Last 5 minutes of BBO-1m snapshots for all ES options (~2,000+ symbols) and ES futures outrights resolved from `ES.FUT`
- **Cost**: ~$0.03/day, ~$0.15/week
- **Validates**: Checks data quality, symbol coverage, strike/expiry distribution
- **Outputs**: Parquet files in `data/raw/` with full metadata

## Key Features

✅ **Smart filtering** - bbo-1m filters on `ts_recv` (snapshot time), not `ts_event` (trade time)  
✅ **Cost estimates** - Shows exact costs before downloading  
✅ **Quality checks** - Validates symbols, strikes, maturities, and price data  
✅ **Clean data** - Removes bad prices, checks spreads, identifies outliers  

## Data Architecture

The project uses a **Bronze-Silver-Gold** data architecture:

```
data/
├── raw/          (Bronze Layer) - Raw files from DataBento
├── silver/       (Silver Layer) - DuckDB database
└── gold/         (Gold Layer) - Transformed, organized Parquet files
```

### Bronze Layer (`data/raw/`)

**Direct downloads from DataBento:**
- `glbx-mdp3-YYYY-MM-DD.bbo-1m.fullday.parquet` - Full trading day data
- `glbx-mdp3-YYYY-MM-DD.bbo-1m.last5m.parquet` - Last 5 minutes of trading day

**Transformed folder structure** (after transformation):
```
data/raw/glbx-mdp3-2025-10-27/
├── continuous_instruments/
│   └── 2025-10-27.parquet      # Contract definitions
├── continuous_quotes_l1/
│   └── 2025-10-27.parquet      # Level 1 quotes (bid/ask)
└── continuous_trades/           # Trade data (if available)
```

**For ES Futures:**
```
data/raw/glbx-mdp3-2025-10-20/
├── fut_instruments/
│   └── 2025-10-20.parquet
├── fut_quotes_l1/
│   └── 2025-10-20.parquet
└── fut_trades/
```

**For ES Options:**
```
data/raw/glbx-mdp3-YYYY-MM-DD/
├── instruments/
│   └── YYYY-MM-DD.parquet
├── quotes_l1/
│   └── YYYY-MM-DD.parquet
└── trades/
```

### Silver Layer (`data/silver/`)

- `market.duckdb` - The main DuckDB database file
- Stores all ingested market data in normalized tables
- Contains fact tables (quotes, trades) and dimension tables (instruments)
- Stores aggregated "gold layer" tables (1-minute bars)

### Gold Layer (`data/gold/`)

- Mirror of `data/raw/` structure with same folder organization
- Ready-to-query Parquet files organized by date and product type
- Can be queried directly with DuckDB without going through the database

### Data Flow

```
DataBento API
    ↓
data/raw/glbx-mdp3-YYYY-MM-DD.bbo-1m.fullday.parquet
    ↓ (transform)
data/raw/glbx-mdp3-YYYY-MM-DD/
    ├── instruments/YYYY-MM-DD.parquet
    ├── quotes_l1/YYYY-MM-DD.parquet
    └── trades/
    ↓ (copy to gold)
data/gold/glbx-mdp3-YYYY-MM-DD/
    ↓ (ingest)
data/silver/market.duckdb
    ├── f_quote_l1 (fact table)
    ├── dim_instrument (dimension table)
    └── g_bar_1m (gold layer - aggregated bars)
```

## Data Schema

Each parquet file contains:
- **Symbol**: ES option symbol (e.g., "ESZ5 C6000") or futures native symbol (e.g., "ESZ5")
- **Timestamps**: `ts_event` (last trade), `ts_recv` (snapshot time)
- **Prices**: `bid_px_00`, `ask_px_00`, `bid_sz_00`, `ask_sz_00`
- **Metadata**: Full BBO-1m schema fields

## Advanced Usage

### Custom Date Range
```python
from src.download.bbo_downloader import download_bbo_last_window
from datetime import date
import databento as db

client = db.Historical(key="YOUR_KEY")
download_bbo_last_window(
    client, 
    symbols=["ES.OPT"],
    start_d=date(2025, 10, 1),
    end_d=date(2025, 10, 31),
    minutes=5,
    stype_in="parent"
)
```

### Analyze Specific Files
```python
from src.validation.data_analyzer import analyze_file
from pathlib import Path

result = analyze_file(Path("data/raw/glbx-mdp3-2025-10-20.bbo-1m.last5m.parquet"))
print(result)
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

### Cost Estimates

Typical costs for ES Options (BBO-1m, last 5 minutes per day):
- **Per day**: ~$0.02-0.03
- **Per week** (5 days): ~$0.10-0.15
- **Per month** (20 days): ~$0.40-0.60

ES Futures are typically cheaper (fewer symbols):
- **Per day**: ~$0.01
- **Per week**: ~$0.05
- **Per month**: ~$0.20

The wrapper always shows exact costs before downloading!

## Documentation

- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Quick command reference
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) - Common issues and solutions
- [docs/ROLL_STRATEGY_GUIDE.md](docs/ROLL_STRATEGY_GUIDE.md) - Roll strategy implementation guide
- [docs/INSTRUMENT_DEFINITIONS.md](docs/INSTRUMENT_DEFINITIONS.md) - Instrument definitions documentation

## Requirements

- Python 3.9+
- DataBento API key (free tier available)
- ~100MB disk space per week of data

## License

[Your License]

## Support

For issues with:
- This code: Open a GitHub issue
- DataBento API: https://databento.com/support

## Advanced Usage - Manual Workflow

If you need fine-grained control, you can use the lower-level tools:

### Download Only (Legacy - No Database)
**Note:** These scripts download but don't ingest. Use `download_and_ingest_*.py` scripts instead.

```powershell
# Download last week's data (legacy, no ingestion)
python scripts/download/download_last_week.py

# Analyze downloaded files
python scripts/analysis/analyze_data.py
```

### Manual Database Operations
```powershell
# Create DB + tables
python orchestrator.py migrate

# Ingest a specific daily drop
python orchestrator.py ingest --product ES_OPTIONS_MDP3 --source data/raw/glbx-mdp3-2025-10-30

# Build gold-minute bars
python orchestrator.py build --product ES_OPTIONS_MDP3

# Sanity checks
python orchestrator.py validate --product ES_OPTIONS_MDP3
```

### Adding New Products

To add another product (e.g., H4N3, DBA-01):

1. Add a block to `config/schema_registry.yml` with inputs, migrations, loader, gold_sql
2. Create a migration file if new tables are needed (e.g., `db/migrations/10xx_product.sql`)
3. Add a loader in `pipelines/products/your_product.py`
4. Run: `migrate` → `ingest --product YOUR_PRODUCT --source ...` → `build` → `validate`





## Database Schema

### ES Options Tables
- `dim_instrument` - Instrument definitions (strikes, expiries, etc.)
- `f_quote_l1` - Level 1 quotes (bid/ask prices and sizes)
- `f_trade` - Trade executions (not populated from BBO-1m data)
- `g_bar_1m` - Gold layer: 1-minute aggregated bars

### ES Futures Tables
- `dim_fut_instrument` - Futures contract definitions (populated with `INSERT OR REPLACE` so reruns stay idempotent)
- `f_fut_quote_l1` - Level 1 quotes for futures
- `f_fut_trade` - Trade executions for futures (optional; loader skips the glob when no trades are present)
- `g_fut_bar_1m` - Gold layer: 1-minute bars for futures (uses DuckDB's `min_by` / `max_by` aggregators)

### Data Flow
```
DataBento API (BBO-1m)
    ↓
DBN Files (data/raw/*.dbn.zst)
    ↓
Transform → Folder Structure (instruments/, quotes_l1/, trades/)
    ↓
Load → Bronze Tables (f_quote_l1, dim_instrument)
    ↓
Build → Gold Tables (g_bar_1m)
```