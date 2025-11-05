# DataBento ES Options & Futures Data Pipeline

Download, transform, and store CME ES (E-mini S&P 500) options and futures BBO-1m data in a DuckDB warehouse.

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
│       └── data_transform.py               # Data transformation (NEW)
├── scripts/                                # User-facing scripts
│   ├── download_and_ingest_options.py      # All-in-one options wrapper (NEW)
│   ├── download_and_ingest_futures.py      # All-in-one futures wrapper (NEW)
│   ├── inspect_futures.py                  # Inspect futures data in DB (NEW)
│   ├── download_last_week.py               # Download only (legacy)
│   └── analyze_data.py                     # Validation runner
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

## Documentation

- [RUNBOOK.md](docs/RUNBOOK.md) - Detailed workflows and procedures
- [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) - Common issues and solutions
- [DATABENTO_ISSUE.md](docs/DATABENTO_ISSUE.md) - API quirks and clarifications

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

### Download Only (no database)
```powershell
# Download last week's data
python scripts/download_last_week.py

# Analyze downloaded files
python scripts/analyze_data.py
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