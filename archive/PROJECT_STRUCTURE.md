# Project Structure

## Overview
Clean, organized structure for downloading and validating ES options data from DataBento.

## Directory Layout

```
databento-es-options/
├── src/                          # Core source code (importable modules)
│   ├── download/                 # Data download functionality
│   │   ├── __init__.py
│   │   ├── batch_downloader.py
│   │   └── bbo_downloader.py
│   ├── validation/               # Data validation and analysis
│   │   ├── __init__.py
│   │   ├── integrity_checks.py
│   │   └── data_analyzer.py
│   └── utils/                    # Shared utilities
│       ├── __init__.py
│       ├── continuous_transform.py
│       ├── data_transform.py
│       └── logging_config.py
│
├── scripts/                      # User-facing CLI scripts
│   ├── download_universe_daily_ohlcv.py
│   ├── download_fred_series.py
│   ├── download_and_ingest_options.py
│   ├── download_and_ingest_futures.py
│   ├── download_and_ingest_continuous.py
│   ├── download_continuous_daily_ohlcv.py
│   ├── download_last_week.py
│   ├── analyze_data.py
│   └── database/…                # Database inspection helpers
│
├── configs/                      # High-level configuration metadata
│   ├── download_universe.yaml
│   ├── fred_series.yaml
│   ├── fred_settings.yaml
│   ├── rates_dv01.yaml
│   ├── contracts.yaml
│   └── universe.yaml
│
├── pipelines/                    # Database pipeline definitions
│   ├── products/                 # Product-specific loaders
│   ├── loader.py
│   ├── registry.py
│   └── common.py
│
├── notebooks/                    # Jupyter notebooks for exploration
├── archive/                      # Archived test/debug scripts
├── data/                         # Data storage (git-ignored)
├── docs/                         # Documentation (this file lives here)
├── logs/                         # Log files (git-ignored)
├── .env / .env.example           # Environment configuration
├── README.md                     # Main documentation
└── requirements.txt              # Python dependencies
```

## Key Files

### User Scripts
- **`scripts/download_universe_daily_ohlcv.py`** - Config-driven OHLCV downloader for multiple roots/ranks
- **`scripts/download_fred_series.py`** - FRED macro downloader that normalizes to daily frequency
- **`scripts/download_and_ingest_*.py`** - One-shot wrappers for options, futures, and continuous data
- **`scripts/download_continuous_daily_ohlcv.py`** - Per-root continuous daily download utility
- **`scripts/analyze_data.py`** - Data validation script (run after download)

### Core Modules
- **`src/download/bbo_downloader.py`** - BBO-1m download logic, cost estimation, file writing
- **`src/download/batch_downloader.py`** - Batch helper for large continuous pulls
- **`src/utils/continuous_transform.py`** - Helpers for continuous contract transforms
- **`src/validation/data_analyzer.py`** - Comprehensive data quality analysis
- **`src/validation/integrity_checks.py`** - Basic validation checks
- **`src/utils/logging_config.py`** - Centralized logging setup

### Config Metadata
- **`configs/download_universe.yaml`** - Defines roots, roll rules, and rank ranges to download
- **`configs/fred_series.yaml`** - FRED series manifest (IDs and descriptions)
- **`configs/fred_settings.yaml`** - FRED API key placeholder and default date range
- **`configs/rates_dv01.yaml`** - Duration proxies for Treasury curve hedging
- **`configs/contracts.yaml`** - Alias mapping for newly added roots
- **`configs/universe.yaml`** - Discovery manifest for orchestration or downstream agents

### Documentation
- **`README.md`** - Quick start and overview
- **`docs/RUNBOOK.md`** - Detailed procedures
- **`docs/TROUBLESHOOTING.md`** - Common problems and solutions

## Workflow

1. **Setup**: Install dependencies, configure `.env`
2. **Download**: Run `python scripts/download_universe_daily_ohlcv.py --weeks 4` (or the wrapper of choice)
3. **Validate**: Run `python scripts/analyze_data.py`
4. **Explore**: Use notebooks for data exploration

## Design Principles

### Separation of Concerns
- **`src/`** - Reusable, importable modules
- **`scripts/`** - Simple CLI wrappers
- **`notebooks/`** - Interactive exploration
- **`archive/`** - Historical/debugging code (not for production)

### Clean Imports
All imports use absolute paths from project root:
```python
from src.download.bbo_downloader import download_bbo_last_window
from src.validation.data_analyzer import analyze_file
from src.utils.logging_config import get_logger
```

### Data Organization
- **Downloaded files**: `data/raw/*.parquet` (one file per day)
- **Naming**: `glbx-mdp3-YYYY-MM-DD.bbo-1m.last5m.parquet`
- **Format**: Parquet (efficient, portable, easy to work with)

## Future Enhancements

- **`tests/`** - Add unit tests
- **`src/analysis/`** - Add advanced analytics (Greeks, IV, etc.)
- **CI/CD** - Automated testing and deployment
- **Database** - Store processed data in SQL/TimescaleDB

## Maintenance

### Adding New Features
1. Add code to appropriate `src/` module
2. Create a simple script in `scripts/` if user-facing
3. Update `README.md` and relevant docs
4. Add tests when `tests/` directory is created

### Cleaning Up
- Keep `archive/` for historical reference
- Delete only when code is truly obsolete
- Document why code was archived

### Version Control
- Commit often with clear messages
- Use .gitignore to exclude data/logs
- Never commit `.env` file (contains API key)

