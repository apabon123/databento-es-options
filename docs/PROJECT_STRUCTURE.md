# Project Structure

## Overview
Clean, organized structure for downloading and validating ES options data from DataBento.

## Directory Layout

```
databento-es-options/
├── src/                          # Core source code (importable modules)
│   ├── download/                 # Data download functionality
│   │   ├── __init__.py
│   │   └── bbo_downloader.py     # Main downloader with all logic
│   ├── validation/               # Data validation and analysis
│   │   ├── __init__.py
│   │   ├── integrity_checks.py   # Basic quality checks
│   │   └── data_analyzer.py      # Comprehensive analysis tool
│   └── utils/                    # Shared utilities
│       ├── __init__.py
│       └── logging_config.py     # Logging configuration
│
├── scripts/                      # User-facing CLI scripts
│   ├── download_last_week.py     # Download last week's data
│   └── analyze_data.py           # Analyze downloaded data
│
├── notebooks/                    # Jupyter notebooks for exploration
│   ├── explore_databento.ipynb   # DataBento API exploration
│   └── (exploratory notebooks)
│
├── archive/                      # Archived test/debug scripts
│   ├── debug_bbo_time_window.py  # Debug time filtering
│   ├── test_5min_cost.py         # Cost comparison tests
│   └── test_symbol_resolution.py # Symbol resolution tests
│
├── data/                         # Data storage (git-ignored)
│   └── raw/                      # Downloaded parquet files
│
├── docs/                         # Documentation
│   ├── RUNBOOK.md               # Step-by-step workflows
│   ├── TROUBLESHOOTING.md       # Common issues
│   ├── DATABENTO_ISSUE.md       # API clarifications
│   └── PROJECT_STRUCTURE.md     # This file
│
├── logs/                         # Log files (git-ignored)
│   └── downloader.log           # Application logs
│
├── .env                          # Environment variables (git-ignored)
├── .env.example                  # Template for .env
├── .gitignore                    # Git ignore rules
├── README.md                     # Main documentation
└── requirements.txt              # Python dependencies
```

## Key Files

### User Scripts
- **`scripts/download_last_week.py`** - Main download script (run this first)
- **`scripts/analyze_data.py`** - Data validation script (run after download)

### Core Modules
- **`src/download/bbo_downloader.py`** - All download logic, cost estimation, file writing
- **`src/validation/data_analyzer.py`** - Comprehensive data quality analysis
- **`src/validation/integrity_checks.py`** - Basic validation checks
- **`src/utils/logging_config.py`** - Centralized logging setup

### Documentation
- **`README.md`** - Quick start and overview
- **`docs/RUNBOOK.md`** - Detailed procedures
- **`docs/TROUBLESHOOTING.md`** - Common problems and solutions

## Workflow

1. **Setup**: Install dependencies, configure `.env`
2. **Download**: Run `python scripts/download_last_week.py`
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

