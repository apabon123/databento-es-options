# Scripts Directory

This directory contains utility scripts organized by function.

## 📁 Directory Structure

### `download/` - Data Download & Ingestion Scripts
Scripts for downloading data from DataBento and ingesting into the database.

- **`download_and_ingest_options.py`** - Download and ingest ES options data
- **`download_and_ingest_futures.py`** - Download and ingest ES futures data
- **`download_and_ingest_continuous.py`** - Download and ingest ES continuous futures data
- **`download_last_week.py`** - Quick script to download last week's data

### `database/` - Database Management Scripts
Scripts for inspecting, maintaining, and checking the database.

- **`check_database.py`** - Check for duplicate rows and show database statistics
- **`inspect_futures.py`** - Inspect ES futures data in detail

### `analysis/` - Data Analysis Scripts
Scripts for analyzing downloaded data.

- **`analyze_data.py`** - Analyze downloaded data
- **`analyze_downloaded_data.py`** - Analyze downloaded data files

## Usage Examples

### Download Data
```powershell
# Download and ingest options
python scripts/download/download_and_ingest_options.py --weeks 3

# Download and ingest futures
python scripts/download/download_and_ingest_futures.py --weeks 1

# Download and ingest continuous futures
python scripts/download/download_and_ingest_continuous.py --weeks 1
```

### Database Management
```powershell
# Check for duplicates
python scripts/database/check_database.py

# Inspect futures data
python scripts/database/inspect_futures.py

# Check specific product
python scripts/database/check_database.py --product ES_CONTINUOUS_MDP3
```

### Analysis
```powershell
# Analyze downloaded data
python scripts/analysis/analyze_downloaded_data.py
```

