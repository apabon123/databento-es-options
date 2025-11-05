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

- **`check_database.py`** - Check for duplicate rows, show database statistics, and verify data coverage
- **`inspect_futures.py`** - Inspect ES futures data in detail

### `utils/` - Utility Scripts
Utility scripts for data management and organization.

- **`organize_raw_folder.py`** - Analyze and clean up raw data folder structure

### `analysis/` - Data Analysis Scripts
Scripts for analyzing downloaded data.

- **`analyze_data.py`** - Analyze downloaded ES options data for quality and completeness

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
# Check for duplicates and show statistics
python scripts/database/check_database.py

# Show statistics only
python scripts/database/check_database.py --stats-only

# Check specific product
python scripts/database/check_database.py --product ES_CONTINUOUS_MDP3

# Verify continuous futures coverage (check for missing dates and data quality)
python scripts/database/check_database.py --verify-coverage --year 2025

# Inspect futures data
python scripts/database/inspect_futures.py
```

### Utilities
```powershell
# Analyze raw folder structure
python scripts/utils/organize_raw_folder.py

# Clean up old/misnamed folders
python scripts/utils/organize_raw_folder.py --delete-old
```

### Analysis
```powershell
# Analyze downloaded data
python scripts/analysis/analyze_data.py
```

