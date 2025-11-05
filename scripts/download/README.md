# Download Scripts

Scripts for downloading data from DataBento and ingesting into the database.

## Scripts

### `download_and_ingest_options.py`
Main wrapper script for downloading and ingesting ES options data.

**Usage:**
```powershell
# Download last 3 weeks
python scripts/download/download_and_ingest_options.py --weeks 3

# Download specific date range
python scripts/download/download_and_ingest_options.py --start 2025-09-01 --end 2025-09-30

# Just ingest existing raw data (no download)
python scripts/download/download_and_ingest_options.py --ingest-only

# View database summary
python scripts/download/download_and_ingest_options.py --summary
```

### `download_and_ingest_futures.py`
Main wrapper script for downloading and ingesting ES futures data.

**Usage:**
```powershell
# Download last week
python scripts/download/download_and_ingest_futures.py --weeks 1

# Download specific contracts
python scripts/download/download_and_ingest_futures.py --weeks 1 --symbols ESZ5,ESH6
```

### `download_and_ingest_continuous.py`
Main wrapper script for downloading and ingesting ES continuous futures data.

**Usage:**
```powershell
# Download full day continuous futures
python scripts/download/download_and_ingest_continuous.py --weeks 1

# Download last 5 minutes only
python scripts/download/download_and_ingest_continuous.py --weeks 1 --last-minutes
```

### `download_last_week.py`
Quick script to download last week's data.

**Usage:**
```powershell
python scripts/download/download_last_week.py
```

