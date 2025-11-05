### Runbook: Wrapper Pipelines

Use these steps to estimate, download, ingest, and validate BBO-1m closes for ES options and futures. The wrapper scripts handle duplicate detection, cost estimation, transformation, ingestion, gold-layer construction, and validation.

Key scripts:
- Options: `scripts/download/download_and_ingest_options.py`
- Futures: `scripts/download/download_and_ingest_futures.py`

### 1. Estimate Only (Dry Run)
```powershell
# Options
python scripts/download/download_and_ingest_options.py --weeks 1 --dry-run

# Futures (resolves ES root to all outrights)
python scripts/download/download_and_ingest_futures.py --weeks 1 --dry-run
```

### 2. Download + Ingest (Interactive)
```powershell
# Options
python scripts/download/download_and_ingest_options.py --weeks 1

# Futures
python scripts/download/download_and_ingest_futures.py --weeks 1
```
*Add `--yes` to either command to skip the cost confirmation prompt.*

### 3. Download Specific Range or Symbols
```powershell
# Options - specific range
python scripts/download/download_and_ingest_options.py --start 2025-09-01 --end 2025-09-30 --yes

# Futures - explicit contracts
python scripts/download/download_and_ingest_futures.py --weeks 2 --symbols ESZ5,ESH6 --yes
```

### 4. Ingest Existing Raw Data Only
```powershell
python scripts/download/download_and_ingest_options.py --ingest-only
python scripts/download/download_and_ingest_futures.py --ingest-only
```

### 5. View Database Summary
```powershell
python scripts/download/download_and_ingest_options.py --summary
python scripts/download/download_and_ingest_futures.py --summary
```

### 6. Check for Un-Ingested Raw Files
```powershell
python scripts/download/download_and_ingest_options.py --check-missing
```
If any dates remain, run the wrapper with `--ingest-only` or the default workflow.

### 7. Force Re-download
```powershell
python scripts/download/download_and_ingest_options.py --weeks 1 --force --yes
```
Use this when you want to replace data that already exists in DuckDB.

### What the Wrappers Do

1. **Duplicate detection** – Checks DuckDB for existing `ts_event` dates and skips them unless `--force`.
2. **Cost estimation** – Calls DataBento's metadata endpoint and prints per-day size/cost before downloading.
3. **Download** – Pulls BBO-1m snapshots for the last N minutes before 15:00 CT for each day, writing filtered parquet files in `data/raw/`.
4. **Transform** – Converts raw files to the folder layout expected by the loaders:
   - Options: `instruments/` and `quotes_l1/` parquet files
   - Futures: resolves `ES.FUT` to instrument IDs, writes `fut_instruments/` and `fut_quotes_l1/`
   - `trades/` or `fut_trades/` directories are created only when trade data is available
5. **Ingest** – Loads parquet files into DuckDB using `INSERT OR REPLACE` for instrument dimensions so reruns remain idempotent. Missing globs are ignored gracefully.
6. **Gold build** – Runs the configured SQL in `config/schema_registry.yml` (now using DuckDB's `min_by`/`max_by` aggregators).
7. **Validation** – Executes product-specific validation queries, reporting counts for each check.
8. **Summary** – Prints total quotes, instrument counts, and date coverage for the product.

### Validation Details
- Required columns are checked (including `ts_recv`)
- Timestamps are converted to timezone-aware types
- Per-symbol ordering and spread sanity checks run automatically
- Results are logged to the console and `logs/downloader.log`

### Time Zone Notes
- RTH end time: 15:00:00 CT (`America/Chicago`)
- Windows (weekends) are skipped automatically
- All API requests are issued in UTC

### Troubleshooting
- **Cost estimate is $0** – No data for the range (holiday/weekend) or the dataset is empty
- **Duplicate key violations** – Use `--force` or delete the affected DuckDB file and rerun
- **No files found for fut_trades/** – Expected when trade data was not downloaded; safe to ignore
- **Symbology errors** – Explicit contracts must be passed via `--symbols`; the default command resolves `ES` to the `ES.FUT` universe automatically

Logs remain available in `logs/downloader.log` for all steps.

