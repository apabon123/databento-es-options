# Download Scripts

Data download & ingestion. See [QUICK_REFERENCE.md](../../docs/QUICK_REFERENCE.md#download-commands) for full documentation.

## Scripts

| Script | Purpose |
|--------|---------|
| `download_and_ingest_options.py` | ES options (BBO-1m) |
| `download_and_ingest_futures.py` | ES futures (BBO-1m) |
| `download_and_ingest_continuous.py` | ES continuous intraday |
| `download_and_ingest_continuous_daily.py` | Continuous daily OHLCV |
| `download_universe_daily_ohlcv.py` | Full universe daily OHLCV |
| `download_fred_series.py` | FRED macro series |
| `download_last_week.py` | Legacy: download only (no ingest) |

## Quick Examples

```powershell
python scripts/download/download_and_ingest_options.py --weeks 3
python scripts/download/download_universe_daily_ohlcv.py --start 2020-01-01
python scripts/download/download_fred_series.py
```
