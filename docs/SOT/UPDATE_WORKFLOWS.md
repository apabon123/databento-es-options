# Update Workflows

Standard procedures for updating data in the canonical research database.

---

## Daily/Weekly Updates

### 1. ES Options & Futures (Last Week)

```powershell
# ES Options - last week
python scripts/download/download_and_ingest_options.py --weeks 1

# ES Futures - last week
python scripts/download/download_and_ingest_futures.py --weeks 1
```

**Frequency:** Weekly (or as needed for recent analysis)

---

### 2. FRED Macro Series (including SPX, NDX spot)

```powershell
# Download all configured series
python scripts/download/download_fred_series.py

# Download and ingest in one step
python scripts/download/download_fred_series.py --ingest

# Download specific series only
python scripts/download/download_fred_series.py --series VIXCLS,FEDFUNDS --ingest

# Download spot indices from FRED
python scripts/download/download_fred_series.py --series SP500,NASDAQ100 --ingest
```

**Frequency:** Weekly (FRED updates daily, but weekly sync is sufficient)

**Series Included:**
- Volatility: VIXCLS, VXVCLS
- Rates: FEDFUNDS, DGS2, DGS5, DGS10, DGS30, SOFR, DTB3
- Spreads: BAMLH0A0HYM2, BAMLC0A0CM, TEDRATE
- Economic: CPIAUCSL, UNRATE, DTWEXBGS
- Yield Curve: T10Y2Y, T10YIE, T5YIFR
- Global Rates: ECBDFR, IRSTCI01JPM156N, IUDSOIA
- **Spot Indices (Price-Return):** SP500, NASDAQ100

---

### 2b. RUT Spot Index (Yahoo/MarketWatch)

```powershell
# RECOMMENDED: Sanity check providers before downloading
python scripts/download/download_index_spot.py --probe

# Download RUT and ingest
python scripts/download/download_index_spot.py --ingest

# Force full backfill from specific date
python scripts/download/download_index_spot.py --backfill --start 1990-01-01 --ingest

# Ingest only (no download)
python scripts/download/download_index_spot.py --ingest-only
```

**Frequency:** Monthly (or as needed) — cached to `data/external/index_spot/`

**Series Included:**
- RUT_SPOT (Russell 2000 price-return index)

**Non-Silent Failure Contract:**
- If both Yahoo and MarketWatch fail, **the script hard-fails and does not ingest**
- Hard-fail if data is stale (latest date > 10 calendar days old)
- Hard-fail if backfill returns insufficient rows (< 1000 rows)
- Hard-fail if historical values change (append-only enforcement)

**Important:**
- Uses `Close` price (NOT `Adj Close`) for price-return level
- Yahoo is primary source, MarketWatch is fallback
- Use `--probe` to verify providers are working before download
- Validates: no duplicates, no negative values, flags >20% daily moves

**One-Time Manual Backfill (2026-01-21):**
- RUT_SPOT was manually seeded for 2020-01-02 to 2026-01-20 via CSV import (`--import-csv`)
- Going forward: **append-only updates via providers** (Yahoo/MarketWatch)
- Manual CSV import (`--import-csv`) should be used **only if it extends history earlier than current min date** (rare)
- Manual CSV import **never overwrites** existing data — it only inserts missing dates

---

### 3. VX/VIX3M/VVIX from financial-data-system

**Prerequisites:**
1. Update CBOE data in financial-data-system first:
   ```powershell
   cd ../data-management/financial-data-system
   python -m src.scripts.market_data.vix.update_vix3m_index
   python update_vx_futures.py
   ```

2. Sync to canonical DB:
   ```powershell
   cd ../databento-es-options
   python scripts/database/sync_vix_vx_from_financial_data_system.py
   ```

**Frequency:** Weekly (after financial-data-system updates)

**Data Synced:**
- VX Futures: @VX=101XN, @VX=201XN, @VX=301XN
- VIX3M Index
- VVIX Index

**Note:** VIX (1M) is **NOT** synced here - use FRED as primary source.

---

## Historical Backfills

### Continuous Daily OHLCV (All Products)

```powershell
# Download all configured products
python scripts/download/download_universe_daily_ohlcv.py --start 2020-01-01 --end 2025-12-31

# Download specific roots
python scripts/download/download_universe_daily_ohlcv.py --roots ES,NQ,ZN --start 2015-01-01

# Exclude optional products (e.g., VX)
python scripts/download/download_universe_daily_ohlcv.py --exclude-optional --start 2020-01-01
```

**Products Included:**
- Equity: ES, NQ, RTY
- Rates: ZT, ZF, ZN, UB
- Commodities: CL, GC
- FX: 6E, 6J, 6B
- STIR: SR3 (full curve, ranks 0-12)
- Vol: VX (optional)

---

### Continuous Intraday (1-minute)

```powershell
# Download for specific date range
python scripts/download/download_and_ingest_continuous.py --start 2025-01-01 --end 2025-01-31

# Download last N days
python scripts/download/download_and_ingest_continuous.py --days 7
```

---

### Instrument Definitions

```powershell
# Download for all instruments in database
python scripts/database/download_instrument_definitions.py --all

# Download for specific root
python scripts/database/download_instrument_definitions.py --root ES

# Force re-download
python scripts/database/download_instrument_definitions.py --root SR3 --force
```

**Frequency:** As needed when new contracts are added

---

## Validation & Quality Checks

### Mandatory Post‑Ingest Diagnostics (REQUIRED)

After **any ingest/build** workflow, run the unified diagnostics and generate the health report.

This is the repo’s governed “post‑ingest health gate” and is designed to:
- use **actual DB data** (DuckDB)
- rely on **canonical views** for downstream health
- avoid **trading schedule assumptions** (no Mon–Fri expectations)

```powershell
# 1) Ensure the data-derived trading calendar is up to date (required for coverage computations)
python scripts/database/sync_session_from_data.py

# 2) Run unified post-ingest diagnostics (hard-fails with exit code 2 on any hard failure)
python scripts/diagnostics/run_post_ingest_diagnostics.py --json-out artifacts/post_ingest_diagnostics.json

# 3) Generate a static visual health report from canonical views
python scripts/diagnostics/generate_health_report.py --out artifacts/health_report.html
```

See: `docs/SOT/DIAGNOSTICS.md` for the canonical checklist and hard-fail criteria.

### Check Database Statistics

```powershell
# General stats and duplicate check
python scripts/database/check_database.py

# Verify coverage (check for missing dates)
python scripts/database/check_database.py --verify-coverage --year 2025
```

### Verify Data Coverage

```sql
-- Check VIX from FRED
SELECT series_id, COUNT(*) as rows, MIN(date) as first_date, MAX(date) as last_date
FROM f_fred_observations
WHERE series_id = 'VIXCLS';

-- Check spot indices (SPX, NDX from FRED, RUT from Yahoo)
SELECT series_id, COUNT(*) as rows, MIN(date) as first_date, MAX(date) as last_date
FROM f_fred_observations
WHERE series_id IN ('SP500', 'NASDAQ100', 'RUT_SPOT')
GROUP BY series_id
ORDER BY series_id;

-- Check VIX3M from CBOE
SELECT symbol, COUNT(*) as rows, 
       MIN(CAST(timestamp AS DATE)) as first_date, 
       MAX(CAST(timestamp AS DATE)) as last_date
FROM market_data_cboe
WHERE symbol = 'VIX3M';

-- Check VX from CBOE
SELECT symbol, COUNT(*) as rows,
       MIN(CAST(timestamp AS DATE)) as first_date,
       MAX(CAST(timestamp AS DATE)) as last_date
FROM market_data
WHERE symbol IN ('@VX=101XN', '@VX=201XN', '@VX=301XN')
GROUP BY symbol;

-- Check continuous futures coverage
SELECT contract_series, COUNT(*) as rows,
       MIN(trading_date) as first_date,
       MAX(trading_date) as last_date
FROM g_continuous_bar_daily
GROUP BY contract_series
ORDER BY contract_series;
```

---

## Maintenance Schedule

| Data Type | Update Frequency | Script |
|-----------|------------------|--------|
| **ES Options** | Weekly | `download_and_ingest_options.py` |
| **ES Futures** | Weekly | `download_and_ingest_futures.py` |
| **FRED Series** | Weekly | `download_fred_series.py` |
| **SPX/NDX Spot** | Weekly (with FRED) | `download_fred_series.py` |
| **RUT Spot** | Daily (EOD) | `download_index_spot.py` |
| **VX/VIX3M/VVIX** | Weekly (after source update) | `sync_vix_vx_from_financial_data_system.py` |
| **Continuous Daily OHLCV** | As needed (backfills) | `download_universe_daily_ohlcv.py` |
| **Instrument Definitions** | As needed (new contracts) | `download_instrument_definitions.py` |

---

## Troubleshooting

### Missing Data

1. **Check date ranges:**
   ```sql
   SELECT MIN(trading_date), MAX(trading_date), COUNT(*)
   FROM g_continuous_bar_daily
   WHERE contract_series = 'ES_FRONT_CALENDAR_2D';
   ```

2. **Check for gaps:**
   ```sql
   WITH dates AS (
     SELECT trading_date,
            LAG(trading_date) OVER (ORDER BY trading_date) as prev_date
     FROM g_continuous_bar_daily
     WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
   )
   SELECT prev_date, trading_date,
          JULIANDAY(trading_date) - JULIANDAY(prev_date) as gap_days
   FROM dates
   WHERE gap_days > 3  -- More than 3 days gap (excluding weekends)
   ORDER BY trading_date;
   ```

### Data Quality Issues

1. **Check for duplicates:**
   ```powershell
   python scripts/database/check_database.py
   ```

2. **Verify source data:**
   - FRED: Check series on FRED website
   - CBOE: Verify in financial-data-system database
   - DataBento: Check download logs

---

## Related Documentation

- [DATA_SOURCE_POLICY.md](./DATA_SOURCE_POLICY.md) - Authoritative data sources
- [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md) - Database architecture
- [INTEROP_CONTRACT.md](INTEROP_CONTRACT.md) - Guaranteed tables and series
- [QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Command reference
