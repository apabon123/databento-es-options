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

### 2. FRED Macro Series

```powershell
# Download all configured series
python scripts/download/download_fred_series.py

# Download and ingest in one step
python scripts/download/download_fred_series.py --ingest

# Download specific series only
python scripts/download/download_fred_series.py --series VIXCLS,FEDFUNDS --ingest
```

**Frequency:** Weekly (FRED updates daily, but weekly sync is sufficient)

**Series Included:**
- Volatility: VIXCLS, VXVCLS
- Rates: FEDFUNDS, DGS2, DGS5, DGS10, DGS30
- Spreads: BAMLH0A0HYM2, BAMLC0A0CM, TEDRATE
- Economic: CPIAUCSL, UNRATE, DTWEXBGS
- Yield Curve: T10Y2Y, T10YIE, T5YIFR

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
