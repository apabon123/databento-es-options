# Data Source Policy

Single source of truth policy for volatility data in the canonical research database.

## Summary

| Data Type | Source | Script | Coverage |
|-----------|--------|--------|----------|
| **VIX Index (1M)** | FRED | `download_fred_series.py` | 2020-01-02 to present |
| **VX Futures (VX1/2/3)** | CBOE → financial-data-system | `sync_vix_vx_from_financial_data_system.py` | 2004-03-26 to present |
| **VIX3M Index (3M)** | CBOE → financial-data-system | `sync_vix_vx_from_financial_data_system.py` | 2009-09-18 to present |
| **VVIX Index** | CBOE → financial-data-system | `sync_vix_vx_from_financial_data_system.py` | 2006-03-06 to present |

## Architecture

### VIX Index (Spot Volatility)

**Primary Source:** FRED (`VIXCLS` series)

```powershell
# Download VIX from FRED
python scripts/download/download_fred_series.py
```

**Storage:**
- Database: `data/silver/market.duckdb`
- Table: `f_fred_observations`
- Series ID: `VIXCLS`

**Rationale:**
- FRED provides clean, stable historical data
- No need to duplicate from financial-data-system
- Consistent with other macro series (rates, spreads, etc.)

### VX Futures Curve (VX1/2/3)

**Primary Source:** CBOE → financial-data-system → canonical DB

Symbols:
- `@VX=101XN` - Front month (VX1)
- `@VX=201XN` - 2nd month (VX2)
- `@VX=301XN` - 3rd month (VX3)

These are unadjusted, continuous, 1-day roll contracts (TradeStation convention).

```powershell
# 1. Update CBOE data in financial-data-system
cd ../data-management/financial-data-system
python update_vx_futures.py

# 2. Sync to canonical DB
cd ../databento-es-options
python scripts/database/sync_vix_vx_from_financial_data_system.py
```

**Storage:**
- Database: `data/silver/market.duckdb`
- Table: `market_data`
- Symbols: `@VX=101XN`, `@VX=201XN`, `@VX=301XN`

**Rationale:**
- financial-data-system maintains CBOE data with proper roll logic
- Clean separation: FRED for indexes, CBOE for futures
- TradeStation continuous contracts are consistent and well-maintained

### VIX3M (3-Month Volatility Index)

**Primary Source:** CBOE → financial-data-system → canonical DB

Symbol: `VIX3M` (formerly VXV - CBOE 3-Month Volatility Index)

```powershell
# 1. Update CBOE data in financial-data-system
cd ../data-management/financial-data-system
python -m src.scripts.market_data.vix.update_vix3m_index

# 2. Sync to canonical DB
cd ../databento-es-options
python scripts/database/sync_vix_vx_from_financial_data_system.py
```

**Storage:**
- Database: `data/silver/market.duckdb`
- Table: `market_data_cboe`
- Symbol: `VIX3M`

**Rationale:**
- FRED coverage for VIX3M is incomplete and inconsistent
- CBOE is the authoritative source (VIX3M successor to VXV)
- Used for VRP term structure analysis and regime filters
- Essential for understanding volatility curve shape (VIX vs VIX3M spread)

### VVIX (VIX Volatility Index)

**Primary Source:** CBOE → financial-data-system → canonical DB

Symbol: `VVIX` (CBOE VIX Volatility Index - Vol-of-Vol)

```powershell
# 1. Update CBOE data in financial-data-system
cd ../data-management/financial-data-system
# (VVIX update script should exist here)

# 2. Sync to canonical DB
cd ../databento-es-options
python scripts/database/sync_vix_vx_from_financial_data_system.py
```

**Storage:**
- Database: `data/silver/market.duckdb`
- Table: `market_data_cboe`
- Symbol: `VVIX`

**Rationale:**
- VVIX is not available via FRED API
- CBOE is the authoritative source
- Measures the expected volatility of VIX (volatility of volatility)
- Useful for volatility regime analysis and VIX forecasting

## Usage in Analysis

### Loading VIX (from FRED)

```python
def load_vix(con, start_date, end_date):
    """Load VIX index from FRED."""
    return con.execute("""
        SELECT date, value as vix_close
        FROM f_fred_observations
        WHERE series_id = 'VIXCLS'
          AND date BETWEEN ? AND ?
        ORDER BY date
    """, [start_date, end_date]).df()
```

### Loading VX Curve (from financial-data-system)

```python
def load_vx_curve(con, start_date, end_date):
    """Load VX1/2/3 curve from canonical DB."""
    return con.execute("""
        WITH vx1 AS (
            SELECT CAST(timestamp AS DATE) AS date, close AS vx1_close
            FROM market_data
            WHERE symbol = '@VX=101XN'
              AND CAST(timestamp AS DATE) BETWEEN ? AND ?
        ),
        vx2 AS (
            SELECT CAST(timestamp AS DATE) AS date, close AS vx2_close
            FROM market_data
            WHERE symbol = '@VX=201XN'
              AND CAST(timestamp AS DATE) BETWEEN ? AND ?
        ),
        vx3 AS (
            SELECT CAST(timestamp AS DATE) AS date, close AS vx3_close
            FROM market_data
            WHERE symbol = '@VX=301XN'
              AND CAST(timestamp AS DATE) BETWEEN ? AND ?
        )
        SELECT vx1.date, vx1_close, vx2_close, vx3_close
        FROM vx1
        LEFT JOIN vx2 USING(date)
        LEFT JOIN vx3 USING(date)
        ORDER BY date
    """, [start_date, end_date, start_date, end_date, start_date, end_date]).df()
```

### Loading VIX3M (from financial-data-system)

```python
def load_vix3m(con, start_date, end_date):
    """Load VIX3M index from canonical DB."""
    return con.execute("""
        SELECT 
            CAST(timestamp AS DATE) AS date, 
            close AS vix3m_close
        FROM market_data_cboe
        WHERE symbol = 'VIX3M'
          AND CAST(timestamp AS DATE) BETWEEN ? AND ?
        ORDER BY date
    """, [start_date, end_date]).df()
```

### Loading VVIX (from financial-data-system)

```python
def load_vvix(con, start_date, end_date):
    """Load VVIX index from canonical DB."""
    return con.execute("""
        SELECT 
            CAST(timestamp AS DATE) AS date, 
            close AS vvix_close
        FROM market_data_cboe
        WHERE symbol = 'VVIX'
          AND CAST(timestamp AS DATE) BETWEEN ? AND ?
        ORDER BY date
    """, [start_date, end_date]).df()
```

### Combined VRP Calculations

```python
def load_vrp_data(con, start_date, end_date):
    """Load VIX + VIX3M + VX for VRP calculations."""
    # VIX (1M) from FRED
    vix = load_vix(con, start_date, end_date)
    
    # VIX3M (3M) from financial-data-system
    vix3m = load_vix3m(con, start_date, end_date)
    
    # VX curve from financial-data-system
    vx = load_vx_curve(con, start_date, end_date)
    
    # Merge all
    df = vix.merge(vix3m, on='date', how='inner')
    df = df.merge(vx, on='date', how='inner')
    
    # Calculate spreads and term structure
    df['vix_vx1_spread'] = df['vix_close'] - df['vx1_close']
    df['vx2_vx1_slope'] = df['vx2_close'] - df['vx1_close']
    df['vix3m_vix_spread'] = df['vix3m_close'] - df['vix_close']  # Term structure
    
    return df
```

## Data Verification

```powershell
# Check VIX (1M) from FRED
python -c "import duckdb; con = duckdb.connect('data/silver/market.duckdb', read_only=True); print(con.execute(\"SELECT series_id, COUNT(*), MIN(date), MAX(date) FROM f_fred_observations WHERE series_id = 'VIXCLS' GROUP BY series_id\").fetchdf()); con.close()"

# Check VIX3M from financial-data-system
python -c "import duckdb; con = duckdb.connect('data/silver/market.duckdb', read_only=True); print(con.execute(\"SELECT symbol, COUNT(*), MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE)) FROM market_data_cboe WHERE symbol = 'VIX3M'\").fetchdf()); con.close()"

# Check VVIX from financial-data-system
python -c "import duckdb; con = duckdb.connect('data/silver/market.duckdb', read_only=True); print(con.execute(\"SELECT symbol, COUNT(*), MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE)) FROM market_data_cboe WHERE symbol = 'VVIX'\").fetchdf()); con.close()"

# Check VX from financial-data-system
python -c "import duckdb; con = duckdb.connect('data/silver/market.duckdb', read_only=True); print(con.execute(\"SELECT symbol, COUNT(*), MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE)) FROM market_data WHERE symbol IN ('@VX=101XN', '@VX=201XN', '@VX=301XN') GROUP BY symbol\").fetchdf()); con.close()"
```

**Or use SQL directly:**

```sql
-- VIX from FRED
SELECT series_id, COUNT(*) as rows, MIN(date) as first_date, MAX(date) as last_date
FROM f_fred_observations
WHERE series_id = 'VIXCLS';

-- VIX3M from CBOE
SELECT symbol, COUNT(*) as rows, 
       MIN(CAST(timestamp AS DATE)) as first_date, 
       MAX(CAST(timestamp AS DATE)) as last_date
FROM market_data_cboe
WHERE symbol = 'VIX3M';

-- VVIX from CBOE
SELECT symbol, COUNT(*) as rows, 
       MIN(CAST(timestamp AS DATE)) as first_date, 
       MAX(CAST(timestamp AS DATE)) as last_date
FROM market_data_cboe
WHERE symbol = 'VVIX';

-- VX from CBOE
SELECT symbol, COUNT(*) as rows,
       MIN(CAST(timestamp AS DATE)) as first_date,
       MAX(CAST(timestamp AS DATE)) as last_date
FROM market_data
WHERE symbol IN ('@VX=101XN', '@VX=201XN', '@VX=301XN')
GROUP BY symbol;
```

## Benefits

1. **No Duplication:** VIX (1M) comes from FRED only, not duplicated from financial-data-system
2. **Clear Ownership:** FRED for 1M spot index, CBOE for 3M index and futures
3. **Single Source of Truth:** Each series has exactly one authoritative source:
   - VIX (1M): FRED
   - VIX3M (3M): CBOE via financial-data-system
   - VVIX: CBOE via financial-data-system
   - VX1/2/3: CBOE via financial-data-system
4. **Complete Term Structure:** VIX (1M) + VIX3M (3M) + VX1/2/3 for full volatility curve analysis
5. **Consistency:** All data accessible from one canonical database

## Maintenance

**Regular Updates:**
1. **VIX (1M) from FRED:** Run `download_fred_series.py` weekly
2. **VIX3M + VX from CBOE:**
   ```powershell
   # In financial-data-system repo
   python -m src.scripts.market_data.vix.update_vix3m_index
   python update_vx_futures.py
   
   # In databento-es-options repo
   python scripts/database/sync_vix_vx_from_financial_data_system.py
   ```

**Quality Checks:**
- Verify date ranges match expected coverage:
  - VIX: 2020+ (FRED)
  - VIX3M: 2009+ (CBOE)
  - VVIX: 2006+ (CBOE)
  - VX1/2/3: 2004+ (CBOE)
- Check for gaps in time series
- Validate term structure is reasonable:
  - VIX3M > VIX (usually in contango)
  - VX1 ≈ VIX (spot vs front month alignment)
  - VX2 > VX1 (typical contango)

## Related Documentation

- [QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Common commands
- [TECHNICAL_REFERENCE.md](TECHNICAL_REFERENCE.md) - Database schema
- [scripts/database/README.md](../scripts/database/README.md) - Database scripts

