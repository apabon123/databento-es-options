# VIX3M ETL Bridge Implementation Summary

Implementation completed: December 9, 2025

## Overview

Extended the VX ETL bridge to include VIX3M (3-month implied volatility index) sync from financial-data-system into the canonical databento-es-options database.

## Changes Made

### 1. Script Updates

**File:** `scripts/database/sync_vix_vx_from_financial_data_system.py`

**Added:**
- `VIX3M_SYMBOL = "VIX3M"` constant
- `sync_vix3m_index()` function to sync VIX3M from `fin.market_data_cboe` → `canon.market_data_cboe`
- Call to `sync_vix3m_index()` in `main()` after VX sync
- Updated docstring and help text to reflect VIX3M inclusion

**Key Features:**
- Idempotent: deletes existing VIX3M rows before inserting fresh data
- Ensures `market_data_cboe` table exists using existing `ensure_table_schema()` function
- Does NOT touch VIX (1M) - that remains FRED-only
- Maintains all existing VX logic intact

### 2. Documentation Updates

**File:** `scripts/database/README.md`
- Updated script description to include VIX3M
- Expanded data source policy section with detailed source/target information

**File:** `docs/SOT/DATA_SOURCE_POLICY.md`
- Updated summary table to show VIX3M as active (not future)
- Replaced "Future Enhancement" section with complete VIX3M implementation details
- Added `load_vix3m()` loader function example
- Updated `load_vrp_data()` to include VIX3M and term structure calculations
- Enhanced data verification section with VIX3M SQL queries
- Updated benefits and maintenance sections

## Data Verification Results

### Source: financial-data-system
```
VIX3M:
  rows: 4,081
  coverage: 2009-09-18 to 2025-12-08
```

### Target: databento-es-options (canonical DB)
```
VIX3M:
  rows: 4,081
  coverage: 2009-09-18 to 2025-12-08

VX1/2/3:
  @VX=101XN: 5,476 rows (2004-03-26 to 2025-12-09)
  @VX=201XN: 5,436 rows (2004-03-26 to 2025-12-09)
  @VX=301XN: 5,121 rows (2004-05-19 to 2025-12-09)

VIX (1M):
  VIXCLS: 1,539 rows (2020-01-02 to 2025-11-25)
```

✅ All data synced successfully, existing data (VX, VIX from FRED) remains intact

## Data Source Policy (Final)

| Data Type | Source | Table | Symbol(s) |
|-----------|--------|-------|-----------|
| **VIX (1M)** | FRED | `f_fred_observations` | `VIXCLS` |
| **VIX3M (3M)** | CBOE → financial-data-system | `market_data_cboe` | `VIX3M` |
| **VX Futures** | CBOE → financial-data-system | `market_data` | `@VX=101XN`, `@VX=201XN`, `@VX=301XN` |

**Key Principles:**
- Each series has exactly ONE authoritative source
- VIX (1M) from FRED only - NOT synced from financial-data-system
- VIX3M from CBOE - FRED coverage insufficient
- VX futures from CBOE - proper roll logic maintained

## Usage

### Update Workflow

```powershell
# 1. Update CBOE data in financial-data-system
cd ../data-management/financial-data-system
python -m src.scripts.market_data.vix.update_vix3m_index
python update_vx_futures.py

# 2. Sync to canonical DB
cd ../databento-es-options
python scripts/database/sync_vix_vx_from_financial_data_system.py
```

### Query Examples

```python
# Load VIX3M
def load_vix3m(con, start_date, end_date):
    return con.execute("""
        SELECT 
            CAST(timestamp AS DATE) AS date, 
            close AS vix3m_close
        FROM market_data_cboe
        WHERE symbol = 'VIX3M'
          AND CAST(timestamp AS DATE) BETWEEN ? AND ?
        ORDER BY date
    """, [start_date, end_date]).df()

# Load complete volatility term structure
def load_vol_term_structure(con, start_date, end_date):
    """Load VIX (1M) + VIX3M (3M) + VX1/2/3 for full term structure."""
    # VIX from FRED
    vix = con.execute("""
        SELECT date, value as vix_close
        FROM f_fred_observations
        WHERE series_id = 'VIXCLS' AND date BETWEEN ? AND ?
    """, [start_date, end_date]).df()
    
    # VIX3M from CBOE
    vix3m = con.execute("""
        SELECT CAST(timestamp AS DATE) AS date, close AS vix3m_close
        FROM market_data_cboe
        WHERE symbol = 'VIX3M' 
          AND CAST(timestamp AS DATE) BETWEEN ? AND ?
    """, [start_date, end_date]).df()
    
    # Merge and calculate term structure
    df = vix.merge(vix3m, on='date', how='inner')
    df['vix3m_vix_spread'] = df['vix3m_close'] - df['vix_close']
    
    return df
```

## Next Steps for Futures-Six

Use the complete volatility term structure for VRP analysis:

1. **VIX-VX1 Spread:** Spot vs front month (risk premium)
2. **VIX3M-VIX Spread:** Term structure (1M vs 3M)
3. **VX2-VX1 Slope:** Futures curve shape
4. **Regime Filters:** Use VIX3M/VIX ratio for market regime classification

All data now available in one canonical database with clean source separation! 🎯

## Files Modified

1. `scripts/database/sync_vix_vx_from_financial_data_system.py` - Extended with VIX3M sync
2. `scripts/database/README.md` - Updated script description and policy
3. `docs/SOT/DATA_SOURCE_POLICY.md` - Complete VIX3M documentation with examples
4. `docs/VIX3M_IMPLEMENTATION_SUMMARY.md` - This summary document

## Testing

- [x] VIX3M syncs successfully (4,081 rows)
- [x] VX data remains intact (16,033 rows)
- [x] VIX from FRED remains intact (1,539 rows)
- [x] Script is idempotent (safe to run multiple times)
- [x] Documentation updated and consistent
- [x] Date ranges verified (2009-2025 for VIX3M)

## Maintenance

**Weekly Updates:**
```powershell
# Update VIX from FRED
python scripts/download/download_fred_series.py

# Update VIX3M + VX from CBOE
cd ../data-management/financial-data-system
python -m src.scripts.market_data.vix.update_vix3m_index
python update_vx_futures.py
cd ../databento-es-options
python scripts/database/sync_vix_vx_from_financial_data_system.py
```

**Quality Checks:**
```sql
-- Verify VIX3M coverage
SELECT COUNT(*) as rows, 
       MIN(CAST(timestamp AS DATE)) as first_date, 
       MAX(CAST(timestamp AS DATE)) as last_date
FROM market_data_cboe
WHERE symbol = 'VIX3M';

-- Check term structure (VIX3M should typically > VIX)
SELECT 
    CAST(v.timestamp AS DATE) as date,
    v.close as vix_close,
    v3.close as vix3m_close,
    v3.close - v.close as term_spread
FROM market_data_cboe v3
JOIN f_fred_observations f ON CAST(v3.timestamp AS DATE) = f.date
WHERE v3.symbol = 'VIX3M' 
  AND f.series_id = 'VIXCLS'
ORDER BY date DESC
LIMIT 10;
```

