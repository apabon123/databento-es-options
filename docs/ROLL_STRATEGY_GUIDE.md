# Roll Strategy Implementation Guide

## Overview

This project supports multiple roll strategies for continuous futures contracts. Each roll strategy represents a different method of transitioning from one futures contract to the next as they approach expiration.

## Roll Strategies

### Currently Available
- **`calendar-2d`**: 2-day pre-expiry calendar roll (default)
  - Rolls to the next contract 2 days before the front month expires
  - DataBento parameter: `roll_rule="2_days_pre_expiry"`

### Future Strategies
- **`calendar-1d`**: 1-day pre-expiry calendar roll
- **`volume`**: Volume-based roll (roll when back month volume exceeds front month)
- **`open-interest`**: Open interest-based roll

## Data Organization

All data is organized by roll strategy to prevent conflicts and ensure clarity:

```
raw/
  ohlcv-1d/
    downloads/
      es/
        calendar-2d/          ← ES with 2-day pre-expiry roll
          glbx-mdp3-es-2025-01-01.ohlcv-1d.fullday.parquet
          glbx-mdp3-es-2025-01-02.ohlcv-1d.fullday.parquet
        volume/               ← ES with volume roll (when implemented)
      nq/
        calendar-2d/
        volume/
    transformed/
      es/
        calendar-2d/
          2025-01-01/
            continuous_bars_daily/...
          2025-01-02/
        volume/
      nq/
        calendar-2d/
        volume/
  
  bbo-1m/
    downloads/
      calendar-2d/            ← All BBO data with 2-day roll
        glbx-mdp3-2025-01-01.bbo-1m.fullday.parquet
    transformed/
      calendar-2d/
        2025-01-01/
          continuous_quotes_l1/...
  
  continuous/
    transformed/
      calendar-2d/            ← L1 quotes/trades with 2-day roll
        2025-01-01/
          continuous_quotes_l1/...
          continuous_trades/...
```

## Database Schema

### Contract Series Naming

Contract series names encode the roll strategy:

**Format**: `{ROOT}_FRONT_{ROLL_STRATEGY}`

**Examples**:
- `ES_FRONT_CALENDAR_2D` - ES front month, 2-day pre-expiry calendar roll
- `ES_FRONT_CALENDAR_1D` - ES front month, 1-day pre-expiry calendar roll
- `ES_FRONT_VOLUME` - ES front month, volume-based roll
- `NQ_FRONT_CALENDAR_2D` - NQ front month, 2-day pre-expiry calendar roll

### dim_continuous_contract

```sql
CREATE TABLE dim_continuous_contract (
    contract_series VARCHAR PRIMARY KEY,  -- e.g., 'ES_FRONT_CALENDAR_2D'
    root VARCHAR,                          -- e.g., 'ES'
    rank INTEGER,                          -- 0 = front month, 1 = 2nd month
    roll_rule VARCHAR,                     -- e.g., '2_days_pre_expiry'
    adjustment_method VARCHAR,             -- e.g., 'unadjusted'
    description VARCHAR
);
```

### g_continuous_bar_daily

Each row represents a daily bar for a specific contract series on a specific date.

```sql
CREATE TABLE g_continuous_bar_daily (
    trading_date DATE,
    contract_series VARCHAR,               -- e.g., 'ES_FRONT_CALENDAR_2D'
    open DECIMAL(10, 2),
    high DECIMAL(10, 2),
    low DECIMAL(10, 2),
    close DECIMAL(10, 2),
    volume BIGINT,
    PRIMARY KEY (trading_date, contract_series)
);
```

## Adding a New Roll Strategy

### 1. Create Folder Structure

```bash
# For OHLCV-1d
mkdir -p "raw/ohlcv-1d/downloads/es/volume"
mkdir -p "raw/ohlcv-1d/downloads/nq/volume"
mkdir -p "raw/ohlcv-1d/transformed/es/volume"
mkdir -p "raw/ohlcv-1d/transformed/nq/volume"

# For BBO-1m
mkdir -p "raw/bbo-1m/downloads/volume"
mkdir -p "raw/bbo-1m/transformed/volume"

# For continuous (L1)
mkdir -p "raw/continuous/transformed/volume"
```

### 2. Download Data with New Roll Strategy

Modify the download script to use the new roll strategy:

```python
# In download script
ROLL_STRATEGY = "volume"  # or "calendar-1d", "open-interest"
OUT_DIR = bronze_root / "ohlcv-1d" / "downloads" / root.lower() / ROLL_STRATEGY
```

### 3. Create Database Definitions

Add new contract series to `dim_continuous_contract`:

```sql
INSERT INTO dim_continuous_contract VALUES
  ('ES_FRONT_VOLUME', 'ES', 0, 'volume_roll', 'unadjusted', 
   'ES continuous front month (roll: volume-based)'),
  ('NQ_FRONT_VOLUME', 'NQ', 0, 'volume_roll', 'unadjusted', 
   'NQ continuous front month (roll: volume-based)');
```

### 4. Ingest Data

The ingestion scripts automatically route data to the correct contract series based on:
1. The root (ES, NQ)
2. The roll strategy (from folder path)

## Benefits

✅ **No Conflicts**: Each roll strategy has its own isolated space  
✅ **Flexibility**: Add new strategies without breaking existing data  
✅ **Clarity**: File paths clearly indicate what roll strategy the data uses  
✅ **Database Integrity**: Each (root + roll) combination is uniquely identified  
✅ **Future-Proof**: Easy to add volume rolls, OI rolls, custom rolls, etc.

## Querying Data

### Get all available contract series
```sql
SELECT contract_series, root, roll_rule, description 
FROM dim_continuous_contract;
```

### Get ES data with 2-day roll
```sql
SELECT trading_date, open, high, low, close, volume
FROM g_continuous_bar_daily
WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
ORDER BY trading_date;
```

### Compare different roll strategies
```sql
-- Compare ES with different roll strategies (when available)
SELECT 
    d1.trading_date,
    d1.close as calendar_2d_close,
    d2.close as volume_close
FROM g_continuous_bar_daily d1
LEFT JOIN g_continuous_bar_daily d2 
  ON d1.trading_date = d2.trading_date
  AND d2.contract_series = 'ES_FRONT_VOLUME'
WHERE d1.contract_series = 'ES_FRONT_CALENDAR_2D'
ORDER BY d1.trading_date;
```

## Migration History

### 2025-11-06: Initial Roll Strategy Implementation
- **Migration**: `db/migrations/1005_update_contract_series_with_roll.sql`
- **Changes**:
  - Renamed `ES_FRONT_MONTH` → `ES_FRONT_CALENDAR_2D`
  - Renamed `NQ_FRONT_MONTH` → `NQ_FRONT_CALENDAR_2D`
  - Updated all fact tables to use new naming
- **File Migration**: `migrate_add_roll_strategy.py`
  - Moved 442 OHLCV-1d files to `{root}/calendar-2d/` structure
  - Moved 442 transformed directories to `{root}/calendar-2d/{date}/` structure
  - Moved 269 BBO-1m files to `calendar-2d/` structure
  - Moved 18 BBO-1m transformed directories to `calendar-2d/{date}/` structure
  - Moved 228 continuous transformed directories to `calendar-2d/{date}/` structure
  - **Total**: 1,399 items reorganized

## References

- DataBento Roll Rules: https://docs.databento.com/knowledge-base/new-users/continuous-contracts
- Project README: `README.md`
- Quick Reference: `QUICK_REFERENCE.md`

