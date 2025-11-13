# Instrument Definitions

## Overview

This document describes how instrument definitions (contract specifications) are downloaded and stored from DataBento's `definition` schema. This includes tick sizes, expiry dates, multipliers, and all other contract specifications needed for curve building and trading.

## Database Schema

### Table: `dim_instrument_definition`

Stores full contract specifications from DataBento's definition schema.

**Primary Key:** `instrument_id` (ensures only ONE definition per instrument)

**Key Fields:**
- `instrument_id` - DataBento instrument ID (PRIMARY KEY - one definition per instrument)
- `native_symbol` - Native symbol (e.g., "SR3H1", "ESH1")
- `definition_date` - Date when this definition was active
- `ts_event` - Timestamp from DataBento definition record

**Contract Specifications:**
- `min_price_increment` - Tick size (e.g., 0.0025 for SR3, 0.2500 for ES)
- `min_price_increment_amount` - Dollar value per tick
- `contract_multiplier` - Contract multiplier
- `expiration` - Expiration date/time
- `maturity_year`, `maturity_month`, `maturity_day` - Maturity information
- `currency` - Currency code (e.g., USD)
- `unit_of_measure` - Unit of measure (e.g., USD, IPNT)
- `unit_of_measure_qty` - Unit of measure quantity
- `high_limit_price`, `low_limit_price` - Price limits
- `min_trade_vol`, `max_trade_vol` - Trading volumes
- And many more fields...

### View: `v_instrument_definition_latest`

Provides the latest definition for each instrument, making it easy to query current contract specifications.

## Usage

### Download Definitions for All Instruments

```bash
python scripts/database/download_instrument_definitions.py --all
```

### Download Definitions for a Specific Root

```bash
python scripts/database/download_instrument_definitions.py --root SR3
python scripts/database/download_instrument_definitions.py --root ES
```

### Force Re-download

```bash
python scripts/database/download_instrument_definitions.py --all --force
```

### Show Summary

```bash
python scripts/database/download_instrument_definitions.py --summary
python scripts/database/download_instrument_definitions.py --summary --root SR3
```

## How It Works

1. **Extract Instrument IDs**: Queries `g_continuous_bar_daily` to get all unique `underlying_instrument_id` values
2. **Get Date Ranges**: Determines when each instrument was active (first and last trading date)
3. **Resolve to Native Symbols**: Uses DataBento's symbology API to resolve instrument IDs to native symbols (e.g., "SR3H1")
4. **Download Definitions**: Downloads definitions from DataBento's `definition` schema for the date ranges when instruments were active
5. **Store in Database**: Stores definitions in `dim_instrument_definition` table

## Key Features

- **Automatic**: Automatically finds all instruments with daily data
- **Efficient**: Only downloads definitions for instruments that don't already have them (unless `--force` is used)
- **Batch Processing**: Processes instruments in batches to handle large datasets
- **Date Ranges**: Uses actual date ranges from daily bars to minimize API calls
- **Error Handling**: Handles errors gracefully and continues processing

## Use Cases

### Curve Building

Use expiration dates to build forward rate curves for SOFR futures:

```sql
SELECT 
    instrument_id,
    native_symbol,
    expiration,
    maturity_year,
    maturity_month,
    maturity_day,
    min_price_increment,
    min_price_increment_amount
FROM v_instrument_definition_latest
WHERE asset = 'SR3'
ORDER BY expiration;
```

### Contract Specifications

Get contract specifications for trading:

```sql
SELECT 
    native_symbol,
    asset,
    min_price_increment,
    min_price_increment_amount,
    contract_multiplier,
    currency,
    high_limit_price,
    low_limit_price,
    min_trade_vol,
    max_trade_vol
FROM v_instrument_definition_latest
WHERE asset = 'ES';
```

### Roll Date Tracking

Use expiration dates to determine when contracts roll:

```sql
SELECT 
    d.native_symbol,
    d.expiration,
    d.maturity_year,
    d.maturity_month,
    d.maturity_day,
    b.trading_date as last_trading_date
FROM v_instrument_definition_latest d
JOIN (
    SELECT 
        underlying_instrument_id,
        MAX(trading_date) as trading_date
    FROM g_continuous_bar_daily
    GROUP BY underlying_instrument_id
) b ON d.instrument_id = b.underlying_instrument_id
WHERE d.asset = 'SR3'
ORDER BY d.expiration;
```

## Notes

- **One Definition Per Instrument**: The table uses `instrument_id` as the primary key, ensuring that each contract has exactly ONE definition stored, even if it appears hundreds of times in daily bars (different dates, ranks, etc.)
- **Deduplication**: The download script automatically deduplicates:
  - Symbols before downloading (one API call per unique symbol)
  - Instrument IDs after downloading (keeps latest definition per instrument_id)
- **Efficient Downloads**: Even if a contract appears 1000 times in the database, its definition is downloaded and stored only ONCE
- Definitions are downloaded for the date ranges when instruments were active (based on daily bars)
- The `v_instrument_definition_latest` view provides the definition for each instrument (since there's only one per instrument, this is a pass-through view)
- Definitions include all contract specifications needed for curve building and trading
- The script handles rate limiting and error handling automatically

## Related Files

- `db/migrations/1007_instrument_definitions.sql` - Database schema
- `scripts/database/download_instrument_definitions.py` - Download script
- `docs/DATABENTO_DEFINITIONS.md` - DataBento definition schema documentation

