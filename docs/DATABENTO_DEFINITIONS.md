# DataBento Instrument Definitions Schema

## Overview

DataBento provides a **`definition` schema** that contains comprehensive contract specifications for futures contracts, including expiry dates, tick sizes, multipliers, and other metadata. This is **directly downloadable** from DataBento, not derived from symbols.

## Available Schema

**Schema Name:** `definition`

**Dataset:** `GLBX.MDP3` (CME Globex)

## Contract Specifications Available

Based on testing, the definition schema provides the following contract specifications:

### Key Fields

| Field | Description | Example (SR3H1) | Example (ESH1) |
|-------|-------------|-----------------|----------------|
| `min_price_increment` | Tick size | 0.0025 | 0.2500 |
| `expiration` | Expiration date/time | 2021-06-15 21:00:00+00:00 | 2021-03-19 13:30:00+00:00 |
| `min_price_increment_amount` | Dollar value per tick | 0.0625 | 0.1250 |
| `contract_multiplier` | Contract multiplier | (varies) | (varies) |
| `currency` | Currency code | USD | USD |
| `unit_of_measure` | Unit of measure | USD | IPNT |
| `unit_of_measure_qty` | Unit of measure quantity | 2500.0 | 50.0 |
| `original_contract_size` | Original contract size | (varies) | (varies) |
| `maturity_year` | Maturity year | 2021 | 2021 |
| `maturity_month` | Maturity month | 6 | 3 |
| `maturity_day` | Maturity day | 15 | 19 |
| `min_lot_size` | Minimum lot size | 0 | 0 |
| `min_trade_vol` | Minimum trade volume | 1 | 1 |
| `max_trade_vol` | Maximum trade volume | 30000 | 3000 |
| `high_limit_price` | High limit price | 100.4525 | 4075.5000 |
| `low_limit_price` | Low limit price | 99.4525 | 3542.5000 |
| `trading_reference_price` | Trading reference price | 99.9525 | 3809.2500 |
| `activation` | Activation date | 2018-04-20 21:30:00+00:00 | 2019-12-20 14:30:00+00:00 |

### Additional Metadata Fields

- `raw_symbol` - Native symbol (e.g., "SR3H1", "ESH1")
- `instrument_id` - DataBento instrument ID
- `security_update_action` - Security update action
- `instrument_class` - Instrument class (e.g., "F" for futures)
- `display_factor` - Display factor
- `underlying_id` - Underlying instrument ID
- `exchange` - Exchange code (e.g., "XCME")
- `asset` - Asset code (e.g., "SR3", "ES")
- `security_type` - Security type (e.g., "FUT")
- `cfi` - Classification of Financial Instruments code
- `match_algorithm` - Matching algorithm
- `md_security_trading_status` - Market data security trading status
- And many more...

## Usage Example

```python
import databento as db
from datetime import date

client = db.Historical(api_key=api_key)

# Get definitions for specific symbols
def_data = client.timeseries.get_range(
    dataset="GLBX.MDP3",
    schema="definition",
    stype_in="native",
    symbols=["SR3H1", "ESH1"],  # March 2021 contracts
    start=date(2021, 3, 1),
    end=date(2021, 3, 2),
)

df = def_data.to_df()

# Access contract specifications
print(df[['raw_symbol', 'min_price_increment', 'expiration', 'contract_multiplier', 'currency']])
```

## Key Benefits

1. **Direct Download**: Contract specifications are available directly from DataBento, no need to derive from symbols
2. **Comprehensive**: Includes expiry dates, tick sizes, multipliers, and many other contract details
3. **Time-Series**: Definitions are available as time-series data, allowing you to track changes over time
4. **Point-in-Time**: Can query definitions for specific dates to get contract specifications as they existed at that time

## Notes

- Some fields may have sentinel values (e.g., `2147483647` for `contract_multiplier`) which should be handled appropriately
- Definitions are available for the date range where data is available (may require subscription/license)
- The `expiration` field provides the exact expiration date/time, which is particularly useful for SOFR futures and other contracts with IMM dates
- The `min_price_increment` field provides the tick size, which is essential for curve building and pricing calculations

## Use Cases

1. **Curve Building**: Use `expiration` dates to build forward rate curves for SOFR futures
2. **Contract Specifications**: Download and store contract specs (tick size, multiplier) for all futures
3. **Roll Date Tracking**: Use expiration dates to determine when contracts roll
4. **Pricing Calculations**: Use `min_price_increment` and `min_price_increment_amount` for pricing calculations

## Implementation

To download and store definitions:

1. Query the `definition` schema for all symbols of interest
2. Store definitions in a database table (e.g., `dim_instrument_definition`)
3. Link definitions to instrument IDs for easy lookup
4. Update definitions periodically to track changes over time

## Related Files

- `src/utils/instrument_metadata.py` - Utilities for parsing and storing instrument metadata
- `scripts/database/populate_instrument_metadata.py` - Script to populate instrument metadata (currently uses symbology API, could be enhanced to use definition schema)
- `db/migrations/1006_instrument_metadata.sql` - Database schema for instrument metadata

