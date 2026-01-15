# Interoperability Contract

**Authoritative specification of what this repository guarantees for downstream systems.**

This document defines the contract between `databento-es-options` (the canonical data repository) and downstream systems (e.g., `Futures-Six`). It specifies what data is guaranteed to exist, what series are authoritative, and what downstream systems must compute themselves.

---

## Guaranteed Tables

The following tables are **guaranteed to exist** in `data/silver/market.duckdb` and are **stable** (schema will not change without notice):

### Continuous Futures (Daily OHLCV)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `dim_continuous_contract` | Contract series definitions | `contract_series`, `root`, `rank`, `roll_rule` |
| `g_continuous_bar_daily` | Daily OHLCV bars | `trading_date`, `contract_series`, `open`, `high`, `low`, `close`, `volume` |

**Query Example:**
```sql
SELECT trading_date, contract_series, open, high, low, close, volume
FROM g_continuous_bar_daily
WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
  AND trading_date BETWEEN '2020-01-01' AND '2025-12-31'
ORDER BY trading_date;
```

### FRED Macro Data

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `dim_fred_series` | FRED series metadata | `series_id`, `name` |
| `f_fred_observations` | Daily observations | `date`, `series_id`, `value` |

**Query Example:**
```sql
SELECT date, value as vix_close
FROM f_fred_observations
WHERE series_id = 'VIXCLS'
  AND date BETWEEN '2020-01-01' AND '2025-12-31'
ORDER BY date;
```

### Volatility Data (CBOE)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `market_data` | VX futures continuous contracts | `timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume` |
| `market_data_cboe` | VIX3M and VVIX indices | `timestamp`, `symbol`, `open`, `high`, `low`, `close` |

**Query Example:**
```sql
-- VX Futures
SELECT CAST(timestamp AS DATE) AS date, close AS vx1_close
FROM market_data
WHERE symbol = '@VX=101XN'
  AND CAST(timestamp AS DATE) BETWEEN '2020-01-01' AND '2025-12-31'
ORDER BY date;

-- VIX3M Index
SELECT CAST(timestamp AS DATE) AS date, close AS vix3m_close
FROM market_data_cboe
WHERE symbol = 'VIX3M'
  AND CAST(timestamp AS DATE) BETWEEN '2020-01-01' AND '2025-12-31'
ORDER BY date;
```

### Instrument Definitions

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `dim_instrument_definition` | Full contract specs | `instrument_id`, `native_symbol`, `expiration`, `contract_multiplier`, `min_price_increment` |
| `v_instrument_definition_latest` | View: latest definition per instrument | Same as above |

---

## Authoritative Series

The following series are **authoritative** and guaranteed to be available. Each series has exactly **one source** (see [DATA_SOURCE_POLICY.md](./DATA_SOURCE_POLICY.md)):

### Volatility Indices

| Series | Source | Table | Symbol/Series ID | Coverage |
|--------|--------|-------|------------------|----------|
| **VIX (1M)** | FRED | `f_fred_observations` | `VIXCLS` | 2020-01-02 to present |
| **VIX3M (3M)** | CBOE → financial-data-system | `market_data_cboe` | `VIX3M` | 2009-09-18 to present |
| **VVIX** | CBOE → financial-data-system | `market_data_cboe` | `VVIX` | 2006-03-06 to present |

### VX Futures (Continuous Contracts)

| Series | Source | Table | Symbol | Coverage |
|--------|--------|-------|--------|----------|
| **VX1** (Front month) | CBOE → financial-data-system | `market_data` | `@VX=101XN` | 2004-03-26 to present |
| **VX2** (2nd month) | CBOE → financial-data-system | `market_data` | `@VX=201XN` | 2004-03-26 to present |
| **VX3** (3rd month) | CBOE → financial-data-system | `market_data` | `@VX=301XN` | 2004-03-26 to present |

**Note:** These are unadjusted, continuous, 1-day roll contracts (TradeStation convention).

### Continuous Futures

All products defined in `configs/download_universe.yaml` are available with contract series naming: `{ROOT}_{RANK}_{ROLL_STRATEGY}` or `{ROOT}_FRONT_{ROLL_STRATEGY}`.

**Examples:**
- `ES_FRONT_CALENDAR_2D` - ES front month, 2-day pre-expiry calendar roll
- `NQ_FRONT_CALENDAR_2D` - NQ front month
- `ZN_FRONT_VOLUME` - ZN front month, volume roll
- `SR3_0_CALENDAR_2D` - SR3 front month (rank 0)
- `SR3_1_CALENDAR_2D` - SR3 second month (rank 1)

### FRED Macro Series

All series in `configs/fred_series.yaml` are available via `f_fred_observations`:

- **Volatility:** VIXCLS, VXVCLS
- **Rates:** FEDFUNDS, DGS2, DGS5, DGS10, DGS30
- **Spreads:** BAMLH0A0HYM2, BAMLC0A0CM, TEDRATE
- **Economic:** CPIAUCSL, UNRATE, DTWEXBGS
- **Yield Curve:** T10Y2Y, T10YIE, T5YIFR

---

## What Downstream Systems May Assume

Downstream systems (e.g., `Futures-Six`) may **assume** the following:

1. **Tables exist** with the schemas described above
2. **Data is available** for the coverage periods specified
3. **Series are authoritative** - no need to cross-reference with other sources
4. **Data is updated** according to the maintenance schedule (see [UPDATE_WORKFLOWS.md](UPDATE_WORKFLOWS.md))
5. **Read-only access** is safe - database is opened with `read_only=True`

---

## What Downstream Systems Must Compute

Downstream systems are **responsible for**:

### Feature Construction

- **Volatility Risk Premium (VRP)** calculations
- **Term structure spreads** (e.g., VIX3M - VIX, VX2 - VX1)
- **Roll yields** and carry calculations
- **Regime indicators** (e.g., contango/backwardation, volatility regimes)
- **Technical indicators** (moving averages, momentum, etc.)
- **Cross-asset relationships** and correlations

### Derived Metrics

- **Implied vs realized volatility** comparisons
- **Volatility term structure** analysis
- **Volatility-of-volatility** metrics beyond VVIX
- **Option Greeks** (if not already computed)
- **Portfolio-level metrics** and aggregations

### Business Logic

- **Regime filters** and classification
- **Signal generation** and trading logic
- **Risk metrics** and position sizing
- **Backtesting** and performance attribution
- **Model training** and inference

---

## Example: VRP Calculation

**This repository provides:**
- VIX (1M) from FRED
- VIX3M (3M) from CBOE
- VX1/2/3 futures from CBOE

**Downstream system must compute:**
```python
# VRP = VIX - Realized Volatility (30-day)
# VRP Term Structure = VIX3M - VIX
# VRP Futures = VIX - VX1

def calculate_vrp(con, start_date, end_date):
    """Downstream system computes VRP metrics."""
    # Load raw data (guaranteed to exist)
    vix = load_vix(con, start_date, end_date)  # From f_fred_observations
    vix3m = load_vix3m(con, start_date, end_date)  # From market_data_cboe
    vx1 = load_vx1(con, start_date, end_date)  # From market_data
    
    # Compute features (downstream responsibility)
    df = vix.merge(vix3m, on='date', how='inner')
    df = df.merge(vx1, on='date', how='inner')
    
    # Feature construction
    df['vix_vx1_spread'] = df['vix_close'] - df['vx1_close']
    df['vix3m_vix_spread'] = df['vix3m_close'] - df['vix_close']
    df['vrp_term_structure'] = df['vix3m_close'] - df['vix_close']
    
    # Regime classification (downstream logic)
    df['contango'] = df['vx1_close'] > df['vix_close']
    df['backwardation'] = df['vix_close'] > df['vx1_close']
    
    return df
```

---

## Database Access

### Connection Pattern

```python
import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Get database path from environment or use default
DB_PATH = os.getenv(
    "MARKET_DATA_DB_PATH",
    r"C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb"
)

# Connect read-only (required for downstream systems)
con = duckdb.connect(str(DB_PATH), read_only=True)

# Query guaranteed tables
df = con.execute("""
    SELECT trading_date, contract_series, close
    FROM g_continuous_bar_daily
    WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
    ORDER BY trading_date DESC LIMIT 100
""").fetchdf()

con.close()
```

### Environment Configuration

**In downstream project `.env`:**
```env
MARKET_DATA_DB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb
```

---

## Versioning & Breaking Changes

### Stable Guarantees

- Table names and core columns will not change
- Series IDs and symbols are stable
- Data coverage will not decrease (only increase)

### Change Notification

If breaking changes are required:
1. **Deprecation period** - old schema maintained for N months
2. **Migration guide** - provided in release notes
3. **Version tagging** - database schema version tracked

### Current Schema Version

**Version:** 1.0  
**Last Updated:** 2025-01-14

---

## Support & Questions

For questions about:
- **Data availability:** Check [DATA_SOURCE_POLICY.md](./DATA_SOURCE_POLICY.md)
- **Update procedures:** See [UPDATE_WORKFLOWS.md](UPDATE_WORKFLOWS.md)
- **Schema details:** See [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md)
- **Technical reference:** See [TECHNICAL_REFERENCE.md](../TECHNICAL_REFERENCE.md)

---

## Summary

| Aspect | Responsibility |
|--------|----------------|
| **Raw data** | This repository (guaranteed) |
| **Feature construction** | Downstream systems |
| **Regime logic** | Downstream systems |
| **Business rules** | Downstream systems |
| **Data updates** | This repository (see UPDATE_WORKFLOWS.md) |
| **Schema stability** | This repository (guaranteed) |

**Key Principle:** This repository provides **authoritative raw data**. Downstream systems are responsible for **all feature engineering and business logic**.
