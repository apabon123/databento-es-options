# Data Sources Summary

Complete list of all data sources pulled into the database and their sources.

## Overview

| Data Category | Source | Script | Database Table(s) |
|--------------|--------|--------|-------------------|
| **ES Options** | DataBento | `download_and_ingest_options.py` | `dim_instrument`, `f_quote_l1`, `f_trade`, `g_bar_1m` |
| **ES Futures** | DataBento | `download_and_ingest_futures.py` | `dim_fut_instrument`, `f_fut_quote_l1`, `f_fut_trade`, `g_fut_bar_1m` |
| **Continuous Futures (Daily)** | DataBento | `download_universe_daily_ohlcv.py` | `dim_continuous_contract`, `g_continuous_bar_daily` |
| **Continuous Futures (Intraday)** | DataBento | `download_and_ingest_continuous.py` | `dim_continuous_instrument`, `f_continuous_quote_l1`, `f_continuous_trade`, `g_continuous_bar_1m` |
| **FRED Macro Series** | FRED API | `download_fred_series.py` → `ingest_fred_series.py` | `dim_fred_series`, `f_fred_observations` |
| **VX Futures & Volatility Indices** | CBOE → financial-data-system | `sync_vix_vx_from_financial_data_system.py` | `market_data`, `market_data_cboe` |
| **Instrument Definitions** | DataBento | `download_instrument_definitions.py` | `dim_instrument_definition` |

---

## 1. DataBento Sources

### 1.1 ES Options (BBO-1m)

**Source:** DataBento API  
**Script:** `scripts/download/download_and_ingest_options.py`  
**Schema:** `bbo-1m`  
**Coverage:** Last N weeks (configurable)

**Database Tables:**
- `dim_instrument` - Option contract definitions (strikes, expiries, Greeks)
- `f_quote_l1` - Level 1 bid/ask quotes
- `f_trade` - Trade executions
- `g_bar_1m` - 1-minute aggregated bars (OHLCV + spreads)

**Usage:**
```powershell
python scripts/download/download_and_ingest_options.py --weeks 3
```

---

### 1.2 ES Futures (BBO-1m)

**Source:** DataBento API  
**Script:** `scripts/download/download_and_ingest_futures.py`  
**Schema:** `bbo-1m`  
**Coverage:** Last N weeks (configurable)

**Database Tables:**
- `dim_fut_instrument` - Futures contract definitions
- `f_fut_quote_l1` - Level 1 bid/ask quotes
- `f_fut_trade` - Trade executions
- `g_fut_bar_1m` - 1-minute aggregated bars

**Usage:**
```powershell
python scripts/download/download_and_ingest_futures.py --weeks 3
```

---

### 1.3 Continuous Futures (Daily OHLCV)

**Source:** DataBento API  
**Script:** `scripts/download/download_universe_daily_ohlcv.py`  
**Schema:** `ohlcv-1d`  
**Config:** `configs/download_universe.yaml`

**Products Included:**

#### Equity Index Futures
- **ES** (S&P 500 e-mini) - Calendar roll, ranks 0-1
- **NQ** (Nasdaq-100 e-mini) - Calendar roll, ranks 0-1
- **RTY** (Russell 2000 e-mini) - Calendar roll, ranks 0-1

#### Interest Rate Futures
- **ZT** (2Y Treasury note) - Volume roll, ranks 0-1
- **ZF** (5Y Treasury note) - Volume roll, ranks 0-1
- **ZN** (10Y Treasury note) - Volume roll, ranks 0-1
- **UB** (Ultra-bond 30Y+) - Volume roll, ranks 0-1

#### Commodity Futures
- **CL** (WTI crude oil) - Volume roll, ranks 0-3
- **GC** (COMEX gold) - Volume roll, ranks 0-2

#### FX Futures
- **6E** (EUR/USD) - Calendar roll, ranks 0-1
- **6J** (JPY/USD) - Calendar roll, ranks 0-1
- **6B** (GBP/USD) - Calendar roll, ranks 0-1

#### STIR Futures
- **SR3** (3M SOFR) - Calendar roll, ranks 0-12 (full curve)

#### Volatility Futures (Optional)
- **VX** (VIX futures) - Calendar roll, ranks 0-2

**Database Tables:**
- `dim_continuous_contract` - Contract series definitions
- `g_continuous_bar_daily` - Daily OHLCV bars

**Usage:**
```powershell
# Download all configured products
python scripts/download/download_universe_daily_ohlcv.py --start 2020-01-01 --end 2025-12-31

# Download specific roots
python scripts/download/download_universe_daily_ohlcv.py --roots ES,NQ,ZN --start 2020-01-01
```

---

### 1.4 Continuous Futures (Intraday 1-minute)

**Source:** DataBento API  
**Script:** `scripts/download/download_and_ingest_continuous.py`  
**Schema:** `bbo-1m`  
**Coverage:** Configurable date ranges

**Database Tables:**
- `dim_continuous_instrument` - Instrument definitions
- `f_continuous_quote_l1` - Level 1 quotes
- `f_continuous_trade` - Trade data
- `g_continuous_bar_1m` - 1-minute aggregated bars

**Usage:**
```powershell
python scripts/download/download_and_ingest_continuous.py --start 2025-01-01 --end 2025-01-31
```

---

### 1.5 Instrument Definitions

**Source:** DataBento API  
**Script:** `scripts/database/download_instrument_definitions.py`  
**Purpose:** Download contract specifications (expiration, multiplier, tick size, etc.)

**Database Tables:**
- `dim_instrument_definition` - Full contract specs from DataBento
- `v_instrument_definition_latest` - View: latest definition per instrument

**Usage:**
```powershell
# Download for all instruments in database
python scripts/database/download_instrument_definitions.py --all

# Download for specific root
python scripts/database/download_instrument_definitions.py --root ES
```

---

## 2. FRED Macro Data

**Source:** FRED (Federal Reserve Economic Data) API  
**Script:** `scripts/download/download_fred_series.py` → `scripts/database/ingest_fred_series.py`  
**Config:** `configs/fred_series.yaml`

### FRED Series Included

#### Volatility Indices
- **VIXCLS** - CBOE Volatility Index (VIX) - 1-month
- **VXVCLS** - CBOE 3-Month Volatility Index (VXV) - *Note: VIX3M preferred from CBOE*

#### Interest Rates
- **FEDFUNDS** - Effective Federal Funds Rate
- **DGS2** - Treasury Constant Maturity Rate - 2 Year
- **DGS5** - Treasury Constant Maturity Rate - 5 Year
- **DGS10** - Treasury Constant Maturity Rate - 10 Year
- **DGS30** - Treasury Constant Maturity Rate - 30 Year

#### Credit Spreads
- **BAMLH0A0HYM2** - ICE BofA US High Yield Index Option-Adjusted Spread
- **BAMLC0A0CM** - ICE BofA US Corporate Index Option-Adjusted Spread

#### Economic Indicators
- **TEDRATE** - TED Spread (3M LIBOR - 3M T-Bill)
- **CPIAUCSL** - Consumer Price Index for All Urban Consumers
- **UNRATE** - Unemployment Rate
- **DTWEXBGS** - Trade Weighted U.S. Dollar Index: Broad, Goods

#### Yield Curve & Inflation
- **T10Y2Y** - 10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity
- **T10YIE** - 10-Year Breakeven Inflation Rate
- **T5YIFR** - 5-Year, 5-Year Forward Inflation Expectation Rate

**Database Tables:**
- `dim_fred_series` - FRED series metadata
- `f_fred_observations` - Daily observations (date, series_id, value)

**Usage:**
```powershell
# Download all configured series
python scripts/download/download_fred_series.py

# Download specific series
python scripts/download/download_fred_series.py --series VIXCLS,FEDFUNDS

# Download and ingest in one step
python scripts/download/download_fred_series.py --ingest
```

---

## 3. CBOE Volatility Data (via financial-data-system)

**Source:** CBOE → financial-data-system database  
**Script:** `scripts/database/sync_vix_vx_from_financial_data_system.py`  
**Purpose:** Sync volatility futures and indices from external financial-data-system database

### Data Synced

#### VX Futures (Continuous Contracts)
- **@VX=101XN** - Front month (VX1) - Unadjusted, continuous, 1-day roll
- **@VX=201XN** - 2nd month (VX2) - Unadjusted, continuous, 1-day roll
- **@VX=301XN** - 3rd month (VX3) - Unadjusted, continuous, 1-day roll

**Coverage:** 2004-03-26 to present

#### Volatility Indices
- **VIX3M** - CBOE 3-Month Volatility Index (formerly VXV)  
  **Coverage:** 2009-09-18 to present  
  **Rationale:** FRED coverage insufficient; CBOE is authoritative source

- **VVIX** - CBOE VIX Volatility Index (Vol-of-Vol)  
  **Coverage:** 2006-03-06 to present  
  **Rationale:** Not available via FRED API

**Database Tables:**
- `market_data` - VX futures continuous contracts
- `market_data_cboe` - VIX3M and VVIX indices

**Usage:**
```powershell
# Sync from financial-data-system (requires FIN_DB_PATH in .env)
python scripts/database/sync_vix_vx_from_financial_data_system.py

# Force re-sync
python scripts/database/sync_vix_vx_from_financial_data_system.py --force
```

**Note:** VIX (1M) is **NOT** synced here - use FRED as primary source (see FRED section above).

---

## Data Source Policy

### Single Source of Truth

Each data series has exactly **one authoritative source**:

| Data Type | Source | Reason |
|-----------|--------|--------|
| **VIX Index (1M)** | FRED | Clean, stable historical data; consistent with other macro series |
| **VX Futures (VX1/2/3)** | CBOE → financial-data-system | Proper roll logic maintained; TradeStation continuous contracts |
| **VIX3M Index (3M)** | CBOE → financial-data-system | FRED coverage insufficient; CBOE is authoritative |
| **VVIX Index** | CBOE → financial-data-system | Not available via FRED API |

See [SOT/DATA_SOURCE_POLICY.md](SOT/DATA_SOURCE_POLICY.md) for detailed policy.

---

## Database Storage

All data is stored in: **`data/silver/market.duckdb`**

### Table Organization

- **`dim_*`** - Dimension tables (instruments, contracts, series metadata)
- **`f_*`** - Fact tables (raw quotes, trades, observations)
- **`g_*`** - Gold/aggregated tables (bars, daily summaries)
- **`v_*`** - Views (latest definitions, summaries)

---

## Update Workflows

### Daily/Weekly Updates

```powershell
# 1. ES Options & Futures (last week)
python scripts/download/download_and_ingest_options.py --weeks 1
python scripts/download/download_and_ingest_futures.py --weeks 1

# 2. FRED macro series (weekly)
python scripts/download/download_fred_series.py --ingest

# 3. VX/VIX3M from financial-data-system (after updating source DB)
python scripts/database/sync_vix_vx_from_financial_data_system.py
```

### Historical Backfills

```powershell
# Continuous daily OHLCV (all products)
python scripts/download/download_universe_daily_ohlcv.py --start 2020-01-01 --end 2025-12-31

# Specific products
python scripts/download/download_universe_daily_ohlcv.py --roots ES,NQ,ZN --start 2015-01-01
```

---

## Related Documentation

- [SOT/DATA_SOURCE_POLICY.md](SOT/DATA_SOURCE_POLICY.md) - Authoritative policy for volatility data sources
- [SOT/DATA_ARCHITECTURE.md](SOT/DATA_ARCHITECTURE.md) - Database architecture and organization
- [SOT/UPDATE_WORKFLOWS.md](SOT/UPDATE_WORKFLOWS.md) - Update procedures and maintenance
- [SOT/INTEROP_CONTRACT.md](SOT/INTEROP_CONTRACT.md) - Guaranteed tables and series for downstream systems
- [TECHNICAL_REFERENCE.md](TECHNICAL_REFERENCE.md) - Complete schema reference
- [QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Command reference and workflows
- [scripts/database/README.md](../scripts/database/README.md) - Database scripts documentation
