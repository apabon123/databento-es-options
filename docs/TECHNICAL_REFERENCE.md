# Technical Reference

Deep dive into database schema, data architecture, roll strategies, and advanced topics.

---

## Table of Contents

1. [Database Schema](#database-schema)
2. [Data Architecture](#data-architecture)
3. [Roll Strategies](#roll-strategies)
4. [Instrument Definitions](#instrument-definitions)
5. [Sharing Data with Other Projects](#sharing-data-with-other-projects)
6. [Adding New Products](#adding-new-products)

---

## Database Schema

The database is stored at `data/silver/market.duckdb`.

### Core Tables by Product

#### Continuous Daily OHLCV

| Table | Description |
|-------|-------------|
| `dim_continuous_contract` | Contract series definitions (ES_FRONT_CALENDAR_2D, etc.) |
| `g_continuous_bar_daily` | Daily OHLCV bars for continuous contracts |

```sql
-- Contract series
SELECT contract_series, root, rank, roll_rule, description
FROM dim_continuous_contract;

-- Daily bars
SELECT trading_date, contract_series, open, high, low, close, volume,
       underlying_native_symbol, underlying_instrument_id
FROM g_continuous_bar_daily
WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
ORDER BY trading_date DESC LIMIT 10;
```

#### Continuous Intraday (1-minute)

| Table | Description |
|-------|-------------|
| `dim_continuous_instrument` | Instrument definitions |
| `f_continuous_quote_l1` | Raw level 1 quotes |
| `f_continuous_trade` | Trade data |
| `g_continuous_bar_1m` | 1-minute aggregated bars |

```sql
-- 1-minute bars
SELECT ts_minute, contract_series, o_mid, h_mid, l_mid, c_mid, v_trades
FROM g_continuous_bar_1m
WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
  AND ts_minute >= '2025-10-27'
ORDER BY ts_minute;
```

#### ES Futures (Individual Contracts)

| Table | Description |
|-------|-------------|
| `dim_fut_instrument` | Futures contract definitions |
| `f_fut_quote_l1` | Level 1 quotes |
| `f_fut_trade` | Trade executions |
| `g_fut_bar_1m` | 1-minute bars |

```sql
-- Futures bars
SELECT ts_minute, native_symbol, o_mid, h_mid, l_mid, c_mid
FROM g_fut_bar_1m
WHERE native_symbol LIKE 'ES%'
ORDER BY ts_minute DESC LIMIT 10;
```

#### ES Options

| Table | Description |
|-------|-------------|
| `dim_instrument` | Option contract definitions (strikes, expiries) |
| `f_quote_l1` | Level 1 quotes |
| `f_trade` | Trade executions |
| `g_bar_1m` | 1-minute bars |

```sql
-- Options bars
SELECT ts_minute, symbol, o_mid, h_mid, l_mid, c_mid, o_spread, c_spread
FROM g_bar_1m
WHERE symbol LIKE 'ESZ5 C%'
ORDER BY ts_minute DESC LIMIT 10;
```

#### FRED Macro Data

| Table | Description |
|-------|-------------|
| `dim_fred_series` | FRED series metadata |
| `f_fred_observations` | Daily observations |

```sql
-- Available series
SELECT series_id, name FROM dim_fred_series ORDER BY series_id;

-- Get VIX data
SELECT date, series_id, value
FROM f_fred_observations
WHERE series_id = 'VIXCLS'
  AND date >= '2020-01-01'
ORDER BY date;
```

#### Instrument Definitions

| Table | Description |
|-------|-------------|
| `dim_instrument_definition` | Full contract specs from DataBento |
| `v_instrument_definition_latest` | View: latest definition per instrument |

```sql
-- Contract specifications
SELECT instrument_id, native_symbol, asset, expiration,
       min_price_increment, min_price_increment_amount,
       contract_multiplier, currency
FROM v_instrument_definition_latest
WHERE asset = 'SR3'
ORDER BY expiration;
```

---

## Data Architecture

### Bronze-Silver-Gold Pattern

```
DataBento API / FRED API
        ↓
data/raw/           (Bronze) - Raw downloads, transformed folders
        ↓
data/silver/market.duckdb  (Silver) - Normalized tables
        ↓
data/gold/          (Gold) - Aggregated outputs (Parquet mirrors)
```

### Raw Data Folder Structure

After download and transformation:

```
data/raw/
├── ohlcv-1d/
│   ├── downloads/
│   │   └── es/
│   │       └── calendar-2d/
│   │           └── glbx-mdp3-es-2025-01-01.ohlcv-1d.fullday.parquet
│   └── transformed/
│       └── es/
│           └── calendar-2d/
│               └── 2025-01-01/
│                   └── continuous_bars_daily/2025-01-01.parquet
├── bbo-1m/
│   ├── downloads/
│   │   └── calendar-2d/
│   │       └── glbx-mdp3-2025-01-01.bbo-1m.fullday.parquet
│   └── transformed/
│       └── calendar-2d/
│           └── 2025-01-01/
│               ├── continuous_quotes_l1/
│               └── continuous_trades/
└── glbx-mdp3-YYYY-MM-DD/        (Legacy: options/futures)
    ├── instruments/
    ├── quotes_l1/
    ├── fut_instruments/
    ├── fut_quotes_l1/
    └── trades/
```

### Data Flow

```
1. DOWNLOAD
   DataBento API → DBN/Parquet files in data/raw/

2. TRANSFORM
   Raw files → Folder structure (instruments/, quotes_l1/, trades/)

3. INGEST
   Folder structure → DuckDB tables (dim_*, f_*)

4. BUILD GOLD
   Fact tables → Aggregated tables (g_*_bar_1m, g_*_bar_daily)

5. VALIDATE
   Run sanity checks on row counts and data quality
```

---

## Roll Strategies

### Overview

The project supports multiple roll strategies for continuous contracts. Each strategy is isolated in the file system and database.

### Available Strategies

| Strategy | DataBento Code | Description |
|----------|----------------|-------------|
| `calendar-2d` | `.c.` | 2-day pre-expiry calendar roll (default) |
| `volume` | `.v.` | Volume-based roll |

### Contract Series Naming

**Format**: `{ROOT}_FRONT_{ROLL_STRATEGY}`

**Examples**:
- `ES_FRONT_CALENDAR_2D` - ES front month, 2-day pre-expiry calendar roll
- `NQ_FRONT_CALENDAR_2D` - NQ front month, 2-day pre-expiry calendar roll
- `SR3_0_CALENDAR_2D` - SR3 front month (rank 0)
- `SR3_1_CALENDAR_2D` - SR3 second month (rank 1)

### Database Schema

```sql
-- Contract series definitions
CREATE TABLE dim_continuous_contract (
    contract_series VARCHAR PRIMARY KEY,  -- 'ES_FRONT_CALENDAR_2D'
    root VARCHAR,                          -- 'ES'
    rank INTEGER,                          -- 0 = front month
    roll_rule VARCHAR,                     -- '2_days_pre_expiry'
    adjustment_method VARCHAR,             -- 'unadjusted'
    description VARCHAR
);

-- Daily bars include underlying info
CREATE TABLE g_continuous_bar_daily (
    trading_date DATE,
    contract_series VARCHAR,
    open DECIMAL, high DECIMAL, low DECIMAL, close DECIMAL,
    volume BIGINT,
    underlying_native_symbol VARCHAR,      -- 'ESZ5'
    underlying_instrument_id INTEGER,
    PRIMARY KEY (trading_date, contract_series)
);
```

### Querying Different Roll Strategies

```sql
-- Get ES data with 2-day calendar roll
SELECT trading_date, open, high, low, close, volume
FROM g_continuous_bar_daily
WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
ORDER BY trading_date;

-- See all contract series available
SELECT contract_series, root, roll_rule
FROM dim_continuous_contract
ORDER BY root, roll_rule;
```

### Adding a New Roll Strategy

1. **Update config** (`configs/download_universe.yaml`):
   ```yaml
   ES:
     roll_rule: .v.  # Change to volume roll
   ```

2. **Download with new strategy**:
   ```powershell
   python scripts/download/download_universe_daily_ohlcv.py --roots ES --start 2020-01-01
   ```

3. **New contract series created**: `ES_FRONT_VOLUME`

---

## Instrument Definitions

### What's Available

DataBento's `definition` schema provides comprehensive contract specifications:

| Field | Description | Example |
|-------|-------------|---------|
| `min_price_increment` | Tick size | 0.2500 (ES), 0.0025 (SR3) |
| `min_price_increment_amount` | Dollar value per tick | $12.50 (ES) |
| `expiration` | Expiration date/time | 2025-03-21 13:30:00 |
| `maturity_year/month/day` | Maturity info | 2025, 3, 21 |
| `contract_multiplier` | Multiplier | 50 (ES) |
| `currency` | Currency code | USD |
| `high_limit_price`, `low_limit_price` | Price limits | |
| `min_trade_vol`, `max_trade_vol` | Volume limits | |

### Download Definitions

```powershell
# Download for all instruments in database
python scripts/database/download_instrument_definitions.py --all

# Download for specific root
python scripts/database/download_instrument_definitions.py --root SR3

# Show summary
python scripts/database/download_instrument_definitions.py --summary
```

### Use Cases

**Curve Building** (expiry dates):
```sql
SELECT native_symbol, expiration, maturity_year, maturity_month
FROM v_instrument_definition_latest
WHERE asset = 'SR3'
ORDER BY expiration;
```

**Contract Specifications** (tick size, multiplier):
```sql
SELECT native_symbol, min_price_increment, min_price_increment_amount,
       contract_multiplier, currency
FROM v_instrument_definition_latest
WHERE asset = 'ES';
```

**Roll Date Tracking**:
```sql
SELECT d.native_symbol, d.expiration,
       MAX(b.trading_date) as last_trading_date
FROM v_instrument_definition_latest d
JOIN g_continuous_bar_daily b 
  ON d.instrument_id = b.underlying_instrument_id
WHERE d.asset = 'SR3'
GROUP BY d.native_symbol, d.expiration
ORDER BY d.expiration;
```

---

## Sharing Data with Other Projects

### Option 1: Read-Only Access (Recommended)

Other projects connect to this database in read-only mode:

```python
import duckdb
from pathlib import Path

# Point to this project's database
DB_PATH = Path(r"C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb")

# Connect read-only
con = duckdb.connect(str(DB_PATH), read_only=True)

# Query data
df = con.execute("""
    SELECT trading_date, contract_series, close
    FROM g_continuous_bar_daily
    WHERE contract_series = 'ES_FRONT_CALENDAR_2D'
    ORDER BY trading_date DESC LIMIT 100
""").fetchdf()

con.close()
```

**In other projects' `.env`**:
```env
MARKET_DATA_DB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb
```

### Option 2: Centralized Database Location

Move database to a shared location:

```powershell
# Create shared location
New-Item -ItemType Directory -Path "C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver" -Force

# Move database
Move-Item `
  -Path ".\data\silver\market.duckdb" `
  -Destination "C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver\market.duckdb"
```

Update `.env` in this project and others:
```env
DUCKDB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver\market.duckdb
```

### Querying Market Data from Other Projects

```python
def get_market_data(contract_series="ES_FRONT_CALENDAR_2D", start_date=None, end_date=None):
    """Query market data from the central database."""
    import duckdb
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    db_path = os.getenv("MARKET_DATA_DB_PATH", 
        r"C:\...\databento-es-options\data\silver\market.duckdb")
    
    con = duckdb.connect(db_path, read_only=True)
    
    query = f"""
        SELECT trading_date, open, high, low, close, volume
        FROM g_continuous_bar_daily
        WHERE contract_series = '{contract_series}'
    """
    if start_date:
        query += f" AND trading_date >= '{start_date}'"
    if end_date:
        query += f" AND trading_date <= '{end_date}'"
    query += " ORDER BY trading_date"
    
    df = con.execute(query).fetchdf()
    con.close()
    return df
```

### Querying FRED Data

```python
def get_fred_data(series_id="VIXCLS", start_date=None):
    """Query FRED data from the database."""
    con = duckdb.connect(DB_PATH, read_only=True)
    
    query = f"""
        SELECT date, value
        FROM f_fred_observations
        WHERE series_id = '{series_id}'
    """
    if start_date:
        query += f" AND date >= '{start_date}'"
    query += " ORDER BY date"
    
    df = con.execute(query).fetchdf()
    con.close()
    return df
```

### Best Practices

1. **Read-Only Mode**: Always use `read_only=True` from other projects
2. **Single Writer**: Only this project should write to the database
3. **Path Configuration**: Use environment variables
4. **Error Handling**: Handle connection errors gracefully

---

## Adding New Products

### 1. Create Schema Migration

Add a new migration file: `db/migrations/10xx_your_product.sql`

```sql
-- Example: New product tables
CREATE TABLE IF NOT EXISTS dim_your_instrument (...);
CREATE TABLE IF NOT EXISTS f_your_quote_l1 (...);
CREATE TABLE IF NOT EXISTS g_your_bar_1m (...);
```

### 2. Register in Schema Registry

Edit `config/schema_registry.yml`:

```yaml
YOUR_PRODUCT_MDP3:
  inputs:
    instruments: "*/your_instruments/*.parquet"
    quotes: "*/your_quotes_l1/*.parquet"
  migrations:
    - 10xx_your_product.sql
  loader: your_product
  gold_sql: |
    INSERT INTO g_your_bar_1m ...
```

### 3. Create Loader

Add `pipelines/products/your_product.py`:

```python
from pipelines.loader import GenericLoader

class YourProductLoader(GenericLoader):
    def load_instruments(self, df):
        # Custom loading logic
        pass
```

### 4. Run Pipeline

```powershell
python orchestrator.py migrate
python orchestrator.py ingest --product YOUR_PRODUCT_MDP3 --source data/raw/your-data
python orchestrator.py build --product YOUR_PRODUCT_MDP3
python orchestrator.py validate --product YOUR_PRODUCT_MDP3
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABENTO_API_KEY` | DataBento API key | (required) |
| `FRED_API_KEY` | FRED API key | (optional) |
| `DATA_BRONZE_ROOT` | Raw data folder | `./data/raw` |
| `DATA_GOLD_ROOT` | Gold data folder | `./data/gold` |
| `DUCKDB_PATH` | Database file path | `./data/silver/market.duckdb` |

Configure in `.env`:
```env
DATABENTO_API_KEY=your_key_here
FRED_API_KEY=your_fred_key_here
DATA_BRONZE_ROOT=./data/raw
DATA_GOLD_ROOT=./data/gold
DUCKDB_PATH=./data/silver/market.duckdb
```

