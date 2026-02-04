# Data Architecture

Bronze-Silver-Gold architecture and database organization for the canonical research database.

---

## Bronze-Silver-Gold Pattern

```
DataBento API / FRED API
        ↓
data/raw/           (Bronze) - Raw downloads, transformed folders
        ↓
data/silver/market.duckdb  (Silver) - Normalized tables
        ↓
data/gold/          (Gold) - Aggregated outputs (Parquet mirrors)
```

### Bronze Layer (Raw Data)

**Location:** `data/raw/`

Raw downloads and transformed folder structures:

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

### Silver Layer (Normalized Database)

**Location:** `data/silver/market.duckdb`

Normalized DuckDB database with organized tables:

#### Table Naming Conventions

- **`dim_*`** - Dimension tables (instruments, contracts, series metadata)
- **`f_*`** - Fact tables (raw quotes, trades, observations)
- **`g_*`** - Gold/aggregated tables (bars, daily summaries)
- **`v_*`** - Views (latest definitions, summaries)

#### Core Tables by Product

**Continuous Daily OHLCV:**
- `dim_continuous_contract` - Contract series definitions
- `g_continuous_bar_daily` - Daily OHLCV bars

**Continuous Intraday (1-minute):**
- `dim_continuous_instrument` - Instrument definitions
- `f_continuous_quote_l1` - Raw level 1 quotes
- `f_continuous_trade` - Trade data
- `g_continuous_bar_1m` - 1-minute aggregated bars

**ES Futures (Individual Contracts):**
- `dim_fut_instrument` - Futures contract definitions
- `f_fut_quote_l1` - Level 1 quotes
- `f_fut_trade` - Trade executions
- `g_fut_bar_1m` - 1-minute bars

**ES Options:**
- `dim_instrument` - Option contract definitions (strikes, expiries)
- `f_quote_l1` - Level 1 quotes
- `f_trade` - Trade executions
- `g_bar_1m` - 1-minute bars

**FRED Macro Data:**
- `dim_fred_series` - FRED series metadata
- `f_fred_observations` - Daily observations

**Volatility Data (CBOE):**
- `market_data` - VX futures continuous contracts
- `market_data_cboe` - VIX3M and VVIX indices

**Instrument Definitions:**
- `dim_instrument_definition` - Full contract specs from DataBento
- `v_instrument_definition_latest` - View: latest definition per instrument

### Gold Layer (Aggregated Outputs)

**Location:** `data/gold/`

Parquet mirrors and aggregated outputs for downstream consumption.

---

## Data Flow

```
1. DOWNLOAD
   DataBento API / FRED API → DBN/Parquet files in data/raw/

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

## Database Schema Details

### Contract Series Naming

**Format**: `{ROOT}_{RANK}_{ROLL_STRATEGY}` or `{ROOT}_FRONT_{ROLL_STRATEGY}`

**Examples**:
- `ES_FRONT_CALENDAR_2D` - ES front month, 2-day pre-expiry calendar roll
- `NQ_FRONT_CALENDAR_2D` - NQ front month, 2-day pre-expiry calendar roll
- `SR3_0_CALENDAR_2D` - SR3 front month (rank 0)
- `SR3_1_CALENDAR_2D` - SR3 second month (rank 1)

### Roll Strategies

| Strategy | DataBento Code | Description |
|----------|----------------|-------------|
| `calendar-2d` | `.c.` | 2-day pre-expiry calendar roll (default) |
| `volume` | `.v.` | Volume-based roll |

### Continuous Contract Schema

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

---

## Access Patterns

### Read-Only Access (Recommended for Downstream Projects)

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

### Environment Configuration

**In other projects' `.env`:**
```env
MARKET_DATA_DB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb
```

---

## Best Practices

1. **Read-Only Mode**: Always use `read_only=True` from other projects
2. **Single Writer**: Only this project should write to the database
3. **Path Configuration**: Use environment variables
4. **Error Handling**: Handle connection errors gracefully
5. **Table Naming**: Follow `dim_*`, `f_*`, `g_*`, `v_*` conventions

---

## Related Documentation

- [DATA_SOURCE_POLICY.md](./DATA_SOURCE_POLICY.md) - Authoritative data sources
- [INTEROP_CONTRACT.md](INTEROP_CONTRACT.md) - Guaranteed tables and series
- [UPDATE_WORKFLOWS.md](UPDATE_WORKFLOWS.md) - Update procedures
- [TECHNICAL_REFERENCE.md](../TECHNICAL_REFERENCE.md) - Complete schema reference
