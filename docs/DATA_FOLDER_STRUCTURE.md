# Data Folder Structure & Data Flow

## Overview

The project uses a **Bronze-Silver-Gold** data architecture pattern:

```
data/
├── raw/          (Bronze Layer) - Raw files from DataBento
├── silver/       (Silver Layer) - DuckDB database
└── gold/         (Gold Layer) - Transformed, organized Parquet files
```

Plus a manual download folder:
```
data/
└── GLBX-*/       - Manual DataBento downloads (unpacked DBN files)
```

---

## Folder Purposes

### 1. `data/raw/` (Bronze Layer)

**What it contains:**
- **Direct downloads from DataBento**: `.parquet` files downloaded via API
- **Transformed folder structure**: Organized Parquet files ready for database ingestion

**Files downloaded from DataBento:**
- `glbx-mdp3-YYYY-MM-DD.bbo-1m.fullday.parquet` - Full trading day data
- `glbx-mdp3-YYYY-MM-DD.bbo-1m.last5m.parquet` - Last 5 minutes of trading day
- `glbx-mdp3-YYYY-MM-DD.bbo-1m.last5m.dbn.zst` - Compressed DBN format (rare)

**Transformed folder structure:**
After transformation, you get organized folders like:
```
data/raw/glbx-mdp3-2025-10-27/
├── continuous_instruments/
│   └── 2025-10-27.parquet      # Contract definitions
├── continuous_quotes_l1/
│   └── 2025-10-27.parquet      # Level 1 quotes (bid/ask)
└── continuous_trades/           # Trade data (empty if no trades)
```

**For ES Futures:**
```
data/raw/glbx-mdp3-2025-10-20/
├── fut_instruments/
│   └── 2025-10-20.parquet
├── fut_quotes_l1/
│   └── 2025-10-20.parquet
└── fut_trades/
```

**For ES Options:**
```
data/raw/glbx-mdp3-YYYY-MM-DD/
├── instruments/
│   └── YYYY-MM-DD.parquet
├── quotes_l1/
│   └── YYYY-MM-DD.parquet
└── trades/
```

---

### 2. `data/silver/` (Silver Layer)

**What it contains:**
- `market.duckdb` - The main DuckDB database file

**Purpose:**
- Stores all ingested market data in normalized tables
- Contains fact tables (quotes, trades) and dimension tables (instruments)
- Stores aggregated "gold layer" tables (1-minute bars)

---

### 3. `data/gold/` (Gold Layer)

**What it contains:**
- **Mirror of `data/raw/` structure**: Same folder organization as raw
- **Ready-to-query Parquet files**: Organized by date and product type

**Purpose:**
- Stores the transformed, organized Parquet files
- Can be queried directly with DuckDB without going through the database
- Used as the source for database ingestion

**Structure matches `data/raw/`:**
```
data/gold/glbx-mdp3-2025-10-27/
├── continuous_instruments/
│   └── 2025-10-27.parquet
├── continuous_quotes_l1/
│   └── 2025-10-27.parquet
└── continuous_trades/
```

---

### 4. `data/GLBX-*/` (Manual Downloads)

**What it contains:**
- Manual downloads from DataBento website
- Unpacked DBN files with metadata

**Example:**
```
data/GLBX-YYYYMMDD-XXXXX/  (manual download folder)
├── glbx-mdp3-YYYY-MM-DD.ohlcv-1m.dbn
├── glbx-mdp3-YYYY-MM-DD.ohlcv-1m.dbn.zst
├── manifest.json
├── metadata.json
├── symbology.json
└── condition.json
```

**Purpose:**
- Manual bulk downloads from DataBento portal
- Can be converted to Parquet and processed like API downloads

---

## Data Flow

### Step 1: Download from DataBento API
```
DataBento API
    ↓
data/raw/glbx-mdp3-2025-10-27.bbo-1m.fullday.parquet
```

### Step 2: Transform to Folder Structure
```
glbx-mdp3-2025-10-27.bbo-1m.fullday.parquet
    ↓ (transform_continuous_to_folder_structure)
data/raw/glbx-mdp3-2025-10-27/
├── continuous_instruments/2025-10-27.parquet
├── continuous_quotes_l1/2025-10-27.parquet
└── continuous_trades/
```

### Step 3: Copy to Gold Layer
```
data/raw/glbx-mdp3-2025-10-27/
    ↓ (copy to gold)
data/gold/glbx-mdp3-2025-10-27/
```

### Step 4: Ingest into Database
```
data/gold/glbx-mdp3-2025-10-27/
    ↓ (pipelines/products/es_continuous_mdp3.py:load)
data/silver/market.duckdb
    ├── f_continuous_quote_l1 (fact table)
    ├── f_continuous_trade (fact table)
    ├── dim_continuous_contract (dimension table)
    └── g_continuous_bar_1m (gold layer - aggregated bars)
```

---

## Key Concepts

### Parquet Files

**Parquet** is a columnar storage format optimized for analytics:
- Efficient compression
- Fast querying
- Schema evolution support
- Language-agnostic (works with Python, R, DuckDB, etc.)

**Why Parquet?**
- DataBento API returns Parquet by default (or DBN which converts to Parquet)
- DuckDB can read Parquet directly (no need to load into memory first)
- Easy to share and archive
- Industry standard for data lakes/warehouses

### Folder Organization

The folder structure follows a **product-date** pattern:
- `glbx-mdp3-YYYY-MM-DD/` - Base folder for each date
- `continuous_instruments/` - Contract definitions (what instruments exist)
- `continuous_quotes_l1/` - Best bid/offer quotes (market data)
- `continuous_trades/` - Trade executions (if available)

This structure allows the database loader to:
1. Find all dates automatically
2. Load instruments first (dimension tables)
3. Load quotes/trades second (fact tables)
4. Maintain referential integrity

---

## Configuration

The folder paths are configured in `pipelines/common.py`:

```python
def get_paths():
    bronze = Path(os.getenv("DATA_BRONZE_ROOT", "./data/raw")).resolve()
    gold   = Path(os.getenv("DATA_GOLD_ROOT", "./data/gold")).resolve()
    dbpath = Path(os.getenv("DUCKDB_PATH", "./data/silver/market.duckdb")).resolve()
    return bronze, gold, dbpath
```

You can override these with environment variables:
- `DATA_BRONZE_ROOT` - Default: `./data/raw`
- `DATA_GOLD_ROOT` - Default: `./data/gold`
- `DUCKDB_PATH` - Default: `./data/silver/market.duckdb`

---

## Common Operations

### See what's in raw folder:
```powershell
ls data/raw/
```

### See transformed structure:
```powershell
ls data/raw/glbx-mdp3-2025-10-27/
```

### Check database size:
```powershell
ls -lh data/silver/market.duckdb
```

### Query Parquet directly (without database):
```python
import duckdb
df = duckdb.read_parquet("data/gold/glbx-mdp3-2025-10-27/continuous_quotes_l1/*.parquet").df()
```

