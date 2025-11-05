# Sharing Data Across Projects

## Overview

Your DataBento data is now centralized, making it easy to share across multiple projects.

## Data Locations

### Centralized (Shared) Data:
- **Raw Data**: `C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\raw\`
- **Gold Data**: `C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\gold\`

### Project-Specific Data:
- **Database**: `./data/silver/market.duckdb` (in each project folder)

## Sharing Data with Another Project

### Option 1: Use the Same Centralized Folders (Recommended)

**Steps:**
1. In your other project, create a `.env` file with the same paths:
   ```env
   DATA_BRONZE_ROOT=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\raw
   DATA_GOLD_ROOT=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\gold
   DUCKDB_PATH=./data/silver/market.duckdb
   ```

2. The other project will automatically use the same centralized data folders.

3. **Database**: Each project has its own database file, so:
   - You can copy the database from this project to the other project
   - Or point both projects to the same database (if you want shared database)
   - Or keep separate databases (recommended - each project can have different schema)

### Option 2: Copy the Database File

If you want to use the same database in another project:

```powershell
# Copy database from this project to another project
Copy-Item `
  -Path "C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb" `
  -Destination "C:\path\to\other-project\data\silver\market.duckdb"
```

### Option 3: Share the Database Location

If you want both projects to use the **same database** (not recommended unless you want them to share data):

1. Create a shared database location:
   ```powershell
   New-Item -ItemType Directory -Path "C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver" -Force
   ```

2. Move/copy the database there:
   ```powershell
   Copy-Item `
     -Path "C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb" `
     -Destination "C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver\market.duckdb"
   ```

3. In both projects' `.env` files:
   ```env
   DUCKDB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver\market.duckdb
   ```

**Warning**: Sharing a database between projects can cause conflicts if both projects try to write at the same time. Use separate databases unless you have a specific reason to share.

## Recommended Setup

### For Each New Project:

1. **Create `.env` file** with centralized paths:
   ```env
   DATABENTO_API_KEY=your-api-key-here
   DATA_BRONZE_ROOT=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\raw
   DATA_GOLD_ROOT=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\gold
   DUCKDB_PATH=./data/silver/market.duckdb
   ```

2. **Copy database** (if you want to use existing data):
   ```powershell
   # Create database directory in new project
   New-Item -ItemType Directory -Path ".\data\silver" -Force
   
   # Copy database
   Copy-Item `
     -Path "C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb" `
     -Destination ".\data\silver\market.duckdb"
   ```

3. **Or start fresh**: The new project will create its own empty database at `./data/silver/market.duckdb` and can ingest data from the centralized folders.

## Accessing Data from Code

Both projects can read from the same centralized folders:

```python
from pipelines.common import get_paths

# These will point to the centralized locations
bronze, gold, dbpath = get_paths()

# Read Parquet files directly
import duckdb
df = duckdb.read_parquet(f"{gold}/glbx-mdp3-2025-10-27/continuous_quotes_l1/*.parquet").df()

# Or connect to database
con = duckdb.connect(str(dbpath))
df = con.execute("SELECT * FROM f_continuous_quote_l1 LIMIT 100").fetchdf()
```

## Summary

✅ **Raw/Gold Data**: Shared automatically via centralized location  
✅ **Database**: Each project has its own (can copy if needed)  
✅ **Setup**: Just copy `.env` configuration to new projects  
✅ **Data Safety**: All paid downloads are in one protected location  

## Quick Checklist for New Project

- [ ] Create `.env` with centralized paths
- [ ] Copy database (optional) if you want existing data
- [ ] Or let it create a new database and ingest from centralized folders
- [ ] Start using the shared data!

