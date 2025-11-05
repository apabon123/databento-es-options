# Read-Only Database Access for Strategy Projects

## Overview

This project (`databento-es-options`) is the **data pipeline project** that:
- Downloads data from DataBento
- Builds and maintains the market data database
- Stores all raw/gold data

Other projects (strategy testing, backtesting, etc.) will:
- **Read-only access** to the database
- Query market data for analysis
- Never modify the database

## Database Location

**Current Location:**
```
C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb
```

## Option 1: Keep Database in This Project (Recommended)

**Setup in other projects:**

1. Create `.env` file pointing to this project's database:
   ```env
   # Point to the data pipeline project's database
   DUCKDB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb
   ```

2. Connect to database (read-only mode):
   ```python
   import duckdb
   from pathlib import Path
   import os
   from dotenv import load_dotenv
   
   load_dotenv()
   
   # Database path from environment or default
   db_path = Path(os.getenv("DUCKDB_PATH", 
       r"C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb"))
   
   # Connect (DuckDB is read-only by default when connecting to existing file)
   con = duckdb.connect(str(db_path), read_only=True)
   
   # Query data
   df = con.execute("""
       SELECT * FROM g_continuous_bar_1m 
       WHERE ts_minute >= '2025-10-27'
       LIMIT 100
   """).fetchdf()
   
   con.close()
   ```

## Option 2: Move Database to Centralized Location

If you want the database in the centralized DataBento folder:

1. **Update `.env` in this project:**
   ```env
   DATA_BRONZE_ROOT=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\raw
   DATA_GOLD_ROOT=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\gold
   DUCKDB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver\market.duckdb
   ```

2. **Move the database:**
   ```powershell
   # Create silver directory
   New-Item -ItemType Directory -Path "C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver" -Force
   
   # Move database
   Move-Item `
     -Path "C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb" `
     -Destination "C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver\market.duckdb"
   ```

3. **In other projects, use centralized path:**
   ```env
   DUCKDB_PATH=C:\Users\alexp\OneDrive\Gdrive\Trading\Data Downloads\DataBento\silver\market.duckdb
   ```

## Read-Only Access Pattern

### In Strategy Projects:

```python
"""
Example: Querying market data from the data pipeline database
"""
import duckdb
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Path to the data pipeline database
DB_PATH = Path(os.getenv(
    "MARKET_DATA_DB_PATH",
    r"C:\Users\alexp\OneDrive\Gdrive\Trading\GitHub Projects\databento-es-options\data\silver\market.duckdb"
))

def get_market_data(contract_series="ES_FRONT_MONTH", start_date=None, end_date=None):
    """Query market data from the central database."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    
    try:
        query = f"""
            SELECT 
                ts_minute,
                contract_series,
                o_mid, h_mid, l_mid, c_mid,
                v_trades, v_notional
            FROM g_continuous_bar_1m
            WHERE contract_series = '{contract_series}'
        """
        
        if start_date:
            query += f" AND ts_minute >= '{start_date}'"
        if end_date:
            query += f" AND ts_minute <= '{end_date}'"
            
        query += " ORDER BY ts_minute"
        
        df = con.execute(query).fetchdf()
        return df
    finally:
        con.close()

# Example usage
if __name__ == "__main__":
    df = get_market_data(start_date="2025-10-27", end_date="2025-10-31")
    print(df.head())
```

## Available Tables

### Continuous Futures (Gold Layer):
- `g_continuous_bar_1m` - 1-minute bars for continuous contracts
- `f_continuous_quote_l1` - Raw level 1 quotes
- `f_continuous_trade` - Trade data
- `dim_continuous_contract` - Contract definitions

### ES Futures:
- `g_fut_bar_1m` - 1-minute bars for futures contracts
- `f_fut_quote_l1` - Raw quotes
- `f_fut_trade` - Trade data

### ES Options:
- `g_bar_1m` - 1-minute bars for options
- `f_quote_l1` - Raw quotes
- `f_trade` - Trade data

## Best Practices

1. **Read-Only Mode**: Always use `read_only=True` when connecting from strategy projects
2. **Single Writer**: Only this project should write to the database
3. **Path Configuration**: Use environment variables for database path
4. **Error Handling**: Handle database connection errors gracefully
5. **Query Optimization**: Use appropriate WHERE clauses and LIMITs

## Summary

✅ **This Project**: Downloads, builds, and maintains the database  
✅ **Other Projects**: Read-only access to query market data  
✅ **Database Location**: Configure via `DUCKDB_PATH` environment variable  
✅ **No Conflicts**: Read-only mode prevents accidental modifications  

