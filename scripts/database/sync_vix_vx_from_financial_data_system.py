"""
Sync VX continuous series, VIX3M index, and VVIX index from financial-data-system into the canonical research DB.

ETL bridge:
- VX futures: Reads VX continuous series (@VX=101XN, @VX=201XN, @VX=301XN) from financial-data-system
- VIX3M index: Reads VIX3M from financial-data-system (CBOE CSV source)
- VVIX index: Reads VVIX from financial-data-system (CBOE CSV source)
- Writes them into the canonical research DB (Databento ES Options DB)
- Idempotent: deletes existing rows for those symbols first, then inserts fresh copies

Data Source Policy:
- VIX index (1M): Use FRED (via download_fred_series.py) - NOT synced here
- VX continuous (VX1/2/3): Use CBOE via financial-data-system (this script)
- VIX3M index: Use CBOE via financial-data-system (this script) - FRED coverage insufficient
- VVIX index: Use CBOE via financial-data-system (this script) - Not available via FRED

Usage:
    # Using environment variable (set FIN_DB_PATH in .env)
    python scripts/database/sync_vix_vx_from_financial_data_system.py
    
    # With explicit path
    python scripts/database/sync_vix_vx_from_financial_data_system.py --fin-db-path "C:/Users/alexp/OneDrive/Gdrive/Trading/GitHub Projects/data-management/financial-data-system/data/financial_data.duckdb"
    
    # Force re-sync
    python scripts/database/sync_vix_vx_from_financial_data_system.py --force

Configuration:
    Add to .env file:
    FIN_DB_PATH=C:\\Users\\alexp\\OneDrive\\Gdrive\\Trading\\GitHub Projects\\data-management\\financial-data-system\\data\\financial_data.duckdb
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb

LOGGER = logging.getLogger("sync_vix_vx")

# Canonical VX continuous series symbols
# Front: @VX=101XN (VX1)
# 2nd:   @VX=201XN (VX2)
# 3rd:   @VX=301XN (VX3)
# Unadjusted, continuous, 1-day roll (TradeStation convention)
VX_SYMBOLS = ("@VX=101XN", "@VX=201XN", "@VX=301XN")

# VIX3M index (3-month implied volatility)
# Formerly known as VXV - CBOE 3-Month Volatility Index
VIX3M_SYMBOL = "VIX3M"

# VVIX index (VIX Volatility Index - Vol-of-Vol)
VVIX_SYMBOL = "VVIX"

# Note: VIX (1M) is NOT synced here - use FRED as primary source
# VIX3M and VVIX have better coverage from CBOE than FRED, so we sync them here


def configure_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def ensure_table_schema(con, source_db: str, target_db: str, table_name: str) -> None:
    """
    Ensure target table exists with same schema as source table.
    
    Args:
        con: DuckDB connection
        source_db: Source database alias (e.g., 'fin')
        target_db: Target database alias (e.g., 'main')
        table_name: Table name (e.g., 'market_data_cboe')
    """
    try:
        # Check if table exists in target database
        # For main database, we can check directly or via information_schema
        try:
            # Try to query the table to see if it exists
            con.execute(f"SELECT COUNT(*) FROM {target_db}.{table_name} LIMIT 0")
            LOGGER.debug("Table %s.%s already exists", target_db, table_name)
            return
        except Exception:
            # Table doesn't exist, create it
            pass
        
        # Create table by copying schema from source
        LOGGER.info("Creating table %s.%s from %s.%s schema", target_db, table_name, source_db, table_name)
        con.execute(f"""
            CREATE TABLE {target_db}.{table_name} AS
            SELECT * FROM {source_db}.{table_name} WHERE 1=0
        """)
        LOGGER.info("Created table %s.%s", target_db, table_name)
        
    except Exception as e:
        # If source table doesn't exist, we can't create the target
        LOGGER.error("Failed to create table %s.%s: %s", target_db, table_name, e)
        raise


def sync_vx_symbols(con, source_db: str, target_db: str, force: bool = False) -> int:
    """
    Sync VX continuous symbols from source to target database.
    
    Returns:
        Number of rows synced
    """
    LOGGER.info("Syncing VX symbols %s from %s.market_data -> %s.market_data", 
                VX_SYMBOLS, source_db, target_db)
    
    # Ensure table exists
    ensure_table_schema(con, source_db, target_db, "market_data")
    
    # Delete existing VX data
    placeholders = ','.join(['?'] * len(VX_SYMBOLS))
    con.execute(f"""
        DELETE FROM {target_db}.market_data
        WHERE symbol IN ({placeholders})
    """, list(VX_SYMBOLS))
    
    LOGGER.debug("Deleted existing VX rows from %s.market_data", target_db)
    
    # Insert VX data
    con.execute(f"""
        INSERT INTO {target_db}.market_data
        SELECT *
        FROM {source_db}.market_data
        WHERE symbol IN ({placeholders})
    """, list(VX_SYMBOLS))
    
    # Get count of inserted rows
    count = con.execute(f"""
        SELECT COUNT(*) 
        FROM {target_db}.market_data 
        WHERE symbol IN ({placeholders})
    """, list(VX_SYMBOLS)).fetchone()[0]
    
    LOGGER.info("Synced %d VX rows", count)
    return count


def sync_continuous_contracts(con, source_db: str, target_db: str, force: bool = False) -> int:
    """
    Sync continuous_contracts metadata for VX symbols.
    
    Returns:
        Number of rows synced
    """
    LOGGER.info("Syncing continuous_contracts rows for %s", VX_SYMBOLS)
    
    # Ensure table exists
    ensure_table_schema(con, source_db, target_db, "continuous_contracts")
    
    # Delete existing VX continuous_contracts data
    placeholders = ','.join(['?'] * len(VX_SYMBOLS))
    con.execute(f"""
        DELETE FROM {target_db}.continuous_contracts
        WHERE symbol IN ({placeholders})
    """, list(VX_SYMBOLS))
    
    LOGGER.debug("Deleted existing continuous_contracts rows from %s.continuous_contracts", target_db)
    
    # Insert continuous_contracts data
    con.execute(f"""
        INSERT INTO {target_db}.continuous_contracts
        SELECT *
        FROM {source_db}.continuous_contracts
        WHERE symbol IN ({placeholders})
    """, list(VX_SYMBOLS))
    
    # Get count of inserted rows
    count = con.execute(f"""
        SELECT COUNT(*) 
        FROM {target_db}.continuous_contracts 
        WHERE symbol IN ({placeholders})
    """, list(VX_SYMBOLS)).fetchone()[0]
    
    LOGGER.info("Synced %d continuous_contracts rows", count)
    return count


def sync_vix3m_index(con, source_db: str, target_db: str, force: bool = False) -> int:
    """
    Sync VIX3M index from source to target database.
    
    VIX3M (3-month implied volatility, formerly VXV) is synced from CBOE
    because FRED coverage is insufficient.
    
    Returns:
        Number of rows synced
    """
    LOGGER.info("Syncing VIX3M index (%s) from %s.market_data_cboe -> %s.market_data_cboe", 
                VIX3M_SYMBOL, source_db, target_db)
    
    # Ensure table exists
    ensure_table_schema(con, source_db, target_db, "market_data_cboe")
    
    # Delete existing VIX3M data (idempotency)
    con.execute(f"""
        DELETE FROM {target_db}.market_data_cboe
        WHERE symbol = ?
    """, [VIX3M_SYMBOL])
    
    LOGGER.debug("Deleted existing VIX3M rows from %s.market_data_cboe", target_db)
    
    # Insert VIX3M data
    con.execute(f"""
        INSERT INTO {target_db}.market_data_cboe
        SELECT *
        FROM {source_db}.market_data_cboe
        WHERE symbol = ?
    """, [VIX3M_SYMBOL])
    
    # Get count of inserted rows
    count = con.execute(f"""
        SELECT COUNT(*) 
        FROM {target_db}.market_data_cboe 
        WHERE symbol = ?
    """, [VIX3M_SYMBOL]).fetchone()[0]
    
    LOGGER.info("Synced %d VIX3M rows", count)
    return count


def sync_vvix_index(con, source_db: str, target_db: str, force: bool = False) -> int:
    """
    Sync VVIX index from source to target database.
    
    VVIX (VIX Volatility Index - Vol-of-Vol) is synced from CBOE
    because it is not available via FRED API.
    
    Returns:
        Number of rows synced
    """
    LOGGER.info("Syncing VVIX index (%s) from %s.market_data_cboe -> %s.market_data_cboe", 
                VVIX_SYMBOL, source_db, target_db)
    
    # Ensure table exists
    ensure_table_schema(con, source_db, target_db, "market_data_cboe")
    
    # Delete existing VVIX data (idempotency)
    con.execute(f"""
        DELETE FROM {target_db}.market_data_cboe
        WHERE symbol = ?
    """, [VVIX_SYMBOL])
    
    LOGGER.debug("Deleted existing VVIX rows from %s.market_data_cboe", target_db)
    
    # Insert VVIX data
    con.execute(f"""
        INSERT INTO {target_db}.market_data_cboe
        SELECT *
        FROM {source_db}.market_data_cboe
        WHERE symbol = ?
    """, [VVIX_SYMBOL])
    
    # Get count of inserted rows
    count = con.execute(f"""
        SELECT COUNT(*) 
        FROM {target_db}.market_data_cboe 
        WHERE symbol = ?
    """, [VVIX_SYMBOL]).fetchone()[0]
    
    # Get date range for logging
    date_range = con.execute(f"""
        SELECT 
            MIN(CAST(timestamp AS DATE)) as first_date,
            MAX(CAST(timestamp AS DATE)) as last_date
        FROM {target_db}.market_data_cboe 
        WHERE symbol = ?
    """, [VVIX_SYMBOL]).fetchone()
    
    if date_range and date_range[0] and date_range[1]:
        LOGGER.info("Synced %d VVIX rows (%s to %s)", count, date_range[0], date_range[1])
    else:
        LOGGER.info("Synced %d VVIX rows", count)
    
    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Sync VX continuous series, VIX3M index, and VVIX index from financial-data-system to canonical DB"
    )
    parser.add_argument(
        "--fin-db-path",
        type=str,
        help="Path to financial-data-system DuckDB database (default: from FIN_DB_PATH env var)"
    )
    parser.add_argument(
        "--canon-db-path",
        type=str,
        help="Path to canonical DuckDB database (default: from DUCKDB_PATH env var)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-sync (delete existing data first)"
    )
    parser.add_argument(
        "--skip-vvix",
        action="store_true",
        help="Skip syncing VVIX index"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    configure_logging(verbose=args.verbose)
    
    # Load environment
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()
    
    # Get financial-data-system DB path
    if args.fin_db_path:
        fin_db_path = Path(args.fin_db_path).resolve()
    else:
        fin_db_path_str = os.getenv("FIN_DB_PATH")
        if not fin_db_path_str:
            LOGGER.error("FIN_DB_PATH environment variable not set and --fin-db-path not provided")
            LOGGER.error("Please set FIN_DB_PATH in .env or provide --fin-db-path")
            LOGGER.error("Example: FIN_DB_PATH=C:\\Users\\alexp\\OneDrive\\Gdrive\\Trading\\GitHub Projects\\data-management\\financial-data-system\\data\\financial_data.duckdb")
            return 1
        fin_db_path = Path(fin_db_path_str).resolve()
    
    if not fin_db_path.exists():
        LOGGER.error("Financial-data-system database not found: %s", fin_db_path)
        return 1
    
    # Get canonical DB path
    if args.canon_db_path:
        canon_db_path = Path(args.canon_db_path).resolve()
    else:
        _, _, canon_db_path = get_paths()
    
    if not canon_db_path.exists():
        LOGGER.warning("Canonical database does not exist, will be created: %s", canon_db_path)
        canon_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    LOGGER.info("Source DB: %s", fin_db_path)
    LOGGER.info("Target DB: %s", canon_db_path)
    
    # Connect to canonical DB (will create if doesn't exist)
    con = connect_duckdb(canon_db_path)
    
    try:
        # Attach financial-data-system DB as read-only
        LOGGER.info("Attaching financial-data-system DB...")
        con.execute(f"ATTACH '{fin_db_path}' AS fin (READ_ONLY)")
        
        # In DuckDB, the main database (the one we connected to) is accessible
        # directly without a schema prefix, or via "main" schema
        # The attached database is accessible via its alias "fin"
        
        # Sync VX symbols (VX1/2/3)
        vx_count = sync_vx_symbols(con, "fin", "main", force=args.force)
        
        # Sync continuous_contracts metadata
        contracts_count = sync_continuous_contracts(con, "fin", "main", force=args.force)
        
        # Sync VIX3M index
        vix3m_count = sync_vix3m_index(con, "fin", "main", force=args.force)
        
        # Sync VVIX index
        vvix_count = 0
        if not args.skip_vvix:
            vvix_count = sync_vvix_index(con, "fin", "main", force=args.force)
        else:
            LOGGER.info("Skipping VVIX sync (--skip-vvix flag set)")
        
        LOGGER.info("=" * 60)
        LOGGER.info("Sync complete:")
        LOGGER.info("  VX symbols (@VX=101XN/201XN/301XN): %d rows", vx_count)
        LOGGER.info("  Continuous contracts metadata: %d rows", contracts_count)
        LOGGER.info("  VIX3M index: %d rows", vix3m_count)
        if not args.skip_vvix:
            LOGGER.info("  VVIX index: %d rows", vvix_count)
        LOGGER.info("=" * 60)
        LOGGER.info("")
        LOGGER.info("Note: VIX (1M) is sourced from FRED, not synced here")
        LOGGER.info("Run: python scripts/download/download_fred_series.py")
        
        return 0
        
    except Exception as e:
        LOGGER.error("Sync failed: %s", e, exc_info=args.verbose)
        return 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

