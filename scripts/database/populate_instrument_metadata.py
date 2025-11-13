"""
Populate instrument metadata (native symbols, expiry dates) and detect roll dates.

This script:
1. Extracts unique instrument_ids from daily bars
2. Resolves them to native symbols using DataBento's symbology API
3. Extracts expiry dates from native symbols
4. Stores metadata in dim_instrument_metadata
5. Detects roll dates by analyzing instrument_id changes over time
6. Stores roll dates in dim_roll_dates

Usage:
    python scripts/database/populate_instrument_metadata.py --root SR3
    python scripts/database/populate_instrument_metadata.py --all
    python scripts/database/populate_instrument_metadata.py --root ES --force
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import date, datetime
from typing import Dict, List, Optional, Set
from collections import defaultdict

import pandas as pd
import databento as db
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.instrument_metadata import (
    resolve_instrument_metadata,
    detect_roll_dates,
    parse_futures_symbol,
    calculate_imm_date
)
from pipelines.common import get_paths, connect_duckdb

logger = logging.getLogger(__name__)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def load_api_key() -> str:
    """Load DataBento API key from .env file."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()
    
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("DATABENTO_API_KEY not found. Set it in your environment or .env file.")
    return api_key


def get_unique_instrument_ids(con, root: Optional[str] = None) -> Set[int]:
    """Get unique instrument IDs from daily bars."""
    query = """
        SELECT DISTINCT underlying_instrument_id
        FROM g_continuous_bar_daily
        WHERE underlying_instrument_id IS NOT NULL
    """
    
    if root:
        query += """
            AND contract_series IN (
                SELECT contract_series
                FROM dim_continuous_contract
                WHERE root = ?
            )
        """
        result = con.execute(query, [root]).fetchdf()
    else:
        result = con.execute(query).fetchdf()
    
    instrument_ids = set(result['underlying_instrument_id'].astype(int).unique())
    logger.info(f"Found {len(instrument_ids)} unique instrument IDs" + (f" for root {root}" if root else ""))
    return instrument_ids


def get_existing_instrument_ids(con) -> Set[int]:
    """Get instrument IDs that already have metadata."""
    result = con.execute("""
        SELECT DISTINCT instrument_id
        FROM dim_instrument_metadata
    """).fetchdf()
    
    if result.empty:
        return set()
    
    return set(result['instrument_id'].astype(int).unique())


def populate_instrument_metadata(
    con,
    client: db.Historical,
    instrument_ids: List[int],
    force: bool = False
) -> int:
    """Resolve instrument IDs and populate metadata table."""
    if not instrument_ids:
        logger.info("No instrument IDs to process")
        return 0
    
    # Filter out existing instrument IDs unless forcing
    if not force:
        existing = get_existing_instrument_ids(con)
        new_ids = [inst_id for inst_id in instrument_ids if inst_id not in existing]
        logger.info(f"Found {len(new_ids)} new instrument IDs (out of {len(instrument_ids)} total)")
        instrument_ids = new_ids
    
    if not instrument_ids:
        logger.info("All instrument IDs already have metadata")
        return 0
    
    # Resolve in batches
    logger.info(f"Resolving {len(instrument_ids)} instrument IDs...")
    metadata = resolve_instrument_metadata(
        instrument_ids=instrument_ids,
        client=client,
        dataset="GLBX.MDP3",
        start_date=date(2020, 1, 1),
        end_date=date.today(),
    )
    
    logger.info(f"Resolved {len(metadata)} instrument IDs")
    
    # Insert into database
    if metadata:
        rows = []
        for inst_id, meta in metadata.items():
            rows.append({
                'instrument_id': inst_id,
                'native_symbol': meta.get('native_symbol'),
                'root': meta.get('root'),
                'month': meta.get('month'),
                'year': meta.get('year'),
                'expiry_date': meta.get('expiry_date'),
                'date_range_start': meta.get('date_range', (None, None))[0],
                'date_range_end': meta.get('date_range', (None, None))[1],
                'last_updated': datetime.now()
            })
        
        # Insert records one by one (DuckDB doesn't support INSERT ... SELECT from DataFrame directly)
        for row in rows:
            con.execute("""
                INSERT OR REPLACE INTO dim_instrument_metadata
                (instrument_id, native_symbol, root, month, year, expiry_date,
                 date_range_start, date_range_end, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row['instrument_id'],
                row['native_symbol'],
                row['root'],
                row['month'],
                row['year'],
                row['expiry_date'],
                row['date_range_start'],
                row['date_range_end'],
                row['last_updated'],
            ])
        logger.info(f"Inserted {len(rows)} instrument metadata records")
        return len(rows)
    
    return 0


def detect_and_store_roll_dates(con, root: Optional[str] = None) -> int:
    """Detect roll dates from daily bars and store in database."""
    logger.info("Detecting roll dates from daily bars...")
    
    # Get daily bars for continuous contracts
    query = """
        SELECT
            b.trading_date,
            b.contract_series,
            b.underlying_instrument_id,
            c.root
        FROM g_continuous_bar_daily b
        JOIN dim_continuous_contract c ON b.contract_series = c.contract_series
        WHERE b.underlying_instrument_id IS NOT NULL
    """
    
    if root:
        query += " AND c.root = ?"
        df = con.execute(query, [root]).fetchdf()
    else:
        df = con.execute(query).fetchdf()
    
    if df.empty:
        logger.warning("No daily bars found")
        return 0
    
    logger.info(f"Analyzing {len(df)} daily bars for roll dates...")
    
    # Parse contract_series to extract rank
    # Format: ROOT_FRONT_CALENDAR or ROOT_RANK_N_CALENDAR or ROOT_FRONT_CALENDAR_2D
    def extract_rank(contract_series: str) -> int:
        if '_FRONT_' in contract_series:
            return 0
        match = pd.Series([contract_series]).str.extract(r'RANK_(\d+)', expand=False)
        if match.iloc[0] and pd.notna(match.iloc[0]):
            return int(match.iloc[0])
        return 0
    
    df['rank'] = df['contract_series'].apply(extract_rank)
    
    # Sort by contract_series, rank, and trading_date
    df = df.sort_values(['contract_series', 'rank', 'trading_date'])
    
    # Detect rolls: when instrument_id changes for the same contract_series and rank
    roll_rows = []
    for (contract_series, rank), group in df.groupby(['contract_series', 'rank']):
        group = group.sort_values('trading_date')
        prev_instrument_id = None
        prev_date = None
        
        for _, row in group.iterrows():
            current_instrument_id = row['underlying_instrument_id']
            current_date = row['trading_date']
            
            if prev_instrument_id is not None and current_instrument_id != prev_instrument_id:
                # Roll occurred
                roll_rows.append({
                    'contract_series': contract_series,
                    'rank': rank,
                    'roll_date': current_date,
                    'old_instrument_id': prev_instrument_id,
                    'new_instrument_id': current_instrument_id,
                })
            
            prev_instrument_id = current_instrument_id
            prev_date = current_date
    
    if not roll_rows:
        logger.info("No roll dates detected")
        return 0
    
    logger.info(f"Detected {len(roll_rows)} roll dates")
    
    # Get metadata for old and new instrument IDs
    roll_df = pd.DataFrame(roll_rows)
    
    # Get instrument metadata
    old_ids = roll_df['old_instrument_id'].unique().tolist()
    new_ids = roll_df['new_instrument_id'].unique().tolist()
    all_ids = list(set(old_ids) | set(new_ids))
    
    if not all_ids:
        logger.warning("No instrument IDs found for metadata lookup")
        return 0
    
    # DuckDB requires a list in the IN clause
    metadata_df = con.execute(f"""
        SELECT
            instrument_id,
            native_symbol,
            expiry_date
        FROM dim_instrument_metadata
        WHERE instrument_id IN ({','.join(['?'] * len(all_ids))})
    """, all_ids).fetchdf()
    
    if metadata_df.empty:
        logger.warning("No instrument metadata found. Run metadata population first.")
        return 0
    
    # Join metadata
    metadata_dict = metadata_df.set_index('instrument_id').to_dict('index')
    
    roll_df['old_native_symbol'] = roll_df['old_instrument_id'].map(
        lambda x: metadata_dict.get(x, {}).get('native_symbol')
    )
    roll_df['new_native_symbol'] = roll_df['new_instrument_id'].map(
        lambda x: metadata_dict.get(x, {}).get('native_symbol')
    )
    roll_df['old_expiry_date'] = roll_df['old_instrument_id'].map(
        lambda x: metadata_dict.get(x, {}).get('expiry_date')
    )
    roll_df['new_expiry_date'] = roll_df['new_instrument_id'].map(
        lambda x: metadata_dict.get(x, {}).get('expiry_date')
    )
    
    # Insert into database
    # Convert to list of dictionaries for insert
    roll_records = roll_df.to_dict('records')
    
    for record in roll_records:
        con.execute("""
            INSERT OR REPLACE INTO dim_roll_dates
            (contract_series, rank, roll_date, old_instrument_id, new_instrument_id,
             old_native_symbol, new_native_symbol, old_expiry_date, new_expiry_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            record['contract_series'],
            record['rank'],
            record['roll_date'],
            record['old_instrument_id'],
            record['new_instrument_id'],
            record.get('old_native_symbol'),
            record.get('new_native_symbol'),
            record.get('old_expiry_date'),
            record.get('new_expiry_date'),
        ])
    
    logger.info(f"Stored {len(roll_rows)} roll dates")
    return len(roll_rows)


def show_metadata_summary(con, root: Optional[str] = None):
    """Show summary of instrument metadata."""
    logger.info("=" * 80)
    logger.info("INSTRUMENT METADATA SUMMARY")
    logger.info("=" * 80)
    
    query = """
        SELECT
            root,
            COUNT(*) as instrument_count,
            MIN(expiry_date) as earliest_expiry,
            MAX(expiry_date) as latest_expiry
        FROM dim_instrument_metadata
        WHERE root IS NOT NULL
    """
    
    if root:
        query += " AND root = ?"
        result = con.execute(query, [root]).fetchdf()
    else:
        result = con.execute(query).fetchdf()
    
    if not result.empty:
        logger.info(result.to_string(index=False))
    else:
        logger.info("No instrument metadata found")
    
    logger.info("\n" + "=" * 80)
    logger.info("ROLL DATES SUMMARY")
    logger.info("=" * 80)
    
    query = """
        SELECT
            r.contract_series,
            r.rank,
            COUNT(*) as roll_count,
            MIN(r.roll_date) as first_roll,
            MAX(r.roll_date) as last_roll
        FROM dim_roll_dates r
        JOIN dim_continuous_contract c ON r.contract_series = c.contract_series
    """
    
    if root:
        query += " WHERE c.root = ?"
        query += " GROUP BY r.contract_series, r.rank ORDER BY c.root, r.rank"
        result = con.execute(query, [root]).fetchdf()
    else:
        query += " GROUP BY r.contract_series, r.rank ORDER BY r.contract_series, r.rank"
        result = con.execute(query).fetchdf()
    
    if not result.empty:
        logger.info(result.to_string(index=False))
    else:
        logger.info("No roll dates found")


def main():
    parser = argparse.ArgumentParser(
        description="Populate instrument metadata and detect roll dates"
    )
    parser.add_argument(
        '--root',
        type=str,
        help='Process only this root (e.g., SR3, ES)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all roots'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-resolution of all instrument IDs'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary and exit'
    )
    parser.add_argument(
        '--detect-rolls-only',
        action='store_true',
        help='Only detect roll dates (skip metadata resolution)'
    )
    
    args = parser.parse_args()
    
    # Connect to database
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    
    try:
        # Run migrations
        from orchestrator import migrate
        logger.info("Running migrations...")
        migrate()
        
        if args.summary:
            show_metadata_summary(con, args.root)
            return 0
        
        if not args.detect_rolls_only:
            # Load API key
            api_key = load_api_key()
            client = db.Historical(api_key)
            
            # Get unique instrument IDs
            if args.all:
                instrument_ids = get_unique_instrument_ids(con)
            elif args.root:
                instrument_ids = get_unique_instrument_ids(con, args.root)
            else:
                parser.error("Must specify --root or --all")
                return 1
            
            if not instrument_ids:
                logger.warning("No instrument IDs found")
                return 0
            
            # Populate metadata
            logger.info("=" * 80)
            logger.info("POPULATING INSTRUMENT METADATA")
            logger.info("=" * 80)
            populate_instrument_metadata(con, client, list(instrument_ids), force=args.force)
        
        # Detect roll dates
        logger.info("\n" + "=" * 80)
        logger.info("DETECTING ROLL DATES")
        logger.info("=" * 80)
        detect_and_store_roll_dates(con, args.root)
        
        # Show summary
        logger.info("\n" + "=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        show_metadata_summary(con, args.root)
        
        logger.info("\n✓ Complete!")
        
    finally:
        con.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

