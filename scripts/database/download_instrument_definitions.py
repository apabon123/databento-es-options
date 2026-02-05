"""
Download and store instrument definitions (contract specifications) from DataBento.

This script:
1. Extracts unique instrument_ids from daily bars
2. Resolves them to native symbols using DataBento's symbology API
3. Downloads definitions from DataBento's definition schema
4. Stores definitions in dim_instrument_definition table

Usage:
    python scripts/database/download_instrument_definitions.py --root SR3
    python scripts/database/download_instrument_definitions.py --all
    python scripts/database/download_instrument_definitions.py --root ES --force
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
import time

import pandas as pd
import databento as db

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.env import load_env

load_env()

from pipelines.common import get_paths, connect_duckdb

logger = logging.getLogger(__name__)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def load_api_key() -> str:
    """Load DataBento API key from .env file."""
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("DATABENTO_API_KEY not found. Set it in your environment or .env file.")
    return api_key


def get_unique_instrument_ids(con, root: Optional[str] = None) -> Set[int]:
    """Get unique instrument IDs from daily bars.
    
    Note: The same instrument (e.g., ESH23) may appear multiple times in the database:
    - On different trading dates
    - As rank 0 (front month)
    - As rank 1 (back month)
    - In different continuous contracts
    
    But each instrument has a unique instrument_id, so we use DISTINCT to ensure
    we only process each instrument once, avoiding duplicate definitions.
    """
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
    logger.info(f"Note: Each instrument (e.g., ESH23) appears only once, regardless of how many times")
    logger.info(f"      it appears in daily bars (different dates, ranks, etc.)")
    return instrument_ids


def get_instrument_date_ranges(con, instrument_ids: Set[int]) -> Dict[int, Tuple[date, date]]:
    """Get date ranges when each instrument was active."""
    if not instrument_ids:
        return {}
    
    # Convert to native Python ints (DuckDB doesn't like numpy types)
    instrument_ids_list = [int(inst_id) for inst_id in instrument_ids]
    
    # Build query with IN clause
    placeholders = ','.join(['?'] * len(instrument_ids_list))
    query = f"""
        SELECT 
            underlying_instrument_id,
            MIN(trading_date) as first_date,
            MAX(trading_date) as last_date
        FROM g_continuous_bar_daily
        WHERE underlying_instrument_id IN ({placeholders})
        GROUP BY underlying_instrument_id
    """
    
    result = con.execute(query, instrument_ids_list).fetchdf()
    
    date_ranges = {}
    for _, row in result.iterrows():
        inst_id = int(row['underlying_instrument_id'])
        first_date = row['first_date']
        last_date = row['last_date']
        date_ranges[inst_id] = (first_date, last_date)
    
    logger.info(f"Got date ranges for {len(date_ranges)} instruments")
    return date_ranges


def resolve_instrument_to_symbols(
    instrument_ids: List[int],
    client: db.Historical,
    dataset: str = "GLBX.MDP3",
    date_ranges: Optional[Dict[int, Tuple[date, date]]] = None,
) -> Dict[int, str]:
    """Resolve instrument IDs to native symbols."""
    if not instrument_ids:
        return {}
    
    symbol_map = {}
    batch_size = 100
    
    # Convert to native Python ints (in case they're numpy types)
    instrument_ids = [int(inst_id) for inst_id in instrument_ids]
    
    for i in range(0, len(instrument_ids), batch_size):
        batch = instrument_ids[i:i + batch_size]
        logger.info(f"Resolving batch {i//batch_size + 1} ({len(batch)} instrument IDs)...")
        
        try:
            # Get date range for this batch
            if date_ranges:
                min_date = min((dr[0] for inst_id in batch if inst_id in date_ranges for dr in [date_ranges[inst_id]]), default=date(2020, 1, 1))
                max_date = max((dr[1] for inst_id in batch if inst_id in date_ranges for dr in [date_ranges[inst_id]]), default=date.today())
            else:
                min_date = date(2020, 1, 1)
                max_date = date.today()
            
            result = client.symbology.resolve(
                dataset=dataset,
                symbols=batch,  # Already converted to native Python ints
                stype_in="instrument_id",
                stype_out="native",
                start_date=min_date,
                end_date=max_date,
            )
            
            # Parse result
            if isinstance(result, dict) and 'result' in result:
                for inst_id in batch:
                    inst_str = str(inst_id)
                    if inst_str in result['result']:
                        mappings = result['result'][inst_str]
                        if mappings:
                            native_symbol = mappings[0].get('s', '')
                            if native_symbol:
                                symbol_map[inst_id] = native_symbol
                                logger.debug(f"  {inst_id} -> {native_symbol}")
            
            # Rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            logger.warning(f"Error resolving batch: {e}")
            continue
    
    logger.info(f"Resolved {len(symbol_map)} instrument IDs to native symbols")
    return symbol_map


def get_existing_definitions(con) -> Set[int]:
    """Get instrument IDs that already have definitions."""
    result = con.execute("""
        SELECT DISTINCT instrument_id
        FROM dim_instrument_definition
    """).fetchdf()
    
    if result.empty:
        return set()
    
    return set(result['instrument_id'].astype(int).unique())


def download_definitions(
    symbols: List[str],
    client: db.Historical,
    dataset: str = "GLBX.MDP3",
    start_date: date = None,
    end_date: date = None,
) -> pd.DataFrame:
    """Download definitions from DataBento for given symbols.
    
    Note: Definitions for futures contracts are usually static and don't change day-to-day.
    We download for a date range covering when the instruments were active to ensure
    we get definitions even if they weren't available on the exact first day.
    """
    if not symbols:
        return pd.DataFrame()
    
    if start_date is None:
        start_date = date(2020, 1, 1)
    if end_date is None:
        end_date = date.today()
    
    # Use the full date range to ensure we get definitions
    # Definitions are usually available throughout the instrument's lifetime
    # Using the full range ensures we capture definitions even if they weren't
    # available on the exact first day or if instruments span multiple years
    query_end_date = end_date
    
    # Ensure end_date is at least 1 day after start_date
    if query_end_date <= start_date:
        query_end_date = start_date + timedelta(days=1)
    
    logger.info(f"Downloading definitions for {len(symbols)} symbols (date range: {start_date} to {query_end_date})...")
    
    try:
        data = client.timeseries.get_range(
            dataset=dataset,
            schema="definition",
            stype_in="native",
            symbols=symbols,
            start=start_date,
            end=query_end_date,
        )
        df = data.to_df()
        logger.info(f"Downloaded {len(df)} definition records")
        
        # Deduplicate by instrument_id - keep only the latest definition per instrument
        if not df.empty and 'instrument_id' in df.columns:
            # Sort by ts_event descending and keep first per instrument_id
            df = df.sort_values('ts_event', ascending=False, na_position='last')
            df = df.drop_duplicates(subset=['instrument_id'], keep='first')
            logger.info(f"Deduplicated to {len(df)} unique instrument definitions")
        elif not df.empty:
            logger.warning(f"Downloaded {len(df)} records but no instrument_id column found")
        
        return df
    except Exception as e:
        logger.error(f"Error downloading definitions: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return pd.DataFrame()


def store_definitions(con, df: pd.DataFrame, force: bool = False) -> int:
    """Store definitions in the database.
    
    Note: The database uses instrument_id as the primary key, ensuring only one
    definition per instrument. If the same instrument (e.g., ESH23) appears multiple
    times in the input DataFrame (which shouldn't happen after deduplication),
    INSERT OR REPLACE will ensure only the latest definition is stored.
    """
    if df.empty:
        return 0
    
    logger.info(f"Storing {len(df)} definition records...")
    
    # Map DataFrame columns to database columns
    column_mapping = {
        'instrument_id': 'instrument_id',
        'raw_symbol': 'native_symbol',
        'ts_event': 'ts_event',
        'min_price_increment': 'min_price_increment',
        'min_price_increment_amount': 'min_price_increment_amount',
        'contract_multiplier': 'contract_multiplier',
        'original_contract_size': 'original_contract_size',
        'contract_multiplier_unit': 'contract_multiplier_unit',
        'expiration': 'expiration',
        'maturity_year': 'maturity_year',
        'maturity_month': 'maturity_month',
        'maturity_day': 'maturity_day',
        'maturity_week': 'maturity_week',
        'high_limit_price': 'high_limit_price',
        'low_limit_price': 'low_limit_price',
        'trading_reference_price': 'trading_reference_price',
        'max_price_variation': 'max_price_variation',
        'min_trade_vol': 'min_trade_vol',
        'max_trade_vol': 'max_trade_vol',
        'min_lot_size': 'min_lot_size',
        'min_lot_size_block': 'min_lot_size_block',
        'min_lot_size_round_lot': 'min_lot_size_round_lot',
        'currency': 'currency',
        'settl_currency': 'settl_currency',
        'unit_of_measure': 'unit_of_measure',
        'unit_of_measure_qty': 'unit_of_measure_qty',
        'underlying_id': 'underlying_id',
        'underlying': 'underlying',
        'underlying_product': 'underlying_product',
        'asset': 'asset',
        'exchange': 'exchange',
        'group': '"group"',  # Quoted because 'group' is a SQL reserved keyword
        'secsubtype': 'secsubtype',
        'instrument_class': 'instrument_class',
        'security_type': 'security_type',
        'cfi': 'cfi',
        'user_defined_instrument': 'user_defined_instrument',
        'match_algorithm': 'match_algorithm',
        'tick_rule': 'tick_rule',
        'flow_schedule_type': 'flow_schedule_type',
        'market_depth': 'market_depth',
        'market_depth_implied': 'market_depth_implied',
        'market_segment_id': 'market_segment_id',
        'md_security_trading_status': 'md_security_trading_status',
        'display_factor': 'display_factor',
        'price_display_format': 'price_display_format',
        'main_fraction': 'main_fraction',
        'sub_fraction': 'sub_fraction',
        'activation': 'activation',
        'trading_reference_date': 'trading_reference_date',
        'decay_start_date': 'decay_start_date',
        'decay_quantity': 'decay_quantity',
        'security_update_action': 'security_update_action',
        'price_ratio': 'price_ratio',
        'inst_attrib_value': 'inst_attrib_value',
        'raw_instrument_id': 'raw_instrument_id',
        'strike_price': 'strike_price',
        'strike_price_currency': 'strike_price_currency',
        'settl_price_type': 'settl_price_type',
        'appl_id': 'appl_id',
        'channel_id': 'channel_id',
    }
    
    # Prepare data for insertion
    rows = []
    for _, row in df.iterrows():
        # Extract definition_date from ts_event
        ts_event_val = row.get('ts_event')
        if pd.isna(ts_event_val):
            ts_event = None
            definition_date = date.today()
        else:
            ts_event = pd.to_datetime(ts_event_val)
            definition_date = ts_event.date()
        
        # Start with required fields
        record = {
            'instrument_id': int(row.get('instrument_id', 0)),
            'native_symbol': str(row.get('raw_symbol', '')),
            'definition_date': definition_date,
            'ts_event': ts_event,
        }
        
        # Add all mapped columns
        for df_col, db_col in column_mapping.items():
            if df_col in row:
                value = row[df_col]
                # Convert NaN to None
                if pd.isna(value):
                    value = None
                # Convert specific types
                elif db_col in ['instrument_id', 'underlying_id', 'raw_instrument_id']:
                    try:
                        value = int(value) if value is not None else None
                    except (ValueError, TypeError):
                        value = None
                elif db_col in ['maturity_year', 'maturity_month', 'maturity_day', 'maturity_week',
                               'min_trade_vol', 'max_trade_vol', 'min_lot_size', 'min_lot_size_block',
                               'min_lot_size_round_lot', 'market_depth', 'market_depth_implied',
                               'market_segment_id', 'md_security_trading_status', 'tick_rule',
                               'flow_schedule_type', 'contract_multiplier_unit', 'decay_quantity',
                               'inst_attrib_value', 'price_display_format', 'main_fraction',
                               'sub_fraction', 'settl_price_type', 'appl_id', 'channel_id',
                               'maturity_year_appl']:
                    try:
                        value = int(value) if value is not None and not pd.isna(value) else None
                    except (ValueError, TypeError):
                        value = None
                elif db_col in ['expiration', 'activation', 'trading_reference_date', 'decay_start_date']:
                    try:
                        if value is not None and not pd.isna(value):
                            value = pd.to_datetime(value)
                        else:
                            value = None
                    except (ValueError, TypeError):
                        value = None
                elif db_col == 'trading_reference_date' and value is not None:
                    try:
                        value = pd.to_datetime(value).date()
                    except (ValueError, TypeError):
                        value = None
                elif db_col == 'definition_date' and value is not None:
                    try:
                        value = pd.to_datetime(value).date() if not isinstance(value, date) else value
                    except (ValueError, TypeError):
                        value = None
                
                record[db_col] = value
        
        # Add last_updated
        record['last_updated'] = datetime.now()
        
        rows.append(record)
    
    # Insert into database
    inserted = 0
    for record in rows:
        try:
            # Primary key is now just instrument_id (one definition per instrument)
            # INSERT OR REPLACE will automatically replace any existing definition for this instrument
            if 'instrument_id' not in record or record['instrument_id'] is None:
                logger.warning(f"Skipping record with missing instrument_id: {record}")
                continue
            
            # Get all columns that are not None
            all_columns = list(record.keys())
            # Ensure instrument_id is first (primary key)
            columns = ['instrument_id'] if 'instrument_id' in all_columns else []
            columns.extend([col for col in all_columns if col != 'instrument_id' and record[col] is not None])
            
            placeholders = ','.join(['?'] * len(columns))
            values = [record[col] for col in columns]
            
            query = f"""
                INSERT OR REPLACE INTO dim_instrument_definition
                ({','.join(columns)})
                VALUES ({placeholders})
            """
            
            con.execute(query, values)
            inserted += 1
            
        except Exception as e:
            logger.warning(f"Error inserting definition for instrument {record.get('instrument_id')}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            continue
    
    logger.info(f"Stored {inserted} definition records")
    return inserted


def show_summary(con, root: Optional[str] = None):
    """Show summary of instrument definitions."""
    logger.info("=" * 80)
    logger.info("INSTRUMENT DEFINITIONS SUMMARY")
    logger.info("=" * 80)
    
    query = """
        SELECT 
            d.asset,
            COUNT(DISTINCT d.instrument_id) as instrument_count,
            COUNT(DISTINCT d.native_symbol) as symbol_count,
            MIN(d.definition_date) as earliest_date,
            MAX(d.definition_date) as latest_date
        FROM dim_instrument_definition d
    """
    
    if root:
        query += """
            WHERE d.asset = ?
        """
        query += " GROUP BY d.asset ORDER BY d.asset"
        result = con.execute(query, [root]).fetchdf()
    else:
        query += " GROUP BY d.asset ORDER BY d.asset"
        result = con.execute(query).fetchdf()
    
    if not result.empty:
        logger.info(result.to_string(index=False))
    else:
        logger.info("No instrument definitions found")
    
    # Show sample definitions
    logger.info("\n" + "=" * 80)
    logger.info("SAMPLE DEFINITIONS")
    logger.info("=" * 80)
    
    query = """
        SELECT 
            instrument_id,
            native_symbol,
            asset,
            min_price_increment,
            min_price_increment_amount,
            expiration,
            maturity_year,
            maturity_month,
            maturity_day,
            currency,
            contract_multiplier
        FROM v_instrument_definition_latest
        LIMIT 10
    """
    
    if root:
        query = query.replace("LIMIT 10", "WHERE asset = ? LIMIT 10")
        result = con.execute(query, [root]).fetchdf()
    else:
        result = con.execute(query).fetchdf()
    
    if not result.empty:
        logger.info(result.to_string(index=False))
    else:
        logger.info("No definitions found")


def main():
    parser = argparse.ArgumentParser(
        description="Download and store instrument definitions from DataBento"
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
        help='Force re-download of all definitions'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary and exit'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Batch size for downloading definitions (default: 50)'
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
            show_summary(con, args.root)
            return 0
        
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
        
        # Filter out existing definitions unless forcing
        if not args.force:
            existing = get_existing_definitions(con)
            new_ids = instrument_ids - existing
            logger.info(f"Found {len(new_ids)} new instrument IDs (out of {len(instrument_ids)} total)")
            instrument_ids = new_ids
        
        if not instrument_ids:
            logger.info("All instrument IDs already have definitions")
            return 0
        
        # Get date ranges for instruments
        logger.info("Getting date ranges for instruments...")
        date_ranges = get_instrument_date_ranges(con, instrument_ids)
        
        # Resolve instrument IDs to native symbols
        logger.info("=" * 80)
        logger.info("RESOLVING INSTRUMENT IDs TO NATIVE SYMBOLS")
        logger.info("=" * 80)
        symbol_map = resolve_instrument_to_symbols(
            list(instrument_ids),
            client,
            date_ranges=date_ranges,
        )
        
        if not symbol_map:
            logger.warning("No symbols resolved")
            return 0
        
        # Download definitions in batches
        logger.info("\n" + "=" * 80)
        logger.info("DOWNLOADING DEFINITIONS")
        logger.info("=" * 80)
        
        # CRITICAL: Deduplicate symbols to ensure we only download each symbol ONCE
        # This prevents duplicate API calls and reduces costs
        # Example: If ESH23 appears 1000 times in the database (different dates, ranks),
        # we still only download its definition ONCE, not 1000 times
        unique_symbols = list(set(symbol_map.values()))
        logger.info(f"Found {len(unique_symbols)} unique symbols (out of {len(symbol_map)} instrument IDs)")
        logger.info(f"CRITICAL: Each symbol will be downloaded ONLY ONCE, minimizing API costs")
        if len(unique_symbols) < len(symbol_map):
            logger.info(f"Note: {len(symbol_map) - len(unique_symbols)} instrument IDs share symbols (deduplicated)")
        
        # Create reverse mapping: symbol -> instrument_id (should be one-to-one)
        # This allows us to get the instrument_id for each symbol when needed
        symbol_to_inst_id = {v: k for k, v in symbol_map.items()}
        
        batch_size = args.batch_size
        total_inserted = 0
        
        # Store original instrument_ids set for filtering (don't modify it)
        target_instrument_ids = instrument_ids.copy()
        
        for i in range(0, len(unique_symbols), batch_size):
            batch_symbols = unique_symbols[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch_symbols)} unique symbols)...")
            logger.info(f"  Making ONE API call per symbol = {len(batch_symbols)} API calls (not {len(symbol_map)} calls)")
            
            # Get date range for this batch
            # Use the instrument_ids that correspond to these symbols
            batch_inst_ids = [symbol_to_inst_id.get(symbol) for symbol in batch_symbols if symbol in symbol_to_inst_id]
            batch_inst_ids = [inst_id for inst_id in batch_inst_ids if inst_id is not None]
            
            if batch_inst_ids and date_ranges:
                min_date = min((date_ranges.get(inst_id, (date(2020, 1, 1), date.today()))[0] for inst_id in batch_inst_ids), default=date(2020, 1, 1))
                max_date = max((date_ranges.get(inst_id, (date(2020, 1, 1), date.today()))[1] for inst_id in batch_inst_ids), default=date.today())
            else:
                min_date = date(2020, 1, 1)
                max_date = date.today()
            
            # Download definitions ONCE for these unique symbols
            df = download_definitions(
                batch_symbols,
                client,
                start_date=min_date,
                end_date=max_date,
            )
            
            if not df.empty:
                # CRITICAL: Filter to only keep definitions for instruments we actually have in daily bars
                # DataBento may return definitions for multiple instruments with the same symbol
                # (e.g., "ESM0" for 2020, 2021, 2022, etc.), but we only want the ones in our database
                if 'instrument_id' in df.columns:
                    before_filter = len(df)
                    # Convert to int for comparison
                    df['instrument_id'] = df['instrument_id'].astype(int)
                    df = df[df['instrument_id'].isin(target_instrument_ids)]
                    after_filter = len(df)
                    if before_filter > after_filter:
                        logger.info(f"  Filtered: {before_filter} definitions -> {after_filter} (removed {before_filter - after_filter} not in daily bars)")
                
                if not df.empty:
                    # DataBento's definition schema returns definitions with instrument_id already included
                    # We've already deduplicated symbols, so we're only downloading each symbol ONCE
                    # This means we're making ONE API call per unique symbol, minimizing costs
                    # Store definitions directly (one per instrument_id)
                    inserted = store_definitions(con, df, force=args.force)
                    total_inserted += inserted
                    logger.info(f"  Stored {inserted} definitions from {len(batch_symbols)} unique symbols (ONE download per symbol)")
                else:
                    logger.warning(f"  No definitions matched our instrument IDs after filtering")
            
            # Rate limiting
            time.sleep(0.5)
        
        # Show summary
        logger.info("\n" + "=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        show_summary(con, args.root)
        
        logger.info(f"\nTotal definitions stored: {total_inserted}")
        logger.info("Complete!")
        
    finally:
        con.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

