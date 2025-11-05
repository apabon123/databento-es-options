"""
Transform downloaded BBO-1m data into database-ingestible format.
"""
from pathlib import Path
from datetime import date, timedelta
import os
import pandas as pd
import databento as db
import re
from typing import Optional

from src.utils.logging_config import get_logger
from src.download.bbo_downloader import DATASET

logger = get_logger(__name__)


def resolve_instrument_symbols(
    instrument_ids: list[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict[str, str]:
    """Resolve instrument IDs to raw symbols using DataBento symbology."""
    if not instrument_ids:
        return {}

    # Ensure IDs are strings
    inst_list = [str(i) for i in instrument_ids if i is not None]
    if not inst_list:
        return {}

    key = os.getenv("DATABENTO_API_KEY")
    try:
        client = db.Historical(key=key) if key else db.Historical()
    except Exception as exc:
        logger.warning(f"Could not initialize Historical client for symbology lookup: {exc}")
        return {}

    resolve_kwargs = {
        "dataset": DATASET,
        "symbols": inst_list,
        "stype_in": "instrument_id",
        "stype_out": "raw_symbol",
    }
    if start_date:
        resolve_kwargs["start_date"] = start_date
    if end_date:
        if start_date and end_date <= start_date:
            resolve_kwargs["end_date"] = start_date + timedelta(days=1)
        else:
            resolve_kwargs["end_date"] = end_date

    try:
        response = client.symbology.resolve(**resolve_kwargs)
    except Exception as exc:
        logger.warning(f"Failed to resolve instrument IDs {inst_list[:5]}...: {exc}")
        return {}

    if isinstance(response, dict):
        payload = response.get("result", {}) or {}
    else:
        payload = getattr(response, "result", {}) or {}

    mapping = {}
    for inst_id, entries in payload.items():
        if not entries:
            continue
        raw_symbol = entries[0].get("s")
        if raw_symbol:
            mapping[str(inst_id)] = raw_symbol
    return mapping


def parse_es_option_symbol(symbol: str) -> Optional[dict]:
    """
    Parse ES option symbol into components.
    Format: "ESZ5 C6000" = ES December 2025 Call at 6000 strike
    
    Returns:
        dict with root, expiry_month, expiry_year, put_call, strike, or None if not an option
    """
    # Pattern: ROOT + MONTH + YEAR + SPACE + C/P + STRIKE
    match = re.match(r'^([A-Z]+)([A-Z])(\d)\s+([CP])(\d+)$', symbol)
    if not match:
        return None
    
    root, month, year, opt_type, strike = match.groups()
    
    # Convert month code to number (F=Jan, G=Feb, ..., Z=Dec)
    month_codes = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                   'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}
    month_num = month_codes.get(month, 1)
    
    # Convert year (5 = 2025, 6 = 2026, etc.)
    year_full = 2020 + int(year)
    
    # ES options typically expire on the 3rd Friday of the month
    # For now, approximate as mid-month; can refine later
    expiry_date = date(year_full, month_num, 15)
    
    return {
        'root': root,
        'expiry': expiry_date,
        'strike': float(strike),
        'put_call': opt_type,
        'symbol_feed': symbol
    }


def parse_es_future_symbol(symbol: str) -> Optional[dict]:
    """
    Parse ES future symbol into components.
    Format: "ESZ5" = ES December 2025 future
    
    Returns:
        dict with root, expiry_month, expiry_year, or None if not a future
    """
    # Pattern: ROOT + MONTH + YEAR (no space, no C/P, no strike)
    match = re.match(r'^([A-Z]+)([A-Z])(\d)$', symbol)
    if not match:
        return None
    
    root, month, year = match.groups()
    
    # Convert month code to number
    month_codes = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                   'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}
    month_num = month_codes.get(month, 1)
    
    # Convert year
    year_full = 2020 + int(year)
    
    # Futures typically expire on the 3rd Friday
    expiry_date = date(year_full, month_num, 15)
    
    return {
        'root': root,
        'expiry': expiry_date,
        'symbol_feed': symbol
    }


def transform_bbo_to_folder_structure(dbn_file: Path, output_dir: Path, product: str = "ES_OPTIONS_MDP3"):
    """
    Transform a BBO-1m DBN file into the folder structure expected by database loader.
    
    Args:
        dbn_file: Path to the downloaded DBN file
        output_dir: Base directory for output (will create instruments/, quotes_l1/ subdirs)
        product: 'ES_OPTIONS_MDP3' or 'ES_FUTURES_MDP3'
    
    The output structure will be:
        output_dir/
            instruments/YYYY-MM-DD.parquet
            quotes_l1/YYYY-MM-DD.parquet
    """
    logger.info(f"Transforming {dbn_file.name} for {product}...")
    
    # Load DBN or parquet file
    if dbn_file.suffix.lower() == ".parquet":
        df = pd.read_parquet(dbn_file)
    else:
        store = db.DBNStore.from_file(str(dbn_file))
        df = store.to_df().reset_index()
    
    if df.empty:
        logger.warning(f"Empty file: {dbn_file}")
        return
    
    # Extract date from filename using regex (e.g., 2025-10-20)
    match = re.search(r"(\d{4}-\d{2}-\d{2})", dbn_file.name)
    if match:
        file_date = match.group(1)
        file_date_obj = date.fromisoformat(file_date)
    else:
        file_date = "unknown"
        file_date_obj = None
    
    logger.info(f"Processing {len(df)} rows, {df['symbol'].nunique()} unique symbols")

    # Resolve instrument IDs to canonical symbols when possible
    instrument_ids = df['instrument_id'].dropna().astype(str).unique().tolist() if 'instrument_id' in df.columns else []
    symbol_map = resolve_instrument_symbols(instrument_ids, start_date=file_date_obj, end_date=file_date_obj)
    if symbol_map:
        df['resolved_symbol'] = df['instrument_id'].astype(str).map(symbol_map)
    else:
        df['resolved_symbol'] = None
    df['resolved_symbol'] = df['resolved_symbol'].fillna(df.get('symbol'))

    if 'ts_recv' not in df.columns and 'ts_event' in df.columns:
        df['ts_recv'] = df['ts_event']

    # Parse symbols to determine instrument type
    df['parsed'] = df['resolved_symbol'].apply(
        parse_es_option_symbol if product == "ES_OPTIONS_MDP3" else parse_es_future_symbol
    )
    
    # Filter to valid symbols only
    df_valid = df[df['parsed'].notna()].copy()
    
    if df_valid.empty:
        logger.warning(f"No valid {product} symbols found in {dbn_file.name}")
        return
    
    logger.info(f"Kept {len(df_valid)} rows with valid {product} symbols")
    
    # Create output directories
    if product == "ES_OPTIONS_MDP3":
        inst_dir = output_dir / "instruments"
        quote_dir = output_dir / "quotes_l1"
    else:
        inst_dir = output_dir / "fut_instruments"
        quote_dir = output_dir / "fut_quotes_l1"
    
    inst_dir.mkdir(parents=True, exist_ok=True)
    quote_dir.mkdir(parents=True, exist_ok=True)
    
    # Build instruments table
    if product == "ES_OPTIONS_MDP3":
        instruments = []
        for _, row in df_valid.iterrows():
            parsed = row['parsed']
            instruments.append({
                'instrument_id': row['instrument_id'],
                'root': parsed['root'],
                'expiry': parsed['expiry'],
                'strike': parsed['strike'],
                'put_call': parsed['put_call'],
                'exerc_style': 'American',
                'multiplier': 50,
                'tick_size': 0.25,
                'symbol_feed': parsed['symbol_feed'],
                'symbol_canonical': parsed['symbol_feed']
            })
    else:  # ES_FUTURES_MDP3
        instruments = []
        for _, row in df_valid.iterrows():
            parsed = row['parsed']
            instruments.append({
                'instrument_id': row['instrument_id'],
                'root': parsed['root'],
                'expiry': parsed['expiry'],
                'symbol_feed': parsed['symbol_feed'],
                'symbol_canonical': parsed['symbol_feed'],
                'tick_size': 0.25,
                'multiplier': 50
            })
    
    inst_df = pd.DataFrame(instruments).drop_duplicates(subset=['instrument_id'])
    inst_file = inst_dir / f"{file_date}.parquet"
    inst_df.to_parquet(inst_file, index=False)
    logger.info(f"Wrote {len(inst_df)} instruments to {inst_file}")
    
    # Build quotes_l1 table
    quotes = []
    has_ts_recv = 'ts_recv' in df_valid.columns
    for _, row in df_valid.iterrows():
        quotes.append({
            'ts_event': row['ts_event'],
            'ts_rcv': row['ts_recv'] if has_ts_recv else row['ts_event'],
            'instrument_id': row['instrument_id'],
            'bid_px': row['bid_px_00'],
            'bid_sz': row['bid_sz_00'],
            'ask_px': row['ask_px_00'],
            'ask_sz': row['ask_sz_00']
        })
    
    quotes_df = pd.DataFrame(quotes)
    quote_file = quote_dir / f"{file_date}.parquet"
    quotes_df.to_parquet(quote_file, index=False)
    logger.info(f"Wrote {len(quotes_df)} quotes to {quote_file}")
    
    # No trades in BBO data, but create empty directory for consistency
    if product == "ES_OPTIONS_MDP3":
        trade_dir = output_dir / "trades"
    else:
        trade_dir = output_dir / "fut_trades"
    trade_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Transformation complete: {output_dir}")


def transform_all_dbn_files(product: str = "ES_OPTIONS_MDP3"):
    """
    Transform all DBN files in data/raw into the proper folder structure.
    
    Args:
        product: 'ES_OPTIONS_MDP3' or 'ES_FUTURES_MDP3'
    
    Creates:
        data/raw/glbx-mdp3-YYYY-MM-DD/
            instruments/*.parquet (or fut_instruments/)
            quotes_l1/*.parquet (or fut_quotes_l1/)
            trades/ (empty, for consistency)
    """
    from pipelines.common import get_paths
    
    bronze, _, _ = get_paths()
    
    # Find all DBN files
    dbn_files = list(bronze.glob("glbx-mdp3-*.dbn*"))
    
    if not dbn_files:
        logger.warning(f"No DBN files found in {bronze}")
        return []
    
    logger.info(f"Found {len(dbn_files)} DBN files to transform")
    
    transformed_dirs = []
    
    for dbn_file in dbn_files:
        # Extract date from filename
        parts = dbn_file.stem.replace('.dbn', '').replace('.zst', '').split('-')
        if len(parts) >= 4:
            date_str = f"{parts[2]}-{parts[3]}-{parts[4].split('.')[0]}"
        else:
            date_str = "unknown"
        
        # Create output directory: data/raw/glbx-mdp3-YYYY-MM-DD/
        output_dir = bronze / f"glbx-mdp3-{date_str}"
        
        if output_dir.exists():
            logger.info(f"Skipping {dbn_file.name} - already transformed to {output_dir.name}")
            transformed_dirs.append(output_dir)
            continue
        
        try:
            transform_bbo_to_folder_structure(dbn_file, output_dir, product)
            transformed_dirs.append(output_dir)
        except Exception as e:
            logger.error(f"Failed to transform {dbn_file.name}: {e}")
            continue
    
    return transformed_dirs

