"""
Transform DataBento continuous futures data into the database-ready structure.
"""

import logging
from pathlib import Path
from datetime import date
import pandas as pd
import databento as db

logger = logging.getLogger(__name__)


def parse_continuous_symbol(symbol: str) -> dict:
    """
    Parse a DataBento continuous contract symbol.
    
    Examples:
        ES.c.0 -> {"root": "ES", "rank": 0}  # front month
        ES.c.1 -> {"root": "ES", "rank": 1}  # back month
    """
    parts = symbol.split(".")
    if len(parts) >= 3 and parts[1] == "c":
        return {
            "root": parts[0],
            "rank": int(parts[2]) if parts[2].isdigit() else 0,
        }
    return {"root": symbol, "rank": 0}


def transform_continuous_to_folder_structure(
    parquet_file: Path,
    output_base: Path,
    product: str = "ES_CONTINUOUS_MDP3",
    roll_rule: str = "2_days_pre_expiry",
) -> Path:
    """
    Transform a downloaded continuous futures parquet file into the folder structure
    expected by the ES_CONTINUOUS_MDP3 loader.
    
    Args:
        parquet_file: Path to the downloaded parquet file (BBO-1m data)
        output_base: Base directory for output (e.g., data/raw/glbx-mdp3-2025-10-20)
        product: Product identifier
        roll_rule: Roll rule description
        
    Returns:
        Path to the created directory
    """
    logger.info(f"Transforming {parquet_file.name} for {product}...")
    
    # Read the parquet file
    df = pd.read_parquet(parquet_file)
    
    if df.empty:
        logger.warning(f"Empty dataframe from {parquet_file}")
        return output_base
    
    logger.info(f"Processing {len(df)} rows, {df['symbol'].nunique()} unique symbols")
    
    # Filter for continuous contracts (ES.c.0, ES.c.1, etc.)
    continuous_mask = df['symbol'].str.contains(r'\.c\.\d+', regex=True, na=False)
    df_continuous = df[continuous_mask].copy()
    
    if df_continuous.empty:
        logger.warning("No continuous contract symbols found in data")
        return output_base
    
    logger.info(f"Kept {len(df_continuous)} rows with continuous contract symbols")
    
    # Parse symbols
    df_continuous['contract_series'] = df_continuous['symbol'].apply(
        lambda s: f"{parse_continuous_symbol(s)['root']}_FRONT_MONTH"
    )
    
    # Create output directories
    inst_dir = output_base / "continuous_instruments"
    quote_dir = output_base / "continuous_quotes_l1"
    trade_dir = output_base / "continuous_trades"
    
    inst_dir.mkdir(parents=True, exist_ok=True)
    quote_dir.mkdir(parents=True, exist_ok=True)
    trade_dir.mkdir(parents=True, exist_ok=True)
    
    # Extract date from filename for output filename
    # Assuming format like: glbx-mdp3-2025-10-20.bbo-1m.last5m.parquet
    try:
        date_part = parquet_file.stem.split('.')[0].split('-')[-3:]
        output_date = '-'.join(date_part)
    except:
        output_date = date.today().isoformat()
    
    # --- 1) Instrument definitions ---
    # Create one row per unique contract series
    unique_series = df_continuous['contract_series'].unique()
    inst_data = []
    
    for series in unique_series:
        parsed = parse_continuous_symbol(df_continuous[df_continuous['contract_series'] == series]['symbol'].iloc[0])
        inst_data.append({
            'contract_series': series,
            'root': parsed['root'],
            'roll_rule': roll_rule,
            'adjustment_method': 'unadjusted',  # DataBento provides unadjusted by default
            'description': f"{parsed['root']} continuous front month (roll {roll_rule})"
        })
    
    inst_df = pd.DataFrame(inst_data).drop_duplicates(subset=['contract_series'])
    inst_path = inst_dir / f"{output_date}.parquet"
    inst_df.to_parquet(inst_path, index=False)
    logger.info(f"Wrote {len(inst_df)} continuous contracts to {inst_path.relative_to(output_base.parent)}")
    
    # --- 2) Quotes ---
    # Map the continuous data to our schema
    # Note: DataBento BBO-1m files don't have ts_recv, use ts_event instead
    quote_df = pd.DataFrame({
        'ts_event': df_continuous['ts_event'],
        'ts_rcv': df_continuous.get('ts_recv', df_continuous['ts_event']),  # Use ts_event if ts_recv not available
        'contract_series': df_continuous['contract_series'],
        'underlying_instrument_id': df_continuous['instrument_id'],
        'bid_px': df_continuous['bid_px_00'],
        'bid_sz': df_continuous['bid_sz_00'],
        'ask_px': df_continuous['ask_px_00'],
        'ask_sz': df_continuous['ask_sz_00'],
    })
    
    quote_path = quote_dir / f"{output_date}.parquet"
    quote_df.to_parquet(quote_path, index=False)
    logger.info(f"Wrote {len(quote_df)} quotes to {quote_path.relative_to(output_base.parent)}")
    
    # --- 3) Trades (optional, usually not in BBO data) ---
    # Create empty directory to satisfy the loader
    # If we had trade data, we'd write it here
    
    logger.info(f"Transformation complete: {output_base.relative_to(output_base.parent.parent)}")
    return output_base


def get_continuous_symbol(root: str, rank: int = 0) -> str:
    """
    Get the DataBento continuous contract symbol.
    
    Args:
        root: Futures root symbol (e.g., "ES")
        rank: 0 for front month, 1 for back month, etc.
        
    Returns:
        DataBento continuous symbol (e.g., "ES.c.0")
    """
    return f"{root}.c.{rank}"


def transform_continuous_ohlcv_daily_to_folder_structure(
    parquet_file: Path,
    output_base: Path,
    product: str = "ES_CONTINUOUS_DAILY_MDP3",
    roll_rule: str = "2_days_pre_expiry",
) -> Path:
    """
    Transform a downloaded continuous futures daily OHLCV parquet file into the folder structure
    expected by the ES_CONTINUOUS_DAILY_MDP3 loader.
    
    Args:
        parquet_file: Path to the downloaded parquet file (ohlcv-daily data)
        output_base: Base directory for output (e.g., data/raw/glbx-mdp3-2025-10-20)
        product: Product identifier
        roll_rule: Roll rule description
        
    Returns:
        Path to the created directory
    """
    logger.info(f"Transforming {parquet_file.name} for {product}...")
    
    # Read the parquet file
    df = pd.read_parquet(parquet_file)
    
    if df.empty:
        logger.warning(f"Empty dataframe from {parquet_file}")
        return output_base
    
    logger.info(f"Processing {len(df)} rows, {df['symbol'].nunique()} unique symbols")
    
    # Filter for continuous contracts (ES.c.0, ES.c.1, etc.)
    continuous_mask = df['symbol'].str.contains(r'\.c\.\d+', regex=True, na=False)
    df_continuous = df[continuous_mask].copy()
    
    if df_continuous.empty:
        logger.warning("No continuous contract symbols found in data")
        return output_base
    
    logger.info(f"Kept {len(df_continuous)} rows with continuous contract symbols")
    
    # Parse symbols and create contract_series
    df_continuous['contract_series'] = df_continuous['symbol'].apply(
        lambda s: f"{parse_continuous_symbol(s)['root']}_FRONT_MONTH"
    )
    
    # Create output directories
    inst_dir = output_base / "continuous_instruments"
    bars_daily_dir = output_base / "continuous_bars_daily"
    
    inst_dir.mkdir(parents=True, exist_ok=True)
    bars_daily_dir.mkdir(parents=True, exist_ok=True)
    
    # Extract date from filename for output filename
    # Format: glbx-mdp3-{root}-YYYY-MM-DD.ohlcv-1d.fullday.parquet
    try:
        # Get the filename without extension
        filename_base = parquet_file.stem.split('.')[0]  # e.g., "glbx-mdp3-es-2025-01-06"
        # Extract the last 3 parts (YYYY-MM-DD)
        date_part = filename_base.split('-')[-3:]
        output_date = '-'.join(date_part)
        trading_date_from_filename = date.fromisoformat(output_date)
    except Exception as e:
        logger.warning(f"Could not extract date from filename {parquet_file.name}: {e}")
        output_date = date.today().isoformat()
        trading_date_from_filename = date.today()
    
    # --- 1) Instrument definitions ---
    # Create one row per unique contract series
    unique_series = df_continuous['contract_series'].unique()
    inst_data = []
    
    for series in unique_series:
        parsed = parse_continuous_symbol(df_continuous[df_continuous['contract_series'] == series]['symbol'].iloc[0])
        inst_data.append({
            'contract_series': series,
            'root': parsed['root'],
            'roll_rule': roll_rule,
            'adjustment_method': 'unadjusted',  # DataBento provides unadjusted by default
            'description': f"{parsed['root']} continuous front month (roll {roll_rule})"
        })
    
    inst_df = pd.DataFrame(inst_data).drop_duplicates(subset=['contract_series'])
    inst_path = inst_dir / f"{output_date}.parquet"
    inst_df.to_parquet(inst_path, index=False)
    logger.info(f"Wrote {len(inst_df)} continuous contracts to {inst_path.relative_to(output_base.parent)}")
    
    # --- 2) Daily Bars ---
    # Map the OHLCV data to our schema
    # For ohlcv-1d schema, DataBento doesn't include a date column, so we ALWAYS use the filename date
    # This is the authoritative source for the trading date
    logger.info(f"Using date from filename for OHLCV-1d data: {trading_date_from_filename}")
    df_continuous['trading_date'] = trading_date_from_filename
    
    # Map OHLCV columns - DataBento uses open, high, low, close, volume
    bar_df = pd.DataFrame({
        'trading_date': df_continuous['trading_date'],
        'contract_series': df_continuous['contract_series'],
        'underlying_instrument_id': df_continuous['instrument_id'],
        'open': df_continuous['open'],
        'high': df_continuous['high'],
        'low': df_continuous['low'],
        'close': df_continuous['close'],
        'volume': df_continuous['volume'].astype('int64'),
    })
    
    # Group by trading_date and contract_series in case there are multiple rows per day
    # (shouldn't happen with daily data, but just in case)
    bar_df = bar_df.groupby(['trading_date', 'contract_series'], as_index=False).agg({
        'underlying_instrument_id': 'first',
        'open': 'first',  # Use first open of the day
        'high': 'max',
        'low': 'min',
        'close': 'last',  # Use last close of the day
        'volume': 'sum',
    })
    
    bar_path = bars_daily_dir / f"{output_date}.parquet"
    bar_df.to_parquet(bar_path, index=False)
    logger.info(f"Wrote {len(bar_df)} daily bars to {bar_path.relative_to(output_base.parent)}")
    
    logger.info(f"Transformation complete: {output_base.relative_to(output_base.parent.parent)}")
    return output_base