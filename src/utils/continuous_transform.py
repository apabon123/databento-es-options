"""
Transform DataBento continuous futures data into the database-ready structure.
"""

import logging
from pathlib import Path
from datetime import date
import pandas as pd

logger = logging.getLogger(__name__)


def _normalize_roll_strategy(roll_strategy: str) -> str:
    """Normalize roll strategy names for contract series identifiers."""
    return roll_strategy.replace("-", "_").replace(" ", "_").upper()


def make_contract_series(root: str, roll_strategy: str, rank: int = 0) -> str:
    """
    Build a contract series identifier incorporating the roll strategy.

    Args:
        root: Futures root symbol (e.g., "ES").
        roll_strategy: Roll strategy slug (e.g., "calendar-2d", "volume").
        rank: Contract rank (0 = front month).

    Returns:
        Contract series identifier, e.g., "ES_FRONT_CALENDAR_2D".
    """
    strategy_tag = _normalize_roll_strategy(roll_strategy)
    prefix = "FRONT" if rank == 0 else f"RANK_{rank}"
    return f"{root}_{prefix}_{strategy_tag}"


def parse_continuous_symbol(symbol: str) -> dict:
    """
    Parse a DataBento continuous contract symbol.
    
    Examples:
        ES.c.0 -> {"root": "ES", "rank": 0}  # front month
        ES.c.1 -> {"root": "ES", "rank": 1}  # back month
    """
    parts = symbol.split(".")
    if len(parts) >= 3 and parts[1] in {"c", "v", "o"}:
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
    roll_strategy: str = "calendar-2d",
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
    code = _roll_strategy_to_code(roll_strategy)
    continuous_mask = df['symbol'].str.contains(rf'\.{code}\.\d+', regex=True, na=False)
    df_continuous = df[continuous_mask].copy()
    
    if df_continuous.empty:
        logger.warning("No continuous contract symbols found in data")
        return output_base
    
    logger.info(f"Kept {len(df_continuous)} rows with continuous contract symbols")
    
    # Parse symbols
    parsed_series = df_continuous['symbol'].apply(parse_continuous_symbol)
    df_continuous['_root'] = parsed_series.apply(lambda p: p['root'])
    df_continuous['_rank'] = parsed_series.apply(lambda p: p.get('rank', 0))
    df_continuous['contract_series'] = df_continuous.apply(
        lambda row: make_contract_series(row['_root'], roll_strategy, row['_rank']),
        axis=1,
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
    inst_rows = []
    for series, group in df_continuous.groupby('contract_series'):
        root = group['_root'].iloc[0]
        rank = int(group['_rank'].iloc[0])
        description_rank = "front month" if rank == 0 else f"rank {rank}"
        inst_rows.append({
            'contract_series': series,
            'root': root,
            'roll_rule': roll_rule,
            'adjustment_method': 'unadjusted',  # DataBento provides unadjusted by default
            'description': f"{root} continuous {description_rank} (roll strategy: {roll_strategy}, rule: {roll_rule})"
        })
    
    inst_df = pd.DataFrame(inst_rows).drop_duplicates(subset=['contract_series'])
    df_continuous = df_continuous.drop(columns=['_root', '_rank'])
    df_continuous = df_continuous.drop(columns=['_root', '_rank'])
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


def _roll_strategy_to_code(roll_strategy: str) -> str:
    """Map roll strategy identifiers to DataBento continuous symbol codes."""
    normalized = roll_strategy.lower()
    if normalized.startswith("calendar"):
        return "c"
    if normalized.startswith("volume"):
        return "v"
    if normalized.startswith("open-interest") or normalized.startswith("open_interest"):
        return "o"
    # Default to calendar roll if unknown
    return "c"


def get_continuous_symbol(root: str, rank: int = 0, roll_strategy: str = "calendar-2d") -> str:
    """
    Get the DataBento continuous contract symbol.
    
    Args:
        root: Futures root symbol (e.g., "ES")
        rank: 0 for front month, 1 for back month, etc.
        roll_strategy: Roll strategy identifier (e.g., "calendar-2d", "volume")
        
    Returns:
        DataBento continuous symbol (e.g., "ES.c.0")
    """
    code = _roll_strategy_to_code(roll_strategy)
    return f"{root}.{code}.{rank}"


def transform_continuous_ohlcv_daily_to_folder_structure(
    parquet_file: Path,
    output_base: Path,
    product: str = "ES_CONTINUOUS_DAILY_MDP3",
    roll_rule: str = "2_days_pre_expiry",
    roll_strategy: str = "calendar-2d",
    output_mode: str = "legacy",
    re_transform: bool = False,
) -> list[Path]:
    """
    Transform a downloaded continuous futures daily OHLCV parquet file into the folder structure
    expected by the ES_CONTINUOUS_DAILY_MDP3 loader.
    
    Args:
        parquet_file: Path to the downloaded parquet file (ohlcv-daily data)
        output_base: Base directory for output (e.g., data/raw/glbx-mdp3-2025-10-20)
        product: Product identifier
        roll_rule: Roll rule description
        
    Returns:
        List of directories created (one per contract series when partitioned, otherwise single entry)
    """
    logger.info(f"Transforming {parquet_file.name} for {product}...")
    
    # Read the parquet file
    df = pd.read_parquet(parquet_file)
    
    if df.empty:
        logger.warning(f"Empty dataframe from {parquet_file}")
        return output_base
    
    logger.info(f"Processing {len(df)} rows, {df['symbol'].nunique()} unique symbols")
    
    # Filter for continuous contracts (ES.c.0, ES.c.1, etc.)
    code = _roll_strategy_to_code(roll_strategy)
    continuous_mask = df['symbol'].str.contains(rf'\.{code}\.\d+', regex=True, na=False)
    df_continuous = df[continuous_mask].copy()
    
    if df_continuous.empty:
        logger.warning("No continuous contract symbols found in data")
        return output_base
    
    logger.info(f"Kept {len(df_continuous)} rows with continuous contract symbols")
    
    # Parse symbols and create contract_series
    parsed_series = df_continuous['symbol'].apply(parse_continuous_symbol)
    df_continuous['_root'] = parsed_series.apply(lambda p: p['root'])
    df_continuous['_rank'] = parsed_series.apply(lambda p: p.get('rank', 0))
    df_continuous['contract_series'] = df_continuous.apply(
        lambda row: make_contract_series(row['_root'], roll_strategy, row['_rank']),
        axis=1,
    )
    
    try:
        filename_base = parquet_file.stem.split('.')[0]
        date_part = filename_base.split('-')[-3:]
        output_date = '-'.join(date_part)
        trading_date_from_filename = date.fromisoformat(output_date)
    except Exception as e:
        logger.warning(f"Could not extract date from filename {parquet_file.name}: {e}")
        output_date = date.today().isoformat()
        trading_date_from_filename = date.today()
    
    outputs: list[Path] = []
    
    if output_mode == "legacy":
        if output_base.exists() and not re_transform:
            logger.info("  ↺ Skipping transformation (exists and re_transform=False)")
            return [output_base]
        
        inst_dir = output_base / "continuous_instruments"
        bars_daily_dir = output_base / "continuous_bars_daily"
        inst_dir.mkdir(parents=True, exist_ok=True)
        bars_daily_dir.mkdir(parents=True, exist_ok=True)
        
        inst_rows = []
        for series, group in df_continuous.groupby('contract_series'):
            root = group['_root'].iloc[0]
            rank = int(group['_rank'].iloc[0])
            description_rank = "front month" if rank == 0 else f"rank {rank}"
            inst_rows.append({
                'contract_series': series,
                'root': root,
                'roll_rule': roll_rule,
                'adjustment_method': 'unadjusted',
                'description': f"{root} continuous {description_rank} (roll strategy: {roll_strategy}, rule: {roll_rule})"
            })
        
        inst_df = pd.DataFrame(inst_rows).drop_duplicates(subset=['contract_series'])
        inst_path = inst_dir / f"{output_date}.parquet"
        inst_df.to_parquet(inst_path, index=False)
        logger.info(f"Wrote {len(inst_df)} continuous contracts to {inst_path.relative_to(output_base.parent)}")
        
        df_continuous['trading_date'] = trading_date_from_filename
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
        bar_df = bar_df.groupby(['trading_date', 'contract_series'], as_index=False).agg({
            'underlying_instrument_id': 'first',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        })
        bar_path = bars_daily_dir / f"{output_date}.parquet"
        bar_df.to_parquet(bar_path, index=False)
        logger.info(f"Wrote {len(bar_df)} daily bars to {bar_path.relative_to(output_base.parent)}")
        outputs.append(output_base)
    else:
        for series, group in df_continuous.groupby('contract_series'):
            root_val = group['_root'].iloc[0]
            rank_val = int(group['_rank'].iloc[0])
            symbol_val = group['symbol'].iloc[0]
            description_rank = "front month" if rank_val == 0 else f"rank {rank_val}"
            
            target_dir = output_base / f"rank={rank_val}" / output_date
            if target_dir.exists() and not re_transform:
                logger.debug(f"  ↺ Skipping {target_dir} (exists and re_transform=False)")
                outputs.append(target_dir)
                continue
            
            if target_dir.exists() and re_transform:
                logger.debug(f"  ⟳ Re-transforming {target_dir}")
                import shutil
                shutil.rmtree(target_dir, ignore_errors=True)
            
            inst_dir = target_dir / "continuous_instruments"
            bars_daily_dir = target_dir / "continuous_bars_daily"
            inst_dir.mkdir(parents=True, exist_ok=True)
            bars_daily_dir.mkdir(parents=True, exist_ok=True)
            
            inst_df = pd.DataFrame([{
                'contract_series': series,
                'root': root_val,
                'rank': rank_val,
                'roll_rule': roll_rule,
                'roll_strategy': roll_strategy,
                'adjustment_method': 'unadjusted',
                'description': f"{root_val} continuous {description_rank} (roll strategy: {roll_strategy}, rule: {roll_rule})"
            }])
            inst_path = inst_dir / f"{output_date}.parquet"
            inst_df.to_parquet(inst_path, index=False)
            logger.info(f"Wrote contract metadata to {inst_path.relative_to(output_base)}")
            
            agg_row = {
                'trading_date': trading_date_from_filename,
                'root': root_val,
                'symbol': symbol_val,
                'rank': rank_val,
                'db_symbol': series,
                'contract_series': series,
                'roll_strategy': roll_strategy,
                'underlying_instrument_id': group['instrument_id'].iloc[0],
                'open': group['open'].iloc[0],
                'high': group['high'].max(),
                'low': group['low'].min(),
                'close': group['close'].iloc[-1],
                'volume': int(group['volume'].sum()),
            }
            bar_df = pd.DataFrame([agg_row])
            bar_path = bars_daily_dir / f"{output_date}.parquet"
            bar_df.to_parquet(bar_path, index=False)
            logger.info(f"Wrote daily bars to {bar_path.relative_to(output_base)}")
            
            outputs.append(target_dir)
    
    logger.info("Transformation complete")
    return outputs