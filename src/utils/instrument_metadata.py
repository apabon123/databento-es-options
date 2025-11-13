"""Utilities for extracting and storing instrument metadata (expiry dates, roll dates)."""
import logging
from datetime import date, datetime
from typing import Dict, Optional, Tuple
import pandas as pd

logger = logging.getLogger(__name__)

# Futures month codes (standard CME format)
MONTH_CODES = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
}

# Reverse mapping
MONTH_CODE_REVERSE = {v: k for k, v in MONTH_CODES.items()}


def parse_futures_symbol(native_symbol: str) -> Optional[Dict[str, any]]:
    """
    Parse a futures native symbol to extract root, month, and year.
    
    Examples:
        SR3H25 -> {'root': 'SR3', 'month': 3, 'year': 2025, 'month_code': 'H'}
        ESH6 -> {'root': 'ES', 'month': 3, 'year': 2026, 'month_code': 'H'}
        ZNH25 -> {'root': 'ZN', 'month': 3, 'year': 2025, 'month_code': 'H'}
    
    Returns None if symbol cannot be parsed.
    """
    if not native_symbol or len(native_symbol) < 3:
        return None
    
    # Try to match pattern: ROOT + MONTH_CODE + YEAR (e.g., SR3H25, ESH6)
    # Year can be 1 or 2 digits
    match = None
    for root_len in range(1, 5):  # Try roots of length 1-4
        if root_len >= len(native_symbol):
            break
        root = native_symbol[:root_len]
        remaining = native_symbol[root_len:]
        
        if len(remaining) >= 2:
            month_code = remaining[0].upper()
            year_str = remaining[1:]
            
            if month_code in MONTH_CODES and year_str.isdigit():
                month = MONTH_CODES[month_code]
                # Convert year: 2-digit years 00-50 = 2000-2050, 51-99 = 1951-1999
                year_int = int(year_str)
                if len(year_str) == 1:
                    # Single digit year (e.g., ESH6 = March 2026)
                    year = 2000 + year_int
                elif year_int <= 50:
                    year = 2000 + year_int
                else:
                    year = 1900 + year_int
                
                match = {
                    'root': root,
                    'month': month,
                    'year': year,
                    'month_code': month_code,
                    'native_symbol': native_symbol
                }
                break
    
    return match


def calculate_imm_date(month: int, year: int) -> date:
    """
    Calculate IMM date (3rd Wednesday of the month) for a given month and year.
    
    IMM dates are used for many futures contracts including:
    - Interest rate futures (SOFR, Eurodollar, Treasury futures)
    - Currency futures
    - Some equity index futures
    
    Args:
        month: Month (1-12)
        year: Year (4-digit)
    
    Returns:
        Date of the 3rd Wednesday of the month
    """
    # Find first day of the month
    first_day = date(year, month, 1)
    
    # Find first Wednesday (weekday 2 in Python: Monday=0, Wednesday=2)
    # If first day is Wednesday (2), add 0 days
    # If first day is Thursday (3), add 6 days to get to next Wednesday
    # etc.
    first_wednesday = first_day
    if first_day.weekday() < 2:  # Monday (0) or Tuesday (1)
        days_to_add = 2 - first_day.weekday()
    elif first_day.weekday() > 2:  # Thursday (3) through Sunday (6)
        days_to_add = 7 - (first_day.weekday() - 2)
    else:  # Already Wednesday
        days_to_add = 0
    
    first_wednesday = first_day + pd.Timedelta(days=days_to_add)
    
    # 3rd Wednesday is 14 days after the 1st Wednesday
    imm_date = first_wednesday + pd.Timedelta(days=14)
    
    return imm_date


def resolve_instrument_metadata(
    instrument_ids: list[int],
    client,
    dataset: str = "GLBX.MDP3",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[int, Dict[str, any]]:
    """
    Resolve instrument IDs to native symbols and extract metadata.
    
    Args:
        instrument_ids: List of instrument IDs to resolve
        client: DataBento Historical client
        dataset: Dataset name (default: GLBX.MDP3)
        start_date: Start date for symbology resolution
        end_date: End date for symbology resolution
    
    Returns:
        Dictionary mapping instrument_id to metadata:
        {
            instrument_id: {
                'native_symbol': str,
                'root': str,
                'month': int,
                'year': int,
                'expiry_date': date,  # IMM date if applicable
                'date_range': tuple(date, date)  # When this instrument was active
            }
        }
    """
    if not instrument_ids:
        return {}
    
    if start_date is None:
        start_date = date.today() - pd.Timedelta(days=365)
    if end_date is None:
        end_date = date.today()
    
    metadata = {}
    
    # Resolve in batches (DataBento may have limits)
    batch_size = 100
    for i in range(0, len(instrument_ids), batch_size):
        batch = instrument_ids[i:i + batch_size]
        try:
            result = client.symbology.resolve(
                dataset=dataset,
                symbols=[int(inst_id) for inst_id in batch],
                stype_in="instrument_id",
                stype_out="native",
                start_date=start_date,
                end_date=end_date,
            )
            
            # Parse result
            if isinstance(result, dict) and 'result' in result:
                for inst_id in batch:
                    inst_str = str(inst_id)
                    if inst_str in result['result']:
                        mappings = result['result'][inst_str]
                        if mappings:
                            mapping = mappings[0]  # Use first mapping
                            native_symbol = mapping.get('s', '')
                            d0 = mapping.get('d0', '')
                            d1 = mapping.get('d1', '')
                            
                            # Parse native symbol to get expiry info
                            parsed = parse_futures_symbol(native_symbol)
                            if parsed:
                                # Calculate expiry date (IMM date for SOFR/Treasury futures)
                                expiry_date = calculate_imm_date(parsed['month'], parsed['year'])
                                
                                metadata[inst_id] = {
                                    'native_symbol': native_symbol,
                                    'root': parsed['root'],
                                    'month': parsed['month'],
                                    'year': parsed['year'],
                                    'expiry_date': expiry_date,
                                    'date_range': (
                                        date.fromisoformat(d0) if d0 else None,
                                        date.fromisoformat(d1) if d1 else None
                                    )
                                }
                            else:
                                # Could not parse, store what we have
                                metadata[inst_id] = {
                                    'native_symbol': native_symbol,
                                    'root': None,
                                    'month': None,
                                    'year': None,
                                    'expiry_date': None,
                                    'date_range': (
                                        date.fromisoformat(d0) if d0 else None,
                                        date.fromisoformat(d1) if d1 else None
                                    )
                                }
        except Exception as e:
            logger.warning(f"Error resolving instrument IDs {batch}: {e}")
            continue
    
    return metadata


def detect_roll_dates(
    df: pd.DataFrame,
    rank: int,
    symbol_col: str = 'symbol',
    instrument_id_col: str = 'instrument_id',
    date_col: str = 'trading_date'
) -> pd.DataFrame:
    """
    Detect roll dates by finding when instrument_id changes for the same rank.
    
    Args:
        df: DataFrame with continuous contract data
        rank: Rank to analyze (e.g., 0 for front month)
        symbol_col: Column name for continuous symbol (e.g., 'SR3.c.0')
        instrument_id_col: Column name for underlying instrument_id
        date_col: Column name for trading date
    
    Returns:
        DataFrame with roll dates:
        columns: [date_col, 'old_instrument_id', 'new_instrument_id', 'roll_date']
    """
    # Filter to the specific rank
    rank_symbol = f'.c.{rank}'  # Assume calendar roll
    rank_df = df[df[symbol_col].str.contains(rank_symbol, na=False)].copy()
    
    if rank_df.empty:
        return pd.DataFrame()
    
    # Sort by date
    rank_df = rank_df.sort_values(date_col)
    
    # Group by date and get the instrument_id for that date
    daily_instruments = rank_df.groupby(date_col)[instrument_id_col].first()
    
    # Find where instrument_id changes
    roll_dates = []
    prev_instrument_id = None
    prev_date = None
    
    for current_date, current_instrument_id in daily_instruments.items():
        if prev_instrument_id is not None and current_instrument_id != prev_instrument_id:
            # Roll occurred
            roll_dates.append({
                date_col: current_date,
                'old_instrument_id': prev_instrument_id,
                'new_instrument_id': current_instrument_id,
                'roll_date': current_date
            })
        prev_instrument_id = current_instrument_id
        prev_date = current_date
    
    return pd.DataFrame(roll_dates)

