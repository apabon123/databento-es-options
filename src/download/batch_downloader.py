"""
Batch downloader for continuous futures data - optimized for large date ranges.
Downloads in monthly chunks to avoid timeout issues with per-day downloads.
"""

import logging
from pathlib import Path
from datetime import date, datetime, timedelta
import pandas as pd
import databento as db
from typing import List

from src.download.bbo_downloader import full_day_window_utc, DATASET, SCHEMA
from pipelines.common import get_paths

logger = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
    UTC = ZoneInfo("UTC")
except Exception:
    import pytz
    UTC = pytz.UTC


def get_month_ranges(start_d: date, end_d: date) -> List[tuple]:
    """Split a date range into monthly chunks for batch downloading."""
    chunks = []
    current = start_d
    
    while current <= end_d:
        # Start of chunk
        chunk_start = current
        
        # End of chunk: last day of current month or end_d, whichever is earlier
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        
        last_day_of_month = next_month - timedelta(days=1)
        chunk_end = min(last_day_of_month, end_d)
        
        chunks.append((chunk_start, chunk_end))
        current = next_month
    
    return chunks


def download_batch_continuous(
    client: db.Historical,
    symbols: List[str],
    start_d: date,
    end_d: date,
    stype_in: str = "continuous"
) -> List[Path]:
    """
    Download continuous futures data in monthly batches, then split into daily files.
    
    This is much more efficient than per-day downloads:
    - Fewer API calls (months instead of days)
    - Less prone to timeouts
    - Faster overall execution
    
    Returns:
        List of daily parquet files created
    """
    # Get configured output directory from .env
    OUT_DIR, _, _ = get_paths()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    downloaded_files = []
    
    # Split date range into monthly chunks
    month_chunks = get_month_ranges(start_d, end_d)
    logger.info(f"Split {start_d} to {end_d} into {len(month_chunks)} monthly chunks")
    
    for i, (chunk_start, chunk_end) in enumerate(month_chunks, 1):
        logger.info(f"Downloading chunk {i}/{len(month_chunks)}: {chunk_start} to {chunk_end}")
        
        # Calculate UTC window for the entire chunk
        # Start: beginning of first trading day
        first_day_start, _ = full_day_window_utc(chunk_start)
        # End: end of last trading day
        _, last_day_end = full_day_window_utc(chunk_end)
        
        try:
            # Download entire month with a single API call
            logger.info(f"  API call: {first_day_start.isoformat()} -> {last_day_end.isoformat()}")
            data = client.timeseries.get_range(
                dataset=DATASET,
                schema=SCHEMA,
                symbols=symbols,
                start=first_day_start,
                end=last_day_end,
                stype_in=stype_in,
            )
            
            df = data.to_df()
            logger.info(f"  Received {len(df):,} rows for {(chunk_end - chunk_start).days + 1} days")
            
            if df.empty:
                logger.warning(f"  No data for chunk {chunk_start} to {chunk_end}")
                continue
            
            # Ensure ts_event is datetime
            if 'ts_event' in df.columns:
                df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True)
            
            # Split the data by trading day and save individual files
            df['trading_date'] = df['ts_event'].dt.date
            
            for trading_date in df['trading_date'].unique():
                # Filter data for this day
                day_data = df[df['trading_date'] == trading_date].copy()
                day_data = day_data.drop(columns=['trading_date'])
                
                # Convert date to proper format
                if isinstance(trading_date, pd.Timestamp):
                    trading_date = trading_date.date()
                
                # Save daily file
                out_file = OUT_DIR / f"glbx-mdp3-{trading_date.isoformat()}.{SCHEMA}.fullday.parquet"
                day_data.to_parquet(out_file, index=False)
                downloaded_files.append(out_file)
                logger.info(f"  Saved {trading_date.isoformat()}: {len(day_data):,} rows")
        
        except db.common.error.BentoServerError as e:
            if "504" in str(e) or "timeout" in str(e).lower():
                logger.error(f"  Timeout downloading chunk {chunk_start} to {chunk_end}")
                logger.info(f"  Skipping this chunk - you can retry later with: --start {chunk_start} --end {chunk_end}")
            else:
                logger.error(f"  API error for chunk {chunk_start} to {chunk_end}: {e}")
            continue
        
        except Exception as e:
            logger.error(f"  Error processing chunk {chunk_start} to {chunk_end}: {e}")
            continue
    
    logger.info(f"Batch download complete: {len(downloaded_files)} daily files created")
    return downloaded_files

