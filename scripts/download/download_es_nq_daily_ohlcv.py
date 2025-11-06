"""
Download and ingest ES and NQ continuous futures daily OHLCV data from DataBento.

This script downloads daily OHLCV bars for ES and NQ continuous futures front month
with a configurable roll strategy (default: calendar-2d = 2-day pre-expiry calendar roll),
transforms them, and ingests them into the database.

Roll strategies are encoded in the folder structure:
    ohlcv-1d/downloads/{root}/calendar-2d/   (2-day pre-expiry)
    ohlcv-1d/downloads/{root}/volume/        (volume-based roll)
    etc.

Usage:
    # Download 2025 data (Jan 1 to today) for both ES and NQ
    python scripts/download/download_es_nq_daily_ohlcv.py --start 2025-01-01 --end 2025-11-05
    
    # Download with auto-confirmation
    python scripts/download/download_es_nq_daily_ohlcv.py --start 2025-01-01 --end 2025-11-05 --yes
    
    # Download for specific roots
    python scripts/download/download_es_nq_daily_ohlcv.py --start 2025-01-01 --roots ES,NQ
    
    # View database summary
    python scripts/download/download_es_nq_daily_ohlcv.py --summary
"""

import sys
import logging
from pathlib import Path
from datetime import date, timedelta, datetime
import argparse
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # Go up 3 levels: download -> scripts -> project root
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.continuous_transform import transform_continuous_ohlcv_daily_to_folder_structure, get_continuous_symbol
from src.utils.db_utils import get_existing_dates_in_db, get_db_summary
from pipelines.common import get_paths, connect_duckdb
from pipelines.loader import load
import databento as db
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
PRODUCT = "ES_CONTINUOUS_DAILY_MDP3"
DATASET = "GLBX.MDP3"
SCHEMA = "ohlcv-1d"  # Daily OHLCV bars
DEFAULT_ROOTS = ["ES", "NQ"]  # Both ES and NQ
ROLL_RULE = "2_days_pre_expiry"

import pytz

UTC = pytz.UTC
CHI = pytz.timezone("America/Chicago")


def load_api_key():
    """Load DataBento API key from .env file."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()
    
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("No API key found. Set DATABENTO_API_KEY in your environment or .env file at project root.")
    
    return api_key


def full_day_window(trade_date: date) -> tuple[date, date]:
    """Get date range for a given trade date (for ohlcv-1d schema, use date to next day)."""
    # For daily OHLCV data, we need start_date to end_date+1 (end is exclusive)
    next_day = trade_date + timedelta(days=1)
    return trade_date, next_day


def get_month_ranges(start_d: date, end_d: date) -> list[tuple[date, date]]:
    """
    Split a date range into chunks for batch downloading.
    For OHLCV-1d data, DataBento can handle very large date ranges efficiently,
    so we use quarterly chunks (or even larger) instead of monthly.
    """
    chunks = []
    current = start_d
    
    while current <= end_d:
        # Start of chunk
        chunk_start = current
        
        # Use quarterly chunks (3 months) for better efficiency
        # DataBento can handle much larger ranges, so quarterly is safe
        if current.month <= 3:
            # Q1: Jan-Mar
            chunk_end = min(date(current.year, 3, 31), end_d)
            next_quarter = date(current.year, 4, 1)
        elif current.month <= 6:
            # Q2: Apr-Jun
            chunk_end = min(date(current.year, 6, 30), end_d)
            next_quarter = date(current.year, 7, 1)
        elif current.month <= 9:
            # Q3: Jul-Sep
            chunk_end = min(date(current.year, 9, 30), end_d)
            next_quarter = date(current.year, 10, 1)
        else:
            # Q4: Oct-Dec
            chunk_end = min(date(current.year, 12, 31), end_d)
            next_quarter = date(current.year + 1, 1, 1)
        
        chunks.append((chunk_start, chunk_end))
        current = next_quarter
    
    return chunks


def day_iter(d0: date, d1: date):
    """Iterate over trading days (Mon-Fri) in the range."""
    current = d0
    while current <= d1:
        if current.weekday() < 5:  # Monday-Friday
            yield current
        current += timedelta(days=1)


def download_continuous_daily_multi_root(
    client, 
    roots: list[str], 
    start_d: date, 
    end_d: date, 
    force: bool = False, 
    yes: bool = False
):
    """
    Download continuous futures daily OHLCV data for multiple roots.
    Downloads in monthly batches for efficiency (one API call per month instead of per day).
    
    Args:
        client: DataBento client
        roots: List of root symbols (e.g., ["ES", "NQ"])
        start_d: Start date
        end_d: End date
        force: Force re-download even if data exists
        yes: Skip confirmation prompts
        
    Returns:
        Dictionary mapping root -> list of downloaded files
    """
    
    downloaded_by_root = {root: [] for root in roots}
    
    for root in roots:
        logger.info("=" * 80)
        logger.info(f"Processing {root}")
        logger.info("=" * 80)
        
        # Get continuous symbol (front month, 2-day pre-expiry roll)
        symbol = get_continuous_symbol(root, rank=0)
        
        logger.info(f"Downloading continuous futures daily OHLCV: {symbol} (roll rule: {ROLL_RULE})")
        logger.info(f"Date range: {start_d} to {end_d}")
        logger.info(f"Schema: {SCHEMA}")
        
        # Get output directory - organized by schema, root, and roll strategy
        bronze_root, _, _ = get_paths()
        roll_strategy = DEFAULT_ROLL_STRATEGY  # Use default roll strategy
        OUT_DIR = bronze_root / "ohlcv-1d" / "downloads" / root.lower() / roll_strategy
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Split date range into quarterly chunks for batch downloading
        month_chunks = get_month_ranges(start_d, end_d)
        logger.info(f"Downloading in {len(month_chunks)} quarterly batch(es)")
        
        for i, (chunk_start, chunk_end) in enumerate(month_chunks, 1):
            logger.info(f"\nBatch {i}/{len(month_chunks)}: {chunk_start} to {chunk_end}")
            
            try:
                # Get list of trading days in this chunk
                trading_days = list(day_iter(chunk_start, chunk_end))
                logger.info(f"  Trading days in chunk: {len(trading_days)}")
                
                if not trading_days:
                    logger.warning(f"  No trading days in chunk {chunk_start} to {chunk_end}")
                    continue
                
                # Download entire month with a single API call
                # For ohlcv-1d, we use dates (start of first day to start of day after last day)
                chunk_start_date, _ = full_day_window(chunk_start)
                _, chunk_end_date = full_day_window(chunk_end)
                
                logger.info(f"  API call: {chunk_start_date} to {chunk_end_date}")
                data = client.timeseries.get_range(
                    dataset=DATASET,
                    schema=SCHEMA,
                    symbols=[symbol],
                    start=chunk_start_date,
                    end=chunk_end_date,
                    stype_in="continuous",
                )
                
                df = data.to_df()
                logger.info(f"  Received {len(df)} rows for {len(trading_days)} trading days")
                
                if df.empty:
                    logger.warning(f"  No data for {root} in chunk {chunk_start} to {chunk_end}")
                    continue
                
                # OHLCV-1d returns one row per trading day per symbol
                # Since there's no date column, we match rows to trading days by order
                # DataBento returns rows in chronological order
                if len(df) != len(trading_days):
                    logger.warning(f"  Expected {len(trading_days)} rows (one per trading day), got {len(df)}")
                    logger.warning(f"  This may indicate missing data for some days")
                
                # Assign dates to rows and save one file per day
                for idx, trade_date in enumerate(trading_days):
                    if idx < len(df):
                        # Get the row for this trading day
                        day_df = df.iloc[[idx]].copy()  # Keep as DataFrame
                        
                        # Save to parquet with root in filename
                        out_file = OUT_DIR / f"glbx-mdp3-{root.lower()}-{trade_date.isoformat()}.{SCHEMA}.fullday.parquet"
                        day_df.to_parquet(out_file, index=False)
                        downloaded_by_root[root].append(out_file)
                        logger.info(f"  Saved {trade_date.isoformat()}: {len(day_df)} row(s)")
                    else:
                        logger.warning(f"  No data for {trade_date.isoformat()} (missing row)")
            
            except Exception as e:
                logger.error(f"  Error downloading {root} for chunk {chunk_start} to {chunk_end}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                continue
        
        logger.info(f"\nDownloaded {len(downloaded_by_root[root])} files for {root}")
    
    # Summary
    total_files = sum(len(files) for files in downloaded_by_root.values())
    logger.info("\n" + "=" * 80)
    logger.info(f"DOWNLOAD COMPLETE: {total_files} total files")
    for root, files in downloaded_by_root.items():
        logger.info(f"  {root}: {len(files)} files")
    logger.info("=" * 80)
    
    return downloaded_by_root


def transform_and_ingest(downloaded_by_root: dict, transformed_dirs_override: list = None, re_transform: bool = False):
    """Transform downloaded files and ingest into database."""
    bronze_root, _, _ = get_paths()
    bronze = bronze_root  # For backwards compatibility
    
    # If we have a list of already-transformed directories, use those
    if transformed_dirs_override:
        transformed_dirs = transformed_dirs_override
        logger.info(f"Using {len(transformed_dirs)} existing transformed directories")
    else:
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: TRANSFORM DATA")
        logger.info("=" * 80)
        
        transformed_dirs = []
        
        # Process all downloaded files
        for root, parquet_files in downloaded_by_root.items():
            logger.info(f"\nTransforming {root} files...")
            
            for parquet_file in parquet_files:
                # Extract date from filename
                # Format: glbx-mdp3-es-2025-10-20.ohlcv-1d.fullday.parquet
                try:
                    parts = parquet_file.stem.split('.')
                    filename_parts = parts[0].split('-')
                    # filename is: glbx-mdp3-{root}-YYYY-MM-DD
                    # So we need to extract the last 3 parts for the date
                    date_part = filename_parts[-3:]
                    date_str = '-'.join(date_part)
                    # Save to organized structure: ohlcv-1d/transformed/{root}/{roll_strategy}/{date}/
                    roll_strategy = DEFAULT_ROLL_STRATEGY
                    output_dir = bronze / "ohlcv-1d" / "transformed" / root.lower() / roll_strategy / date_str
                except Exception as e:
                    logger.error(f"Could not parse date from {parquet_file.name}: {e}")
                    continue
                
                if output_dir.exists() and not re_transform:
                    logger.info(f"  {output_dir.name} already exists, skipping transformation")
                    transformed_dirs.append(output_dir)
                    continue
                elif output_dir.exists() and re_transform:
                    logger.info(f"  {output_dir.name} exists, re-transforming (--re-transform flag)")
                    # Remove existing directory to force re-transformation
                    # Use ignore_errors=True to handle locked files (e.g., OneDrive sync)
                    import shutil
                    import time
                    try:
                        shutil.rmtree(output_dir)
                    except PermissionError:
                        logger.warning(f"  Permission denied deleting {output_dir.name}, trying again with ignore_errors...")
                        shutil.rmtree(output_dir, ignore_errors=True)
                        time.sleep(0.1)  # Brief pause for file system
                
                try:
                    logger.info(f"Transforming {parquet_file.name}...")
                    transform_continuous_ohlcv_daily_to_folder_structure(
                        parquet_file,
                        output_dir,
                        product=PRODUCT,
                        roll_rule=ROLL_RULE
                    )
                    transformed_dirs.append(output_dir)
                except Exception as e:
                    logger.error(f"  Failed to transform {parquet_file.name}: {e}")
                    continue
    
    if not transformed_dirs:
        logger.warning("No directories to ingest")
        return
    
    logger.info(f"Transformed {len(transformed_dirs)} directories")
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 3: INGEST INTO DATABASE")
    logger.info("=" * 80)
    
    # Run migrations
    from orchestrator import migrate
    logger.info("Running migrations...")
    migrate()
    
    # Ingest each directory
    for source_dir in sorted(transformed_dirs):
        # Extract date from directory name
        try:
            # Directory name: glbx-mdp3-{root}-YYYY-MM-DD
            parts = source_dir.name.split('-')
            date_str = '-'.join(parts[-3:])
        except:
            date_str = None
        
        logger.info(f"Ingesting {source_dir.name}...")
        try:
            # Use the loader infrastructure which handles connection and product config
            load(PRODUCT, source_dir, date_str)
            logger.info(f"  ✓ Ingested {source_dir.name}")
        except Exception as e:
            logger.error(f"  ✗ Failed to ingest {source_dir.name}: {e}")
            continue
    
    logger.info("Ingestion complete!")


def show_summary():
    """Show database summary for continuous daily futures."""
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    
    try:
        # Check if daily bar table exists
        result = con.execute("""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_name = 'g_continuous_bar_daily'
        """).fetchone()
        
        if not result or result[0] == 0:
            print("Daily bar table not found. Run migration first:")
            print("  python orchestrator.py migrate")
            return
        
        # Get overall stats
        overall_stats = con.execute("""
            SELECT 
                MIN(trading_date) as min_date,
                MAX(trading_date) as max_date,
                COUNT(*) as total_bars,
                COUNT(DISTINCT contract_series) as unique_series
            FROM g_continuous_bar_daily
        """).fetchdf()
        
        if overall_stats['total_bars'].iloc[0] == 0:
            print("No daily bar data in database yet.")
            return
        
        # Get stats by root
        root_stats = con.execute("""
            SELECT 
                c.root,
                COUNT(*) as bar_count,
                MIN(g.trading_date) as min_date,
                MAX(g.trading_date) as max_date
            FROM g_continuous_bar_daily g
            JOIN dim_continuous_contract c ON g.contract_series = c.contract_series
            GROUP BY c.root
            ORDER BY c.root
        """).fetchdf()
        
        print("\n" + "=" * 80)
        print("DATABASE SUMMARY - CONTINUOUS FUTURES DAILY BARS")
        print("=" * 80)
        print(f"Product: {PRODUCT}")
        print(f"Roll rule: {ROLL_RULE}")
        print()
        print("Overall Coverage:")
        print(f"  Date range: {overall_stats['min_date'].iloc[0]} to {overall_stats['max_date'].iloc[0]}")
        print(f"  Total daily bars: {overall_stats['total_bars'].iloc[0]:,}")
        print(f"  Unique contract series: {overall_stats['unique_series'].iloc[0]}")
        print()
        
        if not root_stats.empty:
            print("Coverage by Root:")
            for _, row in root_stats.iterrows():
                print(f"  {row['root']}:")
                print(f"    Bars: {row['bar_count']:,}")
                print(f"    Date range: {row['min_date']} to {row['max_date']}")
            print()
        
        # Show sample data
        sample = con.execute("""
            SELECT 
                c.root,
                g.trading_date,
                g.open,
                g.high,
                g.low,
                g.close,
                g.volume
            FROM g_continuous_bar_daily g
            JOIN dim_continuous_contract c ON g.contract_series = c.contract_series
            ORDER BY g.trading_date DESC, c.root
            LIMIT 10
        """).fetchdf()
        
        print("Sample data (last 10 bars):")
        print(sample.to_string(index=False))
        print()
        
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Download and ingest ES & NQ continuous futures daily OHLCV data (front month, 2-day pre-expiry roll)"
    )
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)", default="2025-01-01")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--roots", type=str, help="Comma-separated list of roots (default: ES,NQ)", default="ES,NQ")
    parser.add_argument("--force", action="store_true", help="Re-download even if data exists")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")
    parser.add_argument("--summary", action="store_true", help="Show database summary and exit")
    parser.add_argument("--no-ingest", action="store_true", help="Download only, don't ingest")
    parser.add_argument("--ingest-only", action="store_true", help="Ingest existing transformed directories only (no download, no transform)")
    parser.add_argument("--re-transform", action="store_true", help="Re-transform all existing parquet files (deletes existing transformed dirs)")
    
    args = parser.parse_args()
    
    if args.summary:
        show_summary()
        return
    
    # If re-transform, find all parquet files and re-transform them
    if args.re_transform:
        logger.info("=" * 80)
        logger.info("RE-TRANSFORM EXISTING PARQUET FILES")
        logger.info("=" * 80)
        
        bronze, _, _ = get_paths()
        
        # Find all parquet files in the organized structure
        ohlcv_downloads = bronze / "ohlcv-1d" / "downloads"
        parquet_files = []
        if ohlcv_downloads.exists():
            # New structure: root/roll_strategy/*.parquet
            for root_dir in ohlcv_downloads.glob("*"):
                if root_dir.is_dir():
                    for roll_dir in root_dir.glob("*"):
                        if roll_dir.is_dir():
                            parquet_files.extend(roll_dir.glob('glbx-mdp3-*.ohlcv-1d.fullday.parquet'))
        
        if not parquet_files:
            logger.warning("No parquet files found to re-transform")
            return
        
        logger.info(f"Found {len(parquet_files)} parquet files to re-transform")
        
        # Group by root
        downloaded_by_root = {}
        for parquet_file in parquet_files:
            # Extract root from filename: glbx-mdp3-{root}-YYYY-MM-DD
            parts = parquet_file.stem.split('.')[0].split('-')
            if len(parts) >= 3:
                root = parts[2].upper()  # e.g., "es" or "nq"
                if root not in downloaded_by_root:
                    downloaded_by_root[root] = []
                downloaded_by_root[root].append(parquet_file)
        
        logger.info(f"Grouped into {len(downloaded_by_root)} roots: {list(downloaded_by_root.keys())}")
        
        # Re-transform and ingest
        transform_and_ingest(downloaded_by_root, re_transform=True)
        return
    
    # If ingest-only, just run ingestion on existing transformed directories
    if args.ingest_only:
        logger.info("=" * 80)
        logger.info("INGEST EXISTING TRANSFORMED DIRECTORIES")
        logger.info("=" * 80)
        
        # Find all transformed directories in the organized structure
        bronze, _, _ = get_paths()
        ohlcv_transformed = bronze / "ohlcv-1d" / "transformed"
        
        # Find directories in new structure: root/roll_strategy/date/
        transformed_dirs = []
        if ohlcv_transformed.exists():
            for root_dir in ohlcv_transformed.glob("*"):
                if root_dir.is_dir():
                    for roll_dir in root_dir.glob("*"):
                        if roll_dir.is_dir():
                            for date_dir in roll_dir.glob("*"):
                                if date_dir.is_dir() and (date_dir / 'continuous_bars_daily').exists():
                                    transformed_dirs.append(date_dir)
        
        if not transformed_dirs:
            logger.warning("No transformed directories found to ingest")
            return
        
        logger.info(f"Found {len(transformed_dirs)} transformed directories to ingest")
        transform_and_ingest({}, transformed_dirs_override=transformed_dirs)
        return
    
    # Parse roots
    roots = [r.strip().upper() for r in args.roots.split(',')]
    
    # Determine date range
    start_d = date.fromisoformat(args.start)
    if args.end:
        end_d = date.fromisoformat(args.end)
    else:
        end_d = date.today()
    
    if start_d > end_d:
        parser.error("Start date must be before end date")
    
    logger.info("=" * 80)
    logger.info("ES & NQ Continuous Futures Daily OHLCV Download & Ingest")
    logger.info("=" * 80)
    logger.info(f"Roots: {', '.join(roots)}")
    logger.info(f"Roll rule: {ROLL_RULE}")
    logger.info(f"Date range: {start_d} to {end_d}")
    logger.info(f"Symbols: {', '.join([get_continuous_symbol(r, 0) for r in roots])}")
    logger.info("=" * 80)
    
    # Estimate trading days
    trading_days = sum(1 for _ in day_iter(start_d, end_d))
    logger.info(f"Estimated trading days: {trading_days}")
    logger.info(f"Total downloads: {trading_days * len(roots)} (across all roots)")
    
    if not args.yes:
        response = input("\nProceed with download? [y/N]: ")
        if response.lower() != 'y':
            logger.info("Download cancelled")
            return
    
    # Load API key
    api_key = load_api_key()
    client = db.Historical(key=api_key)
    
    # Download
    logger.info("\n" + "=" * 80)
    logger.info("STEP 1: DOWNLOAD DATA")
    logger.info("=" * 80)
    
    downloaded_by_root = download_continuous_daily_multi_root(
        client,
        roots=roots,
        start_d=start_d,
        end_d=end_d,
        force=args.force,
        yes=args.yes
    )
    
    total_files = sum(len(files) for files in downloaded_by_root.values())
    if total_files == 0:
        logger.info("No files downloaded")
        return
    
    # Transform and ingest
    if not args.no_ingest:
        transform_and_ingest(downloaded_by_root, re_transform=args.re_transform)
    
    logger.info("\n" + "=" * 80)
    logger.info("COMPLETE")
    logger.info("=" * 80)
    
    # Show summary
    logger.info("\nRun with --summary to view database contents:")
    logger.info(f"  python {Path(__file__).name} --summary")


if __name__ == "__main__":
    main()

