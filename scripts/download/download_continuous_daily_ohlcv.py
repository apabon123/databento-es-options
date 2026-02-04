"""
Download and ingest continuous futures daily OHLCV data from DataBento.

This script supports multiple roots (ES, NQ, ZN, etc.) and multiple roll strategies
(calendar-2d, volume, open-interest, etc.).

Usage:
    # Download ES and NQ with calendar-2d roll for 2025
    python scripts/download/download_continuous_daily_ohlcv.py --roots ES,NQ --roll calendar-2d --start 2025-01-01 --end 2025-11-05

    # Download ZN with volume roll
    python scripts/download/download_continuous_daily_ohlcv.py --roots ZN --roll volume --start 2025-01-01 --end 2025-11-05 --yes

    # Download multiple roots with different rolls (separate calls needed)
    python scripts/download/download_continuous_daily_ohlcv.py --roots ZN --roll volume --weeks 1
    
    # Show database summary
    python scripts/download/download_continuous_daily_ohlcv.py --summary
"""

import sys
import logging
from pathlib import Path
from datetime import date, timedelta, datetime
import argparse
import pandas as pd
import time
import shutil

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.continuous_transform import transform_continuous_ohlcv_daily_to_folder_structure, get_continuous_symbol
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
PRODUCT = "ES_CONTINUOUS_DAILY_MDP3"  # Product name for ingestion (works for all continuous)
DATASET = "GLBX.MDP3"
SCHEMA = "ohlcv-1d"  # Daily OHLCV bars

import pytz
UTC = pytz.UTC
CHI = pytz.timezone("America/Chicago")

# Roll strategy mappings
ROLL_STRATEGIES = {
    'calendar-2d': {
        'api_suffix': '.c.0',  # DataBento API uses .c.0 for calendar roll
        'folder': 'calendar-2d',
        'db_suffix': 'CALENDAR_2D'
    },
    'volume': {
        'api_suffix': '.v.0',  # DataBento API uses .v.0 for volume roll
        'folder': 'volume',
        'db_suffix': 'VOLUME'
    },
    'calendar-1d': {
        'api_suffix': '.c.0',  # Same API endpoint, different folder
        'folder': 'calendar-1d',
        'db_suffix': 'CALENDAR_1D'
    }
}

def load_api_key():
    """Load DataBento API key from .env file."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise ValueError("DATABENTO_API_KEY not found in environment or .env file")
    return api_key

def get_month_ranges(start_d, end_d):
    """Split date range into quarterly chunks for batch downloading."""
    chunks = []
    current = start_d
    while current <= end_d:
        # Get end of quarter (3 months)
        chunk_end = min(current + timedelta(days=90), end_d)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks

def full_day_window(trade_date):
    """Return (start, end) UTC for a full Chicago trading day.
    
    For ohlcv-1d schema, the end parameter is exclusive, so we add 1 day.
    """
    return (trade_date, trade_date + timedelta(days=1))

def download_continuous_daily(root, roll_strategy, start_d, end_d):
    """Download continuous futures daily OHLCV data for a given root and roll strategy."""
    api_key = load_api_key()
    client = db.Historical(api_key)
    
    # Get roll strategy config
    roll_config = ROLL_STRATEGIES[roll_strategy]
    
    # Build DataBento symbol
    symbol = f"{root}{roll_config['api_suffix']}"
    
    logger.info(f"Downloading continuous futures daily OHLCV: {symbol}")
    logger.info(f"Roll strategy: {roll_strategy} (folder: {roll_config['folder']})")
    logger.info(f"Date range: {start_d} to {end_d}")
    
    # Get output directory - organized by schema, root, and roll strategy
    bronze_root, _, _ = get_paths()
    OUT_DIR = bronze_root / "ohlcv-1d" / "downloads" / root.lower() / roll_config['folder']
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Split date range into quarterly chunks for batch downloading
    month_chunks = get_month_ranges(start_d, end_d)
    logger.info(f"Downloading in {len(month_chunks)} quarterly batch(es)")
    
    downloaded_files = []
    for chunk_start, chunk_end in month_chunks:
        logger.info(f"  Batch: {chunk_start} to {chunk_end}")
        
        try:
            # For ohlcv-1d, end is exclusive, so add 1 day
            data = client.timeseries.get_range(
                dataset=DATASET,
                symbols=[symbol],
                schema=SCHEMA,
                stype_in="continuous",
                start=chunk_start,
                end=chunk_end + timedelta(days=1),  # exclusive end
            )
            
            # Save to parquet
            # The filename should be: glbx-mdp3-{root}-{date}.ohlcv-1d.fullday.parquet
            for d in pd.date_range(chunk_start, chunk_end):
                date_str = d.strftime('%Y-%m-%d')
                filename = f"glbx-mdp3-{root.lower()}-{date_str}.{SCHEMA}.fullday.parquet"
                output_path = OUT_DIR / filename
                
                # Filter data for this specific date
                df = data.to_df()
                if not df.empty:
                    day_data = df[df.index.date == d.date()]
                    if not day_data.empty:
                        day_data.to_parquet(output_path)
                        downloaded_files.append(output_path)
                        logger.info(f"    Saved {filename}")
            
        except Exception as e:
            logger.error(f"  Error downloading {chunk_start} to {chunk_end}: {e}")
            continue
    
    logger.info(f"Downloaded {len(downloaded_files)} parquet files for {root}")
    return {root.upper(): downloaded_files}

def transform_and_ingest(downloaded_by_root, roll_strategy, re_transform=False):
    """Transform downloaded parquet files and ingest into database."""
    bronze, _, _ = get_paths()
    roll_config = ROLL_STRATEGIES[roll_strategy]
    
    for root, parquet_files in downloaded_by_root.items():
        logger.info(f"Processing {len(parquet_files)} parquet files for {root}...")
        
        for parquet_file in parquet_files:
            try:
                # Extract date from filename: glbx-mdp3-{root}-YYYY-MM-DD
                parts = parquet_file.stem.split('.')
                filename_parts = parts[0].split('-')
                date_part = filename_parts[-3:]
                date_str = '-'.join(date_part)
                
                # Save to organized structure: ohlcv-1d/transformed/{root}/{roll_strategy}/{date}/
                output_dir = bronze / "ohlcv-1d" / "transformed" / root.lower() / roll_config['folder'] / date_str
                
                if output_dir.exists() and not re_transform:
                    logger.info(f"  Skipping {date_str} (already transformed)")
                    continue
                
                # Delete existing if re-transforming
                if output_dir.exists() and re_transform:
                    logger.info(f"  Deleting existing {output_dir} for re-transform...")
                    try:
                        shutil.rmtree(output_dir, ignore_errors=True)
                        time.sleep(0.1)  # Brief pause for file system
                    except Exception as e:
                        logger.warning(f"  Could not delete {output_dir}: {e}")
                
                # Transform
                logger.info(f"  Transforming {parquet_file.name}...")
                transform_continuous_ohlcv_daily_to_folder_structure(
                    parquet_file=parquet_file,
                    output_base=output_dir,
                    product=PRODUCT,
                    roll_rule=roll_config['db_suffix'],
                    roll_strategy=roll_strategy,
                    output_mode="legacy",
                    re_transform=re_transform,
                )
                logger.info(f"    Saved to {output_dir}")
                
            except Exception as e:
                logger.error(f"  Error transforming {parquet_file.name}: {e}")
                continue
    
    # Ingest all transformed directories
    logger.info("=" * 80)
    logger.info("INGESTING TRANSFORMED DATA")
    logger.info("=" * 80)
    
    # Find all transformed directories for this roll strategy
    transformed_dirs = []
    ohlcv_transformed = bronze / "ohlcv-1d" / "transformed"
    if ohlcv_transformed.exists():
        for root_dir in ohlcv_transformed.glob("*"):
            if root_dir.is_dir():
                roll_dir = root_dir / roll_config['folder']
                if roll_dir.exists():
                    for date_dir in roll_dir.glob("*"):
                        if date_dir.is_dir() and (date_dir / 'continuous_bars_daily').exists():
                            transformed_dirs.append(date_dir)
    
    logger.info(f"Found {len(transformed_dirs)} transformed directories to ingest")
    
    # Run migrations
    from orchestrator import migrate
    logger.info("Running migrations...")
    migrate()
    
    # Ingest each directory
    for source_dir in sorted(transformed_dirs):
        try:
            # Extract date from directory name
            parts = source_dir.name.split('-')
            date_str = '-'.join(parts[-3:]) if len(parts) >= 3 else source_dir.name
        except:
            date_str = None
        
        logger.info(f"Ingesting {source_dir.name}...")
        try:
            load(PRODUCT, source_dir, date_str)
            logger.info(f"  Ingested {source_dir.name}")
        except Exception as e:
            logger.error(f"  Failed to ingest {source_dir.name}: {e}")
            continue
    
    logger.info("Ingestion complete!")

def show_summary():
    """Show database summary for continuous daily futures."""
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    
    print("\n" + "=" * 80)
    print("DATABASE SUMMARY - CONTINUOUS FUTURES DAILY BARS")
    print("=" * 80)
    print(f"Product: {PRODUCT}")
    
    # Overall coverage
    result = con.execute("""
        SELECT 
            MIN(trading_date) as first_date,
            MAX(trading_date) as last_date,
            COUNT(*) as total_bars,
            COUNT(DISTINCT contract_series) as unique_series
        FROM g_continuous_bar_daily
    """).fetchone()
    
    print(f"\nOverall Coverage:")
    print(f"  Date range: {result[0]} to {result[1]}")
    print(f"  Total daily bars: {result[2]}")
    print(f"  Unique contract series: {result[3]}")
    
    # Coverage by root
    result = con.execute("""
        SELECT 
            c.root,
            COUNT(*) as bar_count,
            MIN(b.trading_date) as first_date,
            MAX(b.trading_date) as last_date
        FROM g_continuous_bar_daily b
        JOIN dim_continuous_contract c ON b.contract_series = c.contract_series
        GROUP BY c.root
        ORDER BY c.root
    """).fetchall()
    
    print(f"\nCoverage by Root:")
    for row in result:
        print(f"  {row[0]}:")
        print(f"    Bars: {row[1]}")
        print(f"    Date range: {row[2]} to {row[3]}")
    
    # Sample data
    df = con.execute("""
        SELECT c.root, b.trading_date, b.open, b.high, b.low, b.close, b.volume
        FROM g_continuous_bar_daily b
        JOIN dim_continuous_contract c ON b.contract_series = c.contract_series
        ORDER BY b.trading_date DESC, c.root
        LIMIT 10
    """).df()
    
    print(f"\nSample data (last 10 bars):")
    print(df.to_string(index=False))
    print("\n")
    
    con.close()

def main():
    parser = argparse.ArgumentParser(description="Download and ingest continuous futures daily OHLCV data")
    parser.add_argument('--roots', default='ES,NQ', help='Comma-separated list of futures roots (default: ES,NQ)')
    parser.add_argument('--roll', '--roll-strategy', dest='roll_strategy', 
                       choices=list(ROLL_STRATEGIES.keys()), 
                       default='calendar-2d',
                       help='Roll strategy (default: calendar-2d)')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD, defaults to today)')
    parser.add_argument('--weeks', type=int, help='Download last N weeks')
    parser.add_argument('--yes', action='store_true', help='Auto-confirm cost estimate')
    parser.add_argument('--summary', action='store_true', help='Show database summary')
    parser.add_argument('--re-transform', action='store_true', help='Re-transform existing parquet files')
    parser.add_argument('--ingest-only', action='store_true', help='Ingest existing transformed directories only')
    
    args = parser.parse_args()
    
    # Show summary and exit
    if args.summary:
        show_summary()
        return
    
    # Parse roots
    roots = [r.strip().upper() for r in args.roots.split(',')]
    
    # Determine date range
    if args.weeks:
        end_d = date.today()
        start_d = end_d - timedelta(weeks=args.weeks)
    elif args.start:
        start_d = date.fromisoformat(args.start)
        end_d = date.fromisoformat(args.end) if args.end else date.today()
    else:
        parser.error("Must specify either --weeks or --start")
    
    logger.info(f"Roots: {', '.join(roots)}")
    logger.info(f"Roll strategy: {args.roll_strategy}")
    logger.info(f"Date range: {start_d} to {end_d}")
    
    # Download and ingest for each root
    all_downloaded = {}
    for root in roots:
        downloaded = download_continuous_daily(root, args.roll_strategy, start_d, end_d)
        all_downloaded.update(downloaded)
    
    # Transform and ingest
    if all_downloaded:
        transform_and_ingest(all_downloaded, args.roll_strategy)

if __name__ == "__main__":
    main()

