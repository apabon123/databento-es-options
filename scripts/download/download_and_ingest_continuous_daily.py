"""
Download and ingest ES continuous futures daily OHLCV data (front month, 2-day pre-expiry roll).

Usage:
    python scripts/download/download_and_ingest_continuous_daily.py --weeks 1
    python scripts/download/download_and_ingest_continuous_daily.py --start 2025-09-01 --end 2025-09-30
    python scripts/download/download_and_ingest_continuous_daily.py --summary
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
from src.utils.env import load_env

load_env()

from pipelines.common import get_paths, connect_duckdb
from pipelines.loader import load as load_product
import databento as db
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
ROOT = "ES"
ROLL_RULE = "2_days_pre_expiry"

try:
    from zoneinfo import ZoneInfo
    UTC = ZoneInfo("UTC")
    CHI = ZoneInfo("America/Chicago")
except Exception:
    import pytz
    UTC = pytz.UTC
    CHI = pytz.timezone("America/Chicago")


def load_api_key():
    """Load DataBento API key from .env file."""
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("No API key found. Set DATABENTO_API_KEY in your environment or .env file at project root.")
    
    return api_key


def full_day_window_utc(trade_date: date) -> tuple[datetime, datetime]:
    """Get full trading day window in UTC for a given trade date."""
    # CME RTH is 8:30 AM - 3:00 PM CT
    start_ct = datetime.combine(trade_date, datetime.min.time().replace(hour=8, minute=30))
    end_ct = datetime.combine(trade_date, datetime.min.time().replace(hour=15, minute=0))
    
    # Convert to UTC (CT is UTC-6 in winter, UTC-5 in summer)
    start_utc = CHI.localize(start_ct).astimezone(UTC).replace(tzinfo=None)
    end_utc = CHI.localize(end_ct).astimezone(UTC).replace(tzinfo=None)
    
    return start_utc, end_utc


def day_iter(d0: date, d1: date):
    """Iterate over trading days (Mon-Fri) in the range."""
    current = d0
    while current <= d1:
        if current.weekday() < 5:  # Monday-Friday
            yield current
        current += timedelta(days=1)


def download_continuous_daily(client, start_d: date, end_d: date, force: bool = False, yes: bool = False):
    """Download continuous futures daily OHLCV data for the specified date range. Returns list of downloaded files."""
    
    # Get continuous symbol (front month, 2-day pre-expiry roll)
    symbol = get_continuous_symbol(ROOT, rank=0)
    
    logger.info(f"Downloading continuous futures daily OHLCV: {symbol} (roll rule: {ROLL_RULE})")
    logger.info(f"Date range: {start_d} to {end_d}")
    logger.info(f"Schema: {SCHEMA}")
    
    # Check for existing dates unless forcing
    new_dates = None
    if not force:
        existing_dates = get_existing_dates_in_db(PRODUCT)
        
        # Generate requested date range
        all_dates = []
        current = start_d
        while current <= end_d:
            if current.weekday() < 5:  # Monday-Friday
                all_dates.append(current)
            current += timedelta(days=1)
        
        # Filter out existing dates
        new_dates = [d for d in all_dates if d not in existing_dates]
        
        if not new_dates:
            logger.info("All requested dates already in database. Use --force to re-download.")
            return []
        
        logger.info(f"Found {len(existing_dates)} existing dates in database")
        logger.info(f"Will download {len(new_dates)} new dates")
        start_d = min(new_dates)
        end_d = max(new_dates)
    
    # Get output directory
    OUT_DIR, _, _ = get_paths()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    downloaded_files = []
    
    # Download each day
    for trade_date in day_iter(start_d, end_d):
        logger.info(f"Downloading {trade_date.isoformat()}...")
        
        try:
            # Get full day window
            start_utc, end_utc = full_day_window_utc(trade_date)
            
            # Download daily OHLCV data
            data = client.timeseries.get_range(
                dataset=DATASET,
                schema=SCHEMA,
                symbols=[symbol],
                start=start_utc,
                end=end_utc,
                stype_in="continuous",
            )
            
            df = data.to_df()
            
            if df.empty:
                logger.warning(f"No data for {trade_date.isoformat()}")
                continue
            
            logger.info(f"Received {len(df)} rows for {trade_date.isoformat()}")
            
            # Save to parquet
            out_file = OUT_DIR / f"glbx-mdp3-{trade_date.isoformat()}.{SCHEMA}.fullday.parquet"
            df.to_parquet(out_file, index=False)
            downloaded_files.append(out_file)
            logger.info(f"Saved to {out_file.name}")
        
        except Exception as e:
            logger.error(f"Error downloading {trade_date.isoformat()}: {e}")
            continue
    
    logger.info(f"Downloaded {len(downloaded_files)} files")
    return downloaded_files


def transform_and_ingest(downloaded_files: list):
    """Transform downloaded files and ingest into database."""
    bronze, _, _ = get_paths()
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 2: TRANSFORM DATA")
    logger.info("=" * 80)
    
    transformed_dirs = []
    
    for parquet_file in downloaded_files:
        # Extract date from filename
        # Format: glbx-mdp3-2025-10-20.ohlcv-daily.fullday.parquet
        try:
            date_part = parquet_file.stem.split('.')[0].split('-')[-3:]
            date_str = '-'.join(date_part)
            output_dir = bronze / f"glbx-mdp3-{date_str}"
        except:
            logger.error(f"Could not parse date from {parquet_file.name}")
            continue
        
        try:
            logger.info(f"Transforming {parquet_file.name}...")
            results = transform_continuous_ohlcv_daily_to_folder_structure(
                parquet_file,
                output_dir,
                product=PRODUCT,
                roll_rule=ROLL_RULE,
                roll_strategy="calendar-2d",
                output_mode="legacy",
                re_transform=re_transform,
            )
            transformed_dirs.extend(results)
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
    
    # Load product config
    from pipelines.registry import get_product_config
    product_cfg = get_product_config(PRODUCT)
    
    # Ingest each directory
    for source_dir in sorted(transformed_dirs):
        # Extract date from directory name
        try:
            date_str = source_dir.name.split('-')[-3:]
            ingest_date = '-'.join(date_str)
        except:
            ingest_date = None
        
        logger.info(f"Ingesting {source_dir.name}...")
        try:
            _, _, dbpath = get_paths()
            con = connect_duckdb(dbpath)
            load_product(con, source_dir, ingest_date, product_cfg)
            con.close()
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
        
        # Get date range
        date_range = con.execute("""
            SELECT 
                MIN(trading_date) as min_date,
                MAX(trading_date) as max_date,
                COUNT(*) as total_bars
            FROM g_continuous_bar_daily
        """).fetchdf()
        
        if date_range['total_bars'].iloc[0] == 0:
            print("No daily bar data in database yet.")
            return
        
        contract_count = con.execute("SELECT COUNT(*) as cnt FROM dim_continuous_contract").fetchdf()['cnt'].iloc[0]
        
        print("\n" + "=" * 80)
        print("DATABASE SUMMARY - ES CONTINUOUS FUTURES DAILY BARS")
        print("=" * 80)
        print(f"Product: {PRODUCT}")
        print(f"Roll rule: {ROLL_RULE}")
        print(f"Symbol: {get_continuous_symbol(ROOT, 0)}")
        print()
        print("Data Coverage:")
        print(f"  Date range: {date_range['min_date'].iloc[0]} to {date_range['max_date'].iloc[0]}")
        print(f"  Total daily bars: {date_range['total_bars'].iloc[0]:,}")
        print(f"  Contract series: {contract_count}")
        print()
        
        # Show sample data
        sample = con.execute("""
            SELECT * 
            FROM g_continuous_bar_daily 
            ORDER BY trading_date DESC 
            LIMIT 5
        """).fetchdf()
        
        print("Sample data (last 5 days):")
        print(sample.to_string(index=False))
        print()
        
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Download and ingest ES continuous futures daily OHLCV data (front month, 2-day pre-expiry roll)"
    )
    parser.add_argument("--weeks", type=int, help="Number of weeks back from today to download")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Re-download even if data exists")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")
    parser.add_argument("--summary", action="store_true", help="Show database summary and exit")
    parser.add_argument("--no-ingest", action="store_true", help="Download only, don't ingest")
    
    args = parser.parse_args()
    
    if args.summary:
        show_summary()
        return
    
    # Determine date range
    if args.weeks:
        end_d = date.today()
        start_d = end_d - timedelta(weeks=args.weeks)
    elif args.start and args.end:
        start_d = date.fromisoformat(args.start)
        end_d = date.fromisoformat(args.end)
    else:
        parser.error("Must specify --weeks or both --start and --end")
    
    if start_d > end_d:
        parser.error("Start date must be before end date")
    
    logger.info(f"ES Continuous Futures Daily OHLCV Download & Ingest")
    logger.info(f"Roll rule: {ROLL_RULE}")
    logger.info(f"Symbol: {get_continuous_symbol(ROOT, 0)}")
    logger.info(f"Date range: {start_d} to {end_d}")
    
    # Load API key
    api_key = load_api_key()
    client = db.Historical(key=api_key)
    
    # Download
    logger.info("\n" + "=" * 80)
    logger.info("STEP 1: DOWNLOAD DATA")
    logger.info("=" * 80)
    
    downloaded_files = download_continuous_daily(
        client,
        start_d=start_d,
        end_d=end_d,
        force=args.force,
        yes=args.yes
    )
    
    if not downloaded_files:
        logger.info("No files downloaded")
        return
    
    # Transform and ingest
    if not args.no_ingest:
        transform_and_ingest(downloaded_files)
    
    logger.info("\n" + "=" * 80)
    logger.info("COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

