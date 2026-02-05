"""
Download and ingest ES continuous futures data (front month, 2-day pre-expiry roll).

Usage:
    python scripts/download/download_and_ingest_continuous.py --weeks 1
    python scripts/download/download_and_ingest_continuous.py --start 2025-09-01 --end 2025-09-30
    python scripts/download/download_and_ingest_continuous.py --summary
"""

import sys
import logging
from pathlib import Path
from datetime import date, timedelta
import argparse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # Go up 3 levels: download -> scripts -> project root
sys.path.insert(0, str(PROJECT_ROOT))

from src.download.bbo_downloader import download_bbo_last_window, estimate_cost, day_iter
from src.download.batch_downloader import download_batch_continuous
from src.utils.continuous_transform import transform_continuous_to_folder_structure, get_continuous_symbol
from src.utils.db_utils import get_existing_dates_in_db, get_db_summary
from src.utils.env import load_env

load_env()

from pipelines.common import get_paths
import databento as db
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
PRODUCT = "ES_CONTINUOUS_MDP3"
DATASET = "GLBX.MDP3"
ROOT = "ES"
ROLL_RULE = "2_days_pre_expiry"


def load_api_key():
    """Load DataBento API key from .env file."""
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("No API key found. Set DATABENTO_API_KEY in your environment or .env file at project root.")
    
    return api_key


def download_continuous(client, start_d: date, end_d: date, minutes: int = 5, force: bool = False, yes: bool = False, full_day: bool = True):
    """Download continuous futures data for the specified date range. Returns DataFrame of downloaded files."""
    
    # Get continuous symbol (front month, 2-day pre-expiry roll)
    # DataBento uses roll_rule parameter in the request
    symbol = get_continuous_symbol(ROOT, rank=0)
    
    if full_day:
        logger.info(f"Downloading continuous futures: {symbol} (roll rule: {ROLL_RULE}) - FULL DAY")
    else:
        logger.info(f"Downloading continuous futures: {symbol} (roll rule: {ROLL_RULE}) - Last {minutes} minutes")
    logger.info(f"Date range: {start_d} to {end_d}")
    
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
    
    # Estimate cost (skip for large date ranges to avoid hanging)
    num_days = len(new_dates) if new_dates is not None else len([d for d in day_iter(start_d, end_d) if d.weekday() < 5])
    
    if num_days > 30 and yes:
        # For large date ranges with --yes, skip detailed cost estimation to avoid hanging
        logger.info(f"Skipping detailed cost estimation for {num_days} days (large range)")
        logger.info(f"Rough estimate: ~${num_days * 0.002:.2f} USD (based on ~$0.002/day)")
        logger.info("Proceeding with download...")
    else:
        logger.info("Estimating cost...")
        try:
            cost_df, total_bytes, total_usd = estimate_cost(
                client,
                symbols=[symbol],
                start_d=start_d,
                end_d=end_d,
                minutes=minutes,
                stype_in="continuous",  # Important: specify continuous symbol type
                full_day=full_day
            )
            
            if cost_df.empty or total_usd == 0:
                logger.warning("Cost estimate returned $0. No data available for this range.")
                return []
            
            # Convert cost_df to display format
            cost_df['size_mb'] = cost_df['size_bytes'].astype(float) / 1e6
            cost_df = cost_df.rename(columns={'cost_usd': 'cost'})
            cost_df = cost_df[['date', 'size_mb', 'cost']]
            
            print("\nCost Estimate:")
            print(cost_df.to_string(index=False))
            print(f"\nEstimated total size: {total_bytes/1e6:.3f} MB")
            print(f"Estimated total cost: ${total_usd:.2f} USD")
            
            # Prompt for confirmation unless --yes
            if not yes:
                response = input(f"\nProceed to download {len(cost_df)} days for ${total_usd:.2f}? [y/N] ")
                if response.lower() != 'y':
                    logger.info("Download cancelled by user")
                    return []
        
        except Exception as e:
            logger.error(f"Cost estimation failed: {e}")
            if not yes:
                response = input("Continue without cost estimate? [y/N] ")
                if response.lower() != 'y':
                    return []
    
    # Download the data using batch downloader (much more efficient for large date ranges)
    logger.info("Downloading continuous futures data using batch downloader...")
    logger.info("This downloads in monthly chunks to avoid timeouts")
    downloaded_files = download_batch_continuous(
        client,
        symbols=[symbol],
        start_d=start_d,
        end_d=end_d,
        stype_in="continuous"
    )
    
    logger.info(f"Downloaded {len(downloaded_files)} files")
    return downloaded_files


def transform_and_ingest(downloaded_files: list):
    """Transform and ingest downloaded files into the database."""
    from pipelines.common import get_paths
    from pipelines.loader import load, apply_gold_sql
    from pipelines.registry import get_product
    
    if not downloaded_files:
        logger.info("No files to transform")
        return
    
    logger.info(f"Transforming {len(downloaded_files)} downloaded files...")
    
    # Get paths (returns: bronze, gold, dbpath)
    raw_dir, _, db_path = get_paths()
    
    # Transform each file
    transformed_dirs = []
    for dbn_file in downloaded_files:
        # Extract date from filename
        # e.g., glbx-mdp3-2025-10-20.bbo-1m.last5m.parquet or glbx-mdp3-2025-10-20.bbo-1m.fullday.parquet
        try:
            # Handle both .last5m.parquet and .fullday.parquet formats
            if '.fullday.' in dbn_file.name:
                date_parts = dbn_file.name.split('.fullday.')[0].split('-')[-3:]
            elif '.last' in dbn_file.name:
                date_parts = dbn_file.name.split('.last')[0].split('-')[-3:]
            else:
                date_parts = dbn_file.stem.split('.')[0].split('-')[-3:]
            file_date = '-'.join(date_parts)
        except:
            logger.warning(f"Could not parse date from {dbn_file.name}, skipping")
            continue
        
        # Create output directory
        output_dir = raw_dir / f"glbx-mdp3-{file_date}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Transform
        try:
            transform_continuous_to_folder_structure(
                dbn_file,
                output_dir,
                product=PRODUCT,
                roll_rule=ROLL_RULE
            )
            transformed_dirs.append(output_dir)
        except Exception as e:
            logger.error(f"Failed to transform {dbn_file.name}: {e}")
            continue
    
    if not transformed_dirs:
        logger.warning("No directories were transformed successfully")
        return
    
    logger.info(f"Transformed {len(transformed_dirs)} directories")
    
    # Ingest into database
    logger.info("Ingesting into database...")
    
    for source_dir in transformed_dirs:
        try:
            logger.info(f"Ingesting {source_dir.name}...")
            # Extract date for the loader
            date_str = source_dir.name.replace('glbx-mdp3-', '')
            load(PRODUCT, source_dir, date=date_str)
            logger.info(f"  {source_dir.name} ingested")
        except Exception as e:
            logger.error(f"Failed to ingest {source_dir.name}: {e}")
            continue
    
    # Build gold layer
    logger.info("Building gold layer (1-minute bars)...")
    try:
        # Delete existing bars for the dates we just re-ingested
        from pipelines.common import connect_duckdb
        con = connect_duckdb(db_path)
        try:
            # Extract dates from transformed directories
            # Directory names are like: glbx-mdp3-10-27.bbo-1m or glbx-mdp3-2025-10-27.bbo-1m
            dates_to_rebuild = []
            for source_dir in transformed_dirs:
                # Extract date from directory name
                dir_name = source_dir.name.replace('glbx-mdp3-', '')
                # Remove .bbo-1m suffix if present
                if '.bbo-1m' in dir_name:
                    dir_name = dir_name.replace('.bbo-1m', '')
                # If it's MM-DD format, we need to get the year from the downloaded files
                # Otherwise it should be YYYY-MM-DD
                if len(dir_name.split('-')) == 2:
                    # MM-DD format, need to extract year from downloaded files
                    # For now, skip this - we'll delete all bars for dates that have new quotes
                    # Better approach: delete bars where we have quotes from today
                    continue
                dates_to_rebuild.append(dir_name)
            
            if dates_to_rebuild:
                logger.info(f"Deleting existing bars for {len(dates_to_rebuild)} dates before rebuilding...")
                for date_str in dates_to_rebuild:
                    # Delete bars for this date
                    con.execute("""
                        DELETE FROM g_continuous_bar_1m 
                        WHERE CAST(ts_minute AS DATE) = ?
                    """, [date_str])
                logger.info(f"Deleted existing bars, now rebuilding...")
            else:
                # Fallback: delete bars for any dates that have quotes from our re-ingested data
                # Get all dates from quotes that were just ingested
                logger.info("Extracting dates from re-ingested quotes...")
                quote_dates = con.execute("""
                    SELECT DISTINCT CAST(ts_event AS DATE) as quote_date
                    FROM f_continuous_quote_l1
                    ORDER BY quote_date DESC
                    LIMIT 10
                """).fetchdf()
                
                if not quote_dates.empty:
                    logger.info(f"Found {len(quote_dates)} dates with quotes, deleting existing bars...")
                    for row in quote_dates.itertuples():
                        date_str = str(row.quote_date)
                        con.execute("""
                            DELETE FROM g_continuous_bar_1m 
                            WHERE CAST(ts_minute AS DATE) = ?
                        """, [date_str])
                    logger.info(f"Deleted existing bars for {len(quote_dates)} dates, now rebuilding...")
        finally:
            con.close()
        
        # Now rebuild with the gold SQL
        apply_gold_sql(PRODUCT)
        logger.info("Gold layer built")
    except Exception as e:
        logger.error(f"Failed to build gold layer: {e}")
    
    # Validate
    logger.info("Validating data...")
    from pipelines.validators import validate_futures
    from pipelines.common import connect_duckdb
    con = connect_duckdb(db_path)
    try:
        results = validate_futures(con)
        for name, cnt in results:
            logger.info(f"  [{name}] -> {cnt}")
    finally:
        con.close()
    
    logger.info("Ingestion complete!")


def show_summary():
    """Show database summary for continuous futures."""
    from pipelines.common import connect_duckdb
    
    _, _, db_path = get_paths()
    
    if not db_path.exists():
        print("Database not found. No data to summarize.")
        return
    
    con = connect_duckdb(db_path)
    
    try:
        # Check if continuous tables exist
        tables_check = con.execute("""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables 
            WHERE table_name = 'f_continuous_quote_l1'
        """).fetchdf()
        
        if tables_check['cnt'].iloc[0] == 0:
            print("Continuous futures tables not found. Run migration first:")
            print("  python orchestrator.py migrate")
            return
        
        # Get summary stats
        summary = con.execute("""
            SELECT 
                COUNT(*) as total_quotes,
                COUNT(DISTINCT contract_series) as unique_series,
                MIN(CAST(ts_event AS DATE)) as first_date,
                MAX(CAST(ts_event AS DATE)) as last_date,
                COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days
            FROM f_continuous_quote_l1
        """).fetchdf()
        
        if summary.empty or summary['total_quotes'].iloc[0] == 0:
            print("No continuous futures data in database yet.")
            return
        
        contract_count = con.execute("SELECT COUNT(*) as cnt FROM dim_continuous_contract").fetchdf()['cnt'].iloc[0]
        bar_count = con.execute("SELECT COUNT(*) as cnt FROM g_continuous_bar_1m").fetchdf()['cnt'].iloc[0]
        
        print("\n" + "=" * 80)
        print("DATABASE SUMMARY - ES CONTINUOUS FUTURES")
        print("=" * 80)
        print(f"Product: {PRODUCT}")
        print(f"Total quotes: {summary['total_quotes'].iloc[0]:,}")
        print(f"Unique contract series: {summary['unique_series'].iloc[0]}")
        print(f"Date range: {summary['first_date'].iloc[0]} to {summary['last_date'].iloc[0]}")
        print(f"Trading days: {summary['trading_days'].iloc[0]}")
        print(f"Contract definitions: {contract_count}")
        print(f"1-minute bars: {bar_count:,}")
        print("=" * 80)
    finally:
        con.close()


def ingest_only():
    """Ingest existing raw continuous futures files without downloading."""
    from pipelines.common import get_paths
    from pipelines.loader import load, apply_gold_sql
    from pipelines.registry import get_product
    
    logger.info("Ingesting existing continuous futures data...")
    
    raw_dir, _, db_path = get_paths()
    product_cfg = get_product(PRODUCT)
    
    # Find all directories with continuous data
    continuous_dirs = []
    for subdir in raw_dir.glob("glbx-mdp3-*"):
        if subdir.is_dir():
            cont_inst_dir = subdir / "continuous_instruments"
            if cont_inst_dir.exists() and any(cont_inst_dir.glob("*.parquet")):
                continuous_dirs.append(subdir)
    
    if not continuous_dirs:
        logger.info("No continuous futures data found in raw directory")
        return
    
    logger.info(f"Found {len(continuous_dirs)} directories with continuous data")
    
    # Ingest each directory
    for source_dir in sorted(continuous_dirs):
        try:
            date_str = source_dir.name.replace('glbx-mdp3-', '')
            logger.info(f"Ingesting {source_dir.name}...")
            load(PRODUCT, source_dir, date=date_str)
            logger.info(f"  {source_dir.name} ingested")
        except Exception as e:
            logger.error(f"Failed to ingest {source_dir.name}: {e}")
    
    # Build gold layer
    logger.info("Building gold layer...")
    try:
        apply_gold_sql(PRODUCT)
        logger.info("Gold layer built")
    except Exception as e:
        logger.error(f"Failed to build gold layer: {e}")
    
    logger.info("Ingestion complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Download and ingest ES continuous futures (front month, 2-day pre-expiry roll)"
    )
    
    # Date range options
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument('--weeks', type=int, help='Download last N weeks')
    date_group.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD, use with --start)')
    parser.add_argument('--minutes', type=int, default=5, help='Window size in minutes (default: 5, only used if --full-day is False)')
    parser.add_argument('--full-day', action='store_true', default=True, help='Download full trading day (default: True)')
    parser.add_argument('--last-minutes', dest='full_day', action='store_false', help='Download only last N minutes instead of full day')
    
    # Action options
    parser.add_argument('--summary', action='store_true', help='Show database summary and exit')
    parser.add_argument('--ingest-only', action='store_true', help='Only ingest existing raw data')
    parser.add_argument('--force', action='store_true', help='Force re-download even if data exists')
    parser.add_argument('--yes', action='store_true', help='Auto-confirm download cost')
    
    args = parser.parse_args()
    
    # Handle summary request
    if args.summary:
        show_summary()
        return 0
    
    # Handle ingest-only request
    if args.ingest_only:
        ingest_only()
        return 0
    
    # Determine date range
    if args.weeks:
        end_d = date.today() - timedelta(days=1)
        start_d = end_d - timedelta(weeks=args.weeks)
    elif args.start and args.end:
        start_d = date.fromisoformat(args.start)
        end_d = date.fromisoformat(args.end)
    elif args.start:
        start_d = date.fromisoformat(args.start)
        end_d = date.today() - timedelta(days=1)
    else:
        parser.error("Must specify either --weeks, --start, --summary, or --ingest-only")
    
    logger.info(f"ES Continuous Futures Download & Ingest")
    logger.info(f"Roll rule: {ROLL_RULE}")
    logger.info(f"Symbol: {get_continuous_symbol(ROOT, 0)}")
    
    # Load API key and create client
    api_key = load_api_key()
    client = db.Historical(key=api_key)
    
    # Download
    downloaded_files = download_continuous(
        client,
        start_d,
        end_d,
        minutes=args.minutes,
        force=args.force,
        yes=args.yes,
        full_day=args.full_day
    )
    
    # Transform and ingest
    if downloaded_files is not None and len(downloaded_files) > 0:
        # downloaded_files is already a list of Paths from batch downloader
        transform_and_ingest(downloaded_files)
        
        # Show summary
        print("\n")
        show_summary()
    else:
        logger.info("No new data to process")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

