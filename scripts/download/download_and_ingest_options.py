"""
All-in-one wrapper for ES Options data pipeline.

This script:
1. Checks what dates are already in the database
2. Downloads new BBO-1m data (with cost estimate & confirmation)
3. Transforms BBO data into proper folder structure
4. Ingests data into DuckDB
5. Builds gold-layer minute bars
6. Validates the ingestion
7. Provides summary of what's in the database

Usage:
    # Download last 3 weeks
    python scripts/download_and_ingest_options.py --weeks 3
    
    # Download specific date range
    python scripts/download_and_ingest_options.py --start 2025-09-01 --end 2025-09-30
    
    # Just ingest existing raw data (no download)
    python scripts/download_and_ingest_options.py --ingest-only
    
    # Check what's missing from raw folder
    python scripts/download_and_ingest_options.py --check-missing
"""
import sys
from pathlib import Path
from datetime import date, timedelta
from typing import List
import argparse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import os
from dotenv import load_dotenv
import databento as db

from src.utils.logging_config import get_logger
from src.utils.db_utils import (
    get_existing_dates_in_db,
    filter_new_dates,
    check_for_missing_ingestions,
    get_db_summary
)
from src.utils.data_transform import transform_all_dbn_files, transform_bbo_to_folder_structure
from src.download.bbo_downloader import (
    estimate_cost,
    pretty_cost,
    download_bbo_last_window
)
from pipelines.common import get_paths
from orchestrator import migrate as run_migrations

logger = get_logger(__name__)

PRODUCT = "ES_OPTIONS_MDP3"


def day_iter(d0: date, d1: date) -> List[date]:
    """Generate list of dates between d0 and d1, skipping weekends."""
    dates = []
    d = d0
    while d <= d1:
        if d.weekday() < 5:  # Skip weekends
            dates.append(d)
        d += timedelta(days=1)
    return dates


def get_date_range_from_weeks(weeks: int) -> tuple[date, date]:
    """Calculate date range for the last N weeks of trading days."""
    today = date.today()
    # Go back N weeks
    start = today - timedelta(weeks=weeks)
    # Round to Monday
    start = start - timedelta(days=start.weekday())
    # End is last Friday
    end = today - timedelta(days=today.weekday() + 3)  # Last Friday
    return start, end


def parse_date(dstr: str) -> date:
    """Parse YYYY-MM-DD string to date."""
    from datetime import datetime
    return datetime.strptime(dstr, "%Y-%m-%d").date()


def download_new_data(start_d: date, end_d: date, api_key: str, force: bool = False) -> List[Path]:
    """
    Download BBO-1m data for date range, with duplicate detection.
    
    Returns:
        List of DBN files that were downloaded
    """
    logger.info("=" * 80)
    logger.info("STEP 1: CHECK FOR EXISTING DATA")
    logger.info("=" * 80)
    
    # Get all requested dates (weekdays only)
    requested_dates = day_iter(start_d, end_d)
    logger.info(f"Requested date range: {start_d} to {end_d}")
    logger.info(f"Trading days in range: {len(requested_dates)}")
    
    if not force:
        # Check database for existing dates
        existing_dates = get_existing_dates_in_db(PRODUCT)
        logger.info(f"Dates already in database: {len(existing_dates)}")
        
        if existing_dates:
            logger.info(f"  Range: {min(existing_dates)} to {max(existing_dates)}")
        
        # Filter out existing dates
        new_dates = [d for d in requested_dates if d not in existing_dates]
        
        if not new_dates:
            logger.info("✓ All requested dates already in database. Nothing to download.")
            return []
        
        logger.info(f"Dates to download: {len(new_dates)}")
        logger.info(f"  New range: {min(new_dates)} to {max(new_dates)}")
        
        # Update date range to only new dates
        start_d = min(new_dates)
        end_d = max(new_dates)
    else:
        logger.info("Force mode: downloading all dates regardless of database content")
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 2: ESTIMATE COST")
    logger.info("=" * 80)
    
    client = db.Historical(key=api_key)
    
    # ES options use parent symbol universe
    symbols_for_api = ["ES.OPT"]
    stype_in = "parent"
    window_min = 5
    
    logger.info(f"Estimating cost for {len(day_iter(start_d, end_d))} trading days...")
    est_df, tot_bytes, tot_usd = estimate_cost(
        client, symbols_for_api, start_d, end_d, minutes=window_min, stype_in=stype_in
    )
    
    pretty_cost(est_df, tot_bytes, tot_usd)
    
    if tot_usd == 0:
        logger.warning("Cost estimate is $0 - no data available for this range?")
        return []
    
    # Prompt user
    ans = input(f"\n💰 Download {len(day_iter(start_d, end_d))} days for ${tot_usd:.2f}? [y/N] ").strip().lower()
    if ans != "y":
        logger.info("Download cancelled by user")
        return []
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 3: DOWNLOAD DATA")
    logger.info("=" * 80)
    
    manifest = download_bbo_last_window(
        client, symbols_for_api, start_d, end_d, minutes=window_min, stype_in=stype_in
    )
    
    if manifest.empty:
        logger.error("No files were downloaded")
        return []
    
    logger.info(f"✓ Downloaded {len(manifest)} files:")
    dbn_files = []
    for _, row in manifest.iterrows():
        file_path = Path(row['file'])
        logger.info(f"  {file_path.name}")
        dbn_files.append(file_path)
    
    return dbn_files


def transform_and_ingest(dbn_files: List[Path] = None):
    """
    Transform DBN files to folder structure and ingest into database.
    
    Args:
        dbn_files: Specific files to process, or None to process all
    """
    bronze, _, _ = get_paths()
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 4: TRANSFORM DATA")
    logger.info("=" * 80)
    
    if dbn_files is None:
        # Transform all DBN files
        logger.info("Transforming all DBN files in data/raw...")
        transformed_dirs = transform_all_dbn_files(product=PRODUCT)
    else:
        # Transform specific files
        logger.info(f"Transforming {len(dbn_files)} downloaded files...")
        transformed_dirs = []
        for dbn_file in dbn_files:
            # Extract date for output directory
            parts = dbn_file.stem.replace('.dbn', '').replace('.zst', '').split('-')
            if len(parts) >= 4:
                date_str = f"{parts[2]}-{parts[3]}-{parts[4].split('.')[0]}"
            else:
                continue
            
            output_dir = bronze / f"glbx-mdp3-{date_str}"
            
            if output_dir.exists():
                logger.info(f"  ✓ {output_dir.name} already exists")
                transformed_dirs.append(output_dir)
            else:
                try:
                    transform_bbo_to_folder_structure(dbn_file, output_dir, product=PRODUCT)
                    transformed_dirs.append(output_dir)
                except Exception as e:
                    logger.error(f"  ✗ Failed to transform {dbn_file.name}: {e}")
    
    if not transformed_dirs:
        logger.warning("No directories to ingest")
        return
    
    logger.info(f"✓ Transformed {len(transformed_dirs)} directories")
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 5: INGEST INTO DATABASE")
    logger.info("=" * 80)
    
    # Ensure migrations are applied
    logger.info("Ensuring database schema is up to date...")
    run_migrations()
    
    # Ingest each directory
    from pipelines.loader import load as load_product
    
    for source_dir in transformed_dirs:
        logger.info(f"Ingesting {source_dir.name}...")
        try:
            # Extract date from directory name
            parts = source_dir.name.split('-')
            if len(parts) >= 4:
                date_str = f"{parts[2]}-{parts[3]}-{parts[4]}"
            else:
                date_str = None
            
            load_product(PRODUCT, source_dir, date_str)
            logger.info(f"  ✓ {source_dir.name} ingested")
        except Exception as e:
            logger.error(f"  ✗ Failed to ingest {source_dir.name}: {e}")
            import traceback
            traceback.print_exc()
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 6: BUILD GOLD LAYER")
    logger.info("=" * 80)
    
    from pipelines.loader import apply_gold_sql
    
    logger.info("Building 1-minute bars...")
    try:
        apply_gold_sql(PRODUCT)
        logger.info("✓ Gold layer built")
    except Exception as e:
        logger.error(f"✗ Failed to build gold layer: {e}")
        import traceback
        traceback.print_exc()
    
    logger.info("\n" + "=" * 80)
    logger.info("STEP 7: VALIDATE")
    logger.info("=" * 80)
    
    from pipelines.validators import validate_options
    from pipelines.common import connect_duckdb, get_paths
    
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    
    try:
        results = validate_options(con)
        for name, cnt in results:
            logger.info(f"  {name}: {cnt}")
        logger.info("✓ Validation complete")
    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
    finally:
        con.close()


def print_summary():
    """Print summary of what's in the database."""
    logger.info("\n" + "=" * 80)
    logger.info("DATABASE SUMMARY")
    logger.info("=" * 80)
    
    summary = get_db_summary(PRODUCT)
    
    if summary['status'] == 'no_database':
        logger.info("No database found. Run with download or ingest to create.")
        return
    
    if summary['status'] == 'error':
        logger.error(f"Error reading database: {summary['message']}")
        return
    
    logger.info(f"Product: {PRODUCT}")
    logger.info(f"Total quotes: {summary['total_quotes']:,}")
    logger.info(f"Unique instruments: {summary['unique_instruments']:,}")
    logger.info(f"Date range: {summary['min_date']} to {summary['max_date']}")
    logger.info(f"Trading days: {summary['trading_days']}")
    logger.info(f"Instrument definitions: {summary['instrument_count']:,}")
    logger.info(f"1-minute bars: {summary['bar_count']:,}")


def check_missing():
    """Check for data in raw folder that hasn't been ingested."""
    logger.info("\n" + "=" * 80)
    logger.info("CHECKING FOR UN-INGESTED DATA")
    logger.info("=" * 80)
    
    missing = check_for_missing_ingestions()
    
    if not missing:
        logger.info("✓ All data in raw folder has been ingested to database")
        return
    
    logger.warning(f"Found {len(missing)} dates in raw folder not yet in database:")
    for d in missing:
        logger.warning(f"  {d}")
    
    ans = input(f"\nIngest these {len(missing)} dates now? [y/N] ").strip().lower()
    if ans == "y":
        transform_and_ingest()


def main():
    parser = argparse.ArgumentParser(
        description="Download and ingest ES Options BBO-1m data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Date range options
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--weeks", type=int, help="Download last N weeks (e.g., --weeks 3)")
    group.add_argument("--start", type=parse_date, help="Start date YYYY-MM-DD")
    
    parser.add_argument("--end", type=parse_date, help="End date YYYY-MM-DD (use with --start)")
    
    # Action options
    parser.add_argument("--ingest-only", action="store_true", 
                       help="Only ingest existing data, don't download")
    parser.add_argument("--check-missing", action="store_true",
                       help="Check for un-ingested data in raw folder")
    parser.add_argument("--force", action="store_true",
                       help="Download even if data exists in database")
    parser.add_argument("--summary", action="store_true",
                       help="Show database summary and exit")
    
    args = parser.parse_args()
    
    # Load API key
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=env_path)
    api_key = os.getenv("DATABENTO_API_KEY")
    
    # Handle special actions
    if args.summary:
        print_summary()
        return 0
    
    if args.check_missing:
        check_missing()
        return 0
    
    if args.ingest_only:
        logger.info("Ingest-only mode: processing existing raw data")
        transform_and_ingest()
        print_summary()
        return 0
    
    # Determine date range
    if args.weeks:
        start_d, end_d = get_date_range_from_weeks(args.weeks)
        logger.info(f"Downloading last {args.weeks} weeks: {start_d} to {end_d}")
    elif args.start:
        start_d = args.start
        end_d = args.end if args.end else date.today()
        logger.info(f"Downloading date range: {start_d} to {end_d}")
    else:
        # Default: last 1 week
        start_d, end_d = get_date_range_from_weeks(1)
        logger.info(f"No date range specified, using last week: {start_d} to {end_d}")
    
    if not api_key:
        logger.error("DATABENTO_API_KEY not found in .env file")
        logger.error(f"Please set it in: {env_path}")
        return 1
    
    # Download new data
    dbn_files = download_new_data(start_d, end_d, api_key, force=args.force)
    
    if not dbn_files and not args.force:
        logger.info("No new data to process")
        print_summary()
        return 0
    
    # Transform and ingest
    transform_and_ingest(dbn_files)
    
    # Final summary
    print_summary()
    
    logger.info("\n" + "=" * 80)
    logger.info("✓ PIPELINE COMPLETE")
    logger.info("=" * 80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

