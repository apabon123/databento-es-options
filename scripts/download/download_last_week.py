"""
Simple script to download last week's 5-minute ES options data.

Usage:
    python scripts/download_last_week.py
"""
from pathlib import Path
from datetime import date, timedelta
import sys
import os

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.env import load_env

load_env()

from src.utils.logging_config import get_logger
import databento as db

# Import downloader functions
from src.download.bbo_downloader import (
    estimate_cost,
    pretty_cost,
    download_bbo_last_window,
)

logger = get_logger(__name__)


def last_week_range(today: date) -> tuple[date, date]:
    """Return last week's Monday..Friday inclusive relative to today."""
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_friday = last_monday + timedelta(days=4)
    return last_monday, last_friday


def main() -> int:
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        logger.error("Set DATABENTO_API_KEY in your .env at project root.")
        return 2

    # Define parameters
    symbols_pattern = "ES.c.0"  # ES options universe
    window_min = 5               # last 5 minutes of RTH
    symbols_for_api = ["ES.OPT"]
    stype_in = "parent"

    start_d, end_d = last_week_range(date.today())
    logger.info(f"Downloading last week {start_d}..{end_d} (Mon-Fri), {window_min}m window, symbols='{symbols_pattern}'")
    logger.info(f"Note: bbo-1m filters on ts_recv (when snapshot received), not ts_event (last trade time)")

    client = db.Historical(key=api_key)

    # Estimate costs
    logger.info("Getting cost estimate...")
    est_df, tot_bytes, tot_usd = estimate_cost(client, symbols_for_api, start_d, end_d, minutes=window_min, stype_in=stype_in)
    pretty_cost(est_df, tot_bytes, tot_usd)

    # Prompt to download
    ans = input("\nDownload now? [y/N] ").strip().lower()
    if ans != "y":
        logger.info("Not downloading. Exiting after estimate.")
        return 0

    # Download
    logger.info("Downloading data...")
    manifest = download_bbo_last_window(client, symbols_for_api, start_d, end_d, minutes=window_min, stype_in=stype_in)
    if manifest.empty:
        logger.error("No files were written (no trading days?).")
        return 1
    
    logger.info(f"\nDownloaded {len(manifest)} files:")
    for _, row in manifest.iterrows():
        logger.info(f"  {row['file']}")

    logger.info("\nDone! Run 'python scripts/analysis/analyze_data.py' to validate the data.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

