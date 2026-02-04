"""
Download spot index prices from Yahoo Finance (primary) or MarketWatch (fallback).

This script handles non-FRED index data (e.g., RUT - Russell 2000) that
isn't available through the FRED API.

Key features:
- Yahoo Finance primary, MarketWatch fallback for RUT
- Append-only updates (hard-fail on history changes)
- Uses Close price (NOT Adj Close) for price-return indices
- Validates data integrity (no duplicates, outliers, gaps)
- Hard-fails if data is stale or insufficient (non-silent failure contract)
- Stores in Bronze (data/external/index_spot/) and ingests to Silver (DuckDB)

If both providers fail, the script hard-fails and does not ingest.

Usage:
    python scripts/download/download_index_spot.py
    python scripts/download/download_index_spot.py --series RUT_SPOT --ingest
    python scripts/download/download_index_spot.py --backfill --start 2000-01-01
    python scripts/download/download_index_spot.py --force  # Re-download all
    python scripts/download/download_index_spot.py --probe  # Sanity check providers
"""

import argparse
import io
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

LOGGER = logging.getLogger("download_index_spot")

# Configuration
DEFAULT_EXTERNAL_ROOT = Path(os.getenv("DATA_EXTERNAL_ROOT", "data/external")).resolve()
INDEX_SPOT_DIR = DEFAULT_EXTERNAL_ROOT / "index_spot"
MANIFEST_PATH = INDEX_SPOT_DIR / "manifest.json"

# Series configuration: series_id -> {yahoo_symbol, marketwatch_symbol, name}
INDEX_SERIES = {
    "RUT_SPOT": {
        "name": "Russell 2000 Index (Price Return)",
        "yahoo_symbol": "^RUT",
        "marketwatch_symbol": "rut",  # MarketWatch uses lowercase
        "description": "Russell 2000 small-cap index, price-return level",
    },
    # Future expansion: add more indices here if needed
    # "DJI_SPOT": {
    #     "name": "Dow Jones Industrial Average",
    #     "yahoo_symbol": "^DJI",
    #     "marketwatch_symbol": "djia",
    # },
}

# Validation thresholds
MAX_DAILY_CHANGE_PCT = 20.0  # Flag if daily move exceeds 20%
MIN_INDEX_VALUE = 100.0  # Russell 2000 should never be below 100

# Non-silent failure contract thresholds
MAX_STALE_DAYS = 10  # Hard-fail if max(date) < today - this many calendar days
MIN_BACKFILL_ROWS = 1000  # Hard-fail if backfill returns fewer rows than this


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def load_manifest() -> Dict:
    """Load the download manifest."""
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_manifest(manifest: Dict) -> None:
    """Save the download manifest."""
    INDEX_SPOT_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def fetch_yahoo(symbol: str, start: date, end: date) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Fetch data from Yahoo Finance.
    
    Returns (DataFrame, error_message). DataFrame is None if failed.
    Uses Close price (NOT Adj Close) for price-return indices.
    """
    try:
        import yfinance as yf
    except ImportError:
        return None, "yfinance not installed. Run: pip install yfinance"
    
    try:
        LOGGER.info("Fetching %s from Yahoo Finance (%s to %s)", symbol, start, end)
        ticker = yf.Ticker(symbol)
        
        # Fetch with auto_adjust=False to get raw Close (not adjusted)
        df = ticker.history(
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
            auto_adjust=False,
        )
        
        if df.empty:
            return None, f"No data returned from Yahoo for {symbol}"
        
        # Use Close (NOT Adj Close) for price-return index
        if "Close" not in df.columns:
            return None, f"No 'Close' column in Yahoo data for {symbol}"
        
        # Prepare output DataFrame
        result = pd.DataFrame({
            "date": df.index.date,
            "value": df["Close"].values,
        })
        result = result.dropna(subset=["value"])
        
        if result.empty:
            return None, f"All Close values are NaN for {symbol}"
        
        LOGGER.info("Yahoo returned %d rows for %s", len(result), symbol)
        return result, ""
        
    except Exception as e:
        return None, f"Yahoo fetch failed: {str(e)}"


def _is_html_response(text: str) -> bool:
    """Check if response text looks like HTML (not CSV)."""
    text_lower = text.strip().lower()
    return (
        text_lower.startswith("<!doctype") or
        text_lower.startswith("<html") or
        text_lower.startswith("<!") or
        "<head>" in text_lower or
        "<body>" in text_lower
    )


def _parse_marketwatch_csv(csv_text: str, symbol: str) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Parse MarketWatch CSV response into DataFrame.
    
    Handles various edge cases: commas, locale formatting, N/A values, etc.
    Returns (DataFrame, error_message).
    """
    try:
        df = pd.read_csv(io.StringIO(csv_text))
    except Exception as e:
        return None, f"Failed to parse MarketWatch CSV: {e}"
    
    if df.empty:
        return None, f"No data returned from MarketWatch for {symbol}"
    
    # MarketWatch CSV typically has columns: Date, Open, High, Low, Close, Volume
    # Find the date and close columns (case-insensitive)
    df.columns = [c.strip() for c in df.columns]
    col_map = {c.lower(): c for c in df.columns}
    
    date_col = col_map.get("date")
    close_col = col_map.get("close")
    
    if not date_col:
        return None, f"No 'Date' column in MarketWatch data. Columns: {list(df.columns)}"
    if not close_col:
        return None, f"No 'Close' column in MarketWatch data. Columns: {list(df.columns)}"
    
    # Parse dates - MarketWatch uses various formats
    # Try MM/DD/YYYY first, then generic parsing
    try:
        dates = pd.to_datetime(df[date_col], format="%m/%d/%Y")
    except Exception:
        try:
            dates = pd.to_datetime(df[date_col])
        except Exception as e:
            return None, f"Failed to parse MarketWatch dates: {e}"
    
    # Clean close values defensively:
    # - Remove $ signs, commas, and whitespace
    # - Handle N/A, null, empty strings
    close_raw = df[close_col].astype(str)
    close_raw = close_raw.str.strip()
    close_raw = close_raw.str.replace(r"[$,]", "", regex=True)
    close_raw = close_raw.replace(["N/A", "n/a", "NA", "null", "NULL", "", "-"], pd.NA)
    close_values = pd.to_numeric(close_raw, errors="coerce")
    
    result = pd.DataFrame({
        "date": dates.dt.date,
        "value": close_values.values,
    })
    result = result.dropna(subset=["value"])
    
    # Sort by date (MarketWatch may return reverse chronological)
    result = result.sort_values("date").reset_index(drop=True)
    
    if result.empty:
        return None, f"All Close values are invalid for {symbol}"
    
    return result, ""


def _try_marketwatch_endpoint(
    url: str,
    params: Dict,
    headers: Dict,
    symbol: str,
    endpoint_name: str,
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Try a single MarketWatch endpoint.
    
    Returns (DataFrame, error_message). DataFrame is None if failed.
    """
    try:
        LOGGER.debug("Trying MarketWatch %s endpoint for %s", endpoint_name, symbol)
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Check content type
        content_type = response.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            return None, f"{endpoint_name}: Returned HTML (Content-Type: {content_type})"
        
        # Check response text for HTML markers
        csv_text = response.text.strip()
        if not csv_text:
            return None, f"{endpoint_name}: Empty response"
        
        if _is_html_response(csv_text):
            return None, f"{endpoint_name}: Response body is HTML (bot-check or error page)"
        
        # Parse the CSV
        return _parse_marketwatch_csv(csv_text, symbol)
        
    except requests.exceptions.HTTPError as e:
        return None, f"{endpoint_name}: HTTP {e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return None, f"{endpoint_name}: Request failed: {str(e)}"
    except Exception as e:
        return None, f"{endpoint_name}: {str(e)}"


def fetch_marketwatch(symbol: str, start: date, end: date) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Fetch data from MarketWatch as fallback.
    
    Returns (DataFrame, error_message). DataFrame is None if failed.
    
    Tries two endpoints for resilience:
    1. Primary: /downloaddatapartial (internal API)
    2. Fallback: /download-data (public page)
    
    If MarketWatch returns HTML, bot-check, or malformed data -> fail the provider.
    No partial ingestion.
    """
    LOGGER.warning("Falling back to MarketWatch for %s", symbol)
    
    # Format dates as MM/DD/YYYY for MarketWatch
    start_str = start.strftime("%m/%d/%Y")
    end_str = end.strftime("%m/%d/%Y")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/csv,application/csv,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://www.marketwatch.com/investing/index/{symbol}",
    }
    
    errors = []
    
    # Endpoint 1: /downloaddatapartial (internal API - may have more data)
    url1 = f"https://www.marketwatch.com/investing/index/{symbol}/downloaddatapartial"
    params1 = {
        "startdate": start_str,
        "enddate": end_str,
        "daterange": "d30",
        "frequency": "p1d",
        "csvdownload": "true",
        "downloadpartial": "false",
        "newdates": "false",
    }
    
    df, error = _try_marketwatch_endpoint(url1, params1, headers, symbol, "downloaddatapartial")
    if df is not None and not df.empty:
        LOGGER.info("MarketWatch (downloaddatapartial) returned %d rows for %s", len(df), symbol)
        return df, ""
    errors.append(error)
    LOGGER.debug("MarketWatch downloaddatapartial failed: %s", error)
    
    # Endpoint 2: /download-data (public stable endpoint)
    url2 = f"https://www.marketwatch.com/investing/index/{symbol}/download-data"
    params2 = {
        "startDate": start_str,
        "endDate": end_str,
        "countryCode": "us",
    }
    
    df, error = _try_marketwatch_endpoint(url2, params2, headers, symbol, "download-data")
    if df is not None and not df.empty:
        LOGGER.info("MarketWatch (download-data) returned %d rows for %s", len(df), symbol)
        return df, ""
    errors.append(error)
    LOGGER.debug("MarketWatch download-data failed: %s", error)
    
    # Both endpoints failed
    return None, f"MarketWatch failed: {'; '.join(errors)}"


def fetch_index_data(
    series_id: str,
    config: Dict,
    start: date,
    end: date,
) -> Tuple[pd.DataFrame, str]:
    """
    Fetch index data with Yahoo primary, MarketWatch fallback.
    
    Returns (DataFrame, source_used).
    Raises RuntimeError if both sources fail (non-silent failure).
    """
    yahoo_symbol = config.get("yahoo_symbol")
    marketwatch_symbol = config.get("marketwatch_symbol")
    
    errors = []
    
    # Try Yahoo first
    if yahoo_symbol:
        df, error = fetch_yahoo(yahoo_symbol, start, end)
        if df is not None and not df.empty:
            return df, "yahoo"
        errors.append(f"Yahoo: {error}")
        LOGGER.warning("Yahoo failed for %s: %s", series_id, error)
    
    # Fallback to MarketWatch
    if marketwatch_symbol:
        df, error = fetch_marketwatch(marketwatch_symbol, start, end)
        if df is not None and not df.empty:
            return df, "marketwatch"
        errors.append(f"MarketWatch: {error}")
        LOGGER.warning("MarketWatch failed for %s: %s", series_id, error)
    
    # Both failed - this is a hard failure, no ingest
    raise RuntimeError(
        f"ALL DATA SOURCES FAILED for {series_id}. No data will be ingested.\n"
        f"Errors:\n  " + "\n  ".join(errors)
    )


def validate_data(df: pd.DataFrame, series_id: str) -> List[str]:
    """
    Validate downloaded data for quality issues.
    
    Returns list of warning/error messages. Empty list means all checks passed.
    """
    issues = []
    
    if df.empty:
        issues.append(f"{series_id}: DataFrame is empty")
        return issues
    
    # Check for duplicate dates
    dup_dates = df[df.duplicated(subset=["date"], keep=False)]
    if not dup_dates.empty:
        issues.append(f"{series_id}: Found {len(dup_dates)} duplicate dates")
    
    # Check for negative or zero values
    invalid_values = df[df["value"] <= 0]
    if not invalid_values.empty:
        issues.append(
            f"{series_id}: Found {len(invalid_values)} non-positive values "
            f"(min: {df['value'].min():.2f})"
        )
    
    # Check minimum index level
    if df["value"].min() < MIN_INDEX_VALUE:
        issues.append(
            f"{series_id}: Index fell below {MIN_INDEX_VALUE} "
            f"(min: {df['value'].min():.2f})"
        )
    
    # Check for large daily moves (potential data errors)
    df_sorted = df.sort_values("date").copy()
    df_sorted["pct_change"] = df_sorted["value"].pct_change() * 100
    large_moves = df_sorted[df_sorted["pct_change"].abs() > MAX_DAILY_CHANGE_PCT]
    if not large_moves.empty:
        for _, row in large_moves.iterrows():
            issues.append(
                f"{series_id}: Large move on {row['date']}: {row['pct_change']:.1f}% "
                f"(value: {row['value']:.2f})"
            )
    
    # Check date ordering
    dates = pd.to_datetime(df["date"])
    if not dates.is_monotonic_increasing:
        issues.append(f"{series_id}: Dates are not monotonically increasing")
    
    return issues


def validate_freshness(
    df: pd.DataFrame,
    series_id: str,
    is_backfill: bool,
    requested_start: Optional[date] = None,
    requested_end: Optional[date] = None,
) -> List[str]:
    """
    Validate data freshness and completeness (non-silent failure contract).
    
    Returns list of FATAL issues that should cause hard-fail.
    
    Backfill row check is scaled based on requested date range:
    - If requesting >5 years of data, require MIN_BACKFILL_ROWS (1000)
    - Otherwise, require ~200 rows per year of requested data (trading days)
    """
    fatal_issues = []
    
    if df.empty:
        fatal_issues.append(f"{series_id}: No data returned - cannot proceed")
        return fatal_issues
    
    max_date = pd.to_datetime(df["date"]).max().date()
    min_date = pd.to_datetime(df["date"]).min().date()
    today = date.today()
    
    # Check staleness: max(date) must be within MAX_STALE_DAYS of today
    stale_cutoff = today - timedelta(days=MAX_STALE_DAYS)
    if max_date < stale_cutoff:
        fatal_issues.append(
            f"{series_id}: Data is STALE - latest date is {max_date}, "
            f"but must be >= {stale_cutoff} (today - {MAX_STALE_DAYS} days)"
        )
    
    # Check row count on backfill - scale based on requested date range
    if is_backfill:
        # Calculate expected rows based on date range
        if requested_start and requested_end:
            years_requested = (requested_end - requested_start).days / 365.25
        else:
            years_requested = (max_date - min_date).days / 365.25
        
        # ~252 trading days per year, be lenient with 200
        # For requests >5 years, use MIN_BACKFILL_ROWS
        # For shorter requests, scale proportionally
        if years_requested > 5:
            min_expected = MIN_BACKFILL_ROWS
        else:
            min_expected = max(50, int(years_requested * 200))  # At least 50 rows
        
        if len(df) < min_expected:
            fatal_issues.append(
                f"{series_id}: Backfill returned only {len(df)} rows, "
                f"expected at least {min_expected} for {years_requested:.1f} years of data. "
                f"This suggests incomplete data from provider."
            )
    
    return fatal_issues


def check_history_changes(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    series_id: str,
    pct_tolerance: float = 0.0002,  # 0.02% relative tolerance
    abs_tolerance: float = 1.0,     # 1.0 index point absolute tolerance
) -> List[str]:
    """
    Check if historical values have changed (append-only enforcement).
    
    Hard-fail if: abs_change > 0.02% OR abs_diff > 1.0 index point
    This catches real data revisions while ignoring trivial float formatting.
    
    Returns list of issues. Empty list means no changes to existing data.
    Also logs max overlap diff even on pass (for drift monitoring).
    """
    issues = []
    
    if existing_df.empty:
        return issues
    
    # Find overlapping dates
    existing_dates = set(existing_df["date"].astype(str))
    new_dates = set(new_df["date"].astype(str))
    overlap_dates = existing_dates & new_dates
    
    if not overlap_dates:
        return issues
    
    # Compare values for overlapping dates
    existing_indexed = existing_df.set_index(
        existing_df["date"].astype(str)
    )["value"]
    new_indexed = new_df.set_index(new_df["date"].astype(str))["value"]
    
    max_pct_diff = 0.0
    max_abs_diff = 0.0
    max_diff_date = None
    
    for date_str in sorted(overlap_dates):
        old_val = existing_indexed.get(date_str)
        new_val = new_indexed.get(date_str)
        
        if old_val is None or new_val is None:
            continue
        
        if pd.isna(old_val) or pd.isna(new_val):
            continue
        
        abs_diff = abs(old_val - new_val)
        pct_diff = abs_diff / max(abs(old_val), 0.01)
        
        # Track max diff for logging
        if pct_diff > max_pct_diff:
            max_pct_diff = pct_diff
            max_abs_diff = abs_diff
            max_diff_date = date_str
        
        # Check for significant difference using EITHER threshold
        # This catches both percentage-based and absolute changes
        if pct_diff > pct_tolerance or abs_diff > abs_tolerance:
            issues.append(
                f"{series_id}: Historical value changed on {date_str}: "
                f"{old_val:.4f} -> {new_val:.4f} "
                f"(diff: {abs_diff:.4f} pts, {pct_diff*100:.4f}%)"
            )
    
    # Log max overlap diff even on pass (for drift monitoring)
    if max_diff_date and not issues:
        LOGGER.info(
            "%s: Overlap check passed (%d dates). Max diff on %s: %.4f pts (%.4f%%)",
            series_id,
            len(overlap_dates),
            max_diff_date,
            max_abs_diff,
            max_pct_diff * 100,
        )
    elif overlap_dates:
        LOGGER.debug("%s: Checked %d overlapping dates", series_id, len(overlap_dates))
    
    return issues


def load_existing_data(series_id: str) -> pd.DataFrame:
    """Load existing parquet data for a series."""
    parquet_path = INDEX_SPOT_DIR / f"{series_id}.parquet"
    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            if not df.empty:
                return df
        except Exception as e:
            LOGGER.warning("Could not read existing file %s: %s", parquet_path, e)
    return pd.DataFrame(columns=["date", "value"])


def load_manual_csv(csv_path: str) -> pd.DataFrame:
    """
    Load index data from a manually provided CSV file.
    
    Expected CSV format:
    - Must have 'date' column (YYYY-MM-DD format)
    - Must have 'close' column (float values)
    - Other columns are ignored
    
    Returns DataFrame with columns: date, value
    - Deduplicated by date (keeps last occurrence)
    - Sorted by date ascending
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    LOGGER.info("Loading CSV from %s", csv_path)
    
    try:
        # Read CSV - try common encodings
        for encoding in ['utf-8', 'utf-8-sig', 'latin-1']:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"Could not decode CSV file: {csv_path}")
        
        # Validate required columns
        required_cols = ['date', 'close']
        missing = [c for c in required_cols if c.lower() not in [col.lower() for col in df.columns]]
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}. Found: {list(df.columns)}")
        
        # Find date and close columns (case-insensitive)
        date_col = next((c for c in df.columns if c.lower() == 'date'), None)
        close_col = next((c for c in df.columns if c.lower() == 'close'), None)
        
        if not date_col or not close_col:
            raise ValueError(f"Could not find date/close columns. Found: {list(df.columns)}")
        
        # Extract and clean
        result_df = pd.DataFrame()
        result_df['date'] = pd.to_datetime(df[date_col], errors='coerce')
        result_df['value'] = pd.to_numeric(df[close_col], errors='coerce')
        
        # Drop rows with invalid dates or values
        result_df = result_df.dropna(subset=['date', 'value'])
        
        if result_df.empty:
            raise ValueError("No valid data rows found in CSV after parsing")
        
        # Convert date to date type (not datetime)
        result_df['date'] = result_df['date'].dt.date
        
        # Deduplicate by date (keep last)
        result_df = result_df.drop_duplicates(subset=['date'], keep='last')
        
        # Sort by date ascending
        result_df = result_df.sort_values('date').reset_index(drop=True)
        
        LOGGER.info("Loaded %d rows from CSV (date range: %s to %s)",
                   len(result_df), result_df['date'].min(), result_df['date'].max())
        
        return result_df[['date', 'value']]
        
    except Exception as e:
        raise ValueError(f"Failed to load CSV {csv_path}: {e}") from e


def save_parquet(df: pd.DataFrame, series_id: str, source: str) -> Path:
    """Save data to parquet with metadata."""
    df = df.copy()
    df["series_id"] = series_id
    df["source"] = source
    df["last_updated"] = datetime.now(timezone.utc).isoformat()
    
    # Ensure consistent column order and types
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df[["date", "series_id", "value", "source", "last_updated"]]
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    
    INDEX_SPOT_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = INDEX_SPOT_DIR / f"{series_id}.parquet"
    df.to_parquet(parquet_path, index=False)
    
    LOGGER.info("Saved %d rows to %s", len(df), parquet_path)
    return parquet_path


def run_probe(series_id: str, config: Dict) -> bool:
    """
    Run probe mode: quick sanity check of both providers.
    
    Pulls last ~30 trading days from each provider and compares.
    Returns True if at least one provider works.
    """
    print(f"\n{'='*60}")
    print(f"PROBE: {series_id} ({config.get('name', '')})")
    print(f"{'='*60}")
    
    end = date.today()
    start = end - timedelta(days=45)  # ~30 trading days
    
    yahoo_symbol = config.get("yahoo_symbol")
    marketwatch_symbol = config.get("marketwatch_symbol")
    
    yahoo_df = None
    marketwatch_df = None
    yahoo_error = None
    marketwatch_error = None
    
    # Test Yahoo
    if yahoo_symbol:
        print(f"\n[Yahoo] Testing {yahoo_symbol}...")
        yahoo_df, yahoo_error = fetch_yahoo(yahoo_symbol, start, end)
        if yahoo_df is not None and not yahoo_df.empty:
            print(f"  Status: OK")
            print(f"  Rows: {len(yahoo_df)}")
            print(f"  Date range: {yahoo_df['date'].min()} to {yahoo_df['date'].max()}")
            print(f"  Last close: {yahoo_df.iloc[-1]['value']:.2f}")
        else:
            print(f"  Status: FAILED")
            print(f"  Error: {yahoo_error}")
    
    # Test MarketWatch
    if marketwatch_symbol:
        print(f"\n[MarketWatch] Testing {marketwatch_symbol}...")
        marketwatch_df, marketwatch_error = fetch_marketwatch(marketwatch_symbol, start, end)
        if marketwatch_df is not None and not marketwatch_df.empty:
            print(f"  Status: OK")
            print(f"  Rows: {len(marketwatch_df)}")
            print(f"  Date range: {marketwatch_df['date'].min()} to {marketwatch_df['date'].max()}")
            print(f"  Last close: {marketwatch_df.iloc[-1]['value']:.2f}")
        else:
            print(f"  Status: FAILED")
            print(f"  Error: {marketwatch_error}")
    
    # Compare overlapping dates if both succeeded
    if yahoo_df is not None and marketwatch_df is not None:
        print(f"\n[Comparison] Checking overlap...")
        yahoo_dates = set(yahoo_df["date"].astype(str))
        mw_dates = set(marketwatch_df["date"].astype(str))
        overlap = yahoo_dates & mw_dates
        
        if overlap:
            yahoo_indexed = yahoo_df.set_index(yahoo_df["date"].astype(str))["value"]
            mw_indexed = marketwatch_df.set_index(marketwatch_df["date"].astype(str))["value"]
            
            diffs = []
            for d in sorted(overlap)[-5:]:  # Last 5 overlapping dates
                y_val = yahoo_indexed.get(d)
                m_val = mw_indexed.get(d)
                if y_val and m_val:
                    pct_diff = (y_val - m_val) / m_val * 100
                    diffs.append(pct_diff)
                    print(f"    {d}: Yahoo={y_val:.2f}, MW={m_val:.2f}, diff={pct_diff:.3f}%")
            
            if diffs:
                avg_diff = sum(abs(d) for d in diffs) / len(diffs)
                print(f"  Average absolute diff: {avg_diff:.4f}%")
                if avg_diff > 0.5:
                    print(f"  WARNING: Differences > 0.5% detected - investigate!")
        else:
            print(f"  No overlapping dates to compare")
    
    # Summary
    print(f"\n{'='*60}")
    yahoo_ok = yahoo_df is not None and not yahoo_df.empty
    mw_ok = marketwatch_df is not None and not marketwatch_df.empty
    
    if yahoo_ok and mw_ok:
        print("RESULT: BOTH providers OK")
        return True
    elif yahoo_ok:
        print("RESULT: Yahoo OK, MarketWatch FAILED")
        return True
    elif mw_ok:
        print("RESULT: Yahoo FAILED, MarketWatch OK")
        return True
    else:
        print("RESULT: BOTH providers FAILED - cannot proceed with download")
        return False


def process_import_csv(
    csv_path: str,
    series_id: str,
    ingest: bool = False,
) -> bool:
    """
    Process a manual CSV import for a series.
    
    This bypasses all providers and loads directly from CSV.
    Implements smart ingestion: only inserts dates > MAX(date_in_db).
    """
    LOGGER.info("Processing CSV import for %s from %s", series_id, csv_path)
    
    # Load CSV
    try:
        csv_df = load_manual_csv(csv_path)
    except Exception as e:
        LOGGER.error("Failed to load CSV: %s", e)
        return False
    
    if csv_df.empty:
        LOGGER.error("CSV file is empty or contains no valid data")
        return False
    
    # Validate data quality (same as normal processing)
    issues = validate_data(csv_df, series_id)
    if issues:
        for issue in issues:
            LOGGER.warning("Validation: %s", issue)
    
    # Check existing data in database to determine what to insert
    from pipelines.common import get_paths, connect_duckdb
    from orchestrator import migrate
    
    _, _, db_path = get_paths()
    con = connect_duckdb(db_path)
    
    try:
        # Run migrations
        migrate()
        
        # Get all existing dates in database for this series
        existing_dates_df = con.execute("""
            SELECT date, value
            FROM f_fred_observations
            WHERE series_id = ?
            ORDER BY date
        """, [series_id]).fetchdf()
        
        if not existing_dates_df.empty:
            existing_dates_df['date'] = pd.to_datetime(existing_dates_df['date']).dt.date
            existing_dates = set(existing_dates_df['date'])
            max_date_in_db = existing_dates_df['date'].max()
            
            LOGGER.info(
                "Existing data in DB: %d rows (%s to %s). "
                "Will import only missing dates.",
                len(existing_dates),
                existing_dates_df['date'].min(),
                max_date_in_db
            )
            
            # Filter CSV to only include dates that don't exist in DB
            csv_df = csv_df[~csv_df['date'].isin(existing_dates)].copy()
            
            if csv_df.empty:
                LOGGER.warning("No new dates to import (all dates already exist in DB)")
                return True  # Not an error, just nothing to do
            
            # Check for any dates in CSV that overlap with existing (defensive check)
            # This shouldn't happen after filtering, but verify values match if it does
            csv_dates = set(csv_df['date'])
            overlap_dates = csv_dates & existing_dates
            
            if overlap_dates:
                LOGGER.warning(
                    "Found %d overlapping dates after filtering (shouldn't happen). "
                    "Checking tolerance...",
                    len(overlap_dates)
                )
                existing_indexed = existing_dates_df.set_index('date')['value']
                csv_indexed = csv_df.set_index('date')['value']
                
                large_diffs = []
                for d in overlap_dates:
                    csv_val = csv_indexed.get(d)
                    db_val = existing_indexed.get(d)
                    if csv_val and db_val:
                        abs_diff = abs(csv_val - db_val)
                        pct_diff = (abs_diff / abs(db_val)) * 100
                        if pct_diff > 0.02 or abs_diff > 1.0:
                            large_diffs.append(d)
                            LOGGER.warning(
                                "Date %s: CSV=%.2f, DB=%.2f, diff=%.4f%% (%.2f points)",
                                d, csv_val, db_val, pct_diff, abs_diff
                            )
                
                if large_diffs:
                    LOGGER.warning(
                        "Found %d overlapping dates with differences > tolerance. "
                        "These will be skipped to preserve history.",
                        len(large_diffs)
                    )
                    csv_df = csv_df[~csv_df['date'].isin(large_diffs)].copy()
        else:
            LOGGER.info("No existing data in DB, importing all %d rows", len(csv_df))
        
        if csv_df.empty:
            LOGGER.warning("No new data to import after filtering")
            return True
        
        # Save to parquet (bronze layer)
        source = "manual_csv_import"
        save_parquet(csv_df, series_id, source)
        
        # Ingest if requested
        if ingest:
            LOGGER.info("Ingesting imported data into database...")
            
            # Get series config for metadata
            config = INDEX_SERIES.get(series_id, {"name": series_id})
            
            # Insert series metadata
            con.execute("""
                INSERT OR REPLACE INTO dim_fred_series 
                (series_id, name, source, last_updated)
                VALUES (?, ?, ?, ?)
            """, [
                series_id,
                config.get("name", series_id),
                source,
                datetime.now(timezone.utc).isoformat(),
            ])
            
            # Prepare observations
            observations = csv_df.copy()
            observations["series_id"] = series_id
            observations["source"] = source
            observations["last_updated"] = datetime.now(timezone.utc).isoformat()
            
            # Insert only new dates
            con.register("temp_csv_import", observations)
            con.execute("""
                INSERT INTO f_fred_observations 
                (date, series_id, value, source, last_updated)
                SELECT 
                    date::DATE,
                    series_id,
                    value::DOUBLE,
                    source,
                    last_updated::TIMESTAMP
                FROM temp_csv_import
                WHERE NOT EXISTS (
                    SELECT 1 FROM f_fred_observations
                    WHERE series_id = temp_csv_import.series_id
                      AND date = temp_csv_import.date::DATE
                )
            """)
            con.unregister("temp_csv_import")
            
            date_range = f"{observations['date'].min()} to {observations['date'].max()}"
            LOGGER.info("Ingested %s: %d rows (%s)", series_id, len(observations), date_range)
        
        return True
        
    except Exception as e:
        LOGGER.error("Failed to process CSV import: %s", e)
        return False
    finally:
        con.close()


def process_series(
    series_id: str,
    config: Dict,
    start: date,
    end: date,
    force: bool = False,
    allow_history_change: bool = False,
) -> bool:
    """
    Download and validate a single index series.
    
    Returns True if successful, False otherwise.
    Implements non-silent failure contract.
    """
    LOGGER.info("Processing %s (%s)", series_id, config.get("name", ""))
    
    # Load existing data
    existing_df = pd.DataFrame() if force else load_existing_data(series_id)
    
    # Determine if this is a backfill (starting from scratch or far back)
    is_backfill = force or existing_df.empty or (date.today() - start).days > 365
    
    # Determine effective start date (only fetch new data in append mode)
    effective_start = start
    if not existing_df.empty and not force:
        last_date = pd.to_datetime(existing_df["date"]).max().date()
        effective_start = max(start, last_date - timedelta(days=5))  # 5-day overlap for verification
        LOGGER.info("Existing data through %s, fetching from %s", last_date, effective_start)
    
    # Fetch data (raises RuntimeError if all providers fail)
    try:
        new_df, source = fetch_index_data(series_id, config, effective_start, end)
    except RuntimeError as e:
        LOGGER.error(str(e))
        return False
    
    # Validate freshness (non-silent failure contract)
    freshness_issues = validate_freshness(
        new_df, series_id, is_backfill,
        requested_start=effective_start,
        requested_end=end,
    )
    if freshness_issues:
        for issue in freshness_issues:
            LOGGER.error("FATAL: %s", issue)
        LOGGER.error("Aborting %s due to freshness/completeness failure.", series_id)
        return False
    
    # Validate data quality
    issues = validate_data(new_df, series_id)
    if issues:
        for issue in issues:
            LOGGER.warning("Validation: %s", issue)
    
    # Check for history changes (append-only enforcement)
    if not existing_df.empty and not force:
        history_issues = check_history_changes(existing_df, new_df, series_id)
        if history_issues:
            for issue in history_issues:
                LOGGER.error("HISTORY CHANGE: %s", issue)
            if not allow_history_change:
                LOGGER.error(
                    "Aborting %s due to historical data changes. "
                    "Use --allow-history-change to override.",
                    series_id,
                )
                return False
            LOGGER.warning("Proceeding despite history changes (--allow-history-change)")
    
    # Merge with existing data (keep new values for overlapping dates)
    if not existing_df.empty and not force:
        # Remove overlapping dates from existing data
        new_dates = set(new_df["date"].astype(str))
        existing_df = existing_df.copy()
        existing_df["date_str"] = existing_df["date"].astype(str)
        existing_df = existing_df[~existing_df["date_str"].isin(new_dates)]
        existing_df = existing_df.drop(columns=["date_str"])
        
        # Combine
        combined_df = pd.concat([
            existing_df[["date", "value"]],
            new_df[["date", "value"]],
        ], ignore_index=True)
    else:
        combined_df = new_df[["date", "value"]]
    
    # Save to parquet
    save_parquet(combined_df, series_id, source)
    
    # Update manifest
    manifest = load_manifest()
    manifest[series_id] = {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "rows": len(combined_df),
        "first_date": str(combined_df["date"].min()),
        "last_date": str(combined_df["date"].max()),
    }
    save_manifest(manifest)
    
    LOGGER.info(
        "Successfully processed %s: %d rows (%s to %s) from %s",
        series_id,
        len(combined_df),
        combined_df["date"].min(),
        combined_df["date"].max(),
        source,
    )
    return True


def ingest_to_database(series_filter: Optional[List[str]] = None, force: bool = False) -> bool:
    """
    Ingest index spot data into DuckDB (f_fred_observations table).
    
    Uses the same table as FRED data for consistency, with source='yahoo' or 'marketwatch'.
    """
    from pipelines.common import get_paths, connect_duckdb
    from orchestrator import migrate
    
    # Get parquet files
    if not INDEX_SPOT_DIR.exists():
        LOGGER.error("Index spot directory not found: %s", INDEX_SPOT_DIR)
        return False
    
    parquet_files = list(INDEX_SPOT_DIR.glob("*.parquet"))
    if series_filter:
        series_upper = {s.upper() for s in series_filter}
        parquet_files = [f for f in parquet_files if f.stem.upper() in series_upper]
    
    if not parquet_files:
        LOGGER.error("No parquet files found in %s", INDEX_SPOT_DIR)
        return False
    
    # Connect to database
    _, _, db_path = get_paths()
    con = connect_duckdb(db_path)
    
    try:
        # Run migrations to ensure schema exists
        LOGGER.info("Running migrations...")
        migrate()
        
        success_count = 0
        for parquet_file in parquet_files:
            series_id = parquet_file.stem
            
            try:
                df = pd.read_parquet(parquet_file)
            except Exception as e:
                LOGGER.error("Failed to read %s: %s", parquet_file, e)
                continue
            
            if df.empty:
                LOGGER.warning("Skipping %s (empty file)", series_id)
                continue
            
            # Prepare data
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["date", "value"])
            
            if df.empty:
                LOGGER.warning("Skipping %s (no valid data)", series_id)
                continue
            
            # Get source from file
            source = df["source"].iloc[0] if "source" in df.columns else "unknown"
            last_updated = df["last_updated"].iloc[0] if "last_updated" in df.columns else None
            
            try:
                # Insert or replace series metadata
                con.execute("""
                    INSERT OR REPLACE INTO dim_fred_series 
                    (series_id, name, source, last_updated)
                    VALUES (?, ?, ?, ?)
                """, [
                    series_id,
                    INDEX_SERIES.get(series_id, {}).get("name"),
                    source,
                    last_updated,
                ])
                
                # Delete existing if force
                if force:
                    con.execute(
                        "DELETE FROM f_fred_observations WHERE series_id = ?",
                        [series_id]
                    )
                
                # Prepare observations
                observations = df[["date", "value"]].copy()
                observations["series_id"] = series_id
                observations["source"] = source
                observations["last_updated"] = last_updated
                
                # Insert using DuckDB register pattern
                con.register("temp_index_obs", observations)
                con.execute("""
                    INSERT OR REPLACE INTO f_fred_observations 
                    (date, series_id, value, source, last_updated)
                    SELECT 
                        date::DATE,
                        series_id,
                        value::DOUBLE,
                        source,
                        CASE WHEN last_updated IS NOT NULL 
                             THEN last_updated::TIMESTAMP 
                             ELSE NULL END
                    FROM temp_index_obs
                """)
                con.unregister("temp_index_obs")
                
                date_range = f"{observations['date'].min()} to {observations['date'].max()}"
                LOGGER.info("Ingested %s: %d rows (%s)", series_id, len(observations), date_range)
                success_count += 1
                
            except Exception as e:
                LOGGER.error("Failed to ingest %s: %s", series_id, e)
        
        LOGGER.info("Ingestion complete: %d/%d series", success_count, len(parquet_files))
        return success_count == len(parquet_files)
        
    finally:
        con.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download spot index prices (RUT, etc.) from Yahoo/MarketWatch."
    )
    parser.add_argument(
        "--series",
        type=str,
        help="Comma-separated list of series IDs (default: all configured)",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Default: 30 years ago.",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Force full backfill from start date (same as --force for downloads).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if data exists.",
    )
    parser.add_argument(
        "--allow-history-change",
        action="store_true",
        help="Allow processing even if historical values have changed.",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Ingest downloaded data into DuckDB after download.",
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Only ingest existing parquet files (no download).",
    )
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Sanity check providers without downloading. Tests both Yahoo and MarketWatch.",
    )
    parser.add_argument(
        "--import-csv",
        type=str,
        help="Import from CSV file instead of downloading. Bypasses providers. CSV must have 'date' and 'close' columns.",
    )
    parser.add_argument(
        "--series-id",
        type=str,
        default="RUT_SPOT",
        help="Series ID for CSV import (default: RUT_SPOT). Only used with --import-csv.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    
    args = parser.parse_args(list(argv) if argv is not None else None)
    
    configure_logging(verbose=args.verbose)
    
    # Load environment
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()
    
    # Parse series filter
    series_filter = None
    if args.series:
        series_filter = [s.strip().upper() for s in args.series.split(",")]
    
    # Filter series
    series_to_process = INDEX_SERIES
    if series_filter:
        series_to_process = {
            k: v for k, v in INDEX_SERIES.items()
            if k.upper() in series_filter
        }
        missing = set(series_filter) - set(k.upper() for k in series_to_process)
        if missing:
            LOGGER.warning("Unknown series: %s", ", ".join(missing))
    
    if not series_to_process:
        LOGGER.error("No series to process")
        return 1
    
    # Handle probe mode
    if args.probe:
        all_ok = True
        for series_id, config in series_to_process.items():
            if not run_probe(series_id, config):
                all_ok = False
        return 0 if all_ok else 1
    
    # Handle ingest-only mode
    if args.ingest_only:
        LOGGER.info("Ingesting existing parquet files into database...")
        success = ingest_to_database(series_filter, force=args.force)
        return 0 if success else 1
    
    # Handle CSV import mode
    if args.import_csv:
        series_id = args.series_id.upper()
        if series_id not in INDEX_SERIES:
            LOGGER.warning("Series ID %s not in configured series, proceeding anyway", series_id)
        
        success = process_import_csv(
            csv_path=args.import_csv,
            series_id=series_id,
            ingest=args.ingest,
        )
        return 0 if success else 1
    
    # Parse dates
    today = date.today()
    start_date = date(today.year - 30, 1, 1)  # Default: 30 years of history
    if args.start:
        start_date = date.fromisoformat(args.start)
    
    end_date = today
    if args.end:
        end_date = date.fromisoformat(args.end)
    
    # Process each series
    LOGGER.info(
        "Processing %d index series from %s to %s",
        len(series_to_process),
        start_date,
        end_date,
    )
    
    force = args.force or args.backfill
    success_count = 0
    
    for series_id, config in series_to_process.items():
        try:
            if process_series(
                series_id,
                config,
                start_date,
                end_date,
                force=force,
                allow_history_change=args.allow_history_change,
            ):
                success_count += 1
        except Exception as e:
            LOGGER.error("Failed to process %s: %s", series_id, e)
    
    LOGGER.info("Download complete: %d/%d series", success_count, len(series_to_process))
    
    # Only ingest if download was successful
    if args.ingest:
        if success_count == len(series_to_process):
            LOGGER.info("Ingesting into database...")
            ingest_to_database(series_filter, force=args.force)
        else:
            LOGGER.error(
                "Skipping ingest due to download failures. "
                "Fix issues and re-run, or use --ingest-only to ingest existing data."
            )
    
    return 0 if success_count == len(series_to_process) else 1


if __name__ == "__main__":
    raise SystemExit(main())
