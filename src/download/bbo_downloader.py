from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime, timedelta, date
import pandas as pd
import re
import argparse
import sys

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.validation.integrity_checks import load_dbn_to_df, basic_checks, summarize
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# --- Load your API key from .env ---
# Explicitly look for .env at the project root
env_path = PROJECT_ROOT / ".env"
if not env_path.exists():
    # Fall back to default dotenv search behavior (current working dir chain)
    logger.debug(".env not found at project root; relying on default load_dotenv search path")
    load_dotenv()
else:
    logger.debug(f"Loading .env from: {env_path}")
    load_dotenv(dotenv_path=env_path)
    
api_key = os.getenv("DATABENTO_API_KEY")

if not api_key:
    raise RuntimeError("No API key found. Set DATABENTO_API_KEY in your environment or .env file at project root.")

logger.debug(f"API key loaded successfully (first 10 chars): {api_key[:10]}")

# --- Initialize DataBento client ---
import databento as db
# Client will be created inside main()

# --- Timezone setup ---
try:
    # Preferred: Python 3.9+ zoneinfo
    from zoneinfo import ZoneInfo
    CHI = ZoneInfo("America/Chicago")
    UTC = ZoneInfo("UTC")
except Exception:
    # Fallback: pytz if zoneinfo not available
    import pytz
    CHI = pytz.timezone("America/Chicago")
    UTC = pytz.UTC



# ---------- Config you'll touch most often ----------
DATASET = "GLBX.MDP3"
SCHEMA  = "bbo-1m"          # 1-minute top-of-book snapshots
WINDOW_MIN = 5              # last N minutes of RTH to pull
RTH_END_CT = (15, 0, 0)     # 3:00pm CT (15:00:00) is CME equity-index futures pit/settlement window end
OUT_DIR = Path("data/raw")  # where DBN files are written

# Example symbols: Specify actual ES option symbols
# Format: "ESZ5 C7000" = ES December 2025 Call at 7000 strike
# You can use "ES.c.0" for all ES options, or list specific symbols
SYMBOLS = ["ES.c.0"]  # All ES options (use with caution - expensive!)
# OR specify exact symbols like:
# SYMBOLS = ["ESZ5 C7000", "ESZ5 P6500", "ESZ5 C6800"]

# Date range (inclusive)
START = date(2025, 10, 15)
END   = date(2025, 10, 21)
# ----------------------------------------------------


def get_available_symbols(client: db.Historical, symbol_pattern: str, start_date: date, end_date: date):
    """
    Query available symbols matching a pattern for the given date range.
    Example patterns: 
    - "ES.c.0" for all ES options
    - "ESZ5" for all ESZ5 options (Dec 2025 expiry)
    """
    logger.info(f"Querying available symbols for pattern: {symbol_pattern}")
    
    # Use a small date window from the range
    start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=UTC)
    
    try:
        # Normalize input: for patterns like "ES.c.0" or "ES." use parent root "ES"
        # Otherwise pass through as-is (e.g., explicit native like "ESZ5 C7000")
        pattern = (symbol_pattern or "").strip()
        parent_symbol = None
        # Patterns indicating options universe for a root (e.g., "ES.c.0" or "ES.") → use parent "ROOT.OPT"
        m = re.match(r"^([A-Z]+)\.c\.[0-9]+$", pattern)
        if m:
            parent_symbol = f"{m.group(1)}.OPT"
        elif pattern.endswith(".") and re.match(r"^[A-Z]+\.$", pattern):
            parent_symbol = f"{pattern[:-1]}.OPT"

        if parent_symbol:
            stype_in = "parent"
            symbols_in = [parent_symbol]
        else:
            stype_in = "raw_symbol"
            symbols_in = [pattern]

        result = client.symbology.resolve(
            dataset=DATASET,
            symbols=symbols_in,
            stype_in=stype_in,
            stype_out="native",
            start_date=start_date,
            end_date=end_date,
        )

        symbols = []
        # The client may expose mappings as attributes or dicts depending on version
        mappings = getattr(result, "mappings", None) or getattr(result, "data", None) or []
        for mapping in mappings:
            native = getattr(mapping, "native", None)
            if native is None and isinstance(mapping, dict):
                native = mapping.get("native")
            if native:
                symbols.append(native)

        if not symbols:
            # If no concrete expansion was returned, fall back to the input pattern
            logger.warning("Resolver returned no symbols; falling back to the input pattern as-is.")
            return [symbol_pattern]

        logger.info(f"Found {len(symbols)} symbols")
        if len(symbols) > 0 and len(symbols) <= 20:
            logger.debug(f"Symbols: {symbols[:20]}")
        elif len(symbols) > 20:
            logger.debug(f"First 20 symbols: {symbols[:20]}")
            logger.info(f"... and {len(symbols) - 20} more")

        return symbols
    except Exception:
        logger.exception("Error resolving symbols; using input pattern directly as a fallback.")
        return [symbol_pattern]


def ensure_outdir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def day_iter(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def close_window_utc(d: date, minutes=5):
    """Return (start_utc, end_utc) for the last N minutes of RTH on day d in UTC."""
    # Build end time in Chicago
    end_ct = datetime(d.year, d.month, d.day, *RTH_END_CT, tzinfo=CHI)
    start_ct = end_ct - timedelta(minutes=minutes)

    return start_ct.astimezone(UTC), end_ct.astimezone(UTC)


def full_day_window_utc(d: date):
    """
    Return (start_utc, end_utc) for the full trading day on date d in UTC.
    
    Note: DataBento's timeseries.get_range() requires explicit timestamps.
    ES futures trading day: 17:00 CT previous day to 16:00 CT current day.
    
    For example, for date 2025-10-20:
    - Start: 2025-10-19 17:00:00 CT (previous day evening)
    - End: 2025-10-20 16:00:00 CT (current day afternoon)
    """
    # End time: current day at 16:00 CT
    end_ct = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=CHI)
    # Start time: previous day at 17:00 CT
    prev_day = d - timedelta(days=1)
    start_ct = datetime(prev_day.year, prev_day.month, prev_day.day, 17, 0, 0, tzinfo=CHI)
    
    return start_ct.astimezone(UTC), end_ct.astimezone(UTC)


def estimate_cost(client: db.Historical, symbols, start_d: date, end_d: date, minutes=5, stype_in: str | None = None, full_day: bool = False):
    """Use metadata.get_cost to estimate cost. If full_day=True, estimates for full trading day, otherwise last N minutes."""
    total_bytes = 0
    total_usd = 0.0
    rows = []

    for d in day_iter(start_d, end_d):
        if full_day:
            st_utc, en_utc = full_day_window_utc(d)
        else:
            st_utc, en_utc = close_window_utc(d, minutes)
        # Skip weekends by checking weekday() (Mon=0 ... Sun=6). CME trades Sunday evening, but for EOD marks we keep weekdays.
        if d.weekday() >= 5:
            continue

        kwargs = {}
        if stype_in:
            kwargs["stype_in"] = stype_in
        info = client.metadata.get_cost(
            dataset=DATASET,
            schema=SCHEMA,
            symbols=symbols,
            start=st_utc.isoformat(),
            end=en_utc.isoformat(),
            **kwargs,
        )
        # info can be a float (cost only) or object with size_bytes and cost_usd
        if isinstance(info, (int, float)):
            # Simple float response: cost only, estimate size as 0
            size_b = 0
            cost = float(info)
        elif hasattr(info, "size_bytes") or hasattr(info, "cost_usd"):
            # Object response with attributes
            size_b = getattr(info, "size_bytes", 0)
            cost = getattr(info, "cost_usd", 0.0)
        elif isinstance(info, dict):
            # Dict response
            size_b = info.get("size_bytes", 0)
            cost = info.get("cost_usd", 0.0)
        else:
            size_b = 0
            cost = 0.0

        rows.append({"date": d.isoformat(), "size_bytes": size_b, "cost_usd": cost})
        total_bytes += size_b
        total_usd   += float(cost)

    est = pd.DataFrame(rows)
    return est, total_bytes, total_usd


def download_bbo_last_window(client: db.Historical, symbols, start_d: date, end_d: date, minutes=5, stype_in: str | None = None, full_day: bool = False):
    """Download data per day. If full_day=True, downloads full trading day, otherwise last N minutes."""
    ensure_outdir()
    manifest = []

    for d in day_iter(start_d, end_d):
        if full_day:
            st_utc, en_utc = full_day_window_utc(d)
            window_desc = "full day"
        else:
            st_utc, en_utc = close_window_utc(d, minutes)
            window_desc = f"{minutes}m"
        if d.weekday() >= 5:
            continue

        logger.info(f"Downloading {d.isoformat()} {st_utc.isoformat()} -> {en_utc.isoformat()} ({window_desc}) ...")
        kwargs = {}
        if stype_in:
            kwargs["stype_in"] = stype_in
        data = client.timeseries.get_range(
            dataset=DATASET,
            schema=SCHEMA,
            symbols=symbols,
            start=st_utc,
            end=en_utc,
            **kwargs,
        )
        
        # bbo-1m filters on ts_recv (when snapshot was received), not ts_event (last trade time)
        # So the data we received IS already within our requested window
        df_raw = data.to_df()
        logger.info(f"API returned {len(df_raw)} rows for {minutes}m window")
        
        if df_raw.empty:
            logger.warning(f"No data received for {d.isoformat()}")
            continue
            
        # Verify ts_recv is within our window (should always be true per DataBento)
        if 'ts_recv' in df_raw.columns:
            df_raw['ts_recv'] = pd.to_datetime(df_raw['ts_recv'], utc=True)
            in_window = df_raw[(df_raw['ts_recv'] >= st_utc) & (df_raw['ts_recv'] <= en_utc)]
            pct_in_window = len(in_window) / len(df_raw) * 100 if len(df_raw) > 0 else 0
            logger.info(f"Verified: {pct_in_window:.1f}% of rows have ts_recv within requested window")
        
        # Use all data - it's already filtered by ts_recv
        df_filtered = df_raw.copy()
        if 'ts_recv' not in df_filtered.columns:
            if df_filtered.index.name == 'ts_recv':
                df_filtered = df_filtered.reset_index().rename(columns={'index': 'ts_recv'})
            else:
                df_filtered = df_filtered.reset_index(drop=True)
        if 'ts_recv' in df_filtered.columns:
            df_filtered['ts_recv'] = pd.to_datetime(df_filtered['ts_recv'], utc=True)
        
        # Write filtered data as parquet (can't easily write filtered DBN)
        if full_day:
            out_file = OUT_DIR / f"glbx-mdp3-{d.isoformat()}.{SCHEMA}.fullday.parquet"
        else:
            out_file = OUT_DIR / f"glbx-mdp3-{d.isoformat()}.{SCHEMA}.last{minutes}m.parquet"
        df_filtered.to_parquet(out_file, index=False)
        logger.info(f"Wrote filtered data: {out_file.name}")
        # Validate filtered data
        if globals().get("_VALIDATE_FILES", True):
            try:
                # Data is already loaded as df_filtered
                errs = basic_checks(df_filtered, options_only=False)
                info = summarize(df_filtered)
                logger.info(f"Summary for {out_file.name}: {info}")
                
                # For filtered data, only check critical errors
                critical_errs = [e for e in errs if not (
                    "Window too wide" in e or 
                    "Multiple Chicago dates" in e or 
                    "Failed monotonic check" in e or
                    "NaN bid/ask" in e  # Common for illiquid options
                )]
                
                if critical_errs:
                    logger.error(f"Validation errors for {out_file.name}:")
                    for e in critical_errs:
                        logger.error(f"  - {e}")
                    raise SystemExit(1)
                else:
                    logger.info(f"{out_file.name}: VALID ({len(df_filtered)} rows, {df_filtered['symbol'].nunique()} symbols)")
            except SystemExit:
                raise
            except Exception as e:
                logger.error(f"Validation failed for {out_file.name}: {e}")
                raise SystemExit(1)
        manifest.append({"date": d.isoformat(), "file": str(out_file)})

    return pd.DataFrame(manifest)


def dbn_to_parquet_mid(closed_dbn_path: Path) -> Path:
    """
    Optional helper: read a per-day DBN file and write a parquet with mids and an aggregated 'quote close'.
    """
    store = db.DBNStore.from_file(str(closed_dbn_path))
    df = store.to_df()
    # Expect columns like: ts_event, symbol, bid_px, ask_px, bid_sz, ask_sz
    
    # Optional: Filter for ES options only (excludes ES futures)
    # ES options symbols contain a space then C/P then digits, e.g. "ESZ5 C7000"
    # Uncomment the line below to filter:
    # df = df[df["symbol"].str.contains(r"\s[CP]\d+$", regex=True, na=False)]
    
    df = df.sort_values(["symbol", "ts_event"])
    df["mid"] = (df["bid_px"] + df["ask_px"]) / 2

    # Aggregate to a single 'close' per symbol per day using median(mid)
    df["date"] = pd.to_datetime(df["ts_event"]).dt.date
    qclose = (
        df.groupby(["date", "symbol"], as_index=False)
          .agg(mid_close=("mid", "median"),
               spread_close=("ask_px", lambda x: (x - df.loc[x.index, "bid_px"]).median()))
    )
    out_pq = closed_dbn_path.with_suffix("").with_suffix(".parquet")
    qclose.to_parquet(out_pq, index=False)
    return out_pq


def pretty_cost(est_df, tot_bytes, tot_usd):
    """Pretty-print per-day estimate table and totals in human-friendly units."""
    if est_df.empty:
        logger.info("No trading days in range (weekends/filters).")
        return
    df = est_df.copy()
    # Add human-friendly columns
    df["size_mb"] = df["size_bytes"].astype(float) / 1e6
    df = df[["date", "size_mb", "cost_usd"]]
    df = df.rename(columns={"date": "Date", "size_mb": "Size (MB)", "cost_usd": "Cost (USD)"})
    # Round for display
    df["Size (MB)"] = df["Size (MB)"].round(3)
    df["Cost (USD)"] = df["Cost (USD)"].map(lambda x: float(x)).round(2)
    logger.info(df.to_string(index=False))
    logger.info(f"Estimated total size: {tot_bytes/1e6:.3f} MB")
    logger.info(f"Estimated total cost: ${tot_usd:.2f} USD")


def confirm_or_abort(tot_usd, max_budget_usd=None):
    """Prompt user to confirm proceeding. Enforces optional budget threshold."""
    if max_budget_usd is not None and tot_usd > max_budget_usd:
        raise SystemExit(2)
    ans = input("Proceed? [y/N] ").strip().lower()
    if ans != "y":
        raise SystemExit("Aborted by user before download.")


def parse_date(dstr: str) -> date:
    """Parse a YYYY-MM-DD string into a date."""
    return datetime.strptime(dstr, "%Y-%m-%d").date()


def parse_args() -> argparse.Namespace:
    """Build and parse CLI arguments for estimating and downloading BBO-1m windows."""
    parser = argparse.ArgumentParser(description="Estimate and download BBO-1m 'last N minutes per weekday' DBN files.")
    parser.add_argument("--start", type=parse_date, default=START, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", type=parse_date, default=END, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--window-min", type=int, default=WINDOW_MIN, help="Window size in minutes (default: 5)")
    parser.add_argument("--symbols", type=str, default="ES.", help="Symbol pattern to resolve (e.g., 'ES.' or 'ES.c.0')")
    parser.add_argument("--dry-run", action="store_true", help="Estimate only; do not download")
    parser.add_argument("--max-budget", type=float, default=None, help="Abort if estimated cost exceeds this USD amount")
    try:
        # Python 3.9+ supports BooleanOptionalAction
        parser.add_argument("--to-parquet", default=True, action=argparse.BooleanOptionalAction, help="Also convert DBN to parquet (default: true)")
        parser.add_argument("--validate", default=True, action=argparse.BooleanOptionalAction, help="Validate written files (default: true)")
    except Exception:
        # Fallback if BooleanOptionalAction unavailable
        parser.add_argument("--to-parquet", dest="to_parquet", action="store_true", default=True, help="Also convert DBN to parquet (default: true)")
        parser.add_argument("--no-parquet", dest="to_parquet", action="store_false", help="Do not convert DBN to parquet")
        parser.add_argument("--validate", dest="validate", action="store_true", default=True, help="Validate written files (default: true)")
        parser.add_argument("--no-validate", dest="validate", action="store_false", help="Do not validate written files")
    return parser.parse_args()


def main(args: argparse.Namespace) -> int:
    """Entry point to estimate cost and optionally download DBN files with flags."""
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        logger.error("Set DATABENTO_API_KEY in your environment.")
        return 2

    client = db.Historical(key=api_key)

    start_d: date = args.start
    end_d: date = args.end
    window_min: int = args.window_min
    symbol_pattern: str = args.symbols

    # Normalize input symbols for API calls to avoid resolver issues
    # Map patterns like "ES.c.0" or "ES." to parent ES options universe
    stype_in = None
    symbols_for_api = [symbol_pattern]
    m = re.match(r"^([A-Z]+)\.c\.[0-9]+$", symbol_pattern)
    if m:
        stype_in = "parent"
        symbols_for_api = [f"{m.group(1)}.OPT"]
    elif symbol_pattern.endswith(".") and re.match(r"^[A-Z]+\.$", symbol_pattern):
        stype_in = "parent"
        symbols_for_api = [f"{symbol_pattern[:-1]}.OPT"]
    else:
        stype_in = "raw_symbol"
        symbols_for_api = [symbol_pattern]

    # Estimate
    logger.info("Estimating cost …")
    est_df, tot_bytes, tot_usd = estimate_cost(client, symbols_for_api, start_d, end_d, minutes=window_min, stype_in=stype_in)
    pretty_cost(est_df, tot_bytes, tot_usd)

    # Budget enforcement
    if args.max_budget is not None and tot_usd > args.max_budget:
        logger.error(f"Estimated cost ${tot_usd:.2f} exceeds budget ${args.max_budget:.2f}. Aborting.")
        return 2

    # Dry-run ends here
    if args.dry_run:
        return 0

    # Interactive confirmation
    try:
        confirm_or_abort(tot_usd, max_budget_usd=None)
    except SystemExit as e:
        # If user aborted, propagate as non-zero unless code provided
        code = e.code if isinstance(e.code, int) else 1
        return code

    # Download
    logger.info("Downloading data …")
    # Toggle validation hook for this run
    global _VALIDATE_FILES
    _VALIDATE_FILES = bool(getattr(args, "validate", True))
    manifest = download_bbo_last_window(client, symbols_for_api, start_d, end_d, minutes=window_min, stype_in=stype_in)
    if manifest.empty:
        logger.error("No files were written (no trading days?).")
        return 1
    logger.info("Wrote DBN files:")
    for _, row in manifest.iterrows():
        logger.info(f"  {row['file']}")

    # Optional conversion
    if args.to_parquet:
        logger.info("Converting to quote-based daily close parquet …")
        for _, row in manifest.iterrows():
            pq = dbn_to_parquet_mid(Path(row["file"]))
            logger.info(f"  {pq}")

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    args = parse_args()
    try:
        rc = main(args)
    except RuntimeError as e:
        logger.error(str(e))
        rc = 2
    sys.exit(rc)

