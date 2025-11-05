from __future__ import annotations

import re
from datetime import timedelta
from typing import List, Dict

import pandas as pd

try:
    # Prefer zoneinfo if available
    from zoneinfo import ZoneInfo  # type: ignore
    CHI = ZoneInfo("America/Chicago")
    UTC = ZoneInfo("UTC")
except Exception:  # pragma: no cover - fallback only
    import pytz  # type: ignore
    CHI = pytz.timezone("America/Chicago")
    UTC = pytz.UTC


def load_dbn_to_df(path: str) -> pd.DataFrame:
    """
    Load a DBN file into a DataFrame using DataBento.
    """
    import databento as db  # local import to keep dependency surface minimal

    store = db.DBNStore.from_file(path)
    return store.to_df()


def _is_tz_aware(series: pd.Series) -> bool:
    try:
        return pd.api.types.is_datetime64tz_dtype(series)
    except Exception:
        return False


def basic_checks(df: pd.DataFrame, options_only: bool = True) -> List[str]:
    """
    Run lightweight integrity checks for BBO-1m windows.

    Returns list of human-readable error strings. Empty list means pass.
    If options_only is True, restrict checks to symbols matching r"\s[CP]\d+$".
    """
    errors: List[str] = []

    if df is None or df.empty:
        return ["DataFrame is empty"]

    # Required columns - support both bbo-1m column names and alternative names
    # bbo-1m schema uses: bid_px_00, ask_px_00, bid_sz_00, ask_sz_00, etc.
    required_base = ["ts_event", "symbol"]
    missing_base = [c for c in required_base if c not in df.columns]
    if missing_base:
        errors.append(f"Missing required columns: {missing_base}")
        return errors
    
    # Check for price/size columns - they may have _00 suffix in bbo-1m
    price_size_cols = []
    if "bid_px_00" in df.columns and "ask_px_00" in df.columns:
        price_size_cols = ["bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]
    elif "bid_px" in df.columns and "ask_px" in df.columns:
        price_size_cols = ["bid_px", "ask_px", "bid_sz", "ask_sz"]
    
    missing_prices = [c for c in price_size_cols if c not in df.columns]
    if price_size_cols and missing_prices:
        errors.append(f"Missing price/size columns: {missing_prices}")
        return errors
    
    if not price_size_cols:
        # No standard price columns found - just warn and continue with basic checks
        errors.append(f"Warning: No standard bid/ask price columns found. Available columns: {list(df.columns)}")
        # Continue with basic checks on ts_event and symbol only

    # Optional filter to options-only rows
    if options_only:
        df = df[df["symbol"].astype(str).str.contains(r"\s[CP]\d+$", regex=True, na=False)]
        if df.empty:
            errors.append("No option symbols after filtering with pattern \\s[CP]\\d+$")
            return errors

    # ts_event tz-awareness
    if not _is_tz_aware(df["ts_event"]):
        errors.append("'ts_event' is not timezone-aware")
        # Convert if naive to avoid failures below, but still record the error
        try:
            df = df.copy()
            df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        except Exception:
            pass

    # Check 10-minute window and single trading date in Chicago time
    try:
        ts = pd.to_datetime(df["ts_event"], utc=True)
        ts_min = ts.min()
        ts_max = ts.max()
        if (ts_max - ts_min) > timedelta(minutes=10):
            errors.append(
                f"Window too wide: {(ts_max - ts_min)} exceeds 10 minutes"
            )
        # Ensure all timestamps map to a single Chicago date
        chi_dates = ts.dt.tz_convert(CHI).dt.date.unique()
        if len(chi_dates) != 1:
            errors.append(
                f"Multiple Chicago dates detected in 'ts_event': {sorted(map(str, chi_dates))}"
            )
    except Exception as e:  # pragma: no cover - defensive
        errors.append(f"Failed time window/date check: {e}")

    # Monotonic non-decreasing ts_event within each symbol
    try:
        # Identify any symbol groups with decreasing time deltas
        def has_decrease(g: pd.DataFrame) -> bool:
            t = pd.to_datetime(g["ts_event"], utc=True)
            dt = t.diff()
            # Decrease is when difference is negative
            return (dt < pd.Timedelta(0)).any()

        bad_symbols = (
            df.sort_values(["symbol", "ts_event"]).groupby("symbol", as_index=False)
            .apply(has_decrease)
        )
        if isinstance(bad_symbols, pd.DataFrame):
            bad_symbols = bad_symbols[0]
        decreasing = list(df.groupby("symbol").groups.keys()) if bool(bad_symbols is True) else []
        # If groupby-apply returns Series mapping, collect offenders
        if hasattr(bad_symbols, "index") and hasattr(bad_symbols, "values"):
            decreasing = [str(sym) for sym, flag in zip(bad_symbols.index, bad_symbols.values) if bool(flag)]
        if decreasing:
            errors.append(
                f"Non-monotonic 'ts_event' detected for symbols: {sorted(set(map(str, decreasing)))[:10]}"
            )
    except Exception as e:  # pragma: no cover - defensive
        errors.append(f"Failed monotonic check: {e}")

    # No NaN in bid_px/ask_px for symbols appearing >= 2 times
    if price_size_cols:
        try:
            bid_col = "bid_px_00" if "bid_px_00" in df.columns else "bid_px"
            ask_col = "ask_px_00" if "ask_px_00" in df.columns else "ask_px"
            counts = df.groupby("symbol")["symbol"].transform("size")
            multi = df[counts >= 2]
            if multi[[bid_col, ask_col]].isna().any().any():
                # Identify offenders
                bad = (
                    multi.assign(
                        bid_nan=multi[bid_col].isna(),
                        ask_nan=multi[ask_col].isna(),
                    )
                    .query("bid_nan or ask_nan")
                    ["symbol"]
                    .astype(str)
                    .unique()
                    .tolist()
                )
                errors.append(
                    f"NaN bid/ask for symbols with >=2 rows: {sorted(bad)[:10]}"
                )
        except Exception as e:  # pragma: no cover - defensive
            errors.append(f"Failed NaN bid/ask check: {e}")

    return errors


def summarize(df: pd.DataFrame) -> Dict[str, object]:
    """
    Small summary useful for logs.
    Keys: rows, symbols, time_min, time_max, median_spread
    """
    rows = int(len(df))
    symbols = int(df["symbol"].nunique()) if "symbol" in df.columns else 0
    try:
        ts = pd.to_datetime(df["ts_event"], utc=True)
        time_min = ts.min().isoformat()
        time_max = ts.max().isoformat()
    except Exception:
        time_min = None
        time_max = None

    try:
        # Support both bbo-1m (_00 suffix) and standard column names
        ask_col = "ask_px_00" if "ask_px_00" in df.columns else "ask_px"
        bid_col = "bid_px_00" if "bid_px_00" in df.columns else "bid_px"
        if ask_col in df.columns and bid_col in df.columns:
            spread = (df[ask_col] - df[bid_col]).median()
            median_spread = float(spread) if pd.notna(spread) else None
        else:
            median_spread = None
    except Exception:
        median_spread = None

    return {
        "rows": rows,
        "symbols": symbols,
        "time_min": time_min,
        "time_max": time_max,
        "median_spread": median_spread,
    }


