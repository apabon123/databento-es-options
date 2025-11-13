"""
Download macroeconomic data from the FRED API and store it in parquet files.

Usage examples:
    python scripts/download/download_fred_series.py
    python scripts/download/download_fred_series.py --series VIXCLS,FEDFUNDS --start 2000-01-01
    python scripts/download/download_fred_series.py --force
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.fred import (
    fetch_series_metadata,
    fetch_series_observations,
    normalize_to_business_daily,
    update_manifest,
    write_parquet_series,
)


LOGGER = logging.getLogger("download_fred_series")

DEFAULT_SERIES_CONFIG = PROJECT_ROOT / "configs" / "fred_series.yaml"
DEFAULT_SETTINGS_CONFIG = PROJECT_ROOT / "configs" / "fred_settings.yaml"
DEFAULT_EXTERNAL_ROOT = Path(os.getenv("DATA_EXTERNAL_ROOT", "data/external")).resolve()
FRED_OUTPUT_DIR = DEFAULT_EXTERNAL_ROOT / "fred"
MANIFEST_PATH = FRED_OUTPUT_DIR / "manifest.json"


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def load_yaml(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_api_key(settings: Dict) -> str:
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()

    api_key = os.getenv("FRED_API_KEY") or settings.get("api_key", "")
    api_key = api_key.strip()
    if not api_key:
        raise RuntimeError(
            "FRED API key not found. "
            "Set FRED_API_KEY in the environment or configs/fred_settings.yaml."
        )
    return api_key


def parse_series_filter(series_arg: Optional[str]) -> Optional[List[str]]:
    if not series_arg:
        return None
    return [s.strip() for s in series_arg.split(",") if s.strip()]


def resolve_date(value: Optional[str], fallback: Optional[str], default: date) -> date:
    if value:
        return date.fromisoformat(value)
    if fallback:
        return date.fromisoformat(fallback)
    return default


def filter_series(config: Dict[str, Dict], selection: Optional[Iterable[str]]) -> Dict[str, Dict]:
    if not selection:
        return config

    selection_upper = {s.upper() for s in selection}
    filtered = {
        series_id: meta
        for series_id, meta in config.items()
        if series_id.upper() in selection_upper
    }
    missing = selection_upper - set(filtered.keys())
    if missing:
        LOGGER.warning("Requested series not found in manifest: %s", ", ".join(sorted(missing)))
    return filtered


def maybe_skip(path: Path, force: bool) -> bool:
    if not path.exists():
        return False
    if force:
        LOGGER.debug("Force enabled; re-downloading %s", path.name)
        return False
    try:
        df = pd.read_parquet(path)
        if df.empty:
            LOGGER.info("Existing file %s is empty; re-downloading", path)
            return False
        LOGGER.info("Skipping %s (already exists with %d rows). Use --force to refresh.", path.name, len(df))
        return True
    except Exception:
        LOGGER.warning("Could not read existing file %s; re-downloading", path)
        return False


def process_series(
    series_id: str,
    meta: Dict,
    api_key: str,
    start: date,
    end: date,
    force: bool,
) -> None:
    output_path = FRED_OUTPUT_DIR / f"{series_id}.parquet"
    if maybe_skip(output_path, force):
        return

    LOGGER.info("Downloading %s (%s)", series_id, meta.get("name", ""))
    units = meta.get("units")
    frequency = meta.get("frequency")
    transform = meta.get("transform")

    observations = fetch_series_observations(
        series_id=series_id,
        api_key=api_key,
        start=start.isoformat(),
        end=end.isoformat(),
        units=units,
        frequency=frequency,
        transform=transform,
    )

    if observations.empty:
        LOGGER.warning("No observations returned for %s", series_id)
        return

    normalized = normalize_to_business_daily(
        observations,
        start=start.isoformat(),
        end=end.isoformat(),
    )

    metadata = fetch_series_metadata(series_id, api_key)
    last_updated_raw = metadata.get("last_updated")
    last_updated = (
        datetime.fromisoformat(last_updated_raw.replace("Z", "+00:00"))
        if last_updated_raw
        else datetime.utcnow()
    )

    write_parquet_series(normalized, output_path, series_id, last_updated=last_updated)
    update_manifest(MANIFEST_PATH, series_id, last_updated, row_count=len(normalized))


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Download macro series from FRED.")
    parser.add_argument("--config", type=str, default=str(DEFAULT_SERIES_CONFIG), help="Path to series manifest YAML.")
    parser.add_argument("--settings", type=str, default=str(DEFAULT_SETTINGS_CONFIG), help="Path to FRED settings YAML.")
    parser.add_argument("--series", type=str, help="Comma-separated list of series IDs to download.")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD). Overrides config default.")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD). Overrides config default.")
    parser.add_argument("--force", action="store_true", help="Re-download even if output already exists.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")

    args = parser.parse_args(list(argv) if argv is not None else None)

    configure_logging(verbose=args.verbose)

    series_config = load_yaml(Path(args.config)).get("series", {})
    if not series_config:
        parser.error("Series configuration is empty.")

    settings = load_yaml(Path(args.settings))
    api_key = load_api_key(settings)

    today = date.today()
    start_date = resolve_date(args.start, settings.get("default_start"), default=date(today.year - 30, 1, 1))
    end_date = resolve_date(args.end, settings.get("default_end"), default=today)

    filtered_series = filter_series(series_config, parse_series_filter(args.series))
    if not filtered_series:
        parser.error("No series selected for download.")

    LOGGER.info("Downloading %d FRED series between %s and %s", len(filtered_series), start_date, end_date)
    for sid, meta in filtered_series.items():
        try:
            process_series(sid, meta or {}, api_key, start_date, end_date, args.force)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.error("Failed to process %s: %s", sid, exc)

    LOGGER.info("FRED download complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

