"""
Download and ingest the configured universe of continuous futures daily OHLCV data.

The universe is defined in `configs/download_universe.yaml`. Each root entry specifies
the DataBento roll rule, rank range, and metadata. This script expands that universe,
downloads the requested symbols, transforms them into the project folder structure,
and optionally ingests the data into DuckDB.
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import databento as db
import pandas as pd
from dotenv import load_dotenv
from calendar import monthrange

try:
    import pyarrow.parquet as pq  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    pq = None

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.continuous_transform import transform_continuous_ohlcv_daily_to_folder_structure
from src.utils.universe_config import DownloadUniverseConfig, RootUniverse, load_download_universe_config
from pipelines.common import get_paths
from pipelines.loader import load as load_product

logger = logging.getLogger("download_universe_daily_ohlcv")


PRODUCT_CODE = "ES_CONTINUOUS_DAILY_MDP3"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "download_universe.yaml"
CHUNK_MONTHS = 6


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def load_api_key() -> str:
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()

    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError("DATABENTO_API_KEY not found. Set it in the environment or .env file.")
    return api_key


def add_months(d: date, months: int) -> date:
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)


def month_chunks(start_d: date, end_d: date, span_months: int = CHUNK_MONTHS) -> List[tuple[date, date]]:
    chunks: List[tuple[date, date]] = []
    current = date(start_d.year, start_d.month, 1)
    while current <= end_d:
        next_period_start = add_months(current, span_months)
        chunk_start = max(current, start_d)
        chunk_end = min(next_period_start - timedelta(days=1), end_d)
        chunks.append((chunk_start, chunk_end))
        current = next_period_start
    return chunks


def normalize_trading_date(df: pd.DataFrame, fallback: date) -> pd.Series:
    # Check if ts_event is in columns (for some schemas)
    if "ts_event" in df.columns:
        ts = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
        if not ts.isna().all():
            return ts.dt.date
    # Check if ts_event is the index (for ohlcv-1d schema)
    if df.index.name == "ts_event" or isinstance(df.index, pd.DatetimeIndex):
        ts = pd.to_datetime(df.index, utc=True, errors="coerce")
        if not ts.isna().all():
            return ts.date
    # Check for trading_date column
    if "trading_date" in df.columns:
        ts = pd.to_datetime(df["trading_date"], errors="coerce")
        if not ts.isna().all():
            return ts.dt.date
    # Fallback to provided date
    return pd.Series([fallback] * len(df))


def parquet_has_rows(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        if pq is not None:
            metadata = pq.ParquetFile(path).metadata
            return bool(metadata and metadata.num_rows and metadata.num_rows > 0)
        temp = pd.read_parquet(path)
        return not temp.empty
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("  ⚠ Could not inspect parquet '%s': %s", path, exc)
        return False


def report_missing_dates(
    root_cfg: RootUniverse,
    start_d: date,
    end_d: date,
    transformed_base: Path,
) -> None:
    expected_days = pd.bdate_range(start=start_d, end=end_d)
    if expected_days.empty:
        return

    for rank in root_cfg.ranks:
        rank_dir = transformed_base / f"rank={rank}"
        if rank_dir.exists():
            available = {
                d.name
                for d in rank_dir.iterdir()
                if d.is_dir()
            }
        else:
            available = set()

        missing = [
            dt for dt in expected_days
            if dt.strftime("%Y-%m-%d") not in available
        ]

        if missing:
            sample = ", ".join(f"{dt.strftime('%Y-%m-%d')}({dt.day_name()[:3]})" for dt in missing[:10])
            if len(missing) > 10:
                sample += f" … {len(missing) - 10} more"
            logger.warning(
                "Missing daily bars for %s rank=%s between %s and %s: %s",
                root_cfg.root,
                rank,
                start_d,
                end_d,
                sample,
            )


def download_root_daily(
    client: db.Historical,
    root_cfg: RootUniverse,
    universe_cfg: DownloadUniverseConfig,
    start_d: date,
    end_d: date,
    force_download: bool,
    instrument_base: Path,
) -> List[Path]:
    logger.info(
        "Downloading %s (%s) ranks %s | %s → %s",
        root_cfg.root,
        root_cfg.roll_strategy,
        ",".join(map(str, root_cfg.ranks)),
        start_d,
        end_d,
    )

    download_dir = instrument_base / "downloads" / root_cfg.folder / root_cfg.root.upper()
    download_dir.mkdir(parents=True, exist_ok=True)

    downloaded: List[Path] = []
    for chunk_start, chunk_end in month_chunks(start_d, end_d):
        logger.info("  Chunk %s → %s", chunk_start, chunk_end)
        try:
            data = client.timeseries.get_range(
                dataset=universe_cfg.dataset,
                schema=universe_cfg.schema,
                stype_in=universe_cfg.stype_in,
                symbols=root_cfg.symbols(),
                start=chunk_start,
                end=chunk_end + timedelta(days=1),
            )
        except Exception as exc:
            logger.error("  ✗ API error for %s chunk: %s", root_cfg.root, exc)
            continue

        df = data.to_df()
        if df.empty:
            logger.warning("  ⚠ No data returned for %s chunk %s → %s", root_cfg.root, chunk_start, chunk_end)
            continue

        df["trading_date"] = normalize_trading_date(df, chunk_start)

        for trading_date, day_df in df.groupby("trading_date"):
            if pd.isnull(trading_date):
                logger.warning("    ⚠ Skipping row with null trading_date for %s", root_cfg.root)
                continue
            date_str = trading_date.isoformat()
            out_path = download_dir / f"glbx-mdp3-{root_cfg.root.lower()}-{date_str}.{universe_cfg.schema}.fullday.parquet"
            if out_path.exists() and not force_download:
                if parquet_has_rows(out_path):
                    logger.debug("    ↺ Skipping existing file %s (manifest OK)", out_path.name)
                    continue
                logger.info("    ⟳ Existing file %s is empty; re-downloading", out_path.name)
            day_df = day_df.drop(columns=["trading_date"])
            day_df.to_parquet(out_path, index=False)
            downloaded.append(out_path)
            logger.info("    ✓ Saved %s", out_path.name)

    logger.info("Completed %s: %d parquet files", root_cfg.root, len(downloaded))
    return downloaded


def transform_and_ingest(
    downloaded: Dict[str, List[Path]],
    root_configs: Dict[str, RootUniverse],
    product_code: str,
    re_transform: bool,
    perform_ingest: bool,
    start_d: Optional[date] = None,
    end_d: Optional[date] = None,
) -> None:
    if not downloaded:
        logger.info("No files downloaded; skipping transform and ingest.")
        return

    bronze_root, _, _ = get_paths()
    transformed_dirs: List[Path] = []

    for root, parquet_files in downloaded.items():
        if not parquet_files:
            continue
        root_cfg = root_configs[root]
        logger.info("Transforming %d files for %s", len(parquet_files), root)

        output_parent = bronze_root / "ohlcv-1d" / "transformed" / root_cfg.folder / root_cfg.root.upper()
        for parquet_file in parquet_files:
            try:
                parts = parquet_file.stem.split(".")[0].split("-")
                date_str = "-".join(parts[-3:])
            except Exception as exc:
                logger.error("  ✗ Could not parse date from %s: %s", parquet_file.name, exc)
                continue

            try:
                results = transform_continuous_ohlcv_daily_to_folder_structure(
                    parquet_file=parquet_file,
                    output_base=output_parent,
                    product=product_code,
                    roll_rule=root_cfg.roll_rule_desc,
                    roll_strategy=root_cfg.roll_strategy,
                    output_mode="partitioned",
                    re_transform=re_transform,
                )
                transformed_dirs.extend(results)
            except Exception as exc:
                logger.error("  ✗ Failed to transform %s: %s", parquet_file.name, exc)
                continue

        if start_d is not None and end_d is not None:
            report_missing_dates(
                root_cfg=root_cfg,
                start_d=start_d,
                end_d=end_d,
                transformed_base=output_parent,
            )

    if not perform_ingest:
        logger.info("Skipping ingest (requested).")
        return

    unique_dirs = sorted(set(transformed_dirs))

    if not unique_dirs:
        logger.info("No transformed directories to ingest.")
        return

    logger.info("Running migrations prior to ingest…")
    from orchestrator import migrate

    migrate()

    logger.info("Ingesting %d transformed directories", len(unique_dirs))
    for source_dir in unique_dirs:
        if not (source_dir / "continuous_bars_daily").exists():
            logger.debug("  ⚠ Skipping %s (no continuous_bars_daily folder)", source_dir)
            continue
        try:
            date_str = source_dir.name
        except Exception:
            date_str = None
        try:
            load_product(product_code, source_dir, date_str)
            logger.info("  ✓ Ingested %s", source_dir)
        except Exception as exc:
            logger.error("  ✗ Failed to ingest %s: %s", source_dir, exc)


def parse_roots_arg(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def resolve_date(value: Optional[str], fallback: Optional[str], default: date) -> date:
    if value:
        return date.fromisoformat(value)
    if fallback:
        return date.fromisoformat(fallback)
    return default


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Download configured continuous daily OHLCV universe.")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG_PATH), help="Path to download universe YAML.")
    parser.add_argument("--roots", type=str, help="Comma-separated list of roots to include (defaults to all).")
    parser.add_argument("--exclude-optional", action="store_true", help="Exclude optional roots from the run.")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD).")
    parser.add_argument("--force-download", action="store_true", help="Overwrite existing downloaded parquet files.")
    parser.add_argument("--re-transform", action="store_true", help="Re-run transformations even if outputs exist.")
    parser.add_argument("--no-ingest", action="store_true", help="Skip ingest step after transformation.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")

    args = parser.parse_args(list(argv) if argv is not None else None)

    configure_logging(verbose=args.verbose)

    config_path = Path(args.config)
    if not config_path.exists():
        parser.error(f"Config file not found: {config_path}")

    universe_cfg = load_download_universe_config(config_path)

    selected_roots = universe_cfg.selected_roots(
        include_optionals=not args.exclude_optional,
        filter_roots=parse_roots_arg(args.roots),
    )

    if not selected_roots:
        parser.error("No roots selected. Check --roots or config file.")

    today = date.today()
    start_d = resolve_date(args.start, universe_cfg.default_start, default=today.replace(year=today.year - 1))
    end_d = resolve_date(args.end, universe_cfg.default_end, default=today)

    if start_d > end_d:
        parser.error("Start date must be on or before end date.")

    api_key = load_api_key()
    client = db.Historical(api_key)

    bronze_root, _, _ = get_paths()
    ohlcv_base = bronze_root / "ohlcv-1d"
    ohlcv_base.mkdir(parents=True, exist_ok=True)

    downloaded: Dict[str, List[Path]] = {}
    for root_cfg in selected_roots:
        files = download_root_daily(
            client=client,
            root_cfg=root_cfg,
            universe_cfg=universe_cfg,
            start_d=start_d,
            end_d=end_d,
            force_download=args.force_download,
            instrument_base=ohlcv_base,
        )
        downloaded[root_cfg.root] = files

    transform_and_ingest(
        downloaded=downloaded,
        root_configs={cfg.root: cfg for cfg in selected_roots},
        product_code=PRODUCT_CODE,
        re_transform=args.re_transform,
        perform_ingest=not args.no_ingest,
        start_d=start_d,
        end_d=end_d,
    )

    logger.info("Download universe run complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

