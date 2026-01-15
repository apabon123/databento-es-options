"""
Ingest FRED series parquet files into the database.

Usage:
    python scripts/database/ingest_fred_series.py
    python scripts/database/ingest_fred_series.py --series VIXCLS,FEDFUNDS
    python scripts/database/ingest_fred_series.py --force
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
from orchestrator import migrate

LOGGER = logging.getLogger("ingest_fred_series")

DEFAULT_EXTERNAL_ROOT = Path(os.getenv("DATA_EXTERNAL_ROOT", "data/external")).resolve()
FRED_DIR = DEFAULT_EXTERNAL_ROOT / "fred"


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def get_fred_parquet_files(series_filter: Optional[List[str]] = None, fred_dir: Optional[Path] = None) -> List[Path]:
    """Get list of FRED parquet files to ingest."""
    if fred_dir is None:
        fred_dir = FRED_DIR
    if not fred_dir.exists():
        raise FileNotFoundError(f"FRED directory not found: {fred_dir}")
    
    parquet_files = sorted(fred_dir.glob("*.parquet"))
    
    if series_filter:
        # Filter to requested series
        series_upper = {s.upper() for s in series_filter}
        parquet_files = [
            f for f in parquet_files
            if f.stem.upper() in series_upper
        ]
        missing = series_upper - {f.stem.upper() for f in parquet_files}
        if missing:
            LOGGER.warning("Requested series not found: %s", ", ".join(sorted(missing)))
    
    return parquet_files


def ingest_series_file(con, parquet_path: Path, force: bool = False) -> bool:
    """Ingest a single FRED series parquet file into the database."""
    series_id = parquet_path.stem
    
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        LOGGER.error("Failed to read %s: %s", parquet_path.name, e)
        return False
    
    if df.empty:
        LOGGER.warning("Skipping %s (empty file)", series_id)
        return False
    
    if "date" not in df.columns or "value" not in df.columns:
        LOGGER.error("Invalid schema in %s: missing 'date' or 'value' columns", series_id)
        return False
    
    # Prepare data
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    
    # Get metadata
    name = None
    last_updated = None
    if "last_updated" in df.columns:
        try:
            last_updated = pd.to_datetime(df["last_updated"], errors="coerce").max()
        except Exception:
            pass
    
    # Drop rows with missing values
    df = df.dropna(subset=["date", "value"])
    
    if df.empty:
        LOGGER.warning("Skipping %s (no valid data after cleaning)", series_id)
        return False
    
    try:
        # Insert or replace series metadata
        con.execute("""
            INSERT OR REPLACE INTO dim_fred_series 
            (series_id, name, source, last_updated)
            VALUES (?, ?, ?, ?)
        """, [
            series_id,
            name,
            df.get("source", pd.Series(["FRED"])).iloc[0] if "source" in df.columns else "FRED",
            last_updated.isoformat() if last_updated and not pd.isna(last_updated) else None
        ])
        
        # Insert or replace observations
        if force:
            # Delete existing data for this series
            con.execute("DELETE FROM f_fred_observations WHERE series_id = ?", [series_id])
        
        # Prepare observations for insertion
        observations = df[["date", "value"]].copy()
        observations["series_id"] = series_id
        source_val = df["source"].iloc[0] if "source" in df.columns else "FRED"
        observations["source"] = source_val
        last_updated_str = last_updated.isoformat() if last_updated and not pd.isna(last_updated) else None
        observations["last_updated"] = last_updated_str
        
        # Insert observations using DuckDB's register/query pattern
        con.register("temp_fred_obs", observations)
        con.execute("""
            INSERT OR REPLACE INTO f_fred_observations 
            (date, series_id, value, source, last_updated)
            SELECT 
                date::DATE,
                series_id,
                value::DOUBLE,
                source,
                CASE WHEN last_updated IS NOT NULL THEN last_updated::TIMESTAMP ELSE NULL END
            FROM temp_fred_obs
        """)
        con.unregister("temp_fred_obs")
        
        row_count = len(observations)
        date_range = f"{observations['date'].min()} to {observations['date'].max()}"
        LOGGER.info("Ingested %s: %d rows (%s)", series_id, row_count, date_range)
        return True
        
    except Exception as e:
        LOGGER.error("Failed to ingest %s: %s", series_id, e)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest FRED series into database.")
    parser.add_argument(
        "--series",
        type=str,
        help="Comma-separated list of series IDs to ingest (default: all)"
    )
    parser.add_argument(
        "--external-root",
        type=str,
        default=str(DEFAULT_EXTERNAL_ROOT),
        help="Path to external data root (default: DATA_EXTERNAL_ROOT or data/external)"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        help="Path to DuckDB database (default: from .env)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion (delete existing data first)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    configure_logging(verbose=args.verbose)
    
    # Load environment
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()
    
    # Get paths
    if args.external_root:
        external_root = Path(args.external_root).resolve()
        fred_dir = external_root / "fred"
    else:
        external_root = DEFAULT_EXTERNAL_ROOT
        fred_dir = FRED_DIR
    
    # Connect to database
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()
    
    con = connect_duckdb(db_path)
    
    try:
        # Run migrations to ensure schema exists
        LOGGER.info("Running migrations...")
        migrate()
        
        # Get files to ingest
        series_filter = None
        if args.series:
            series_filter = [s.strip() for s in args.series.split(",") if s.strip()]
        
        parquet_files = get_fred_parquet_files(series_filter, fred_dir)
        
        if not parquet_files:
            LOGGER.error("No FRED parquet files found in %s", fred_dir)
            return 1
        
        LOGGER.info("Ingesting %d FRED series from %s", len(parquet_files), fred_dir)
        
        # Ingest each file
        success_count = 0
        for parquet_file in parquet_files:
            if ingest_series_file(con, parquet_file, force=args.force):
                success_count += 1
        
        LOGGER.info("Ingestion complete: %d/%d series ingested", success_count, len(parquet_files))
        
        # Show summary
        summary = con.execute("""
            SELECT 
                series_id,
                COUNT(*) as row_count,
                MIN(date) as first_date,
                MAX(date) as last_date
            FROM f_fred_observations
            GROUP BY series_id
            ORDER BY series_id
        """).fetchdf()
        
        if not summary.empty:
            LOGGER.info("\nDatabase summary:")
            LOGGER.info("\n%s", summary.to_string(index=False))
        
        return 0 if success_count == len(parquet_files) else 1
        
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

