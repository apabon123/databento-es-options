from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import json

import pandas as pd
import requests

logger = logging.getLogger(__name__)

FRED_API_BASE = "https://api.stlouisfed.org/fred"
OBSERVATIONS_ENDPOINT = f"{FRED_API_BASE}/series/observations"
SERIES_ENDPOINT = f"{FRED_API_BASE}/series"


def _request(url: str, params: Dict[str, str], timeout: int = 30) -> Dict:
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if "error_code" in payload:
        code = payload.get("error_code")
        message = payload.get("error_message")
        raise RuntimeError(f"FRED API error {code}: {message}")
    return payload


def fetch_series_metadata(series_id: str, api_key: str) -> Dict:
    params = {
        "series_id": series_id,
        "file_type": "json",
        "api_key": api_key,
    }
    payload = _request(SERIES_ENDPOINT, params=params)
    series = payload.get("seriess", [])
    if series:
        return series[0]
    return {}


def fetch_series_observations(
    series_id: str,
    api_key: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    units: Optional[str] = None,
    frequency: Optional[str] = None,
    transform: Optional[str] = None,
) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "file_type": "json",
        "api_key": api_key,
    }
    if start:
        params["observation_start"] = start
    if end:
        params["observation_end"] = end
    if units:
        params["units"] = units
    if frequency:
        params["frequency"] = frequency
    if transform:
        params["transform"] = transform

    payload = _request(OBSERVATIONS_ENDPOINT, params=params)
    observations = payload.get("observations", [])
    if not observations:
        return pd.DataFrame(columns=["date", "value", "realtime_start", "realtime_end"])

    df = pd.DataFrame(observations)
    # Ensure consistent columns
    for col in ("date", "value"):
        if col not in df.columns:
            df[col] = pd.NA
    return df


def normalize_to_business_daily(
    df: pd.DataFrame,
    value_col: str = "value",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", value_col])

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.set_index("date").sort_index()

    start_dt = pd.to_datetime(start) if start else df.index.min()
    end_dt = pd.to_datetime(end) if end else df.index.max()
    if start_dt is None or pd.isna(start_dt):
        start_dt = df.index.min()
    if end_dt is None or pd.isna(end_dt):
        end_dt = df.index.max()

    bdays = pd.bdate_range(start=start_dt, end=end_dt)
    normalized = df.reindex(bdays).ffill()
    normalized.index.name = "date"
    return normalized.reset_index()


def write_parquet_series(
    df: pd.DataFrame,
    path: Path,
    series_id: str,
    source: str = "FRED",
    last_updated: Optional[datetime] = None,
) -> None:
    last_updated = last_updated or datetime.utcnow()
    df = df.copy()
    df["series_id"] = series_id
    df["source"] = source
    df["last_updated"] = last_updated.isoformat()

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df[["date", "series_id", "value", "source", "last_updated"]]
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("Saved %s rows to %s", len(df), path)


def update_manifest(
    manifest_path: Path,
    series_id: str,
    last_updated: datetime,
    row_count: int,
) -> None:
    manifest: Dict[str, Dict[str, str]] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            manifest = {}

    manifest[series_id] = {
        "last_run": datetime.utcnow().isoformat(),
        "last_updated": last_updated.isoformat(),
        "rows": row_count,
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))

