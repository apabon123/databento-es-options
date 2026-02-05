## Post‑Ingest Diagnostics (Standard)

This repository is a governed data layer for Futures‑Six (CTA-style trading system). Its responsibility is **ingestion, transformation, and validation** only.

This document defines the **canonical post‑ingest diagnostics checklist** and the **standard tooling** to run it.

### Guiding Principles

- **DB-first**: diagnostics must run on the DuckDB warehouse after ingest/build (not on raw downloads).
- **No schedule assumptions**: do **not** assume equity calendars or Mon–Fri schedules. When “coverage” is discussed, it must be either:
  - **data-derived** (e.g., the data calendar (distinct `trading_date` observed in the data) `dim_session` populated from actual data), or
  - a **calendar timeline visualization** (which does not imply a trading schedule expectation).
- **Canonical views for downstream health**: downstream-facing health checks must use canonical views, primarily:
  - `v_canonical_continuous_bar_daily`
- **No ingest modifications**: diagnostics must not change ingest behavior. (Separate sync scripts may exist; diagnostics should fail loudly if prerequisites aren’t met.)

### Definition of “day” in this repository

This repository defines a **day** as a calendar date attached to a bar’s `trading_date`. Diagnostics operate only on **data-presence days**. No assumptions are made about exchange calendars or expected trading sessions.

---

## Canonical Checklist

### A) Environment / Schema Preconditions (Hard Fail)

- **DuckDB exists and is reachable**
  - From `DUCKDB_PATH` (or default `data/silver/market.duckdb`)
- **Required tables exist**
  - `g_continuous_bar_daily`
  - `dim_session` (data calendar — distinct `trading_date` observed in the data; name is legacy, does NOT represent exchange sessions)
  - `dim_canonical_series`
- **Required canonical view exists**
  - `v_canonical_continuous_bar_daily`
- **Canonical mapping is in sync**
  - `configs/canonical_series.yaml` must match `dim_canonical_series` (root → contract_series, optional flag)

### B) Data-derived Calendar Health (Hard Fail)

`dim_session` is the **data calendar** (distinct `trading_date` observed in the data), populated from `g_continuous_bar_daily`. The table name is legacy and does **NOT** represent exchange sessions.

- **`dim_session` must be populated** when `g_continuous_bar_daily` has data
- **`dim_session` must include the latest `g_continuous_bar_daily.trading_date`**

If either fails, fix by running:

```powershell
python scripts/database/sync_session_from_data.py
```

### C) Continuous Daily OHLCV Integrity (Hard Fail)

Run the repository’s existing continuous-daily validators on `g_continuous_bar_daily`, including:

- duplicate `(trading_date, contract_series)` groups
- negative volume
- NULL required fields (e.g., `close`)
- OHLC sanity (high/low/open/close relationships)

### D) Canonical Daily View Integrity (Hard Fail)

- `v_canonical_continuous_bar_daily` must be unique on `(root, trading_date)`

### E) Canonical Coverage Signals (Warning)

These are **signals**, not schedule assumptions.

- **Non-optional roots present in the recent window**
  - For a recent calendar window (default: last 14 calendar days), each non-optional root should have at least one row somewhere in that window.
  - Missing roots are warnings (investigate ingestion gaps, product coverage choices, or upstream availability).

### F) Options/Futures Integrity (Hard Fail when present)

If the relevant tables exist, run the repository’s existing validators:

- Options (`f_quote_l1`)
  - negative spreads (ask < bid)
  - unlinked instrument IDs (quotes/trades)
- Futures (`f_fut_quote_l1`)
  - unlinked instrument IDs (quotes/trades)

### G) Duplicate Key Integrity (Hard Fail when present)

If tables exist, ensure no duplicate natural keys, e.g.:

- `f_quote_l1` on `(ts_event, instrument_id)`
- `f_fut_quote_l1` on `(ts_event, instrument_id)`
- `f_continuous_quote_l1` on `(ts_event, contract_series, underlying_instrument_id)`
- `g_continuous_bar_daily` on `(trading_date, contract_series)`
- `f_fred_observations` on `(series_id, date)`

---

## Standard Tooling

### 1) Unified runner (required)

Script:
- `scripts/diagnostics/run_post_ingest_diagnostics.py`

Behavior:
- prints a structured report to stdout
- returns **exit code 2** if any **hard failure** is present
- can write a JSON artifact for CI / automation

Examples:

```powershell
# Default: infer end date from canonical data, use last 14 calendar days
python scripts/diagnostics/run_post_ingest_diagnostics.py

# Explicit window + JSON artifact
python scripts/diagnostics/run_post_ingest_diagnostics.py --start 2026-01-01 --end 2026-02-01 --json-out artifacts/post_ingest_diagnostics.json
```

### 2) Visual health report generator (required)

Script:
- `scripts/diagnostics/generate_health_report.py`

Artifact:
- static HTML (default `artifacts/health_report.html`)

Examples:

```powershell
# Default: last 365 calendar days ending at latest canonical trading_date
python scripts/diagnostics/generate_health_report.py

# Custom range + output path
python scripts/diagnostics/generate_health_report.py --start 2025-01-01 --end 2025-12-31 --out artifacts/health_report_2025.html
```

