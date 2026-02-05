"""
Unified post-ingest diagnostics runner for databento-es-options.

Design goals:
  - DB-first (DuckDB) checks using actual ingested data
  - Prefer canonical views for downstream-facing health (e.g., Futures-Six)
  - No trading-schedule assumptions (no Mon–Fri calendar expectations)
  - Structured report + non-zero exit code on hard failures
  - Optional JSON artifact output
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.env import load_env

load_env()

from pipelines.common import connect_duckdb, get_paths
from pipelines.validators import (
    validate_continuous_daily,
    validate_futures,
    validate_options,
)


Status = str  # PASS | WARN | FAIL | SKIP | ERROR
Severity = str  # HARD | WARN | INFO


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    name: str
    status: Status
    severity: Severity
    message: str
    metrics: Dict[str, Any] = field(default_factory=dict)


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _table_exists(con, table_name: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
        > 0
    )


def _view_exists(con, view_name: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.views
            WHERE table_schema = 'main' AND view_name = ?
            """,
            [view_name],
        ).fetchone()[0]
        > 0
    )


def _sql_scalar(con, sql: str, params: Optional[List[Any]] = None) -> Any:
    if params is None:
        params = []
    return con.execute(sql, params).fetchone()[0]


def _sql_df(con, sql: str, params: Optional[List[Any]] = None):
    if params is None:
        params = []
    return con.execute(sql, params).fetchdf()


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _format_date(d: Optional[date]) -> Optional[str]:
    if d is None:
        return None
    return d.isoformat()


def _canonical_config_expected() -> Dict[str, Dict[str, Any]]:
    """
    Load configs/canonical_series.yaml into a normalized dict keyed by root.
    """
    import yaml

    cfg_path = PROJECT_ROOT / "configs" / "canonical_series.yaml"
    data = yaml.safe_load(cfg_path.read_text())
    roots = data.get("roots", {}) if isinstance(data, dict) else {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for root, spec in roots.items():
        if not isinstance(spec, dict):
            continue
        normalized[str(root)] = {
            "contract_series": spec.get("contract_series"),
            "description": spec.get("description"),
            "optional": bool(spec.get("optional", False)),
        }
    return normalized


def _canonical_config_actual(con) -> Dict[str, Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT root, contract_series, description, optional
        FROM dim_canonical_series
        ORDER BY root
        """
    ).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for root, contract_series, description, optional in rows:
        out[str(root)] = {
            "contract_series": str(contract_series) if contract_series is not None else None,
            "description": str(description) if description is not None else None,
            "optional": bool(optional) if optional is not None else False,
        }
    return out


def _diff_canonical_config(
    expected: Dict[str, Dict[str, Any]],
    actual: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    exp_roots = set(expected.keys())
    act_roots = set(actual.keys())
    missing_in_db = sorted(exp_roots - act_roots)
    extra_in_db = sorted(act_roots - exp_roots)

    mismatches: Dict[str, Dict[str, Any]] = {}
    for root in sorted(exp_roots & act_roots):
        exp = expected[root]
        act = actual[root]
        diffs = {}
        for k in ("contract_series", "optional"):
            if exp.get(k) != act.get(k):
                diffs[k] = {"expected": exp.get(k), "actual": act.get(k)}
        # description is informational; mismatch is a warning-level signal at most
        if diffs:
            mismatches[root] = diffs

    return {
        "missing_in_db": missing_in_db,
        "extra_in_db": extra_in_db,
        "mismatches": mismatches,
    }


def _select_default_window(con) -> Tuple[Optional[date], Optional[date]]:
    """
    Choose a default diagnostic window driven by actual canonical data.
    """
    if not _view_exists(con, "v_canonical_continuous_bar_daily"):
        return None, None
    max_d = _sql_scalar(con, "SELECT MAX(trading_date) FROM v_canonical_continuous_bar_daily")
    if max_d is None:
        return None, None
    if isinstance(max_d, date):
        return max_d, max_d
    return _parse_date(str(max_d)), _parse_date(str(max_d))


def _run() -> Tuple[int, Dict[str, Any]]:
    parser = argparse.ArgumentParser(description="Run standard post-ingest diagnostics")
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to DuckDB database (default: from env/config)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date for windowed checks (YYYY-MM-DD). Default: inferred from data + --window-days",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date for windowed checks (YYYY-MM-DD). Default: inferred from data",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=14,
        help="Calendar-day window length when --start/--end not provided (default: 14)",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default=None,
        help="Optional path to write a JSON diagnostics artifact",
    )
    args = parser.parse_args()

    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()

    checks: List[CheckResult] = []

    if not db_path.exists():
        checks.append(
            CheckResult(
                check_id="env.db_exists",
                name="DuckDB database exists",
                status="FAIL",
                severity="HARD",
                message=f"Database not found at {db_path}",
                metrics={"db_path": str(db_path)},
            )
        )
        report = _finalize_report(db_path, None, None, checks)
        return 2, report

    con = connect_duckdb(db_path)
    try:
        # --- Determine window (calendar timeline, not a trading schedule expectation) ---
        if args.end:
            end = _parse_date(args.end)
        else:
            inferred_end, _ = _select_default_window(con)
            end = inferred_end
        if args.start:
            start = _parse_date(args.start)
        else:
            if end is None:
                start = None
            else:
                start = end - timedelta(days=max(1, int(args.window_days) - 1))

        if start and end and start > end:
            raise ValueError("start must be <= end")

        # --- Schema / migration health ---
        required_tables = ["dim_session", "dim_canonical_series", "g_continuous_bar_daily"]
        for t in required_tables:
            checks.append(
                CheckResult(
                    check_id=f"schema.table.{t}",
                    name=f"Table exists: {t}",
                    status="PASS" if _table_exists(con, t) else "FAIL",
                    severity="HARD",
                    message="ok" if _table_exists(con, t) else "missing",
                )
            )

        checks.append(
            CheckResult(
                check_id="schema.view.v_canonical_continuous_bar_daily",
                name="View exists: v_canonical_continuous_bar_daily",
                status="PASS" if _view_exists(con, "v_canonical_continuous_bar_daily") else "FAIL",
                severity="HARD",
                message="ok" if _view_exists(con, "v_canonical_continuous_bar_daily") else "missing (run migrations)",
            )
        )

        # --- Canonical mapping consistency: configs/ vs dim_canonical_series ---
        if _table_exists(con, "dim_canonical_series"):
            expected = _canonical_config_expected()
            actual = _canonical_config_actual(con)
            diff = _diff_canonical_config(expected, actual)
            has_diff = bool(diff["missing_in_db"] or diff["extra_in_db"] or diff["mismatches"])
            checks.append(
                CheckResult(
                    check_id="canonical.mapping.sync",
                    name="Canonical mapping matches configs/canonical_series.yaml",
                    status="PASS" if not has_diff else "FAIL",
                    severity="HARD",
                    message="ok" if not has_diff else "dim_canonical_series does not match config",
                    metrics=diff if has_diff else {},
                )
            )
        else:
            checks.append(
                CheckResult(
                    check_id="canonical.mapping.sync",
                    name="Canonical mapping matches configs/canonical_series.yaml",
                    status="SKIP",
                    severity="HARD",
                    message="dim_canonical_series missing",
                )
            )

        # --- dim_session presence & freshness (data-derived calendar) ---
        if _table_exists(con, "dim_session") and _table_exists(con, "g_continuous_bar_daily"):
            sess_cnt = int(_sql_scalar(con, "SELECT COUNT(*) FROM dim_session") or 0)
            bars_cnt = int(_sql_scalar(con, "SELECT COUNT(*) FROM g_continuous_bar_daily") or 0)
            if bars_cnt > 0 and sess_cnt == 0:
                checks.append(
                    CheckResult(
                        check_id="calendar.dim_session.populated",
                        name="dim_session populated (data-derived calendar)",
                        status="FAIL",
                        severity="HARD",
                        message="dim_session is empty; run scripts/database/sync_session_from_data.py",
                        metrics={"dim_session_rows": sess_cnt, "g_continuous_bar_daily_rows": bars_cnt},
                    )
                )
            else:
                checks.append(
                    CheckResult(
                        check_id="calendar.dim_session.populated",
                        name="dim_session populated (data-derived calendar)",
                        status="PASS",
                        severity="HARD",
                        message="ok",
                        metrics={"dim_session_rows": sess_cnt},
                    )
                )

            sess_max = _sql_scalar(con, "SELECT MAX(trade_date) FROM dim_session")
            bars_max = _sql_scalar(con, "SELECT MAX(trading_date) FROM g_continuous_bar_daily")
            # Only evaluate freshness if both sides have data
            if bars_max is not None:
                is_stale = sess_max is None or str(sess_max) < str(bars_max)
                checks.append(
                    CheckResult(
                        check_id="calendar.dim_session.fresh",
                        name="dim_session includes latest continuous daily date",
                        status="PASS" if not is_stale else "FAIL",
                        severity="HARD",
                        message="ok" if not is_stale else "dim_session is behind g_continuous_bar_daily; run scripts/database/sync_session_from_data.py",
                        metrics={
                            "dim_session_max_trade_date": str(sess_max) if sess_max is not None else None,
                            "g_continuous_bar_daily_max_trading_date": str(bars_max),
                        },
                    )
                )
        else:
            checks.append(
                CheckResult(
                    check_id="calendar.dim_session.populated",
                    name="dim_session populated (data-derived calendar)",
                    status="SKIP",
                    severity="HARD",
                    message="required tables missing",
                )
            )

        # --- Core data-quality checks (reuse existing validator logic) ---
        if _table_exists(con, "g_continuous_bar_daily"):
            for name, cnt in validate_continuous_daily(con):
                cnt_i = int(cnt)
                checks.append(
                    CheckResult(
                        check_id=f"continuous_daily.validator.{_slug(name)}",
                        name=f"Continuous daily: {name}",
                        status="PASS" if cnt_i == 0 else "FAIL",
                        severity="HARD",
                        message="ok" if cnt_i == 0 else f"{cnt_i} violations",
                        metrics={"violations": cnt_i},
                    )
                )
        else:
            checks.append(
                CheckResult(
                    check_id="continuous_daily.validator",
                    name="Continuous daily quality validators",
                    status="SKIP",
                    severity="HARD",
                    message="g_continuous_bar_daily missing",
                )
            )

        # --- Canonical view uniqueness & coverage signals (no schedule assumptions) ---
        if _view_exists(con, "v_canonical_continuous_bar_daily"):
            # One row per (root, trading_date) is expected for a daily bar view.
            dup_cnt = int(
                _sql_scalar(
                    con,
                    """
                    SELECT COUNT(*) FROM (
                      SELECT root, trading_date, COUNT(*) AS cnt
                      FROM v_canonical_continuous_bar_daily
                      GROUP BY root, trading_date
                      HAVING COUNT(*) > 1
                    )
                    """,
                )
                or 0
            )
            checks.append(
                CheckResult(
                    check_id="canonical.view.unique_root_date",
                    name="Canonical daily view: unique (root, trading_date)",
                    status="PASS" if dup_cnt == 0 else "FAIL",
                    severity="HARD",
                    message="ok" if dup_cnt == 0 else f"{dup_cnt} duplicate root-date groups",
                    metrics={"duplicate_groups": dup_cnt},
                )
            )

            # Presence over a recent window (warning-only; no schedule assumptions)
            if start and end and _table_exists(con, "dim_canonical_series"):
                roots = _sql_df(
                    con,
                    "SELECT root, optional FROM dim_canonical_series ORDER BY root",
                )
                missing_non_optional: List[str] = []
                per_root_last: Dict[str, Any] = {}
                for _, row in roots.iterrows():
                    root = str(row["root"])
                    optional = bool(row["optional"])
                    last_d = _sql_scalar(
                        con,
                        "SELECT MAX(trading_date) FROM v_canonical_continuous_bar_daily WHERE root = ?",
                        [root],
                    )
                    per_root_last[root] = str(last_d) if last_d is not None else None
                    in_window = int(
                        _sql_scalar(
                            con,
                            """
                            SELECT COUNT(*) FROM v_canonical_continuous_bar_daily
                            WHERE root = ? AND trading_date >= ? AND trading_date <= ?
                            """,
                            [root, start.isoformat(), end.isoformat()],
                        )
                        or 0
                    )
                    if (not optional) and in_window == 0:
                        missing_non_optional.append(root)

                checks.append(
                    CheckResult(
                        check_id="canonical.view.presence_recent_window",
                        name="Canonical daily view: non-optional roots present in window",
                        status="PASS" if not missing_non_optional else "WARN",
                        severity="WARN",
                        message="ok" if not missing_non_optional else "some non-optional roots have no rows in the window",
                        metrics={
                            "window_start": start.isoformat(),
                            "window_end": end.isoformat(),
                            "missing_non_optional_roots": missing_non_optional,
                            "per_root_last_trading_date": per_root_last,
                        }
                        if missing_non_optional
                        else {"window_start": start.isoformat(), "window_end": end.isoformat()},
                    )
                )
        else:
            checks.append(
                CheckResult(
                    check_id="canonical.view.unique_root_date",
                    name="Canonical daily view: unique (root, trading_date)",
                    status="SKIP",
                    severity="HARD",
                    message="v_canonical_continuous_bar_daily missing",
                )
            )

        # --- Options/futures validators (run only if tables exist) ---
        if _table_exists(con, "f_quote_l1"):
            for name, cnt in validate_options(con):
                cnt_i = int(cnt)
                checks.append(
                    CheckResult(
                        check_id=f"options.validator.{_slug(name)}",
                        name=f"Options: {name}",
                        status="PASS" if cnt_i == 0 else "FAIL",
                        severity="HARD",
                        message="ok" if cnt_i == 0 else f"{cnt_i} violations",
                        metrics={"violations": cnt_i},
                    )
                )
        else:
            checks.append(
                CheckResult(
                    check_id="options.validator",
                    name="Options validators (if options tables exist)",
                    status="SKIP",
                    severity="INFO",
                    message="f_quote_l1 not present",
                )
            )

        if _table_exists(con, "f_fut_quote_l1"):
            for name, cnt in validate_futures(con):
                cnt_i = int(cnt)
                checks.append(
                    CheckResult(
                        check_id=f"futures.validator.{_slug(name)}",
                        name=f"Futures: {name}",
                        status="PASS" if cnt_i == 0 else "FAIL",
                        severity="HARD",
                        message="ok" if cnt_i == 0 else f"{cnt_i} violations",
                        metrics={"violations": cnt_i},
                    )
                )
        else:
            checks.append(
                CheckResult(
                    check_id="futures.validator",
                    name="Futures validators (if futures tables exist)",
                    status="SKIP",
                    severity="INFO",
                    message="f_fut_quote_l1 not present",
                )
            )

        # --- Duplicate key checks (global; no schedule assumptions) ---
        checks.extend(_duplicate_key_checks(con))

        report = _finalize_report(db_path, start, end, checks)

        # Optional JSON artifact
        if args.json_out:
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(report, indent=2, sort_keys=True))

        exit_code = 2 if report["summary"]["hard_failures"] > 0 else 0
        _print_report(report)
        return exit_code, report
    finally:
        con.close()


def _slug(s: str) -> str:
    import re

    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")[:80]


def _duplicate_key_checks(con) -> List[CheckResult]:
    """
    Simple duplicate checks on primary natural keys for key fact tables.
    These are DB integrity checks and are always HARD failures when violated.
    """
    candidates: List[Tuple[str, List[str]]] = [
        ("f_quote_l1", ["ts_event", "instrument_id"]),
        ("f_fut_quote_l1", ["ts_event", "instrument_id"]),
        ("f_continuous_quote_l1", ["ts_event", "contract_series", "underlying_instrument_id"]),
        ("g_continuous_bar_daily", ["trading_date", "contract_series"]),
        ("f_fred_observations", ["series_id", "date"]),
    ]
    out: List[CheckResult] = []
    for table, cols in candidates:
        if not _table_exists(con, table):
            out.append(
                CheckResult(
                    check_id=f"integrity.duplicates.{table}",
                    name=f"Duplicates absent: {table} on ({', '.join(cols)})",
                    status="SKIP",
                    severity="INFO",
                    message="table not present",
                )
            )
            continue

        cols_str = ", ".join(cols)
        dup_groups = int(
            _sql_scalar(
                con,
                f"""
                SELECT COUNT(*) FROM (
                  SELECT {cols_str}, COUNT(*) AS cnt
                  FROM {table}
                  GROUP BY {cols_str}
                  HAVING COUNT(*) > 1
                )
                """,
            )
            or 0
        )
        out.append(
            CheckResult(
                check_id=f"integrity.duplicates.{table}",
                name=f"Duplicates absent: {table} on ({', '.join(cols)})",
                status="PASS" if dup_groups == 0 else "FAIL",
                severity="HARD",
                message="ok" if dup_groups == 0 else f"{dup_groups} duplicate key groups",
                metrics={"duplicate_key_groups": dup_groups} if dup_groups else {},
            )
        )
    return out


def _finalize_report(
    db_path: Path,
    start: Optional[date],
    end: Optional[date],
    checks: List[CheckResult],
) -> Dict[str, Any]:
    hard_failures = sum(1 for c in checks if c.status in ("FAIL", "ERROR") and c.severity == "HARD")
    warnings = sum(1 for c in checks if c.status == "WARN" or (c.status in ("FAIL", "ERROR") and c.severity == "WARN"))
    skipped = sum(1 for c in checks if c.status == "SKIP")
    passed = sum(1 for c in checks if c.status == "PASS")
    overall = "FAIL" if hard_failures else ("WARN" if warnings else "PASS")

    return {
        "meta": {
            "generated_at_utc": _utc_now_iso(),
            "db_path": str(db_path),
            "window_start": _format_date(start),
            "window_end": _format_date(end),
        },
        "summary": {
            "overall_status": overall,
            "hard_failures": hard_failures,
            "warnings": warnings,
            "skipped": skipped,
            "passed": passed,
            "total_checks": len(checks),
        },
        "checks": [asdict(c) for c in checks],
    }


def _print_report(report: Dict[str, Any]) -> None:
    meta = report["meta"]
    summary = report["summary"]
    print("=" * 88)
    print("POST-INGEST DIAGNOSTICS")
    print("=" * 88)
    print(f"Database: {meta['db_path']}")
    if meta.get("window_start") and meta.get("window_end"):
        print(f"Window:   {meta['window_start']} .. {meta['window_end']} (calendar timeline)")
    print(f"Status:   {summary['overall_status']}")
    print(
        f"Checks:   {summary['passed']} passed, {summary['warnings']} warnings, "
        f"{summary['hard_failures']} hard failures, {summary['skipped']} skipped"
    )
    print("-" * 88)

    for c in report["checks"]:
        status = c["status"]
        sev = c["severity"]
        name = c["name"]
        msg = c["message"]
        if status == "PASS":
            prefix = "[PASS]"
        elif status == "WARN":
            prefix = "[WARN]"
        elif status == "FAIL":
            prefix = "[FAIL]"
        elif status == "SKIP":
            prefix = "[SKIP]"
        else:
            prefix = "[ERROR]"
        print(f"{prefix} ({sev}) {name} — {msg}")

    print("-" * 88)
    if summary["hard_failures"] > 0:
        print("Result: HARD FAILURES present (exit code 2).")
    elif summary["warnings"] > 0:
        print("Result: warnings only (exit code 0).")
    else:
        print("Result: all checks passed (exit code 0).")
    print("=" * 88)


def main() -> int:
    try:
        exit_code, _ = _run()
        return int(exit_code)
    except Exception as e:
        # Hard fail on unexpected exceptions; keep output structured.
        print("=" * 88)
        print("POST-INGEST DIAGNOSTICS")
        print("=" * 88)
        print("[ERROR] Unhandled exception")
        print(str(e))
        print("=" * 88)
        return 1


if __name__ == "__main__":
    sys.exit(main())

