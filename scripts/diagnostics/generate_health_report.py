"""
Generate a lightweight static health report artifact from canonical views.

Artifact contents (required):
  a) Coverage heatmap / dot-matrix over time for v_canonical_continuous_bar_daily
  b) Bars-per-day over time (count per trading_date)
  c) Per-root min/max date ranges

Constraints:
  - No trading-schedule assumptions (timeline is calendar days)
  - Use actual data and canonical views
  - Generate a static artifact (HTML)
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.env import load_env

load_env()

from pipelines.common import connect_duckdb, get_paths


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _iso(d: Optional[date]) -> str:
    return d.isoformat() if d else ""


def _to_date(d: Optional[date]) -> Optional[date]:
    """Normalize date-like values (e.g. pandas.Timestamp, datetime) to datetime.date for consistent keys/comparisons."""
    if d is None:
        return None
    if hasattr(d, "date") and callable(getattr(d, "date", None)):
        return d.date()
    return d


def _calendar_days(start: date, end: date) -> List[date]:
    days: List[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _view_exists(con, view_name: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.views
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [view_name],
        ).fetchone()[0]
        > 0
    )


def _sql_scalar(con, sql: str, params: Optional[List[object]] = None):
    if params is None:
        params = []
    return con.execute(sql, params).fetchone()[0]


def _sql_df(con, sql: str, params: Optional[List[object]] = None):
    if params is None:
        params = []
    return con.execute(sql, params).fetchdf()


def _default_window(con, days_back: int) -> Tuple[Optional[date], Optional[date]]:
    if not _view_exists(con, "v_canonical_continuous_bar_daily"):
        return None, None
    max_d = _sql_scalar(con, "SELECT MAX(trading_date) FROM v_canonical_continuous_bar_daily")
    if max_d is None:
        return None, None
    end = max_d if isinstance(max_d, date) else _parse_date(str(max_d))
    start = end - timedelta(days=max(1, int(days_back) - 1))
    return start, end


@dataclass(frozen=True)
class RootRange:
    root: str
    optional: bool
    min_date: Optional[date]
    max_date: Optional[date]
    days: int


def _month_starts(days: List[date]) -> List[int]:
    out: List[int] = []
    prev = None
    for i, d in enumerate(days):
        key = (d.year, d.month)
        if key != prev:
            out.append(i)
            prev = key
    return out


def _render_svg_per_root_timelines(
    root_summary: List[Tuple[str, int, int, float]],
    days: List[date],
    present: Dict[Tuple[str, date], bool],
    cell_w: int = 3,
    row_h: int = 22,
    label_w: int = 80,
    timeline_w: int = 1000,
    status_w: int = 100,
) -> str:
    """One row per root: label | timeline (one rect per day) | ✔ or ⚠ missing: N. Monthly ticks only."""
    total_w = label_w + timeline_w + status_w
    total_h = 28 + len(root_summary) * row_h
    month_cols = _month_starts(days)
    n_days = len(days)
    inner_timeline_w = timeline_w - 20

    parts: List[str] = []
    parts.append(f'<svg width="{total_w}" height="{total_h}" viewBox="0 0 {total_w} {total_h}" role="img">')
    parts.append('<style>')
    parts.append(".lbl{font:13px ui-monospace, SFMono-Regular, Menlo, monospace; fill:#222;}")
    parts.append(".status-ok{fill:#0d9488;} .status-warn{fill:#b45309;}")
    parts.append(".msep{stroke:#eee; stroke-width:1;}")
    parts.append(".on{fill:#1565c0;} .off{fill:#e5e7eb;}")
    parts.append("</style>")

    day_to_x = (inner_timeline_w / n_days) if n_days else 0

    # Month separators (vertical lines at start of each month)
    for idx in month_cols:
        x = label_w + 10 + idx * day_to_x
        parts.append(f'<line class="msep" x1="{x:.1f}" y1="24" x2="{x:.1f}" y2="{total_h}"></line>')

    # Date axis (month ticks only)
    for idx in month_cols:
        if idx < n_days:
            x = label_w + 10 + idx * day_to_x
            d = days[idx]
            parts.append(f'<text class="lbl" x="{x:.1f}" y="18" font-size="11">{d.year}-{d.month:02d}</text>')

    y0 = 28
    for r_i, (root, present_days, missing_days, _) in enumerate(root_summary):
        y = y0 + r_i * row_h
        # Root label
        parts.append(f'<text class="lbl" x="0" y="{y + row_h - 6}">{root}</text>')
        # One rect per day (tiled across inner_timeline_w)
        rect_w = (inner_timeline_w / n_days) - 0.3 if n_days else 0
        rect_w = max(0.5, rect_w)
        for d_i, d in enumerate(days):
            x = label_w + 10 + d_i * (inner_timeline_w / n_days)
            cls = "on" if present.get((root, d), False) else "off"
            parts.append(f'<rect class="{cls}" x="{x:.2f}" y="{y + 2}" width="{rect_w:.2f}" height="{row_h - 4}"></rect>')
        # Status: ✔ or ⚠ missing: N
        sx = label_w + timeline_w + 8
        if missing_days == 0:
            parts.append(f'<text class="status-ok" x="{sx}" y="{y + row_h - 6}" font-size="14">✔</text>')
        else:
            parts.append(f'<text class="status-warn" x="{sx}" y="{y + row_h - 6}" font-size="14">⚠</text>')
            parts.append(f'<text class="lbl" x="{sx + 18}" y="{y + row_h - 6}" font-size="12">missing: {missing_days}</text>')

    parts.append("</svg>")
    return "".join(parts)


def _render_svg_line(
    days: List[date],
    values: List[int],
    width: int = 1100,
    height: int = 220,
    pad_l: int = 50,
    pad_r: int = 20,
    pad_t: int = 10,
    pad_b: int = 30,
    expected_roots_per_day: Optional[int] = None,
) -> str:
    assert len(days) == len(values)
    inner_w = max(1, width - pad_l - pad_r)
    inner_h = max(1, height - pad_t - pad_b)
    vmax = max(values) if values else 1
    if expected_roots_per_day is not None:
        vmax = max(vmax, expected_roots_per_day)
    vmax = max(1, int(vmax))

    def sx(i: int) -> float:
        return pad_l + (i / max(1, len(values) - 1)) * inner_w

    def sy(v: float) -> float:
        return pad_t + (1 - (v / vmax)) * inner_h

    # Month ticks only
    month_cols = _month_starts(days)
    ticks = [(i, days[i]) for i in month_cols]

    pts = " ".join(f"{sx(i):.2f},{sy(float(v)):.2f}" for i, v in enumerate(values))

    parts: List[str] = []
    parts.append(f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">')
    parts.append('<style>')
    parts.append(".ax{stroke:#ccc;stroke-width:1;} .grid{stroke:#eee;stroke-width:1;}")
    parts.append(".lbl{font:12px ui-monospace, SFMono-Regular, Menlo, monospace; fill:#222;}")
    parts.append(".ln{fill:none;stroke:#1565c0;stroke-width:2;}")
    parts.append(".ref{stroke:#0d9488;stroke-width:1.5;stroke-dasharray:4,4;}")
    parts.append("</style>")

    # Axes
    parts.append(f'<line class="ax" x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t+inner_h}"></line>')
    parts.append(f'<line class="ax" x1="{pad_l}" y1="{pad_t+inner_h}" x2="{pad_l+inner_w}" y2="{pad_t+inner_h}"></line>')

    # Horizontal grid (0, 50%, 100%)
    for frac in (0.0, 0.5, 1.0):
        y = pad_t + (1 - frac) * inner_h
        parts.append(f'<line class="grid" x1="{pad_l}" y1="{y:.2f}" x2="{pad_l+inner_w}" y2="{y:.2f}"></line>')
        parts.append(f'<text class="lbl" x="0" y="{y+4:.2f}">{int(frac*vmax):>5}</text>')

    # Expected roots-per-day reference line
    if expected_roots_per_day is not None and expected_roots_per_day >= 0:
        ey = sy(float(expected_roots_per_day))
        parts.append(f'<line class="ref" x1="{pad_l}" y1="{ey:.2f}" x2="{pad_l+inner_w}" y2="{ey:.2f}"></line>')
        parts.append(f'<text class="lbl" x="{pad_l+inner_w-140}" y="{ey-4:.2f}" font-size="11" fill="#0d9488">expected roots per day</text>')

    # Month ticks
    for i, d in ticks:
        x = sx(i)
        parts.append(f'<line class="grid" x1="{x:.2f}" y1="{pad_t}" x2="{x:.2f}" y2="{pad_t+inner_h}"></line>')
        label = f"{d.year}-{d.month:02d}"
        parts.append(f'<text class="lbl" x="{x+2:.2f}" y="{height-10}">{label}</text>')

    parts.append(f'<polyline class="ln" points="{pts}"></polyline>')
    parts.append("</svg>")
    return "".join(parts)


def _render_root_ranges_svg(
    ranges: List[RootRange],
    overall_start: date,
    overall_end: date,
    width: int = 1100,
    height_per: int = 20,
    label_w: int = 80,
    pad_t: int = 10,
    pad_b: int = 10,
) -> str:
    total_h = pad_t + pad_b + len(ranges) * height_per
    inner_w = max(1, width - label_w - 20)
    span_days = max(1, (overall_end - overall_start).days)

    def sx(d: date) -> float:
        d = _to_date(d) or d
        return label_w + 10 + ((d - overall_start).days / span_days) * inner_w

    parts: List[str] = []
    parts.append(f'<svg width="{width}" height="{total_h}" viewBox="0 0 {width} {total_h}" role="img">')
    parts.append('<style>')
    parts.append(".lbl{font:12px ui-monospace, SFMono-Regular, Menlo, monospace; fill:#222;}")
    parts.append(".rng{stroke:#1565c0;stroke-width:4;} .opt{stroke:#9aa0a6;stroke-width:4;}")
    parts.append(".ax{stroke:#eee;stroke-width:1;}")
    parts.append("</style>")

    # Axis baseline
    parts.append(f'<line class="ax" x1="{label_w+10}" y1="{pad_t-2}" x2="{label_w+10+inner_w}" y2="{pad_t-2}"></line>')
    parts.append(f'<text class="lbl" x="{label_w+10}" y="{pad_t-4}">{overall_start.isoformat()}</text>')
    parts.append(f'<text class="lbl" x="{label_w+10+inner_w-90}" y="{pad_t-4}">{overall_end.isoformat()}</text>')

    for i, rr in enumerate(ranges):
        y = pad_t + i * height_per + 10
        parts.append(f'<text class="lbl" x="0" y="{y}">{rr.root}</text>')
        if rr.min_date and rr.max_date:
            x1 = sx(rr.min_date)
            x2 = sx(rr.max_date)
            cls = "opt" if rr.optional else "rng"
            parts.append(f'<line class="{cls}" x1="{x1:.2f}" y1="{y-4}" x2="{x2:.2f}" y2="{y-4}"></line>')
    parts.append("</svg>")
    return "".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate static canonical health report (HTML)")
    parser.add_argument("--db-path", type=str, default=None, help="DuckDB path (default: from env/config)")
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--days-back", type=int, default=365, help="Window size if start/end not provided (default: 365)")
    parser.add_argument(
        "--window-days",
        type=int,
        default=None,
        help="Show only last N calendar days ending at latest canonical trading_date (overrides --days-back / --start/--end)",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="artifacts/health_report.html",
        help="Output HTML path (default: artifacts/health_report.html)",
    )
    args = parser.parse_args()

    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        return 1

    con = connect_duckdb(db_path)
    try:
        if not _view_exists(con, "v_canonical_continuous_bar_daily"):
            print("ERROR: View v_canonical_continuous_bar_daily does not exist. Run migrations.")
            return 1

        if args.start and args.end and args.window_days is None:
            start = _parse_date(args.start)
            end = _parse_date(args.end)
        else:
            start, end = _default_window(con, args.days_back)
            if start is None or end is None:
                print("ERROR: No canonical continuous daily data found to infer window.")
                return 1

        if args.window_days is not None:
            n = max(1, args.window_days)
            max_d = _sql_scalar(con, "SELECT MAX(trading_date) FROM v_canonical_continuous_bar_daily")
            if max_d is None:
                print("ERROR: No canonical continuous daily data found for --window-days.")
                return 1
            end = _to_date(max_d) if _to_date(max_d) else _parse_date(str(max_d))
            start = end - timedelta(days=n - 1)

        if start > end:
            print("ERROR: start must be <= end")
            return 1

        days = _calendar_days(start, end)

        roots_df = _sql_df(con, "SELECT root, optional FROM dim_canonical_series ORDER BY root")
        roots = [str(r) for r in roots_df["root"].tolist()]
        optional_map = {str(r): bool(o) for r, o in zip(roots_df["root"].tolist(), roots_df["optional"].tolist())}

        agg = _sql_df(
            con,
            """
            SELECT CAST(trading_date AS DATE) AS trading_date, root, COUNT(*) AS bars
            FROM v_canonical_continuous_bar_daily
            WHERE trading_date >= ? AND trading_date <= ?
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            [start.isoformat(), end.isoformat()],
        )

        present: Dict[Tuple[str, date], bool] = {}
        bars_by_day: Dict[date, int] = {d: 0 for d in days}
        for _, row in agg.iterrows():
            day = _to_date(row["trading_date"])
            root = str(row["root"])
            bars = int(row["bars"])
            present[(root, day)] = bars > 0
            bars_by_day[day] = bars_by_day.get(day, 0) + bars

        bars_series = [int(bars_by_day.get(d, 0)) for d in days]

        total_days = len(days)
        root_summary: List[Tuple[str, int, int, float]] = []
        for root in roots:
            present_days = sum(1 for d in days if present.get((root, d), False))
            missing_days = total_days - present_days
            coverage_pct = (present_days / total_days * 100) if total_days else 0.0
            root_summary.append((root, present_days, missing_days, coverage_pct))
        root_summary.sort(key=lambda x: -x[2])  # missing_days descending
        n_required = sum(1 for r in roots if not optional_map.get(r, False))

        ranges_df = _sql_df(
            con,
            """
            SELECT root,
                   MIN(CAST(trading_date AS DATE)) AS min_date,
                   MAX(CAST(trading_date AS DATE)) AS max_date,
                   COUNT(DISTINCT CAST(trading_date AS DATE)) AS days
            FROM v_canonical_continuous_bar_daily
            GROUP BY root
            ORDER BY root
            """,
        )
        root_ranges: List[RootRange] = []
        for _, row in ranges_df.iterrows():
            root = str(row["root"])
            min_d = _to_date(row["min_date"]) if row["min_date"] is not None else None
            max_d = _to_date(row["max_date"]) if row["max_date"] is not None else None
            root_ranges.append(
                RootRange(
                    root=root,
                    optional=bool(optional_map.get(root, False)),
                    min_date=min_d,
                    max_date=max_d,
                    days=int(row["days"] or 0),
                )
            )

        # Coverage summary (root x day pairs within calendar window)
        total_cells = len(roots) * len(days)
        on_cells = sum(1 for r in roots for d in days if present.get((r, d), False))
        coverage_pct = (on_cells / total_cells * 100) if total_cells else 0.0

        timeline_svg = _render_svg_per_root_timelines(
            root_summary=root_summary,
            days=days,
            present=present,
        )
        line_svg = _render_svg_line(
            days=days,
            values=bars_series,
            expected_roots_per_day=n_required,
        )
        ranges_svg = _render_root_ranges_svg(
            ranges=root_ranges,
            overall_start=start,
            overall_end=end,
        )

        generated_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        html = _render_html(
            generated_at=generated_at,
            db_path=str(db_path),
            start=start,
            end=end,
            roots=roots,
            root_ranges=root_ranges,
            root_summary=root_summary,
            coverage_pct=coverage_pct,
            timeline_svg=timeline_svg,
            line_svg=line_svg,
            ranges_svg=ranges_svg,
        )
        out_path.write_text(html, encoding="utf-8")
        print(f"Wrote health report: {out_path}")
        return 0
    finally:
        con.close()


def _render_html(
    *,
    generated_at: str,
    db_path: str,
    start: date,
    end: date,
    roots: List[str],
    root_ranges: List[RootRange],
    root_summary: List[Tuple[str, int, int, float]],
    coverage_pct: float,
    timeline_svg: str,
    line_svg: str,
    ranges_svg: str,
) -> str:
    # Section (c) table rows
    range_rows = []
    for rr in root_ranges:
        opt = "optional" if rr.optional else "required"
        range_rows.append(
            "<tr>"
            f"<td><code>{rr.root}</code></td>"
            f"<td>{opt}</td>"
            f"<td><code>{_iso(_to_date(rr.min_date))}</code></td>"
            f"<td><code>{_iso(_to_date(rr.max_date))}</code></td>"
            f"<td style='text-align:right'>{rr.days:,}</td>"
            "</tr>"
        )

    # Section (a) per-root summary table (sorted by missing_days desc)
    summary_rows = []
    for root, present_days, missing_days, cov_pct in root_summary:
        summary_rows.append(
            "<tr>"
            f"<td><code>{root}</code></td>"
            f"<td style='text-align:right'>{present_days}</td>"
            f"<td style='text-align:right'>{missing_days}</td>"
            f"<td style='text-align:right'>{cov_pct:.1f}%</td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Canonical Health Report</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: #111; }}
    .meta {{ color:#444; font-size: 13px; }}
    h1 {{ font-size: 20px; margin: 0 0 8px 0; }}
    h2 {{ font-size: 16px; margin: 22px 0 8px 0; }}
    code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px 14px; margin: 12px 0; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #eee; padding: 8px 6px; font-size: 13px; }}
    th {{ text-align: left; color: #333; }}
    .kpi {{ display:flex; gap:16px; flex-wrap:wrap; }}
    .kpi div {{ border: 1px solid #eee; border-radius: 10px; padding: 10px 12px; min-width: 180px; }}
    .kpi .v {{ font-weight: 700; font-size: 18px; }}
    .note {{ color:#555; font-size: 13px; }}
  </style>
</head>
<body>
  <h1>Canonical Health Report</h1>
  <div class="meta">
    Generated: <code>{generated_at}</code><br/>
    Database: <code>{db_path}</code><br/>
    Window (calendar date, data presence): <code>{start.isoformat()}</code> .. <code>{end.isoformat()}</code>
  </div>

  <div class="card kpi">
    <div><div class="v">{len(roots)}</div><div class="note">Canonical roots</div></div>
    <div><div class="v">{coverage_pct:.1f}%</div><div class="note">Root×day presence (window)</div></div>
  </div>

  <h2>a) Per-root coverage (v_canonical_continuous_bar_daily)</h2>
  <div class="card">
    <div class="note">One row per root: blue = bar exists, gray = missing. Sorted by missing days (worst first).</div>
    <div style="margin-bottom: 12px;">
      <table>
        <thead>
          <tr>
            <th>root</th>
            <th style="text-align:right">present_days</th>
            <th style="text-align:right">missing_days</th>
            <th style="text-align:right">coverage_pct</th>
          </tr>
        </thead>
        <tbody>
          {''.join(summary_rows)}
        </tbody>
      </table>
    </div>
    <div style="overflow:auto; padding-top: 8px;">{timeline_svg}</div>
  </div>

  <h2>b) Bars-per-day over time</h2>
  <div class="card">
    <div class="note">Count of rows per calendar date (data presence). Dashed line = expected roots per day (required canonical roots).</div>
    <div style="overflow:auto; padding-top: 8px;">{line_svg}</div>
  </div>

  <h2>c) Per-root min/max date ranges</h2>
  <div class="card">
    <div class="note">Ranges are based on all rows available in <code>v_canonical_continuous_bar_daily</code>.</div>
    <div style="overflow:auto; padding-top: 8px;">{ranges_svg}</div>
    <div style="margin-top: 14px;">
      <table>
        <thead>
          <tr>
            <th>root</th>
            <th>required?</th>
            <th>min_date</th>
            <th>max_date</th>
            <th style="text-align:right">distinct_days</th>
          </tr>
        </thead>
        <tbody>
          {''.join(range_rows)}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(main())

