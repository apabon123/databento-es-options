"""
Audit contract series coverage in g_continuous_bar_daily.

This is a READ-ONLY audit tool to inform canonical mapping decisions.
It queries g_continuous_bar_daily for all distinct contract_series values,
calculates coverage metrics (first_date, last_date, row_count, coverage_years),
groups by root (extracted from series name), and identifies which series per root
has the BEST coverage (most rows, latest end date).

This script does NOT modify the database or any configuration files.
It is used to gather data before updating canonical_series.yaml.

Usage:
    python scripts/analysis/audit_contract_series.py
    python scripts/analysis/audit_contract_series.py --db-path custom/path/market.duckdb
"""

import argparse
import sys
from pathlib import Path
from datetime import date

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.env import load_env

load_env()

from pipelines.common import get_paths
import pandas as pd
import duckdb


def extract_root(contract_series: str) -> str:
    """Extract root symbol from contract_series (before first underscore)."""
    if '_' in contract_series:
        return contract_series.split('_')[0]
    return contract_series


def calculate_coverage_years(first_date: date, last_date: date) -> float:
    """Calculate coverage in years as a float."""
    if first_date is None or last_date is None:
        return 0.0
    delta = last_date - first_date
    return delta.days / 365.25


def audit_contract_series(db_path: Path) -> pd.DataFrame:
    """
    Query g_continuous_bar_daily for all distinct contract_series and their coverage.
    
    Returns:
        DataFrame with columns: root, contract_series, first_date, last_date, 
        row_count, coverage_years
    """
    con = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Check if table exists
        table_check = con.execute("""
            SELECT COUNT(*) as exists
            FROM information_schema.tables 
            WHERE table_name = 'g_continuous_bar_daily'
        """).fetchone()[0]
        
        if table_check == 0:
            print("ERROR: Table g_continuous_bar_daily does not exist.")
            return pd.DataFrame()
        
        # Query all distinct contract_series with their coverage metrics
        df = con.execute("""
            SELECT 
                contract_series,
                COUNT(*) as row_count,
                MIN(trading_date) as first_date,
                MAX(trading_date) as last_date
            FROM g_continuous_bar_daily
            GROUP BY contract_series
            ORDER BY contract_series
        """).fetchdf()
        
        if df.empty:
            print("No data found in g_continuous_bar_daily.")
            return df
        
        # Extract root from contract_series
        df['root'] = df['contract_series'].apply(extract_root)
        
        # Calculate coverage_years
        df['coverage_years'] = df.apply(
            lambda row: calculate_coverage_years(
                pd.to_datetime(row['first_date']).date() if pd.notna(row['first_date']) else None,
                pd.to_datetime(row['last_date']).date() if pd.notna(row['last_date']) else None
            ),
            axis=1
        )
        
        # Reorder columns
        df = df[['root', 'contract_series', 'first_date', 'last_date', 'row_count', 'coverage_years']]
        
        return df
        
    finally:
        con.close()


def identify_best_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify which contract_series per root has the BEST coverage.
    
    Best coverage = most rows, then latest end date if tied.
    
    Returns:
        DataFrame with one row per root showing the recommended canonical series.
    """
    if df.empty:
        return pd.DataFrame()
    
    best_series = []
    
    for root in sorted(df['root'].unique()):
        root_df = df[df['root'] == root].copy()
        
        # Sort by row_count (desc), then last_date (desc)
        root_df = root_df.sort_values(
            by=['row_count', 'last_date'],
            ascending=[False, False]
        )
        
        # Best is the first one
        best = root_df.iloc[0]
        best_series.append({
            'root': root,
            'contract_series': best['contract_series'],
            'first_date': best['first_date'],
            'last_date': best['last_date'],
            'row_count': best['row_count'],
            'coverage_years': best['coverage_years']
        })
    
    return pd.DataFrame(best_series)


def format_table(df: pd.DataFrame) -> str:
    """Format DataFrame as a table with aligned columns."""
    if df.empty:
        return ""
    
    # Format dates
    df_formatted = df.copy()
    df_formatted['first_date'] = pd.to_datetime(df_formatted['first_date']).dt.strftime('%Y-%m-%d')
    df_formatted['last_date'] = pd.to_datetime(df_formatted['last_date']).dt.strftime('%Y-%m-%d')
    df_formatted['row_count'] = df_formatted['row_count'].astype(int)
    df_formatted['coverage_years'] = df_formatted['coverage_years'].round(1)
    
    return df_formatted.to_string(index=False)


def main():
    parser = argparse.ArgumentParser(
        description="Audit contract series coverage in g_continuous_bar_daily (READ-ONLY)"
    )
    parser.add_argument(
        '--db-path',
        type=str,
        help='Path to DuckDB database (default: from env/config)'
    )
    
    args = parser.parse_args()
    
    # Get database path
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        _, _, db_path = get_paths()
    
    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        return 1
    
    print("=" * 80)
    print("CONTRACT SERIES AUDIT (g_continuous_bar_daily)")
    print("=" * 80)
    print(f"Database: {db_path}")
    print()
    
    # Query all contract series
    df = audit_contract_series(db_path)
    
    if df.empty:
        return 1
    
    # Display all series grouped by root
    print("ALL CONTRACT SERIES (grouped by root):")
    print("-" * 80)
    
    # Sort by root, then by row_count descending
    df_sorted = df.sort_values(by=['root', 'row_count'], ascending=[True, False])
    
    # Format and display
    print(format_table(df_sorted))
    print()
    
    # Identify best coverage per root
    best_df = identify_best_coverage(df)
    
    if not best_df.empty:
        print("=" * 80)
        print("RECOMMENDED CANONICAL (best coverage per root):")
        print("=" * 80)
        print()
        print(format_table(best_df))
        print()
        
        # Summary by root
        print("=" * 80)
        print("SUMMARY BY ROOT:")
        print("=" * 80)
        summary = df.groupby('root', as_index=False).agg({
            'contract_series': 'count',
            'row_count': 'sum',
            'first_date': 'min',
            'last_date': 'max'
        }).rename(columns={
            'contract_series': 'series_count',
            'row_count': 'total_rows'
        })
        summary['first_date'] = pd.to_datetime(summary['first_date']).dt.strftime('%Y-%m-%d')
        summary['last_date'] = pd.to_datetime(summary['last_date']).dt.strftime('%Y-%m-%d')
        print(summary.to_string(index=False))
        print()
    
    print("=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)
    print()
    print("NOTE: This is a READ-ONLY audit. No database or config files were modified.")
    print("      Use this output to inform updates to canonical_series.yaml")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
