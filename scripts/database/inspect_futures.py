"""
Futures Data Inspection Script

This script inspects futures BBO-1m data stored in the DuckDB database.
Supports all futures roots (ES, SI, NQ, GC, CL, etc.) and continuous contracts.

Usage:
    python scripts/database/inspect_futures.py
    python scripts/database/inspect_futures.py --contract ESH6
    python scripts/database/inspect_futures.py --root SI
    python scripts/database/inspect_futures.py --export
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import argparse
from datetime import datetime
import os




def setup_display():
    """Configure pandas display options."""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)


def connect_to_db(db_path: Path):
    """Connect to the DuckDB database."""
    print(f"Database path: {db_path}")
    print(f"Exists: {db_path.exists()}")
    
    if not db_path.exists():
        print("\nERROR: Database not found!")
        print("Please run: python scripts/download/download_and_ingest_futures.py --weeks 1")
        return None
    
    con = duckdb.connect(str(db_path), read_only=True)
    print("Connected to database successfully!\n")
    return con


def list_tables(con):
    """List all tables in the database."""
    print("=" * 80)
    print("DATABASE TABLES")
    print("=" * 80)
    
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchdf()
    
    print(tables.to_string(index=False))
    print()


def show_summary(con):
    """Show summary of futures tables."""
    print("=" * 80)
    print("FUTURES TABLES SUMMARY")
    print("=" * 80)
    
    # Check if futures tables exist
    tables_check = con.execute("""
        SELECT COUNT(*) as exists
        FROM information_schema.tables 
        WHERE table_name = 'dim_fut_instrument'
    """).fetchone()[0]
    
    if tables_check == 0:
        print("Futures tables do not exist in database.")
        print("Run: python scripts/download/download_and_ingest_futures.py --weeks 1")
        print()
    else:
        summary = con.execute("""
            SELECT 
                'dim_fut_instrument' as table_name,
                COUNT(*) as row_count,
                COUNT(DISTINCT instrument_id) as unique_instruments
            FROM dim_fut_instrument
            
            UNION ALL
            
            SELECT 
                'f_fut_quote_l1' as table_name,
                COUNT(*) as row_count,
                COUNT(DISTINCT instrument_id) as unique_instruments
            FROM f_fut_quote_l1
            
            UNION ALL
            
            SELECT 
                'g_fut_bar_1m' as table_name,
                COUNT(*) as row_count,
                COUNT(DISTINCT instrument_id) as unique_instruments
            FROM g_fut_bar_1m
        """).fetchdf()
        
        print(summary.to_string(index=False))
        print()
        
        # Show date range for all futures
        date_range = con.execute("""
            SELECT 
                MIN(CAST(ts_event AS DATE)) as first_date,
                MAX(CAST(ts_event AS DATE)) as last_date,
                COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days
            FROM f_fut_quote_l1
        """).fetchdf()
        
        if not date_range.empty and date_range['first_date'].iloc[0] is not None:
            print("Futures Date Range:")
            print(date_range.to_string(index=False))
            print()
        
        # Show summary by root
        roots_summary = con.execute("""
            SELECT 
                i.root,
                COUNT(DISTINCT i.instrument_id) as unique_instruments,
                COUNT(DISTINCT q.instrument_id) as instruments_with_quotes,
                MIN(CAST(q.ts_event AS DATE)) as first_date,
                MAX(CAST(q.ts_event AS DATE)) as last_date,
                COUNT(*) as total_quotes
            FROM dim_fut_instrument i
            LEFT JOIN f_fut_quote_l1 q ON i.instrument_id = q.instrument_id
            GROUP BY i.root
            ORDER BY i.root
        """).fetchdf()
        
        if not roots_summary.empty:
            print("=" * 80)
            print("FUTURES BY ROOT")
            print("=" * 80)
            print(roots_summary.to_string(index=False))
            print()
    
    # Check if continuous futures tables exist
    tables_check = con.execute("""
        SELECT COUNT(*) as has_continuous
        FROM information_schema.tables 
        WHERE table_name = 'f_continuous_quote_l1'
    """).fetchdf()
    
    if tables_check['has_continuous'].iloc[0] > 0:
        print("=" * 80)
        print("CONTINUOUS FUTURES TABLES SUMMARY")
        print("=" * 80)
        
        # Build query dynamically based on which tables exist
        queries = []
        
        # Always check these tables
        queries.append("""
            SELECT 
                'dim_continuous_contract' as table_name,
                COUNT(*) as row_count,
                COUNT(DISTINCT contract_series) as unique_series
            FROM dim_continuous_contract
        """)
        
        queries.append("""
            SELECT 
                'f_continuous_quote_l1' as table_name,
                COUNT(*) as row_count,
                COUNT(DISTINCT contract_series) as unique_series
            FROM f_continuous_quote_l1
        """)
        
        # Check if 1-minute bar table exists
        bar_1m_check = con.execute("""
            SELECT COUNT(*) as exists
            FROM information_schema.tables 
            WHERE table_name = 'g_continuous_bar_1m'
        """).fetchone()[0]
        
        if bar_1m_check > 0:
            queries.append("""
                SELECT 
                    'g_continuous_bar_1m' as table_name,
                    COUNT(*) as row_count,
                    COUNT(DISTINCT contract_series) as unique_series
                FROM g_continuous_bar_1m
            """)
        
        # Check if daily bar table exists
        bar_daily_check = con.execute("""
            SELECT COUNT(*) as exists
            FROM information_schema.tables 
            WHERE table_name = 'g_continuous_bar_daily'
        """).fetchone()[0]
        
        if bar_daily_check > 0:
            queries.append("""
                SELECT 
                    'g_continuous_bar_daily' as table_name,
                    COUNT(*) as row_count,
                    COUNT(DISTINCT contract_series) as unique_series
                FROM g_continuous_bar_daily
            """)
        
        # Combine all queries
        query = " UNION ALL ".join(queries)
        continuous_summary = con.execute(query).fetchdf()
        
        print(continuous_summary.to_string(index=False))
        print()


def show_instruments(con, root_filter=None):
    """Show all futures instruments, optionally filtered by root."""
    print("=" * 80)
    print("INSTRUMENT DEFINITIONS" + (f" (Root: {root_filter})" if root_filter else ""))
    print("=" * 80)
    
    query = """
        SELECT 
            instrument_id,
            root,
            expiry,
            symbol_feed,
            symbol_canonical,
            tick_size,
            multiplier
        FROM dim_fut_instrument
    """
    
    params = []
    if root_filter:
        query += " WHERE root = ?"
        params.append(root_filter)
    
    query += " ORDER BY root, symbol_canonical"
    
    instruments = con.execute(query, params).fetchdf()
    
    if instruments.empty:
        if root_filter:
            print(f"No instruments found for root: {root_filter}")
        else:
            print("No instruments found in database.")
        print()
        return
    
    print(f"Total instruments: {len(instruments)}")
    
    # Group by root if no filter
    if not root_filter:
        roots = instruments['root'].unique()
        print(f"Roots: {', '.join(sorted(roots))}")
        print()
        
        # Show summary by root
        by_root = instruments.groupby('root', as_index=False).agg({
            'instrument_id': 'count'
        }).rename(columns={'instrument_id': 'count'})
        print("Instruments by root:")
        print(by_root.to_string(index=False))
        print()
        
        # Show sample from each root
        print("Sample instruments (first 3 per root):")
        for root in sorted(roots):
            root_instruments = instruments[instruments['root'] == root].head(3)
            if not root_instruments.empty:
                print(f"\n{root}:")
                print(root_instruments[['symbol_canonical', 'expiry', 'tick_size', 'multiplier']].to_string(index=False))
    else:
        print()
        print(instruments.to_string(index=False))
    
    print()
    
    # Show continuous contracts if they exist
    tables_check = con.execute("""
        SELECT COUNT(*) as has_continuous
        FROM information_schema.tables 
        WHERE table_name = 'dim_continuous_contract'
    """).fetchdf()
    
    if tables_check['has_continuous'].iloc[0] > 0:
        continuous_contracts = con.execute("""
            SELECT 
                contract_series,
                root,
                roll_rule,
                adjustment_method,
                description
            FROM dim_continuous_contract
            ORDER BY contract_series
        """).fetchdf()
        
        if len(continuous_contracts) > 0:
            print("=" * 80)
            print("CONTINUOUS CONTRACT DEFINITIONS")
            print("=" * 80)
            print(f"Total continuous contracts: {len(continuous_contracts)}")
            print()
            print(continuous_contracts.to_string(index=False))
            print()


def show_quote_coverage(con):
    """Show quote data coverage."""
    print("=" * 80)
    print("QUOTE DATA COVERAGE")
    print("=" * 80)
    
    date_range = con.execute("""
        SELECT 
            MIN(CAST(ts_event AS DATE)) as first_date,
            MAX(CAST(ts_event AS DATE)) as last_date,
            COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days,
            COUNT(*) as total_quotes,
            COUNT(DISTINCT instrument_id) as unique_contracts
        FROM f_fut_quote_l1
    """).fetchdf()
    
    print(date_range.to_string(index=False))
    print()


def show_quotes_per_contract(con):
    """Show quotes per contract."""
    print("=" * 80)
    print("QUOTES PER CONTRACT")
    print("=" * 80)
    
    quotes_per_contract = con.execute("""
        SELECT 
            i.symbol_canonical,
            COUNT(*) as quote_count,
            MIN(CAST(q.ts_event AS DATE)) as first_quote,
            MAX(CAST(q.ts_event AS DATE)) as last_quote,
            AVG(q.ask_px - q.bid_px) as avg_spread_pts,
            AVG(q.bid_sz + q.ask_sz) as avg_total_size
        FROM f_fut_quote_l1 q
        JOIN dim_fut_instrument i ON q.instrument_id = i.instrument_id
        GROUP BY i.symbol_canonical
        ORDER BY quote_count DESC
    """).fetchdf()
    
    print(quotes_per_contract.to_string(index=False))
    print()


def show_daily_quotes(con):
    """Show daily quote summary."""
    print("=" * 80)
    print("DAILY QUOTE SUMMARY")
    print("=" * 80)
    
    daily_quotes = con.execute("""
        SELECT 
            CAST(ts_event AS DATE) as date,
            COUNT(*) as quote_count,
            COUNT(DISTINCT instrument_id) as active_contracts,
            AVG(ask_px - bid_px) as avg_spread_pts
        FROM f_fut_quote_l1
        GROUP BY CAST(ts_event AS DATE)
        ORDER BY date
    """).fetchdf()
    
    print(daily_quotes.to_string(index=False))
    print()


def show_bars_per_contract(con):
    """Show 1-minute bars per contract."""
    print("=" * 80)
    print("1-MINUTE BARS PER CONTRACT")
    print("=" * 80)
    
    bars = con.execute("""
        SELECT 
            i.symbol_canonical,
            COUNT(*) as bar_count,
            MIN(CAST(b.ts_minute AS DATE)) as first_bar,
            MAX(CAST(b.ts_minute AS DATE)) as last_bar,
            AVG(b.o_mid) as avg_open,
            AVG(b.c_mid) as avg_close
        FROM g_fut_bar_1m b
        JOIN dim_fut_instrument i ON b.instrument_id = i.instrument_id
        GROUP BY i.symbol_canonical
        ORDER BY bar_count DESC
    """).fetchdf()
    
    print(bars.to_string(index=False))
    print()


def show_contract_bars(con, contract_symbol='ESZ5'):
    """Show bars for a specific contract."""
    print("=" * 80)
    print(f"{contract_symbol} - 1-MINUTE BARS")
    print("=" * 80)
    
    contract_bars = con.execute(f"""
        SELECT 
            b.ts_minute,
            b.o_mid as mid_open,
            b.h_mid as mid_high,
            b.l_mid as mid_low,
            b.c_mid as mid_close,
            b.v_trades as volume,
            b.v_notional as notional
        FROM g_fut_bar_1m b
        JOIN dim_fut_instrument i ON b.instrument_id = i.instrument_id
        WHERE i.symbol_canonical = '{contract_symbol}'
        ORDER BY b.ts_minute
    """).fetchdf()
    
    if len(contract_bars) == 0:
        print(f"No bars found for {contract_symbol}")
        print()
        return
    
    print(f"Total bars: {len(contract_bars)}")
    print()
    print("First 5 bars:")
    print(contract_bars.head(5).to_string(index=False))
    print()
    print("Last 5 bars:")
    print(contract_bars.tail(5).to_string(index=False))
    print()
    
    # Summary statistics
    print("Summary Statistics (min/median/max):")
    summary = contract_bars[['mid_open', 'mid_high', 'mid_low', 'mid_close']].agg(['min', 'median', 'max']).T
    print(summary.to_string())
    print()


def show_spread_analysis(con):
    """Show spread analysis across all contracts."""
    print("=" * 80)
    print("SPREAD ANALYSIS BY CONTRACT")
    print("=" * 80)
    
    spread_stats = con.execute("""
        SELECT 
            i.symbol_canonical,
            COUNT(*) as quote_count,
            MIN(q.ask_px - q.bid_px) as min_spread,
            AVG(q.ask_px - q.bid_px) as avg_spread,
            MAX(q.ask_px - q.bid_px) as max_spread,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY q.ask_px - q.bid_px) as median_spread
        FROM f_fut_quote_l1 q
        JOIN dim_fut_instrument i ON q.instrument_id = i.instrument_id
        GROUP BY i.symbol_canonical
        ORDER BY avg_spread
    """).fetchdf()
    
    print(spread_stats.to_string(index=False))
    print()


def show_continuous_daily_bars(con, contract_series='ES_FRONT_CALENDAR_2D'):
    """Show daily bars for a continuous contract."""
    print("=" * 80)
    print(f"CONTINUOUS CONTRACT: {contract_series} - DAILY BARS")
    print("=" * 80)
    
    # Check if table exists
    table_check = con.execute("""
        SELECT COUNT(*) as exists
        FROM information_schema.tables 
        WHERE table_name = 'g_continuous_bar_daily'
    """).fetchone()[0]
    
    if table_check == 0:
        print("Daily bars table does not exist.")
        print()
        return
    
    daily_bars = con.execute("""
        SELECT 
            trading_date,
            contract_series,
            open,
            high,
            low,
            close,
            volume
        FROM g_continuous_bar_daily
        WHERE contract_series = ?
        ORDER BY trading_date
    """, [contract_series]).fetchdf()
    
    if len(daily_bars) == 0:
        print(f"No daily bars found for {contract_series}")
        print()
        return
    
    print(f"Total daily bars: {len(daily_bars)}")
    print(f"Date range: {daily_bars['trading_date'].min()} to {daily_bars['trading_date'].max()}")
    print()
    print("First 5 bars:")
    print(daily_bars.head(5).to_string(index=False))
    print()
    print("Last 5 bars:")
    print(daily_bars.tail(5).to_string(index=False))
    print()
    
    # Summary statistics
    print("Summary Statistics (min/median/max):")
    summary = daily_bars[['open', 'high', 'low', 'close', 'volume']].agg(['min', 'median', 'max']).T
    print(summary.to_string())
    print()


def show_continuous_daily_coverage(con):
    """Show daily bars coverage by contract series."""
    print("=" * 80)
    print("CONTINUOUS DAILY BARS COVERAGE")
    print("=" * 80)
    
    # Check if table exists
    table_check = con.execute("""
        SELECT COUNT(*) as exists
        FROM information_schema.tables 
        WHERE table_name = 'g_continuous_bar_daily'
    """).fetchone()[0]
    
    if table_check == 0:
        print("Daily bars table does not exist.")
        print()
        return
    
    # Get coverage with root information
    coverage = con.execute("""
        SELECT 
            c.root,
            g.contract_series,
            COUNT(*) as bar_count,
            MIN(g.trading_date) as first_date,
            MAX(g.trading_date) as last_date,
            COUNT(DISTINCT g.trading_date) as trading_days,
            AVG(g.volume) as avg_volume,
            SUM(g.volume) as total_volume
        FROM g_continuous_bar_daily g
        JOIN dim_continuous_contract c ON g.contract_series = c.contract_series
        GROUP BY c.root, g.contract_series
        ORDER BY c.root, g.contract_series
    """).fetchdf()
    
    if len(coverage) == 0:
        print("No daily bars found in database.")
        print()
        return
    
    print(f"Total contract series: {len(coverage)}")
    print()
    print(coverage.to_string(index=False))
    print()
    
    # Show summary by root
    summary_by_root = con.execute("""
        SELECT 
            c.root,
            COUNT(DISTINCT g.contract_series) as contract_count,
            COUNT(*) as total_bars,
            MIN(g.trading_date) as first_date,
            MAX(g.trading_date) as last_date,
            COUNT(DISTINCT g.trading_date) as total_trading_days,
            SUM(g.volume) as total_volume
        FROM g_continuous_bar_daily g
        JOIN dim_continuous_contract c ON g.contract_series = c.contract_series
        GROUP BY c.root
        ORDER BY c.root
    """).fetchdf()
    
    if len(summary_by_root) > 0:
        print("=" * 80)
        print("SUMMARY BY ROOT")
        print("=" * 80)
        print(summary_by_root.to_string(index=False))
        print()


def show_continuous_root_rank_summary(con):
    """Summarize continuous daily coverage by root and rank."""
    table_check = con.execute("""
        SELECT COUNT(*) as exists
        FROM information_schema.tables 
        WHERE table_name = 'g_continuous_bar_daily'
    """).fetchone()[0]
    
    if table_check == 0:
        return
    
    summary = con.execute("""
        SELECT 
            c.root,
            COALESCE(try_cast(regexp_extract(c.contract_series, 'RANK_([0-9]+)', 1) AS INTEGER), 0) AS rank,
            COUNT(*) AS bar_count,
            COUNT(DISTINCT g.trading_date) AS trading_days,
            MIN(g.trading_date) AS first_date,
            MAX(g.trading_date) AS last_date,
            SUM(g.volume) AS total_volume
        FROM g_continuous_bar_daily g
        JOIN dim_continuous_contract c ON g.contract_series = c.contract_series
        GROUP BY 1, 2
        ORDER BY c.root, rank
    """).fetchdf()
    
    if len(summary) == 0:
        return
    
    print("=" * 80)
    print("CONTINUOUS DAILY COVERAGE BY ROOT / RANK")
    print("=" * 80)
    summary['rank'] = summary['rank'].astype(int)
    print(summary.to_string(index=False))
    print()
    
    # Check for missing ranks (especially for SR3 which needs many ranks for curve building)
    print("=" * 80)
    print("MISSING RANKS ANALYSIS")
    print("=" * 80)
    
    # Expected ranks from config (hardcoded for now, could load from config)
    expected_ranks = {
        'SR3': list(range(0, 13)),  # 0-12 for curve building
        'ES': [0, 1],
        'NQ': [0, 1],
        'RTY': [0, 1],
        'CL': [0, 1, 2, 3],
        'GC': [0, 1, 2],
        'VX': [0, 1, 2],
    }
    
    missing_ranks_found = False
    for root in sorted(summary['root'].unique()):
        root_data = summary[summary['root'] == root]
        existing_ranks = set(root_data['rank'].tolist())
        
        if root in expected_ranks:
            expected = set(expected_ranks[root])
            missing = expected - existing_ranks
            
            if missing:
                missing_ranks_found = True
                print(f"\n{root}: Missing ranks {sorted(missing)}")
                print(f"  Expected: {sorted(expected)}")
                print(f"  Found: {sorted(existing_ranks)}")
                if root == 'SR3' and len(missing) > 5:
                    print(f"  ⚠ WARNING: SR3 needs all ranks 0-12 for curve building!")
                    print(f"     Only {len(existing_ranks)} of {len(expected)} ranks present.")
                    print(f"     Re-download with: python scripts/download/download_universe_daily_ohlcv.py --roots SR3")
    
    if not missing_ranks_found:
        print("\nAll expected ranks are present for configured roots.")
    print()

    root_totals = summary.groupby('root', as_index=False).agg({
        'bar_count': 'sum',
        'trading_days': 'max',
        'first_date': 'min',
        'last_date': 'max',
        'total_volume': 'sum'
    }).rename(columns={
        'bar_count': 'total_bars',
        'trading_days': 'max_trading_days',
        'total_volume': 'total_volume_sum'
    })

    print("ROOT TOTALS")
    print(root_totals.to_string(index=False))
    print()


def show_quality_checks(con):
    """Show data quality checks."""
    print("=" * 80)
    print("DATA QUALITY CHECKS")
    print("=" * 80)
    
    quality_checks = con.execute("""
        SELECT 
            'Total Quotes' as check_name,
            COUNT(*) as count
        FROM f_fut_quote_l1
        
        UNION ALL
        
        SELECT 
            'Quotes with NULL bid' as check_name,
            COUNT(*) as count
        FROM f_fut_quote_l1
        WHERE bid_px IS NULL
        
        UNION ALL
        
        SELECT 
            'Quotes with NULL ask' as check_name,
            COUNT(*) as count
        FROM f_fut_quote_l1
        WHERE ask_px IS NULL
        
        UNION ALL
        
        SELECT 
            'Quotes with zero bid' as check_name,
            COUNT(*) as count
        FROM f_fut_quote_l1
        WHERE bid_px = 0
        
        UNION ALL
        
        SELECT 
            'Quotes with crossed market' as check_name,
            COUNT(*) as count
        FROM f_fut_quote_l1
        WHERE bid_px > ask_px
        
        UNION ALL
        
        SELECT 
            'Quotes with negative spread' as check_name,
            COUNT(*) as count
        FROM f_fut_quote_l1
        WHERE (ask_px - bid_px) < 0
    """).fetchdf()
    
    print(quality_checks.to_string(index=False))
    print()


def show_instrument_definitions(con):
    """Show instrument definitions (contract specifications) from dim_instrument_definition.
    
    Only shows instruments that have actual daily bar data in g_continuous_bar_daily.
    """
    print("=" * 80)
    print("INSTRUMENT DEFINITIONS (Contract Specifications)")
    print("=" * 80)
    
    # Check if tables exist
    def_table_check = con.execute("""
        SELECT COUNT(*) as exists
        FROM information_schema.tables 
        WHERE table_name = 'dim_instrument_definition'
    """).fetchone()[0]
    
    daily_table_check = con.execute("""
        SELECT COUNT(*) as exists
        FROM information_schema.tables 
        WHERE table_name = 'g_continuous_bar_daily'
    """).fetchone()[0]
    
    if def_table_check == 0:
        print("Instrument definitions table does not exist.")
        print()
        return
    
    if daily_table_check == 0:
        print("Daily bars table does not exist. Cannot filter by actual data.")
        print("Showing all definitions:")
        total_count = con.execute("SELECT COUNT(*) as count FROM dim_instrument_definition").fetchone()[0]
        print(f"Total instrument definitions: {total_count}")
        print()
        return
    
    # Get instruments that have actual daily bar data
    instruments_with_data = con.execute("""
        SELECT DISTINCT underlying_instrument_id
        FROM g_continuous_bar_daily
        WHERE underlying_instrument_id IS NOT NULL
    """).fetchdf()
    
    if instruments_with_data.empty:
        print("No daily bar data found. Cannot show instrument definitions with data.")
        print()
        return
    
    instrument_ids_with_data = set(instruments_with_data['underlying_instrument_id'].astype(int).unique())
    
    # Get summary by asset - ONLY for instruments with daily data
    summary = con.execute("""
        SELECT 
            d.asset,
            COUNT(*) as definition_count,
            COUNT(DISTINCT d.native_symbol) as unique_symbols
        FROM dim_instrument_definition d
        WHERE d.asset IS NOT NULL
          AND d.instrument_id IN (
              SELECT underlying_instrument_id 
              FROM g_continuous_bar_daily 
              WHERE underlying_instrument_id IS NOT NULL
          )
        GROUP BY d.asset
        ORDER BY d.asset
    """).fetchdf()
    
    # Get total counts
    total_defs = con.execute("SELECT COUNT(*) as count FROM dim_instrument_definition").fetchone()[0]
    total_with_data = con.execute("""
        SELECT COUNT(*) 
        FROM dim_instrument_definition 
        WHERE instrument_id IN (
            SELECT underlying_instrument_id 
            FROM g_continuous_bar_daily 
            WHERE underlying_instrument_id IS NOT NULL
        )
    """).fetchone()[0]
    total_without_data = total_defs - total_with_data
    
    print(f"Total instrument definitions: {total_defs}")
    print(f"  - With daily bar data: {total_with_data}")
    print(f"  - Without daily bar data: {total_without_data} (orphaned definitions)")
    print()
    
    if not summary.empty:
        print("Summary by Asset (instruments WITH daily bar data):")
        print(summary.to_string(index=False))
        print()
        
        # Show expected vs actual for major assets
        print("=" * 80)
        print("EXPECTED VS ACTUAL COUNTS (for major assets with data)")
        print("=" * 80)
        
        # Get date range to calculate expected counts
        date_range = con.execute("""
            SELECT 
                MIN(trading_date) as first_date,
                MAX(trading_date) as last_date
            FROM g_continuous_bar_daily
        """).fetchdf()
        
        if not date_range.empty and date_range['first_date'].iloc[0] is not None:
            first_date = pd.to_datetime(date_range['first_date'].iloc[0])
            last_date = pd.to_datetime(date_range['last_date'].iloc[0])
            years = (last_date - first_date).days / 365.25
            
            print(f"Data date range: {first_date.date()} to {last_date.date()} (~{years:.1f} years)")
            print()
            
            # Expected counts for quarterly (4 per year) and monthly (12 per year)
            expected_quarterly = int(years * 4)
            expected_monthly = int(years * 12)
            
            # Check major assets
            major_assets = ['ES', 'NQ', 'RTY', 'SI', 'GC', 'CL', 'ZT', 'ZF', 'ZN', 'UB']
            comparison = []
            
            for asset in major_assets:
                asset_data = summary[summary['asset'] == asset]
                if not asset_data.empty:
                    actual = asset_data['definition_count'].iloc[0]
                    # Most equity/commodity futures are quarterly, some are monthly
                    # ES, NQ, RTY are quarterly
                    # SI, GC, CL are typically monthly
                    if asset in ['ES', 'NQ', 'RTY']:
                        expected = expected_quarterly
                        freq = "quarterly"
                    elif asset in ['SI', 'GC', 'CL']:
                        expected = expected_monthly
                        freq = "monthly"
                    else:
                        expected = None
                        freq = "unknown"
                    
                    comparison.append({
                        'asset': asset,
                        'actual': actual,
                        'expected': expected if expected else 'N/A',
                        'frequency': freq,
                        'difference': actual - expected if expected else None
                    })
            
            if comparison:
                comp_df = pd.DataFrame(comparison)
                print(comp_df.to_string(index=False))
                print()
                
                # Warn about significant discrepancies
                for row in comparison:
                    if row['expected'] and row['difference']:
                        diff_pct = (row['difference'] / row['expected']) * 100
                        if abs(diff_pct) > 20:  # More than 20% difference
                            print(f"WARNING: {row['asset']} has {row['actual']} contracts (expected ~{row['expected']} for {row['frequency']})")
                            print(f"         Difference: {row['difference']:+d} ({diff_pct:+.1f}%)")
                print()
    
    # Get first 5 and last 5 definitions (only those with data)
    all_defs = con.execute("""
        SELECT 
            d.instrument_id,
            d.native_symbol,
            d.asset,
            d.min_price_increment,
            d.min_price_increment_amount,
            d.expiration,
            d.maturity_year,
            d.maturity_month,
            d.maturity_day,
            d.contract_multiplier,
            d.currency,
            d.definition_date
        FROM dim_instrument_definition d
        WHERE d.instrument_id IN (
            SELECT underlying_instrument_id 
            FROM g_continuous_bar_daily 
            WHERE underlying_instrument_id IS NOT NULL
        )
        ORDER BY d.instrument_id
    """).fetchdf()
    
    if len(all_defs) > 0:
        print("First 5 definitions:")
        print(all_defs.head(5).to_string(index=False))
        print()
        
        if len(all_defs) > 5:
            print("Last 5 definitions:")
            print(all_defs.tail(5).to_string(index=False))
            print()
        
        # Show sample by asset (if we have multiple assets)
        assets = all_defs['asset'].dropna().unique()
        if len(assets) > 1:
            print("Sample definitions by asset:")
            for asset in sorted(assets)[:5]:  # Show up to 5 assets
                asset_defs = all_defs[all_defs['asset'] == asset].head(2)
                if not asset_defs.empty:
                    print(f"\n{asset} (showing {len(asset_defs)} of {len(all_defs[all_defs['asset'] == asset])}):")
                    print(asset_defs[['instrument_id', 'native_symbol', 'expiration', 'maturity_year', 'maturity_month']].to_string(index=False))
            print()
    
    # Show orphaned definitions (definitions without data)
    if total_without_data > 0:
        print("=" * 80)
        print("ORPHANED DEFINITIONS (definitions without daily bar data)")
        print("=" * 80)
        
        orphaned = con.execute("""
            SELECT 
                d.asset,
                COUNT(*) as count
            FROM dim_instrument_definition d
            WHERE d.instrument_id NOT IN (
                SELECT underlying_instrument_id 
                FROM g_continuous_bar_daily 
                WHERE underlying_instrument_id IS NOT NULL
            )
            AND d.asset IS NOT NULL
            GROUP BY d.asset
            ORDER BY count DESC
            LIMIT 20
        """).fetchdf()
        
        if not orphaned.empty:
            print(f"Top assets with orphaned definitions (showing top 20):")
            print(orphaned.to_string(index=False))
            print()
            print(f"Total orphaned definitions: {total_without_data}")
            print("(These are definitions that exist but have no daily bar data)")
        print()
    
    # Check for specific assets with data
    asset_checks = con.execute("""
        SELECT 
            d.asset,
            COUNT(*) as count
        FROM dim_instrument_definition d
        WHERE d.asset IN ('SR3', 'SI', 'GC', 'CL', 'ES', 'NQ')
          AND d.instrument_id IN (
              SELECT underlying_instrument_id 
              FROM g_continuous_bar_daily 
              WHERE underlying_instrument_id IS NOT NULL
          )
        GROUP BY d.asset
        ORDER BY d.asset
    """).fetchdf()
    
    if not asset_checks.empty:
        print("Major assets with definitions AND daily data:")
        print(asset_checks.to_string(index=False))
        print()
        
        # Check for silver specifically
        si_check = asset_checks[asset_checks['asset'] == 'SI']
        if not si_check.empty:
            print(f"Found {si_check['count'].iloc[0]} SI (Silver) instrument definitions with daily data")
        else:
            print("Note: No SI (Silver) instrument definitions with daily data found")
            print("   (SI definitions may exist but no daily bar data has been ingested)")
    else:
        print("Note: No instrument definitions with daily data found for common assets (SR3, SI, GC, CL, ES, NQ)")
        print("   (This is expected if data hasn't been downloaded/ingested yet)")
    print()


def show_fred_summary(con):
    """Show summary of FRED series from database."""
    print("=" * 80)
    print("FRED SERIES COVERAGE (from database)")
    print("=" * 80)
    
    # Check if FRED tables exist
    table_check = con.execute("""
        SELECT COUNT(*) as exists
        FROM information_schema.tables 
        WHERE table_name = 'f_fred_observations'
    """).fetchone()[0]
    
    if table_check == 0:
        print("FRED tables do not exist in database.")
        print("Run: python scripts/database/ingest_fred_series.py")
        print()
        return
    
    # Get summary from database
    summary = con.execute("""
        SELECT 
            series_id,
            COUNT(*) as rows,
            MIN(date) as first_date,
            MAX(date) as last_date
        FROM f_fred_observations
        GROUP BY series_id
        ORDER BY series_id
    """).fetchdf()
    
    if summary.empty:
        print("No FRED series found in database.")
        print("Run: python scripts/database/ingest_fred_series.py")
        print()
        return
    
    print(f"Total series: {len(summary)}")
    print()
    print(summary.to_string(index=False))
    print()
    
    # Show sample data for a few series
    sample_series = summary.head(3)['series_id'].tolist()
    if sample_series:
        print("Sample data (latest 3 observations per series):")
        for series_id in sample_series:
            sample = con.execute("""
                SELECT date, value
                FROM f_fred_observations
                WHERE series_id = ?
                ORDER BY date DESC
                LIMIT 3
            """, [series_id]).fetchdf()
            if not sample.empty:
                print(f"\n{series_id}:")
                print(sample.to_string(index=False))
        print()


def export_sample_data(con, output_dir: Path):
    """Export sample data to CSV."""
    print("=" * 80)
    print("EXPORTING SAMPLE DATA")
    print("=" * 80)
    
    sample_data = con.execute("""
        SELECT 
            i.symbol_canonical,
            q.ts_event,
            q.ts_rcv,
            q.bid_px as bid_price,
            q.ask_px as ask_price,
            q.bid_sz as bid_size,
            q.ask_sz as ask_size,
            (q.ask_px - q.bid_px) as spread,
            ((q.bid_px + q.ask_px) / 2.0) as mid_price
        FROM f_fut_quote_l1 q
        JOIN dim_fut_instrument i ON q.instrument_id = i.instrument_id
        ORDER BY q.ts_event
    """).fetchdf()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"futures_sample_{timestamp}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sample_data.to_csv(output_path, index=False)
    
    print(f"Exported {len(sample_data)} quotes to: {output_path}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Inspect ES futures data in DuckDB database"
    )
    parser.add_argument(
        '--contract',
        type=str,
        default='ESZ5',
        help='Contract symbol to inspect in detail (default: ESZ5)'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Export sample data to CSV'
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default='data/silver/market.duckdb',
        help='Path to DuckDB database (default: data/silver/market.duckdb)'
    )
    parser.add_argument(
        '--max-series',
        type=int,
        default=5,
        help='Maximum number of continuous contract series samples to display (default: 5)'
    )
    parser.add_argument(
        '--root',
        type=str,
        help='Filter by root symbol (e.g., ES, SI, NQ, GC, etc.)'
    )
    
    args = parser.parse_args()
    
    # Setup
    setup_display()
    
    # Connect to database
    db_path = Path(args.db_path)
    con = connect_to_db(db_path)
    if con is None:
        return 1
    
    try:
        # Run all inspections
        list_tables(con)
        show_summary(con)
        
        # Futures detailed inspection (all roots or filtered)
        print("=" * 80)
        print("FUTURES DETAILED INSPECTION" + (f" (Root: {args.root})" if args.root else ""))
        print("=" * 80)
        print()
        show_instruments(con, root_filter=args.root)
        show_quote_coverage(con)
        show_quotes_per_contract(con)
        show_daily_quotes(con)
        show_bars_per_contract(con)
        show_contract_bars(con, args.contract)
        show_spread_analysis(con)
        show_quality_checks(con)
        
        # Show continuous daily bars if they exist
        show_continuous_daily_coverage(con)
        show_continuous_root_rank_summary(con)
        
        # Show instrument definitions (contract specifications)
        show_instrument_definitions(con)
        
        # Show bars for each contract series found
        table_check = con.execute("""
            SELECT COUNT(*) as exists
            FROM information_schema.tables 
            WHERE table_name = 'g_continuous_bar_daily'
        """).fetchone()[0]
        
        if table_check > 0:
            contract_series_list = con.execute("""
                SELECT DISTINCT contract_series
                FROM g_continuous_bar_daily
                ORDER BY contract_series
            """).fetchdf()
            
            if len(contract_series_list) > 0 and args.max_series != 0:
                series_values = contract_series_list['contract_series'].tolist()
                sample_series = series_values if args.max_series < 0 else series_values[:args.max_series]
                if len(series_values) > len(sample_series):
                    print(f"Displaying first {len(sample_series)} contract series (of {len(series_values)} total).")
                    print()
                for contract_series in sample_series:
                    show_continuous_daily_bars(con, contract_series)
        
        # Export if requested
        if args.export:
            output_dir = Path("data/gold")
            export_sample_data(con, output_dir)
        
        # FRED series summary (from database)
        show_fred_summary(con)
        
        print("=" * 80)
        print("INSPECTION COMPLETE")
        print("=" * 80)
        print()
        
    finally:
        con.close()
        print("Database connection closed.")
    
    return 0


if __name__ == "__main__":
    exit(main())

