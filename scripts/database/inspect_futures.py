"""
ES Futures Data Inspection Script

This script inspects ES futures BBO-1m data stored in the DuckDB database.

Usage:
    python scripts/database/inspect_futures.py
    python scripts/database/inspect_futures.py --contract ESH6
    python scripts/database/inspect_futures.py --export
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import argparse
from datetime import datetime


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


def show_instruments(con):
    """Show all ES futures instruments."""
    print("=" * 80)
    print("INSTRUMENT DEFINITIONS")
    print("=" * 80)
    
    instruments = con.execute("""
        SELECT 
            instrument_id,
            root,
            expiry,
            symbol_feed,
            symbol_canonical,
            tick_size,
            multiplier
        FROM dim_fut_instrument
        ORDER BY symbol_canonical
    """).fetchdf()
    
    print(f"Total instruments: {len(instruments)}")
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
    print("First 10 bars:")
    print(contract_bars.head(10).to_string(index=False))
    print()
    print("Last 10 bars:")
    print(contract_bars.tail(10).to_string(index=False))
    print()
    
    # Summary statistics
    print("Summary Statistics:")
    print(contract_bars[['mid_open', 'mid_high', 'mid_low', 'mid_close']].describe().to_string())
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


def show_continuous_daily_bars(con, contract_series='ES_FRONT_MONTH'):
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
    print("First 10 bars:")
    print(daily_bars.head(10).to_string(index=False))
    print()
    print("Last 10 bars:")
    print(daily_bars.tail(10).to_string(index=False))
    print()
    
    # Summary statistics
    print("Summary Statistics:")
    print(daily_bars[['open', 'high', 'low', 'close', 'volume']].describe().to_string())
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
        show_instruments(con)
        show_quote_coverage(con)
        show_quotes_per_contract(con)
        show_daily_quotes(con)
        show_bars_per_contract(con)
        show_contract_bars(con, args.contract)
        show_spread_analysis(con)
        show_quality_checks(con)
        
        # Show continuous daily bars if they exist
        show_continuous_daily_coverage(con)
        
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
            
            if len(contract_series_list) > 0:
                for _, row in contract_series_list.iterrows():
                    contract_series = row['contract_series']
                    show_continuous_daily_bars(con, contract_series)
        
        # Export if requested
        if args.export:
            output_dir = Path("data/gold")
            export_sample_data(con, output_dir)
        
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

