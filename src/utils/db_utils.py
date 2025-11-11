"""
Database utilities for checking existing data and preventing duplicates.
"""
from pathlib import Path
from datetime import date, datetime
from typing import Set, List
import pandas as pd
import duckdb


def get_existing_dates_in_db(product: str) -> Set[date]:
    """
    Query database to find what dates already have data.
    
    Args:
        product: Product code such as 'ES_OPTIONS_MDP3', 'ES_FUTURES_MDP3',
            'ES_CONTINUOUS_MDP3', or 'ES_CONTINUOUS_DAILY_MDP3'.
    
    Returns:
        Set of dates that already have data in the database
    """
    from pipelines.common import get_paths, connect_duckdb
    
    _, _, dbpath = get_paths()
    if not dbpath.exists():
        return set()
    
    con = connect_duckdb(dbpath)
    
    # Determine which table to query based on product
    if product == "ES_OPTIONS_MDP3":
        table = "f_quote_l1"
        date_column = "ts_event"
    elif product == "ES_FUTURES_MDP3":
        table = "f_fut_quote_l1"
        date_column = "ts_event"
    elif product == "ES_CONTINUOUS_MDP3":
        table = "f_continuous_quote_l1"
        date_column = "ts_event"
    elif product == "ES_CONTINUOUS_DAILY_MDP3":
        table = "g_continuous_bar_daily"
        date_column = "trading_date"
    else:
        raise ValueError(f"Unknown product: {product}")
    
    try:
        # Get unique dates from the relevant table
        query = f"""
        SELECT DISTINCT CAST({date_column} AS DATE) as trade_date
        FROM {table}
        ORDER BY trade_date
        """
        result = con.execute(query).fetchdf()
        
        if result.empty:
            return set()
        
        # Convert to Python date objects
        dates = set(pd.to_datetime(result['trade_date']).dt.date)
        return dates
    except Exception:
        # Table might not exist yet
        return set()
    finally:
        con.close()


def get_existing_daily_dates_for_series(contract_series: str) -> Set[date]:
    """Return the set of trading dates present for a given continuous contract series."""
    from pipelines.common import get_paths, connect_duckdb

    _, _, dbpath = get_paths()
    if not dbpath.exists():
        return set()

    con = connect_duckdb(dbpath)

    try:
        result = con.execute(
            """
            SELECT DISTINCT trading_date
            FROM g_continuous_bar_daily
            WHERE contract_series = ?
            ORDER BY trading_date
            """,
            [contract_series],
        ).fetchdf()

        if result.empty:
            return set()

        return set(pd.to_datetime(result['trading_date']).dt.date)
    except Exception:
        return set()
    finally:
        con.close()


def get_dates_from_raw_folder() -> Set[date]:
    """
    Scan data/raw folder to find what dates have been downloaded.
    
    Returns:
        Set of dates that have parquet files in data/raw
    """
    from pipelines.common import get_paths
    
    bronze, _, _ = get_paths()
    
    if not bronze.exists():
        return set()
    
    dates = set()
    
    # Look for parquet files matching pattern: glbx-mdp3-YYYY-MM-DD.*.parquet
    for parquet_file in bronze.glob("glbx-mdp3-*.parquet"):
        # Extract date from filename
        parts = parquet_file.stem.split('-')
        if len(parts) >= 4:
            try:
                year = int(parts[2])
                month = int(parts[3])
                day = int(parts[4].split('.')[0])
                dates.add(date(year, month, day))
            except (ValueError, IndexError):
                continue
    
    return dates


def check_for_missing_ingestions() -> List[date]:
    """
    Find dates that have been downloaded but not yet ingested into database.
    
    Returns:
        List of dates that need to be ingested
    """
    raw_dates = get_dates_from_raw_folder()
    db_dates_options = get_existing_dates_in_db("ES_OPTIONS_MDP3")
    db_dates_futures = get_existing_dates_in_db("ES_FUTURES_MDP3")
    
    # Union of all DB dates
    db_dates = db_dates_options | db_dates_futures
    
    # Dates in raw but not in DB
    missing = raw_dates - db_dates
    
    return sorted(list(missing))


def filter_new_dates(requested_dates: List[date], product: str) -> List[date]:
    """
    Filter out dates that already exist in database.
    
    Args:
        requested_dates: List of dates user wants to download
        product: 'ES_OPTIONS_MDP3' or 'ES_FUTURES_MDP3'
    
    Returns:
        List of dates that are NOT yet in the database
    """
    existing = get_existing_dates_in_db(product)
    
    new_dates = [d for d in requested_dates if d not in existing]
    
    return new_dates


def get_db_summary(product: str) -> dict:
    """
    Get summary statistics about data in database.
    
    Args:
        product: 'ES_OPTIONS_MDP3' or 'ES_FUTURES_MDP3'
    
    Returns:
        Dictionary with summary statistics
    """
    from pipelines.common import get_paths, connect_duckdb
    
    _, _, dbpath = get_paths()
    if not dbpath.exists():
        return {
            'status': 'no_database',
            'message': 'Database does not exist yet'
        }
    
    con = connect_duckdb(dbpath)
    
    if product == "ES_OPTIONS_MDP3":
        quote_table = "f_quote_l1"
        inst_table = "dim_instrument"
        bar_table = "g_bar_1m"
    elif product == "ES_FUTURES_MDP3":
        quote_table = "f_fut_quote_l1"
        inst_table = "dim_fut_instrument"
        bar_table = "g_fut_bar_1m"
    else:
        raise ValueError(f"Unknown product: {product}")
    
    try:
        # Get quote statistics
        quote_stats = con.execute(f"""
        SELECT 
            COUNT(*) as total_quotes,
            COUNT(DISTINCT instrument_id) as unique_instruments,
            MIN(CAST(ts_event AS DATE)) as min_date,
            MAX(CAST(ts_event AS DATE)) as max_date,
            COUNT(DISTINCT CAST(ts_event AS DATE)) as trading_days
        FROM {quote_table}
        """).fetchone()
        
        # Get instrument count
        inst_count = con.execute(f"SELECT COUNT(*) FROM {inst_table}").fetchone()[0]
        
        # Get bar count
        try:
            bar_count = con.execute(f"SELECT COUNT(*) FROM {bar_table}").fetchone()[0]
        except:
            bar_count = 0
        
        return {
            'status': 'ok',
            'total_quotes': quote_stats[0],
            'unique_instruments': quote_stats[1],
            'min_date': quote_stats[2],
            'max_date': quote_stats[3],
            'trading_days': quote_stats[4],
            'instrument_count': inst_count,
            'bar_count': bar_count
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }
    finally:
        con.close()

