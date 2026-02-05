"""
Trading calendar utilities derived from actual data.

IMPORTANT: This module derives the trading calendar from actual ingested data in
g_continuous_bar_daily. It does NOT assume equity-style Mon-Fri trading or CME
holiday schedules. Futures trade ~314 days/year including many Sundays.

The source of truth is whatever days have data in the database.
"""

from datetime import date
from typing import List


def get_trading_days_from_data(con, start: date, end: date) -> List[date]:
    """
    Return list of trading dates in [start, end] from actual g_continuous_bar_daily data.
    
    This derives the calendar from actual ingested data rather than assuming
    any particular trading schedule.
    
    Args:
        con: DuckDB connection
        start: Start date (inclusive)
        end: End date (inclusive)
        
    Returns:
        List of dates that have data in g_continuous_bar_daily, sorted ascending
    """
    rows = con.execute(
        """
        SELECT DISTINCT trading_date
        FROM g_continuous_bar_daily
        WHERE trading_date >= ? AND trading_date <= ?
        ORDER BY trading_date
        """,
        [start.isoformat(), end.isoformat()],
    ).fetchall()
    return [r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])) for r in rows]


def sync_dim_session_from_data(con, dry_run: bool = False) -> int:
    """
    Populate dim_session from actual trading_dates in g_continuous_bar_daily.
    
    This derives the trading calendar from actual data rather than assuming
    any particular schedule. is_holiday is set to FALSE for all rows since
    we only know which days have data, not which days are "holidays".
    
    Args:
        con: DuckDB connection
        dry_run: If True, don't insert, just return count of what would be inserted
        
    Returns:
        Count of rows inserted (or would be inserted if dry_run=True)
    """
    # Count how many new rows would be inserted
    count_query = """
        SELECT COUNT(DISTINCT trading_date) as cnt
        FROM g_continuous_bar_daily
        WHERE trading_date NOT IN (SELECT trade_date FROM dim_session)
    """
    count_result = con.execute(count_query).fetchone()[0]
    
    if dry_run:
        return count_result
    
    if count_result == 0:
        return 0
    
    # Insert new trading dates from g_continuous_bar_daily
    # week = ISO week number, month = month number, quarter = quarter number
    # is_holiday = FALSE (we don't know which days are holidays, only which days have data)
    con.execute(
        """
        INSERT OR IGNORE INTO dim_session (trade_date, week, month, quarter, is_holiday)
        SELECT DISTINCT 
            trading_date AS trade_date,
            EXTRACT(WEEK FROM trading_date)::INT AS week,
            EXTRACT(MONTH FROM trading_date)::INT AS month,
            EXTRACT(QUARTER FROM trading_date)::INT AS quarter,
            FALSE AS is_holiday
        FROM g_continuous_bar_daily
        WHERE trading_date NOT IN (SELECT trade_date FROM dim_session)
        """
    )
    
    return count_result


def get_dim_session_count(con) -> int:
    """
    Return the number of rows in dim_session.
    
    Args:
        con: DuckDB connection
        
    Returns:
        Count of rows in dim_session
    """
    result = con.execute("SELECT COUNT(*) FROM dim_session").fetchone()[0]
    return result


def get_trading_days_from_dim_session(con, start: date, end: date) -> List[date]:
    """
    Return list of trading dates in [start, end] from dim_session.
    
    Args:
        con: DuckDB connection
        start: Start date (inclusive)
        end: End date (inclusive)
        
    Returns:
        List of dates from dim_session, sorted ascending
    """
    rows = con.execute(
        """
        SELECT trade_date
        FROM dim_session
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
        """,
        [start.isoformat(), end.isoformat()],
    ).fetchall()
    return [r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])) for r in rows]
