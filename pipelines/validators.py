from typing import List, Tuple

from .common import connect_duckdb, get_paths


def validate_options(con) -> List[Tuple[str, int]]:
    checks = [
        ("Negative spreads", "select count(*) from f_quote_l1 where ask_px < bid_px"),
        ("Unlinked instruments (quotes)", "select count(*) from f_quote_l1 q left join dim_instrument d using(instrument_id) where d.instrument_id is null"),
        ("Unlinked instruments (trades)", "select count(*) from f_trade t left join dim_instrument d using(instrument_id) where d.instrument_id is null"),
    ]
    return [(name, con.execute(sql).fetchone()[0]) for name, sql in checks]


def validate_futures(con) -> List[Tuple[str, int]]:
    checks = [
        ("Unlinked instruments (quotes)", "select count(*) from f_fut_quote_l1 q left join dim_fut_instrument d using(instrument_id) where d.instrument_id is null"),
        ("Unlinked instruments (trades)", "select count(*) from f_fut_trade t left join dim_fut_instrument d using(instrument_id) where d.instrument_id is null"),
    ]
    return [(name, con.execute(sql).fetchone()[0]) for name, sql in checks]


def validate_continuous_daily(con) -> List[Tuple[str, int]]:
    """
    Validate g_continuous_bar_daily data quality.
    
    Checks (data quality only, no calendar assumptions):
      a. Duplicate (trading_date, contract_series) pairs
      b. Negative volume values
      c. Non-CL: zero or negative OHLC prices; CL: only NULL/absurd OHLC (sign excluded)
      d. NULL in required columns (trading_date, contract_series, close)
      e. OHLC sanity: high >= low, high >= open, high >= close, low <= open, low <= close
    
    Args:
        con: DuckDB connection
    
    Returns:
        List of tuples (check_name, violation_count)
    """
    checks = [
        # a. Duplicate (trading_date, contract_series) pairs
        (
            "Duplicate (trading_date, contract_series) pairs",
            """
            SELECT COUNT(*) FROM (
                SELECT trading_date, contract_series, COUNT(*) as cnt
                FROM g_continuous_bar_daily
                GROUP BY trading_date, contract_series
                HAVING COUNT(*) > 1
            )
            """
        ),
        # b. Negative volume values
        (
            "Negative volume values",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE volume < 0"
        ),
        # c. Non-CL: zero or negative OHLC; CL: only null/absurd (sign excluded)
        (
            "Non-CL: zero or negative OHLC prices",
            """
            SELECT COUNT(*)
            FROM g_continuous_bar_daily
            WHERE split_part(contract_series, '_', 1) != 'CL'
              AND (open <= 0 OR high <= 0 OR low <= 0 OR close <= 0)
            """
        ),
        (
            "CL: NULL in OHLC",
            """
            SELECT COUNT(*)
            FROM g_continuous_bar_daily
            WHERE split_part(contract_series, '_', 1) = 'CL'
              AND (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL)
            """
        ),
        # d. NULL in required columns
        (
            "NULL trading_date",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE trading_date IS NULL"
        ),
        (
            "NULL contract_series",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE contract_series IS NULL"
        ),
        (
            "NULL close",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE close IS NULL"
        ),
        # e. OHLC sanity checks
        (
            "High < Low",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE high < low"
        ),
        (
            "High < Open",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE high < open"
        ),
        (
            "High < Close",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE high < close"
        ),
        (
            "Low > Open",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE low > open"
        ),
        (
            "Low > Close",
            "SELECT COUNT(*) FROM g_continuous_bar_daily WHERE low > close"
        ),
    ]
    return [(name, con.execute(sql).fetchone()[0]) for name, sql in checks]


