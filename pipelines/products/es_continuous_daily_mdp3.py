from pathlib import Path


def _pattern(base: Path, pat: str):
    return str((base / pat).as_posix())


def _has_files(base: Path, pat: str) -> bool:
    return any(base.glob(pat))


def load(con, source_dir: Path, date, product_cfg: dict):
    """Load continuous futures daily OHLCV data into DuckDB."""
    
    # Load continuous contract definitions (shared with 1-minute data)
    inst_pat = product_cfg["inputs"].get("instruments")
    if inst_pat and _has_files(source_dir, inst_pat):
        con.execute("""
        insert or replace into dim_continuous_contract
        select
          contract_series,
          root,
          roll_rule,
          adjustment_method,
          description
        from read_parquet(?)
        """, [_pattern(source_dir, inst_pat)])

    # Load daily bars
    bars_pat = product_cfg["inputs"].get("bars_daily")
    if bars_pat and _has_files(source_dir, bars_pat):
        con.execute("""
        insert or replace into g_continuous_bar_daily
        select
          trading_date::DATE,
          contract_series,
          underlying_instrument_id::BIGINT,
          open::DOUBLE,
          high::DOUBLE,
          low::DOUBLE,
          close::DOUBLE,
          volume::BIGINT
        from read_parquet(?)
        """, [_pattern(source_dir, bars_pat)])

    return True

