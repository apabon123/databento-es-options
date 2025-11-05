from pathlib import Path


def _pattern(base: Path, pat: str):
    return str((base / pat).as_posix())


def _has_files(base: Path, pat: str) -> bool:
    return any(base.glob(pat))


def load(con, source_dir: Path, date, product_cfg: dict):
    # Instruments (optional but preferred)
    inst_pat = product_cfg["inputs"].get("instruments")
    if inst_pat and _has_files(source_dir, inst_pat):
        con.execute("""
        insert or replace into dim_instrument
        select
          instrument_id::BIGINT,
          root,
          cast(expiry as date) as expiry,
          strike::DOUBLE,
          put_call,
          coalesce(exerc_style, 'American') as exerc_style,
          coalesce(multiplier, 50)::INT as multiplier,
          coalesce(tick_size, 0.25)::DOUBLE as tick_size,
          symbol_feed,
          coalesce(symbol_canonical, symbol_feed) as symbol_canonical
        from read_parquet(?)
        """, [_pattern(source_dir, inst_pat)])

    # Quotes
    q_pat = product_cfg["inputs"].get("quotes_l1")
    if q_pat and _has_files(source_dir, q_pat):
        con.execute("""
        insert or ignore into f_quote_l1
        select
          ts_event::TIMESTAMP,
          ts_rcv::TIMESTAMP,
          instrument_id::BIGINT,
          bid_px::DOUBLE, bid_sz::DOUBLE,
          ask_px::DOUBLE, ask_sz::DOUBLE
        from read_parquet(?)
        """, [_pattern(source_dir, q_pat)])

    # Trades
    t_pat = product_cfg["inputs"].get("trades")
    if t_pat and _has_files(source_dir, t_pat):
        con.execute("""
        insert into f_trade
        select
          ts_event::TIMESTAMP,
          ts_rcv::TIMESTAMP,
          instrument_id::BIGINT,
          last_px::DOUBLE, last_sz::DOUBLE,
          aggressor
        from read_parquet(?)
        """, [_pattern(source_dir, t_pat)])

    return True


