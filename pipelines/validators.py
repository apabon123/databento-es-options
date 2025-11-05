from .common import connect_duckdb, get_paths


def validate_options(con):
    checks = [
        ("Negative spreads", "select count(*) from f_quote_l1 where ask_px < bid_px"),
        ("Unlinked instruments (quotes)", "select count(*) from f_quote_l1 q left join dim_instrument d using(instrument_id) where d.instrument_id is null"),
        ("Unlinked instruments (trades)", "select count(*) from f_trade t left join dim_instrument d using(instrument_id) where d.instrument_id is null"),
    ]
    return [(name, con.execute(sql).fetchone()[0]) for name, sql in checks]


def validate_futures(con):
    checks = [
        ("Unlinked instruments (quotes)", "select count(*) from f_fut_quote_l1 q left join dim_fut_instrument d using(instrument_id) where d.instrument_id is null"),
        ("Unlinked instruments (trades)", "select count(*) from f_fut_trade t left join dim_fut_instrument d using(instrument_id) where d.instrument_id is null"),
    ]
    return [(name, con.execute(sql).fetchone()[0]) for name, sql in checks]


