from pathlib import Path
from typing import Optional
from .registry import get_loader_callable, get_product
from .common import connect_duckdb, get_paths


def load(product_code: str, source_dir: Path, date: Optional[str] = None):
    loader, prod = get_loader_callable(product_code)
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    return loader(con, Path(source_dir), date, prod)


def apply_gold_sql(product_code: str):
    from .common import connect_duckdb, get_paths
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    sql = get_product(product_code).get("gold_sql")
    if sql:
        con.execute(sql)


