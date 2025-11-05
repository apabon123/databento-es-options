import typer
from pathlib import Path
from pipelines.common import get_paths, connect_duckdb
from pipelines.loader import load as load_product, apply_gold_sql
from pipelines.validators import validate_options, validate_futures


app = typer.Typer(help="Market DB Orchestrator (DuckDB + Parquet)")


@app.command()
def migrate():
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    con.execute("""
        create table if not exists _migrations (
          id varchar primary key,
          applied_at timestamp default current_timestamp
        );
    """)
    mig_dir = Path("db/migrations")
    for sql_file in sorted(mig_dir.glob("*.sql")):
        mig_id = sql_file.name
        exists = con.execute("select 1 from _migrations where id = ?", [mig_id]).fetchone()
        if exists: 
            continue
        con.execute(sql_file.read_text())
        con.execute("insert into _migrations(id) values (?)", [mig_id])
    print("Migrations applied.")


@app.command()
def ingest(product: str = typer.Option(..., "--product"),
           source: Path = typer.Option(..., "--source"),
           date: str = typer.Option(None, "--date")):
    migrate()
    load_product(product, source, date)
    print(f"Ingest complete for {product} from {source}")


@app.command()
def build(product: str = typer.Option(..., "--product")):
    apply_gold_sql(product)
    print(f"Gold build complete for {product}")


@app.command()
def validate(product: str = typer.Option(..., "--product")):
    _, _, dbpath = get_paths()
    con = connect_duckdb(dbpath)
    if product == "ES_OPTIONS_MDP3":
        results = validate_options(con)
    elif product == "ES_FUTURES_MDP3":
        results = validate_futures(con)
    else:
        results = []
    for name, cnt in results:
        print(f"[{name}] -> {cnt}")
    print("Validation complete.")


if __name__ == "__main__":
    app()


