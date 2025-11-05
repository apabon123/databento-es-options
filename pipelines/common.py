import os
from dotenv import load_dotenv
import yaml
import duckdb
from pathlib import Path


load_dotenv()


def get_paths():
    bronze = Path(os.getenv("DATA_BRONZE_ROOT", "./data/raw")).resolve()
    gold   = Path(os.getenv("DATA_GOLD_ROOT", "./data/gold")).resolve()
    dbpath = Path(os.getenv("DUCKDB_PATH", "./data/silver/market.duckdb")).resolve()
    dbpath.parent.mkdir(parents=True, exist_ok=True)
    return bronze, gold, dbpath


def connect_duckdb(dbpath: Path):
    return duckdb.connect(str(dbpath))


def load_registry():
    with open("config/schema_registry.yml","r") as f:
        return yaml.safe_load(f)


