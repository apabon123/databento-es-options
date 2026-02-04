"""Check if SR3 data exists in raw files."""
import os
from pathlib import Path
import pandas as pd

bronze = Path(os.getenv('DATA_BRONZE_ROOT', 'C:/Users/alexp/OneDrive/Gdrive/Trading/Data Downloads/DataBento/raw'))

# Check raw downloads
sr3_download_path = bronze / 'ohlcv-1d' / 'downloads' / 'calendar' / 'SR3'
print(f"SR3 download path: {sr3_download_path}")
print(f"Exists: {sr3_download_path.exists()}")

if sr3_download_path.exists():
    files = list(sr3_download_path.glob('*.parquet'))
    print(f"Found {len(files)} SR3 download files")
    if files:
        print(f"Sample files: {[f.name for f in files[:5]]}")
        # Check one file
        df = pd.read_parquet(files[0])
        print(f"Sample file columns: {list(df.columns)}")
        print(f"Sample file symbols: {df['symbol'].unique()[:10] if 'symbol' in df.columns else 'No symbol column'}")

# Check transformed data
sr3_transformed_path = bronze / 'ohlcv-1d' / 'transformed' / 'calendar' / 'SR3'
print(f"\nSR3 transformed path: {sr3_transformed_path}")
print(f"Exists: {sr3_transformed_path.exists()}")

if sr3_transformed_path.exists():
    # Look for rank directories
    rank_dirs = [d for d in sr3_transformed_path.iterdir() if d.is_dir() and d.name.startswith('rank=')]
    print(f"Found {len(rank_dirs)} rank directories")
    for rank_dir in rank_dirs[:5]:
        date_dirs = [d for d in rank_dir.iterdir() if d.is_dir()]
        print(f"  {rank_dir.name}: {len(date_dirs)} date directories")


