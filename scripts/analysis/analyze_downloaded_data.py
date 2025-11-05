"""
Analyze downloaded ES options BBO data for quality and completeness.

Checks:
1. Symbol counts per day
2. Unique symbols per minute snapshot
3. Strike coverage (how many strikes per expiry)
4. Expiry/maturity distribution
5. Data quality (bid/ask spreads, missing prices, outliers)
6. Time coverage (do we have all 5 minutes?)
"""
from pathlib import Path
from datetime import datetime, timedelta
import sys
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from utils.logging_config import get_logger

logger = get_logger(__name__)

try:
    from zoneinfo import ZoneInfo
    CHI = ZoneInfo("America/Chicago")
    UTC = ZoneInfo("UTC")
except:
    import pytz
    CHI = pytz.timezone("America/Chicago")
    UTC = pytz.UTC


def parse_es_option_symbol(symbol: str) -> dict:
    """
    Parse ES option symbol into components.
    Format: "ESZ5 C6000" = ES December 2025 Call at 6000 strike
    Returns: {'root': 'ES', 'month': 'Z', 'year': '5', 'type': 'C', 'strike': 6000, 'expiry_code': 'ESZ5'}
    """
    import re
    # Pattern: ROOT + MONTH + YEAR + SPACE + C/P + STRIKE
    match = re.match(r'^([A-Z]+)([A-Z])(\d)\s+([CP])(\d+)$', symbol)
    if not match:
        return None
    
    root, month, year, opt_type, strike = match.groups()
    return {
        'root': root,
        'month': month,
        'year': year,
        'type': opt_type,
        'strike': int(strike),
        'expiry_code': f"{root}{month}{year}",
        'full_symbol': symbol,
    }


def load_file(file_path: Path) -> pd.DataFrame:
    """Load either DBN or parquet file."""
    if file_path.suffix in ['.dbn', '.zst']:
        # Load DBN file
        import databento as db
        store = db.DBNStore.from_file(str(file_path))
        return store.to_df()
    elif file_path.suffix == '.parquet':
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")


def analyze_file(file_path: Path) -> dict:
    """Analyze a single DBN or parquet file and return summary statistics."""
    logger.info(f"Analyzing {file_path.name}...")
    
    df = load_file(file_path)
    
    if df.empty:
        return {'error': 'Empty file', 'file': file_path.name}
    
    # Parse timestamps
    for col in ['ts_event', 'ts_recv']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
    
    # Parse option symbols
    df['parsed'] = df['symbol'].apply(parse_es_option_symbol)
    df_valid = df[df['parsed'].notna()].copy()
    
    # Extract components
    df_valid['strike'] = df_valid['parsed'].apply(lambda x: x['strike'])
    df_valid['expiry_code'] = df_valid['parsed'].apply(lambda x: x['expiry_code'])
    df_valid['option_type'] = df_valid['parsed'].apply(lambda x: x['type'])
    
    # Calculate mid price and spread
    bid_col = 'bid_px_00' if 'bid_px_00' in df_valid.columns else 'bid_px'
    ask_col = 'ask_px_00' if 'ask_px_00' in df_valid.columns else 'ask_px'
    
    if bid_col in df_valid.columns and ask_col in df_valid.columns:
        df_valid['mid'] = (df_valid[bid_col] + df_valid[ask_col]) / 2
        df_valid['spread'] = df_valid[ask_col] - df_valid[bid_col]
        df_valid['spread_pct'] = (df_valid['spread'] / df_valid['mid']) * 100
    
    # Time coverage
    time_col = 'ts_recv' if 'ts_recv' in df_valid.columns else 'ts_event'
    unique_minutes = df_valid[time_col].dt.floor('min').nunique()
    time_range = df_valid[time_col].max() - df_valid[time_col].min()
    
    # Symbol statistics
    total_rows = len(df_valid)
    unique_symbols = df_valid['symbol'].nunique()
    unique_expiries = df_valid['expiry_code'].nunique()
    
    # Strikes per expiry
    strikes_per_expiry = df_valid.groupby(['expiry_code', 'option_type'])['strike'].nunique()
    
    # Data quality checks
    if bid_col in df_valid.columns and ask_col in df_valid.columns:
        missing_bid = df_valid[bid_col].isna().sum()
        missing_ask = df_valid[ask_col].isna().sum()
        negative_spreads = (df_valid['spread'] < 0).sum()
        zero_prices = ((df_valid[bid_col] == 0) | (df_valid[ask_col] == 0)).sum()
        median_spread = df_valid['spread'].median()
        median_spread_pct = df_valid['spread_pct'].median()
    else:
        missing_bid = missing_ask = negative_spreads = zero_prices = None
        median_spread = median_spread_pct = None
    
    # Expiry distribution (calls vs puts per expiry)
    expiry_dist = df_valid.groupby(['expiry_code', 'option_type']).size().unstack(fill_value=0)
    
    return {
        'file': file_path.name,
        'total_rows': total_rows,
        'unique_symbols': unique_symbols,
        'unique_expiries': unique_expiries,
        'unique_minutes': unique_minutes,
        'time_range_minutes': time_range.total_seconds() / 60 if pd.notna(time_range) else None,
        'avg_symbols_per_minute': total_rows / unique_minutes if unique_minutes > 0 else 0,
        'strikes_per_expiry': strikes_per_expiry.to_dict() if not strikes_per_expiry.empty else {},
        'expiry_distribution': expiry_dist.to_dict() if not expiry_dist.empty else {},
        'missing_bid': missing_bid,
        'missing_ask': missing_ask,
        'negative_spreads': negative_spreads,
        'zero_prices': zero_prices,
        'median_spread': median_spread,
        'median_spread_pct': median_spread_pct,
    }


def print_summary(results: list):
    """Print human-readable summary of all analyzed files."""
    print("\n" + "=" * 100)
    print("DATA QUALITY SUMMARY")
    print("=" * 100)
    
    for res in results:
        if 'error' in res:
            print(f"\n❌ {res['file']}: {res['error']}")
            continue
        
        print(f"\n📊 {res['file']}")
        print(f"   {'─' * 90}")
        
        # Basic stats
        print(f"   Total rows:          {res['total_rows']:>8,}")
        print(f"   Unique symbols:      {res['unique_symbols']:>8,}")
        print(f"   Unique expiries:     {res['unique_expiries']:>8,}")
        print(f"   Unique minutes:      {res['unique_minutes']:>8,}")
        print(f"   Avg symbols/minute:  {res['avg_symbols_per_minute']:>8,.0f}")
        
        # Data quality
        print(f"\n   Data Quality:")
        if res['missing_bid'] is not None:
            print(f"     Missing bid prices:    {res['missing_bid']:>6,} ({res['missing_bid']/res['total_rows']*100:>5.1f}%)")
            print(f"     Missing ask prices:    {res['missing_ask']:>6,} ({res['missing_ask']/res['total_rows']*100:>5.1f}%)")
            print(f"     Zero prices:           {res['zero_prices']:>6,} ({res['zero_prices']/res['total_rows']*100:>5.1f}%)")
            print(f"     Negative spreads:      {res['negative_spreads']:>6,} ({res['negative_spreads']/res['total_rows']*100:>5.1f}%)")
            print(f"     Median spread:         ${res['median_spread']:>6.2f}")
            print(f"     Median spread %:       {res['median_spread_pct']:>6.2f}%")
        
        # Expiry distribution
        if res['expiry_distribution']:
            print(f"\n   Expiry Distribution (Calls vs Puts):")
            expiry_df = pd.DataFrame(res['expiry_distribution'])
            if 'C' not in expiry_df.columns:
                expiry_df['C'] = 0
            if 'P' not in expiry_df.columns:
                expiry_df['P'] = 0
            expiry_df = expiry_df.sort_index()
            print(f"     {'Expiry':<10} {'Calls':>8} {'Puts':>8} {'Total':>8}")
            for expiry, row in expiry_df.iterrows():
                calls = int(row.get('C', 0))
                puts = int(row.get('P', 0))
                print(f"     {expiry:<10} {calls:>8,} {puts:>8,} {calls+puts:>8,}")
        
        # Strike coverage (sample for top 3 expiries)
        if res['strikes_per_expiry']:
            print(f"\n   Strike Coverage (top 3 expiries):")
            strikes_df = pd.Series(res['strikes_per_expiry']).sort_index()
            print(f"     {'Expiry':<10} {'Type':<6} {'Strikes':>8}")
            for (expiry, opt_type), count in list(strikes_df.items())[:6]:
                print(f"     {expiry:<10} {opt_type:<6} {count:>8,}")
    
    # Overall summary
    print(f"\n" + "=" * 100)
    print("OVERALL SUMMARY")
    print("=" * 100)
    
    valid_results = [r for r in results if 'error' not in r]
    if valid_results:
        total_files = len(valid_results)
        avg_symbols = sum(r['unique_symbols'] for r in valid_results) / total_files
        avg_expiries = sum(r['unique_expiries'] for r in valid_results) / total_files
        total_rows = sum(r['total_rows'] for r in valid_results)
        
        print(f"   Files analyzed:         {total_files}")
        print(f"   Total rows:             {total_rows:,}")
        print(f"   Avg symbols per file:   {avg_symbols:,.0f}")
        print(f"   Avg expiries per file:  {avg_expiries:,.1f}")
        
        # Check consistency
        symbol_counts = [r['unique_symbols'] for r in valid_results]
        symbol_std = np.std(symbol_counts)
        symbol_cv = symbol_std / avg_symbols if avg_symbols > 0 else 0
        
        print(f"\n   Consistency Check:")
        print(f"     Symbol count std dev:  {symbol_std:,.1f}")
        print(f"     Coefficient of var:    {symbol_cv:.1%}")
        
        if symbol_cv < 0.05:
            print(f"     ✅ Symbol counts are consistent across days")
        else:
            print(f"     ⚠️  Symbol counts vary significantly - investigate")
    
    print("=" * 100 + "\n")


def main():
    """Analyze all downloaded DBN and parquet files in data/raw/."""
    data_dir = PROJECT_ROOT / "data" / "raw"
    
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return 1
    
    # Find all data files (DBN and parquet)
    dbn_files = list(data_dir.glob("*.dbn")) + list(data_dir.glob("*.dbn.zst"))
    parquet_files = list(data_dir.glob("*.parquet"))
    all_files = sorted(dbn_files + parquet_files)
    
    if not all_files:
        logger.error(f"No data files found in {data_dir}")
        logger.info("Run the download script first: python scripts/run_last_week_5m.py")
        return 1
    
    logger.info(f"Found {len(all_files)} files to analyze ({len(dbn_files)} DBN, {len(parquet_files)} parquet)")
    
    # Analyze each file
    results = []
    for file_path in all_files:
        try:
            result = analyze_file(file_path)
            results.append(result)
        except Exception as e:
            logger.error(f"Error analyzing {file_path.name}: {e}")
            results.append({'error': str(e), 'file': file_path.name})
    
    # Print summary
    print_summary(results)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

