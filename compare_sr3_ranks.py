"""Compare SR3 rank 0 and rank 1 data to check if they're identical."""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths, connect_duckdb
import pandas as pd

def compare_sr3_ranks():
    """Compare SR3 rank 0 (FRONT) and rank 1 data."""
    _, _, dbpath = get_paths()
    
    if not dbpath.exists():
        print("Database not found at:", dbpath)
        return
    
    con = connect_duckdb(dbpath)
    
    try:
        # Get data for both ranks (including underlying instrument IDs)
        rank0 = con.execute("""
            SELECT 
                trading_date,
                underlying_instrument_id,
                open,
                high,
                low,
                close,
                volume
            FROM g_continuous_bar_daily
            WHERE contract_series = 'SR3_FRONT_CALENDAR'
            ORDER BY trading_date
        """).fetchdf()
        
        rank1 = con.execute("""
            SELECT 
                trading_date,
                underlying_instrument_id,
                open,
                high,
                low,
                close,
                volume
            FROM g_continuous_bar_daily
            WHERE contract_series = 'SR3_RANK_1_CALENDAR'
            ORDER BY trading_date
        """).fetchdf()
        
        print("=" * 80)
        print("SR3 RANK 0 vs RANK 1 COMPARISON")
        print("=" * 80)
        
        print(f"\nRank 0 (FRONT) records: {len(rank0)}")
        print(f"Rank 1 records: {len(rank1)}")
        
        # Find overlapping dates
        rank0_dates = set(rank0['trading_date'].dt.date)
        rank1_dates = set(rank1['trading_date'].dt.date)
        overlap_dates = rank0_dates.intersection(rank1_dates)
        
        print(f"\nOverlapping dates: {len(overlap_dates)}")
        print(f"Rank 0 only dates: {len(rank0_dates - rank1_dates)}")
        print(f"Rank 1 only dates: {len(rank1_dates - rank0_dates)}")
        
        if len(overlap_dates) > 0:
            # Merge on trading_date
            merged = rank0.merge(
                rank1,
                on='trading_date',
                suffixes=('_rank0', '_rank1'),
                how='inner'
            )
            
            print(f"\nMerged records for comparison: {len(merged)}")
            
            # Check if underlying instrument IDs are the same
            same_underlying = (merged['underlying_instrument_id_rank0'] == merged['underlying_instrument_id_rank1']).sum()
            print(f"\nDates with SAME underlying_instrument_id: {same_underlying} / {len(merged)} ({100*same_underlying/len(merged):.1f}%)")
            print(f"Dates with DIFFERENT underlying_instrument_id: {len(merged) - same_underlying} / {len(merged)} ({100*(len(merged)-same_underlying)/len(merged):.1f}%)")
            
            if same_underlying > 0:
                print("\n" + "=" * 80)
                print("WARNING: Some dates have the same underlying contract!")
                print("=" * 80)
                same_underlying_dates = merged[merged['underlying_instrument_id_rank0'] == merged['underlying_instrument_id_rank1']]
                print(f"\nSample dates with same underlying ID (first 10):")
                sample_cols = ['trading_date', 'underlying_instrument_id_rank0', 'close_rank0', 'close_rank1']
                print(same_underlying_dates[sample_cols].head(10).to_string(index=False))
            
            # Get underlying symbols from instrument definitions if available
            print("\n" + "=" * 80)
            print("UNDERLYING CONTRACT SYMBOLS (sample dates)")
            print("=" * 80)
            
            # Try to get symbols from instrument definitions table
            try:
                sample_dates = merged.head(5)['trading_date'].tolist()
                for td in sample_dates:
                    r0_id = merged[merged['trading_date'] == td]['underlying_instrument_id_rank0'].iloc[0]
                    r1_id = merged[merged['trading_date'] == td]['underlying_instrument_id_rank1'].iloc[0]
                    
                    r0_symbol = con.execute("""
                        SELECT native_symbol FROM dim_instrument_metadata 
                        WHERE instrument_id = ?
                    """, [r0_id]).fetchone()
                    
                    r1_symbol = con.execute("""
                        SELECT native_symbol FROM dim_instrument_metadata 
                        WHERE instrument_id = ?
                    """, [r1_id]).fetchone()
                    
                    print(f"\n{td}:")
                    print(f"  Rank 0 (FRONT): instrument_id={r0_id}, symbol={r0_symbol[0] if r0_symbol else 'N/A'}")
                    print(f"  Rank 1:        instrument_id={r1_id}, symbol={r1_symbol[0] if r1_symbol else 'N/A'}")
            except Exception as e:
                print(f"Could not retrieve underlying symbols: {e}")
            
            # Compare values
            price_cols = ['open', 'high', 'low', 'close']
            differences = {}
            
            for col in price_cols:
                diff = (merged[f'{col}_rank0'] - merged[f'{col}_rank1']).abs()
                differences[col] = {
                    'identical': (diff == 0).sum(),
                    'different': (diff > 0).sum(),
                    'max_diff': diff.max(),
                    'mean_diff': diff.mean()
                }
            
            # Volume comparison
            vol_diff = (merged['volume_rank0'] - merged['volume_rank1']).abs()
            differences['volume'] = {
                'identical': (vol_diff == 0).sum(),
                'different': (vol_diff > 0).sum(),
                'max_diff': vol_diff.max(),
                'mean_diff': vol_diff.mean()
            }
            
            print("\n" + "=" * 80)
            print("VALUE COMPARISON (on overlapping dates)")
            print("=" * 80)
            for col, stats in differences.items():
                print(f"\n{col.upper()}:")
                print(f"  Identical values: {stats['identical']} / {len(merged)} ({100*stats['identical']/len(merged):.1f}%)")
                print(f"  Different values: {stats['different']} / {len(merged)} ({100*stats['different']/len(merged):.1f}%)")
                if stats['different'] > 0:
                    print(f"  Max difference: {stats['max_diff']:.6f}")
                    print(f"  Mean difference: {stats['mean_diff']:.6f}")
            
            # Show sample of differences if any
            if differences['close']['different'] > 0:
                print("\n" + "=" * 80)
                print("SAMPLE DATES WITH DIFFERENCES")
                print("=" * 80)
                for col in price_cols:
                    diff_col = f'{col}_diff'
                    merged[diff_col] = (merged[f'{col}_rank0'] - merged[f'{col}_rank1']).abs()
                
                # Find dates with any price difference
                has_diff = merged[[f'{c}_diff' for c in price_cols]].sum(axis=1) > 0
                diff_dates = merged[has_diff].head(10)
                
                if len(diff_dates) > 0:
                    print("\nFirst 10 dates with differences:")
                    display_cols = ['trading_date'] + [f'{c}_rank0' for c in price_cols] + [f'{c}_rank1' for c in price_cols]
                    print(diff_dates[display_cols].to_string(index=False))
            else:
                print("\n" + "=" * 80)
                print("RESULT: All overlapping dates have IDENTICAL price values!")
                print("=" * 80)
            
            # Show sample of identical data
            print("\n" + "=" * 80)
            print("SAMPLE DATA (first 5 overlapping dates)")
            print("=" * 80)
            sample = merged.head(5)[['trading_date', 'close_rank0', 'close_rank1', 'volume_rank0', 'volume_rank1']]
            print(sample.to_string(index=False))
            
    finally:
        con.close()

if __name__ == "__main__":
    compare_sr3_ranks()

