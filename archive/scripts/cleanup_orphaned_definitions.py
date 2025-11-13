"""Remove instrument definitions that aren't in daily bars."""
from pipelines.common import get_paths, connect_duckdb
import argparse

parser = argparse.ArgumentParser(description="Remove instrument definitions not in daily bars")
parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without deleting')
args = parser.parse_args()

_, _, dbpath = get_paths()
con = connect_duckdb(dbpath)

print("=" * 80)
print("CLEANING UP ORPHANED INSTRUMENT DEFINITIONS")
print("=" * 80)
print()

# Get definitions not in daily bars
orphaned = con.execute("""
    SELECT 
        instrument_id,
        native_symbol,
        asset,
        expiration
    FROM dim_instrument_definition
    WHERE instrument_id NOT IN (
        SELECT DISTINCT underlying_instrument_id
        FROM g_continuous_bar_daily
        WHERE underlying_instrument_id IS NOT NULL
    )
    ORDER BY asset, instrument_id
""").fetchdf()

if orphaned.empty:
    print("No orphaned definitions found!")
    con.close()
    exit(0)

print(f"Found {len(orphaned)} orphaned definitions (not in daily bars):")
print(orphaned.to_string(index=False))
print()

if args.dry_run:
    print("DRY RUN: Would delete these definitions")
    print("Run without --dry-run to actually delete them")
else:
    # Delete orphaned definitions
    deleted = con.execute("""
        DELETE FROM dim_instrument_definition
        WHERE instrument_id NOT IN (
            SELECT DISTINCT underlying_instrument_id
            FROM g_continuous_bar_daily
            WHERE underlying_instrument_id IS NOT NULL
        )
    """).fetchone()[0]
    
    print(f"Deleted {deleted} orphaned definitions")
    print()

con.close()

