"""
Organize and clean up the raw data folder.

This script:
1. Identifies raw downloads (KEEP - user paid for these)
2. Identifies old/misnamed folders (can delete)
3. Ensures proper folder structure
4. Documents what's safe to delete
"""
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common import get_paths

def main():
    raw_dir, _, _ = get_paths()
    
    print("=" * 80)
    print("RAW FOLDER ANALYSIS")
    print("=" * 80)
    print()
    
    # 1. Raw Downloads (KEEP - user paid for these)
    print("1. RAW DOWNLOADS (KEEP - You paid for these):")
    print("-" * 80)
    fullday_files = sorted(raw_dir.glob('glbx-mdp3-*.bbo-1m.fullday.parquet'))
    last5m_files = sorted(raw_dir.glob('glbx-mdp3-*.bbo-1m.last5m.parquet'))
    print(f"  • Full day files: {len(fullday_files)} files")
    print(f"  • Last 5m files: {len(last5m_files)} files (test data, can delete if needed)")
    print(f"  • Total size: ~{sum(f.stat().st_size for f in fullday_files) / (1024*1024):.1f} MB")
    print()
    
    # 2. Correctly named transformed folders
    print("2. CORRECTLY NAMED TRANSFORMED FOLDERS (glbx-mdp3-YYYY-MM-DD):")
    print("-" * 80)
    correct_folders = sorted([d for d in raw_dir.iterdir() 
                             if d.is_dir() and d.name.startswith('glbx-mdp3-2025-')])
    print(f"  • Count: {len(correct_folders)} folders")
    
    # Check structure
    valid_count = 0
    for folder in correct_folders:
        if (folder / 'continuous_quotes_l1').exists():
            valid_count += 1
    print(f"  • With valid structure: {valid_count} folders")
    print()
    
    # 3. Old/Misnamed folders (can delete)
    print("3. OLD/MISNAMED FOLDERS (Safe to delete):")
    print("-" * 80)
    old_folders = [d for d in raw_dir.iterdir() 
                  if d.is_dir() and d.name.startswith('glbx-mdp3-') 
                  and not d.name.startswith('glbx-mdp3-2025-')]
    
    if old_folders:
        print(f"  • Found {len(old_folders)} old/misnamed folders:")
        for folder in sorted(old_folders)[:20]:  # Show first 20
            print(f"    - {folder.name}")
        if len(old_folders) > 20:
            print(f"    ... and {len(old_folders) - 20} more")
    else:
        print("  • None found")
    print()
    
    # 4. Summary and recommendations
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print()
    print("KEEP (Raw downloads - you paid for these):")
    print(f"  [KEEP] {len(fullday_files)} .fullday.parquet files")
    print(f"  [OPTIONAL] {len(last5m_files)} .last5m.parquet files (test data, can delete)")
    print()
    print("KEEP (Transformed data - needed for ingestion):")
    print(f"  [KEEP] {valid_count} correctly named transformed folders")
    print()
    
    if old_folders:
        total_size = sum(sum(f.stat().st_size for f in folder.rglob('*') if f.is_file()) 
                        for folder in old_folders) / (1024*1024)
        print("CAN DELETE (Old/misnamed folders):")
        print(f"  [DELETE] {len(old_folders)} folders (saves ~{total_size:.1f} MB)")
        print()
        print("To delete old folders, run:")
        print("  python scripts/organize_raw_folder.py --delete-old")
    else:
        print("No old folders to delete.")
    
    return 0

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Organize raw data folder")
    parser.add_argument('--delete-old', action='store_true', 
                       help='Delete old/misnamed folders (dry-run by default)')
    args = parser.parse_args()
    
    if args.delete_old:
        raw_dir, _, _ = get_paths()
        old_folders = [d for d in raw_dir.iterdir() 
                      if d.is_dir() and d.name.startswith('glbx-mdp3-') 
                      and not d.name.startswith('glbx-mdp3-2025-')]
        
        if old_folders:
            print(f"Deleting {len(old_folders)} old folders...")
            for folder in old_folders:
                import shutil
                shutil.rmtree(folder)
                print(f"  Deleted: {folder.name}")
            print("Done!")
        else:
            print("No old folders to delete.")
    else:
        sys.exit(main())

