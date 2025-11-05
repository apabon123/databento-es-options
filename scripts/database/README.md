# Database Scripts

Scripts for inspecting, maintaining, and checking the database.

## Scripts

### `check_database.py`
Check for duplicate rows, show database statistics, and verify data coverage.

**Usage:**
```powershell
# Check all products for duplicates and show statistics
python scripts/database/check_database.py

# Check specific product
python scripts/database/check_database.py --product ES_CONTINUOUS_MDP3

# Show statistics only
python scripts/database/check_database.py --stats-only

# Verify continuous futures coverage (checks for missing dates and data quality)
python scripts/database/check_database.py --verify-coverage --year 2025
```

**Coverage verification checks:**
- All expected trading days are present
- Each day has expected quote counts (~1,300 for full days)
- Identifies missing dates, partial days, and test data
- Monthly summary with quote counts

### `inspect_futures.py`
Inspect ES futures data in detail.

**Usage:**
```powershell
# Full inspection
python scripts/database/inspect_futures.py

# Inspect specific contract
python scripts/database/inspect_futures.py --contract ESH6

# Export sample data
python scripts/database/inspect_futures.py --export
```

