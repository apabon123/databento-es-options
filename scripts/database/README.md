# Database Scripts

Scripts for inspecting, maintaining, and checking the database.

## Scripts

### `check_database.py`
Check for duplicate rows and show database statistics.

**Usage:**
```powershell
# Check all products for duplicates
python scripts/database/check_database.py

# Check specific product
python scripts/database/check_database.py --product ES_CONTINUOUS_MDP3

# Show statistics only
python scripts/database/check_database.py --stats-only
```

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

### `view_continuous_bars.py`
Temporary script to view ES continuous contract bars.

**Usage:**
```powershell
python scripts/database/view_continuous_bars.py
```

