# Roll Strategy Migration - November 6, 2025

## Summary

Successfully implemented roll-strategy support for continuous futures data across the entire system:
- File structure reorganized by roll strategy
- Database schema updated with new naming convention
- All scripts updated to handle the new structure
- All 442 daily bars (ES + NQ) remain accessible

## What Changed

### 1. File Structure
**Before:**
```
raw/
  glbx-mdp3-es-2025-01-01.ohlcv-1d.fullday.parquet
  glbx-mdp3-nq-2025-01-01.ohlcv-1d.fullday.parquet
  glbx-mdp3-2025-01-01/
    continuous_bars_daily/...
```

**After:**
```
raw/
  ohlcv-1d/
    downloads/
      es/calendar-2d/*.parquet          (221 files)
      nq/calendar-2d/*.parquet          (221 files)
    transformed/
      es/calendar-2d/2025-01-01/...     (221 directories)
      nq/calendar-2d/2025-01-01/...     (221 directories)
  
  bbo-1m/
    downloads/calendar-2d/              (269 files)
    transformed/calendar-2d/            (18 directories)
  
  continuous/
    transformed/calendar-2d/            (228 directories)
  
  futures/
    transformed/                        (5 directories)
```

### 2. Database Schema
**Before:**
- `ES_FRONT_MONTH`
- `NQ_FRONT_MONTH`

**After:**
- `ES_FRONT_CALENDAR_2D`
- `NQ_FRONT_CALENDAR_2D`

**Naming Convention:** `{ROOT}_FRONT_{ROLL_STRATEGY}`

### 3. Scripts Updated
✅ `scripts/download/download_es_nq_daily_ohlcv.py`
  - Downloads to `ohlcv-1d/downloads/{root}/calendar-2d/`
  - Transforms to `ohlcv-1d/transformed/{root}/calendar-2d/{date}/`
  - `--re-transform` and `--ingest-only` flags updated for new structure

✅ `db/migrations/1005_update_contract_series_with_roll.sql`
  - Renamed contract_series in all tables
  - Updated descriptions

✅ `README.md`
  - Updated data storage section
  - Updated roll strategy documentation

✅ `ROLL_STRATEGY_GUIDE.md` (NEW)
  - Complete guide for adding new roll strategies
  - Examples and SQL queries
  - Migration history

## Migration Statistics

| Item | Count | Status |
|------|-------|--------|
| OHLCV-1d downloads (parquet) | 442 | ✅ Moved |
| OHLCV-1d transformed (directories) | 442 | ✅ Moved |
| BBO-1m downloads | 269 | ✅ Moved |
| BBO-1m transformed | 18 | ✅ Moved |
| Continuous transformed | 228 | ✅ Moved |
| **Total items migrated** | **1,399** | **✅ Complete** |

## Data Integrity

✅ All 442 daily bars accessible in database:
- ES_FRONT_CALENDAR_2D: 221 bars (2025-01-01 to 2025-11-05)
- NQ_FRONT_CALENDAR_2D: 221 bars (2025-01-01 to 2025-11-05)

## Benefits

1. **No Conflicts** - Each roll strategy has its own isolated space
2. **Flexibility** - Can now add volume rolls, OI rolls, custom rolls
3. **Clarity** - File paths clearly indicate roll strategy
4. **Future-Proof** - Easy to scale to multiple roll strategies
5. **Database Integrity** - Each (root + roll) is uniquely identified

## Adding New Roll Strategies

To add a new roll strategy (e.g., volume-based):

1. **Create folder structure:**
   ```bash
   mkdir -p "raw/ohlcv-1d/downloads/es/volume"
   mkdir -p "raw/ohlcv-1d/downloads/nq/volume"
   ```

2. **Add to database:**
   ```sql
   INSERT INTO dim_continuous_contract VALUES
     ('ES_FRONT_VOLUME', 'ES', 0, 'volume_roll', 'unadjusted', 
      'ES continuous front month (roll: volume-based)');
   ```

3. **Download with new strategy:**
   - Modify `DEFAULT_ROLL_STRATEGY` in download script
   - Or add `--roll-strategy` CLI argument

4. **Ingest normally** - Scripts automatically route to correct series

## Files Created/Modified

### New Files
- ✅ `ROLL_STRATEGY_GUIDE.md` - Comprehensive guide
- ✅ `db/migrations/1005_update_contract_series_with_roll.sql` - Database migration
- ✅ `MIGRATION_SUMMARY_2025-11-06.md` - This file

### Modified Files
- ✅ `scripts/download/download_es_nq_daily_ohlcv.py` - Updated for new structure
- ✅ `README.md` - Updated data storage documentation

### Temporary Files (Deleted)
- ❌ `migrate_add_roll_strategy.py` - Migration script (deleted after use)
- ❌ `verify_structure.py` - Verification script (deleted after use)

## Verification Commands

```powershell
# View database summary
python scripts/download/download_es_nq_daily_ohlcv.py --summary

# Inspect futures in detail
python scripts/database/inspect_futures.py

# Check specific contract series
python -c "import duckdb; con = duckdb.connect('data/silver/market.duckdb'); print(con.execute('SELECT * FROM dim_continuous_contract').df()); con.close()"
```

## Next Steps (Optional)

1. **Add Volume Roll** - Implement volume-based roll strategy
2. **Add Calendar-1d** - Implement 1-day pre-expiry roll
3. **Add Open Interest Roll** - Implement OI-based roll
4. **Backfill Data** - Download historical data with different roll strategies
5. **Comparative Analysis** - Compare performance across different roll strategies

## Contact

For questions about this migration, refer to:
- `ROLL_STRATEGY_GUIDE.md` - Detailed documentation
- `README.md` - General project documentation
- Database inspection scripts in `scripts/database/`

---

**Migration Date:** November 6, 2025  
**Migration Status:** ✅ Complete  
**Data Integrity:** ✅ Verified  
**Total Duration:** ~20 minutes

