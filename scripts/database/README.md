# Database Scripts

Database management and inspection. See [QUICK_REFERENCE.md](../../QUICK_REFERENCE.md#database-commands) for full documentation.

## Scripts

| Script | Purpose |
|--------|---------|
| `check_database.py` | Check duplicates, stats, verify coverage |
| `inspect_futures.py` | Inspect ES futures data |
| `download_instrument_definitions.py` | Download contract specs from DataBento |
| `ingest_fred_series.py` | Ingest FRED parquets into database |
| `populate_instrument_metadata.py` | Populate instrument metadata |
| `sync_vix_vx_from_financial_data_system.py` | Sync VX continuous (VX1/2/3) and VIX3M index from financial-data-system DB |

## Quick Examples

```powershell
python scripts/database/check_database.py
python scripts/database/check_database.py --verify-coverage --year 2025
python scripts/database/inspect_futures.py --contract ESH6
python scripts/database/download_instrument_definitions.py --all
python scripts/database/sync_vix_vx_from_financial_data_system.py
```

## Data Source Policy

See [docs/SOT/DATA_SOURCE_POLICY.md](../../docs/SOT/DATA_SOURCE_POLICY.md) for authoritative policy.

**Volatility Data:**
- **VIX Index (1M)**: Use FRED (via `download_fred_series.py`) - primary source for spot VIX
- **VX Futures (VX1/2/3)**: Use CBOE via financial-data-system → `sync_vix_vx_from_financial_data_system.py`
  - Sources: `financial-data-system.market_data` (symbols: `@VX=101XN`, `@VX=201XN`, `@VX=301XN`)
  - Targets: `market_data`, `continuous_contracts`
- **VIX3M Index (3M)**: Use CBOE via financial-data-system → `sync_vix_vx_from_financial_data_system.py`
  - Source: `financial-data-system.market_data_cboe` (symbol: `VIX3M`)
  - Target: `market_data_cboe`
  - Reason: FRED coverage for VIX3M is insufficient; CBOE is authoritative source

## Related Documentation

- [docs/SOT/DATA_SOURCE_POLICY.md](../../docs/SOT/DATA_SOURCE_POLICY.md) - Authoritative data source policy
- [docs/SOT/INTEROP_CONTRACT.md](../../docs/SOT/INTEROP_CONTRACT.md) - Guaranteed tables and series
- [docs/SOT/UPDATE_WORKFLOWS.md](../../docs/SOT/UPDATE_WORKFLOWS.md) - Update procedures
