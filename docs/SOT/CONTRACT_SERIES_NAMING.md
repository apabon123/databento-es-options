# Contract Series Naming Convention

**Authoritative specification of contract series naming patterns in the canonical database.**

This document explains how continuous contract series are named and how to identify the canonical series for each root.

---

## Naming Pattern

**Format:** `{ROOT}_{RANK}_{ROLL_STRATEGY}`

### Components

1. **ROOT** - The underlying futures root symbol (e.g., `ES`, `NQ`, `ZN`, `SR3`)
2. **RANK** - The contract position in the term structure:
   - `FRONT` = rank 0 (front month)
   - Numeric: `0`, `1`, `2`, ... (0 = front month, 1 = second month, etc.)
3. **ROLL_STRATEGY** - The roll timing and method:
   - `CALENDAR` - Calendar-based roll (typically 1-day pre-expiry)
   - `CALENDAR_2D` - Calendar-based roll with 2-day pre-expiry timing
   - `VOLUME` - Volume-based roll (rolls when volume shifts to next contract)

---

## RANK Values

- **`FRONT`** is equivalent to rank `0` (front month)
- **Numeric ranks** (`0`, `1`, `2`, ...) explicitly specify the position:
  - `0` = front month
  - `1` = second month
  - `2` = third month
  - etc.

**Examples:**
- `ES_FRONT_CALENDAR_2D` = ES front month (rank 0), 2-day pre-expiry calendar roll
- `SR3_0_CALENDAR_2D` = SR3 front month (rank 0), 2-day pre-expiry calendar roll
- `SR3_1_CALENDAR_2D` = SR3 second month (rank 1), 2-day pre-expiry calendar roll

---

## Roll Strategies

### CALENDAR

Calendar-based roll that occurs on a fixed schedule relative to contract expiration.

**DataBento Code:** `.c.`

**Common Usage:** Equity indices (ES, NQ, RTY), FX (6E, 6J, 6B), STIR (SR3), Volatility (VX)

### CALENDAR_2D

Calendar-based roll with **2-day pre-expiry** timing. The `_2D` suffix indicates that the roll occurs 2 trading days before contract expiration.

**DataBento Code:** `.c.` (with 2-day pre-expiry parameter)

**Common Usage:** Equity indices, STIR contracts

**Note:** The `_2D` suffix is important for distinguishing roll timing. A series without this suffix may use different roll timing (e.g., 1-day pre-expiry).

### VOLUME

Volume-based roll that occurs when trading volume shifts from the front contract to the next contract.

**DataBento Code:** `.v.`

**Common Usage:** Rates (ZT, ZF, ZN, UB), Commodities (CL, GC)

---

## Multiple Series Per Root

**Important:** Multiple contract series may exist for the same root with different:
- **Ranks** (e.g., `SR3_0_CALENDAR_2D`, `SR3_1_CALENDAR_2D`, `SR3_2_CALENDAR_2D`)
- **Roll strategies** (e.g., `ES_FRONT_CALENDAR`, `ES_FRONT_CALENDAR_2D`, `ES_FRONT_VOLUME`)

All available series are stored in `dim_continuous_contract` and can be queried from `g_continuous_bar_daily`.

---

## Canonical Series

### Definition

The **canonical series** for each root is the series designated for use by downstream systems (e.g., Futures-Six). Canonical series are defined in `configs/canonical_series.yaml`.

### Selection Criteria

**Critical:** The canonical series choice should be based on **COVERAGE**, not naming convention.

- The canonical series is the series with the **best historical coverage** for that root
- Coverage may vary due to:
  - Data availability from DataBento
  - Roll strategy differences affecting data continuity
  - Historical contract listing dates
- Multiple series may exist, but only one is canonical per root

### Verification

To verify canonical series coverage and identify the best series:

```bash
python scripts/analysis/audit_contract_series.py
```

This script will:
- Analyze coverage for all available series per root
- Identify gaps and data quality issues
- Recommend the series with best coverage for canonical selection

### Querying Canonical Series

Downstream systems should use the view `v_canonical_continuous_bar_daily`:

```sql
SELECT trading_date, root, contract_series, open, high, low, close, volume
FROM v_canonical_continuous_bar_daily
WHERE trading_date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY root, trading_date;
```

This view automatically selects the canonical series per root as defined in `configs/canonical_series.yaml`.

---

## Examples

### Equity Indices

```
ES_FRONT_CALENDAR_2D    # ES front month, 2-day pre-expiry calendar roll
NQ_FRONT_CALENDAR_2D    # NQ front month, 2-day pre-expiry calendar roll
RTY_FRONT_CALENDAR_2D   # RTY front month, 2-day pre-expiry calendar roll
```

### Rates

```
ZN_FRONT_VOLUME         # ZN front month, volume roll
UB_FRONT_VOLUME         # UB front month, volume roll
```

### STIR (Multiple Ranks)

```
SR3_0_CALENDAR_2D       # SR3 front month (rank 0)
SR3_1_CALENDAR_2D       # SR3 second month (rank 1)
SR3_2_CALENDAR_2D       # SR3 third month (rank 2)
...
SR3_12_CALENDAR_2D      # SR3 13th month (rank 12)
```

### Commodities

```
CL_FRONT_VOLUME         # CL front month, volume roll
GC_FRONT_VOLUME         # GC front month, volume roll
```

---

## Related Documentation

- [INTEROP_CONTRACT.md](./INTEROP_CONTRACT.md) - Guaranteed tables and series for downstream systems
- [DATA_ARCHITECTURE.md](./DATA_ARCHITECTURE.md) - Database schema and structure
- `configs/canonical_series.yaml` - Current canonical series definitions
- `configs/download_universe.yaml` - All available contract series

---

## Summary

| Aspect | Description |
|--------|-------------|
| **Naming Pattern** | `{ROOT}_{RANK}_{ROLL_STRATEGY}` |
| **RANK** | `FRONT` or numeric (`0`, `1`, `2`, ...) |
| **ROLL_STRATEGY** | `CALENDAR`, `CALENDAR_2D`, `VOLUME` |
| **`_2D` Suffix** | Indicates 2-day pre-expiry roll timing |
| **Multiple Series** | Multiple series per root may exist with different ranks/strategies |
| **Canonical Selection** | Based on **COVERAGE**, not naming convention |
| **Verification** | Run `scripts/analysis/audit_contract_series.py` to verify coverage |
