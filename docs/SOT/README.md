# Single Source of Truth (SOT) Documentation

**Authoritative documentation for the canonical research database.**

This folder contains the **definitive** specifications for what this repository provides, how data is organized, and what downstream systems can rely on.

---

## Documents

### [DATA_SOURCE_POLICY.md](DATA_SOURCE_POLICY.md) ⭐ **Authoritative**

**Single source of truth policy for volatility data.**

Defines which data series come from which sources, why those choices were made, and how to maintain them. This is the **authoritative** reference for:
- VIX (1M) from FRED
- VIX3M (3M) from CBOE via financial-data-system
- VVIX from CBOE via financial-data-system
- VX Futures (VX1/2/3) from CBOE via financial-data-system

**Use this when:** You need to know where volatility data comes from or how to update it.

---

### [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md)

**Database architecture and organization.**

Describes the Bronze-Silver-Gold pattern, table naming conventions, schema details, and access patterns. This is the **authoritative** reference for:
- Database structure (`data/silver/market.duckdb`)
- Table organization (`dim_*`, `f_*`, `g_*`, `v_*`)
- Contract series naming conventions
- Roll strategies

**Use this when:** You need to understand the database structure or query patterns.

---

### [UPDATE_WORKFLOWS.md](UPDATE_WORKFLOWS.md)

**Standard procedures for updating data.**

Documents the maintenance schedule, update commands, validation procedures, and troubleshooting. This is the **authoritative** reference for:
- Daily/weekly update procedures
- Historical backfill workflows
- Data validation and quality checks
- Maintenance schedule

**Use this when:** You need to update data or verify data quality.

---

### [INTEROP_CONTRACT.md](INTEROP_CONTRACT.md) ⭐ **Critical for Downstream Systems**

**Guaranteed tables and series for downstream systems.**

Defines the contract between this repository and downstream systems (e.g., `Futures-Six`). Specifies what is **guaranteed** to exist and what downstream systems must compute themselves. This is the **authoritative** reference for:
- Guaranteed tables and schemas
- Authoritative series and their sources
- What downstream systems may assume
- What downstream systems must compute (feature construction, regime logic, etc.)

**Use this when:** You're building a downstream system that depends on this database.

---

## Quick Reference

| Question | Document |
|----------|----------|
| Where does VIX data come from? | [DATA_SOURCE_POLICY.md](DATA_SOURCE_POLICY.md) |
| What tables are guaranteed to exist? | [INTEROP_CONTRACT.md](INTEROP_CONTRACT.md) |
| How do I update FRED data? | [UPDATE_WORKFLOWS.md](UPDATE_WORKFLOWS.md) |
| What's the database schema? | [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md) |
| What can I assume exists? | [INTEROP_CONTRACT.md](INTEROP_CONTRACT.md) |
| What must I compute myself? | [INTEROP_CONTRACT.md](INTEROP_CONTRACT.md) |

---

## For Downstream Systems

If you're building a system that depends on this database:

1. **Start with** [INTEROP_CONTRACT.md](INTEROP_CONTRACT.md) - understand what's guaranteed
2. **Reference** [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md) - understand the schema
3. **Check** [DATA_SOURCE_POLICY.md](DATA_SOURCE_POLICY.md) - understand data sources
4. **Follow** [UPDATE_WORKFLOWS.md](UPDATE_WORKFLOWS.md) - if you need to update data

---

## Related Documentation

- [../TECHNICAL_REFERENCE.md](../TECHNICAL_REFERENCE.md) - Complete schema reference
- [../DATA_SOURCES_SUMMARY.md](../DATA_SOURCES_SUMMARY.md) - Complete list of all data sources
- [../../QUICK_REFERENCE.md](../../QUICK_REFERENCE.md) - Command reference
- [../../README.md](../../README.md) - Project overview

---

## Version

**SOT Documentation Version:** 1.0  
**Last Updated:** 2025-01-14
