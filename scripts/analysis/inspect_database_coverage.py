"""Inspect database tables, symbols, and coverage for Futures-Six integration."""
import duckdb
from pathlib import Path
from pipelines.common import get_paths, connect_duckdb

def get_database_path():
    """Get the database path."""
    _, _, db_path = get_paths()
    return db_path

def list_all_tables(con):
    """List all tables in the database."""
    tables = con.execute("""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchdf()
    return tables

def inspect_table_coverage(con, table_name):
    """Inspect coverage for a specific table."""
    try:
        # Get table schema
        schema = con.execute(f"DESCRIBE {table_name}").fetchdf()
        columns = schema['column_name'].tolist()
        
        # Try to identify key columns
        date_cols = [c for c in columns if 'date' in c.lower() or 'timestamp' in c.lower() or 'ts_' in c.lower()]
        symbol_cols = [c for c in columns if any(x in c.lower() for x in ['symbol', 'series', 'contract', 'root', 'series_id', 'asset'])]
        
        results = {
            'table': table_name,
            'columns': columns,
            'date_columns': date_cols,
            'symbol_columns': symbol_cols,
            'row_count': None,
            'coverage': []
        }
        
        # Get row count
        row_count = con.execute(f"SELECT COUNT(*) as cnt FROM {table_name}").fetchone()[0]
        results['row_count'] = row_count
        
        if row_count == 0:
            return results
        
        # Try to get date range
        if date_cols:
            date_col = date_cols[0]
            try:
                date_range = con.execute(f"""
                    SELECT 
                        MIN({date_col}) as min_date,
                        MAX({date_col}) as max_date
                    FROM {table_name}
                """).fetchone()
                results['date_range'] = (date_range[0], date_range[1])
            except:
                pass
        
        # Try to get symbol/identifier breakdown
        if symbol_cols:
            symbol_col = symbol_cols[0]
            try:
                # Get distinct symbols and their coverage
                symbol_query = f"""
                    SELECT 
                        {symbol_col} as identifier,
                        COUNT(*) as row_count,
                        MIN({date_cols[0]}) as min_date,
                        MAX({date_cols[0]}) as max_date
                    FROM {table_name}
                    GROUP BY {symbol_col}
                    ORDER BY {symbol_col}
                """
                symbol_coverage = con.execute(symbol_query).fetchdf()
                results['coverage'] = symbol_coverage.to_dict('records')
            except Exception as e:
                # If that fails, just get distinct symbols
                try:
                    distinct_symbols = con.execute(f"SELECT DISTINCT {symbol_col} FROM {table_name} ORDER BY {symbol_col}").fetchdf()
                    results['distinct_symbols'] = distinct_symbols[symbol_col].tolist()
                except:
                    pass
        
        return results
        
    except Exception as e:
        return {
            'table': table_name,
            'error': str(e)
        }

def main():
    """Main inspection function."""
    db_path = get_database_path()
    print(f"\n{'='*80}")
    print(f"Database Location: {db_path}")
    print(f"Exists: {db_path.exists()}")
    print(f"{'='*80}\n")
    
    if not db_path.exists():
        print("ERROR: Database file not found!")
        return
    
    con = connect_duckdb(db_path)
    
    try:
        # List all tables
        print("ALL TABLES IN DATABASE:")
        print("-" * 80)
        tables = list_all_tables(con)
        print(tables.to_string(index=False))
        print()
        
        # Inspect each table
        print("\n" + "="*80)
        print("TABLE COVERAGE DETAILS")
        print("="*80 + "\n")
        
        for _, row in tables.iterrows():
            table_name = row['table_name']
            table_type = row['table_type']
            
            print(f"\n{'-'*80}")
            print(f"Table: {table_name} ({table_type})")
            print(f"{'-'*80}")
            
            coverage = inspect_table_coverage(con, table_name)
            
            if 'error' in coverage:
                print(f"  ERROR: {coverage['error']}")
                continue
            
            print(f"  Row Count: {coverage['row_count']:,}")
            
            if 'date_range' in coverage:
                min_date, max_date = coverage['date_range']
                print(f"  Date Range: {min_date} to {max_date}")
            
            if coverage.get('symbol_columns'):
                print(f"  Symbol/Identifier Columns: {', '.join(coverage['symbol_columns'])}")
            
            if coverage.get('coverage'):
                print(f"\n  Coverage by {coverage['symbol_columns'][0]}:")
                print(f"  {'Identifier':<30} {'Rows':>12} {'Min Date':<12} {'Max Date':<12}")
                print(f"  {'-'*30} {'-'*12} {'-'*12} {'-'*12}")
                for item in coverage['coverage']:
                    identifier = str(item['identifier'])[:28]
                    rows = item['row_count']
                    min_d = str(item['min_date'])[:10] if item['min_date'] else 'N/A'
                    max_d = str(item['max_date'])[:10] if item['max_date'] else 'N/A'
                    print(f"  {identifier:<30} {rows:>12,} {min_d:<12} {max_d:<12}")
            elif coverage.get('distinct_symbols'):
                print(f"\n  Distinct {coverage['symbol_columns'][0]}: {len(coverage['distinct_symbols'])}")
                if len(coverage['distinct_symbols']) <= 20:
                    print(f"  {', '.join(map(str, coverage['distinct_symbols']))}")
                else:
                    print(f"  {', '.join(map(str, coverage['distinct_symbols'][:20]))} ... (and {len(coverage['distinct_symbols'])-20} more)")
        
        # Special queries for key tables
        print(f"\n\n{'='*80}")
        print("KEY TABLES FOR FUTURES-SIX")
        print("="*80 + "\n")
        
        # Spot indices
        print("Spot Indices (f_fred_observations):")
        print("-" * 80)
        try:
            spot_indices = con.execute("""
                SELECT series_id, COUNT(*) as rows, 
                       MIN(date) as first_date, MAX(date) as last_date
                FROM f_fred_observations
                WHERE series_id IN ('SP500', 'NASDAQ100', 'RUT_SPOT')
                GROUP BY series_id
                ORDER BY series_id
            """).fetchdf()
            print(spot_indices.to_string(index=False))
        except Exception as e:
            print(f"  ERROR: {e}")
        
        # Continuous contracts
        print("\n\nContinuous Contract Series (dim_continuous_contract):")
        print("-" * 80)
        try:
            contracts = con.execute("""
                SELECT contract_series, root, roll_rule, description
                FROM dim_continuous_contract
                ORDER BY root, contract_series
            """).fetchdf()
            print(contracts.to_string(index=False))
        except Exception as e:
            print(f"  ERROR: {e}")
        
        # Continuous daily bars coverage
        print("\n\nContinuous Daily Bars Coverage (g_continuous_bar_daily):")
        print("-" * 80)
        try:
            bars_coverage = con.execute("""
                SELECT 
                    contract_series,
                    COUNT(*) as rows,
                    MIN(trading_date) as first_date,
                    MAX(trading_date) as last_date
                FROM g_continuous_bar_daily
                GROUP BY contract_series
                ORDER BY contract_series
            """).fetchdf()
            print(bars_coverage.to_string(index=False))
        except Exception as e:
            print(f"  ERROR: {e}")
        
        # FRED series
        print("\n\nFRED Series (f_fred_observations):")
        print("-" * 80)
        try:
            fred_series = con.execute("""
                SELECT series_id, COUNT(*) as rows,
                       MIN(date) as first_date, MAX(date) as last_date
                FROM f_fred_observations
                GROUP BY series_id
                ORDER BY series_id
            """).fetchdf()
            print(fred_series.to_string(index=False))
        except Exception as e:
            print(f"  ERROR: {e}")
        
    finally:
        con.close()

if __name__ == "__main__":
    main()
