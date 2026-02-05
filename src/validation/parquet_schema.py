"""
Parquet schema validation utilities.

Provides functions to validate parquet file columns and types
before ingestion into the database.
"""
from pathlib import Path
from typing import Dict, List

import pyarrow.parquet as pq


def validate_parquet_columns(path: Path, required: List[str]) -> List[str]:
    """
    Validate that a parquet file contains all required columns.
    
    Args:
        path: Path to the parquet file
        required: List of required column names
    
    Returns:
        List of missing column names (empty if all required columns present)
    """
    schema = pq.read_schema(path)
    existing_columns = set(schema.names)
    missing = [col for col in required if col not in existing_columns]
    return missing


def validate_parquet_types(path: Path, expected_types: Dict[str, str]) -> List[str]:
    """
    Validate that parquet file columns have expected types.
    
    Args:
        path: Path to the parquet file
        expected_types: Dictionary mapping column names to expected Arrow type strings
            (e.g., {"close": "double", "volume": "int64", "trading_date": "date32[day]"})
    
    Returns:
        List of columns with wrong types, formatted as "column: expected X, got Y"
    """
    schema = pq.read_schema(path)
    schema_dict = {field.name: str(field.type) for field in schema}
    
    wrong_types = []
    for col, expected_type in expected_types.items():
        if col not in schema_dict:
            # Column missing - handled by validate_parquet_columns
            continue
        actual_type = schema_dict[col]
        if actual_type != expected_type:
            wrong_types.append(f"{col}: expected {expected_type}, got {actual_type}")
    
    return wrong_types
