"""
Analyze downloaded ES options data for quality and completeness.

Usage:
    python scripts/analyze_data.py
"""
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Import analyzer
from src.validation.data_analyzer import main as analyze_main

if __name__ == "__main__":
    sys.exit(analyze_main())

