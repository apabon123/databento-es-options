"""
Analyze downloaded ES options data for quality and completeness.

Usage:
    python scripts/analysis/analyze_data.py
"""
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.env import load_env

load_env()

# Import analyzer
from src.validation.data_analyzer import main as analyze_main

if __name__ == "__main__":
    sys.exit(analyze_main())

