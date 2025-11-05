"""
Transform and ingest all downloaded continuous futures parquet files.
"""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.continuous_transform import transform_continuous_to_folder_structure
from pipelines.common import get_paths
from pipelines.loader import load, apply_gold_sql
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PRODUCT = "ES_CONTINUOUS_MDP3"
ROLL_RULE = "2_days_pre_expiry"

def main():
    raw_dir, _, db_path = get_paths()
    
    # Find all downloaded parquet files
    parquet_files = sorted(raw_dir.glob('glbx-mdp3-2025-*.bbo-1m.fullday.parquet'))
    
    if not parquet_files:
        logger.info("No parquet files found to transform")
        return 0
    
    logger.info(f'Found {len(parquet_files)} parquet files to transform and ingest')
    
    success_count = 0
    processed_dates = set()
    
    # First, ingest all existing transformed directories
    logger.info(f'Checking for existing transformed directories...')
    for transformed_dir in sorted(raw_dir.glob('glbx-mdp3-*')):
        if transformed_dir.is_dir():
            # Extract date from directory name
            dir_name = transformed_dir.name.replace('glbx-mdp3-', '')
            # Check if it has the required subdirectories
            if (transformed_dir / 'continuous_instruments').exists() and (transformed_dir / 'continuous_quotes_l1').exists():
                try:
                    date_str = dir_name
                    logger.info(f'Ingesting existing directory: {transformed_dir.name}')
                    load(PRODUCT, transformed_dir, date=date_str)
                    processed_dates.add(date_str)
                    success_count += 1
                except Exception as e:
                    logger.error(f'Error ingesting {transformed_dir.name}: {e}')
    
    # Now transform and ingest remaining parquet files
    for i, pq_file in enumerate(parquet_files, 1):
        try:
            # Extract date from filename: glbx-mdp3-2025-01-01.bbo-1m.fullday.parquet
            date_str = pq_file.stem.split('.')[0].replace('glbx-mdp3-', '')
            
            # Skip if already processed
            if date_str in processed_dates:
                continue
            
            logger.info(f'[{i}/{len(parquet_files)}] Processing {pq_file.name}')
            
            # Transform to folder structure
            output_dir = raw_dir / f'glbx-mdp3-{date_str}'
            transform_continuous_to_folder_structure(pq_file, output_dir, roll_rule=ROLL_RULE)
            
            # Ingest into database
            load(PRODUCT, output_dir, date=date_str)
            
            processed_dates.add(date_str)
            success_count += 1
            
            if i % 10 == 0:
                logger.info(f'Progress: {i}/{len(parquet_files)} files processed, {success_count} total ingested')
                
        except Exception as e:
            logger.error(f'Error processing {pq_file.name}: {e}')
            import traceback
            traceback.print_exc()
            continue
    
    logger.info(f'Transformation and ingestion complete: {success_count}/{len(parquet_files)} files processed')
    
    # Build gold layer (1-minute bars)
    logger.info('Building gold layer...')
    try:
        apply_gold_sql(PRODUCT)
        logger.info('Gold layer built successfully')
    except Exception as e:
        logger.error(f'Error building gold layer: {e}')
    
    logger.info('All done!')
    return 0

if __name__ == "__main__":
    sys.exit(main())

