"""
Verify and complete ingestion of all continuous futures data.
"""
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.continuous_transform import transform_continuous_to_folder_structure
from pipelines.common import get_paths, connect_duckdb
from pipelines.loader import load, apply_gold_sql
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PRODUCT = "ES_CONTINUOUS_MDP3"
ROLL_RULE = "2_days_pre_expiry"

def main():
    raw_dir, _, db_path = get_paths()
    
    # Get all parquet files
    parquet_files = sorted(raw_dir.glob('glbx-mdp3-2025-*.bbo-1m.fullday.parquet'))
    logger.info(f'Found {len(parquet_files)} parquet files')
    
    # Get dates already in database
    con = connect_duckdb(db_path)
    db_dates = set(con.execute('''
        SELECT DISTINCT CAST(ts_event AS DATE) as date
        FROM f_continuous_quote_l1
        WHERE CAST(ts_event AS DATE) IS NOT NULL
    ''').fetchdf()['date'].tolist())
    con.close()
    logger.info(f'Dates already in database: {len(db_dates)}')
    
    # Extract dates from parquet files
    parquet_dates = {}
    for f in parquet_files:
        date_str = f.stem.split('.')[0].replace('glbx-mdp3-', '')
        try:
            d = datetime.strptime(date_str, '%Y-%m-%d').date()
            parquet_dates[d] = f
        except:
            pass
    
    # Find missing dates
    missing_dates = set(parquet_dates.keys()) - db_dates
    logger.info(f'Missing dates: {len(missing_dates)}')
    
    if not missing_dates:
        logger.info('All dates are already in database!')
        return 0
    
    # Process missing dates - transform ALL parquet files first, then ingest
    success_count = 0
    
    # First, transform all parquet files that need transformation
    logger.info(f'Transforming {len(parquet_files)} parquet files...')
    for i, pq_file in enumerate(parquet_files, 1):
        try:
            date_str = pq_file.stem.split('.')[0].replace('glbx-mdp3-', '')
            output_dir = raw_dir / f'glbx-mdp3-{date_str}'
            quotes_file = output_dir / 'continuous_quotes_l1' / f'{date_str}.parquet'
            
            if not output_dir.exists() or not quotes_file.exists():
                if i % 50 == 0:
                    logger.info(f'  Transforming [{i}/{len(parquet_files)}]: {date_str}')
                transform_continuous_to_folder_structure(pq_file, output_dir, roll_rule=ROLL_RULE)
        except Exception as e:
            logger.error(f'Error transforming {pq_file.name}: {e}')
            continue
    
    logger.info(f'Transformation complete. Now ingesting {len(missing_dates)} missing dates...')
    
    # Now ingest all missing dates
    for i, date_val in enumerate(sorted(missing_dates), 1):
        try:
            date_str = date_val.strftime('%Y-%m-%d')
            output_dir = raw_dir / f'glbx-mdp3-{date_str}'
            
            if not output_dir.exists():
                logger.warning(f'  [{i}/{len(missing_dates)}] {date_str}: No transformed directory, skipping')
                continue
            
            if i % 20 == 0:
                logger.info(f'  Ingesting [{i}/{len(missing_dates)}]: {date_str}')
            
            load(PRODUCT, output_dir, date=date_str)
            success_count += 1
                
        except Exception as e:
            logger.error(f'Error ingesting {date_val}: {e}')
            import traceback
            traceback.print_exc()
            continue
    
    logger.info(f'Completed: {success_count}/{len(missing_dates)} dates processed')
    
    # Build gold layer
    logger.info('Building gold layer...')
    try:
        apply_gold_sql(PRODUCT)
        logger.info('Gold layer built successfully')
    except Exception as e:
        logger.error(f'Error building gold layer: {e}')
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

