import sys
sys.path.insert(0, 'src')

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

import os
extracted_dir = 'storage/uploads/BS_Computer_Science/01_-_PHI-1114_Logic_and_Argumentation'
print('Path exists?', os.path.exists(extracted_dir))
print('Is dir?', os.path.isdir(extracted_dir))
if not os.path.exists(extracted_dir):
    print('ERROR: path not found')
    sys.exit(1)

from worker.ingestion_worker import IngestionWorker
from dotenv import load_dotenv

load_dotenv('.env')

os.environ['DEFAULT_UNIVERSITY_ID'] = '69be64cd355271ea5c3da6b7'
os.environ['DEFAULT_AUTHOR_ID'] = '69be9af5f30e4168f886ac50'

worker = IngestionWorker(
    s3_bucket=os.getenv('S3_CDN_BUCKET'),
    cdn_url=os.getenv('CDN_URL')
)

print(f'Ingesting from: {extracted_dir}')
result = worker.ingest(
    source_type='zip',
    payload={
        'zip_path': extracted_dir,
        'university_id': '69be64cd355271ea5c3da6b7',
        'author_id': '69be9af5f30e4168f886ac50',
        'institution': 'SFC',
        'force': True,
    },
    task_id='phi1114-canvas-extracted'
)
print('\n=== RESULT ===')
print(result)
