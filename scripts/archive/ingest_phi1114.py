import sys
sys.path.insert(0, 'src')

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

from worker.ingestion_worker import IngestionWorker
from dotenv import load_dotenv
import os

load_dotenv('.env')

os.environ['DEFAULT_UNIVERSITY_ID'] = '69be64cd355271ea5c3da6b7'
os.environ['DEFAULT_AUTHOR_ID'] = '69be9af5f30e4168f886ac50'

worker = IngestionWorker(
    s3_bucket=os.getenv('S3_CDN_BUCKET'),
    cdn_url=os.getenv('CDN_URL')
)

print('=== Ingesting PHI-1114 Canvas course ===')
result = worker.ingest(
    source_type='zip',
    payload={
        'zip_path': 'storage/uploads/BS_Computer_Science/01_-_PHI-1114_Logic_and_Argumentation.zip',
        'university_id': '69be64cd355271ea5c3da6b7',
        'author_id': '69be9af5f30e4168f886ac50',
        'institution': 'SFC',
        'force': True,
    },
    task_id='phi1114-canvas'
)

print('\n=== RESULT ===')
print(result)
