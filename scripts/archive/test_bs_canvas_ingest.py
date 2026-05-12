"""
Use Windows extended-length path prefix to bypass space restrictions.
Processes the BS Computer Science PHI-1114 course directly.
"""
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

# Use extended-length path prefix
zip_path = r'\\?\B:\EduvateHub\CourseOnboarding\storage\uploads\BS_Computer_Science\01 - PHI-1114 Logic and Argumentation.zip'
print(f'Attempting ingestion with extended path prefix...')
print(f'Path: {zip_path}')

worker = IngestionWorker(
    s3_bucket=os.getenv('S3_CDN_BUCKET'),
    cdn_url=os.getenv('CDN_URL')
)

result = worker.ingest(
    source_type='zip',
    payload={
        'zip_path': zip_path,
        'university_id': '69be64cd355271ea5c3da6b7',
        'author_id': '69be9af5f30e4168f886ac50',
        'institution': 'SFC',
        'force': True,
    },
    task_id='test-bs-phi-1114-canvas'
)
print('\n=== RESULT ===')
print(result)
