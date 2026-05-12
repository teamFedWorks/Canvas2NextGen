import sys
sys.path.insert(0, 'src')
from pathlib import Path
import json

# Read current report
report_path = Path('storage/outputs/WBU/validation_leadership-management-development-spring-1st8wks-2026-vc01.json')
data = json.loads(report_path.read_text())

print('Current featuredImage:', data.get('featuredImage', 'N/A'))
print('S3 bucket in URL?', 's3' in data.get('featuredImage', '').lower())

# Get course from MongoDB to check featuredImage
from pymongo import MongoClient
import os

uri = os.getenv('MONGODB_URI')
client = MongoClient(uri)
db = client['course-db']
collection = db['courses']

course_doc = collection.find_one({'slug': data.get('slug')})
if course_doc:
    current_img = course_doc.get('featuredImage', '')
    print('Current DB featuredImage:', current_img[:100] if current_img else 'None')
    
    # Clear S3 URL from featuredImage (set to empty or placeholder)
    if current_img and ('s3.amazonaws.com' in current_img or 'uhub-lms-bucket' in current_img):
        print('S3 URL detected — would clear this field')
        # Update in DB
        collection.update_one(
            {'_id': course_doc['_id']},
            {'$set': {'featuredImage': ''}}
        )
        print('Cleared featuredImage in MongoDB')
    else:
        print('No S3 URL in featuredImage — already clean')
else:
    print('Course not found in DB')
