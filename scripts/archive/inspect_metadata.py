import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def list_metadata():
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1'),
    )
    table = dynamodb.Table(os.getenv('DYNAMODB_METADATA_TABLE', 'CourseMetadata'))
    
    try:
        response = table.scan(Limit=10)
        items = response.get('Items', [])
        print(f"Found {len(items)} items in {table.table_name}:")
        for item in items:
            print(f"- CourseID: {item.get('course_id')} | Code: {item.get('course_code')} | University: {item.get('university_id')}")
    except Exception as e:
        print(f"Error scanning table: {str(e)}")

if __name__ == "__main__":
    list_metadata()
