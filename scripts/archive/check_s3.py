import boto3
import os

s3_bucket = 'uhub-lms-bucket'
region = 'us-east-2'

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=region
)

try:
    response = s3.head_bucket(Bucket=s3_bucket)
    print(f'Bucket {s3_bucket} is accessible')
    region_header = response.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amz-bucket-region', 'unknown')
    print(f'Region: {region_header}')
except Exception as e:
    print(f'Bucket access error: {e}')

try:
    resp = s3.list_objects_v2(Bucket=s3_bucket, MaxKeys=10)
    if 'Contents' in resp:
        print(f'\nRecent objects in {s3_bucket}:')
        for obj in sorted(resp['Contents'], key=lambda x: x['LastModified'], reverse=True)[:10]:
            print(f'  {obj["Key"]}  ({obj["Size"]} bytes)  {obj["LastModified"]}')
        print(f'Total objects (up to 1000): {resp.get("KeyCount",0)}')
    else:
        print('\nBucket is empty (no objects)')
except Exception as e:
    print(f'List error: {e}')
