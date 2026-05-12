import boto3, os

s3_bucket = 'uhub-lms-bucket'
region = 'us-east-2'

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=region
)

# List objects in WBU/ folder for the newly ingested course
prefix = 'WBU/'
resp = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix, MaxKeys=50)
if 'Contents' in resp:
    print(f'Objects in {s3_bucket}/{prefix}:')
    for obj in sorted(resp['Contents'], key=lambda x: x['LastModified'], reverse=True):
        print(f'  {obj["Key"]}  ({obj["Size"]} bytes)')
    print(f'\nTotal: {len(resp["Contents"])} objects (showing up to 50)')
else:
    print(f'No objects found under {prefix}')
