import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

class MinioClient:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.s3 = boto3.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

    def bucket_exists(self, bucket_name):
        return any(bucket.name == bucket_name for bucket in self.s3.buckets.all())

    def create_bucket(self, bucket_name):
        try:
            self.s3.create_bucket(Bucket=bucket_name)
        except (BotoCoreError, ClientError) as error:
            print(f"Error creating bucket {bucket_name}: {error}")
            return False
        return True

    def save_file(self, file_name, bucket_name, object_name=None):
        if not self.bucket_exists(bucket_name):
            self.create_bucket(bucket_name)
        if object_name is None:
            object_name = file_name
        self.s3.Bucket(bucket_name).upload_file(Filename=file_name, Key=object_name)

    def get_file(self, bucket_name, object_name):
        self.s3.Bucket(bucket_name).download_file(Key=object_name, Filename=f'downloaded_{object_name}')