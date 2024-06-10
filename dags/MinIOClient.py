import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from boto3.s3.transfer import TransferConfig
import os

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
         
        # Configuración para la carga multipartes
        config = TransferConfig(multipart_threshold=1024**2, max_concurrency=10,
                                multipart_chunksize=1024**2, use_threads=True)

        self.s3.Bucket(bucket_name).upload_file(Filename=file_name, Key=object_name, Config=config)


    def get_file(self, bucket_name, object_name):
        print(f"Downloading {object_name} from {bucket_name} bucket...")
        filename = f'downloaded_{object_name}'        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.s3.Bucket(bucket_name).download_file(Key=object_name, Filename=filename)
        print(f"File downloaded to {filename}")