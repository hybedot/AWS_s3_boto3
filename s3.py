import os 

import boto3
from dotenv import load_dotenv
import uuid

load_dotenv()

def aws_session(region_name='us-east-1'):
     return boto3.session.Session(region_name=region_name)

session = aws_session()
s3_object = session.resource('s3')


def create_bucket_name(bucket_prefix='data'):
    return ''.join([bucket_prefix, '-', str(uuid.uuid4())])
    

def create_bucket():    
    bucket_name = create_bucket_name()
    return s3_object.create_bucket(Bucket=bucket_name)


def list_all_buckets():
    for bucket in  s3_object.buckets.all():
        print (bucket.name)


def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name

first_file_name = create_temp_file(300, 'firstfile.txt', 'f')   


def upload_file_to_bucket(bucket_name, file_path=first_file_name):
    file_dir, file_name = os.path.split(file_path)

    bucket = s3_object.Bucket(bucket_name)
    bucket.upload_file(
      Filename=file_path,
      Key=file_name,
      ExtraArgs={'ACL': 'public-read'}
    )

    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
    return s3_url

def download_file_from_bucket(bucket_name, s3_key, dst_path):
    bucket = s3_object.Bucket(bucket_name)
    bucket.download_file(Key=s3_key, Filename=dst_path)

