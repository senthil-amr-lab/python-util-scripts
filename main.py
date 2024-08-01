import boto3
import json
from collections import defaultdict
from datetime import datetime
import configparser
import argparse

def initialize_aws_connection(profile_name):
    session = boto3.Session(profile_name=profile_name)
    return session


def get_s3_objects_size(session, bucket_name):
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)
    
    sizes = []
    for page in pages:
        for obj in page.get('Contents', []):
            sizes.append({
                'bucketName': bucket_name,
                'objectKey': obj['Key'],
                'size': obj['Size'],
                'sizeType': 'bytes',
                'objectType': 'directory' if obj['Key'].endswith('/') else 'file'
            })
    return sizes


def get_sizes_for_buckets(session, bucket_names):
    bucket_list = bucket_names.split(',')
    sizes = {}

    for bucket in bucket_list:
        bucket = bucket.strip()
        sizes[bucket] = {
            "bucket-name": bucket,
            "objects_sizes": get_s3_objects_size(session, bucket)
        }

    return sizes


def write_sizes_to_s3(session, bucket_names, output_bucket, output_directory):
    sizes = get_sizes_for_buckets(session, bucket_names)
    s3 = session.client('s3')

    # Get current date in dd-mm-yyyy format
    current_date = datetime.now().strftime("%d-%m-%Y")

    for bucket, data in sizes.items():
        output_key = f"{output_directory}/{current_date}/{bucket}.json"
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=json.dumps(data, indent=4))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process environment parameter.')
    parser.add_argument('--env', required=True, help='Environment name (e.g., dev, prod)')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(f'config-{args.env}.ini')

    profile_name = config['aws']['profile_name']
    region_name = config['aws']['region_name']
    bucket_names = config['buckets']['bucket_names']
    output_bucket = config['buckets']['output_bucket']
    output_directory = config['buckets']['output_directory']

    session = initialize_aws_connection(profile_name)
    write_sizes_to_s3(session, bucket_names, output_bucket, output_directory)
    print(f"Sizes for buckets '{bucket_names}' have been written to '{output_bucket}/{output_directory}'")