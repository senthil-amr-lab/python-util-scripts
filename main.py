import boto3
import json
from collections import defaultdict
from datetime import datetime
import configparser
import argparse
import sys

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

def get_directories_sizes(bucket_name, objects_sizes):
    directory_sizes = defaultdict(int)
    
    for obj in objects_sizes:
        if obj['objectType'] == 'file':
            directory = '/'.join(obj['objectKey'].split('/')[:-1])
            directory = directory if directory else '/'
            directory_sizes[directory] += obj['size']
    
    # Convert dictionary to list of dictionaries
    directory_sizes_list = [{'bucketName': bucket_name, 'directory': dir_name, 'size': size} for dir_name, size in directory_sizes.items()]
    
    return total_directory_size(directory_sizes_list)

def chunk_list(data, chunk_size):
    """Split a list into chunks of specified size."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def total_directory_size(directories_sizes):

    json_array = []

    for dir_a in directories_sizes:
        for dir_b in directories_sizes:
            if dir_a['directory'] == "/":
                dir_a['size'] += dir_b['size']
            elif dir_b['directory'].startswith(dir_a['directory']):
                dir_a['size'] += dir_b['size']
        json_array.append(dir_a)

    return json_array
                

def get_sizes_for_buckets(session, bucket_names, output_bucket, output_directory, chunk_size):
    bucket_list = bucket_names.split(',')
    sizes = {}

    for bucket in bucket_list:
        print(f"Getting sizes for bucket '{bucket}'")
        bucket = bucket.strip()
        sizes[bucket] = {
            "bucket-name": bucket,
            "objects_sizes": get_s3_objects_size(session, bucket)
        }

        directories_sizes = get_directories_sizes(bucket, sizes[bucket]['objects_sizes'])
        
        # Split directories_sizes into chunks and write each chunk to S3
        for idx, chunk in enumerate(chunk_list(directories_sizes, chunk_size)):
            chunk_data = json.dumps(chunk)
            write_sizes_to_s3(session, bucket, output_bucket, output_directory, chunk_data, idx)
            print(f"Result chunk {idx} for bucket '{bucket}' written to S3")

def write_sizes_to_s3(session, bucket_name, output_bucket, output_directory, data, chunk_index):
    s3 = session.client('s3')

    # Get current date in dd-mm-yyyy format
    current_date = datetime.now().strftime("%d-%m-%Y")
    output_key_prefix = f"{output_directory}/{current_date}/{bucket_name}"
    output_key = f"{output_key_prefix}_chunk_{chunk_index}.json"
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=data)

    output_key_prefix = f"{output_directory}/latest/{bucket_name}"
    output_key = f"{output_key_prefix}_chunk_{chunk_index}.json"
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=data)

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
    chunk_size = int(config['buckets']['chunk_size'] )

    session = initialize_aws_connection(profile_name)
    get_sizes_for_buckets(session, bucket_names, output_bucket, output_directory, chunk_size)
    print(f"Export of directories sizes for buckets '{bucket_names}' completed successfully")