import boto3
import json
from collections import defaultdict
from datetime import datetime
import configparser

def initialize_aws_connection(aws_access_key_id, aws_secret_access_key, region_name):
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    return session

def get_s3_objects_size(session, bucket_name):
    s3 = session.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name)

    objects_size = {}

    if 'Contents' in response:
        for obj in response['Contents']:
            objects_size[obj['Key']] = {
                "size": obj['Size'],
                "size-type": "bytes",
                "object-type": "directory" if obj['Key'].endswith('/') else "file"
            }

    return objects_size

def get_sizes_for_buckets(session, bucket_names):
    bucket_list = bucket_names.split(',')
    sizes = {}

    for bucket in bucket_list:
        bucket = bucket.strip()
        sizes[bucket] = {
            "bucket-name": bucket,
            "objects": get_s3_objects_size(session, bucket)
        }

    return sizes

def get_directory_size(session, bucket_name, directory_prefix):
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=directory_prefix)

    total_size = 0

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']

    return {
        "size": total_size,
        "size-type": "bytes"
    }

def get_all_directories_size(session, bucket_name):
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    directory_sizes = defaultdict(int)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if '/' in key:
                    directory = key.split('/')[0] + '/'
                    directory_sizes[directory] += obj['Size']
                else:
                    directory_sizes['root/'] += obj['Size']

    # Convert sizes to the desired format
    directory_sizes_with_type = {k: {"size": v, "size-type": "bytes"} for k, v in directory_sizes.items()}

    return json.dumps(directory_sizes_with_type, indent=4)

def write_sizes_to_s3(session, bucket_names, output_bucket, output_directory):
    sizes = get_sizes_for_buckets(session, bucket_names)
    s3 = session.client('s3')

    # Get current date in dd-mm-yyyy format
    current_date = datetime.now().strftime("%d-%m-%Y")

    for bucket, data in sizes.items():
        output_key = f"{output_directory}/{current_date}/{bucket}.json"
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=json.dumps(data, indent=4))

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    profile_name = config['aws']['profile_name']
    region_name = config['aws']['region_name']
    bucket_names = config['buckets']['bucket_names']
    output_bucket = config['buckets']['output_bucket']
    output_directory = config['buckets']['output_directory']

    session = initialize_aws_connection(profile_name, region_name)
    write_sizes_to_s3(session, bucket_names, output_bucket, output_directory)
    print(f"Sizes for buckets '{bucket_names}' have been written to '{output_bucket}/{output_directory}'")