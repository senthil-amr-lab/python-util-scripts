import unittest
import json
from unittest.mock import patch, MagicMock
from main import (
    initialize_aws_connection,
    get_s3_objects_size,
    get_sizes_for_buckets,
    get_directory_size,
    get_all_directories_size,
    write_sizes_to_s3
)
import boto3

class TestMain(unittest.TestCase):

    @patch('boto3.Session')
    def test_initialize_aws_connection(self, mock_session):
        mock_session.return_value = MagicMock()
        session = initialize_aws_connection('fake_access_key', 'fake_secret_key', 'fake_region')
        self.assertIsNotNone(session)
        mock_session.assert_called_once_with(
            aws_access_key_id='fake_access_key',
            aws_secret_access_key='fake_secret_key',
            region_name='fake_region'
        )

    @patch('boto3.Session.client')
    def test_get_s3_objects_size(self, mock_client):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'file1.txt', 'Size': 123},
                {'Key': 'folder/', 'Size': 0},
                {'Key': 'folder/file2.txt', 'Size': 456}
            ]
        }
        session = boto3.Session()
        sizes = get_s3_objects_size(session, 'fake_bucket')
        expected_sizes = {
            'file1.txt': {'size': 123, 'size-type': 'bytes', 'object-type': 'file'},
            'folder/': {'size': 0, 'size-type': 'bytes', 'object-type': 'directory'},
            'folder/file2.txt': {'size': 456, 'size-type': 'bytes', 'object-type': 'file'}
        }
        self.assertEqual(sizes, expected_sizes)

    @patch('main.get_s3_objects_size')
    def test_get_sizes_for_buckets(self, mock_get_s3_objects_size):
        mock_get_s3_objects_size.return_value = {'file1.txt': {'size': 123, 'size-type': 'bytes', 'object-type': 'file'}}
        session = boto3.Session()
        sizes = get_sizes_for_buckets(session, 'bucket1,bucket2')
        expected_sizes = {
            'bucket1': {'bucket-name': 'bucket1', 'objects': {'file1.txt': {'size': 123, 'size-type': 'bytes', 'object-type': 'file'}}},
            'bucket2': {'bucket-name': 'bucket2', 'objects': {'file1.txt': {'size': 123, 'size-type': 'bytes', 'object-type': 'file'}}}
        }
        self.assertEqual(sizes, expected_sizes)

    @patch('boto3.Session.client')
    def test_get_directory_size(self, mock_client):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {'Contents': [{'Key': 'folder/file1.txt', 'Size': 123}, {'Key': 'folder/file2.txt', 'Size': 456}]}
        ]
        session = boto3.Session()
        size = get_directory_size(session, 'fake_bucket', 'folder/')
        expected_size = {'size': 579, 'size-type': 'bytes'}
        self.assertEqual(size, expected_size)

    @patch('boto3.Session.client')
    def test_get_all_directories_size(self, mock_client):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {'Contents': [{'Key': 'folder1/file1.txt', 'Size': 123}, {'Key': 'folder2/file2.txt', 'Size': 456}]}
        ]
        session = boto3.Session()
        sizes = get_all_directories_size(session, 'fake_bucket')
        expected_sizes = json.dumps({
            'folder1/': {'size': 123, 'size-type': 'bytes'},
            'folder2/': {'size': 456, 'size-type': 'bytes'}
        }, indent=4)
        self.assertEqual(sizes, expected_sizes)

    @patch('boto3.Session.client')
    @patch('main.get_sizes_for_buckets')
    def test_write_sizes_to_s3(self, mock_get_sizes_for_buckets, mock_client):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_get_sizes_for_buckets.return_value = {
            'bucket1': {'bucket-name': 'bucket1', 'objects': {'file1.txt': {'size': 123, 'size-type': 'bytes', 'object-type': 'file'}}}
        }
        session = boto3.Session()
        write_sizes_to_s3(session, 'bucket1', 'output_bucket', 'output_directory')
        mock_s3.put_object.assert_called_once()

if __name__ == '__main__':
    unittest.main()