import unittest
import json
from unittest.mock import patch, MagicMock
from main import (
    initialize_aws_connection,
    get_s3_objects_size,
    get_sizes_for_buckets,
    write_sizes_to_s3
)
import boto3

class TestMain(unittest.TestCase):

    @patch('boto3.Session')
    def test_initialize_aws_connection(self, mock_session):
        mock_session.return_value = MagicMock()
        session = initialize_aws_connection('dev-user')
        self.assertIsNotNone(session)
        mock_session.assert_called_once_with(profile_name='dev-user')

    @patch('boto3.Session.client')
    def test_get_s3_objects_size(self, mock_client):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {'Contents': [
                {'Key': 'file1.txt', 'Size': 123},
                {'Key': 'folder/', 'Size': 0},
                {'Key': 'folder/file2.txt', 'Size': 456}
            ]}
        ]
        session = boto3.Session()
        sizes = get_s3_objects_size(session, 'fake_bucket')
        expected_sizes = [
            {'bucketName': 'fake_bucket', 'objectKey': 'file1.txt', 'size': 123, 'sizeType': 'bytes', 'objectType': 'file'},
            {'bucketName': 'fake_bucket', 'objectKey': 'folder/', 'size': 0, 'sizeType': 'bytes', 'objectType': 'directory'},
            {'bucketName': 'fake_bucket', 'objectKey': 'folder/file2.txt', 'size': 456, 'sizeType': 'bytes', 'objectType': 'file'}
        ]
        self.assertEqual(sizes, expected_sizes)

    @patch('main.get_s3_objects_size')
    def test_get_sizes_for_buckets(self, mock_get_s3_objects_size):
        mock_get_s3_objects_size.return_value = [
            {'bucketName': 'bucket1', 'objectKey': 'file1.txt', 'size': 123, 'sizeType': 'bytes', 'objectType': 'file'}
        ]
        session = boto3.Session()
        sizes = get_sizes_for_buckets(session, 'bucket1,bucket2')
        expected_sizes = {
            'bucket1': {
                'bucket-name': 'bucket1',
                'objects_sizes': [
                    {'bucketName': 'bucket1', 'objectKey': 'file1.txt', 'size': 123, 'sizeType': 'bytes', 'objectType': 'file'}
                ]
            },
            'bucket2': {
                'bucket-name': 'bucket2',
                'objects_sizes': [
                    {'bucketName': 'bucket1', 'objectKey': 'file1.txt', 'size': 123, 'sizeType': 'bytes', 'objectType': 'file'}
                ]
            }
        }
        self.assertEqual(sizes, expected_sizes)


    @patch('boto3.Session.client')
    @patch('main.get_sizes_for_buckets')
    def test_write_sizes_to_s3(self, mock_get_sizes_for_buckets, mock_client):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_get_sizes_for_buckets.return_value = {
            'bucket1': {
                'bucket-name': 'bucket1',
                'objects_sizes': [
                    {'bucketName': 'bucket1', 'objectKey': 'file1.txt', 'size': 123, 'sizeType': 'bytes', 'objectType': 'file'}
                ]
            }
        }
        session = boto3.Session()
        write_sizes_to_s3(session, 'bucket1', 'output_bucket', 'output_directory')
        mock_s3.put_object.assert_called_once()

    def test_new_function(self):
        # Add your new test function here
        pass

if __name__ == '__main__':
    unittest.main()