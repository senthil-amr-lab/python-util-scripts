import unittest
from unittest.mock import patch, MagicMock
import json
import boto3  # Add this import
from main import get_sizes_for_buckets

class TestGetSizesForBuckets(unittest.TestCase):

    @patch('main.write_sizes_to_s3')
    @patch('main.get_directories_sizes')
    @patch('main.get_s3_objects_size')
    @patch('boto3.Session.client')
    def test_get_sizes_for_buckets(self, mock_client, mock_get_s3_objects_size, mock_get_directories_sizes, mock_write_sizes_to_s3):
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3

        # Mock return values for get_s3_objects_size and get_directories_sizes
        mock_get_s3_objects_size.return_value = [
            {'bucketName': 'bucket1', 'objectKey': 'file1.txt', 'size': 123, 'sizeType': 'bytes', 'objectType': 'file'}
        ]
        mock_get_directories_sizes.return_value = [
            {'bucketName': 'bucket1', 'directory': '/', 'size': 123}
        ]

        session = boto3.Session()
        bucket_names = 'bucket1'
        output_bucket = 'output_bucket'
        output_directory = 'output_directory'
        chunk_size = 2

        get_sizes_for_buckets(session, bucket_names, output_bucket, output_directory, chunk_size)

        # Verify get_s3_objects_size was called with the correct parameters
        mock_get_s3_objects_size.assert_called_once_with(session, 'bucket1')

        # Verify get_directories_sizes was called with the correct parameters
        mock_get_directories_sizes.assert_called_once_with('bucket1', mock_get_s3_objects_size.return_value)

        # Verify write_sizes_to_s3 was called with the correct parameters
        mock_write_sizes_to_s3.assert_called_once_with(
            session,
            'bucket1',
            output_bucket,
            output_directory,
            json.dumps(mock_get_directories_sizes.return_value),
            0
        )

if __name__ == '__main__':
    unittest.main()