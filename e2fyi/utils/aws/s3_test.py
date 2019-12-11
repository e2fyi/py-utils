"""Unit test for s3 bucket."""
import unittest

from unittest.mock import MagicMock

import boto3

from e2fyi.utils.aws.s3 import S3Bucket


class S3BucketTest(unittest.TestCase):
    """TestCase for S3Bucket"""

    def setUp(self):
        s3client = boto3.client("s3")
        s3client.upload_fileobj = MagicMock()  # type: ignore
        self.s3client = s3client

    def test_basic(self):
        bucket = S3Bucket(
            "bucketname", get_prefix=lambda filename: "prefix/%s" % filename
        )
        self.assertEqual(bucket.name, "bucketname")
        self.assertEqual(bucket.prefix, "prefix/")

    def test_create_resource_key(self):
        bucket = S3Bucket(name="bucket", get_prefix=lambda x: "folder/%s" % x)
        key = bucket.create_resource_key("filename.ext")

        self.assertEqual(key, "folder/filename.ext")

    def test_create_resource_uri(self):
        bucket = S3Bucket(name="bucket", get_prefix=lambda x: "folder/%s" % x)
        key = bucket.create_resource_uri("filename.ext")

        self.assertEqual(key, "s3a://bucket/folder/filename.ext")
