"""Unit tests for s3 resources"""
import io
import json
import unittest

from unittest.mock import MagicMock

import boto3

from pydantic import BaseModel  # pylint: disable=no-name-in-module

from e2fyi.utils.aws.s3_stream import S3Stream
from e2fyi.utils.aws.s3_resource import S3Resource


class S3ResourceTest(unittest.TestCase):
    """TestCase for S3Resource"""

    def test_basic_ok(self):
        data = {"key1": "foo", "key2": "bar"}
        data_str = json.dumps(data)
        s3stream = S3Stream(io.StringIO(data_str))
        resource = S3Resource(
            "filename.ext",
            content_type="application/json",
            prefix="prefix/",
            bucketname="bucketname",
            protocol="protocol://",
            stream=s3stream,
            Metadata={"tag": "metadata"},
        )

        self.assertEqual(resource.key, "prefix/filename.ext")
        self.assertEqual(resource.uri, "protocol://bucketname/prefix/filename.ext")
        self.assertEqual(resource.content_type, "application/json")
        self.assertEqual(resource.stream, s3stream)
        self.assertDictEqual(resource.extra_args, {"Metadata": {"tag": "metadata"}})
        self.assertEqual(resource.read(), data_str)
        self.assertDictEqual(resource.load(), data)

    def test_full_path(self):
        data = {"key1": "foo", "key2": "bar"}
        data_str = json.dumps(data)
        s3stream = S3Stream(io.StringIO(data_str))
        resource = S3Resource(
            "subprefix/filename.ext",
            content_type="application/json",
            prefix="prefix/",
            bucketname="bucketname",
            protocol="protocol://",
            stream=s3stream,
        )

        self.assertEqual(resource.filename, "filename.ext")
        self.assertEqual(resource.prefix, "prefix/subprefix/")
        self.assertEqual(resource.key, "prefix/subprefix/filename.ext")
        self.assertEqual(
            resource.uri, "protocol://bucketname/prefix/subprefix/filename.ext"
        )

    def test_not_s3_stream(self):
        data = {"key1": "foo", "key2": "bar"}
        data_str = json.dumps(data)
        resource = S3Resource(
            "filename.ext",
            content_type="application/json",
            prefix="prefix/",
            bucketname="bucketname",
            protocol="protocol://",
            stream=data_str,
        )

        self.assertTrue(isinstance(resource.stream, S3Stream))
        self.assertEqual(resource.content_type, "application/json")
        self.assertEqual(resource.read(), data_str)
        self.assertDictEqual(resource.load(), data)

    def test_load_dict(self):
        data = {"key1": "foo", "key2": "bar"}
        data_str = json.dumps(data)
        resource = S3Resource(
            "filename.ext",
            content_type="application/json",
            prefix="prefix/",
            bucketname="bucketname",
            protocol="protocol://",
            stream=S3Stream(io.StringIO(data_str)),
        )

        # pass in a transform func and do not unpack content
        transformed = resource.load(
            lambda content: {**content, "extra": True}, unpack=False
        )
        self.assertDictEqual(transformed, {"key1": "foo", "key2": "bar", "extra": True})

        # pass in a basemodel and unpack content
        class DummyClass(BaseModel):
            """dummy class"""

            key1: str
            key2: str

        dummy = resource.load(DummyClass)
        self.assertIsInstance(dummy, DummyClass)
        self.assertDictEqual(dummy.dict(), data)

    def test_load_list(self):
        data = ["a", "b", "c"]
        data_str = json.dumps(data)
        resource = S3Resource(
            "filename.ext",
            content_type="application/json",
            stream=S3Stream(io.StringIO(data_str)),
        )

        # pass in a transform func and do not unpack content
        transformed = resource.load(lambda content: content + ["d"], unpack=False)
        self.assertListEqual(transformed, ["a", "b", "c", "d"])
        # pass in a transform func and unpack content
        transformed = resource.load(lambda a, b, c: "%s:%s:%s" % (a, b, c), unpack=True)
        self.assertEqual(transformed, "a:b:c")

    def test_save(self):
        data = {"key1": "foo", "key2": "bar"}
        data_str = json.dumps(data)
        s3stream = S3Stream(io.StringIO(data_str))

        # mock s3 client
        s3client = boto3.client("s3")
        s3client.upload_fileobj = MagicMock(return_value={"msg": "boto3 response"})

        resource = S3Resource(
            "filename.ext",
            content_type="application/json",
            prefix="prefix/",
            bucketname="bucketname",
            protocol="protocol://",
            stream=s3stream,
            s3client=s3client,
            Metadata={"tag": "metadata"},
        )
        resource.save()
        s3client.upload_fileobj.assert_called_with(
            s3stream,
            "bucketname",
            "prefix/filename.ext",
            ExtraArgs={
                "ContentType": "application/json",
                "Metadata": {"tag": "metadata"},
            },
        )
        self.assertDictEqual(resource.last_resp, {"msg": "boto3 response"})
