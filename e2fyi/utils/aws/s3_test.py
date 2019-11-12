"""Unit test for s3 helpers."""
import io
import json
import unittest

from unittest.mock import MagicMock

import boto3
import pandas as pd

from pydantic import BaseModel

from e2fyi.utils.aws.s3 import S3Bucket, S3Resource, S3ResourceHelper


class S3ResourceTest(unittest.TestCase):
    """TestCase for S3Resource"""

    def test_basic_ok(self):
        data = {"key1": "foo", "key2": "bar"}
        data_str = json.dumps(data)
        resource = S3Resource(
            "filename.ext",
            content_type="application/json",
            prefix="prefix/",
            bucketname="bucketname",
            protocol="protocol://",
            stream=io.StringIO(data_str),
            metadata={"tag": "metadata"},
        )

        self.assertEqual(resource.key, "prefix/filename.ext")
        self.assertEqual(resource.uri, "protocol://bucketname/prefix/filename.ext")
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
            stream=io.StringIO(data_str),
            metadata={"tag": "metadata"},
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
            stream=io.StringIO(data_str),
        )

        # pass in a transform func and do not unpack content
        transformed = resource.load(lambda content: content + ["d"], unpack=False)
        self.assertListEqual(transformed, ["a", "b", "c", "d"])
        # pass in a transform func and unpack content
        transformed = resource.load(lambda a, b, c: "%s:%s:%s" % (a, b, c), unpack=True)
        self.assertEqual(transformed, "a:b:c")


class S3ResourceHelperTest(unittest.TestCase):
    """TestCase for S3ResourceHelper"""

    def test_wrap_str(self):
        data = "foo bar"
        resource = S3ResourceHelper.wrap_raw(data, content_type="text/plain")
        self.assertEqual(resource.content_type, "text/plain")
        self.assertEqual(resource.read(), data)

    def test_wrap_dict(self):
        data = {"foo": "bar"}
        resource = S3ResourceHelper.wrap_raw(data, content_type="text/plain")
        self.assertEqual(resource.content_type, "application/json")
        self.assertEqual(json.loads(resource.read()), data)

    def test_pandas_csv(self):
        data = pd.DataFrame(data=[{"name": "a"}, {"name": "b"}])
        resource = S3ResourceHelper.wrap_pandas_as_csv(data, index=False)
        self.assertEqual(resource.content_type, "application/csv")
        self.assertEqual(resource.read(), "name\na\nb\n")

    def test_pandas_dict(self):
        data = pd.DataFrame(data=[{"name": "a"}, {"name": "b"}])
        resource = S3ResourceHelper.wrap_pandas_as_json(data, orient="records")
        self.assertEqual(resource.content_type, "application/json")
        self.assertEqual(resource.load(), [{"name": "a"}, {"name": "b"}])


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

    def test_upload_str(self):
        bucket = S3Bucket(
            "bucketname",
            get_prefix=lambda filename: "prefix/%s" % filename,
            s3client=self.s3client,
        )
        result = bucket.upload("some/path/file.ext", body="hello")
        resource = result.value

        self.s3client.upload_fileobj.assert_called_once()
        self.assertTrue(result.is_ok)

        self.assertIsInstance(resource, S3Resource)
        self.assertEqual(resource.uri, "s3a://bucketname/prefix/some/path/file.ext")
        self.assertEqual(resource.content_type, "text/plain")
        self.assertEqual(resource.read(), "hello")

    def test_upload_dict(self):
        bucket = S3Bucket(
            "bucketname",
            get_prefix=lambda filename: "prefix/%s" % filename,
            s3client=self.s3client,
        )

        result = bucket.upload("some/path/file.ext", body={"foo": "bar"})
        resource = result.value

        self.s3client.upload_fileobj.assert_called_once()
        self.assertTrue(result.is_ok)

        self.assertIsInstance(resource, S3Resource)
        self.assertEqual(resource.uri, "s3a://bucketname/prefix/some/path/file.ext")
        self.assertEqual(resource.content_type, "application/json")
        self.assertDictEqual(resource.load(), {"foo": "bar"})

    def test_upload_basemodel(self):
        bucket = S3Bucket(
            "bucketname",
            get_prefix=lambda filename: "prefix/%s" % filename,
            s3client=self.s3client,
        )

        class DummyModel(BaseModel):
            """Dummy base model"""

            some_int: int
            some_str: str

        model = DummyModel(some_int=1, some_str="foo bar")
        result = bucket.upload("some/path/file.ext", body=model)
        resource = result.value

        self.s3client.upload_fileobj.assert_called_once()
        self.assertTrue(result.is_ok)

        self.assertIsInstance(resource, S3Resource)
        self.assertEqual(resource.uri, "s3a://bucketname/prefix/some/path/file.ext")
        self.assertEqual(resource.content_type, "application/json")
        self.assertDictEqual(resource.load(), model.dict())

    def test_upload_pandasdf(self):
        bucket = S3Bucket(
            "bucketname",
            get_prefix=lambda filename: "prefix/%s" % filename,
            s3client=self.s3client,
        )

        df = pd.DataFrame([{"col1": value, "col2": "foo"} for value in range(0, 5)])
        result = bucket.upload(
            "some/path/file.ext", body=df, content_type="application/csv", index=False
        )
        resource = result.value

        self.s3client.upload_fileobj.assert_called_once()
        self.assertTrue(result.is_ok)

        self.assertIsInstance(resource, S3Resource)
        self.assertEqual(resource.uri, "s3a://bucketname/prefix/some/path/file.ext")
        self.assertEqual(resource.content_type, "application/csv")
        self.assertEqual(
            resource.read(), "col1,col2\n0,foo\n1,foo\n2,foo\n3,foo\n4,foo\n"
        )

    def test_upload_pandasdf_asrecord(self):
        bucket = S3Bucket(
            "bucketname",
            get_prefix=lambda filename: "prefix/%s" % filename,
            s3client=self.s3client,
        )

        df = pd.DataFrame([{"col1": value, "col2": "foo"} for value in range(0, 5)])
        result = bucket.upload(
            "some/path/file.ext",
            body=df,
            content_type="application/json",
            orient="records",
        )
        resource = result.value

        self.s3client.upload_fileobj.assert_called_once()
        self.assertTrue(result.is_ok)

        self.assertIsInstance(resource, S3Resource)
        self.assertEqual(resource.uri, "s3a://bucketname/prefix/some/path/file.ext")
        self.assertEqual(resource.content_type, "application/json")
        self.assertListEqual(
            resource.load(),
            [
                {"col1": 0, "col2": "foo"},
                {"col1": 1, "col2": "foo"},
                {"col1": 2, "col2": "foo"},
                {"col1": 3, "col2": "foo"},
                {"col1": 4, "col2": "foo"},
            ],
        )

    def test_create_resource_key(self):
        bucket = S3Bucket(name="bucket", get_prefix=lambda x: "folder/%s" % x)
        key = bucket.create_resource_key("filename.ext")

        self.assertEqual(key, "folder/filename.ext")

    def test_create_resource_uri(self):
        bucket = S3Bucket(name="bucket", get_prefix=lambda x: "folder/%s" % x)
        key = bucket.create_resource_uri("filename.ext")

        self.assertEqual(key, "s3a://bucket/folder/filename.ext")
