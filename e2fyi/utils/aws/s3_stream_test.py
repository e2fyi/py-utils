"""Unit test for s3 helpers."""
import json
import unittest

from unittest.mock import MagicMock

import joblib
import pandas as pd

from pydantic import BaseModel  # pylint: disable=no-name-in-module

from e2fyi.utils.aws.s3_stream import S3Stream


class S3StreamTest(unittest.TestCase):
    """TestCase for S3ResourceHelper"""

    def test_str(self):
        data = "foo bar"
        stream = S3Stream.from_any(data, content_type="text/plain")
        self.assertEqual(stream.content_type, "text/plain")
        self.assertEqual(stream.read(), data)

    def test_dict(self):
        data = {"foo": "bar"}
        stream = S3Stream.from_any(data, content_type="text/plain")
        self.assertEqual(stream.content_type, "application/json")
        self.assertDictEqual(json.loads(stream.read()), data)

    def test_model(self):
        class Model(BaseModel):
            """dummy model"""

            name: str
            value: int

        data = Model(name="foo", value=10)
        stream = S3Stream.from_any(data, content_type="text/plain")
        self.assertEqual(stream.content_type, "application/json")
        self.assertDictEqual(json.loads(stream.read()), data.dict())

    def test_pickle(self):
        class Model:
            """dummy class"""

            def __init__(self, name: str, value: int):
                self.name = name
                self.value = value

        joblib.dump = MagicMock()
        data = Model(name="foo", value=10)
        stream = S3Stream.from_any(data, content_type="application/octet-stream")
        self.assertEqual(stream.content_type, "application/octet-stream")
        joblib.dump.assert_called_once()

    def test_pandas_csv(self):
        data = pd.DataFrame(data=[{"name": "a"}, {"name": "b"}])
        stream = S3Stream.from_any(data, output_as="csv", index=False)
        self.assertEqual(stream.content_type, "application/csv")
        self.assertEqual(stream.read(), "name\na\nb\n")

    def test_pandas_dict(self):
        data = pd.DataFrame(data=[{"name": "a"}, {"name": "b"}])
        stream = S3Stream.from_any(data, output_as="json", orient="records")
        self.assertEqual(stream.content_type, "application/json")
        self.assertEqual(json.loads(stream.read()), [{"name": "a"}, {"name": "b"}])
