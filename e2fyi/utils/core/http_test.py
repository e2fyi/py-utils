import unittest

from unittest.mock import MagicMock, patch

import requests_mock

from e2fyi.utils.core.http import HttpStream


class HttpStreamTest(unittest.TestCase):
    def test_http_stream_read_text(self):

        with requests_mock.Mocker() as mock:
            mocked_url = "https://foo.bar"
            expected_text = "hello\nworld\n"
            mock.get(mocked_url, text=expected_text)

            self.assertEqual(HttpStream(mocked_url).read(), expected_text)
            self.assertEqual(list(HttpStream(mocked_url)), [b"hello", b"world"])

            with HttpStream("https://foo.bar") as stream:
                self.assertEqual(stream.read(), expected_text)

    def test_http_stream_read_bin(self):

        with requests_mock.Mocker() as mock:
            mocked_url = "https://foo.bar"
            expected_content = b"hello\nworld\n"
            mock.get(mocked_url, content=expected_content)

            self.assertEqual(HttpStream(mocked_url, mode="rb").read(), expected_content)
            self.assertEqual(
                list(HttpStream(mocked_url, mode="rb", chunk_size=5)),
                [b"hello", b"\nworl", b"d\n"],
            )

            with HttpStream("https://foo.bar", mode="rb") as stream:
                self.assertEqual(stream.read(), expected_content)

    def test_http_stream_write_text(self):

        streamed_content = None

        def side_effect(*args, **kwargs):
            nonlocal streamed_content
            if "data" in kwargs:
                streamed_content = kwargs["data"].read()
            return MagicMock()

        with patch("requests.post", side_effect=side_effect) as mock_post:

            with HttpStream("https://foo.bar", mode="w") as stream:
                stream.write("line1")
                stream.write("line2")

            assert streamed_content == b"line1line2"
