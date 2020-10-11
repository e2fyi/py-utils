import unittest

from unittest.mock import MagicMock, patch

import e2fyi.utils.core.http


class HttpStreamTest(unittest.TestCase):
    def test_http_stream(self):

        with e2fyi.utils.core.http.HttpStream("https://google.com") as stream:
            self.assertEqual(stream.read(), "a")
