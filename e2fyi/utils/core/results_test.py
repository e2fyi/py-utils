"""Unit test for result."""

import unittest

from e2fyi.utils.core.results import Result


class ResultHelper(unittest.TestCase):
    """TestCase for Result"""

    def test_ok(self):
        expected_value = "foo bar"
        result_ok = Result[str](expected_value)
        self.assertTrue(result_ok.is_ok)
        self.assertEqual(result_ok.value, expected_value)
        self.assertEqual(result_ok.with_default("bar foo"), expected_value)

    def test_not_ok_with_default(self):
        default_value = "foo bar"
        expected_exception = ValueError("some value error.")

        result_exception_and_value = Result[str](
            "bar foo", exception=expected_exception
        )
        self.assertFalse(result_exception_and_value.is_ok)
        self.assertEqual(
            result_exception_and_value.with_default(default_value), default_value
        )
        self.assertEqual(result_exception_and_value.exception, expected_exception)

        result_exception_only = Result[str](exception=expected_exception)
        self.assertFalse(result_exception_only.is_ok)
        self.assertEqual(
            result_exception_only.with_default(default_value), default_value
        )
        self.assertEqual(result_exception_only.exception, expected_exception)
