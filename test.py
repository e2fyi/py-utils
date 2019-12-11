#!/usr/bin/env python
# noqa
# pylint: skip-file
import sys
import unittest
import subprocess

import coverage


def suite():
    """test suite"""
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover("e2fyi", pattern="*_test.py")
    return test_suite


def run_unittest():
    cov = coverage.Coverage()
    cov.start()
    print("unittest:")
    result = unittest.TextTestRunner(verbosity=2).run(suite())
    cov.stop()
    cov.save()

    print("coverage report:")
    cov.report()

    return result.wasSuccessful()


def run_pylint():
    print("checking with pylint:")
    return (
        subprocess.call(["pylint", "e2fyi"], stdout=sys.stdout, stderr=sys.stderr) == 0
    )


def run_mypy():
    print("checking with mypy:")
    return subprocess.call(["mypy", "e2fyi"], stdout=sys.stdout, stderr=sys.stderr) == 0


def run_black():
    print("checking with black:")
    return (
        subprocess.call(
            ["black", "--check", "e2fyi"], stdout=sys.stdout, stderr=sys.stderr
        )
        == 0
    )


def main():

    tests = [run_pylint, run_black, run_mypy, run_unittest]
    is_successful = True
    for test in tests:
        is_successful = is_successful and test()
    return 0 if is_successful else 1


if __name__ == "__main__":
    exit(main())
