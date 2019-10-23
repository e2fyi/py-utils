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
    unittest.TextTestRunner(verbosity=2).run(suite())
    cov.stop()
    cov.save()

    print("coverage report:")
    cov.report()


def run_pylint():
    print("checking with pylint:")
    subprocess.call(["pylint", "e2fyi"], stdout=sys.stdout, stderr=sys.stderr)


def run_mypy():
    print("checking with mypy:")
    subprocess.call(["mypy", "e2fyi"], stdout=sys.stdout, stderr=sys.stderr)


def run_black():
    print("checking with black:")
    subprocess.call(["black", "--check", "e2fyi"], stdout=sys.stdout, stderr=sys.stderr)


def main():
    run_pylint()
    run_black()
    run_mypy()
    run_unittest()


if __name__ == "__main__":
    main()
