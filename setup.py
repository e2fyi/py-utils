#!/usr/bin/env python
# noqa
# pylint: skip-file
"""The setup script."""
from setuptools import setup

with open("requirements.txt", "r") as filein:
    requirements = filein.readlines()

with open("requirements-dev.txt", "r") as filein:
    test_requirements = filein.readlines()

with open("version.txt", "r") as filein:
    version = filein.read()

setup_requirements: list = [
    "setuptools >= 41.0.0",
    # python3 specifically requires wheel 0.26
    'wheel; python_version < "3"',
    'wheel >= 0.26; python_version >= "3"',
]

setup(
    author="eterna2",
    author_email="eterna2@hotmail.com",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    description="General utils for interacting with aws resources.",
    include_package_data=True,
    package_data={"": ["version.txt", "requirements.txt", "test.py"]},
    extras_require={"pandas": ["pandas"]},
    keywords="util aws s3",
    name="e2fyi-utils",
    packages=["e2fyi.utils"],
    setup_requires=setup_requirements,
    python_requires=">=3.5",
    install_requires=requirements,
    test_suite="e2fyi",
    tests_require=test_requirements,
    version=version,
    zip_safe=False,
)
