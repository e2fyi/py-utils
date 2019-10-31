# e2fyi-utils

[![PyPI version](https://badge.fury.io/py/e2fyi-utils.svg)](https://badge.fury.io/py/e2fyi-utils)
[![Build Status](https://travis-ci.org/e2fyi/py-utils.svg?branch=master)](https://travis-ci.org/e2fyi/py-utils)
[![Coverage Status](https://coveralls.io/repos/github/e2fyi/py-utils/badge.svg?branch=master)](https://coveralls.io/github/e2fyi/py-utils?branch=master)
[![Documentation Status](https://readthedocs.org/projects/e2fyi-utils/badge/?version=latest)](https://e2fyi-utils.readthedocs.io/en/latest/?badge=latest)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://pepy.tech/badge/e2fyi-utils)](https://pepy.tech/project/e2fyi-utils)

`e2fyi-utils` is an `e2fyi` namespaced python package with `utils` subpackage
(i.e. `e2fyi.utils`) which holds a collections of useful helper classes and
functions to interact with various cloud resources.

API documentation can be found at [https://e2fyi-utils.readthedocs.io/en/latest/](https://e2fyi-utils.readthedocs.io/en/latest/).

> - Python 3.6 and above
> - Licensed under [Apache-2.0](./LICENSE).

#### CHANGELOG

| version        | description                                     |
| -------------- | ----------------------------------------------- |
| v0.1.0a1       | Initial release. Support AWS S3 bucket.         |
| v0.1.0a1.post1 | Add README to pypi.                             |
| v0.1.0a2       | Fix bug: include requirements.txt into setup.py |

## Quickstart

```bash
pip install e2fyi-utils
```

### e2fyi.utils.aws.S3Bucket

**S3 bucket with preset prefix**

```py
from e2fyi.utils.aws import S3Bucket

# upload a dict to s3 bucket
S3Bucket("foo").upload("some_folder/some_file.json", {"foo": "bar"})

# creates a s3 bucket with std prefix rule
foo_bucket = S3Bucket("foo", get_prefix=lambda prefix: "some_folder/%s" % prefix)
foo_bucket.upload("some_file.json", {"foo": "bar"})  # some_folder/some_file.json
```

**Uploading to S3 bucket**

```py
import logging

import pandas as pd

from e2fyi.utils.aws import S3Bucket
from pydantic import BaseModel

# s3 bucket with prefix rule
s3 = S3Bucket("foo", get_prefix=lambda prefix: "some_folder/%s" % prefix)

# check if upload is successful
result = s3.upload("some_file.txt", "hello world")
if not result.is_ok:
    logging.exception(result.exception)

# upload string as text/plain file
s3.upload("some_file.txt", "hello world")

# upload dict as application/json file
s3.upload("some_file.json", {"foo": "bar"})

# upload pandas df as text/csv and/or application/json files
df = pd.DataFrame([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
s3.upload("some_file.csv", df, content_type="text/csv", index=False)  # extra kwargs can be passed to pandas.to_csv method
s3.upload("some_file.json", df, content_type="application/json", orient="records")  # extra kwargs can be passed to pandas.to_json method

# upload pydantic models as application/json file
class KeyValue(BaseModel):
    key: str
    value: int
model = KeyValue(key="a", value=1)
s3.upload("some_file.json", model)

```

**Listing contents inside S3 buckets**

```py
from e2fyi.utils.aws import S3Bucket

# list files
S3Bucket("foo").list("some_folder/")

# list files inside "some_folder/"
foo_bucket.list()
```

### e2fyi.utils.core.Result

```py
import logging

from e2fyi.utils.core import Result


def load_from_file(filepath: str) -> Result[string]:
    """loads the content of a file."""
    try:
        with open(filepath, "r") as fp:
            return Result(fp.read())
    except IOError as err:
        return Result(exception=err)

data = load_from_file("some_file.json")

# print with a default value fallback
print(data.with_default("default value"))

# print data if ok, else log exception
if data.is_ok:
    print(data)
else:
    logging.exception(data.exception)

```
