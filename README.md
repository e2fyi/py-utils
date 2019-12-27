# e2fyi-utils

[![PyPI version](https://badge.fury.io/py/e2fyi-utils.svg)](https://badge.fury.io/py/e2fyi-utils)
[![Build Status](https://travis-ci.org/e2fyi/py-utils.svg?branch=master)](https://travis-ci.org/e2fyi/py-utils)
[![Coverage Status](https://coveralls.io/repos/github/e2fyi/py-utils/badge.svg?branch=master)](https://coveralls.io/github/e2fyi/py-utils?branch=master)
[![Documentation Status](https://readthedocs.org/projects/e2fyi-utils/badge/?version=latest)](https://e2fyi-utils.readthedocs.io/en/latest/?badge=latest)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://pepy.tech/badge/e2fyi-utils/month)](https://pepy.tech/project/e2fyi-utils/month)

`e2fyi-utils` is an `e2fyi` namespaced python package with `utils` subpackage
(i.e. `e2fyi.utils`) which holds a collections of useful helper classes and
functions to interact with various cloud resources.

API documentation can be found at [https://e2fyi-utils.readthedocs.io/en/latest/](https://e2fyi-utils.readthedocs.io/en/latest/).

Change logs are available in [CHANGELOG.md](./CHANGELOG.md).

> - Python 3.6 and above
> - Licensed under [Apache-2.0](./LICENSE).


## Quickstart

```bash
pip install e2fyi-utils>=0.2
```

### S3Stream

`S3Stream` represents the data stream of a S3 resource, and provides static
methods to convert any python objects into a stream. This is generally used with
`S3Resource` to upload or download resource from S3 buckets.

> NOTE:
> - `str`, `float`, `int`, and `bool` will be saved as txt files with mime type "text/plain".
> - `pydantic` models, `dict` or `list` will be saved as json files with mime type "application/json" (fallback to pickle if unable to serialize into json string).
> - `pandas` dataframe or series can be saved as either a csv ("application/csv") or json format ("application/json").
> - path to files will be read with `open` and mime type will be inferred (fallback to "application/octet-stream").
> - all other python objects will be pickled with `joblib`.

```py
import io

import pandas as pd

from e2fyi.utils.aws import S3Stream
from pydantic import BaseModel

# create a s3 stream
stream = S3Stream(io.StringIO("random text"), "text/plain")
print(stream.read())        # prints "random text"
print(stream.content_type)  # prints "text/plain"

# string
stream = S3Stream.from_any("hello world")
print(stream.read())        # prints "hello world"
print(stream.content_type)  # prints "text/plain"

# dict
stream = S3Stream.from_any({"foo": "bar"})
print(stream.read())        # prints "{"foo": "bar"}"
print(stream.content_type)  # prints "application/json"

# pandas dataframe as csv
df = pd.DataFrame([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
stream = S3Stream.from_any(df, index=False)  # do not include index column
print(stream.read())        # prints string as csv format for the dataframe
print(stream.content_type)  # prints "application/csv"

# pandas dataframe as json
stream = S3Stream.from_any(df, orient="records")  # orient dataframe as records
print(stream.read())        # prints string as json list for the dataframe
print(stream.content_type)  # prints "application/json"


# pydantic model
class Person(BaseModel):
    name: str
    age: int
stream = S3Stream.from_any(Person(name="william", age=21))
print(stream.read())        # prints "{"name": "william", "age": 21}"
print(stream.content_type)  # prints "application/json"


# any other python objects
class Pet:
    name: str
    age: int
stream = S3Stream.from_any(Pet(name="kopi", age=1))
print(stream.read())        # prints some binary bytes
print(stream.content_type)  # prints "application/octet-stream"

```

### S3Resource

`S3Resource` represents a resource in S3 currently or a local resource that will
be uploaded to S3. `S3Resource` constructor will automatically attempts to convert
any inputs into a `S3Stream`, but for more granular control `S3Stream.from_any`
should be used instead to create the `S3Stream`.

`S3Resource` is a readable stream - i.e. it has `read`, `seek`, and `close`.

> NOTE:
>
> See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html
> for additional keyword arguments that can be passed to S3Resource.

#### Example: Creating S3Resource from local python object or file.
```py
import boto3

from e2fyi.utils.aws import S3Resource, S3Stream

# create custom s3 client
s3client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# creates a local copy of s3 resource with S3Stream from a local file
obj = S3Resource(
    # full path shld be "prefix/some_file.json"
    filename="some_file.json",
    prefix="prefix/",
    # bucket to download from or upload to
    bucketname="some_bucket",
    # or "s3n://" or "s3://"
    protocol="s3a://",
    # uses default client if not provided
    s3client=s3client,
    # attempts to convert to S3Stream if input is not a S3Stream
    stream=S3Stream.from_file("./some_path/some_file.json"),
    # addition kwarg to pass to `s3.upload_fileobj` or `s3.download_fileobj` methods
    Metadata={"label": "foo"}
)
print(obj.key)  # prints "prefix/some_file.json"
print(obj.uri)  # prints "s3a://some_bucket/prefix/some_file.json"

# will attempt to fix prefix and filename if incorrect filename is provided
obj = S3Resource(
    filename="subfolder/some_file.json",
    prefix="prefix/"
)
print(obj.filename)     # prints "some_file.json"
print(obj.prefix)       # prints "prefix/subfolder/"
```

#### Example: Upload S3Resource to S3
```py
# creates a local copy of s3 resource with some python object
obj = S3Resource(
    filename="some_file.txt",
    prefix="prefix/",
    bucketname="some_bucket",
    stream={"some": "dict"},
)

# upload obj to s3 bucket "some_bucket" with the key "prefix/some_file.json"
# with the json string content.
obj.save()

# upload to s3 bucket "another_bucket" instead with a metadata tag.
obj.save("another_bucket", MetaData={"label": "foo"})
```

#### Example: Read S3Resource from S3
```py
from pydantic import BaseModel

# do not provide a stream input to the S3Resource constructor
obj = S3Resource(
    filename="some_file.json",
    prefix="prefix/",
    bucketname="some_bucket",
    content_type="application/json"
)

# read the resource like a normal file object from S3
data = obj.read()
print(type(data))       # prints <class 'str'>

# read and load json string into a dict or list
# for content_type == "application/json" only
data_obj = obj.load()
print(type(data_obj))   # prints <class 'dict'> or <class 'list'>


# read and convert into a pydantic model
class Person(BaseModel):
    name: str
    age: int

# automatically unpack the dict
data_obj = obj.load(lambda name, age: Person(name=name, age=age))
# alternatively, do not unpack
data_obj = obj.load(lambda data: Person(**data), unpack=False)
print(type(data_obj))   # prints <class 'Person'>
```

### S3Bucket

`S3Bucket` is an abstraction of the actual S3 bucket with methods to interact
with the actual S3 bucket (e.g. list objects inside the bucket), and some utility
methods.

Prefix rules can also be set during the creation of the `S3Bucket` object - i.e.
enforce a particular prefix rules for a particular bucket.

#### Quickstart
```py
from e2fyi.utils.aws import S3Bucket

# prints key for all resources with prefix "some_folder/"
for resource in S3Bucket("some_bucket").list("some_folder/"):
    print(resource.key)

# prints key for the first 2,000 resources with prefix "some_folder/"
for resource in S3Bucket("some_bucket").list("some_folder/", max_objects=2000):
    print(resource.key)

# creates a s3 bucket with prefix rule
prj_bucket = S3Bucket("some_bucket", get_prefix=lambda prefix: "prj-a/%s" % prefix)
for resource in prj_bucket.list("some_folder/"):
    print(resource.key)  # prints "prj-a/some_folder/<resource_name>"
    print(resource.stats)  # prints metadata for the resource (e.g. size, etag)

# get obj key in the bucket
print(prj_bucket.create_resource_key("foo.json"))  # prints "prj-a/foo.json"

# get obj uri in the bucket
# prints "s3a://some_bucket/prj-a/foo.json"
print(prj_bucket.create_resource_uri("foo.json", "s3a://"))

# create S3Resource in bucket to read in
foo = prj_bucket.create_resource("foo.json", "application/json")
# read "s3a://some_bucket/prj-a/foo.json" and load as a dict (or list)
foo_dict = foo.load()

# create S3Resource in bucket and save to "s3a://some_bucket/prj-a/foo.json"
prj_bucket.create_resource("foo.json", obj={"foo": "bar"}).save()
```

### e2fyi.utils.core.Maybe

`Maybe` represents an uncertain object (exception might be raised so no value
will be returned). This is generally used inside a function where all exceptions
will be caught.

> NOTE:
> - `Maybe.value` is the actual returned value.
> - `Maybe.exception` is the exception caught (if any).
> - `Maybe.with_default` method can be used to provide a default value if no value
is returned.
> - `Maybe.is_ok` method can be used to check if any value is returned.

```py
import logging

from e2fyi.utils.core import Maybe


def load_from_file(filepath: str) -> Maybe[string]:
    """loads the content of a file."""
    try:
        with open(filepath, "r") as fp:
            return Maybe(fp.read())
    except IOError as err:
        return Maybe(exception=err)

data = load_from_file("some_file.json")

# print with a default value fallback
print(data.with_default("default value"))

# print data if ok, else log exception
if data.is_ok:
    print(data)
else:
    logging.exception(data.exception)

```
