"""
Helpers for interacting with S3 buckets.

S3 bucket with preset prefix::

    from e2fyi.utils.aws import S3Bucket

    # upload a dict to s3 bucket
    S3Bucket("foo").upload("some_folder/some_file.json", {"foo": "bar"})

    # creates a s3 bucket with std prefix rule
    foo_bucket = S3Bucket("foo", get_prefix=lambda prefix: "some_folder/%s" % prefix)
    foo_bucket.upload("some_file.json", {"foo": "bar"})  # some_folder/some_file.json

Uploading to S3 bucket::

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
    # extra kwargs can be passed to pandas.to_csv method
    s3.upload("some_file.csv", df, content_type="text/csv", index=False)
    # extra kwargs can be passed to pandas.to_json method
    s3.upload("some_file.json", df, content_type="application/json", orient="records")

    # upload pydantic models as application/json file
    class KeyValue(BaseModel):
        key: str
        value: int
    model = KeyValue(key="a", value=1)
    s3.upload("some_file.json", model)


Listing contents inside S3 buckets::

    from e2fyi.utils.aws import S3Bucket

    # list files
    S3Bucket("foo").list("some_folder/")

    # list files inside "some_folder/"
    foo_bucket.list()
"""
from e2fyi.utils.aws.s3 import S3Bucket
from e2fyi.utils.aws.s3_stream import S3Stream
from e2fyi.utils.aws.s3_resource import S3Resource
