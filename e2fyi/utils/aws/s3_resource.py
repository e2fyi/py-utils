"""
Provides `S3Resource` to represent resources in S3 buckets.
"""
import io
import json
import os.path

from uuid import uuid4
from typing import Union, Generic, TypeVar, Callable, Optional

import boto3

from e2fyi.utils.aws.s3_stream import S3Stream

T = TypeVar("T")
StringOrBytes = TypeVar("StringOrBytes", bytes, str)


class S3Resource(Generic[StringOrBytes]):
    """
    `S3Resource` represents a resource in S3 currently or a local resource that will
    be uploaded to S3. `S3Resource` constructor will automatically attempts to convert
    any inputs into a `S3Stream`, but for more granular control `S3Stream.from_any`
    should be used instead to create the `S3Stream`.

    `S3Resource` is a readable stream - i.e. it has `read`, `seek`, and `close`.


    Example::

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
            # addition kwarg to pass to `s3.upload_fileobj` or `s3.download_fileobj`
            # methods
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


    Saving to S3::

        from e2fyi.utils.aws import S3Resource

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


    Reading from S3::

        from e2fyi.utils.aws import S3Resource
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

    """

    def __init__(
        self,
        filename: str,
        content_type: str = "",
        bucketname: str = "",
        prefix: str = "",
        protocol: str = "s3a://",
        stream: S3Stream[StringOrBytes] = None,
        s3client: boto3.client = None,
        stats: dict = None,
        **kwargs
    ):
        """
        Creates a new instance of S3Resource, which will use
        `boto3.s3.transfer.S3Transfer` under the hood to download/upload the s3
        resource.

        See
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer

        Args:
            filename (str): filename of the object.
            content_type (str, optional): mime type of the object. Defaults to "".
            bucketname (str, optional): name of the bucket the obj is or should be.
                Defaults to "".
            prefix (str, optional): prefix to be added to the filename to get the s3
                object key. Defaults to "application/octet-stream".
            protocol (str, optional): s3 client protocol. Defaults to "s3a://".
            stream (S3Stream[StringOrBytes], optional): data stream. Defaults to None.
            s3_client (boto3.client, optional): s3 client to use to retrieve
                resource. Defaults to None.
            Metadata (dict, optional): metadata for the object. Defaults to None.
            **kwargs: Any additional args to pass to `boto3.s3.transfer.S3Transfer`
                function.
        """
        # random name if filename is not provided
        filename = filename or uuid4().hex
        dirname = os.path.dirname(filename)

        if dirname:
            filename = filename[len(dirname) + 1 :]
            prefix = os.path.join(prefix, dirname) + os.path.sep

        if stream:
            if not isinstance(stream, S3Stream):
                stream = S3Stream.from_any(stream, content_type)
            if content_type:
                stream.content_type = content_type

        self.filename = filename
        self._content_type = content_type
        self.bucketname = bucketname
        self.prefix = prefix
        self.protocol = protocol
        self._stream: Optional[S3Stream[StringOrBytes]] = stream
        self.extra_args = kwargs
        self.s3client = s3client
        self.last_resp = None
        self.stats = stats

    @property
    def content_type(self) -> str:
        """mime type of the resource"""
        if self._stream and hasattr(self._stream, "content_type"):
            return self._stream.content_type
        return self._content_type or "application/octet-stream"

    @property
    def key(self) -> str:
        """Key for the resource."""
        if not self.filename:
            raise ValueError("filename cannot be empty.")
        return "%s%s" % (self.prefix, self.filename)

    @property
    def uri(self) -> str:
        """URI to the resource."""
        if not self.bucketname:
            raise ValueError("bucketname cannot be empty.")
        return "%s%s/%s" % (self.protocol, self.bucketname, self.key)

    @property
    def stream(self) -> S3Stream[StringOrBytes]:
        """data stream for the resource."""
        if self._stream:
            return self._stream

        if self.bucketname:
            stream = io.BytesIO()
            s3client = self.s3client or boto3.client("s3")
            self.last_resp = s3client.download_fileobj(
                self.bucketname, self.key, stream, ExtraArgs=self.extra_args
            )
            stream.seek(0)  # reset to initial counter
            self._stream = S3Stream(stream, self._content_type)
            # overwrite infered mime if provided
            if self._content_type:
                self._stream.content_type = self._content_type
            return self._stream

        raise RuntimeError("S3Resource does not have a stream.")

    def read(self, size=-1) -> StringOrBytes:
        """duck-typing for a readable stream."""
        return self.stream.read(size)  # type: ignore

    def seek(self, offset: int, whence: int = 0) -> int:
        """duck-typing for readable stream.
        See https://docs.python.org/3/library/io.html

        Change the stream position to the given byte offset. offset is interpreted
        relative to the position indicated by whence. The default value for whence
        is SEEK_SET. Values for whence are:

            SEEK_SET or 0 – start of the stream (the default); offset should be zero
                or positive

            SEEK_CUR or 1 – current stream position; offset may be negative

            SEEK_END or 2 – end of the stream; offset is usually negative

        Return the new absolute position.
        """
        return self.stream.seek(offset, whence)  # type: ignore

    def close(self) -> "S3Resource":
        """Close the resource stream."""
        self.stream.close()
        return self

    def get_value(self) -> StringOrBytes:
        """Retrieve the entire contents of the S3Resource."""
        self.seek(0)
        return self.read()

    def load(
        self, constructor: Callable[..., T] = None, unpack: bool = True
    ) -> Union[dict, list, T]:
        """
        load the content of the stream into memory using `json.loads`. If a
        `constructor` is provided, it will be used to create a new object. Setting
        `unpack` to be true will unpack the content when creating the object
        with the `constructor` (i.e. * for list, ** for dict)

        Args:
            constructor (Callable[..., T], optional): A constructor function.
                Defaults to None.
            unpack (bool, optional): whether to unpack the content when passing
                it to the constructor. Defaults to True.

        Raises:
            TypeError: [description]

        Returns:
            Union[dict, list, T]: [description]
        """
        if self.content_type != "application/json":
            raise TypeError(
                "Content type is '%s' instead 'application/json'." % self.content_type
            )

        self.seek(0)  # reset buffer offset
        result = json.loads(self.read())

        if not constructor:
            return result  # type: ignore
        if unpack:
            if isinstance(result, dict):
                return constructor(**result)
            if isinstance(result, list):
                return constructor(*result)
        return constructor(result)

    def save(
        self, bucketname: str = None, s3client: boto3.client = None, **kwargs
    ) -> "S3Resource":
        """
        Saves the current S3Resource to the provided s3 bucket (in constructor or
        in arg). Extra args can be pass to `boto3.s3.transfer.S3Transfer` via
        keyword arguments of the same name.

        See
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS

        Args:
            bucketname (str, optional): bucket to save the resource to. Overwrites
                the bucket name provided in the constructor. Defaults to None.
            s3client (boto3.client, optional): custom s3 client to use. Defaults to
                None.
            **kwargs: additional args to pass to `boto3.s3.transfer.S3Transfer`.

        Raises:
            ValueError: "S3 bucket name must be provided."

        Returns:
            S3Resource: S3Resource object.
        """

        bucketname = bucketname or self.bucketname
        if not bucketname:
            raise ValueError("S3 bucket name must be provided.")

        self.stream.seek(0)
        sample = self.stream.read(10)
        self.stream.seek(0)

        if isinstance(sample, str):
            stream = io.BytesIO(self.stream.read().encode("utf-8"))
        else:
            stream = self.stream

        s3client = s3client or self.s3client or boto3.client("s3")
        self.last_resp = s3client.upload_fileobj(
            stream,
            bucketname,
            self.key,
            ExtraArgs={"ContentType": self.content_type, **self.extra_args, **kwargs},
        )
        self.stream.seek(0)
        return self

    def __str__(self) -> str:
        """String representation of a S3Resource."""
        try:
            return self.uri
        except ValueError:
            return self.key
