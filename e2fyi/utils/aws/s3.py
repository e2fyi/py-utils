"""utils to interact with s3 buckets."""
import io
import json
import logging
import os.path

from typing import IO, Dict, List, Union, Generic, TypeVar, Callable, Optional

import boto3
import pandas as pd
import botocore.exceptions

from pydantic import BaseModel

from e2fyi.utils.aws.compat import LIB_MAGIC_AVAILABLE
from e2fyi.utils.core.results import Result

T = TypeVar("T")
StringOrBytes = TypeVar("StringOrBytes", bytes, str)


def _noop(key: T) -> T:
    """Do nothing"""
    return key


class S3Resource(Generic[StringOrBytes]):
    """
    S3Resource is a wrapper class to bind the relationship between a S3 object
    and its local representation (e.g. local file, in-memory stream).
    """

    def __init__(
        self,
        filename: str,
        content_type: str,
        bucketname: str = "",
        prefix: str = "",
        protocol: str = "s3a://",
        stream: Union[io.StringIO, io.BytesIO, IO[StringOrBytes]] = None,
        metadata: Dict[str, str] = None,
    ):
        """
        Creates a new instance of S3Resource.

        Args:
            filename (str): filename of the object.
            content_type (str): mime type of the object.
            bucketname (str, optional): name of the bucket the obj is or should be.
                Defaults to "".
            prefix (str, optional): prefix to be added to the filename to get the s3
                object key. Defaults to "".
            protocol (str, optional): s3 client protocol. Defaults to "s3a://".
            stream (Union[io.StringIO, io.BytesIO, IO[StringOrBytes]], optional): data
                stream. Defaults to None.
            metadata (dict, optional): metadata for the object. Defaults to None.
        """
        self.filename = filename
        self.content_type = content_type
        self.bucketname = bucketname
        self.prefix = prefix
        self.protocol = protocol
        self.stream: Optional[
            Union[io.StringIO, io.BytesIO, IO[StringOrBytes]]
        ] = stream
        self.metadata = metadata or {}

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

    def read(self, size=-1) -> StringOrBytes:
        """duck-typing for a readable stream."""
        if self.stream:
            return self.stream.read(size)  # type: ignore
        raise RuntimeError(
            "Unable to read from stream: S3Resource does not have a stream."
        )

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
        if self.stream:
            return self.stream.seek(offset, whence)  # type: ignore
        raise RuntimeError(
            "Unable to seek from stream: S3Resource does not have a stream."
        )

    def close(self) -> "S3Resource":
        """Close the resource stream."""
        if self.stream:
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
        if not self.content_type == "application/json":
            raise TypeError("Content type is not 'application/json'.")

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

    def __str__(self) -> str:
        """String representation of a S3Resource."""
        try:
            return self.uri
        except ValueError:
            return "%s(metadata=%s)" % (self.__class__.__name__, self.metadata)


class S3ResourceHelper:
    """
    S3ResourceHelper is a helper class to wrap S3Resource over local files or
    in-memory data.

    Example::

        import pandas as pd

        from e2fyi.utils.aws import S3ResourceHelper
        from pydantic import BaseModel

        # creates a text s3 resource
        text_resource = S3ResourceHelper.wrap("hello world", "text/plain")

        # creates a json s3 resource
        json_resource = S3ResourceHelper.wrap({"hello": "world"}, "application/json")

        # creates a s3 resource from file
        file_resource = S3ResourceHelper.wrap(open("some_file.csv"), "text/csv")

        # creates a s3 resource from a dataframe or series
        df = pd.DataFrame([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
        df_csv_resource = S3ResourceHelper.wrap(df, "text/csv", index=False)
        df_json_resource = S3ResourceHelper.wrap(df,
                                                 "application/json",
                                                 orient="records")


        # creates a s3 resource from a pydantic basemodel
        class DummyModel(BaseModel):
            key: string
            value: int

        model = DummyModel(key="a", value=1)
        model_resource = S3ResourceHelper.wrap(model, "application/json")
    """

    @classmethod
    def wrap(
        cls,
        obj: Union[
            str, bytes, dict, IO[StringOrBytes], pd.DataFrame, pd.Series, BaseModel
        ],
        content_type: str,
        metadata: dict = None,
        **kwargs
    ) -> S3Resource:
        """
        wrap returns a S3Resource with a data stream of the provided obj.

        Args:
            obj (Union[str, bytes, dict, IO[StringOrBytes], pd.DataFrame, pd.Series,
                BaseModel]): data
            content_type (str): mime type of data
            metadata (dict, optional): Additional metadata for s3 object. Defaults to
                None.

        Raises:
            TypeError: "Cannot create S3Resource because obj type is not supported: %s"

        Returns:
            S3Resource: S3Resource with a data stream of the provided obj.
        """

        if isinstance(obj, (str, bytes, dict)):
            return cls.wrap_raw(obj, content_type, metadata=metadata)

        if isinstance(obj, (pd.DataFrame, pd.Series)):
            if content_type == "application/json":
                return cls.wrap_pandas_as_json(obj, metadata=metadata, **kwargs)
            return cls.wrap_pandas_as_csv(obj, metadata=metadata, **kwargs)

        if isinstance(obj, BaseModel):
            return cls.wrap_raw(obj.dict(), "application/json", metadata=metadata)

        if hasattr(obj, "read"):
            return cls.wrap_io(obj, content_type, metadata=metadata)

        raise TypeError(
            "Cannot create S3Resource because obj type is not supported: %s" % type(obj)
        )

    @classmethod
    def wrap_raw(
        cls, obj: Union[str, bytes, dict], content_type: str, metadata: dict = None
    ) -> S3Resource:
        """
        wrap_raw returns a S3Resource with a corresponding io stream.

        Args:
            obj (Union[str, bytes, dict]): data
            content_type (str): mime type for the data
            metadata (dict, optional): Additional metadata for s3 object. Defaults to
                None.

        Raises:
            ValueError: "obj must be one of the following: string, bytes, or dict."

        Returns:
            S3Resource: S3Resource with a corresponding io stream.
        """
        if isinstance(obj, str):
            if os.path.isfile(obj):
                return cls.wrap_file(obj, content_type, metadata=metadata)
            return cls.wrap_io(io.StringIO(obj), content_type, metadata=metadata)

        if isinstance(obj, bytes):
            return cls.wrap_io(io.BytesIO(obj), content_type, metadata=metadata)

        if isinstance(obj, dict):
            stream = io.StringIO(json.dumps(obj))
            return cls.wrap_io(stream, "application/json", metadata=metadata)

        raise ValueError("obj must be one of the following: string, bytes, or dict.")

    @staticmethod
    def wrap_pandas_as_csv(
        df: Union[pd.DataFrame, pd.Series], metadata: dict = None, **kwargs: dict
    ) -> S3Resource:
        """
        wrap_pandas_as_csv returns a S3Resource with a csv stream.

        Args:
            df (Union[pd.DataFrame, pd.Series]): pandas dataframe
            metadata (dict, optional): Additional metadata for s3 object. Defaults to
                None.
            **kwargs: keyword arguments to pass to pandas.to_csv method.

        Returns:
            S3Resource: S3Resource with a csv stream.
        """
        stream = io.StringIO()
        df.to_csv(stream, **kwargs)
        # set buffer position to beginning as there should not be any write
        # operation after this.
        stream.seek(0)
        return S3Resource[StringOrBytes](
            filename="",
            content_type="application/csv",
            stream=stream,
            metadata={"source": "%s" % type(df), **(metadata or {})},
        )

    @staticmethod
    def wrap_pandas_as_json(
        df: Union[pd.DataFrame, pd.Series], metadata: dict = None, **kwargs: dict
    ) -> S3Resource:
        """
        wrap_pandas_as_json returns a S3Resource with a json stream.

        Args:
            df (Union[pd.DataFrame, pd.Series]): pandas dataframe
            metadata (dict, optional): Additional metadata for s3 object. Defaults to
                None.
            **kwargs: keyword arguments to pass to pandas.to_json method.

        Returns:
            S3Resource: S3Resource with a json stream.
        """
        stream = io.StringIO()
        df.to_json(stream, **kwargs)
        # set buffer position to beginning as there should not be any write
        # operation after this.
        stream.seek(0)
        return S3Resource[StringOrBytes](
            filename="",
            content_type="application/json",
            stream=stream,
            metadata={"source": "%s" % type(df), **(metadata or {})},
        )

    @staticmethod
    def wrap_io(
        stream: IO[StringOrBytes], content_type: str, metadata: dict = None
    ) -> S3Resource:
        """
        wrap_io wraps returns a S3Resource with the io stream.

        Args:
            stream (IO): io stream
            content_type (str): mime type for the data
            metadata (dict, optional): Additional metadata for s3 object. Defaults to
                None.

        Returns:
            S3Resource: S3Resource with the io stream.
        """

        return S3Resource[StringOrBytes](
            filename="",
            content_type=content_type,
            stream=stream,
            metadata={"source": "%s" % type(stream), **(metadata or {})},
        )

    @classmethod
    def wrap_file(
        cls, filepath: str, content_type: str = "", metadata: dict = None
    ) -> S3Resource:
        """
        wrap_file returns a S3Resource with a binary data stream.
        If content_type is not provided, method will attempt to infer from the
        file.

        Args:
            filepath (str): path to the file.
            content_type (str, optional): mime type. Defaults to "".
            metadata (dict, optional): Additional metadata for s3 object. Defaults to
                None.

        Raises:

            ValueError: Unable to infer mime type because python-magic is not
                available. Please provide the content_type for the filepath.

        Returns:
            S3Resource: [description]
        """
        content_type = content_type or cls._infer_mime(filepath)

        prefix = os.path.dirname(filepath) or ""
        filename = os.path.basename(filepath)

        return S3Resource[bytes](
            filename=filename,
            content_type=content_type,
            prefix=prefix,
            stream=open(filepath, "rb"),
            metadata={"source": filepath, **(metadata or {})},
        )

    @staticmethod
    def _infer_mime(filepath: str) -> str:
        """Infer the mime type of the file."""
        if not LIB_MAGIC_AVAILABLE:
            raise ValueError(
                """
                Unable to infer mime type because python-magic is not available.
                Please provide the content_type for the filepath.

                For debian or ubuntu machines, you might need to also install:

                ```
                sudo apt-get install libmagic-dev
                ```
                """
            )
        import magic

        return magic.from_file(filepath, mime=True)  # type: ignore


class S3Bucket:
    """
    S3Bucket models a s3 bucket configuration.

    Example::

        from e2fyi.utils.aws import S3Bucket

        # upload a dict to s3 bucket
        S3Bucket("foo").upload("some_folder/some_file.json", {"foo": "bar"})

        # creates a s3 bucket with std prefix rule
        foo_bucket = S3Bucket("foo",
                              get_prefix=lambda prefix: "some_folder/%s" % prefix)
        foo_bucket.upload("some_file.json", {"foo": "bar"})

        # list files
        S3Bucket("foo").list("some_folder/")

        # list files inside "some_folder/"
        foo_bucket.list()

    """

    def __init__(
        self,
        name: str,
        get_prefix: Callable[[str], str] = _noop,
        s3client: boto3.client = None,
    ):
        """
        Creates a new instance of s3 bucket.

        Args:
            name (str): name of the bucket
            get_prefix (Callable[[str], str], optional): function that takes a filename
                and return the full path to the resource in the bucket.
            s3client (boto3.Session.client, optional): use a custom boto3
                s3 client.
        """
        self.name = name
        self.prefix = get_prefix("")
        self._get_prefix = get_prefix
        self._s3client = s3client or boto3.client("s3")

    def __str__(self) -> str:
        return self.name

    def create_resource_key(self, filename: str) -> str:
        """
        Creates a resource key based on the s3 bucket name, and configured prefix.

        Example::

            from e2fyi.utils.aws import S3Bucket

            s3 = S3Bucket(name="foo", get_prefix=lambda x: "bar/%s" % x)

            print(s3.create_resource_key("hello.world"))  # > bar/hello.world

        Args:
            filename (str): path for the file.

        Returns:
            str: key for the resource in s3.
        """
        return self._get_prefix(filename)

    def create_resource_uri(self, filename: str, protocol: str = "s3a://") -> str:
        """
        Create a resource uri based on the s3 bucket name, and configured prefix.

        Example::

            from e2fyi.utils.aws import S3Bucket

            s3 = S3Bucket(name="foo", get_prefix=lambda x: "bar/%s" % x)

            print(s3.create_resource_uri("hello.world"))  # > s3a://foo/bar/hello.world

        Args:
            filename (str): path for the file.
            protocol (str, optional): protocol for the uri. Defaults to "s3a://".

        Returns:
            str: uri string for the resource.
        """
        return "%s%s/%s" % (protocol, self.name, self.create_resource_key(filename))

    def upload(
        self,
        filepath: str,
        body: Union[
            IO[StringOrBytes],
            str,
            bytes,
            dict,
            BaseModel,
            pd.DataFrame,
            pd.Series,
            S3Resource[StringOrBytes],
        ],
        content_type: str = "text/plain",
        protocol: str = "s3a://",
        metadata: dict = None,
        **kwargs
    ) -> Result[S3Resource[StringOrBytes]]:
        """Upload a payload to s3 bucket."""

        key = self._get_prefix(filepath)
        try:
            resource = S3ResourceHelper.wrap(body, content_type, **kwargs)
            resource.bucketname = self.name
            resource.protocol = protocol
            resource.prefix = (
                os.path.join(resource.prefix, os.path.dirname(key))
                if resource.prefix
                else os.path.dirname(key) + os.path.sep
            )
            resource.filename = os.path.basename(key)
            if metadata:
                resource.metadata.update(metadata)

            self._s3client.upload_fileobj(
                resource,
                Bucket=resource.bucketname,
                Key=resource.key,
                ExtraArgs={
                    "ContentType": resource.content_type,
                    "Metadata": resource.metadata or {},
                },
                Callback=lambda n: logging.info(
                    "[%s/%s] bytes transferred: %d",
                    resource.bucketname,
                    resource.key,
                    n,
                ),
            )
        except (botocore.exceptions.ClientError, ValueError) as exc:
            return Result(None, exception=exc)

        return Result(resource)

    def list(
        self, prefix: str = "", max_keys: int = 255, within_project: bool = True
    ) -> Result[List[str]]:
        """Get a list of keys in an S3 bucket."""
        # constraint list to within project
        if within_project:
            prefix = self._get_prefix(prefix)
        try:
            resp = boto3.client("s3").list_objects_v2(
                Bucket=self.name, Prefix=prefix, MaxKeys=max_keys
            )
            items = [obj["Key"] for obj in resp["Contents"]]
        except botocore.exceptions.ClientError as exc:
            return Result(None, exception=exc)
        return Result(items)

    def create_resource(
        self,
        filename: str,
        content_type: str,
        protocol: str = "s3a://",
        stream: Union[io.StringIO, io.BytesIO, IO[StringOrBytes]] = None,
        metadata: Dict[str, str] = None,
    ) -> S3Resource:
        """
        create_s3_resource creates a new instance of S3Resource binds to the
        current bucket.

        Args:
            filename (str): name of the resource.
            content_type (str): mime type.
            protocol (str, optional): protocol. Defaults to "s3a://".
            stream (Union[io.StringIO, io.BytesIO, IO[StringOrBytes]], optional):
                content of the resource. Defaults to None.
            metadata (Dict[str, str], optional): metadata for the resource.
                Defaults to None.

        Returns:
            S3Resource: a S3Resource related to the active S3Bucket.
        """
        return S3Resource(
            filename=filename,
            content_type=content_type,
            protocol=protocol,
            bucketname=self.name,
            prefix=self.prefix,
            stream=stream,
            metadata=metadata,
        )
