"""
Provides `S3Stream` which represents the data stream to and from S3 buckets.
"""
import io
import json
import logging
import os.path

from typing import IO, Any, Union, Generic, TypeVar, BinaryIO

import joblib
import pandas as pd

from pydantic import BaseModel  # pylint: disable=no-name-in-module

from e2fyi.utils.aws.compat import LIB_MAGIC_AVAILABLE

StringOrBytes = TypeVar("StringOrBytes", bytes, str)


def _infer_mime(filepath: str) -> str:
    """Infer the mime type of the file. Returns "application/octet-stream" if
        unable to infer mime type."""
    if not LIB_MAGIC_AVAILABLE:
        logging.warning(
            """
                Unable to infer mime type because python-magic is not available.
                Please provide the content_type for the filepath.

                For debian or ubuntu machines, you might need to also install:

                ```
                sudo apt-get install libmagic-dev
                ```
                """
        )
        return "application/octet-stream"
    import magic  # pylint: disable=import-outside-toplevel

    return magic.from_file(filepath, mime=True)  # type: ignore


class S3Stream(Generic[StringOrBytes]):
    """
    `S3Stream` represents the data stream of a S3 resource, and provides static
    methods to convert any python objects into a stream. This is generally used with
    `S3Resource` to upload or download resource from S3 buckets.

    Examples::

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
    """

    def __init__(
        self,
        stream: Union[IO, BinaryIO, io.BytesIO, io.StringIO],
        content_type: str = "application/octet-stream",
    ):
        """
        Creates a new S3Stream.

        Args:
            stream (Union[IO, BinaryIO, io.BytesIO, io.StringIO]): any stream object.
            content_type (str, optional): mime type for the data in the stream.
                Defaults to "application/octet-stream".
        """
        self.stream = stream
        self.content_type = content_type

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

    def close(self) -> "S3Stream":
        """Close the resource stream."""
        self.stream.close()
        return self

    def get_value(self) -> StringOrBytes:
        """Retrieve the entire contents of the S3Resource."""
        self.seek(0)
        return self.read()

    @classmethod
    def from_any(
        cls, obj: Any, content_type: str = "", output_as="csv", **kwargs
    ) -> "S3Stream":
        """
        Returns a S3Stream from any python object.

        Args:
            obj (Any): any python object.
            content_type (str, optional): mime type. Defaults to "".
            output_as (str, optional): format to output as if obj is a pandas object.
                Defaults to "csv".
            **kwargs: additional keyword arguments to pass to `pandas.to_csv`,
                `pandas.to_json`, `json.dumps` or `joblib.dumps` methods
                depending on the input object.

        Returns:
            S3Stream: S3Stream object.
        """
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            return cls.from_pandas(obj, output_as=output_as, **kwargs)
        if hasattr(obj, "read") and callable(obj.read):
            return cls.from_io(obj, content_type)
        return cls.from_object(obj, content_type, **kwargs)

    @staticmethod
    def from_file(filepath: str, content_type: str = "") -> "S3Stream":
        """
        Returns a S3Stream from a file. If content_type is not provided,
        `python-magic` will be used to infer the mime type from the file data.

        Args:
            filepath (str): path to the file.
            content_type (str, optional): mime type of the file. Defaults to "".

        Returns:
            [type]: [description]
        """
        return S3Stream(open(filepath, "rb"), content_type or _infer_mime(filepath))

    @classmethod
    def from_object(
        cls,
        obj: Union[str, bytes, dict, BaseModel, object],
        content_type: str = "application/octet-stream",
        **kwargs
    ) -> "S3Stream":
        """
        Returns a S3Stream from any string, bytes, dict, pydantic models, or
        any python object.

        Dicts and pydantic models will be converted into a JSON string stream with
        `json.dumps` and the content type "application/json".

        Anything that is not a string, bytes, dict, or pydantic model will be
        converted into a pickle binary stream with `joblib`.

        Any extra keyword arguments will be passed to `json.dumps` or `joblib`.

        See:
        https://joblib.readthedocs.io/en/latest/generated/joblib.dump.html#joblib.dump

        Args:
            obj (Union[str, bytes, dict, pydantic.BaseModel, object]): [description]
            content_type (str, optional): [description]. Defaults to
                "application/octet-stream".
            **kwargs: Additional keyword arguments to pass to `joblib.dump` or
                `json.dumps`.

        Returns:
            S3Stream: S3Stream object.
        """
        if isinstance(obj, (str, int, float, bool)):
            obj = str(obj)
            if os.path.isfile(obj):
                return cls.from_file(obj, content_type)
            # set mime to text/plain for string input if content_type not provided.
            content_type = (
                "text/plain"
                if content_type == "application/octet-stream"
                else content_type
            )
            return cls.from_io(io.StringIO(obj), content_type)

        if isinstance(obj, bytes):
            return cls.from_io(io.BytesIO(obj), content_type)

        if isinstance(obj, (dict, list, BaseModel)):
            if isinstance(obj, BaseModel):
                obj = obj.dict()

            try:
                return cls.from_io(
                    io.StringIO(json.dumps(obj, **kwargs)), "application/json"
                )
            except TypeError as error:
                logging.warning(
                    "Serializing as pickle because unable to encode as JSON: %s", error
                )

        stream = io.BytesIO()
        joblib.dump(obj, stream, **kwargs)
        return S3Stream[bytes](stream, content_type)

    @staticmethod
    def from_pandas(
        df: Union[pd.DataFrame, pd.Series], output_as: str = "csv", **kwargs: dict
    ) -> "S3Stream":
        """
        Returns a S3Stream object from a pandas dataframe or series. When output
        as a "csv", content type will be "application/csv", otherwise it will
        be "application/json".

        Example::

            import pandas

            from e2fyi.utils.aws.s3_stream import S3Stream

            # create some pandas dataframe
            df = pd.DataFrame([...])

            # create a csv stream, and don't output an index column.
            csv_stream = S3Stream.from_pandas(df, index=False)

            # create a json stream - output as records
            json_stream = S3Stream.from_pandas(df, orient="records")

        Args:
            df (Union[pd.DataFrame, pd.Series]): pandas dataframe or series.
            output_as (str, optional): either "csv" or "json". Defaults to "csv".
            **kwargs: additional keyword arguments to pass to either `pandas.to_csv`
                or `pandas.to_json` methods.

        Returns:
            S3Stream: S3Stream object.
        """
        if output_as == "csv":
            stream = io.StringIO()
            df.to_csv(stream, **kwargs)
            stream.seek(0)
            return S3Stream(stream, "application/csv")

        stream = io.StringIO()
        df.to_json(stream, **kwargs)
        # set buffer position to beginning as there should not be any write
        # operation after this.
        stream.seek(0)
        return S3Stream[str](stream, "application/json")

    @staticmethod
    def from_io(
        stream: Union[IO, BinaryIO, io.StringIO, io.BytesIO],
        content_type: str = "application/octet-stream",
    ) -> "S3Stream":
        """
        Returns a S3Stream object from an io stream.

        Args:
            stream (Union[IO, BinaryIO, io.StringIO, io.BytesIO]): any
                stream object.
            content_type (str, optional): mime type of the stream. Defaults to
                "application/octet-stream".

        Returns:
            S3Stream: S3Stream object.
        """

        return S3Stream[StringOrBytes](stream, content_type)
