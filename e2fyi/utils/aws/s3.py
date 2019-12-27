"""utils to interact with s3 buckets."""
from typing import Any, Dict, TypeVar, Callable, Generator

import boto3

from e2fyi.utils.aws.s3_stream import S3Stream
from e2fyi.utils.aws.s3_resource import S3Resource

T = TypeVar("T")
StringOrBytes = TypeVar("StringOrBytes", bytes, str)

ALLOWED_DOWNLOAD_ARGS = [
    "VersionId",
    "SSECustomerAlgorithm",
    "SSECustomerKey",
    "SSECustomerKeyMD5",
    "RequestPayer",
]

ALLOWED_UPLOAD_ARGS = [
    "ACL",
    "CacheControl",
    "ContentDisposition",
    "ContentEncoding",
    "ContentLanguage",
    "ContentType",
    "Expires",
    "GrantFullControl",
    "GrantRead",
    "GrantReadACP",
    "GrantWriteACP",
    "Metadata",
    "RequestPayer",
    "ServerSideEncryption",
    "StorageClass",
    "SSECustomerAlgorithm",
    "SSECustomerKey",
    "SSECustomerKeyMD5",
    "SSEKMSKeyId",
    "WebsiteRedirectLocation",
]


def _noop(key: T) -> T:
    """Do nothing"""
    return key


class S3Bucket:
    """
    `S3Bucket` is an abstraction of the actual S3 bucket with methods to interact
    with the actual S3 bucket (e.g. list objects inside the bucket), and some utility
    methods.

    Prefix rules can also be set during the creation of the `S3Bucket` object - i.e.
    enforce a particular prefix rules for a particular bucket.

    Example::

        from e2fyi.utils.aws import S3Bucket

        # prints key for all resources with prefix "some_folder/"
        for resource in S3Bucket("some_bucket").list("some_folder/"):
            print(resource.key)

        # prints key for the first 2,000 resources with prefix "some_folder/"
        for resource in S3Bucket("some_bucket").list("some_folder/", max_objects=2000):
            print(resource.key)

        # creates a s3 bucket with prefix rule
        prj_bucket = S3Bucket(
            "some_bucket", get_prefix=lambda prefix: "prj-a/%s" % prefix
        )
        for resource in prj_bucket.list("some_folder/"):
            print(resource.key)  # prints "prj-a/some_folder/<resource_name>"

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

    def list(
        self, prefix: str = "", within_project: bool = True, max_objects: int = -1
    ) -> Generator[S3Resource[bytes], None, None]:
        """
        Returns a generator that yield S3Resource objects inside the S3Bucket
        that matches the provided prefix.

        Example::

            # prints key for all resources with prefix "some_folder/"
            for resource in S3Bucket("some_bucket").list("some_folder/"):
                print(resource.key)

            # prints key for the first 2,000 resources with prefix "some_folder/"
            for resource in S3Bucket(
                "some_bucket").list("some_folder/", max_objects=2000
            ):
                print(resource.key)

            # creates a s3 bucket with prefix rule
            prj_bucket = S3Bucket(
                "some_bucket", get_prefix=lambda prefix: "prj-a/%s" % prefix
            )
            for resource in prj_bucket.list("some_folder/"):
                print(resource.key)  # prints "prj-a/some_folder/<resource_name>"


        Args:
            prefix (str, optional): [description]. Defaults to "".
            within_project (bool, optional): [description]. Defaults to True.
            max_objects (int, optional): max number of object to return. Negative
                or zero means all objects will be returned. Defaults to -1.

        Returns:
            Generator[S3Resource[bytes], None, None]: [description]

        Yields:
            Generator[S3Resource[bytes], None, None]: [description]
        """
        count = 0

        # constraint list to within project
        if within_project:
            prefix = self._get_prefix(prefix)

        continuation_token = None

        def to_s3_resource(item):
            """Converts the response object from s3.list_objects_v2 into a
            S3Resource."""
            key = item.get("Key", "")
            chunks = key.split("/")
            if len(chunks) >= 2:
                filename = chunks[-1]
                prefix = "%s/" % "/".join(chunks[0:-1])
            else:
                filename = key
                prefix = ""

            return S3Resource(
                filename=filename,
                content_type="application/octet-stream",
                prefix=prefix,
                bucketname=self.name,
                s3client=self._s3client,
                stats=item,
            )

        while True:
            list_kwargs = dict(MaxKeys=1000, Prefix=prefix, Bucket=self.name)
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token
            response = self._s3client.list_objects_v2(**list_kwargs)
            resources = [to_s3_resource(item) for item in response.get("Contents", [])]
            size = len(resources)

            # terminate if max_objects hit
            if count >= max_objects > 0:
                break

            # terminate if max_objects hit
            if count + size > max_objects > 0:
                yield from resources[: max_objects - count]
                break

            # yield normally
            for resource in resources:
                count += 1
                yield resource

            if not response.get("IsTruncated"):  # At the end of the list?
                break

            continuation_token = response.get("NextContinuationToken")

    def create_resource(
        self,
        filename: str,
        content_type: str = "",
        obj: Any = None,
        protocol: str = "s3a://",
        metadata: Dict[str, str] = None,
        pandas_kwargs: dict = None,
        **kwargs
    ) -> S3Resource:
        """
        Creates a new instance of S3Resource binds to the current bucket.

        Example::

            # create S3Resource in bucket to read in
            foo = prj_bucket.create_resource("foo.json", "application/json")
            # read "s3a://some_bucket/prj-a/foo.json" and load as a dict (or list)
            foo_dict = foo.load()

            # create S3Resource in bucket and save to "s3a://some_bucket/prj-a/foo.json"
            prj_bucket.create_resource("foo.json", obj={"foo": "bar"}).save()


        Args:
            filename (str): name of the resource.
            content_type (str, optional): mime type. Defaults to
                "application/octet-stream".
            obj (Any, optional): python object to convert into a resource. Defaults
                to None.
            protocol (str, optional): protocol. Defaults to "s3a://".
            stream (Union[io.StringIO, io.BytesIO, IO[StringOrBytes]], optional):
                content of the resource. Defaults to None.
            metadata (dict, optional): metadata for the object. Defaults to None.
            pandas_kwargs: Any additional args to pass to `pandas`.
            **kwargs: Any additional args to pass to `S3Resource`.

        Returns:
            S3Resource: a S3Resource related to the active S3Bucket.
        """
        stream = (
            S3Stream.from_any(obj, content_type, **(pandas_kwargs or {}))
            if obj is not None
            else None
        )

        if not content_type:
            if stream:
                content_type = stream.content_type

        return S3Resource(
            filename=filename,
            prefix=self.prefix,
            bucketname=self.name,
            protocol=protocol,
            content_type=content_type or "application/octet-stream",
            stream=stream,
            s3client=self._s3client,
            Metadata=metadata or {},
            **kwargs
        )

    def upload(  # pylint: disable=no-self-use
        self,
        filepath: str,
        body: Any,
        content_type: str = "text/plain",
        protocol: str = "s3a://",
        metadata: dict = None,
        **kwargs
    ) -> Any:
        """
        Deprecated since v0.2.0. Use S3Resource.save instead.
        """
        raise DeprecationWarning(
            "S3Bucket.upload is deprecated since v0.2.0. Please "
            "use S3Resource.save instead."
        )
