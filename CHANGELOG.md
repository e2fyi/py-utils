# `e2fyi.utils`

## v0.2.0

- New features:

  - `S3Stream` is a wrapper class for any io streams, with static methods to convert any python objects to a stream.
  - Added `S3Resource.save` to upload resource to S3 bucket.

- Changes:

  - `S3Bucket.list` now return a generator which yields `S3Resource` in the S3 bucket, instead of a list of string.
  - Renamed `Result` into `Maybe`.

- Deprecations
  - Deprecated `S3ResourceHelper` (use `S3Stream.from_any` instead).
  - Deprecated `S3Bucket.upload` (use `S3Resource.save` instead).
  - Removed `e2fyi.utils.io` (empty module).

## v0.1.0

- Patches:
  - Catch exception if `python-magic` is not installed properly.

## v0.1.0a4

- New features:
  - Added methods: `create_resource_key`, `create_resource_uri`, and `create_resource` to S3Bucket.

## v0.1.0a3

- Bug fixes
  - incuded `e2fyi.utils.aws`, `e2fyi.utils.core` and `e2fyi.utils.io` into setup.py

## v0.1.0a2

- Bug fixes
  - included requirements.txt into setup.py

## v0.1.0a1.post1

- Added README.md to pypi.

## v0.1.0a1

- Initial release.
