import io
import tempfile
import functools

from typing import Tuple, Union, TypeVar, Iterator, Optional

import requests

from requests_toolbelt.streaming_iterator import StreamingIterator

IntermediateObj = TypeVar("IntermediateObj")
StringOrBytes = TypeVar("StringOrBytes", str, bytes)


class HttpStream:
    INMEM_SIZE: int = 0

    def __init__(
        self,
        uri: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: str = None,
        newline: str = None,
        inmem_size: int = None,
        decode_unicode: bool = False,
        delimiter: Union[str, bytes] = None,
        chunk_size: int = io.DEFAULT_BUFFER_SIZE,
        **kwargs,
    ):
        self.uri = uri
        self.mode = mode
        self.buffering = buffering
        self.newline = newline
        self.encoding = encoding

        self._decode_unicode = decode_unicode
        self._delimiter = delimiter
        self._chunk_size = chunk_size
        self._inmem_size = inmem_size
        self._kwargs = kwargs
        self._file = self._tempfile()
        self._state: Optional[requests.Response] = None

    def _tempfile(self) -> tempfile.SpooledTemporaryFile:
        return tempfile.SpooledTemporaryFile(
            max_size=self._inmem_size or self.INMEM_SIZE,
            mode="w+b" if "b" in self.mode else "w+",
            buffering=self.buffering,
            encoding=self.encoding,
            newline=self.newline,
        )

    @property
    def closed(self) -> bool:
        return self._file.closed

    def seek(self, offset, whence: int = 0):
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        return self._file.tell()

    def close(self):
        return self._file.close()

    def write(self, data: Union[str, bytes, bytearray]) -> int:
        return self._file.write(data)

    def is_empty(self) -> bool:
        current = self.tell()
        # go to the end of stream
        self.seek(0, 2)
        is_empty = self.tell() == 0
        self.seek(current)
        return is_empty

    def read(self, size: Optional[int] = -1) -> Union[str, bytes, bytearray]:
        if not self._state:
            self._state = HttpStream._read2state(self.uri, **self._kwargs)
            self._file = HttpStream._state2fileobj(self._state, self._tempfile())
            self.seek(0)
        return self._file.read(size)  # type: ignore

    def flush(self):
        self._file.flush()

    @staticmethod
    def _read2state(uri: str, **kwargs) -> requests.Response:
        state = requests.get(uri, **kwargs)
        state.raise_for_status()
        return state

    @staticmethod
    def _state2fileobj(
        state: requests.Response, fileobj: tempfile.SpooledTemporaryFile
    ) -> tempfile.SpooledTemporaryFile:
        if "b" in fileobj.mode:
            fileobj.write(state.content)
        else:
            fileobj.write(state.text)
        return fileobj

    @classmethod
    def _read2fileobj(
        cls, uri: str, fileobj: tempfile.SpooledTemporaryFile, **kwargs
    ) -> Tuple[requests.Response, tempfile.SpooledTemporaryFile]:
        state = cls._read2state(uri, **kwargs)
        file_ = cls._state2fileobj(state, fileobj)
        file_.seek(0)
        return state, file_

    def _cleanup(self):
        self._state = None
        if self._file:
            self._file.close()

    def __iter__(self) -> Iterator[Union[str, bytes, bytearray]]:
        if not self._state:
            self._state, self._file = HttpStream._read2fileobj(
                self.uri, self._tempfile(), **self._kwargs
            )
        if "b" in self.mode:
            return self._state.iter_content(
                chunk_size=self._chunk_size, decode_unicode=self._decode_unicode
            )
        return self._state.iter_lines(
            chunk_size=self._chunk_size,
            decode_unicode=self._decode_unicode,
            delimiter=self._delimiter,  # type: ignore
        )

    def __enter__(self) -> "HttpStream":
        if "w" in self.mode:
            self._file.close()
            self._file = self._tempfile()
            return self

        if "a" in self.mode:
            if not self._state:
                self._file.close()
                self._state, self._file = HttpStream._read2fileobj(
                    self.uri, self._tempfile(), **self._kwargs
                )
            self.seek(0, 2)  # go to end of stream
            return self

        self.seek(0)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if "w" in self.mode or "a" in self.mode:
                read = functools.partial(self._file.read, io.DEFAULT_BUFFER_SIZE)
                iter_data = StreamingIterator(self.tell(), iter(read, ""))
                self.seek(0)
                resp = requests.post(self.uri, data=iter_data, **self._kwargs)
                resp.raise_for_status()
        finally:
            self._file.close()
            self._state = None
