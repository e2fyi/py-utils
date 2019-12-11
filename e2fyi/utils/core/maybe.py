"""This module provides a Maybe generic class to describe an unreliable output.
"""
import json

from typing import Generic, TypeVar, Optional

T = TypeVar("T")


class Maybe(Generic[T]):
    """
    Maybe is a generic class used to describe an output that can potentially
    fails (e.g. raise exception).


    Example::

        import logging

        from e2fyi.utils.core import Maybe


        def load_from_file(filepath: str) -> Maybe[string]:
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
    """

    def __init__(self, value: Optional[T] = None, exception: BaseException = None):
        """Creates a new instance of Maybe. If an exception is provided, the
        Maybe value is considered to be not ok."""
        self.value = value
        self.exception = exception

    @property
    def is_ok(self) -> bool:
        """whether the output is generated successfully."""
        return not self.exception

    def with_default(self, default_value: T) -> T:
        """returns a default value if the outputs is not generated successfully."""
        return (self.value or default_value) if self.is_ok else default_value

    def __str__(self) -> str:
        """string representation"""
        if self.value and isinstance(self.value, list):
            return "\n".join(self.value)
        if self.value and isinstance(self.value, dict):
            return json.dumps(self.value, indent=2)
        return "%s" % (self.value or self.exception)
