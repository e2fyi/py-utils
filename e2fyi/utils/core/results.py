"""
This module is deprecated since v0.2.0.
"""
from typing import Generic, TypeVar, Optional

T = TypeVar("T")


class Result(Generic[T]):
    """
    `Result` is deprecated since v0.2.0. Please use `e2fyi.utils.core.Maybe` instead.
    """

    def __init__(self, value: Optional[T] = None, exception: BaseException = None):
        """Deprecated class."""
        raise DeprecationWarning(
            "`Result` is deprecated since v0.2.0. Please use `e2fyi.utils.core.Maybe` "
            "instead."
        )
