"""
Module for compatability so that no errors will be thrown even if a optional
package is not available.
"""
import logging

try:
    import magic  # noqa pylint: disable=unused-import

    LIB_MAGIC_AVAILABLE = True
except ImportError as exc:
    LIB_MAGIC_AVAILABLE = False
    logging.warning(
        """
        Unable to load python package[python-magic]:

        You can install it manually with the following commands:
        ```
        # macOS/windows
        pip install python-magic-bin==0.4.*

        # Debian/Ubuntu
        sudo apt-get install libmagic-dev
        pip install python-magic==0.4.*
        ```

        Exception: %s
        """,
        exc,
    )
