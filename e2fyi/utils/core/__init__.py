"""
Core helpers.


Wrapping function with Result::

    import logging

    from e2fyi.utils.core import Result


    def load_from_file(filepath: str) -> Result[string]:
        try:
            with open(filepath, "r") as fp:
                return Result(fp.read())
        except IOError as err:
            return Result(exception=err)

    data = load_from_file("some_file.json")

    # print with a default value fallback
    print(data.with_default("default value"))

    # print data if ok, else log exception
    if data.is_ok:
        print(data)
    else:
        logging.exception(data.exception)

"""
from e2fyi.utils.core.maybe import Maybe
from e2fyi.utils.core.results import Result
