# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` classes and utility functions

These are meant for internal use only
"""

import functools
import json
import re
import time
import warnings
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import jmespath
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.streaming.query import StreamingQuery

from dbldatagen.config import OutputDataset
from dbldatagen.datagen_types import ColumnLike


def deprecated(message: str = "") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Define a deprecated decorator without dependencies on 3rd party libraries

    Note there is a 3rd party library called `deprecated` that provides this feature but goal is to only have
    dependencies on packages already used in the Databricks runtime
    """

    # create closure around function that follows use of the decorator
    def deprecated_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def deprecated_func(*args: object, **kwargs: object) -> object:
            warnings.warn(
                f"`{func.__name__}` is a deprecated function or method. \n{message}",
                category=DeprecationWarning,
                stacklevel=1,
            )
            warnings.simplefilter("default", DeprecationWarning)
            return func(*args, **kwargs)

        return deprecated_func

    return deprecated_decorator


class DataGenError(Exception):
    """Exception class to represent data generation errors

    :param msg: message related to error
    :param baseException: underlying exception, if any that caused the issue
    """

    def __init__(self, msg: str, baseException: object | None = None) -> None:
        """constructor"""
        super().__init__(msg)
        self._underlyingException: object | None = baseException
        self._msg: str = msg

    def __repr__(self) -> str:
        return f"DataGenError(msg='{self._msg}', baseException={self._underlyingException})"

    def __str__(self) -> str:
        return f"DataGenError(msg='{self._msg}', baseException={self._underlyingException})"


def coalesce_values(*args: object) -> object | None:
    """For a supplied list of arguments, returns the first argument that does not have the value `None`

    :param args: variable list of arguments which are evaluated
    :returns: First argument in list that evaluates to a non-`None` value
    """
    for x in args:
        if x is not None:
            return x
    return None


def ensure(cond: bool, msg: str = "condition does not hold true") -> None:
    """ensure(cond, s) => throws Exception(s) if c is not true

    :param cond: condition to test
    :param msg: Message to add to exception if exception is raised
    :raises: `DataGenError` exception if condition does not hold true
    :returns: Does not return anything but raises exception if condition does not hold
    """

    def strip_margin(text: str) -> str:
        return re.sub(r"\n[ \t]*\|", "\n", text)

    if not cond:
        raise DataGenError(strip_margin(msg))


def mkBoundsList(x: int | list[int] | None, default: int | list[int]) -> tuple[bool, list[int]]:
    """make a bounds list from supplied parameter - otherwise use default

    :param x: integer or list of 2 values that define bounds list
    :param default: default value if X is `None`
    :returns: list of form [x,y]
    """
    if x is None:
        retval = (True, [default, default]) if isinstance(default, int) else (True, list(default))
        return retval
    elif isinstance(x, int):
        bounds_list: list[int] = [x, x]
        assert len(bounds_list) == 2, "bounds list must be of length 2"
        return False, bounds_list
    else:
        bounds_list: list[int] = list(x)
        assert len(bounds_list) == 2, "bounds list must be of length 2"
        return False, bounds_list


def topologicalSort(
    sources: list[tuple[str, set[str]]], initial_columns: list[str] | None = None, flatten: bool = True
) -> list[str] | list[list[str]]:
    """Perform a topological sort over sources

    Used to compute the column test data generation order of the column generation dependencies.

    The column generation dependencies are based on the value of the `baseColumn` attribute for `withColumn` or
    `withColumnSpec` statements in the data generator specification.

    :arg sources: list of ``(name, set(names of dependencies))`` pairs
    :arg initial_columns: force ``initial_columns`` to be computed first
    :arg flatten: if true, flatten output list
    :returns: list of names in dependency order separated into build phases

    .. note::
       The algorith will give preference to retaining order of inbound sequence
       over modifying order to produce a lower number of build phases.

       Overall the effect is that the input build order should be retained unless there are forward references
    """
    # generate a copy so that we can modify in place
    pending: list[tuple[str, set[str]]] = [(name, set(deps)) for name, deps in sources]
    provided: list[str] = [] if initial_columns is None else initial_columns[:]
    build_orders: list[list[str]] = [] if initial_columns is None else [initial_columns]

    while pending:
        next_pending: list[tuple[str, set[str]]] = []
        gen: list[str] = []
        value_emitted: bool = False
        defer_emitted: bool = False
        gen_provided: list[str] = []
        for entry in pending:
            name, deps = entry
            deps.difference_update(provided)
            if deps:
                next_pending.append((name, set(deps)))

                # if dependencies will be satisfied by item emitted in this round, defer output
                if not deps.difference(gen_provided):
                    defer_emitted = True
            elif defer_emitted:
                next_pending.append((name, set(deps)))
            elif name in provided:
                value_emitted = True
            else:
                gen.append(name)
                gen_provided.append(name)
                value_emitted = True
        provided.extend(gen_provided)
        build_orders.append(gen)

        if not value_emitted:
            raise ValueError(f"cyclic or missing dependency detected [{next_pending}]")

        pending = next_pending

    if flatten:
        flattened_list: list[str] = [item for sublist in build_orders for item in sublist]
        return flattened_list
    else:
        return build_orders


PATTERN_NAME_EQUALS_VALUE = re.compile(r"(\w+)\s*\=\s*([0-9]+)")
PATTERN_VALUE_SPACE_NAME = re.compile(r"([0-9]+)\s+(\w+)")
_WEEKS_PER_YEAR = 52


def parse_time_interval(spec: str) -> timedelta:
    """parse time interval from string"""
    hours: int = 0
    minutes: int = 0
    weeks: int = 0
    microseconds: int = 0
    milliseconds: int = 0
    seconds: int = 0
    years: int = 0
    days: int = 0

    assert spec is not None, "Must have valid time interval specification"

    # get time specs such as 12 days, etc. Supported timespans are years, days, hours, minutes, seconds
    timespecs: list[str] = [x.strip() for x in spec.strip().split(",")]

    for ts in timespecs:
        # allow both 'days=1' and '1 day' syntax
        timespec_parts: list[tuple[str, str]] = re.findall(PATTERN_NAME_EQUALS_VALUE, ts)
        # findall returns list of tuples
        if timespec_parts is not None and len(timespec_parts) > 0:
            num_parts: int = len(timespec_parts[0])
            assert num_parts >= 1, "must have numeric specification and time element such as `12 hours` or `hours=12`"
            time_value: int = int(timespec_parts[0][num_parts - 1])
            time_type: str = timespec_parts[0][0].lower()
        else:
            timespec_parts = re.findall(PATTERN_VALUE_SPACE_NAME, ts)
            num_parts = len(timespec_parts[0])
            assert num_parts >= 1, "must have numeric specification and time element such as `12 hours` or `hours=12`"
            time_value = int(timespec_parts[0][0])
            time_type = timespec_parts[0][num_parts - 1].lower()

        if time_type in ["years", "year"]:
            years = time_value
        elif time_type in ["weeks", "weeks"]:
            weeks = time_value
        elif time_type in ["days", "day"]:
            days = time_value
        elif time_type in ["hours", "hour"]:
            hours = time_value
        elif time_type in ["minutes", "minute"]:
            minutes = time_value
        elif time_type in ["seconds", "second"]:
            seconds = time_value
        elif time_type in ["microseconds", "microsecond"]:
            microseconds = time_value
        elif time_type in ["milliseconds", "millisecond"]:
            milliseconds = time_value

    delta: timedelta = timedelta(
        days=days,
        seconds=seconds,
        microseconds=microseconds,
        milliseconds=milliseconds,
        minutes=minutes,
        hours=hours,
        weeks=weeks + (years * _WEEKS_PER_YEAR),
    )

    return delta


def strip_margins(s: str, marginChar: str) -> str:
    """
    Python equivalent of Scala stripMargins method
    Takes a string (potentially multiline) and strips all chars up and including the first occurrence of `marginChar`.
    Used to control the formatting of generated text
    `strip_margins("one\n    |two\n    |three", '|')`
    will produce
    ``
    one
    two
    three
    ``

    :param s: string to strip margins from
    :param marginChar: character to strip
    :return: modified string
    """
    assert s is not None and isinstance(s, str)
    assert marginChar is not None and isinstance(marginChar, str)

    lines: list[str] = s.split("\n")
    revised_lines: list[str] = []

    for line in lines:
        if marginChar in line:
            revised_line: str = line[line.index(marginChar) + 1 :]
            revised_lines.append(revised_line)
        else:
            revised_lines.append(line)

    return "\n".join(revised_lines)


def split_list_matching_condition(lst: list[Any], cond: Callable[[Any], bool]) -> list[list[Any]]:
    """
    Split a list on elements that match a condition

    This will find all matches of a specific condition in the list and split the list into sub lists around the
    element that matches this condition.

    It will handle multiple matches performing splits on each match.

    For example, the following code will produce the results below:

    x = ['id', 'city_name', 'id', 'city_id', 'city_pop', 'id', 'city_id', 'city_pop','city_id', 'city_pop','id']
    splitListOnCondition(x, lambda el: el == 'id')

    Result:
    `[['id'], ['city_name'], ['id'], ['city_id', 'city_pop'],
    ['id'], ['city_id', 'city_pop', 'city_id', 'city_pop'], ['id']]`

    :param lst: list of items to perform condition matches against
    :param cond: lambda function or function taking single argument and returning True or False
    :returns: list of sublists
    """
    retval: list[list[Any]] = []

    def match_condition(matchList: list[Any], matchFn: Callable[[Any], bool]) -> int:
        """Return first index of element of list matching condition"""
        if matchList is None or len(matchList) == 0:
            return -1

        for i, matchValue in enumerate(matchList):
            if matchFn(matchValue):
                return i

        return -1

    if lst is None:
        retval = lst
    elif len(lst) == 1:
        retval = [lst]
    else:
        ix: int = match_condition(lst, cond)
        if ix != -1:
            retval.extend(split_list_matching_condition(lst[0:ix], cond))
            retval.append(lst[ix : ix + 1])
            retval.extend(split_list_matching_condition(lst[ix + 1 :], cond))
        else:
            retval = [lst]

    # filter out empty lists
    return [el for el in retval if el != []]


def json_value_from_path(searchPath: str, jsonData: str, defaultValue: object) -> object:
    """Get JSON value from JSON data referenced by searchPath

    searchPath should be a JSON path as supported by the `jmespath` package
    (see https://jmespath.org/)

    :param searchPath: A `jmespath` compatible JSON search path
    :param jsonData: The json data to search (string representation of the JSON data)
    :param defaultValue: The default value to be returned if the value was not found
    :return: Returns the json value if present, otherwise returns the default value
    """
    assert searchPath is not None and len(searchPath) > 0, "search path cannot be empty"
    assert jsonData is not None and len(jsonData) > 0, "JSON data cannot be empty"

    jsonDict: dict = json.loads(jsonData)

    jsonValue: Any = jmespath.search(searchPath, jsonDict)

    if jsonValue is not None:
        return jsonValue

    return defaultValue


def system_time_millis() -> int:
    """return system time as milliseconds since start of epoch

    :return: system time millis as long
    """
    curr_time: int = round(time.time() / 1000)
    return curr_time


def write_data_to_output(df: DataFrame, output_dataset: OutputDataset) -> StreamingQuery | None:
    """
    Writes a DataFrame to the sink configured in the output configuration.

    :param df: Spark DataFrame to write
    :param output_dataset: Output dataset configuration passed as an `OutputDataset`
    :returns: A Spark `StreamingQuery` if data is written in streaming, otherwise `None`
    """
    if df.isStreaming:
        if not output_dataset.trigger:
            query = (
                df.writeStream.format(output_dataset.format)
                .outputMode(output_dataset.output_mode)
                .options(**output_dataset.options)
                .start(output_dataset.location)
            )
        else:
            query = (
                df.writeStream.format(output_dataset.format)
                .outputMode(output_dataset.output_mode)
                .options(**output_dataset.options)
                .trigger(**output_dataset.trigger)
                .start(output_dataset.location)
            )
        return query

    else:
        (
            df.write.format(output_dataset.format)
            .mode(output_dataset.output_mode)
            .options(**output_dataset.options)
            .save(output_dataset.location)
        )

    return None


def ensure_column(column: ColumnLike) -> Column:
    """
    Normalizes an input ``ColumnLike`` value into a Column expression.

    :param column: Column name as a string or a ``pyspark.sql.Column`` expression
    :return: ``pyspark.sql.Column`` expression
    """
    if isinstance(column, Column):
        return column

    if isinstance(column, str):
        return col(column)
