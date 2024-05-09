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
from datetime import timedelta

import jmespath


def deprecated(message=""):
    """
    Define a deprecated decorator without dependencies on 3rd party libraries

    Note there is a 3rd party library called `deprecated` that provides this feature but goal is to only have
    dependencies on packages already used in the Databricks runtime
    """

    # create closure around function that follows use of the decorator

    def deprecated_decorator(func):
        @functools.wraps(func)
        def deprecated_func(*args, **kwargs):
            warnings.warn(f"`{func.__name__}` is a deprecated function or method. \n{message}",
                          category=DeprecationWarning, stacklevel=1)
            warnings.simplefilter('default', DeprecationWarning)
            return func(*args, **kwargs)

        return deprecated_func

    return deprecated_decorator


class DataGenError(Exception):
    """Exception class to represent data generation errors

        :param msg: message related to error
        :param baseException: underlying exception, if any that caused the issue
    """

    def __init__(self, msg, baseException=None):
        """ constructor
        """
        super().__init__(msg)
        self._underlyingException = baseException
        self._msg = msg

    def __repr__(self):
        return f"DataGenError(msg='{self._msg}', baseException={self._underlyingException})"

    def __str__(self):
        return f"DataGenError(msg='{self._msg}', baseException={self._underlyingException})"


def coalesce_values(*args):
    """For a supplied list of arguments, returns the first argument that does not have the value `None`

    :param args: variable list of arguments which are evaluated
    :returns: First argument in list that evaluates to a non-`None` value
    """
    for x in args:
        if x is not None:
            return x
    return None


def ensure(cond, msg="condition does not hold true"):
    """ensure(cond, s) => throws Exception(s) if c is not true

    :param cond: condition to test
    :param msg: Message to add to exception if exception is raised
    :raises: `DataGenError` exception if condition does not hold true
    :returns: Does not return anything but raises exception if condition does not hold
    """

    def strip_margin(text):
        return re.sub(r'\n[ \t]*\|', '\n', text)

    if not cond:
        raise DataGenError(strip_margin(msg))


def mkBoundsList(x, default):
    """ make a bounds list from supplied parameter - otherwise use default

        :param x: integer or list of 2 values that define bounds list
        :param default: default value if X is `None`
        :returns: list of form [x,y]
    """
    if x is None:
        retval = (True, [default, default]) if type(default) is int else (True, list(default))
        return retval
    elif type(x) is int:
        bounds_list = [x, x]
        assert len(bounds_list) == 2, "bounds list must be of length 2"
        return False, bounds_list
    else:
        bounds_list = list(x)
        assert len(bounds_list) == 2, "bounds list must be of length 2"
        return False, bounds_list


def topologicalSort(sources, initial_columns=None, flatten=True):
    """ Perform a topological sort over sources

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
    pending = [(name, set(deps)) for name, deps in sources]
    provided = [] if initial_columns is None else initial_columns[:]
    build_orders = [] if initial_columns is None else [initial_columns]

    while pending:
        next_pending = []
        gen = []
        value_emitted = False
        defer_emitted = False
        gen_provided = []
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
        flattened_list = [item for sublist in build_orders for item in sublist]
        return flattened_list
    else:
        return build_orders


PATTERN_NAME_EQUALS_VALUE = re.compile(r"(\w+)\s*\=\s*([0-9]+)")
PATTERN_VALUE_SPACE_NAME = re.compile(r"([0-9]+)\s+(\w+)")
_WEEKS_PER_YEAR = 52


def parse_time_interval(spec):
    """parse time interval from string"""
    hours = 0
    minutes = 0
    weeks = 0
    microseconds = 0
    milliseconds = 0
    seconds = 0
    years = 0
    days = 0

    assert spec is not None, "Must have valid time interval specification"

    # get time specs such as 12 days, etc. Supported timespans are years, days, hours, minutes, seconds
    timespecs = [x.strip() for x in spec.strip().split(",")]

    for ts in timespecs:
        # allow both 'days=1' and '1 day' syntax
        timespec_parts = re.findall(PATTERN_NAME_EQUALS_VALUE, ts)
        # findall returns list of tuples
        if timespec_parts is not None and len(timespec_parts) > 0:
            num_parts = len(timespec_parts[0])
            assert num_parts >= 1, "must have numeric specification and time element such as `12 hours` or `hours=12`"
            time_value = int(timespec_parts[0][num_parts - 1])
            time_type = timespec_parts[0][0].lower()
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

    delta = timedelta(
        days=days,
        seconds=seconds,
        microseconds=microseconds,
        milliseconds=milliseconds,
        minutes=minutes,
        hours=hours,
        weeks=weeks + (years * _WEEKS_PER_YEAR)
    )

    return delta


def strip_margins(s, marginChar):
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
    assert s is not None and type(s) is str
    assert marginChar is not None and type(marginChar) is str

    lines = s.split('\n')
    revised_lines = []

    for line in lines:
        if marginChar in line:
            revised_line = line[line.index(marginChar) + 1:]
            revised_lines.append(revised_line)
        else:
            revised_lines.append(line)

    return '\n'.join(revised_lines)


def split_list_matching_condition(lst, cond):
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

    :arg lst: list of items to perform condition matches against
    :arg cond: lambda function or function taking single argument and returning True or False
    :returns: list of sublists
    """
    retval = []

    def match_condition(matchList, matchFn):
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
        ix = match_condition(lst, cond)
        if ix != -1:
            retval.extend(split_list_matching_condition(lst[0:ix], cond))
            retval.append(lst[ix:ix + 1])
            retval.extend(split_list_matching_condition(lst[ix + 1:], cond))
        else:
            retval = [lst]

    # filter out empty lists
    return [el for el in retval if el != []]


def json_value_from_path(searchPath, jsonData, defaultValue):
    """ Get JSON value from JSON data referenced by searchPath

    searchPath should be a JSON path as supported by the `jmespath` package
    (see https://jmespath.org/)

    :param searchPath: A `jmespath` compatible JSON search path
    :param jsonData: The json data to search (string representation of the JSON data)
    :param defaultValue: The default value to be returned if the value was not found
    :return: Returns the json value if present, otherwise returns the default value
    """
    assert searchPath is not None and len(searchPath) > 0, "search path cannot be empty"
    assert jsonData is not None and len(jsonData) > 0, "JSON data cannot be empty"

    jsonDict = json.loads(jsonData)

    jsonValue = jmespath.search(searchPath, jsonDict)

    if jsonValue is not None:
        return jsonValue

    return defaultValue


def system_time_millis():
    """ return system time as milliseconds since start of epoch

    :return: system time millis as long
    """
    curr_time = round(time.time() / 1000)
    return curr_time
