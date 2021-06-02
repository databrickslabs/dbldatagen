# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` classes and utility functions

These are meant for internal use only
"""

import warnings
import functools

def deprecated(message=""):
  ''' Define a deprecated decorator without dependencies on 3rd party libraries

  Note there is a 3rd party library called `deprecated` that provides this feature but goal is to only have
  dependencies on packages already used in the Databricks runtime
  '''
  def deprecated_decorator(func):
      @functools.wraps(func)
      def deprecated_func(*args, **kwargs):
          warnings.warn("`{}` is a deprecated function or method. \n{}".format(func.__name__, message),
                        category=DeprecationWarning, stacklevel=1)
          warnings.simplefilter('default', DeprecationWarning)
          return func(*args, **kwargs)
      return deprecated_func
  return deprecated_decorator

class DataGenError(Exception):
    """Exception class to represent data generation errors"""
    pass


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
    import re

    def strip_margin(text):
        return re.sub('\n[ \t]*\|', '\n', text)

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

    The column generation dependencies are based on the value of the `base_column` attribute for `withColumn` or
    `withColumnSpec` statements in the data generator specification.

    :arg sources: list of ``(name, set(names of dependencies))`` pairs
    :arg initial_columns: force ``initial_columns`` to be computed first
    :arg flatten: if true, flatten output list
    :returns: list of names in dependency order. If not flattened, result will be list of lists
    """
    # generate a copy so that we can modify in place
    pending = [(name, set(deps)) for name, deps in sources]
    provided = [] if initial_columns is None else initial_columns[:]
    build_orders = [] if initial_columns is None else [initial_columns]

    while pending:
        next_pending = []
        gen = []
        value_emitted = False
        gen_provided = []
        for entry in pending:
            name, deps = entry
            deps.difference_update(provided)
            if deps:
                next_pending.append((name, set(deps)))
            elif name in provided:
                pass
                value_emitted |= True
            else:
                gen.append(name)
                gen_provided.append(name)
                value_emitted |= True
        provided.extend(gen_provided)
        build_orders.append(gen)
        if not value_emitted:
            raise ValueError("cyclic or missing dependency detected %r" % (next_pending,))

        pending = next_pending

    if flatten:
        return [item for sublist in build_orders for item in sublist]
    else:
        return build_orders
