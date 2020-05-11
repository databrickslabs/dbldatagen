# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` classes and utility functions
"""


class DataGenError(Exception):
    """Used to represent data generation errors"""
    pass


def ensure(x, msg="condition does not hold true"):
    """ensure(c, s) => throws Exception(s) if c is not true"""
    import re

    def strip_margin(text):
        return re.sub('\n[ \t]*\|', '\n', text)

    if not x:
        raise DataGenError(strip_margin(msg))


def topological_sort(sources):
    """ Perform a topological sort over sources

    :arg sources: list of ``(name, set(names of dependencies))`` pairs
    :returns: list of names in dependency order
    """
    # generate a copy so that we can modify in place
    pending = [(name, set(deps)) for name, deps in sources]
    provided = []

    while pending:
        next_pending = []
        value_emitted = False

        for entry in pending:
            name, deps = entry
            deps.difference_update(provided)
            if deps:
                next_pending.append(entry)
            else:
                yield name
                provided.append(name)
                value_emitted |= True
        if not value_emitted:
            raise ValueError("cyclic or missing dependency detected %r" % (next_pending,))

        pending = next_pending
