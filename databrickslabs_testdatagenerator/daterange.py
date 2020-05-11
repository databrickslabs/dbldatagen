# See the License for the specific language governing permissions and
# limitations under the License.
#
import datetime


class DateRange(object):

    @classmethod
    def parseInterval(cls, interval_str):
        assert interval_str is not None
        results = []
        for kv in interval_str.split(","):
            key, value = kv.split('=')
            results.append("'{}':{}".format(key, value))

        return eval("{{ {}  }} ".format(",".join(results)))

    def __init__(self, begin, end, interval=None, datetime_format="%Y-%m-%d %H:%M:%S"):
        assert begin is not None
        assert end is not None

        self.begin = begin if not isinstance(begin, str) else datetime.datetime.strptime(begin, datetime_format)
        self.end = end if not isinstance(end, str) else datetime.datetime.strptime(end, datetime_format)
        self.interval = interval if not isinstance(interval, str) else datetime.timedelta(
            **self.parseInterval(interval))
