# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the DataRange abstract class

its not used directly, but used as base type for explicit DateRange and NRange types to ensure correct tracking of
changes to method names when refactoring

"""


class DataRange(object):
    """ Abstract class used as base class for NRange and DateRange """

    def isEmpty(self):
        """Check if object is empty (i.e all instance vars of note are `None`)"""
        raise NotImplementedError("method not implemented")

    def isFullyPopulated(self):
        """Check is all instance vars are populated"""
        raise NotImplementedError("method not implemented")

    def adjustForColumnDatatype(self, ctype):
        """ Adjust default values for column output type"""
        raise NotImplementedError("method not implemented")

    def getDiscreteRange(self):
        """Convert range to discrete range"""
        raise NotImplementedError("method not implemented")

    def getContinuousRange(self):
        """Convert range to continuous range"""
        raise NotImplementedError("method not implemented")

    def getScale(self):
        """Get scale of range"""
        raise NotImplementedError("method not implemented")

    @property
    def min(self):
        """get the `min` attribute"""
        return self.minValue
        
    @property
    def max(self):
        """get the `max` attribute"""
        return self.maxValue
