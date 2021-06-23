# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the base class for statistical distributions

"""
import copy


class DataDistribution(object):
    """ Base class for all distributions"""
    def __init__(self):
        self._minValue = None
        self._maxValue = None
        self._rectify = False
        self._randomSeed = None
        self._rounding = False

    def withRandomSeed(self, randomSeed):
        """ Create copy of object and set the random seed

        :param randomSeed: random seed value to set. Should be integer
        :return: new instance of data distribution object with random seed set. U

        Using -1 for randomSeed parameter means don't set specific random seed
        """
        assert isinstance(randomSeed, int), "seed value should be int"

        new_distribution_instance = copy.copy(self)
        new_distribution_instance._randomSeed = randomSeed
        return new_distribution_instance

    @property
    def randomSeed(self):
        """get the `randomSeed` attribute """
        return self._randomSeed

    def withRange(self, minValue, maxValue, stepValue):
        """ Create copy of object and set the min, max and step

        :param minValue: minValue for data generation
        :param maxValue: maxValue for data generation
        :param stepValue: stepValue for data generation
        :return: new instance of data distribution object with random seed set. U

        Sets the scaling information for data generated from distribution
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._minValue = minValue
        new_distribution_instance._maxValue = maxValue
        new_distribution_instance._stepValue = stepValue
        new_distribution_instance._rectify = True
        return new_distribution_instance

    @property
    def valueRange(self):
        """get the range of values for the Data Distribution as tuple"""
        return self._minValue, self._maxValue, self._stepValue

    @property
    def minValue(self):
        """get the minValue attribute"""
        return self._minValue

    @property
    def maxValue(self):
        """get the minValue attribute"""
        return self._maxValue

    @property
    def step(self):
        """get the `step` attribute"""
        return self._stepValue

    def withRounding(self, rounding):
        """ Create copy of object and set the rounding attribute

        :param rounding: rounding value to set. Should be True or False
        :return: new instance of data distribution object with rounding set
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._rounding = rounding
        return new_distribution_instance

    @property
    def rounding(self):
        """get the `rounding` attribute """
        return self._rounding


