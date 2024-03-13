# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `GenerationModel` class
"""


class GenerationModel(object):

    def __init__(self, generationSpec):
        self._generationSpec = generationSpec
        self._constraints = []

    @property
    def constraints(self):
        return self._constraints

    def withConstraint(self, constraint):
        """ Add a constraint to the generation model"""
        self._constraints.append(constraint)

    def withConstraints(self, constraints):
        """ Add multiple constraints to the generation model"""
        self._constraints.extend(constraints)