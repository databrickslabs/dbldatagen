# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines utility functions for type checking enforcement, and related functions

These are meant for internal use only
"""

import inspect
from abc import ABC


def optional_abstractmethod(fn):
    """Decorator to mark a class method or function as an optional abstract method

    This is used to mark a method as optional in a subclass of an abstract class. When the subclass is overridden,
    the method will not be required to be implemented. However, if it is, it must have the same signature as the
    parent class method.

    :param fn: Function to mark as optional abstract
    :return: Function with attribute set
    """
    fn._is_optional_abstractmethod_ = True

    return fn


def is_optional_abstractmethod(fn):
    """Check if a method is an optional abstract method

    :param fn: Function to check
    :return: True if the function is an optional abstract method
    """
    return hasattr(fn, "_is_optional_abstractmethod_")


def get_optional_abstract_methods(cls):
    """Get optional abstract methods for a class

    :param  cls: Class to check for optional abstract methods
    :return: List of optional abstract methods
    """
    # note inspect returns a tuple of (name, method) for each method
    return [method[1] for method in inspect.getmembers(cls, inspect.isfunction) if
            is_optional_abstractmethod(method[1])]


def check_optional_abstract_methods(cls, baseClass):
    """Check that optional abstract methods have the same signature as the base class

    :param cls: derived class to check
    :param baseClass: base class to check against
    :return: Nothing

    raises errors if type signatures are different
    """
    assert issubclass(cls, baseClass), f"Class {cls.__name__} is not a subclass of {baseClass.__name__}"

    # get optional override methods
    # note inspect returns a tuple of (name, method) for each method
    optional_methods = [method for method in inspect.getmembers(baseClass, inspect.isfunction) if
                        is_optional_abstractmethod(method[1])]

    for method_name, method_obj1 in optional_methods:
        if hasattr(cls, method_name):
            method_obj2 = getattr(cls, method_name)

            if not inspect.isfunction(method_obj1):
                raise TypeError(f"Object {method_name} in subclass {cls.__name__} shadows base method {method_obj1}")

            if inspect.signature(method_obj1) != inspect.signature(method_obj2):
                raise TypeError(f"""Method {method_name} in subclass {cls.__name__} has different signature.
                Expected signature {inspect.signature(method_obj1)} but got {inspect.signature(method_obj2)}""")


def abstract_with_optional_methods(baseClass):
    """Class decorator to enforce that optional abstract methods are implemented

    This decorator is used to enforce that optional abstract methods are implemented in a subclass of an abstract class.

    Note it does not require that the subclass is an abstract class itself

    :param baseClass: Class to check for optional abstract methods
    :return: Class with optional abstract methods enforced

    By adding the decorator to a class, it will enforce that any optional abstract methods are implemented with the same
    signature as the base class. This is useful for ensuring that optional abstract methods are implemented correctly
    """
    class AbstractClassWithOptionalMethods(baseClass):
        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)

            # check for the optional methods that may be implemented to ensure that they have the same signature
            check_optional_abstract_methods(cls, baseClass)

    return AbstractClassWithOptionalMethods
