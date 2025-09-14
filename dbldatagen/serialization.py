""" Defines interface contracts."""
import sys
from typing import TypeVar


T = TypeVar("T", bound="SerializableToDict")

class SerializableToDict:
    """ Serializable objects must implement a `_getConstructorOptions` method
        which converts the object properties to a Python dictionary whose keys
        are the named arguments to the class constructor.
    """

    @classmethod
    def _fromInitializationDict(cls: type[T], options: dict) -> T:
        """ Converts a Python dictionary to an object using the object's constructor.
            :param options: Python dictionary with class constructor options
            :return: An instance of the class
        """
        _options: dict = options.copy()
        _options.pop("kind")
        _ir = {}
        for key, value in _options.items():
            if isinstance(value, dict):
                value_kind = value.get("kind")
                value_class = getattr(sys.modules["dbldatagen"], value_kind)
                if not issubclass(value_class, SerializableToDict):
                    raise NotImplementedError(f"Object of class {value_kind} is not serializable.")
                value.pop("kind")
                _ir[key] = value_class(**value)
                continue
            _ir[key] = value
        return cls(**_ir)

    def _toInitializationDict(self) -> dict:
        """ Converts an object to a Python dictionary. Keys represent the object's
            constructor arguments.
            :return: Python dictionary representation of the object
        """
        raise NotImplementedError(
            f"Object is not serializable. {self.__class__.__name__} does not implement '_toInitializationDict'")
