""" Defines interface contracts."""
import sys


class SerializableToDict:
    """ Serializable objects must implement a `_getConstructorOptions` method
        which converts the object properties to a Python dictionary whose keys
        are the named arguments to the class constructor.
    """

    def _getConstructorOptions(self):
        """ Returns an internal mapping dictionary for the object. Keys represent the
            class constructor arguments and values representing the object's internal data.
            Implement this method to make a class serializable via `fromDict` and `toDict`.
        """
        raise NotImplementedError(
            f"Object is not serializable. {self.__class__.__name__} does not implement '_getConstructorOptions'")

    @classmethod
    def _fromConstructorOptions(cls, options):
        """ Converts a Python dictionary to an object using the object's constructor.
            :param options: Python dictionary with class constructor options
            :return: An instance of the class
        """
        _options = options.copy()
        _options.pop("kind")
        _ir = {}
        for key, value in _options.items():
            if isinstance(value, dict):
                _kind = value.get("kind")
                if dict in [type(i) for i in value.values()]:
                    _ir[key] = getattr(sys.modules["dbldatagen"], _kind)._fromConstructorOptions(value)
                    continue
                value.pop("kind")
                _ir[key] = getattr(sys.modules["dbldatagen"], _kind)(**value)
                continue
            _ir[key] = value
        return cls(**_ir)

    def _toConstructorOptions(self):
        """ Converts an object to a Python dictionary. Keys represent the object's
            constructor arguments.
            :return: Python dictionary representation of the object
        """
        args = self._getConstructorOptions()
        return {"kind": self.__class__.__name__,
                **{
                    k: v._toConstructorOptions()
                    if isinstance(v, SerializableToDict) else v
                    for k, v in args.items() if v is not None
                }}
