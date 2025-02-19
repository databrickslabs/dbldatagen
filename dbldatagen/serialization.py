""" Defines interface contracts."""
import sys


class Serializable:
    """ Serializable objects must implement a `toDict` method which
        converts the object properties to a Python dictionary whose
        keys are the named arguments to the class constructor.
    """

    @classmethod
    def getMapping(cls):
        """ Returns the internal mapping dictionary for the class.
            You must implement this method to make classes serializable
            via `fromDict` and `toDict`.
        """
        raise NotImplementedError(f"Object is not serializable. {cls.__name__} does not implement '_args'")

    @classmethod
    def fromDict(cls, options):
        """ Converts a Python Dictionary to the object using the object's constructor."""
        internal_representation = options.copy()
        internal_representation.pop("kind")
        d = {}
        for key, value in internal_representation.items():
            if isinstance(value, dict):
                c = value.get("kind")
                if dict in [type(i) for i in value.values()]:
                    d[key] = getattr(sys.modules[__name__], c).fromDict(value)
                    continue
                value.pop("kind")
                d[key] = getattr(sys.modules[__name__], c)(**value)
                continue
            d[key] = value
        return cls(**d)

    def toDict(self):
        """ Converts the object to a Python dictionary with the object's constructor arguments."""
        args = self.getMapping()
        return {"kind": self.__class__.__name__,
                **{
                    public_key: getattr(self, internal_key).toDict()
                    if isinstance(getattr(self, internal_key), Serializable)
                    else getattr(self, internal_key)
                    for public_key, internal_key in args.items()
                    if hasattr(self, internal_key) and getattr(self, internal_key) is not None
                }}
