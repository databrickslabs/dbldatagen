# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the base class for statistical distributions

Each inherited version of the DataDistribution object is used to generate random numbers drawn from
a specific distribution.

As the test data generator needs to scale the set of values generated across different data ranges,
the generate function is intended to generate values scaled to values between 0 and 1.

AS some distributions don't have easily predicted bounds, we scale the random data sets
by taking the minimum and maximum value of each generated data set and using that as the range for the generated data.

For some distributions, there may be alternative more efficient mechanisms for scaling the data to the [0, 1] interval.

Some data distributions are scaled to the [0,1] interval as part of their data generation
and no further scaling is needed.
"""

import ast
import copy
import inspect
import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import numpy as np
from pyspark.sql import Column

from dbldatagen.serialization import SerializableToDict


# Matches a bare distribution name like "normal" or a parameterized spec like
# "beta(alpha=5.0, beta=2.0)". Whitespace is permitted around all tokens; the name
# is a Python identifier.
_SPEC_PATTERN = re.compile(r"^\s*(?P<distribution>\w+)\s*(?:\((?P<args>[^)]*)\))?\s*$")

# Matches a single `key=value` pair. The key and value groups may be empty. Callers
# check each side separately to produce error messages for missing keys or values.
# Numeric validation of parsed values happens downstream.
_KWARG_PATTERN = re.compile(r"^\s*(?P<key>\w*)\s*=\s*(?P<value>.*?)\s*$")

# Registry of distribution name -> (subclass, default kwargs). Populated by the
# `@register_distribution(name, ...)` decorator on `DataDistribution` subclasses.
# Bare-name lookups instantiate with the registered defaults; parameterized specs
# override them. Module-private; access via `DataDistribution.fromName` etc.
_REGISTRY: dict[str, tuple[type["DataDistribution"], dict[str, Any]]] = {}


class DataDistribution(SerializableToDict, ABC):
    """Base class for all distributions"""

    _randomSeed: int | np.int32 | np.int64 | None = None
    _rounding: bool = False

    @classmethod
    def fromName(cls, spec: str) -> "DataDistribution":
        """Resolves a distribution spec string to an instance.

        The spec is either a distribution name (e.g., ``"normal"``) or a name with keyword
        arguments (e.g., ``"beta(alpha=5.0, beta=2.0)"``). Distribution names are registered
        using :func:`register_distribution`; explicit kwargs override those defaults.
        Matching is case-insensitive on the name; values must be numeric literals.

        :param spec: Distribution specification string (e.g. ``"beta(alpha=5.0, beta=2.0)"``)
        :return: New instance of the named distribution
        :raises ValueError: If the distribution is not registered, the specification is malformed,
            a keyword argument is not accepted by the constructor, or an argument value is non-numeric
        """
        distribution_name, distribution_args = cls._parseSpec(spec)
        entry = _REGISTRY.get(distribution_name.lower())
        if entry is None:
            valid_names = ", ".join(sorted(_REGISTRY.keys()))
            raise ValueError(
                f"Unknown distribution '{distribution_name}'. "
                f"Valid distribution names are: {valid_names}. "
                f"Alternatively, pass a distribution object directly "
                f"(e.g., dist.Normal(), dist.Beta(alpha=5.0, beta=2.0))."
            )
        subclass, default_kwargs = entry
        if distribution_args:
            valid_kwargs = {p.name for p in inspect.signature(subclass).parameters.values() if p.name != "self"}
            unknown_args = sorted(set(distribution_args) - valid_kwargs)
            if unknown_args:
                raise ValueError(
                    f"Unknown keyword argument(s) for distribution '{distribution_name}': {unknown_args}. "
                    f"Valid keyword arguments are: {sorted(valid_kwargs)}."
                )
        return subclass(**{**default_kwargs, **distribution_args})

    @classmethod
    def registeredNames(cls) -> list[str]:
        """Returns a sorted list of registered distribution names.

        :return: Sorted list of lowercase distribution names recognized by :meth:`fromName`
        """
        return sorted(_REGISTRY.keys())

    @staticmethod
    def get_np_random_generator(random_seed: int | np.int32 | np.int64 | None) -> np.random.Generator:
        """Gets a numpy random number generator.

        :param random_seed: Numeric random seed to use; If < 0, then no random
        :return: Numpy random number generator
        """
        if random_seed not in (-1, -1.0):
            rng = np.random.default_rng(random_seed)
        else:
            rng = np.random.default_rng()
        return rng

    @abstractmethod
    def generateNormalizedDistributionSample(self) -> Column:
        """Generates a sample of data for the distribution. Implementors must provide an implementation for this method.

        :return: Pyspark SQL column expression for the sample
        """
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not implement 'generateNormalizedDistributionSample'"
        )

    def withRounding(self, rounding: bool) -> "DataDistribution":
        """Creates a copy of the object and sets the rounding attribute.

        :param rounding: Rounding value to set
        :return: New instance of data distribution object with rounding set
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._rounding = rounding
        return new_distribution_instance

    @property
    def rounding(self) -> bool:
        """Returns the rounding attribute.

        :return: Rounding attribute
        """
        return self._rounding

    def withRandomSeed(self, seed: int | np.int32 | np.int64 | None) -> "DataDistribution":
        """Creates a copy of the object and with a new random seed value.

        :param seed: Random generator seed value to set; Should be integer,  float or None
        :return: New instance of data distribution object with random seed set
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._randomSeed = seed
        return new_distribution_instance

    @property
    def randomSeed(self) -> int | np.int32 | np.int64 | None:
        """Returns the random seed attribute.

        :return: Random seed attribute
        """
        return self._randomSeed

    @staticmethod
    def _parseSpec(spec: str) -> tuple[str, dict[str, float]]:
        """Parses a distribution specification to get the distribution name and keyword arguments.

        :param spec: Spec of the form ``"distribution"`` or ``"distribution(key=value, ...)"``
        :return: Tuple of (distribution, keyword arguments)
        :raises ValueError: If the specification is malformed or an argument value is not a numeric literal
        """
        match = _SPEC_PATTERN.match(spec)
        if not match:
            raise ValueError(
                f"Invalid distribution spec '{spec}'. "
                f"Expected format: 'distribution' or 'distribution(key=value, ...)'."
            )
        distribution_name = match.group("distribution")
        arguments = match.group("args")
        overrides: dict[str, float] = {}
        if arguments is None or not arguments.strip():
            return distribution_name, overrides
        for part in arguments.split(","):
            matched_arg = _KWARG_PATTERN.match(part)
            if not matched_arg:
                raise ValueError(
                    f"Invalid keyword argument '{part.strip()}' in distribution spec '{spec}'. "
                    f"Expected 'key=value' with a numeric value."
                )
            arg_key = matched_arg.group("key")
            arg_value = matched_arg.group("value")
            if not arg_key:
                raise ValueError(f"Missing keyword for value '{arg_value}' in distribution spec '{spec}'.")
            if not arg_value:
                raise ValueError(f"Missing value for keyword '{arg_key}' in distribution spec '{spec}'.")
            if arg_key in overrides:
                raise ValueError(f"Duplicate keyword '{arg_key}' in distribution spec '{spec}'.")
            overrides[arg_key] = DataDistribution._parseNumericValue(arg_value, arg_key, spec)
        return distribution_name, overrides

    @staticmethod
    def _parseNumericValue(raw: str, key: str, spec: str) -> int | float:
        """Parses a numeric literal from a distribution spec value.

        :param raw: Raw value string (e.g., ``"2.5"``, ``"-3"``)
        :param key: Argument name the value is bound to (used for error messages)
        :param spec: Full spec string (used for error messages)
        :return: Parsed argument value as an integer or float
        :raises ValueError: If ``raw`` cannot be parsed to a numeric literal value
        """
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as e:
            raise ValueError(
                f"Invalid value '{raw}' for argument '{key}' in distribution spec '{spec}'. "
                f"Expected a numeric literal."
            ) from e
        if isinstance(parsed, float | int) and not isinstance(parsed, bool):
            return parsed
        raise ValueError(
            f"Invalid value '{raw}' for argument '{key}' in distribution spec '{spec}'. " f"Expected a numeric literal."
        )


def register_distribution(name: str, **default_kwargs) -> Callable[[type], type]:
    """Registers a :class:`DataDistribution` subclass under a string name.

    The registered class is instantiated with ``**default_kwargs`` when resolved
    via a bare name (e.g., ``"beta"``). Parameterized specs like
    ``"beta(alpha=3.0)"`` override the corresponding defaults.

    Example::

        @register_distribution("beta", alpha=2.0, beta=5.0)
        class Beta(DataDistribution):
            ...

    :param name: Case-insensitive name to register under (e.g., ``"normal"``)
    :param default_kwargs: Default keyword arguments for the class constructor
    :return: Class decorator that registers the class and returns it unchanged
    """
    key = name.strip().lower()

    def _register(cls: type) -> type:
        _REGISTRY[key] = (cls, default_kwargs)
        return cls

    return _register
