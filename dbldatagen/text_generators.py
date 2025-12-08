# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines various text generation classes and methods
"""

import math
import random
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import numpy.random
import pandas as pd

from dbldatagen.serialization import SerializableToDict


#: list of hex digits for template generation
_HEX_LOWER = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]

#: list of upper case hex digits for template generation
_HEX_UPPER = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"]

#: list of non-zero digits for template generation
_DIGITS_NON_ZERO = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]

#: list of digits for template generation
_DIGITS_ZERO = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]

#: list of uppercase letters for template generation
_LETTERS_UPPER = [
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "T",
    "S",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
]

#: list of lowercase letters for template generation
_LETTERS_LOWER = [
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
]

#: list of all letters uppercase and lowercase
_LETTERS_ALL = _LETTERS_LOWER + _LETTERS_UPPER

#: list of alphanumeric chars in lowercase
_ALNUM_LOWER = _LETTERS_LOWER + _DIGITS_ZERO

#: list of alphanumeric chars in uppercase
_ALNUM_UPPER = _LETTERS_UPPER + _DIGITS_ZERO

""" words for ipsum lorem based text generation"""
_WORDS_LOWER = [
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
    "et",
    "dolore",
    "magna",
    "aliqua",
    "ut",
    "enim",
    "ad",
    "minim",
    "veniam",
    "quis",
    "nostrud",
    "exercitation",
    "ullamco",
    "laboris",
    "nisi",
    "ut",
    "aliquip",
    "ex",
    "ea",
    "commodo",
    "consequat",
    "duis",
    "aute",
    "irure",
    "dolor",
    "in",
    "reprehenderit",
    "in",
    "voluptate",
    "velit",
    "esse",
    "cillum",
    "dolore",
    "eu",
    "fugiat",
    "nulla",
    "pariatur",
    "excepteur",
    "sint",
    "occaecat",
    "cupidatat",
    "non",
    "proident",
    "sunt",
    "in",
    "culpa",
    "qui",
    "officia",
    "deserunt",
    "mollit",
    "anim",
    "id",
    "est",
    "laborum",
]

_WORDS_UPPER = [
    "LOREM",
    "IPSUM",
    "DOLOR",
    "SIT",
    "AMET",
    "CONSECTETUR",
    "ADIPISCING",
    "ELIT",
    "SED",
    "DO",
    "EIUSMOD",
    "TEMPOR",
    "INCIDIDUNT",
    "UT",
    "LABORE",
    "ET",
    "DOLORE",
    "MAGNA",
    "ALIQUA",
    "UT",
    "ENIM",
    "AD",
    "MINIM",
    "VENIAM",
    "QUIS",
    "NOSTRUD",
    "EXERCITATION",
    "ULLAMCO",
    "LABORIS",
    "NISI",
    "UT",
    "ALIQUIP",
    "EX",
    "EA",
    "COMMODO",
    "CONSEQUAT",
    "DUIS",
    "AUTE",
    "IRURE",
    "DOLOR",
    "IN",
    "REPREHENDERIT",
    "IN",
    "VOLUPTATE",
    "VELIT",
    "ESSE",
    "CILLUM",
    "DOLORE",
    "EU",
    "FUGIAT",
    "NULLA",
    "PARIATUR",
    "EXCEPTEUR",
    "SINT",
    "OCCAECAT",
    "CUPIDATAT",
    "NON",
    "PROIDENT",
    "SUNT",
    "IN",
    "CULPA",
    "QUI",
    "OFFICIA",
    "DESERUNT",
    "MOLLIT",
    "ANIM",
    "ID",
    "EST",
    "LABORUM",
]


class TextGenerator(ABC):
    """
    Base class for all text generation classes.
    """

    _randomSeed: int
    _rngInstance: numpy.random.Generator | None

    def __init__(self) -> None:
        self._randomSeed = 42
        self._rngInstance = None

    def __repr__(self) -> str:
        return f"TextGenerator(randomSeed={self._randomSeed})"

    def __str__(self) -> str:
        return f"TextGenerator(randomSeed={self._randomSeed})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self._randomSeed == other._randomSeed

    def __hash__(self) -> int:
        return hash(self._randomSeed)

    def withRandomSeed(self, seed: int) -> "TextGenerator":
        """
        Sets the TextGenerator's random seed.

        :param seed: Random seed value
        :return: Text generator with the specified seed value
        """
        assert not seed or isinstance(seed, int), "expecting an integer seed for Text Generator"
        self._randomSeed = seed
        return self

    @property
    def randomSeed(self) -> int:
        """
        Gets the TextGenerator's random seed.

        :return: Random seed value
        """
        return self._randomSeed

    def getNPRandomGenerator(self, forceNewInstance: bool = False) -> numpy.random.Generator:
        """
        Gets a NumPy random number generator.

        :return: Random number generator initialized from previously supplied random seed.
        """
        assert self._randomSeed is None or type(self._randomSeed) in [
            int,
            np.int32,
            np.int64,
        ], f"`random_seed` must be int or int-like not {type(self._randomSeed)}"

        if self._rngInstance is not None and not forceNewInstance:
            return self._rngInstance

        if self._randomSeed is not None and self._randomSeed not in (-1, -1.0):
            rng = numpy.random.default_rng(seed=self._randomSeed)
        else:
            rng = numpy.random.default_rng()

        if not forceNewInstance:
            self._rngInstance = rng
        return rng

    @staticmethod
    def compactNumpyTypeForValues(listValues: list | numpy.ndarray) -> np.dtype:
        """
        Determines the smallest numpy type to represent the supplied values.

        :param listValues: List or `np.ndarray` of values to get `np.dtype` for
        :return: `np.dtype` that is most compact representation for values provided
        """
        if isinstance(listValues, list):
            max_value_represented = np.max(np.array(listValues).flatten())
        else:
            max_value_represented = np.max(listValues.flatten()) + 1
        bits_required = math.ceil(math.log2(max_value_represented))

        if bits_required <= 8:
            # for small values, use byte representation
            retval = np.dtype("B")
        else:
            # compute bytes required and raise to nearest power of 2
            bytesRequired = math.ceil(bits_required / 8.0)
            retval = np.dtype(f"u{bytesRequired}")
        return retval

    @staticmethod
    def getAsTupleOrElse(
        v: int | tuple[int, int] | None, defaultValue: tuple[int, int], valueName: str = "value"
    ) -> tuple[int, int]:
        """get value v as tuple or return default value

        :param v: value to test
        :param defaultValue: value to use as a default if value of `v` is None. Must be a tuple.
        :param valueName: name of value for debugging and logging purposes
        :returns: return `v` as tuple if not `None` or value of `default_v` if `v` is `None`. If `v` is a single
                  value, returns the tuple (`v`, `v`)"""
        assert not v or isinstance(v, int | tuple), f"param {valueName} must be an int, a tuple or None"
        assert isinstance(defaultValue, tuple) and len(defaultValue) == 2, "default value must be tuple"

        if not v:
            assert len(defaultValue) == 2, "must have list or iterable with lenght 2"
            assert isinstance(defaultValue[0], int) and isinstance(
                defaultValue[1], int
            ), "all elements must be integers"
            return defaultValue

        if isinstance(v, tuple):
            assert len(v) == 2, "expecting tuple of length 2"
            assert isinstance(v[0], int) and isinstance(v[1], int), "expecting tuple with both elements as integers"
            return v[0], v[1]

        return v, v

    @abstractmethod
    def pandasGenerateText(self, v: pd.Series) -> pd.Series:
        """
        Generates text from a template using Pandas.

        :param v: Pandas series of values passed as base values
        :return: Pandas series of expanded templates
        """
        raise NotImplementedError("Subclasses should implement unique versions of `pandasGenerateText`")


class TemplateGenerator(TextGenerator, SerializableToDict):  # lgtm [py/missing-equals]
    """
    This class handles the generation of text from templates.

    :param template: Template string to use in text generation
    :param escapeSpecialChars: Whether to escape special characters (e.g. "a" or "d") in the template (default is
        ``False``).
        * If ``False``, unescaped special characters correspond to character classes (e.g. "a" for lowercase alphabetical
        characters).
        * If ``True``, special characters must be escaped using ``\\``.
    :param extendedWordList: Optional list of words to use during text generation

    The template generator generates text from a template to allow for generation of synthetic account card numbers,
    VINs, IBANs and many other structured codes.

    The base value is passed to the template generation and may be used in the generated text. The base value is the
    value the column would have if the template generation had not been applied.

    It uses the following special chars:

    ==========  ======================================
    Chars       Meaning
    ==========  ======================================
    ``\\``       Apply escape to next char.
    v0,v1,..v9  Use base value as an array of values and substitute the `nth` element ( 0 .. 9). Always escaped.
    x           Insert a random lowercase hex digit
    X           Insert an uppercase random hex digit
    d           Insert a random lowercase decimal digit
    D           Insert an uppercase random decimal digit
    a           Insert a random lowercase alphabetical character
    A           Insert a random uppercase alphabetical character
    k           Insert a random lowercase alphanumeric character
    K           Insert a random uppercase alphanumeric character
    n           Insert a random number between 0 .. 255 inclusive. This option must always be escaped
    N           Insert a random number between 0 .. 65535 inclusive. This option must always be escaped
    w           Insert a random lowercase word from the ipsum lorem word set. Always escaped
    W           Insert a random uppercase word from the ipsum lorem word set. Always escaped
    ==========  ======================================

    .. note::
              If escape is used and`escapeSpecialChars` is False, then the following
              char is assumed to have no special meaning.

              If the `escapeSpecialChars` option is set to True, then the following char only has its special
              meaning when preceded by an escape.

              Some options must be always escaped for example  ``\\v``, ``\\n`` and ``\\w``.

              A special case exists for ``\\v`` - if immediately followed by a digit 0 - 9, the underlying base value
              is interpreted as an array of values and the nth element is retrieved where `n` is the digit specified.

    In all other cases, the char itself is used.

    The setting of the `escapeSpecialChars` determines how templates generate data.

    If set to False, then the template ``r"\\dr_\\v"`` will generate the values ``"dr_0"`` ... ``"dr_999"`` when applied
    to the values zero to 999. This conforms to earlier implementations for backwards compatibility.

    If set to True, then the template ``r"dr_\\v"`` will generate the values ``"dr_0"`` ... ``"dr_999"``
    when applied to the values zero to 999. This conforms to the preferred style going forward
    """

    _template: str
    _escapeSpecialChars: bool
    _extendedWordList: list[str] | None
    _escapeSpecialMeaning: bool
    _templates: list[str]
    _wordList: np.ndarray
    _upperWordList: np.ndarray
    _np_digits_zero: np.ndarray
    _np_digits_non_zero: np.ndarray
    _np_hex_upper: np.ndarray
    _np_hex_lower: np.ndarray
    _np_alnum_lower: np.ndarray
    _np_alnum_upper: np.ndarray
    _np_letters_lower: np.ndarray
    _np_letters_upper: np.ndarray
    _np_letters_all: np.ndarray
    _lenWords: int
    _templateMappings: dict[str, tuple[int, np.ndarray]]
    _templateEscapedMappings: dict[str, tuple[int, np.ndarray | None]]

    def __init__(
        self, template: str, escapeSpecialChars: bool = False, extendedWordList: list[str] | None = None
    ) -> None:
        super().__init__()

        self._template = template
        self._escapeSpecialChars = escapeSpecialChars
        self._extendedWordList = extendedWordList
        self._escapeSpecialMeaning = bool(escapeSpecialChars)
        self._templates = self._splitTemplates(self._template)
        self._wordList = np.array(extendedWordList if extendedWordList is not None else _WORDS_LOWER)
        self._upperWordList = np.array(
            [x.upper() for x in extendedWordList] if extendedWordList is not None else _WORDS_UPPER
        )

        self._np_digits_zero = np.array(_DIGITS_ZERO)
        self._np_digits_non_zero = np.array(_DIGITS_NON_ZERO)
        self._np_hex_upper = np.array(_HEX_UPPER)
        self._np_hex_lower = np.array(_HEX_LOWER)
        self._np_alnum_lower = np.array(_ALNUM_LOWER)
        self._np_alnum_upper = np.array(_ALNUM_UPPER)
        self._np_letters_lower = np.array(_LETTERS_LOWER)
        self._np_letters_upper = np.array(_LETTERS_UPPER)
        self._np_letters_all = np.array(_LETTERS_ALL)
        self._lenWords = len(self._wordList)

        # mappings must be mapping from string to tuple(length of mappings, mapping array or list)
        self._templateMappings = {
            "a": (26, self._np_letters_lower),
            "A": (26, self._np_letters_upper),
            "x": (16, self._np_hex_lower),
            "X": (16, self._np_hex_upper),
            "d": (10, self._np_digits_zero),
            "D": (9, self._np_digits_non_zero),
            "k": (36, self._np_alnum_lower),
            "K": (36, self._np_alnum_upper),
        }

        # ensure that each mapping is mapping from string to list or numpy array
        for k, v in self._templateMappings.items():
            assert k and isinstance(k, str) and len(k) > 0, "key must be non-empty string"
            assert v and isinstance(v, tuple) and len(v) == 2, "value must be tuple of length 2"
            mapping_length, mappings = v
            assert isinstance(mapping_length, int), "mapping length must be of type int"
            assert isinstance(mappings, list | np.ndarray), "mappings are lists or numpy arrays"
            assert mapping_length == 0 or len(mappings) == mapping_length, "mappings must match mapping_length"

        self._templateEscapedMappings = {
            "n": (256, None),
            "N": (65536, None),
            "w": (self._lenWords, self._wordList),
            "W": (self._lenWords, self._upperWordList),
        }

        # ensure that each escaped mapping is mapping from string to None, list or numpy array
        for k, v in self._templateEscapedMappings.items():  # type: ignore[assignment]
            assert k and isinstance(k, str) and len(k) > 0, "key must be non-empty string"
            assert v and isinstance(v, tuple) and len(v) == 2, "value must be tuple of length 2"
            mapping_length, mappings = v
            assert isinstance(mapping_length, int), "mapping length must be of type int"
            assert mappings is None or isinstance(mappings, list | np.ndarray), "mappings are lists or numpy arrays"

            # for escaped mappings, the mapping can be None in which case the mapping is to the number itself
            # i.e mapping[4] = 4
            assert mappings is None or len(mappings) == mapping_length, "mappings must match mapping_length"

        # get the template metadata - this will be list of metadata entries for each template
        # for each template, metadata will be tuple of number of placeholders followed by list of random bounds
        # to be computed when replacing non static placeholder
        template_info = [
            self._prepareTemplateStrings(template, escapeSpecialMeaning=escapeSpecialChars)
            for template in self._templates
        ]

        self._max_placeholders = max(x[0] for x in template_info)
        self._max_rnds_needed = max(len(x[1]) for x in template_info)
        self._placeholders_needed = [x[0] for x in template_info]
        self._template_rnd_bounds = [x[1] for x in template_info]

    def __repr__(self) -> str:
        return f"TemplateGenerator(template='{self._template}')"

    @property
    def templates(self) -> list[str]:
        """Get effective templates for text generator"""
        return self._templates

    def classicGenerateText(self, v: str) -> str:
        """
        Generates text from a template.

        :param v: Value passed as a base value
        :return: Expanded template
        """

        values = pd.Series([v])
        results = self.pandasGenerateText(values).iloc[0]
        return str(results)

    def pandasGenerateText(self, v: pd.Series) -> pd.Series:
        """
        Generates text from a template using Pandas.

        :param v: Pandas series of values passed as base values
        :return: Pandas series of expanded templates
        """
        # placeholders is numpy array used to hold results
        placeholders = np.full((v.shape[0], self._max_placeholders), "", dtype=np.object_)

        # prepare template selections, bounds, rnd values to drive application of algorithm
        template_choices, _, template_rnds = self._prepare_random_bounds(v)
        template_choices_t = template_choices.T

        # create masked arrays, with all elements initially masked
        # as we substitute template expansion, we'll mask and unmask rows corresponding to each template
        # calling the method to substitute the values on the masked placeholders
        masked_placeholders: np.ma.MaskedArray = np.ma.MaskedArray(placeholders, mask=False)
        masked_rnds: np.ma.MaskedArray = np.ma.MaskedArray(template_rnds, mask=False)
        masked_matrices = [masked_placeholders, masked_rnds]

        # test logic for template expansion
        for x in range(len(self._templates)):  # pylint: disable=consider-using-enumerate
            masked_placeholders[template_choices_t != x, :] = np.ma.masked
            masked_rnds[template_choices_t != x, :] = np.ma.masked

            # harden mask, preventing modifications
            for m in masked_matrices:
                np.ma.harden_mask(m)

            # expand values into placeholders without affect masked values
            self._applyTemplateStringsForTemplate(
                v, self._templates[x], masked_placeholders, masked_rnds, escapeSpecialMeaning=self._escapeSpecialMeaning
            )

            # soften and clear mask, allowing modifications
            for m in masked_matrices:
                np.ma.soften_mask(m)
                m.mask = False

        # join strings in placeholders
        output = pd.Series(list(placeholders))
        results = output.apply(lambda placeholder_items: "".join([str(elem) for elem in placeholder_items]))

        return results

    def _toInitializationDict(self) -> dict[str, Any]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.
        :return: Python dictionary representation of the object
        """
        _options = {
            "kind": self.__class__.__name__,
            "template": self._template,
            "escapeSpecialChars": self._escapeSpecialChars,
            "extendedWordList": self._extendedWordList,
        }
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    @staticmethod
    def _splitTemplates(templateStr: str) -> list[str]:
        """
        Splits the template string into a list of template strings.

        :param templateStr: Template string
        :return: List of template strings
        """
        tmp_template = templateStr.replace(r"\\", "$__escape__").replace(r"\|", "$__sep__")
        results = [x.replace("$__escape__", r"\\").replace("$__sep__", "|") for x in tmp_template.split("|")]
        return results

    @staticmethod
    def _getRandomInt(low: int, high: int = -1, rng: np.random.Generator | None = None) -> int | np.int32:
        """
        Generates a random integer between the provided low and high values.

        :param low: Low value, if no high value is specified, treat low value as high value and low of 0
        :param high: High value for random number generation
        :param rng: A numpy random number generator to use for generating hte random value
        :return: A random integer between the provided low and high values
        """
        if high == -1:
            high = low
            low = 0

        if rng:
            # numpy interval is different to ``randint``
            return rng.integers(low, high + 1, dtype=np.int32)

        # use standard random for now as it performs better when generating values one at a time
        return random.randint(low, high)

    def _prepareTemplateStrings(self, genTemplate: str, escapeSpecialMeaning: bool = False) -> tuple[int, list[int]]:
        """
        Prepares a list of random numbers needed to generate the template value in vectorized form.

        :param genTemplate: Template string used to control text generation
        :param escapeSpecialMeaning: If ``True``, requires escape on special meaning chars
        :returns: A tuple with the number of placeholders and a vector of random values upper bounds

        The first element of the tuple is the number of placeholders needed to populate the generated template. The
        second element is a vector of integer values which determine bounds for random number vector for template
        generation.

        Each element of the vector will be used to generate a random number between 0 and the element inclusive,
        which is then used to select words (e.g. from wordlists) for template expansion.

        The `_escapeSpecialMeaning` parameter allows for backwards compatibility with old style syntax while allowing
        for preferred new style template syntax. Specify as True to force escapes for special meanings,.
        """
        retval = []

        escape = False
        use_value = False
        template_len = len(genTemplate)
        num_placeholders = 0

        # in the following code, the construct `(not escape) ^ self._escapeSpecialMeaning` means apply
        # special meaning if either escape is not true or the option `self._escapeSpecialMeaning` is true.
        # This corresponds to the logical xor operation
        for i in range(0, template_len):
            char = genTemplate[i]
            following_char = genTemplate[i + 1] if i + 1 < template_len else None

            if char == "\\":
                escape = True
            elif use_value and ("0" <= char <= "9"):
                # val_index = int(char)
                # retval.append(str(baseValue[val_index]))
                num_placeholders += 1
                use_value = False
            elif (char in self._templateMappings) and (not escape) ^ escapeSpecialMeaning:
                # handle case for ['a','A','k', 'K', 'x', 'X']
                bound, _ = self._templateMappings[char]
                retval.append(bound)
                num_placeholders += 1
                escape = False
            elif (char in self._templateEscapedMappings) and escape:
                # handle case for ['n', 'N', 'w', 'W']
                bound, _mappingArr = self._templateEscapedMappings[char][0], self._templateEscapedMappings[char][1]
                retval.append(bound)
                num_placeholders += 1
                escape = False
            elif char == "v" and escape:
                escape = False
                if following_char is not None and ("0" <= following_char <= "9"):
                    use_value = True
                else:
                    num_placeholders += 1
                    # retval.append(str(baseValue))
            elif char == "V" and escape:
                # retval.append(str(baseValue))
                num_placeholders += 1
                escape = False
            else:
                # retval.append(char)
                num_placeholders += 1
                escape = False

        if use_value:
            # retval.append(str(baseValue))
            num_placeholders += 1

        return num_placeholders, retval

    def _applyTemplateStringsForTemplate(
        self,
        baseValue: pd.Series | pd.DataFrame,
        genTemplate: str,
        placeholders: np.ndarray,
        rnds: np.ndarray,
        *,
        escapeSpecialMeaning: bool = False,
    ) -> np.ndarray:
        """
        Vectorized implementation of template driven text substitution. Applies substitutions to placeholders using
        random numbers.

        :param baseValue: Pandas Series or DataFrame of base value for applying the template
        :param genTemplate: A template string to control text generation
        :param placeholders: A masked nparray of type np.object_ pre-allocated to hold the generated strings
        :param rnds: A masked numpy 2d array of random numbers needed for vectorized generation
        :param escapeSpecialMeaning: If ``True``, requires escape on special meaning chars.
        :returns: placeholders

        The vectorized implementation populates the placeholder Numpy array with the substituted values.

        The `_escapeSpecialMeaning` parameter allows for backwards compatibility with old style syntax while allowing
        for preferred new style template syntax. Specify as True to force escapes for special meanings,.

        .. note::
                Both `placeholders` and `rnds` are numpy masked arrays. If there are multiple templates in the template
                generation source template, then this method will be called multiple times with each of
                the distinct templates passed and the `placeholders` and `rnds` arrays masked so that the each call
                will apply the template to rows to which that template applies.

                The template may be the empty string.
        """
        assert baseValue.shape[0] == placeholders.shape[0]
        assert baseValue.shape[0] == rnds.shape[0]

        _cached_values = {}

        regular_keys = self._templateMappings.keys()
        escaped_keys = self._templateEscapedMappings.keys()

        def _get_values_as_np_array() -> np.ndarray:
            """Get baseValue which is pd.Series or Dataframe as a numpy array and cache it"""
            if "np_values" not in _cached_values:
                _cached_values["np_values"] = baseValue.to_numpy()

            values = _cached_values["np_values"]
            if not isinstance(values, np.ndarray):
                raise TypeError("Value for 'np_values' should be of type 'np.ndarray'")

            return values

        def _get_values_subelement(elem: int) -> np.ndarray:
            """Get element from base values as np array and cache it"""
            cache_key = f"v_{elem}"
            if cache_key not in _cached_values:
                np_values = _get_values_as_np_array()
                # element_values = []
                element_values: np.ndarray = np.ndarray(np_values.shape[0], dtype=np_values.dtype)

                for x in range(baseValue.shape[0]):
                    # element_values.append(baseValue[x][elem])
                    element_values[x] = baseValue[x][elem]
                _cached_values[cache_key] = element_values

            sub_element = _cached_values[cache_key]
            if not isinstance(sub_element, np.ndarray):
                raise TypeError("Sub-element value must be of type 'np.ndarray'")

            return sub_element

        escape = False
        use_value = False
        template_len = len(genTemplate)
        num_placeholders = 0
        rnd_offset = 0

        unmasked_rows = None  # unmasked_rows is None, indicates that all rows are unmasked

        assert isinstance(placeholders, np.ma.MaskedArray), "expecting MaskArray"

        # if template is empty, then nothing needs to be done
        if template_len > 0 and isinstance(placeholders, np.ma.MaskedArray):
            active_rows = ~placeholders.mask
            unmasked_rows = active_rows[:, 0]

            if np.all(active_rows):
                unmasked_rows = None

        # in the following code, the construct `(not escape) ^ self._escapeSpecialMeaning` means apply
        # special meaning if either escape is not true or the option `self._escapeSpecialMeaning` is true.
        # This corresponds to the logical xor operation
        for i in range(0, template_len):
            char = genTemplate[i]
            following_char = genTemplate[i + 1] if i + 1 < template_len else None

            if char == "\\":
                escape = True
            elif use_value and ("0" <= char <= "9"):
                val_index = int(char)
                placeholders[:, num_placeholders] = _get_values_subelement(val_index)
                # placeholders[:, num_placeholders] = pd_base_values.apply(lambda x: str(x[val_index]))
                num_placeholders += 1
                use_value = False
            elif char in regular_keys and (not escape) ^ escapeSpecialMeaning:
                # note vectorized lookup - `rnds[:, rnd_offset]` will get vertical column of
                # random numbers from `rnds` 2d array
                _, value_mappings = self._templateMappings[char]

                if unmasked_rows is not None:
                    placeholders[unmasked_rows, num_placeholders] = value_mappings[rnds[unmasked_rows, rnd_offset]]
                else:
                    placeholders[:, num_placeholders] = value_mappings[rnds[:, rnd_offset]]

                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                escape = False
                # used for retval.append(_HEX_LOWER[self._getRandomInt(0, 15, rndGenerator)])
            elif char in escaped_keys and escape:
                _bound, value_mappings = self._templateEscapedMappings[char]  # type: ignore[assignment]

                if value_mappings is not None:
                    if unmasked_rows is not None:
                        placeholders[unmasked_rows, num_placeholders] = value_mappings[rnds[unmasked_rows, rnd_offset]]
                    else:
                        placeholders[:, num_placeholders] = value_mappings[rnds[:, rnd_offset]]
                elif unmasked_rows is not None:  # type: ignore[unreachable]
                    placeholders[unmasked_rows, num_placeholders] = rnds[unmasked_rows, rnd_offset]
                else:
                    placeholders[:, num_placeholders] = rnds[:, rnd_offset]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(str(self._getRandomInt(0, 255, rndGenerator)))
                escape = False
            elif char == "v" and escape:
                escape = False
                if following_char is not None and ("0" <= following_char <= "9"):
                    use_value = True
                else:
                    placeholders[:, num_placeholders] = _get_values_as_np_array()
                    num_placeholders += 1
                    # retval.append(str(baseValue))
            elif char == "V" and escape:
                placeholders[:, num_placeholders] = _get_values_as_np_array()
                # retval.append(str(baseValue))
                num_placeholders += 1
                escape = False
            else:
                placeholders[:, num_placeholders] = char
                # retval.append(char)
                num_placeholders += 1
                escape = False

        if use_value:
            placeholders[:, num_placeholders] = _get_values_as_np_array()
            # retval.append(str(baseValue))
            num_placeholders += 1

        return placeholders

    def _prepare_random_bounds(self, v: pd.Series) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Prepares the random bounds for processing the template expansion.

        For each template, we will have a vector of random numbers to generate for expanding the template. If we have
        multiple templates, there will be a separate vector of random numbers for each template.

        :param v: Pandas Series of values passed as base values
        :return: A vector of templates chosen, template random bounds (1 for each substitution) and selected
                 random numbers for each row (as numpy array)
        """
        # choose templates
        num_templates = len(self.templates)
        assert num_templates >= 1, "Expecting at least 1 template"

        rng = self.getNPRandomGenerator()

        if num_templates > 1:
            # choose template at random from 0 .. num_templates
            templates_chosen = rng.integers(np.full(v.size, num_templates))
        else:
            # always use template 0
            templates_chosen = np.full(v.size, 0)

        # populate template random numbers
        template_rnd_bounds = np.full((v.size, self._max_rnds_needed), -1)
        masked_template_bounds: np.ma.MaskedArray = np.ma.MaskedArray(template_rnd_bounds, mask=False)

        for i in range(num_templates):
            # assign the
            len_bounds_i = len(self._template_rnd_bounds[i])
            masked_template_bounds[i == templates_chosen.T, 0:len_bounds_i] = self._template_rnd_bounds[i]

        masked_template_bounds[template_rnd_bounds == -1] = np.ma.masked

        r_mask = masked_template_bounds.mask

        template_rnds = template_rnd_bounds.copy()

        template_rnds[~r_mask] = rng.integers(masked_template_bounds[~r_mask])

        return templates_chosen, template_rnd_bounds, template_rnds


class ILText(TextGenerator, SerializableToDict):  # lgtm [py/missing-equals]
    """
    This class generates Ipsum Lorem text paragraphs, words, and sentences.

    :param paragraphs: Number of paragraphs to generate. If a tuple is provided, we will generate a random number of
        paragraphs in the provided range.
    :param sentences: Number of sentences per paragraph to generate. If a tuple is provided, we will generate a random
        number of sentences in the provided range.
    :param words: Number of words per sentence to generate. If a tuple is provided, we will generate a random number of
        words in the provided range.
    :param extendedWordList: Optional list of words to use instead of the default Ipsum Lorem list.
    """

    def __init__(
        self,
        paragraphs: int | tuple[int, int] | None = None,
        sentences: int | tuple[int, int] | None = None,
        words: int | tuple[int, int] | None = None,
        extendedWordList: list[str] | None = None,
    ) -> None:
        assert (
            paragraphs is not None or sentences is not None or words is not None
        ), "At least one of the params `paragraphs`, `sentences` or `words` must be specified"

        super().__init__()

        self._paragraphs = paragraphs
        self._sentences = sentences
        self._words = words
        self._extendedWordList = extendedWordList

        self.paragraphs = self.getAsTupleOrElse(paragraphs, (1, 1), "paragraphs")
        self.words = self.getAsTupleOrElse(words, (2, 12), "words")
        self.sentences = self.getAsTupleOrElse(sentences, (1, 1), "sentences")
        self.wordList = extendedWordList if extendedWordList is not None else _WORDS_LOWER
        self.shape = [self.paragraphs[1], self.sentences[1], self.words[1]]

        # values needed for the text generation
        # numpy uses fixed sizes for strings , so compute whats needed
        self._npWords = np.array(self.wordList)

        self._processStats()
        self._processWordList()

    def __repr__(self) -> str:
        paras, sentences, words = self.paragraphs, self.sentences, self.words
        wl = self.wordList.__repr__ if self.wordList is not None else "None"
        return f"ILText(paragraphs={paras}, sentences={sentences}, words={words}, wordList={wl})"

    def generateText(self, baseValues: list | pd.Series | np.ndarray, rowCount: int = 1) -> list[str] | pd.Series:
        """
        generate text for seed based on configuration parameters.

        As it uses numpy, repeatability is restricted depending on version of the runtime

        :param baseValues: List or array-like list of baseValues
        :param rowCount: Number of rows
        :returns: List or Pandas series of generated strings of same size as input seed
        """
        assert baseValues is not None, "`baseValues` param must be specified"
        rng = self.getNPRandomGenerator(forceNewInstance=True)
        word_offset_type = self._wordOffsetType
        stats_shape = [rowCount, self.paragraphs[1], self.sentences[1], 3]

        # determine counts of paragraphs, sentences and words
        para_stats_raw = np.round(rng.normal(self._meanValues, self._stdVals2, size=stats_shape))
        para_stats = np.clip(para_stats_raw, self._minValues, self._maxValues)

        # Convert to the compact dtype after clipping
        para_stats = para_stats.astype(self._textGenerationValues.dtype)

        # replicate paragraphs and sentences from first row of each paragraph through other elememnts
        # this is used to efficiently create mask for manipulating word offsets
        # after this every row will contain :
        # number of good paragraphs, number of good sentences, number of number of good words
        # for current sentence in current paragraph in outer rows collection
        para_stats[:, :, :, 0] = para_stats[:, :, 0, 0, np.newaxis]
        para_stats[:, :, :, 1] = para_stats[:, :, 0, 1, np.newaxis]

        # set up shape for arrays used to process word offsets
        # this will be masked so that we don't waste resources processing invalid paragraphs, sentences and words
        output_shape = (rowCount, self.paragraphs[1], self.sentences[1], self.words[1])

        # compute the masks for paragraphs, sentences, and words
        # get the set of indices for shape  - r = rows, p = paragraphs, s = sentences, w = words
        # the indices will produce a set of rows of values for each dimension
        # the mask is then produced by iterating comparing index with good value
        # for example - if number of good words is 3 and sentence is index array of [0, 1, 2, 3, 4, 5 ,6]
        # then mask is produced via conditions indices <= good_word
        # note value of True means masked for numpy
        _, p, s, w = np.indices(output_shape)

        good_words = para_stats[:, :, :, 2]
        good_paragraphs = para_stats[:, :, :, 0]
        good_sentences = para_stats[:, :, :, 1]

        # build masks in each dimension and `or` them together
        words_mask = (w.T >= good_words.T).T
        para_mask = (p.T >= good_paragraphs.T).T
        sentences_mask = (s.T >= good_sentences.T).T
        final_mask = words_mask | para_mask | sentences_mask

        word_offsets = np.full(output_shape, dtype=word_offset_type, fill_value=self._emptyStringOffset)
        masked_offsets: np.ma.MaskedArray = np.ma.MaskedArray(word_offsets, mask=final_mask)

        # note numpy random differs from standard random in that it never produces upper bound
        masked_offsets[~masked_offsets.mask] = rng.integers(
            self._wordOffsetSize, size=output_shape, dtype=self._wordOffsetType
        )[~masked_offsets.mask]

        # hardening a mask prevents masked values from being changed
        np.ma.harden_mask(masked_offsets)
        # Cast offsets to the same dtype as the array to avoid casting errors
        capitals_offset = self._wordOffsetType.type(self._startOfCapitalsOffset)
        spaced_words_offset = self._wordOffsetType.type(self._startOfSpacedWordsOffset)
        masked_offsets[:, :, :, 0] = masked_offsets[:, :, :, 0] + capitals_offset
        masked_offsets[:, :, :, 1:] = masked_offsets[:, :, :, 1:] + spaced_words_offset
        np.ma.soften_mask(masked_offsets)

        # add period to every sentence
        # we'll replicate column 0 in order to preserve the mask and fill unmasked entries
        # with the sentence end offset
        new_word_offsets = masked_offsets[:, :, :, 0][:]
        new_col = new_word_offsets[:, :, :, np.newaxis]
        terminated_word_offsets = np.ma.concatenate((masked_offsets, new_col), axis=3)
        new_column = terminated_word_offsets[:, :, :, -1]
        sentence_end_offset = self._wordOffsetType.type(self._sentenceEndOffset)
        new_column[~new_column.mask] = sentence_end_offset

        # reshape to paragraphs
        shape = terminated_word_offsets.shape
        paragraph_offsets = terminated_word_offsets.reshape((rowCount, shape[1], shape[2] * shape[3]))

        if self.paragraphs[1] > 1:
            # add paragraph terminator to every paragraph
            # we'll take a copy of the first column so as to preserve masking
            # i.e if first word of first sentence is masked, then end marker position should be masked
            new_word_offsets = paragraph_offsets[:, :, 0][:]
            new_col = new_word_offsets[:, :, np.newaxis]
            terminated_paragraph_offsets = np.ma.concatenate((paragraph_offsets, new_col), axis=2)

            # set the paragraph end marker on all paragraphs except last
            # new_masked_elements = terminated_paragraph_offsets[:,:,-1]
            new_column = terminated_paragraph_offsets[:, :, -1]
            paragraph_end_offset = self._wordOffsetType.type(self._paragraphEnd)
            new_column[~new_column.mask] = paragraph_end_offset
        else:
            terminated_paragraph_offsets = paragraph_offsets

        # reshape to rows containing word offset sequences. We'll join using pandas apply to avoid
        # memory issues with fixed strings in numpy
        shape = terminated_paragraph_offsets.shape
        terminated_paragraph_offsets = terminated_paragraph_offsets.reshape((rowCount, shape[1] * shape[2]))

        empty_string_offset = self._wordOffsetType.type(self._emptyStringOffset)
        final_data = terminated_paragraph_offsets.filled(fill_value=empty_string_offset)  # pylint: disable=no-member

        # its faster to manipulate text in data frames as numpy strings are fixed length
        all_python_words = self._wordsAsPythonStrings

        base_results = pd.DataFrame(final_data)

        # build our lambda expression, copying point to word list locally for efficiency
        empty_string_offsets = [self._emptyStringOffset, self._emptyStringOffset + self._startOfSpacedWordsOffset]
        # mk_str_fn = lambda x: ("".join([all_python_words[x1] for x1 in x if x1 not in empty_string_offsets])).strip()
        # mk_str_fn = lambda x: ("".join([all_python_words[x1] for x1 in x ]))

        # ... and execute it
        results = base_results.apply(lambda w: self._get_word(w, all_python_words, empty_string_offsets), axis=1)
        return results

    def classicGenerateText(self, v: str) -> str:
        """
        Generates text using PySpark UDFs (non-Pandas).

        :param v: Value passed as a base value
        :return: Expanded template
        """
        return self.generateText([v], 1)[0]

    def pandasGenerateText(self, v: pd.Series) -> pd.Series:
        """
        Generates text with Pandas UDFs.

        :param v: Pandas series of values passed as base values
        :return: Pandas series of expanded templates
        """
        rows = v.to_numpy()
        results = self.generateText(rows, rows.size)
        return pd.Series(results)

    def _toInitializationDict(self) -> dict[str, Any]:
        """
        Converts an object to a Python dictionary. Keys represent the object's constructor arguments.

        :return: Python dictionary representation of the object
        """
        _options = {
            "kind": self.__class__.__name__,
            "paragraphs": self._paragraphs,
            "sentences": self._sentences,
            "words": self._words,
            "extendedWordList": self._extendedWordList,
        }
        return _options

    def _processStats(self) -> None:
        """
        Computes statistics needed for the text generation.
        """

        vals = [self.paragraphs, self.sentences, self.words]
        self._textGenerationValues = np.array(vals, dtype=self.compactNumpyTypeForValues(vals))
        self._minValues = self._textGenerationValues[:, 0]
        self._maxValues = self._textGenerationValues[:, 1]

        self._meanValues = np.mean(self._textGenerationValues, axis=1)

        # we want to force wider spread of sentence length, so we're not simply computing the std_deviation
        # - but computing a target std_dev that will spread sentence length
        self._stdVals = self._meanValues / 2
        self._stdVals2 = np.std(self._textGenerationValues, axis=1)

    def _processWordList(self) -> None:
        """
        Sets up the word lists needed for text generation.
        """
        np_words = np.array(self.wordList, np.dtype(np.str_))
        np_capitalized_words = np.char.capitalize(np_words[:])

        all_words = np_words[:]

        self._wordOffsetSize = all_words.size
        self._sentenceEndOffset = all_words.size
        self._paragraphEnd = self._sentenceEndOffset + 1
        self._wordSpaceOffset = self._paragraphEnd + 1
        self._emptyStringOffset = self._wordSpaceOffset + 1

        punctuation = [". ", "\n\n", " ", ""]
        all_words = np.concatenate((all_words, np.array(punctuation)))

        self._startOfCapitalsOffset = all_words.size
        all_words = np.concatenate((all_words, np_capitalized_words, np.array(punctuation)))

        # for efficiency, we'll create list of words preceded by spaces - it will reduce memory consumption during join
        # and array manipulation as we dont have to hold offset for space
        self._startOfSpacedWordsOffset = all_words.size

        np_spaced_words = np.array([" " + x for x in self.wordList], np.dtype(np.str_))
        all_words = np.concatenate((all_words, np_spaced_words, np.array(punctuation)))

        # set up python list of all words so that we dont have to convert between numpy and python representations
        self._allWordsSize = all_words.size
        self._wordsAsPythonStrings = [str(x) for x in all_words]

        # get smallest type that can represent word offset
        self._wordOffsetType = self.compactNumpyTypeForValues([all_words.size * 2 + 10])

    @staticmethod
    def _get_word(elements: list[int], words: list[str], excluded: list[int]) -> str:
        return "".join([words[element] for element in elements if element not in excluded]).strip()
