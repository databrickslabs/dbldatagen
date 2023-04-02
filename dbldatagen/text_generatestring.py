# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the RandomStr text generator
"""

import math
import random

import numpy as np
import pandas as pd

from .text_generators import TextGenerator
from .text_generators import _DIGITS_ZERO, _LETTERS_UPPER, _LETTERS_LOWER, _LETTERS_ALL

import pyspark.sql.functions as F

class GenerateString(TextGenerator):  # lgtm [py/missing-equals]
    """This class handles the generation of string text of specified length drawn from alphanumeric characters.

    The set of chars to be used can be modified based on the parameters

    This will generate deterministic strings chosen from the pool of characters `0-9`, `a-z`, `A-Z`, or from a
    custom character range if specified.

    :param length: length of string. Can be integer, or tuple (min, max)
    :param leadingAlpha:  If True, leading character will be in range a-zAA-Z
    :param allUpper:  If True, any alpha chars will be uppercase
    :param allLower:  If True, any alpha chars will be lowercase
    :param allAlpha:  If True, all chars will be non numeric
    :param customChars:  If supplied, specifies a list of chars to use, or string of chars to use.

    This method will generate deterministic strings varying in size from `minLength` to `maxLength`.
    The characters chosen will be in the range 0-9`, `a-z`, `A-Z` unless modified using the `leadingAlpha`,
    `allUpper`, `allLower`, `allAlpha` or `customChars` parameters.

    The modifiers can be combined - for example GenerateString(1, 5, leadingAlpha=True, allUpper=True)

    When the length is specified to be a tuple, it wll generate variable length strings of lengths from the lower bound
    to the upper bound inclusive.

    The strings are generated deterministically so that they can be used for predictable primary and foreign keys.

    If the column definition that includes this specifies `random` then the string generation will be determined by a
    seeded random number according to the rules for random numbers and random seeds used in other columns

    If random is false, then the string will be generated from a pseudo random sequence generated purely from the
    SQL hash of the `baseColumns`

    .. note::
       If customChars are specified, then the flag `allAlpha` will only remove digits.

    """

    def __init__(self, length, leadingAlpha=True, allUpper=False, allLower=False, allAlpha=False, customChars=None):
        super().__init__()

        assert not customChars or isinstance(customChars, list) or isinstance(customChars, str),  \
               "`customChars` should be list of characters or string containing custom chars"

        assert not allUpper or not allLower, "allUpper and allLower cannot both be True"

        if isinstance(customChars, str):
            assert len(customChars) > 0, "string of customChars must be non-empty"
        elif isinstance(customChars, list):
            assert all(isinstance(c, str) for c in customChars)
            assert len(customChars) > 0, "list of customChars must be non-empty"

        self.leadingAlpha = leadingAlpha
        self.allUpper = allUpper
        self.allLower = allLower
        self.allAlpha = allAlpha

        # determine base alphabet
        if isinstance(customChars, list):
            charAlphabet = set("".join(customChars))
        elif isinstance(customChars, str):
            charAlphabet = set(customChars)
        else:
            charAlphabet = set(_LETTERS_ALL).union(set(_DIGITS_ZERO))

        if allLower:
            charAlphabet = charAlphabet.difference(set(_LETTERS_UPPER))
        elif allUpper:
            charAlphabet = charAlphabet.difference(set(_LETTERS_LOWER))

        if allAlpha:
            charAlphabet = charAlphabet.difference(set(_DIGITS_ZERO))

        self._charAlphabet = np.array(list(charAlphabet))

        if leadingAlpha:
            self._firstCharAlphabet = np.array(list(charAlphabet.difference(set(_DIGITS_ZERO))))
        else:
            self._firstCharAlphabet = self._charAlphabet

        # compute string lengths
        if isinstance(length, int):
            self._minLength = length
            self._maxLength = length
        elif isinstance(length, tuple):
            assert len(length) == 2, "only 2 elements can be specified if length is a tuple"
            assert all(isinstance(el, int) for el in length)
            self._minLength, self._maxLength = length
        else:
            raise ValueError("`length` must be an integer or a tuple of two integers")

        # compute bounds for generated strings
        bounds = [len(self._firstCharAlphabet)]
        for ix in range(1, self._maxLength):
            bounds.append(len(self._charAlphabet))

        self._bounds = bounds

    def __repr__(self):
        return f"GenerateString(length={(self._minLength, self._maxLength)}, leadingAlpha={self.leadingAlpha})"

    def make_variable_length_mask(self, v, lengths):
        """ given 2-d array of dimensions[r, c] and lengths of dimensions[r]

           generate mask for each row where col_index[r,c] < lengths[r]
        """
        print(v.shape, lengths.shape)
        assert v.shape[0] == lengths.shape[0], "values and lengths must agree on dimension 0]"
        _, c_ix = np.indices(v.shape)

        return (c_ix.T < lengths.T).T

    def mk_bounds(self, v, minLength, maxLength):
        rng = default_rng(42)
        v_bounds = np.full(v.shape[0], (maxLength - minLength) + 1)
        return rng.integers(v_bounds) + minLength

    def prepareBaseValue(self, baseDef):
        """ Prepare the base value for processing

        :param baseDef: base value expression
        :return: base value expression unchanged

        For generate string processing , we'll use the SQL function abs(hash(baseDef)

        This will ensure that even if there are multiple base values, only a single value is passed to the UDF
        """
        return F.abs(F.hash(baseDef))

    def pandasGenerateText(self, v):
        """ entry point to use for pandas udfs

        Implementation uses vectorized implementation of process

        :param v: Pandas series of values passed as base values
        :return: Pandas series of expanded templates

        """
        # placeholders is numpy array used to hold results

        rnds = np.full((v.shape[0], self._maxLength), len(self._charAlphabet), dtype=np.object_)

        rng = self.getNPRandomGenerator()
        rnds2 = rng.integers(rnds)

        placeholders = np.full((v.shape[0], self._maxLength), '', dtype=np.object_)

        lengths = (v.to_numpy() % (self._maxLength - self._minLength) + self._minLength)

        v1 = np.full((v.shape[0], self._maxLength), -1)

        placeholder_mask = self.make_variable_length_mask(placeholders, lengths)
        masked_placeholders = np.ma.MaskedArray(placeholders, mask=placeholder_mask)

        masked_placeholders[~placeholder_mask] = self._charAlphabet[rnds2[~placeholder_mask]]

        output = pd.Series(list(placeholders))

        # join strings in placeholders
        results = output.apply(lambda placeholder_items: "".join([str(elem) for elem in placeholder_items]))

        return results
