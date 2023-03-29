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

        print("testing")

        assert customChars is None or isinstance(customChars, list) or isinstance(customChars, str),  \
               "`customChars` should be list of characters or string containing custom chars"

        assert not allUpper or not allLower, "allUpper and allLower cannot both be True"

        if isinstance(customChars, str):
            assert len(customChars) > 0, "string of customChars must be non-empty"
        elif isinstance(customChars, list):
            assert all(isinstance(c, str) for c in customChars)
            assert len(customChars) > 0, "list of customChars must be non-empty"

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
            self._minLength, self._maxLength = length, length
        elif isinstance(length, tuple):
            assert len(length) == 2, "only 2 elements can be specified if length is a tuple"
            assert all(isinstance(el, int) for el in length)
            self._minLength, self._maxLength = length
        else:
            raise ValueError("`length` must be an integer or a tuple of two integers")

        # compute bounds for generated strings
        bounds = []
        bounds.append( len(self._firstCharAlphabet))
        for ix in range(1, self._maxLength):
            bounds.append(len(self._charAlphabet))

        self._bounds = bounds

    def __repr__(self):
        return f"GenerateString(length={length}, leadingAlpha={leadingAlpha})"

    def prepareBaseValue(self, baseDef):
        """ Prepare the base value for processing

        :param baseDef: base value expression
        :return: base value expression unchanged

        For generate string processing , we'll use the SQL function abs(hash(baseDef)

        This will ensure that even if there are multiple base values, only a single value is passed to the UDF
        """
        return F.abs(F.hash(baseDef))

    def _getRandomInt(self, low, high=-1, rng=None):
        """ generate random integer between low and high inclusive

        :param low: low value, if no high value is specified, treat low value as high value and low of 0
        :param high: high value for random number generation
        :param rng: if provided, an instance of a numpy random number generator
        :return: generated value
        """
        if high == -1:
            high = low
            low = 0

        if rng is not None:
            # numpy interval is different to ``randint``
            return rng.integers(low, high + 1, dtype=np.int32)

        # use standard random for now as it performs better when generating values one at a time
        return random.randint(low, high)

    def _prepareTemplateStrings(self, genTemplate, escapeSpecialMeaning=False):
        """ Prepare list of random numbers needed to generate template in vectorized form

        :param genTemplate: template string to control text generation
        :param escapeSpecialMeaning: if True, requires escape on special meaning chars.
        :returns: tuple containing number of placeholders and vector of random values upper bounds

        The first element of the tuple is the number of placeholders needed to populate the template

        The second elememt is a vector of integer values which determine bounds for random number vector for
        template generation

        Each element of the vector will be used to generate a random number between 0 and the element inclusive,
        which is then used to select words from wordlists etc for template expansion

        `_escapeSpecialMeaning` parameter allows for backwards compatibility with old style syntax while allowing
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

            if char == '\\':
                escape = True
            elif use_value and ('0' <= char <= '9'):
                # val_index = int(char)
                # retval.append(str(baseValue[val_index]))
                num_placeholders += 1
                use_value = False
            elif char == 'x' and (not escape) ^ escapeSpecialMeaning:
                retval.append(16)
                num_placeholders += 1
                # used for retval.append(_HEX_LOWER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'X' and (not escape) ^ escapeSpecialMeaning:
                retval.append(16)
                num_placeholders += 1
                # retval.append(_HEX_UPPER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'd' and (not escape) ^ escapeSpecialMeaning:
                retval.append(10)
                num_placeholders += 1
                # retval.append(_DIGITS_ZERO[self._getRandomInt(0, 9, rndGenerator)])
            elif char == 'D' and (not escape) ^ escapeSpecialMeaning:
                retval.append(9)
                num_placeholders += 1
                # retval.append(_DIGITS_NON_ZERO[self._getRandomInt(0, 8, rndGenerator)])
            elif char == 'a' and (not escape) ^ escapeSpecialMeaning:
                retval.append(26)
                num_placeholders += 1
                # retval.append(_LETTERS_LOWER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'A' and (not escape) ^ escapeSpecialMeaning:
                retval.append(26)
                num_placeholders += 1
                # retval.append(_LETTERS_UPPER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'k' and (not escape) ^ escapeSpecialMeaning:
                retval.append(26)
                num_placeholders += 1
                # retval.append(_ALNUM_LOWER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'K' and (not escape) ^ escapeSpecialMeaning:
                retval.append(36)
                num_placeholders += 1
                # retval.append(_ALNUM_UPPER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'n' and escape:
                retval.append(256)
                num_placeholders += 1
                # retval.append(str(self._getRandomInt(0, 255, rndGenerator)))
                escape = False
            elif char == 'N' and escape:
                retval.append(65536)
                num_placeholders += 1
                # retval.append(str(self._getRandomInt(0, 65535, rndGenerator)))
                escape = False
            elif char == 'W' and escape:
                retval.append(self._lenWords)
                num_placeholders += 1
                # retval.append(self._upperWordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'w' and escape:
                retval.append(self._lenWords)
                num_placeholders += 1
                # retval.append(self._wordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'v' and escape:
                escape = False
                if following_char is not None and ('0' <= following_char <= '9'):
                    use_value = True
                else:
                    num_placeholders += 1
                    # retval.append(str(baseValue))
            elif char == 'V' and escape:
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

    def _applyTemplateStringsForTemplate(self, baseValue, genTemplate, placeholders, rnds, escapeSpecialMeaning=False):
        """ Vectorized implementation of template driven text substitution

         Apply substitutions to placeholders using random numbers

        :param baseValue: Pandas series or data frame of base value for applying template
        :param genTemplate: template string to control text generation
        :param placeholders: masked nparray of type np.object_ pre-allocated to hold strings emitted
        :param rnds: masked numpy 2d array of random numbers needed for vectorized generation
        :param escapeSpecialMeaning: if True, requires escape on special meaning chars.
        :returns: placeholders

        The vectorized implementation populates the placeholder Numpy array with the substituted values.

        `_escapeSpecialMeaning` parameter allows for backwards compatibility with old style syntax while allowing
        for preferred new style template syntax. Specify as True to force escapes for special meanings,.

        """
        assert baseValue.shape[0] == placeholders.shape[0]
        assert baseValue.shape[0] == rnds.shape[0]

        _cached_values = {}

        def _get_values_as_np_array():
            """Get baseValue which is pd.Series or Dataframe as a numpy array and cache it"""
            if "np_values" not in _cached_values:
                _cached_values["np_values"] = baseValue.to_numpy()

            return _cached_values["np_values"]

        def _get_values_subelement(elem):
            """Get element from base values as np array and cache it"""
            cache_key = f"v_{elem}"
            if cache_key not in _cached_values:
                np_values = _get_values_as_np_array()
                #element_values = []
                element_values = np.ndarray( np_values.shape[0], dtype=np_values.dtype)

                for x in range(baseValue.shape[0]):
                    #element_values.append(baseValue[x][elem])
                    element_values[x] = baseValue[x][elem]
                _cached_values[cache_key] = element_values

            return _cached_values[cache_key]

        escape = False
        use_value = False
        template_len = len(genTemplate)
        num_placeholders = 0
        rnd_offset = 0

        # in the following code, the construct `(not escape) ^ self._escapeSpecialMeaning` means apply
        # special meaning if either escape is not true or the option `self._escapeSpecialMeaning` is true.
        # This corresponds to the logical xor operation
        for i in range(0, template_len):
            char = genTemplate[i]
            following_char = genTemplate[i + 1] if i + 1 < template_len else None

            if char == '\\':
                escape = True
            elif use_value and ('0' <= char <= '9'):
                val_index = int(char)
                placeholders[:, num_placeholders] = _get_values_subelement(val_index)
                #placeholders[:, num_placeholders] = pd_base_values.apply(lambda x: str(x[val_index]))
                num_placeholders += 1
                use_value = False
            elif char == 'x' and (not escape) ^ escapeSpecialMeaning:
                # note vectorized lookup - `rnds[:, rnd_offset]` will get vertical column of
                # random numbers from `rnds` 2d array
                placeholders[:, num_placeholders] = self._np_hex_lower[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # used for retval.append(_HEX_LOWER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'X' and (not escape) ^ escapeSpecialMeaning:
                placeholders[:, num_placeholders] = self._np_hex_upper[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(_HEX_UPPER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'd' and (not escape) ^ escapeSpecialMeaning:
                placeholders[:, num_placeholders] = self._np_digits_zero[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(_DIGITS_ZERO[self._getRandomInt(0, 9, rndGenerator)])
            elif char == 'D' and (not escape) ^ escapeSpecialMeaning:
                placeholders[:, num_placeholders] = self._np_digits_non_zero[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(_DIGITS_NON_ZERO[self._getRandomInt(0, 8, rndGenerator)])
            elif char == 'a' and (not escape) ^ escapeSpecialMeaning:
                placeholders[:, num_placeholders] = self._np_letters_lower[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(_LETTERS_LOWER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'A' and (not escape) ^ escapeSpecialMeaning:
                placeholders[:, num_placeholders] = self._np_letters_upper[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(_LETTERS_UPPER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'k' and (not escape) ^ escapeSpecialMeaning:
                placeholders[:, num_placeholders] = self._np_alnum_lower[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(_ALNUM_LOWER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'K' and (not escape) ^ escapeSpecialMeaning:
                placeholders[:, num_placeholders] = self._np_alnum_upper[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(_ALNUM_UPPER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'n' and escape:
                placeholders[:, num_placeholders] = rnds[:, rnd_offset]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(str(self._getRandomInt(0, 255, rndGenerator)))
                escape = False
            elif char == 'N' and escape:
                placeholders[:, num_placeholders] = rnds[:, rnd_offset]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(str(self._getRandomInt(0, 65535, rndGenerator)))
                escape = False
            elif char == 'W' and escape:
                placeholders[:, num_placeholders] = self._upperWordList[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(self._upperWordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'w' and escape:
                placeholders[:, num_placeholders] = self._wordList[rnds[:, rnd_offset]]
                num_placeholders += 1
                rnd_offset = rnd_offset + 1
                # retval.append(self._wordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'v' and escape:
                escape = False
                if following_char is not None and ('0' <= following_char <= '9'):
                    use_value = True
                else:
                    placeholders[:, num_placeholders] = _get_values_as_np_array()
                    num_placeholders += 1
                    # retval.append(str(baseValue))
            elif char == 'V' and escape:
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

    def classicGenerateText(self, v):
        """entry point to use for classic udfs"""

        pdValues = pd.Series([v])
        results = self.pandasGenerateText(pdValues)
        return results[0]

    def _prepare_random_bounds(self, v):
        """
        Prepare the random bounds for processing of the template expansion

        For each template, we will have a vector of random numbers to generate for expanding the template

        If we have multiple templates, there will be a separate vector of random numbers for each template


        :param v: Pandas series of values passed as base values
        :return: vector of templates chosen, template random bounds (1 for each substitution) and selected
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
        masked_template_bounds = np.ma.MaskedArray(template_rnd_bounds, mask=False)

        for i in range(num_templates):
            # assign the
            len_bounds_i = len(self._template_rnd_bounds[i])
            masked_template_bounds[templates_chosen.T == i, 0:len_bounds_i] = self._template_rnd_bounds[i]

        masked_template_bounds[template_rnd_bounds == -1] = np.ma.masked

        r_mask = masked_template_bounds.mask

        template_rnds = template_rnd_bounds.copy()

        template_rnds[~r_mask] = rng.integers(masked_template_bounds[~r_mask])

        return templates_chosen, template_rnd_bounds, template_rnds

    def pandasGenerateText(self, v):
        """ entry point to use for pandas udfs

        Implementation uses vectorized implementation of process

        :param v: Pandas series of values passed as base values
        :return: Pandas series of expanded templates

        """
        # placeholders is numpy array used to hold results
        placeholders = np.full((v.shape[0], self._max_placeholders), '', dtype=np.object_)

        # prepare template selections, bounds, rnd values to drive application of algorithm
        template_choices, template_rnd_bounds, template_rnds = self._prepare_random_bounds(v)
        template_choices_t = template_choices.T

        # create masked arrays, with all elements initially masked
        # as we substitute template expansion, we'll mask and unmask rows corresponding to each template
        # calling the method to substitute the values on the masked placeholders
        masked_placeholders = np.ma.MaskedArray(placeholders, mask=False)
        masked_rnds = np.ma.MaskedArray(template_rnds, mask=False)
        # masked_base_values = np.ma.MaskedArray(baseValues, mask=False)
        masked_matrices = [masked_placeholders, masked_rnds]

        # test logic for template expansion
        for x in range(len(self._templates)):
            masked_placeholders[template_choices_t != x, :] = np.ma.masked
            masked_rnds[template_choices_t != x, :] = np.ma.masked
            #masked_base_values[template_choices_t != x] = np.ma.masked

            # harden mask, preventing modifications
            for m in masked_matrices:
                np.ma.harden_mask(m)

            # expand values into placeholders
            #self._applyTemplateStringsForTemplate(v.to_numpy(dtype=np.object_), #masked_base_values,
            self._applyTemplateStringsForTemplate(v,
                                                    #masked_base_values,
                                                    self._templates[x],
                                                    masked_placeholders,
                                                    masked_rnds,
                                                    escapeSpecialMeaning=self._escapeSpecialMeaning
                                                    )

            # soften and clear mask, allowing modifications
            for m in masked_matrices:
                np.ma.soften_mask(m)
                m.mask = False

        # join strings in placeholders
        output = pd.Series(list(placeholders))
        results = output.apply(lambda placeholder_items: "".join([str(elem) for elem in placeholder_items]))

        return results


class ILText2(TextGenerator):  # lgtm [py/missing-equals]
    """ Class to generate Ipsum Lorem text paragraphs, words and sentences

    :param paragraphs: Number of paragraphs to generate. If tuple will generate random number in range
    :param sentences:  Number of sentences to generate. If tuple will generate random number in tuple range
    :param words:  Number of words per sentence to generate. If tuple, will generate random number in tuple range

    """

    def __init__(self, paragraphs=None, sentences=None, words=None, extendedWordList=None):
        """
        Initialize the ILText with text generation parameters
        """
        assert paragraphs is not None or sentences is not None or words is not None, \
            "At least one of the params `paragraphs`, `sentences` or `words` must be specified"

        super().__init__()

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

    def _processStats(self):
        """ Compute the stats needed for the text generation """

        vals = [self.paragraphs, self.sentences, self.words]
        self._textGenerationValues = np.array(vals, dtype=self.compactNumpyTypeForValues(vals))
        self._minValues = self._textGenerationValues[:, 0]
        self._maxValues = self._textGenerationValues[:, 1]

        self._meanValues = np.mean(self._textGenerationValues, axis=1)

        # we want to force wider spread of sentence length, so we're not simply computing the std_deviation
        # - but computing a target std_dev that will spread sentence length
        self._stdVals = self._meanValues / 2
        self._stdVals2 = np.std(self._textGenerationValues, axis=1)

    def _processWordList(self):
        """ Set up the word lists"""
        np_words = np.array(self.wordList, np.dtype(np.str_))
        np_capitalized_words = np.char.capitalize(np_words[:])

        all_words = np_words[:]

        self._wordOffsetSize = all_words.size
        self._sentenceEndOffset = all_words.size
        self._paragraphEnd = self._sentenceEndOffset + 1
        self._wordSpaceOffset = self._paragraphEnd + 1
        self._emptyStringOffset = self._wordSpaceOffset + 1

        punctuation = [". ", "\n\n", " ", ""]
        all_words = np.concatenate((all_words, punctuation))

        self._startOfCapitalsOffset = all_words.size
        all_words = np.concatenate((all_words, np_capitalized_words, punctuation))

        # for efficiency, we'll create list of words preceded by spaces - it will reduce memory consumption during join
        # and array manipulation as we dont have to hold offset for space
        self._startOfSpacedWordsOffset = all_words.size

        np_spaced_words = np.array([" " + x for x in self.wordList], np.dtype(np.str_))
        all_words = np.concatenate((all_words, np_spaced_words, punctuation))

        # set up python list of all words so that we dont have to convert between numpy and python representations
        self._allWordsSize = all_words.size
        self._wordsAsPythonStrings = list([str(x) for x in all_words])

        # get smallest type that can represent word offset
        self._wordOffsetType = self.compactNumpyTypeForValues([all_words.size * 2 + 10])

    def __repr__(self):
        paras, sentences, words = self.paragraphs, self.sentences, self.words
        wl = self.wordList.__repr__ if self.wordList is not None else "None"
        return f"ILText(paragraphs={paras}, sentences={sentences}, words={words}, wordList={wl})"

    def generateText(self, baseValues, rowCount=1):
        """
        generate text for seed based on configuration parameters.

        As it uses numpy, repeatability is restricted depending on version of the runtime

        :param baseValues: list or array-like list of baseValues
        :param rowCount: number of rows
        :returns: list or Pandas series of generated strings of same size as input seed
        """
        assert baseValues is not None, "`baseValues` param must be specified"
        rng = self.getNPRandomGenerator(forceNewInstance=True)
        word_offset_type = self._wordOffsetType

        stats_shape = [rowCount, self.paragraphs[1], self.sentences[1], 3]

        # determine counts of paragraphs, sentences and words
        stats_array = np.empty(stats_shape, dtype=self._textGenerationValues.dtype)
        para_stats_raw = np.round(rng.normal(self._meanValues, self._stdVals2, size=stats_shape))
        para_stats = np.clip(para_stats_raw, self._minValues, self._maxValues, out=stats_array)

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
        r, p, s, w = np.indices(output_shape)

        good_words = para_stats[:, :, :, 2]
        good_paragraphs = para_stats[:, :, :, 0]
        good_sentences = para_stats[:, :, :, 1]

        # build masks in each dimension and `or` them together
        words_mask = (w.T >= good_words.T).T
        para_mask = (p.T >= good_paragraphs.T).T
        sentences_mask = (s.T >= good_sentences.T).T
        final_mask = words_mask | para_mask | sentences_mask

        word_offsets = np.full(output_shape, dtype=word_offset_type, fill_value=self._emptyStringOffset)
        masked_offsets = np.ma.MaskedArray(word_offsets, mask=final_mask)

        # note numpy random differs from standard random in that it never produces upper bound
        masked_offsets[~masked_offsets.mask] = rng.integers(self._wordOffsetSize,
                                                            size=output_shape,
                                                            dtype=self._wordOffsetType)[~masked_offsets.mask]

        # hardening a mask prevents masked values from being changed
        np.ma.harden_mask(masked_offsets)
        masked_offsets[:, :, :, 0] = masked_offsets[:, :, :, 0] + self._startOfCapitalsOffset
        masked_offsets[:, :, :, 1:] = masked_offsets[:, :, :, 1:] + self._startOfSpacedWordsOffset
        np.ma.soften_mask(masked_offsets)

        # add period to every sentence
        # we'll replicate column 0 in order to preserve the mask and fill unmasked entries
        # with the sentence end offset
        new_word_offsets = masked_offsets[:, :, :, 0][:]
        new_col = new_word_offsets[:, :, :, np.newaxis]
        terminated_word_offsets = np.ma.concatenate((masked_offsets, new_col), axis=3)
        new_column = terminated_word_offsets[:, :, :, -1]
        new_column[~new_column.mask] = self._sentenceEndOffset

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
            new_column[~new_column.mask] = self._paragraphEnd
        else:
            terminated_paragraph_offsets = paragraph_offsets

        # reshape to rows containing word offset sequences. We'll join using pandas apply to avoid
        # memory issues with fixed strings in numpy
        shape = terminated_paragraph_offsets.shape
        terminated_paragraph_offsets = terminated_paragraph_offsets.reshape((rowCount, shape[1] * shape[2]))

        final_data = terminated_paragraph_offsets.filled(fill_value=self._emptyStringOffset)

        # its faster to manipulate text in data frames as numpy strings are fixed length
        all_python_words = self._wordsAsPythonStrings

        base_results = pd.DataFrame(final_data)

        # build our lambda expression, copying point to word list locally for efficiency
        empty_string_offsets = [self._emptyStringOffset, self._emptyStringOffset + self._startOfSpacedWordsOffset]
        mk_str_fn = lambda x: ("".join([all_python_words[x1] for x1 in x if x1 not in empty_string_offsets])).strip()
        # mk_str_fn = lambda x: ("".join([all_python_words[x1] for x1 in x ]))

        # ... and execute it
        results = base_results.apply(mk_str_fn, axis=1)
        return results

    def classicGenerateText(self, v):
        """
        classic udf entry point for text generation

        :param v: base value to control generation of random numbers
        """
        return self.generateText([v], 1)[0]

    def pandasGenerateText(self, v):
        """
        pandas udf entry point for text generation

        :param v: pandas series of base values for random text generation
        :returns: Pandas series of generated strings
        """
        rows = v.to_numpy()
        results = self.generateText(rows, rows.size)
        return pd.Series(results)
