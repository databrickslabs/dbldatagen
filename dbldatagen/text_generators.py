# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines various text generation classes and methods
"""

import math
import random

import numpy as np
import pandas as pd

#: list of hex digits for template generation
_HEX_LOWER = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

#: list of upper case hex digits for template generation
_HEX_UPPER = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']

#: list of non-zero digits for template generation
_DIGITS_NON_ZERO = ['1', '2', '3', '4', '5', '6', '7', '8', '9']

#: list of digits for template generation
_DIGITS_ZERO = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

#: list of uppercase letters for template generation
_LETTERS_UPPER = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                  'Q', 'R', 'T', 'S', 'U', 'V', 'W', 'X', 'Y', 'Z']

#: list of lowercase letters for template generation
_LETTERS_LOWER = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
                  'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

#: list of all letters uppercase and lowercase
_LETTERS_ALL = _LETTERS_LOWER + _LETTERS_UPPER

#: list of alphanumeric chars in lowercase
_ALNUM_LOWER = _LETTERS_LOWER + _DIGITS_ZERO

#: list of alphanumeric chars in uppercase
_ALNUM_UPPER = _LETTERS_UPPER + _DIGITS_ZERO

""" words for ipsum lorem based text generation"""
_WORDS_LOWER = ['lorem', 'ipsum', 'dolor', 'sit', 'amet', 'consectetur', 'adipiscing', 'elit', 'sed', 'do',
                'eiusmod', 'tempor', 'incididunt', 'ut', 'labore', 'et', 'dolore', 'magna', 'aliqua', 'ut',
                'enim', 'ad', 'minim', 'veniam', 'quis', 'nostrud', 'exercitation', 'ullamco', 'laboris',
                'nisi', 'ut', 'aliquip', 'ex', 'ea', 'commodo', 'consequat', 'duis', 'aute', 'irure', 'dolor',
                'in', 'reprehenderit', 'in', 'voluptate', 'velit', 'esse', 'cillum', 'dolore', 'eu', 'fugiat',
                'nulla', 'pariatur', 'excepteur', 'sint', 'occaecat', 'cupidatat', 'non', 'proident', 'sunt',
                'in', 'culpa', 'qui', 'officia', 'deserunt', 'mollit', 'anim', 'id', 'est', 'laborum']

_WORDS_UPPER = ['LOREM', 'IPSUM', 'DOLOR', 'SIT', 'AMET', 'CONSECTETUR', 'ADIPISCING', 'ELIT', 'SED', 'DO',
                'EIUSMOD', 'TEMPOR', 'INCIDIDUNT', 'UT', 'LABORE', 'ET', 'DOLORE', 'MAGNA', 'ALIQUA', 'UT',
                'ENIM', 'AD', 'MINIM', 'VENIAM', 'QUIS', 'NOSTRUD', 'EXERCITATION', 'ULLAMCO', 'LABORIS',
                'NISI', 'UT', 'ALIQUIP', 'EX', 'EA', 'COMMODO', 'CONSEQUAT', 'DUIS', 'AUTE', 'IRURE',
                'DOLOR', 'IN', 'REPREHENDERIT', 'IN', 'VOLUPTATE', 'VELIT', 'ESSE', 'CILLUM', 'DOLORE',
                'EU', 'FUGIAT', 'NULLA', 'PARIATUR', 'EXCEPTEUR', 'SINT', 'OCCAECAT', 'CUPIDATAT', 'NON',
                'PROIDENT', 'SUNT', 'IN', 'CULPA', 'QUI', 'OFFICIA', 'DESERUNT', 'MOLLIT', 'ANIM', 'ID', 'EST',
                'LABORUM']


class TextGenerator(object):
    """ Base class for text generation classes

    """

    def __init__(self):
        self._randomSeed = 42
        self._rngInstance = None

    def __repr__(self):
        return f"TextGenerator(randomSeed={self._randomSeed})"

    def __str__(self):
        return f"TextGenerator(randomSeed={self._randomSeed})"

    def __eq__(self, other):
        return type(self) == type(other) and self._randomSeed == other._randomSeed

    def withRandomSeed(self, seed):
        """ Set the random seed for the text generator

        :param seed: seed value to set
        :return: self
        """
        assert seed is None or type(seed) is int, "expecting an integer seed for Text Generator"
        self._randomSeed = seed
        return self

    @property
    def randomSeed(self):
        """ Get random seed for text generator"""
        return self._randomSeed

    def getNPRandomGenerator(self, forceNewInstance=False):
        """ Get numpy random number generator

        :return: returns random number generator initialized from previously supplied random seed
        """
        assert self._randomSeed is None or type(self._randomSeed) in [int, np.int32, np.int64], \
            f"`random_seed` must be int or int-like not {type(self._randomSeed)}"

        if self._rngInstance is not None and not forceNewInstance:
            return self._rngInstance

        from numpy.random import default_rng
        if self._randomSeed is not None and self._randomSeed not in (-1, -1.0):
            print(f"using seed {self._randomSeed}")
            rng = default_rng(seed=self._randomSeed)
        else:
            rng = default_rng()

        if not forceNewInstance:
            self._rngInstance = rng
        return rng

    @staticmethod
    def compactNumpyTypeForValues(listValues):
        """ determine smallest numpy type to represent values

        :param listValues: list or np.ndarray of values to get np.dtype for
        :return: np.dtype that is most compact representation for values provided
        """
        if type(listValues) is list:
            max_value_represented = np.max(np.array(listValues).flatten())
        else:
            max_value_represented = np.max(listValues.flatten()) + 1
        bits_required = math.ceil(math.log2(max_value_represented))

        if bits_required <= 8:
            # for small values, use byte representation
            retval = np.dtype('B')
        else:
            # compute bytes required and raise to nearest power of 2
            bytesRequired = int(math.ceil(bits_required / 8.0))
            retval = np.dtype(f"u{bytesRequired}")
        return retval

    @staticmethod
    def getAsTupleOrElse(v, defaultValue, valueName):
        """ get value v as tuple or return default value

            :param v: value to test
            :param defaultValue: value to use as a default if value of `v` is None. Must be a tuple.
            :param valueName: name of value for debugging and logging purposes
            :returns: return `v` as tuple if not `None` or value of `default_v` if `v` is `None`. If `v` is a single
                      value, returns the tuple (`v`, `v`)"""
        assert v is None or type(v) is int or type(v) is tuple, f"param {valueName} must be an int, a tuple or None"
        assert type(defaultValue) is tuple and len(defaultValue) == 2, "default value must be tuple"

        if type(v) is int:
            return v, v
        elif type(v) is tuple:
            assert len(v) == 2, "expecting tuple of length 2"
            assert type(v[0]) is int and type(v[1]) is int, "expecting tuple with both elements as integers"
            return v
        else:
            assert len(defaultValue) == 2, "must have list or iterable with lenght 2"
            assert type(defaultValue[0]) is int and type(defaultValue[1]) is int, "all elements must be integers"

        return defaultValue


class TemplateGenerator(TextGenerator):  # lgtm [py/missing-equals]
    """This class handles the generation of text from templates

    :param template: template string to use in text generation
    :param escapeSpecialChars: By default special chars in the template have special meaning if unescaped
                               If set to true, then the special meaning requires escape char ``\\``
    :param extendedWordList: if provided, use specified word list instead of default word list

    The template generator generates text from a template to allow for generation of synthetic account card numbers,
    VINs, IBANs and many other structured codes.

    The base value is passed to the template generation and may be used in the generated text. The base value is the
    value the column would have if the template generation had not been applied.

    It uses the following special chars:

    ========   ======================================
    Chars      Meaning
    ========   ======================================
    ``\\``     Apply escape to next char.
    v0,v1,..v9 Use base value as an array of values and substitute the `nth` element ( 0 .. 9). Always escaped.
    x          Insert a random lowercase hex digit
    X          Insert an uppercase random hex digit
    d          Insert a random lowercase decimal digit
    D          Insert an uppercase random decimal digit
    a          Insert a random lowercase alphabetical character
    A          Insert a random uppercase alphabetical character
    k          Insert a random lowercase alphanumeric character
    K          Insert a random uppercase alphanumeric character
    n          Insert a random number between 0 .. 255 inclusive. This option must always be escaped
    N          Insert a random number between 0 .. 65535 inclusive. This option must always be escaped
    w          Insert a random lowercase word from the ipsum lorem word set. Always escaped
    W          Insert a random uppercase word from the ipsum lorem word set. Always escaped
    ========   ======================================

    .. note::
              If escape is used and`escapeSpecialChars` is False, then the following
              char is assumed to have no special meaning.

              If the `escapeSpecialChars` option is set to True, then the following char only has its special
              meaning when preceded by an escape.

              Some options must be always escaped for example ``\\0``, ``\\v``, ``\\n`` and ``\\w``.

              A special case exists for ``\\v`` - if immediately followed by a digit 0 - 9, the underlying base value
              is interpreted as an array of values and the nth element is retrieved where `n` is the digit specified.

    In all other cases, the char itself is used.

    The setting of the `escapeSpecialChars` determines how templates generate data.

    If set to False, then the template ``r"\\dr_\\v"`` will generate the values ``"dr_0"`` ... ``"dr_999"`` when applied
    to the values zero to 999. This conforms to earlier implementations for backwards compatibility.

    If set to True, then the template ``r"dr_\\v"`` will generate the values ``"dr_0"`` ... ``"dr_999"``
    when applied to the values zero to 999. This conforms to the preferred style going forward

    """

    def __init__(self, template, escapeSpecialChars=False, extendedWordList=None):
        assert template is not None, "`template` must be specified"
        super().__init__()

        self._template = template
        self._escapeSpecialMeaning = bool(escapeSpecialChars)
        template_str0 = self._template
        self._templates = [x.replace('$__sep__', '|') for x in template_str0.replace(r'\|', '$__sep__').split('|')]
        self._wordList = extendedWordList if extendedWordList is not None else _WORDS_LOWER
        self._upperWordList = [x.upper() for x in extendedWordList] if extendedWordList is not None else _WORDS_UPPER
        self._lenWords = len(self._wordList)

    def __repr__(self):
        return f"TemplateGenerator(template='{self._template}')"

    @property
    def templates(self):
        """ Get effective templates for text generator"""
        return self._templates

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

    def _getRandomWordOffset(self, size=None, rndGenerator=None):
        """

        :param wordList: Word list to generate offset for
        :param size:     Size of expected wordlis
        :return:
        """

        # note generates range of [0 .. size - 1]
        retval = self._getRandomInt(0, size-1, rndGenerator)
        return retval

    def stringsFromSingleTemplate(self, baseValue, genTemplate, escapeSpecialMeaning=False, rndGenerator=None,
                                  stage=""):
        """ Generate text from a single template

        :param rndGenerator: random number generator instance
        :param baseValue: underlying base value to seed template generation.
          Ignored unless template outputs it
        :param genTemplate: template string to control text generation
        :param escapeSpecialMeaning: if True, requires escape on special meaning chars.
        :returns: array of strings generated from template

        `_escapeSpecialMeaning` parameter allows for backwards compatibility with old style syntax while allowing
        for preferred new style template syntax. Specify as True to force escapes for special meanings,.

        """
        retval = []

        escape = False
        use_value = False
        template_len = len(genTemplate)

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
                retval.append(str(baseValue[val_index]))
                use_value = False
            elif char == 'x' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_HEX_LOWER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'X' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_HEX_UPPER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'd' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_DIGITS_ZERO[self._getRandomInt(0, 9, rndGenerator)])
            elif char == 'D' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_DIGITS_NON_ZERO[self._getRandomInt(0, 8, rndGenerator)])
            elif char == 'a' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_LETTERS_LOWER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'A' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_LETTERS_UPPER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'k' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_ALNUM_LOWER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'K' and (not escape) ^ escapeSpecialMeaning:
                retval.append(_ALNUM_UPPER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'n' and escape:
                retval.append(str(self._getRandomInt(0, 255, rndGenerator)))
                escape = False
            elif char == 'N' and escape:
                retval.append(str(self._getRandomInt(0, 65535, rndGenerator)))
                escape = False
            elif char == 'W' and escape:
                retval.append(self._upperWordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'w' and escape:
                retval.append(self._wordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'v' and escape:
                escape = False
                if following_char is not None and ('0' <= following_char <= '9'):
                    use_value = True
                else:
                    retval.append(str(baseValue))
            elif char == 'V' and escape:
                retval.append(str(baseValue))
                escape = False
            else:
                retval.append(char)
                escape = False

        if use_value:
            retval.append(str(baseValue))

        return retval

    def prepareStringsFromSingleTemplate(self,  genTemplate, escapeSpecialMeaning=False, rndGenerator=None):
        """ Prepare list of random numbers needed to generate template in vectorized form

        :param rndGenerator: random number generator instance
        :param baseValue: underlying base value to seed template generation.
          Ignored unless template outputs it
        :param genTemplate: template string to control text generation
        :param escapeSpecialMeaning: if True, requires escape on special meaning chars.
        :returns: list of integer values which determine bounds for random number vector for template generation

        Each element of the list will be used to generate a random number between 0 and the element inclusive,
        which is then used to select words from wordlists etc for template expansion

        `_escapeSpecialMeaning` parameter allows for backwards compatibility with old style syntax while allowing
        for preferred new style template syntax. Specify as True to force escapes for special meanings,.

        """
        retval = []

        escape = False
        use_value = False
        template_len = len(genTemplate)

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
                # retval.append(str(baseValue[val_index]))
                use_value = False
            elif char == 'x' and (not escape) ^ escapeSpecialMeaning:
                retval.append(15)
                # used for retval.append(_HEX_LOWER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'X' and (not escape) ^ escapeSpecialMeaning:
                retval.append(15)
                # retval.append(_HEX_UPPER[self._getRandomInt(0, 15, rndGenerator)])
            elif char == 'd' and (not escape) ^ escapeSpecialMeaning:
                retval.append(9)
                # retval.append(_DIGITS_ZERO[self._getRandomInt(0, 9, rndGenerator)])
            elif char == 'D' and (not escape) ^ escapeSpecialMeaning:
                retval.append(8)
                #retval.append(_DIGITS_NON_ZERO[self._getRandomInt(0, 8, rndGenerator)])
            elif char == 'a' and (not escape) ^ escapeSpecialMeaning:
                retval.append(25)
                #retval.append(_LETTERS_LOWER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'A' and (not escape) ^ escapeSpecialMeaning:
                retval.append(25)
                #retval.append(_LETTERS_UPPER[self._getRandomInt(0, 25, rndGenerator)])
            elif char == 'k' and (not escape) ^ escapeSpecialMeaning:
                retval.append(25)
                # retval.append(_ALNUM_LOWER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'K' and (not escape) ^ escapeSpecialMeaning:
                retval.append(35)
                # retval.append(_ALNUM_UPPER[self._getRandomInt(0, 35, rndGenerator)])
            elif char == 'n' and escape:
                retval.append(255)
                #retval.append(str(self._getRandomInt(0, 255, rndGenerator)))
                escape = False
            elif char == 'N' and escape:
                retval.append(65535)
                #retval.append(str(self._getRandomInt(0, 65535, rndGenerator)))
                escape = False
            elif char == 'W' and escape:
                retval.append(self._lenWords - 1)
                #retval.append(self._upperWordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'w' and escape:
                retval.append(self._lenWords - 1)
                #retval.append(self._wordList[self._getRandomWordOffset(self._lenWords, rndGenerator=rndGenerator)])
                escape = False
            elif char == 'v' and escape:
                escape = False
                if following_char is not None and ('0' <= following_char <= '9'):
                    use_value = True
                else:
                    pass
                    #retval.append(str(baseValue))
            elif char == 'V' and escape:
                #retval.append(str(baseValue))
                escape = False
            else:
                #retval.append(char)
                escape = False

        if use_value:
            #retval.append(str(baseValue))
            pass

        return retval

    def valueFromSingleTemplate(self, baseValue, genTemplate, escapeSpecialMeaning=False, rndGenerator=None):
        """ Generate text from a single template

        :param baseValue: underlying base value to seed template generation.
          Ignored unless template outputs it
        :param genTemplate: template string to control text generation
        :param escapeSpecialMeaning: if True, requires escape on special meaning chars.
        :returns: combined string from template

        `_escapeSpecialMeaning` parameter allows for backwards compatibility with old style syntax while allowing
        for preferred new style template syntax. Specify as True to force escapes for special meanings,.

        """
        retval = self.stringsFromSingleTemplate(baseValue, genTemplate, escapeSpecialMeaning, rndGenerator)
        output = "".join(retval)
        return output

    def classicGenerateText(self, v):
        """entry point to use for classic udfs"""

        pdValues = pd.Series([v])
        results = self.pandasGenerateText(pdValues)
        return results[0]

    def pandasGenerateText(self, v):
        """ entry point to use for pandas udfs"""

        def valueFromRandomTemplate(originalValue, altTemplates, escapeSpecialMeaning, rndGenerator):
            numAlternatives = len(altTemplates)

            # choose alternative
            template = altTemplates[self._getRandomInt(0, numAlternatives - 1, rndGenerator)]
            return self.valueFromSingleTemplate(originalValue, template, escapeSpecialMeaning)

        rng = None  # use standard random for performance reasons
        if len(self._templates) > 1:
            results = v.apply(lambda v1, t, e, rndGenerator: valueFromRandomTemplate(v1, t, e, rndGenerator),
                              args=(self._templates, self._escapeSpecialMeaning, rng))
        else:
            results = v.apply(lambda v1, t, e, rndGenerator: self.valueFromSingleTemplate(v1, t, e, rndGenerator),
                              args=(self._templates[0], self._escapeSpecialMeaning, rng))
        return results


class ILText(TextGenerator):  # lgtm [py/missing-equals]
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
