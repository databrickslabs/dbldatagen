# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines various text generation classes and methods
"""

import math
import random

import numpy as np
import numpy.random as rnd
import pandas as pd

from .utils import ensure


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
        pass


class TemplateGenerator(TextGenerator):
    """This class handles the generation of text from templates

    :param template: template string to use in text generation
    :param escapeSpecialChars: By default special chars in the template have special meaning if unescaped
                               If set to true, then the special meaning requires escape char ``\``

    The template generator generates text from a template to allow for generation of synthetic credit card numbers,
    VINs, IBANs and many other structured codes.

    The base value is passed to the template generation and may be used in the generated text. The base value is the
    value the column would have if the template generation had not been applied.

    It uses the following special chars:

    ========  ======================================
    Chars     Meaning
    ========  ======================================
    ``\``     Apply escape to next char.
    0,1,..9   Use base value as an array of values and substitute the `nth` element ( 0 .. 9). Always escaped.
    x         Insert a random lowercase hex digit
    X         Insert an uppercase random hex digit
    d         Insert a random lowercase decimal digit
    D         Insert an uppercase random decimal digit
    a         Insert a random lowercase alphabetical character
    A         Insert a random uppercase alphabetical character
    k         Insert a random lowercase alphanumeric character
    K         Insert a random uppercase alphanumeric character
    n         Insert a random number between 0 .. 255 inclusive. This option must always be escaped
    N         Insert a random number between 0 .. 65535 inclusive. This option must always be escaped
    w         Insert a random lowercase word from the ipsum lorem word set. Always escaped
    W         Insert a random uppercase word from the ipsum lorem word set. Always escaped
    ========  ======================================

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

    def __init__(self, template, escapeSpecialChars=False):
        assert template is not None, "`template` must be specified"
        super().__init__()

        self.template = template
        self.escapeSpecialMeaning = bool(escapeSpecialChars)
        template_str0 = self.template
        self.templates = [x.replace('$__sep__', '|') for x in template_str0.replace(r'\|', '$__sep__').split('|')]

    def __repr__(self):
        return f"TemplateGenerator(template='{self.template}')"

    def valueFromSingleTemplate(self, base_value, gen_template):
        """ Generate text from a single template

        :param base_value: underlying base value to seed template generation.
          Ignored unless template outputs it
        :param gen_template: template string to control text generation

        """
        retval = []

        escape = False
        use_value = False
        template_len = len(gen_template)

        # in the following code, the construct `(not escape) ^ self.escapeSpecialMeaning` means apply special meaning if
        # either escape is not true or the option `self.escapeSpecialMeaning` is true.
        # This corresponds to the logical xor operation
        for i in range(0, template_len):
            char = gen_template[i]
            following_char = gen_template[i + 1] if i + 1 < template_len else None

            if char == '\\':
                escape = True
            elif use_value and ('0' <= char <= '9'):
                val_index = int(char)
                retval.append(str(base_value[val_index]))
                use_value = False
            elif char == 'x' and (not escape) ^ self.escapeSpecialMeaning:
                retval.append(_HEX_LOWER[random.randint(0, 15)])
            elif char == 'X' and(not escape) ^ self.escapeSpecialMeaning:
                retval.append(_HEX_UPPER[random.randint(0, 15)])
            elif char == 'd' and (not escape) ^ self.escapeSpecialMeaning:
                retval.append(_DIGITS_ZERO[random.randint(0, 9)])
            elif char == 'D' and (not escape) ^ self.escapeSpecialMeaning:
                retval.append(_DIGITS_NON_ZERO[random.randint(0, 8)])
            elif char == 'a' and (not escape) ^ self.escapeSpecialMeaning:
                retval.append(_LETTERS_LOWER[random.randint(0, 25)])
            elif char == 'A' and (not escape) ^ self.escapeSpecialMeaning:
                retval.append(_LETTERS_UPPER[random.randint(0, 25)])
            elif char == 'k' and (not escape) ^ self.escapeSpecialMeaning:
                retval.append(_ALNUM_LOWER[random.randint(0, 35)])
            elif char == 'K' and (not escape) ^ self.escapeSpecialMeaning:
                retval.append(_ALNUM_UPPER[random.randint(0, 35)])
            elif char == 'n' and escape:
                retval.append(str(random.randint(0, 255)))
                escape = False
            elif char == 'N' and escape:
                retval.append(str(random.randint(0, 65535)))
                escape = False
            elif char == 'W' and escape:
                retval.append(_WORDS_UPPER[random.randint(0, len(_WORDS_UPPER)) - 1])
                escape = False
            elif char == 'w' and escape:
                retval.append(_WORDS_LOWER[random.randint(0, len(_WORDS_LOWER)) - 1])
                escape = False
            elif char == 'v' and escape:
                escape = False
                if following_char is not None and ('0' <= following_char <= '9'):
                    use_value = True
                else:
                    retval.append(str(base_value))
            elif char == 'V' and escape:
                retval.append(str(base_value))
                escape = False
            else:
                retval.append(char)
                escape = False

        if use_value:
            retval.append(str(base_value))

        output = "".join(retval)
        return output

    def classicGenerateText(self, v):
        """entry point to use for classic udfs"""

        def value_from_template(original_value, alt_templates):
            num_alternatives = len(alt_templates)

            # choose alternative
            alt = alt_templates[random.randint(0, num_alternatives - 1)]
            return self.valueFromSingleTemplate(original_value, alt)

        return value_from_template(v, self.templates)

    def pandasGenerateText(self, v):
        """ entry point to use for pandas udfs"""

        def value_from_random_template(original_value, alt_templates):
            num_alternatives = len(alt_templates)

            # choose alternative
            alt = alt_templates[random.randint(0, num_alternatives - 1)]
            return self.valueFromSingleTemplate(original_value, alt)

        # return [ str(x)+"_Test" for x in v]
        if len(self.templates) > 1:
            results = v.apply(lambda v1, t: value_from_random_template(v1, t), args=(self.templates,))
        else:
            results = v.apply(lambda v1, t: self.valueFromSingleTemplate(v1, t), args=(self.templates[0],))
        return results


class ILText(TextGenerator):
    """ Class to generate Ipsum Lorem text paragraphs, words and sentences

    :param paragraphs: Number of paragraphs to generate. If tuple will generate random number in range
    :param sentences:  Number of sentences to generate. If tuple will generate random number in tuple range
    :param words:  Number of words per sentence to generate. If tuple, will generate random number in tuple range

    """

    @staticmethod
    def getAsTupleOrElse(v, default_v, v_name):
        """ get value v as tuple or return default value

            :param v: value to test
            :param default_v: value to use as a default if value of `v` is None. Must be a tuple.
            :returns: return `v` as tuple if not `None` or value of `default_v` if `v` is `None`. If `v` is a single
                      value, returns the tuple (`v`, `v`)"""
        assert v is None or type(v) is int or type(v) is tuple, f"param {v_name} must be an int, a tuple or None"
        assert type(default_v) is tuple and len(default_v) == 2, "default value must be tuple"

        if type(v) is int:
            return v, v
        elif type(v) is tuple:
            assert len(v) == 2, "expecting tuple of length 2"
            assert type(v[0]) is int and type(v[1]) is int, "expecting tuple with both elements as integers"
            return v
        else:
            assert len(default_v) == 2, "must have list or iterable with lenght 2"
            assert type(default_v[0]) is int and type(default_v[1]) is int, "all elements must be integers"

        return default_v

    def __init__(self, paragraphs=None, sentences=None, words=None):
        """
        Initialize the ILText with text generation parameters
        """
        assert paragraphs is not None or sentences is not None or words is not None, \
            "At least one of the params `paragraphs`, `sentences` or `words` must be specified"

        super().__init__()

        self.paragraphs = self.getAsTupleOrElse(paragraphs, (1, 1), "paragraphs")
        self.words = self.getAsTupleOrElse(words, (2, 12), "words")
        self.sentences = self.getAsTupleOrElse(sentences, (1, 1), "sentences")
        self.shape = [self.paragraphs[1], self.sentences[1], self.words[1]]

        # values needed for the text generation
        # numpy uses fixed sizes for strings , so compute whats needed
        self.np_words = np.array(_WORDS_LOWER)
        max_word_len = max([len(s) for s in _WORDS_LOWER])
        sentence_usize = (max_word_len + 1) * self.words[1] + 10
        paragraph_usize = sentence_usize * self.sentences[1] + 10
        text_usize = paragraph_usize * self.paragraphs[1] + 15
        self.sentence_dtype = f"U{sentence_usize}"
        self.para_dtype = f"U{paragraph_usize}"
        self.text_dtype = f"U{text_usize}"

        # build array of minValue and maxValue values for paragraphs, sentences and words
        self.max_vals = np.array([self.paragraphs[1], self.sentences[1], self.words[1]])
        self.min_vals = np.array([self.paragraphs[0], self.sentences[0], self.words[0]])

        # compute the range and std dev (allowing for 3.5 x std dev either side of mean)
        # we're generating truncated std normal values with mean +/- 3.5 x std_dev
        # this will determine number of paragraphs , sentences and words

        # so this will result in x paragraphs, each having y sentences, each having z words
        # we could use for loops to generate the structure, but generating the maxValue number of
        # random numbers needed in numpy will be much faster and we'll just ignore what we dont need
        self.range_vals = self.max_vals - self.min_vals
        self.mean_vals = (self.range_vals / 2.0) + self.min_vals
        self.std_vals = self.range_vals / 6.0

    def __repr__(self):
        return f"ILText(paragraphs={self.paragraphs}, sentences={self.sentences}, words={self.words})"

    def _randomGauss(self, bounds):
        """Compute random gauss value (truncated normal value) within bounds

        :param bounds: tuple containing lower and upper bound for random gaussian value
        :returns: scaled gaussian value such that `bounds[0]` <= return value <= `bounds[1]`
        """
        assert type(bounds) is tuple and len(bounds) == 2, "`bounds` must be tuple of length 2"

        min_v = bounds[0] * 1.0
        max_v = bounds[1] * 1.0

        if min_v == max_v:
            return min_v

        mean_v = (min_v + max_v) / 2
        std_v = (mean_v - min_v) / 3.5
        rnd_v = random.gauss(mean_v, std_v)
        rnd_v = min(max(rnd_v, min_v), max_v)
        return rnd_v

    def generateText(self, seed, default_seed):
        """
        generate text for seed based on configuration parameters.

        As it uses numpy, repeatability is restricted depending on version of the runtime

        :param seed: list or array-like set of seed values
        :param default_seed: seed value to use if value of seed is None or null
        :returns: list or Pandas series of generated strings of same size as input seed
        """
        assert seed is not None, "`seed` param must be specified"
        assert default_seed is not None, "`default_seed` value must be specified"

        seed_size = len(seed) if type(seed) is list else seed.shape[0]
        assert seed_size > 0, "must have valud `seed` parameter"

        stats_shape = [seed_size, self.paragraphs[1], self.sentences[1], 3]

        # get number of paragraphs, number of sentences and number of words shaped to size of the
        # word selections generated afterwards. We'll only use the first rows value for paragraphs and sentences
        # but its faster to generate all rows than to generate a ragged array
        para_stats = np.array(
            np.maximum(np.minimum(np.round(rnd.normal(self.mean_vals, self.std_vals, size=stats_shape)),
                                  self.max_vals),
                       self.min_vals),
            dtype='int')

        # get offsets for random words
        word_offsets = rnd.randint(self.np_words.size,
                                   size=(seed_size, self.paragraphs[1], self.sentences[1], self.words[1]))

        # get words
        candidate_words = self.np_words[word_offsets]

        # add words per sentence to start of array and capitalize the first word
        candidate_words2 = np.concatenate((para_stats[:, :, :, 2:3],
                                           np.char.capitalize(candidate_words[:, :, :, 0:1]),
                                           candidate_words[:, :, :, 1:]), axis=3)

        candidate_words3 = np.array(candidate_words2, dtype=self.sentence_dtype)

        # make sentence from each sentence dimension limiting the sentence to first n words
        # reminder dimensions are (rows, paragraphs, sentences, words)
        candidate_sentences = np.apply_along_axis(lambda x: " ".join(x[1:int(x[0]) + 1]) + r".", 3, candidate_words3)
        candidate_sentences2 = np.concatenate((para_stats[:, :, 0, 1:2],
                                               candidate_sentences), axis=2)
        candidate_sentences3 = np.array(candidate_sentences2, dtype=self.para_dtype)

        candidate_paras = np.apply_along_axis(lambda x: " ".join(x[1:int(x[0]) + 1]), 2, candidate_sentences3)
        candidate_paras2 = np.concatenate((para_stats[:, 0, 0, 0:1],
                                           candidate_paras), axis=1)
        candidate_paras3 = np.array(candidate_paras2, dtype=self.text_dtype)

        candidate_text = np.apply_along_axis(lambda x: "\n\n".join(x[1:int(x[0]) + 1]), 1, candidate_paras3)

        # now dimensions are rows, paras, sentences

        return [str(x) for x in candidate_text]
        # return [ [ [":::".join(s) for s in p ] for p in r] for r in candidate_sentences ]

    def classicGenerateText(self, seed):
        """
        classic udf entry point for text generation

        :param seed: seed value to control generation of random numbers
        """
        #retval = [int(round(self._randomGauss(self.paragraphs))),
        #          int(round(self._randomGauss(self.sentences))),
        #          int(round(self._randomGauss(self.words)))]

        return self.generateText([seed], 42)[0]

    def pandasGenerateText(self, v):
        """
        pandas udf entry point for text generation

        :param v: pandas series of seed values for random text generation
        :returns: Pandas series of generated strings
        """
        results = self.generateText(v.to_numpy(), 42)
        return pd.Series(results)
