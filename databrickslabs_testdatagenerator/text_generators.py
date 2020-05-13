# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` and `DataGenerator` classes
"""

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round, array, expr, udf
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType
import math
from datetime import date, datetime, timedelta
from .utils import ensure
import numpy as np
import pandas as pd

import random

hex_lower = [ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
hex_upper = [ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']
digits_non_zero = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
digits_zero = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
letters_upper = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                 'Q', 'R', 'T', 'S', 'U', 'V', 'W', 'X', 'Y', 'Z']
letters_lower = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
                 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
letters_all = letters_lower + letters_upper
alnum_lower = letters_lower+digits_zero
alnum_upper = letters_upper+digits_zero

words_lower = ['lorem', 'ipsum', 'dolor', 'sit', 'amet', 'consectetur', 'adipiscing', 'elit', 'sed', 'do',
               'eiusmod', 'tempor', 'incididunt', 'ut', 'labore', 'et', 'dolore', 'magna', 'aliqua', 'ut',
               'enim', 'ad', 'minim', 'veniam', 'quis', 'nostrud', 'exercitation', 'ullamco', 'laboris',
               'nisi', 'ut', 'aliquip', 'ex', 'ea', 'commodo', 'consequat', 'duis', 'aute', 'irure', 'dolor',
               'in', 'reprehenderit', 'in', 'voluptate', 'velit', 'esse', 'cillum', 'dolore', 'eu', 'fugiat',
               'nulla', 'pariatur', 'excepteur', 'sint', 'occaecat', 'cupidatat', 'non', 'proident', 'sunt',
               'in', 'culpa', 'qui', 'officia', 'deserunt', 'mollit', 'anim', 'id', 'est', 'laborum']

words_upper = ['LOREM', 'IPSUM', 'DOLOR', 'SIT', 'AMET', 'CONSECTETUR', 'ADIPISCING', 'ELIT', 'SED', 'DO',
               'EIUSMOD', 'TEMPOR', 'INCIDIDUNT', 'UT', 'LABORE', 'ET', 'DOLORE', 'MAGNA', 'ALIQUA', 'UT',
               'ENIM', 'AD', 'MINIM', 'VENIAM', 'QUIS', 'NOSTRUD', 'EXERCITATION', 'ULLAMCO', 'LABORIS',
               'NISI', 'UT', 'ALIQUIP', 'EX', 'EA', 'COMMODO', 'CONSEQUAT', 'DUIS', 'AUTE', 'IRURE',
               'DOLOR', 'IN', 'REPREHENDERIT', 'IN', 'VOLUPTATE', 'VELIT', 'ESSE', 'CILLUM', 'DOLORE',
               'EU', 'FUGIAT', 'NULLA', 'PARIATUR', 'EXCEPTEUR', 'SINT', 'OCCAECAT', 'CUPIDATAT', 'NON',
               'PROIDENT', 'SUNT', 'IN', 'CULPA', 'QUI', 'OFFICIA', 'DESERUNT', 'MOLLIT', 'ANIM', 'ID', 'EST', 'LABORUM']


class TextGenerators:

    @staticmethod
    def value_from_single_template(v, s):
        retval = []

        escape = False
        for char in s:
            if char == '\\':
                escape = True
            elif char == 'x' and not escape:
                retval.append(hex_lower[random.randint(0, 15)])
            elif char == 'X' and not escape:
                retval.append(hex_upper[random.randint(0, 15)])
            elif char == 'd' and not escape:
                retval.append(digits_zero[random.randint(0, 9)])
            elif char == 'D' and not escape:
                retval.append(digits_non_zero[random.randint(0, 8)])
            elif char == 'a' and not escape:
                retval.append(letters_lower[random.randint(0, 25)])
            elif char == 'A' and not escape:
                retval.append(letters_upper[random.randint(0, 25)])
            elif char == 'k' and not escape:
                retval.append(alnum_lower[random.randint(0, 35)])
            elif char == 'K' and not escape:
                retval.append(alnum_upper[random.randint(0, 35)])
            elif char == 'n' and escape:
                retval.append(str(random.randint(0, 255)))
                escape = False
            elif char == 'N' and escape:
                retval.append(str(random.randint(0, 65535)))
                escape = False
            elif char == 'W' and escape:
                retval.append(words_upper[random.randint(0, len(words_upper)) - 1])
                escape = False
            elif char == 'w' and escape:
                retval.append(words_lower[random.randint(0, len(words_lower)) - 1])
                escape = False
            elif char == 'v' and escape:
                retval.append(v)
                escape = False
            else:
                retval.append(char)
                escape = False

        return "".join(retval)

    @staticmethod
    def value_from_template(v, s):
        alternatives = s.replace(r'\|', '$escaped-sep').split('|')

        num_alternatives = len(alternatives)

        # choose alternative
        alt = alternatives[random.randint(0, num_alternatives - 1)].replace('$escaped-sep', '|')
        return TextGenerators.value_from_single_template(v, alt)

    @staticmethod
    def pandas_value_from_template(v, s):
        def value_from_template(v, s):
            alternatives = s.replace(r'\|', '$escaped-sep').split('|')

            num_alternatives = len(alternatives)

            # choose alternative
            alt = alternatives[random.randint(0, num_alternatives - 1)].replace('$escaped-sep', '|')
            return TextGenerators.value_from_single_template(v, alt)

        vlen = v.size
        i = 0
        retvals = []
        while i < vlen:
            retvals.append(value_from_template(v.at[i], s.at[i]))
            i = i + 1
        return pd.Series(retvals)
