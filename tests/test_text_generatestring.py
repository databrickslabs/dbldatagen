import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

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


# Test manipulation and generation of test data for a large schema
class TestTextGenerateString:

    @pytest.mark.parametrize("length, leadingAlpha, allUpper, allLower, allAlpha, customChars",
                             [
                                 (5, True, True, False, False, None),
                                 (5, True, False, True, False, None),
                                 (5, True, False, False, True, None),
                                 (5, False, False, False, False, None),
                                 (5, False, True, False, True, None),
                                 (5, False, False, True, True, None),
                                 (5, False, False, False, False, "01234567890ABCDEF"),
                             ])
    def test_basics(self, length, leadingAlpha, allUpper, allLower, allAlpha, customChars):

        tg1 = dg.GenerateString(length, leadingAlpha=leadingAlpha, allUpper=allUpper, allLower=allLower,
                                allAlpha=allAlpha, customChars=customChars)

        assert tg1._charAlphabet is not None
        assert tg1._firstCharAlphabet is not None

        if allUpper and allAlpha:
            alphabet = _LETTERS_UPPER
        elif allLower and allAlpha:
            alphabet = _LETTERS_LOWER
        elif allLower:
            alphabet = _LETTERS_LOWER + _DIGITS_ZERO
        elif allUpper:
            alphabet = _LETTERS_UPPER + _DIGITS_ZERO
        elif allAlpha:
            alphabet = _LETTERS_UPPER + _LETTERS_LOWER
        else:
            alphabet = _LETTERS_UPPER + _LETTERS_LOWER + _DIGITS_ZERO

        if customChars is not None:
            alphabet = set(alphabet).intersection(set(customChars))

        assert set(tg1._charAlphabet) == set(alphabet)

    @pytest.mark.parametrize("genstr",
                             [
                                 dg.GenerateString((1, 10)),
                                 dg.GenerateString((1, 10), leadingAlpha=True),
                                 dg.GenerateString((4, 64), allUpper=True),
                                 dg.GenerateString((10, 20), allLower=True),
                                 dg.GenerateString((1, 10)),
                                 dg.GenerateString((3, 15)),
                                 dg.GenerateString((17, 22)),
                                 dg.GenerateString((1, 10)),
                             ])
    def test_simple_data(self, genstr):
        dgspec = (dg.DataGenerator(sparkSession=spark, name="alt_data_set", rows=10000,
                                   partitions=4, seedMethod='hash_fieldname', verbose=True,
                                   seedColumnName="_id")
                  .withIdOutput()
                  .withColumn("code2", IntegerType(), min=0, max=10)
                  .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                  .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                  .withColumn("code5", StringType(), text=dg.GenerateString((1, 10)))
                  )

        fieldsFromGenerator = set(dgspec.getOutputColumnNames())

        df_testdata = dgspec.build()

        df_testdata.show()
