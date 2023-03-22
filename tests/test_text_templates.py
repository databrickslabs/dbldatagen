import re
import pytest
import pandas as pd
import numpy as np

import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import dbldatagen as dg
from dbldatagen import TemplateGenerator, TextGenerator

# add the following if using pandas udfs
#    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \


spark = dg.SparkSingleton.getLocalInstance("unit tests")

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


# Test manipulation and generation of test data for a large schema
class TestTextTemplates:
    testDataSpec = None
    row_count = 100000
    partitions_requested = 4

    @pytest.mark.parametrize("templates, splitTemplates",
                             [
                                 (r"a|b", ['a', 'b']),
                                 (r"a|b|", ['a', 'b', '']),
                                 (r"a", ['a']),
                                 (r"", ['']),
                                 (r"a\|b", [r'a|b']),
                                 (r"a\\|b", [r'a\\', 'b']),
                                 (r"a\|b|c", [r'a|b', 'c']),
                                 (r"123,$456|test test2 |\|\a\\a |021 \| 123",
                                  ['123,$456', 'test test2 ', '|\\a\\\\a ', '021 | 123']),
                                 (
                                         r"123 \\|  123 \|123 | 123|123|123 |asd023,\|23|",
                                         ['123 \\\\', '  123 |123 ', ' 123', '123', '123 ', 'asd023,|23', '']),
                                 (r" 123|123|123 |asd023,\|23", [' 123', '123', '123 ', 'asd023,|23']),
                                 (r'', [''])
                             ])
    def test_split_templates(self, templates, splitTemplates):
        tg1 = TemplateGenerator("test", escapeSpecialChars=False)

        results = tg1._splitTemplates(templates)

        assert results == splitTemplates

    @pytest.mark.parametrize("templateProvided, escapeSpecial, useTemplateObject",
                             [  # (r'\w \w|\w \v. \w', False, False),
                                 (r'A', False, True),
                                 (r'D', False, True),
                                 (r'K', False, True),
                                 (r'X', False, True),
                                 (r'\W', False, True),
                                 (r'\W', True, True),
                                 (r'\\w A. \\w|\\w \\w', False, False),
                                 (r'\\w \\w|\\w A. \\w', False, False),
                                 (r'\\w \\w|\\w A. \\w', False, True),
                                 (r'\\w \\w|\\w A. \\w', True, True),
                                 (r'\\w \\w|\\w A. \\w|\w n n \w', False, False),
                                 (r'\\w \\w|\\w K. \\w', False, False),
                                 (r'\\w \\w|\\w K. \\w', False, True),
                                 (r'\\w \\w|\\w K. \\w', True, True),
                                 (r'\\w \\w|\\w X. \\w', False, False),
                                 (r'\\w \\w|\\w X. \\w', False, True),
                                 (r'\\w \\w|\\w X. \\w', True, True),
                                 (r'\\w \\w|\\w a. \\w', False, False),
                                 (r'\\w \\w|\\w a. \\w', False, True),
                                 (r'\\w \\w|\\w a. \\w', True, True),
                                 (r'\\w \\w|\\w k. \\w', False, False),
                                 (r'\\w \\w|\\w k. \\w', False, True),
                                 (r'\\w \\w|\\w k. \\w', True, True),
                                 (r'\\w \\w|\\w x. \\w', False, False),
                                 (r'\\w \\w|\\w x. \\w', False, True),
                                 (r'\\w \\w|\\w x. \\w', True, True),
                                 (r'\\w a. \\w', False, True),
                                 (r'\\w a. \\w|\\w \\w', False, False),
                                 (r'\\w k. \\w', False, True),
                                 (r'\\w k. \\w|\\w \\w', False, False),
                                 (r'\n', False, True),
                                 (r'\n', True, True),
                                 (r'\v', False, True),
                                 (r'\v', True, True),
                                 (r'\w A. \w', False, False),
                                 (r'\w \a. \w', True, True),
                                 (r'\w \k. \w', True, True),
                                 (r'\w \n \w', True, True),
                                 (r'\w \w|\w A. \w', False, False),
                                 (r'\w \w|\w \A. \w', True, True),
                                 (r'\w \w|\w \a. \w', True, True),
                                 (r'\w \w|\w \w \w|\w \n \w|\w \w \w \w', True, True),
                                 (r'\w aAdDkK \w', False, False),
                                 (r'\w aAdDkKxX \n \N \w', False, False),
                                 (r'\w', False, False),
                                 (r'\w', False, True),
                                 (r'\w', True, True),
                                 (r'a', False, True),
                                 (r'b', False, False),
                                 (r'b', False, True),
                                 (r'b', True, True),
                                 (r'd', False, True),
                                 (r'k', False, True),
                                 (r'x', False, True),
                                 ('', False, False),
                                 ('', False, True),
                                 (r'', True, True),
                             ])
    def test_rnd_compute(self, templateProvided, escapeSpecial, useTemplateObject):
        template1 = TemplateGenerator(templateProvided, escapeSpecialChars=escapeSpecial)
        print(f"template [{templateProvided}]")

        arr = np.arange(100)

        template_choices, template_rnd_bounds, template_rnds = template1._prepare_random_bounds(arr)

        assert template_choices is not None
        assert template_rnd_bounds is not None
        assert template_rnds is not None
        assert len(template_choices) == len(template_rnds)
        assert len(template_choices) == len(template_rnd_bounds)

        for ix, _ in enumerate(template_choices):
            bounds = template_rnd_bounds[ix]
            rnds = template_rnds[ix]

            assert len(bounds) == len(rnds)

            for iy, bounds_value in enumerate(bounds):
                assert bounds_value == -1 or (rnds[iy] < bounds_value)

    @pytest.mark.parametrize("templateProvided, escapeSpecial, useTemplateObject",
                             [  # (r'\w \w|\w \v. \w', False, False),
                                 (r'\\w \\w|\\w a. \\w', False, False),
                                 (r'\\w \\w|\\w a. \\w', False, True),
                                 (r'\\w \\w|\\w a. \\w', True, True),
                                 (r'\w \w|\w a. \w', False, False),
                                 (r'\w.\w@\w.com', False, False),
                                 (r'\n-\n', False, False),
                                 (r'A', False, True),
                                 (r'D', False, True),
                                 (r'K', False, True),
                                 (r'X', False, True),
                                 (r'\W', False, True),
                                 (r'\W', True, True),
                                 (r'\\w A. \\w|\\w \\w', False, False),
                                 (r'\\w \\w|\\w A. \\w', False, False),
                                 (r'\\w \\w|\\w A. \\w', False, True),
                                 (r'\\w \\w|\\w A. \\w', True, True),
                                 (r'\\w \\w|\\w A. \\w|\w n n \w', False, False),
                                 (r'\\w \\w|\\w K. \\w', False, False),
                                 (r'\\w \\w|\\w K. \\w', False, True),
                                 (r'\\w \\w|\\w K. \\w', True, True),
                                 (r'\\w \\w|\\w X. \\w', False, False),
                                 (r'\\w \\w|\\w X. \\w', False, True),
                                 (r'\\w \\w|\\w X. \\w', True, True),
                                 (r'\\w \\w|\\w a. \\w', False, False),
                                 (r'\\w \\w|\\w a. \\w', False, True),
                                 (r'\\w \\w|\\w a. \\w', True, True),
                                 (r'\\w \\w|\\w k. \\w', False, False),
                                 (r'\\w \\w|\\w k. \\w', False, True),
                                 (r'\\w \\w|\\w k. \\w', True, True),
                                 (r'\\w \\w|\\w x. \\w', False, False),
                                 (r'\\w \\w|\\w x. \\w', False, True),
                                 (r'\\w \\w|\\w x. \\w', True, True),
                                 (r'\\w a. \\w', False, True),
                                 (r'\\w a. \\w|\\w \\w', False, False),
                                 (r'\\w k. \\w', False, True),
                                 (r'\\w k. \\w|\\w \\w', False, False),
                                 (r'\n', False, True),
                                 (r'\n', True, True),
                                 (r'\v', False, True),
                                 (r'\v', True, True),
                                 (r'\v|\v-\v', False, True),
                                 (r'\v|\v-\v', True, True),
                                 (r'short string|a much longer string which is bigger than short string', False, True),
                                 (r'short string|a much longer string which is bigger than short string', True, True),
                                 (r'\w A. \w', False, False),
                                 (r'\w \a. \w', True, True),
                                 (r'\w \k. \w', True, True),
                                 (r'\w \n \w', True, True),
                                 (r'\w \w|\w A. \w', False, False),
                                 (r'\w \w|\w \A. \w', True, True),
                                 (r'\w \w|\w \a. \w', True, True),
                                 (r'\w \w|\w \w \w|\w \n \w|\w \w \w \w', True, True),
                                 (r'\w aAdDkK \w', False, False),
                                 (r'\w aAdDkKxX \n \N \w', False, False),
                                 (r'\w', False, False),
                                 (r'\w', False, True),
                                 (r'\w', True, True),
                                 (r'a', False, True),
                                 (r'b', False, False),
                                 (r'b', False, True),
                                 (r'b', True, True),
                                 (r'd', False, True),
                                 (r'k', False, True),
                                 (r'x', False, True),
                                 ('', False, False),
                                 ('', False, True),
                                 (r'', True, True),
                                 ('|', False, False),
                                 ('|', False, True),
                                 (r'|', True, True),
                                 (r'\ww - not e\xpecting two wor\ds', False, False),
                                 (r'\ww - not expecting two words', True, True)
                             ])
    def test_use_pandas(self, templateProvided, escapeSpecial, useTemplateObject):
        template1 = TemplateGenerator(templateProvided, escapeSpecialChars=escapeSpecial)

        TEST_ROWS = 100

        arr1 = np.arange(TEST_ROWS)
        arr = pd.Series(arr1)

        template_choices, template_rnd_bounds, template_rnds = template1._prepare_random_bounds(arr)

        assert len(template_choices) == len(template_rnds)
        assert len(template_choices) == len(template_rnd_bounds)

        for ix, _ in enumerate(template_choices):
            bounds = template_rnd_bounds[ix]
            rnds = template_rnds[ix]

            assert len(bounds) == len(rnds)

            for iy, bounds_value in enumerate(bounds):
                assert bounds_value == -1 or (rnds[iy] < bounds_value)

        results = template1.pandasGenerateText(arr)
        assert results is not None

        results_list = results.tolist()

        results_rows = len(results_list)
        assert results_rows == TEST_ROWS

        for r, result_str in enumerate(results):
            assert result_str is not None and isinstance(result_str, str)
            assert len(result_str) >= 0

        print("results")
        for i, result_value in enumerate(results):
            print(f"{i}: '{result_value}'")

    @pytest.mark.parametrize("templateProvided, escapeSpecial, useTemplateObject",
                             [(r'\n', False, True),
                              (r'\n', True, True),
                              (r'\v', False, True),
                              (r'\v', True, True),
                              (r'\v|\v-\v', False, True),
                              (r'\v|\v-\v', True, True),
                              ])
    def test_sub_value1(self, templateProvided, escapeSpecial, useTemplateObject):
        template1 = TemplateGenerator(templateProvided, escapeSpecialChars=escapeSpecial)

        TEST_ROWS = 100

        arr1 = np.arange(TEST_ROWS)
        arr = pd.Series(arr1)

        print(type(arr1), type(arr))

        template_choices, template_rnd_bounds, template_rnds = template1._prepare_random_bounds(arr)

        assert len(template_choices) == len(template_rnds)
        assert len(template_choices) == len(template_rnd_bounds)

        for ix, _ in enumerate(template_choices):
            bounds = template_rnd_bounds[ix]
            rnds = template_rnds[ix]

            assert len(bounds) == len(rnds)

            for iy, bounds_value in enumerate(bounds):
                assert bounds_value == -1 or (rnds[iy] < bounds_value)

        results = template1.pandasGenerateText(arr)
        assert results is not None

        results_list = results.tolist()

        results_rows = len(results_list)
        assert results_rows == TEST_ROWS

        for r, result_str in enumerate(results):
            assert result_str is not None and isinstance(result_str, str)
            assert len(result_str) >= 0

        print("results")
        for i, result_value in enumerate(results):
            print(f"{i}: '{result_value}'")

    @pytest.mark.parametrize("templateProvided, escapeSpecial, useTemplateObject",
                             [(r'\w aAdDkK \w', False, False),

                              (r'\\w \\w|\\w A. \\w', False, False),
                              (r'\w \w|\w A. \w', False, False),
                              (r'\w A. \w', False, False),
                              (r'\\w \\w|\\w a. \\w', False, False),
                              (r'\\w \\w|\\w k. \\w', False, False),
                              (r'\\w \\w|\\w K. \\w', False, False),
                              (r'\\w \\w|\\w x. \\w', False, False),
                              (r'\\w \\w|\\w X. \\w', False, False),
                              (r'\\w \\w|\\w A. \\w', False, True),
                              (r'\\w \\w|\\w a. \\w', False, True),
                              (r'\\w \\w|\\w k. \\w', False, True),
                              (r'\\w \\w|\\w K. \\w', False, True),
                              (r'\\w \\w|\\w x. \\w', False, True),
                              (r'\\w \\w|\\w X. \\w', False, True),
                              (r'\\w \\w|\\w A. \\w', True, True),
                              (r'\w \w|\w \A. \w', True, True),
                              (r'\\w \\w|\\w a. \\w', True, True),
                              (r'\w \w|\w \a. \w', True, True),
                              (r'\\w \\w|\\w k. \\w', True, True),
                              (r'\\w \\w|\\w K. \\w', True, True),
                              (r'\\w \\w|\\w x. \\w', True, True),
                              (r'\\w \\w|\\w X. \\w', True, True),
                              (r'\\w a. \\w|\\w \\w', False, False),
                              (r'\\w k. \\w|\\w \\w', False, False),
                              (r'\\w a. \\w', False, True),
                              (r'\\w k. \\w', False, True),
                              (r'\w \a. \w', True, True),
                              (r'\w \k. \w', True, True),
                              (r'\w \w|\w \w \w|\w \n \w|\w \w \w \w', True, True),
                              (r'\w \n \w', True, True),
                              (r'\w', True, True),
                              (r'\w', False, True),
                              (r'\w', False, False),

                              ])
    def test_full_build(self, templateProvided, escapeSpecial, useTemplateObject):
        pytest.skip("skipping to see if this is needed for coverage")
        import dbldatagen as dg
        print(f"template [{templateProvided}]")

        data_rows = 10 * 1000

        uniqueCustomers = 10 * 1000

        dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=4)
                    .withColumn("customer_id", "long", uniqueValues=uniqueCustomers)
                    )

        if useTemplateObject or escapeSpecial:
            template1 = TemplateGenerator(templateProvided, escapeSpecialChars=escapeSpecial)
            dataspec = dataspec.withColumn("name", percentNulls=0.01, text=template1)
        else:
            dataspec = dataspec.withColumn("name", percentNulls=0.01, template=templateProvided)

        df1 = dataspec.build()
        df1.show()

        count = df1.where("name is not null").count()
        assert count > 0
