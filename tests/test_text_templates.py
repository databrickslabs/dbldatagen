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
                                   (r'',[ ''])
                               ])
    def test_split_templates(self, templates, splitTemplates):
        tg1 = TemplateGenerator("test", escapeSpecialChars=False)

        results = tg1._splitTemplates(templates)

        assert results == splitTemplates


    @pytest.mark.parametrize("template_provided, escapeSpecial, useTemplateObject",
                             [ #(r'\w \w|\w \v. \w', False, False),
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

    def test_rnd_compute(self, template_provided, escapeSpecial, useTemplateObject):
        template1 = TemplateGenerator(template_provided, escapeSpecialChars=escapeSpecial)
        print(f"template [{template_provided}]")

        arr = np.arange(100)

        template_choices, template_rnd_bounds, template_rnds = template1._prepare_random_bounds(arr)

        assert template_choices is not None
        assert template_rnd_bounds is not None
        assert template_rnds is not None
        assert len(template_choices) == len(template_rnds)
        assert len(template_choices) == len(template_rnd_bounds)

        for ix in range(len(template_choices)):
            bounds = template_rnd_bounds[ix]
            rnds = template_rnds[ix]

            assert len(bounds) == len(rnds)

            for iy in range(len(bounds)):
                assert bounds[iy] == -1 or (rnds[iy] < bounds[iy])

    @pytest.mark.parametrize("template_provided, escapeSpecial, useTemplateObject",
                             [ #(r'\w \w|\w \v. \w', False, False),
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
                             ])
    def test_use_pandas(self, template_provided, escapeSpecial, useTemplateObject):
        template1 = TemplateGenerator(template_provided, escapeSpecialChars=escapeSpecial)
        print(f"template [{template_provided}]")

        print("max_placeholders", template1._max_placeholders )
        print("max_rnds", template1._max_rnds_needed)
        print("placeholders", template1._placeholders_needed )
        print("bounds", template1._template_rnd_bounds)

        print("templates", template1.templates)

        arr = np.arange(100)

        template_choices, template_rnd_bounds, template_rnds = template1._prepare_random_bounds(arr)

        print("choices", template_choices)
        print("rnd bounds", template_rnd_bounds)
        print("template_rnds", template_rnds)

        assert len(template_choices) == len(template_rnds)
        assert len(template_choices) == len(template_rnd_bounds)

        for ix in range(len(template_choices)):
            bounds = template_rnd_bounds[ix]
            rnds = template_rnds[ix]

            assert len(bounds) == len(rnds)

            for iy in range(len(bounds)):
                assert bounds[iy] == -1 or (rnds[iy] < bounds[iy])


        results = template1.pandasGenerateText(arr)
        print(results)

    @pytest.mark.parametrize("template_provided, escapeSpecial, useTemplateObject",
                             [ (r'\w aAdDkK \w', False, False),

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

    def test_full_build(self, template_provided, escapeSpecial, useTemplateObject):
        pytest.skip("skipping to see if this is needed for coverage")
        import dbldatagen as dg
        print(f"template [{template_provided}]")

        data_rows = 10 * 1000

        uniqueCustomers = 10 * 1000

        dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=4)
                    .withColumn("customer_id", "long", uniqueValues=uniqueCustomers)
                    )

        if useTemplateObject or escapeSpecial:
            template1 = TemplateGenerator(template_provided, escapeSpecialChars=escapeSpecial)
            dataspec = dataspec.withColumn("name", percentNulls=0.01, text=template1)
        else:
            dataspec = dataspec.withColumn("name", percentNulls=0.01, template=template_provided)

        df1 = dataspec.build()
        df1.show()

        count = df1.where("name is not null").count()
        assert count > 0


