import re
import logging
import pytest

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestUseOfOptions:

    @pytest.fixture
    def errorsAndWarningsLog(self, caplog):
        class ErrorsAndWarningsLogs:

            def __init__(self, caplog):
                self._caplog = caplog
                caplog.set_level(logging.WARNING)

            def clear(self):
                self._caplog.clear()

            def findMessage(self, searchText):
                seed_column_warnings_and_errors = 0
                for r in self._caplog.records:
                    if (r.levelname in ["WARNING", "ERROR"]) and searchText in r.message:
                        seed_column_warnings_and_errors += 1

                return seed_column_warnings_and_errors

        return ErrorsAndWarningsLogs(caplog)

    def test_basic(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=20000, partitions=4)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, random=True)
            .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, random=True)
            .withColumn("code4", "integer", minValue=1, maxValue=20, step=1, random=True)
            # base column specifies dependent column

            .withColumn("site_cd", "string", prefix='site', baseColumn='code1')
            .withColumn("device_status", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
            .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
            .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
        )

        df = testdata_generator.build()  # build our dataset

        numRows = df.count()

        assert numRows == 20000

        df2 = testdata_generator.option("startingId", 200000).build()  # build our dataset

        df2.count()

        # check `code` values
        code1_values = [r[0] for r in df.select("code1").distinct().collect()]
        assert set(code1_values) == set(range(1, 21))

        code2_values = [r[0] for r in df.select("code2").distinct().collect()]
        assert set(code2_values) == set(range(1, 21))

        code3_values = [r[0] for r in df.select("code3").distinct().collect()]
        assert set(code3_values) == set(range(1, 21))

        code4_values = [r[0] for r in df.select("code3").distinct().collect()]
        assert set(code4_values) == set(range(1, 21))

        site_codes = [f"site_{x}" for x in range(1, 21)]
        site_code_values = [r[0] for r in df.select("site_cd").distinct().collect()]
        assert set(site_code_values) == set(site_codes)

        status_codes = [f"status_{x}" for x in range(1, 201)]
        status_code_values = [r[0] for r in df.select("device_status").distinct().collect()]
        assert set(status_code_values) == set(status_codes)

        # check `tech` values
        tech_values = [r[0] for r in df.select("tech").distinct().collect()]
        assert set(tech_values) == set(["GSM", "UMTS", "LTE", "UNKNOWN"])

        # check test cell values
        test_cell_values = [r[0] for r in df.select("test_cell_flg").distinct().collect()]
        assert set(test_cell_values) == {0, 1}

    def test_aliased_options(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=10000, partitions=4)
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, distribution="normal")
            .withColumn("code3", "integer", min=1, max=20, step=1, base_column="code1")
            .withColumn("code4", "integer", min=1, max=20, step=1, baseColumn="code1")

            # implicit allows column definition to be overridden - used by system when initializing from schema
            .withColumn("code5", "integer", min=1, max=20, step=1, baseColumn="code1", implicit=True)

            .withColumn("code5", "integer", min=1, max=20, step=1, baseColumn="code4", random_seed=45)
            .withColumn("code6", "integer", minValue=1, maxValue=20, step=1, omit=True)
            .withColumn("code7", "integer", min=1, max=20, step=1, baseColumn="code6")
            .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, distribution="normal")
            .withColumn("site_cd1", "string", prefix='site', baseColumn='code1', text_separator="")
            .withColumn("site_cd2", "string", prefix='site', baseColumn='code1', textSeparator="-")

        )

        colSpec1 = testdata_generator.getColumnSpec("site_cd1")

        print("options", colSpec1.specOptions)

        val1 = colSpec1.getOrElse("textSeparator", "n/a")
        assert "" == val1, "invalid `textSeparator` option value for ``site_cd1``"

        val2 = colSpec1.getOrElse("text_separator", "n/a")
        assert "" == val2, "invalid `text_separator` option value for ``site_cd1``"

        colSpec2 = testdata_generator.getColumnSpec("site_cd2")
        val3 = colSpec2.getOrElse("textSeparator", "n/a")
        assert "-" == val3, "invalid `textSeparator` option value"

        df = testdata_generator.build()  # build our dataset

        match_pattern1 = re.compile(r"\s*site[0-9]+")
        match_pattern2 = re.compile(r"\s*site-[0-9]+")

        df.show()

        output = df.limit(100).collect()

        for row in output:
            site_cd1 = row["site_cd1"]
            assert site_cd1 is not None
            assert match_pattern1.match(site_cd1)

            site_cd2 = row["site_cd2"]
            assert site_cd2 is not None
            assert match_pattern2.match(site_cd2)

    def test_aliased_options2(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=10000, partitions=4)
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("site_cd1", "string", prefix='site', baseColumn='code1',
                        random_seed_method=dg.RANDOM_SEED_FIXED)
            .withColumn("site_cd2", "string", prefix='site', baseColumn='code1',
                        randomSeedMethod=dg.RANDOM_SEED_HASH_FIELD_NAME)

        )

        colSpec1 = testdata_generator.getColumnSpec("site_cd1")
        assert "randomSeedMethod" in colSpec1.specOptions, "expecting option ``randomSeedMethod`` for `site_cd1`"

        colSpec2 = testdata_generator.getColumnSpec("site_cd2")
        assert "randomSeedMethod" in colSpec2.specOptions, "expecting option ``randomSeedMethod`` for `site_cd2`"

    def test_prop_name_utils(self):
        aliases = {"One": "one"}

        props = {"one": 1, "two": 2, "three": 3}

        options = dg.ColumnSpecOptions(props, aliases)

        assert options.getOrElse("two", None) == 2, "get two"
        assert options.getOrElse("One", None) == 1, "get One"
        assert options.getOrElse("four", 4) == 4, "get four with default"

    def test_random1(self):
        # will have implied column `id` for ordinal of row
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=20000, partitions=4)
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, random=True)
            .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, random=True)
            .withColumn("code4", "integer", minValue=1, maxValue=20, step=1, random=True)
            # base column specifies dependent column

            .withColumn("site_cd", "string", prefix='site', baseColumn='code1')
            .withColumn("device_status", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
            .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
            .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
        )
        assert ds.random is False

        colSpec1 = ds.getColumnSpec("code1")
        assert colSpec1.random is False

        colSpec1 = ds.getColumnSpec("code2")
        assert colSpec1.random is True

    def test_random2(self):
        # will have implied column `id` for ordinal of row
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=20000, partitions=4, random=True)
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, random=False)
            .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, random=True)
            .withColumn("code4", "integer", minValue=1, maxValue=20, step=1)
            # base column specifies dependent column

            .withColumn("site_cd", "string", values=["one", "two", "three"], weights=[1, 2, 3], baseColumn='code1')
            .withColumn("device_status", "string", minValue=1, maxValue=200, step=1, prefix='status')
            .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"])
            .withColumn("test_cell_flg", "integer", values=[0, 1])
        )

        assert ds.random is True

        colSpec1 = ds.getColumnSpec("code1")
        assert colSpec1.random is True

        colSpec2 = ds.getColumnSpec("code2")
        assert colSpec2.random is False

        colSpec3 = ds.getColumnSpec("code3")
        assert colSpec3.random is True

    def test_random_multiple_columns(self):
        # will have implied column `id` for ordinal of row
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=1000, partitions=4, random=True)
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, numColumns=5)
            .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, numFeatures=(3, 5), structType="array")
            .withColumn("code4", "integer", minValue=1, maxValue=20, step=1)

        )

        df = ds.build()

        # check for randomness in data
        data1 = df.select("code2_0", "code2_1", "code2_2", "code2_3", "code2_4")

        df.show()

    @pytest.mark.parametrize("numFeaturesSupplied",
                             [(3, 5, 3),
                              (3.4, 3),
                              ("3", "5"),
                              "3",
                              (5, 3)
                              ])
    def test_random_multiple_columns_bad(self, numFeaturesSupplied):
        # will have implied column `id` for ordinal of row

        with pytest.raises(ValueError):
            ds = (
                dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=1000, partitions=4, random=True)
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, numColumns=5)
                .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, numFeatures=numFeaturesSupplied,
                            structType="array")
                .withColumn("code4", "integer", minValue=1, maxValue=20, step=1)

            )

            df = ds.build()

    def test_random_multiple_columns_warning(self, errorsAndWarningsLog):
        # will have implied column `id` for ordinal of row

        errorsAndWarningsLog.clear()

        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=1000, partitions=4, random=True)
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, numColumns=5)
            .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, numColumns=(3, 5))
            .withColumn("code4", "integer", minValue=1, maxValue=20, step=1, numFeatures=(3, 5))

        )

        df = ds.build()

        # find warngings about lower bounds
        msgs = errorsAndWarningsLog.findMessage("Lower bound")

        assert msgs > 0

    @pytest.mark.parametrize("numFeaturesSupplied",
                             [ 3,
                               (2, 4),
                               0,
                               (0, 3)
                              ])
    def test_multiple_columns_email(self, numFeaturesSupplied):
        # will have implied column `id` for ordinal of row

        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=1000, partitions=4, random=True)
            .withColumn("name", "string", percentNulls=0.01, template=r'\\w \\w|\\w A. \\w|test')
            .withColumn("emails", "string", template=r'\\w.\\w@\\w.com', random=True,
                        numFeatures=numFeaturesSupplied, structType="array")
        )

        df = ds.build()

        data = df.selectExpr("emails").collect()

        lengths = [len(r["emails"]) for r in data ]
        set_lengths = set(lengths)

        if isinstance(numFeaturesSupplied, int):
            min_lengths, max_lengths = numFeaturesSupplied, numFeaturesSupplied
        else:
            min_lengths, max_lengths = numFeaturesSupplied

        assert min(set_lengths) == min_lengths
        assert max(set_lengths) == max_lengths

    @pytest.mark.parametrize("numFeaturesSupplied",
                             [ 3,
                               (2, 4),
                               0,
                               (0, 3)
                              ])
    def test_multi_email_random(self, numFeaturesSupplied):
        # will have implied column `id` for ordinal of row

        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=1000, partitions=4, random=True)
            .withColumn("name", "string", percentNulls=0.01, template=r'\\w \\w|\\w A. \\w|test')
            .withColumn("emails", "string", template=r'\\w.\\w@\\w.com', random=True,
                        numFeatures=numFeaturesSupplied, structType="array")
        )

        df = ds.build()

        data = df.selectExpr("emails").collect()

        lengths = [len(set(r["emails"])) for r in data]
        set_lengths = set(lengths)

        if isinstance(numFeaturesSupplied, int):
            min_lengths, max_lengths = numFeaturesSupplied, numFeaturesSupplied
        else:
            min_lengths, max_lengths = numFeaturesSupplied

        assert min(set_lengths) == min_lengths
        assert max(set_lengths) == max_lengths
