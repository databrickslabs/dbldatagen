import re
import pytest
import pandas as pd
import numpy as np

import pyspark.sql.functions as F

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import dbldatagen as dg

# add the following if using pandas udfs
#    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \


spark = dg.SparkSingleton.getLocalInstance("unit tests")


# Test manipulation and generation of test data for a large schema
class TestValueBasedPRNG:

    def test_basics(self):
        arr = np.arange(100)

        rng = dg.ValueBasedPRNG(arr, shape=(len(arr), 10))

        assert rng is not None

        assert rng.shape == (len(arr), 10)

        print(rng.seedValues)

        assert rng.seedValues is not None
        print(rng.seedValues.shape, rng.seedValues.dtype)
        assert np.array_equal(rng.seedValues.reshape(arr.shape), arr)

    @pytest.mark.parametrize("data, shape, bounds, endpoint",
                             [
                                 (np.arange(1024), (1024, 10), 255, False),
                                 (np.arange(1024), (1024, 15), np.arange(15) * 3 + 1, False),
                                 (np.arange(1024), (1024,), 255, False),
                                 (np.arange(1024), (1024, 10), 255, False),
                                 (np.arange(1024), (1024, 15), np.arange(15) * 3 + 1, False),
                                 (1, (1, 5), [1, 2, 3, 4, 5], False),
                                 (10, None, [1, 2, 3, 4, 5], False),
                             ])
    def test_integers(self, data, shape, bounds, endpoint):

        arr = np.array(data)
        rng = dg.ValueBasedPRNG(arr, shape=shape)

        bounds_arr = np.full(shape, bounds)

        # make sure that we have results that are always simply the passed in bounds
        # they can be occasionally equal but not always equal

        cumulative_equality = bounds_arr >= 0  # will be array of boolean values all true

        for ix in range(10):
            results = rng.integers(bounds_arr, endpoint=endpoint)

            assert results is not None, "results should be not None"
            assert results.shape == bounds_arr.shape, "results should be of target shape"

            results_equality = results == bounds_arr
            cumulative_equality = cumulative_equality and results_equality
            print(cumulative_equality)

        assert not np.array_equal(results, bounds_arr)

    @pytest.mark.parametrize("data, shape",
                             [
                                 (23, (1024, 10)),
                                 (23.10, (1024, 15)),
                                 (23, None),
                                 (23.10, None),
                                 ([[1, 2, 3], [1, 2, 3]], None),
                                 ([[1, "two", 3], [1, 2, "three"]], None),
                                 ("test", None),
                                 (["test", "test2"], None),
                                 ([True, False, True], None),
                                 ((1, 2, 3), None),
                                 (np.datetime64('2005-10-24'), None)

                             ])
    def test_initialization_success(self, data, shape):

        arr = np.array(data)
        rng = dg.ValueBasedPRNG(arr, shape=shape)

        assert rng is not None

        print(rng.seedValues.shape, rng.seedValues.dtype, rng.seedValues)

    @pytest.mark.parametrize("data, shape",
                             [
                                 ({'a': 1, 'b': 2}, 25),  # bad shape
                                 ( 34, 45),               # bad shape
                                 ([[[1, 2, 3], [4, 5, 6]],
                                   [[1, 2, 3], [4, 5, 6]],
                                   [[1, 2, 3], [4, 5, 6]]], None),  # bad value - dimensions
                             ])
    def test_initialization_fail_value(self, data, shape):

        with pytest.raises(ValueError):
            arr = np.array(data)
            rng = dg.ValueBasedPRNG(arr, shape=shape)

            assert rng is not None

    @pytest.mark.parametrize("data, shape",
                             [
                                 ({'a': 1, 'b': 2}, (1024,)),
                             ])
    def test_initialization_fail_type(self, data, shape):

        with pytest.raises(TypeError):
            arr = np.array(data)
            rng = dg.ValueBasedPRNG(arr, shape=shape)

            assert rng is not None

    @pytest.mark.skip(reason="no way of currently testing this")
    def test_udfs(self):

        def exampleUdf(v):
            v1 = pd.DataFrame(v)
            mk_str_fn = lambda x: str(hash(tuple(x)))  # str(x)
            results = v1.apply(mk_str_fn, axis=1)
            return pd.Series(results)

        testUdf = F.pandas_udf(exampleUdf, returnType=StringType()).asNondeterministic()

        df = (spark.range(1024)
              .withColumn("v1", F.expr("id * id"))
              .withColumn("v2", F.expr("id * id"))
              .withColumn("v3", F.expr("id * id"))
              .withColumn("v4", F.expr("'test'"))
              .withColumn("v5", testUdf(F.array(F.col("v3"), F.col("v4"))))
              .withColumn("v6", testUdf(F.col("v3")))
              )

        df.show()
