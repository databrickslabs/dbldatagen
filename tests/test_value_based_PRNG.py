import re
import pytest
import pandas as pd
import numpy as np

import pyspark.sql.functions as F

import pyspark.sql.functions as F

from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import dbldatagen as dg
from dbldatagen import TemplateGenerator, TextGenerator

# add the following if using pandas udfs
#    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \


spark = dg.SparkSingleton.getLocalInstance("unit tests")


# Test manipulation and generation of test data for a large schema
class TestValueBasedPRNG:

    def test_basics(self):
        arr = np.arange(100)

        rng = dg.ValueBasedPRNG(arr, shape=(len(arr), 10) )

        assert rng is not None

        assert rng.shape == (len(arr), 10)

        assert rng.seedValues is not None and np.array_equal(rng.seedValues, arr)


    @pytest.mark.parametrize("data, shape, bounds, endpoint",
                             [
                                 (np.arange(1024), (1024, 10), 255, False),
                                 (np.arange(1024), (1024, 15), np.arange(15) * 3 + 1, False),
                                 (np.arange(1024), (1024, ), 255, False),
                                 (np.arange(1024), (1024, 10), 255, False),
                                 (np.arange(1024), (1024, 15), np.arange(15) * 3 + 1, False),
                                 (1, (1, 5), [1,2,3,4,5], False),
                                 (10, None, [1, 2, 3, 4, 5], False),
                             ])
    def test_integers(self, data, shape, bounds, endpoint):

        arr = np.array(data)
        rng = dg.ValueBasedPRNG(arr, shape=shape)

        bounds_arr = np.full(shape, bounds)

        results = rng.integers(bounds_arr, endpoint=endpoint)

        assert results is not None , "results should be not None"
        assert results.shape == bounds_arr.shape, "results should be of target shape"

        # make sure that we have results that are always simply the passed in bounds
        assert not np.array_equal(results, bounds_arr)

    @pytest.mark.parametrize("data, shape",
                             [
                                 (23, (1024, 10) ),
                                 (23.10, (1024, 15) ),
                                 (23, None),
                                 (23.10, None),
                             ])
    def test_initialization_success(self, data, shape):

        arr = np.array(data)
        rng = dg.ValueBasedPRNG(arr, shape=shape)

        assert rng is not None

    @pytest.mark.parametrize("data, shape",
                             [
                                 ("test", (1024, )),
                                 ("test", None),
                             ])
    def test_initialization_fail_value(self, data, shape):

        with pytest.raises(ValueError):
            arr = np.array(data)
            rng = dg.ValueBasedPRNG(arr, shape=shape)

            assert rng is not None


    def test_udfs(self):

        def exampleUdf(v):
            v1 = pd.DataFrame(v)
            mk_str_fn = lambda x: str(hash(tuple(x)))  #str(x)
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



