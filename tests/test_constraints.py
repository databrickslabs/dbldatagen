import logging

import pytest
from pyspark.sql.types import IntegerType, StringType, FloatType

import dbldatagen as dg
from dbldatagen.constraints import SqlExpr, LiteralRelation, ChainedRelation, LiteralRange, RangedValues

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestConstraints:

    @pytest.fixture()
    def generationSpec1(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
                        .withColumn("code1", IntegerType(), unique_values=100)
                        .withColumn("code2", IntegerType(), min=1, max=200)
                        .withColumn("code3", IntegerType(), maxValue=10)
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code6", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        )

        return testDataSpec

    def test_simple_constraints(self, generationSpec1):
        testDataSpec = (generationSpec1
                        .withSqlConstraint("id < 100")
                        .withSqlConstraint("id > 0")
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_simple_constraints2(self, generationSpec1):
        testDataSpec = (generationSpec1
                        .withConstraint(SqlExpr("id < 100"))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 100

    def test_multiple_constraints(self, generationSpec1):
        testDataSpec = (generationSpec1
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_scalar_relation(self, generationSpec1):
        testDataSpec = (generationSpec1
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        .withConstraint(LiteralRelation("id", "<", 50))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 49

    def test_scalar_relation_bad(self, generationSpec1):
        with pytest.raises(ValueError):
            testDataSpec = (generationSpec1
                            .withConstraints([SqlExpr("id < 100"),
                                              SqlExpr("id > 0")])
                            .withConstraint(LiteralRelation("id", "<<<", 50))
                            )

            testDataDF = testDataSpec.build()

            rowCount = testDataDF.count()
            assert rowCount == 49

    @pytest.fixture()
    def generationSpec2(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
                        .withColumn("code1", IntegerType(), min=1, max=100)
                        .withColumn("code2", IntegerType(), min=50, max=150)
                        .withColumn("code3", IntegerType(), min=100, max=200)
                        )

        return testDataSpec

    def test_chained_relation(self, generationSpec2):
        testDataSpec = (generationSpec2
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        .withConstraint(ChainedRelation(["code1", "code2", "code3"], "<"))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_chained_relation2(self, generationSpec2):
        testDataSpec = (generationSpec2
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        .withConstraint(ChainedRelation(["code1", "code2", "code3"], "<="))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_chained_relation3(self, generationSpec2):
        testDataSpec = (generationSpec2
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        .withConstraint(ChainedRelation(["code3", "code2", "code1"], ">"))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_chained_relation4(self, generationSpec2):
        testDataSpec = (generationSpec2
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        .withConstraint(ChainedRelation(["code3", "code2", "code1"], ">="))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_chained_relation_bad(self, generationSpec2):
        with pytest.raises(ValueError):
            testDataSpec = (generationSpec2
                            .withConstraints([SqlExpr("id < 100"),
                                              SqlExpr("id > 0")])
                            .withConstraint(ChainedRelation(["code3", "code2", "code1"], ">>>"))
                            )

            testDataDF = testDataSpec.build()

            rowCount = testDataDF.count()
            assert rowCount == 99

    def test_literal_range(self, generationSpec2):
        testDataSpec = (generationSpec2
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        .withConstraint(LiteralRange("id", 0, 100, strict=True))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_ranged_values(self, generationSpec2):
        testDataSpec = (generationSpec2
                        .withConstraints([SqlExpr("id < 100"),
                                          SqlExpr("id > 0")])
                        .withConstraint(RangedValues("code2", "code1", "code3", strict=True))
                        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99
