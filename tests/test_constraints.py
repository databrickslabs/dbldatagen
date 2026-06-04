import logging

import pytest
from pyspark.sql.types import IntegerType, StringType, FloatType

import dbldatagen as dg
from dbldatagen.constraints import (
    SqlExpr,
    LiteralRelation,
    ChainedRelation,
    LiteralRange,
    RangedValues,
    PositiveValues,
    NegativeValues,
    UniqueCombinations,
    Constraint,
)

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestConstraints:

    @pytest.fixture()
    def generationSpec1(self):
        testDataSpec = (
            dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
            .withIdOutput()
            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
            .withColumn("code1", IntegerType(), unique_values=100)
            .withColumn("code2", IntegerType(), min=1, max=200)
            .withColumn("code3", IntegerType(), maxValue=10)
            .withColumn("positive_and_negative", IntegerType(), minValue=-100, maxValue=100)
            .withColumn("code4", StringType(), values=['a', 'b', 'c'])
            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True)
            .withColumn("code6", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
        )

        return testDataSpec

    def test_simple_constraints(self, generationSpec1):
        testDataSpec = generationSpec1.withSqlConstraint("id < 100").withSqlConstraint("id > 0")

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_simple_constraints2(self, generationSpec1):
        testDataSpec = generationSpec1.withConstraint(SqlExpr("id < 100"))

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 100

    def test_sql_expr_requires_non_empty_expression(self):
        with pytest.raises(ValueError, match="Expression must be a valid non-empty SQL string"):
            SqlExpr("")

    def test_multiple_constraints(self, generationSpec1):
        testDataSpec = generationSpec1.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")])

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    def test_streaming_exception(self, generationSpec1):
        with pytest.raises(RuntimeError):
            testDataSpec = generationSpec1.withConstraint(UniqueCombinations(["code1", "code2"]))

            testDataDF = testDataSpec.build(withStreaming=True)
            assert testDataDF is not None

    @pytest.mark.parametrize(
        "constraints,producesExpression",
        [
            ([SqlExpr("id < 100"), SqlExpr("id > 0")], True),
            ([UniqueCombinations()], False),
            ([UniqueCombinations("*")], False),
            ([UniqueCombinations(["a", "b"])], False),
        ],
    )
    def test_combine_constraints(self, constraints, producesExpression):
        constraintExpressions = [c.filterExpression for c in constraints]

        combinedConstraintExpression = Constraint.mkCombinedConstraintExpression(constraintExpressions)

        if producesExpression:
            assert combinedConstraintExpression is not None
        else:
            assert combinedConstraintExpression is None

    def test_constraint_filter_expression_cache(self):
        # check that the filter expression is the same for multiple calls
        constraint = SqlExpr("id < 100")
        filterExpression = constraint.filterExpression
        filterExpression2 = constraint.filterExpression
        assert filterExpression is filterExpression2

    @pytest.mark.parametrize(
        "column, operation, literalValue, expectedRows",
        [
            ("id", "<", 50, 49),
            ("id", "<=", 50, 50),
            ("id", ">", 50, 49),
            ("id", ">=", 50, 50),
            ("id", "==", 50, 1),
            ("id", "!=", 50, 98),
        ],
    )
    def test_scalar_relation(
        self, column, operation, literalValue, expectedRows, generationSpec1
    ):  # pylint: disable=too-many-positional-arguments

        testDataSpec = generationSpec1.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")]).withConstraint(
            LiteralRelation(column, operation, literalValue)
        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == expectedRows

    @pytest.mark.parametrize(
        "columns, strictFlag,  expectedRows",
        [
            ("positive_and_negative", True, 99),
            ("positive_and_negative", False, 100),
            ("positive_and_negative", "skip", 100),
        ],
    )
    def testNegativeValues(self, generationSpec1, columns, strictFlag, expectedRows):
        testDataSpec = generationSpec1.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")]).withConstraint(
            NegativeValues(columns, strict=strictFlag) if strictFlag != "skip" else NegativeValues(columns)
        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == 99

    @pytest.mark.parametrize(
        "columns, strictFlag,  expectedRows",
        [
            ("positive_and_negative", True, 99),
            ("positive_and_negative", False, 100),
            ("positive_and_negative", "skip", 100),
        ],
    )
    def testPositiveValues(self, generationSpec1, columns, strictFlag, expectedRows):
        testDataSpec = generationSpec1.withConstraints([SqlExpr("id < 200"), SqlExpr("id > 0")]).withConstraint(
            PositiveValues(columns, strict=strictFlag) if strictFlag != "skip" else PositiveValues(columns)
        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == expectedRows

    def test_scalar_relation_bad(self, generationSpec1):
        with pytest.raises(ValueError):
            testDataSpec = generationSpec1.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")]).withConstraint(
                LiteralRelation("id", "<<<", 50)
            )

            testDataDF = testDataSpec.build()

            rowCount = testDataDF.count()
            assert rowCount == 49

    @pytest.fixture()
    def generationSpec2(self):
        testDataSpec = (
            dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
            .withIdOutput()
            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
            .withColumn("code1", IntegerType(), min=1, max=100)
            .withColumn("code2", IntegerType(), min=50, max=150)
            .withColumn("code3", IntegerType(), min=100, max=200)
            .withColumn("code4", IntegerType(), min=1, max=300)
        )

        return testDataSpec

    @pytest.mark.parametrize(
        "columns, operation,  expectedRows",
        [
            (["code1", "code2", "code3"], "<", 99),
            (["code1", "code2", "code3"], "<=", 99),
            (["code3", "code2", "code1"], ">", 99),
            (["code3", "code2", "code1"], ">=", 99),
        ],
    )
    def test_chained_relation(self, generationSpec2, columns, operation, expectedRows):
        testDataSpec = generationSpec2.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")]).withConstraint(
            ChainedRelation(columns, operation)
        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == expectedRows

    @pytest.mark.parametrize(
        "columns, operation",
        [
            (["code3", "code2", "code1"], "<<<"),
            (None, "<="),
            (["code3"], ">"),
        ],
    )
    def test_chained_relation_bad(self, generationSpec2, columns, operation):
        with pytest.raises(ValueError):
            testDataSpec = generationSpec2.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")]).withConstraint(
                ChainedRelation(columns, operation)
            )

            testDataDF = testDataSpec.build()

    def test_unique_combinations(self, generationSpec2):
        validationDataSpec = generationSpec2.clone()
        df = validationDataSpec.build()

        validationCount = df.select('code1', 'code4').distinct().count()
        validationCount2 = df.dropDuplicates(['code1', 'code4']).count()
        print(validationCount, validationCount2)

        testDataSpec = generationSpec2.withConstraint(UniqueCombinations(["code1", "code4"]))

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == validationCount

    @pytest.fixture()
    def generationSpec3(self):
        testDataSpec = (
            dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
            .withColumn("code1", IntegerType(), min=1, max=20)
            .withColumn("code2", IntegerType(), min=1, max=30)
            .withColumn("code3", IntegerType(), min=1, max=5)
            .withColumn("code4", IntegerType(), min=1, max=10)
        )

        return testDataSpec

    def test_unique_combinations2(self, generationSpec3):
        validationDataSpec = generationSpec3.clone()
        df = validationDataSpec.build()

        validationCount = df.distinct().count()
        validationCount2 = df.dropDuplicates().count()
        print(validationCount, validationCount2)

        testDataSpec = generationSpec3.withConstraint(UniqueCombinations("*"))

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        print("rowCount", rowCount)
        assert rowCount == validationCount

    @pytest.mark.parametrize(
        "column, minValue, maxValue, strictFlag,  expectedRows",
        [
            ("id", 0, 100, True, 99),
            ("id", 0, 100, False, 99),
            ("id", 10, 20, True, 9),
            ("id", 10, 20, False, 11),
        ],
    )
    def test_literal_range(
        self, column, minValue, maxValue, strictFlag, expectedRows, generationSpec2
    ):  # pylint: disable=too-many-positional-arguments

        testDataSpec = generationSpec2.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")]).withConstraint(
            LiteralRange(column, minValue, maxValue, strict=strictFlag)
        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == expectedRows

    @pytest.mark.parametrize("strictSetting, expectedRows", [(True, 99), (False, 99)])
    def test_ranged_values(self, generationSpec2, strictSetting, expectedRows):
        testDataSpec = generationSpec2.withConstraints([SqlExpr("id < 100"), SqlExpr("id > 0")]).withConstraint(
            RangedValues("code2", "code1", "code3", strict=strictSetting)
        )

        testDataDF = testDataSpec.build()

        rowCount = testDataDF.count()
        assert rowCount == expectedRows
