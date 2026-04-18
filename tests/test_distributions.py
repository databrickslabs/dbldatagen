import datetime

import pyspark.sql.functions as F
import pyspark.sql as psql
import pandas as pd
import numpy as np
import pytest

import dbldatagen as dg
import dbldatagen.distributions as dist

spark = dg.SparkSingleton.getLocalInstance("unit tests")


desired_weights = [9, 1, 1, 1]


class TestDistributions:
    TESTDATA_ROWS = 10000

    @pytest.fixture
    def basicDistributionInstance(self):
        class BasicDistribution(dist.DataDistribution):

            def generateNormalizedDistributionSample(self):
                return F.expr("rand()")

        return BasicDistribution()

    @classmethod
    def unique_timestamp_seconds(cls):
        return (datetime.datetime.utcnow() - datetime.datetime.fromtimestamp(0)).total_seconds()

    @classmethod
    def weights_as_percentages(cls, w):
        assert w is not None
        total = sum(w)
        percentages = [x / total * 100 for x in w]
        return percentages

    @classmethod
    def get_observed_weights(cls, df, column, values):
        assert df is not None
        assert column is not None
        assert values is not None

        observed_weights = (
            df.cube(column)
            .count()
            .withColumnRenamed(column, "value")
            .withColumnRenamed("count", "rc")
            .where("value is not null")
            .collect()
        )

        print(observed_weights)

        counts = {x.value: x.rc for x in observed_weights}
        value_count_pairs = [{'value': x, 'count': counts[x]} for x in values]
        print(value_count_pairs)

        return value_count_pairs

    def test_valid_basic_distribution(self, basicDistributionInstance):
        assert basicDistributionInstance is not None

    def test_bad_distribution_inheritance(self, basicDistributionInstance):
        # define a bad derived class (due to lack of abstract methods) to test enforcement
        with pytest.raises(TypeError):

            class MyDistribution(dist.DataDistribution):
                def dummyMethod(self):
                    pass

            myDistribution = MyDistribution()  # pylint: disable=abstract-class-instantiated
            assert myDistribution is not None

    def test_basic_np_rng(self, basicDistributionInstance):
        # check for random number generator
        rng_random = basicDistributionInstance.get_np_random_generator(-1)
        assert rng_random is not None
        random_val = rng_random.random()
        assert random_val is not None
        assert isinstance(random_val, float)

        rng_fixed = basicDistributionInstance.get_np_random_generator(42)
        assert rng_random is not None
        random_val2 = rng_fixed.random()
        assert random_val2 is not None
        assert isinstance(random_val2, float)

    def test_basic_np_basic_normal(self, basicDistributionInstance):
        # check for random number generator
        rnd_expr = basicDistributionInstance.generateNormalizedDistributionSample()
        assert rnd_expr is not None
        assert isinstance(rnd_expr, psql.Column)

        rnd_expr2 = basicDistributionInstance.withRandomSeed(42).generateNormalizedDistributionSample()
        assert rnd_expr2 is not None
        assert isinstance(rnd_expr2, psql.Column)

    def test_basic_normal_distribution(self):
        normal_dist = dist.Normal(mean=0.0, stddev=1.0)
        assert normal_dist is not None
        print(normal_dist)

        normal_dist2 = normal_dist.withRandomSeed(42)

        assert normal_dist2.randomSeed == 42
        print(normal_dist2)

        normal_dist3 = normal_dist2.withRounding(True)
        assert normal_dist3.rounding is not None

    def test_simple_normal_distribution(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.TESTDATA_ROWS, partitions=4)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
            .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution="normal")
            .withColumn(
                "sector_status_desc",
                "string",
                minValue=1,
                maxValue=200,
                step=1,
                prefix='status',
                random=True,
                distribution="normal",
            )
            .withColumn(
                "tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"], weights=desired_weights, random=True
            )
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(
            F.min('code4').alias('min_c4'),
            F.max('code4').alias('max_c4'),
            F.avg('code4').alias('mean_c4'),
            F.stddev('code4').alias('stddev_c4'),
        )
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        assert summary_data['min_c4'] == 1
        assert summary_data['max_c4'] == 40

    def test_normal_distribution(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.TESTDATA_ROWS, partitions=4)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
            .withColumn(
                "code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution=dist.Normal(1.0, 1.0)
            )
            .withColumn(
                "sector_status_desc",
                "string",
                minValue=1,
                maxValue=200,
                step=1,
                prefix='status',
                random=True,
                distribution="normal",
            )
            .withColumn(
                "tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"], weights=desired_weights, random=True
            )
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(
            F.min('code4').alias('min_c4'),
            F.max('code4').alias('max_c4'),
            F.avg('code4').alias('mean_c4'),
            F.stddev('code4').alias('stddev_c4'),
        )
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        assert summary_data['min_c4'] == 1
        assert summary_data['max_c4'] == 40

    def test_normal_distribution_seeded1(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.TESTDATA_ROWS, partitions=4, seed=42)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
            .withColumn(
                "code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution=dist.Normal(1.0, 1.0)
            )
            .withColumn(
                "sector_status_desc",
                "string",
                minValue=1,
                maxValue=200,
                step=1,
                prefix='status',
                random=True,
                distribution="normal",
            )
            .withColumn(
                "tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"], weights=desired_weights, random=True
            )
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(
            F.min('code4').alias('min_c4'),
            F.max('code4').alias('max_c4'),
            F.avg('code4').alias('mean_c4'),
            F.stddev('code4').alias('stddev_c4'),
        )
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        assert summary_data['min_c4'] == 1
        assert summary_data['max_c4'] == 40

    def test_normal_distribution_seeded2(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(
                sparkSession=spark, rows=self.TESTDATA_ROWS, partitions=4, seed=42, seedMethod="hash_fieldname"
            )
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
            .withColumn(
                "code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution=dist.Normal(1.0, 1.0)
            )
            .withColumn(
                "sector_status_desc",
                "string",
                minValue=1,
                maxValue=200,
                step=1,
                prefix='status',
                random=True,
                distribution="normal",
            )
            .withColumn(
                "tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"], weights=desired_weights, random=True
            )
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(
            F.min('code4').alias('min_c4'),
            F.max('code4').alias('max_c4'),
            F.avg('code4').alias('mean_c4'),
            F.stddev('code4').alias('stddev_c4'),
        )
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        assert summary_data['min_c4'] == 1
        assert summary_data['max_c4'] == 40

    def test_normal_generation_func(self):
        dist_instance = dist.Normal(20.0, 1.0)  # instance of normal distribution

        data_size = 10000
        # check the normal function
        means = pd.Series(np.full(data_size, 100.0))
        std_deviations = pd.Series(np.full(data_size, 20.0))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.normal_func(means, std_deviations, seeds)

        assert len(results) == len(means)

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)
        print(m1, s1)

        assert s1 == pytest.approx(0.2, abs=0.1)
        assert m1 == pytest.approx(0.5, abs=0.1)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.normal_func(means, std_deviations, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        assert s2 == pytest.approx(0.2, abs=0.1)
        assert m2 == pytest.approx(0.5, abs=0.1)

    def test_gamma_distribution(self):
        # will have implied column `id` for ordinal of row
        gamma_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.TESTDATA_ROWS, partitions=4)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
            .withColumn(
                "code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution=dist.Gamma(0.5, 0.5)
            )
            .withColumn(
                "sector_status_desc",
                "string",
                minValue=1,
                maxValue=200,
                step=1,
                prefix='status',
                random=True,
                distribution="normal",
            )
            .withColumn(
                "tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"], weights=desired_weights, random=True
            )
        )
        df_gamma_data = gamma_data_generator.build().cache()

        df_summary_general = df_gamma_data.agg(
            F.min('code4').alias('min_c4'),
            F.max('code4').alias('max_c4'),
            F.avg('code4').alias('mean_c4'),
            F.stddev('code4').alias('stddev_c4'),
        )
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        assert summary_data['min_c4'] == 1
        assert summary_data['max_c4'] == 40

    def test_gamma_generation_func(self):
        dist_instance = dist.Gamma(0.5, 0.5)  # instance of exponential distribution with sc

        data_size = 10000
        # check the normal function
        shapes = pd.Series(np.full(data_size, dist_instance.shape))
        scales = pd.Series(np.full(data_size, dist_instance.scale))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.gamma_func(shapes, scales, seeds)

        assert len(results) == len(scales)

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)
        print(m1, s1)

        assert s1 == pytest.approx(0.075, abs=0.05)
        assert m1 == pytest.approx(0.055, abs=0.05)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.gamma_func(shapes, scales, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        assert s2 == pytest.approx(0.075, abs=0.05)
        assert m2 == pytest.approx(0.055, abs=0.05)

    def test_beta_distribution(self):
        # will have implied column `id` for ordinal of row
        beta_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.TESTDATA_ROWS, partitions=4)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
            .withColumn(
                "code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution=dist.Beta(0.5, 0.5)
            )
            .withColumn(
                "sector_status_desc",
                "string",
                minValue=1,
                maxValue=200,
                step=1,
                prefix='status',
                random=True,
                distribution="normal",
            )
            .withColumn(
                "tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"], weights=desired_weights, random=True
            )
        )
        df_beta_data = beta_data_generator.build().cache()

        df_summary_general = df_beta_data.agg(
            F.min('code4').alias('min_c4'),
            F.max('code4').alias('max_c4'),
            F.avg('code4').alias('mean_c4'),
            F.stddev('code4').alias('stddev_c4'),
        )
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        assert summary_data['min_c4'] == 1
        assert summary_data['max_c4'] == 40

    def test_beta_generation_func(self):
        dist_instance = dist.Beta(0.5, 0.5)  # instance of beta distribution

        data_size = 10000
        # check the normal function
        alphas = pd.Series(np.full(data_size, dist_instance.alpha))
        betas = pd.Series(np.full(data_size, dist_instance.beta))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.beta_func(alphas, betas, seeds)

        assert len(results) == len(alphas)

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)

        assert s1 == pytest.approx(0.35, abs=0.1)
        assert m1 == pytest.approx(0.5, abs=0.1)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.beta_func(alphas, betas, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        assert s2 == pytest.approx(0.35, abs=0.1)
        assert m2 == pytest.approx(0.5, abs=0.1)

    def test_exponential_distribution(self):
        # will have implied column `id` for ordinal of row
        exponential_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.TESTDATA_ROWS, partitions=4)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
            .withColumn(
                "code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution=dist.Exponential(0.5)
            )
            .withColumn(
                "sector_status_desc",
                "string",
                minValue=1,
                maxValue=200,
                step=1,
                prefix='status',
                random=True,
                distribution="normal",
            )
            .withColumn(
                "tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"], weights=desired_weights, random=True
            )
        )
        df_exponential_data = exponential_data_generator.build().cache()

        df_summary_general = df_exponential_data.agg(
            F.min('code4').alias('min_c4'),
            F.max('code4').alias('max_c4'),
            F.avg('code4').alias('mean_c4'),
            F.stddev('code4').alias('stddev_c4'),
        )
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        assert summary_data['min_c4'] == 1
        assert summary_data['max_c4'] == 40

    def test_exponential_generation_func(self):
        dist_instance = dist.Exponential(0.5)  # instance of exponential distribution with sc

        data_size = 10000
        # check the normal function
        scales = pd.Series(np.full(data_size, dist_instance.scale))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.exponential_func(scales, seeds)

        assert len(results) == len(scales)

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)

        assert s1 == pytest.approx(0.10, abs=0.05)
        assert m1 == pytest.approx(0.10, abs=0.05)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.exponential_func(scales, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        assert s2 == pytest.approx(0.10, abs=0.05)
        assert m2 == pytest.approx(0.10, abs=0.05)

    def test_exponential_requires_rate_for_scale(self):
        """Ensure accessing scale without a rate produces a clear error."""
        exp = dist.Exponential()
        with pytest.raises(ValueError, match="Cannot compute value for 'scale'; Missing value for 'rate'"):
            _ = exp.scale

    def test_exponential_requires_rate_for_generation(self):
        """Ensure generating samples without a rate produces a clear error."""
        exp = dist.Exponential()
        with pytest.raises(ValueError, match="Cannot compute value for 'scale'; Missing value for 'rate'"):
            _ = exp.generateNormalizedDistributionSample()

    def test_distribution_string_normal(self):
        """Test that 'normal' string resolves to a Normal distribution."""
        data_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_normal_str", rows=100, seedMethod='hash_fieldname')
            .withIdOutput()
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution="normal")
        )
        df = data_generator.build()
        assert df.count() == 100

    def test_distribution_string_beta(self):
        """Test that 'beta' string resolves to a Beta distribution."""
        data_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_beta_str", rows=100, seedMethod='hash_fieldname')
            .withIdOutput()
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution="beta")
        )
        df = data_generator.build()
        assert df.count() == 100

    def test_distribution_string_gamma(self):
        """Test that 'gamma' string resolves to a Gamma distribution."""
        data_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_gamma_str", rows=100, seedMethod='hash_fieldname')
            .withIdOutput()
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution="gamma")
        )
        df = data_generator.build()
        assert df.count() == 100

    def test_distribution_string_exponential(self):
        """Test that 'exponential' string resolves to an Exponential distribution."""
        data_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_exp_str", rows=100, seedMethod='hash_fieldname')
            .withIdOutput()
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution="exponential")
        )
        df = data_generator.build()
        assert df.count() == 100

    def test_distribution_string_case_insensitive(self):
        """Test that distribution string matching is case-insensitive."""
        data_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_case", rows=100, seedMethod='hash_fieldname')
            .withIdOutput()
            .withColumn("code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution="Normal")
        )
        df = data_generator.build()
        assert df.count() == 100

    def test_distribution_string_invalid_raises_error(self):
        """Test that an invalid distribution string raises a clear ValueError."""
        with pytest.raises(ValueError, match="Unknown distribution 'uniform'"):
            dg.DataGenerator(
                sparkSession=spark, name="test_invalid", rows=100, seedMethod='hash_fieldname'
            ).withIdOutput().withColumn(
                "code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution="uniform"
            )

    def test_distribution_string_invalid_lists_valid_options(self):
        """Test that the error message for an invalid distribution lists valid options."""
        with pytest.raises(ValueError, match="Valid distribution names are: beta, exponential, gamma, normal"):
            dg.DataGenerator(
                sparkSession=spark, name="test_invalid2", rows=100, seedMethod='hash_fieldname'
            ).withIdOutput().withColumn(
                "code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution="foo"
            )

    def test_distribution_string_invalid_empty(self):
        """Test that an empty distribution string raises a clear ValueError."""
        with pytest.raises(ValueError, match="Invalid distribution spec"):
            dg.DataGenerator(
                sparkSession=spark, name="test_empty", rows=100, seedMethod='hash_fieldname'
            ).withIdOutput().withColumn(
                "code1", "integer", minValue=1, maxValue=20, step=1, random=True, distribution=""
            )

    def test_distribution_string_with_kwargs(self):
        """Test that a parameterized distribution spec overrides default kwargs."""
        beta = dist.DataDistribution.fromName("beta(alpha=3.0, beta=7.0)")
        assert isinstance(beta, dist.Beta)
        assert beta.alpha == 3.0
        assert beta.beta == 7.0

    def test_distribution_string_with_partial_kwargs(self):
        """Test that a parameterized spec overrides only the supplied kwargs."""
        beta = dist.DataDistribution.fromName("beta(alpha=4.0)")
        assert beta.alpha == 4.0
        assert beta.beta == 5.0  # registered default

    def test_distribution_string_kwargs_case_insensitive_name(self):
        """Test that the name portion of a parameterized spec is case-insensitive."""
        normal = dist.DataDistribution.fromName("Normal(mean=5, stddev=2)")
        assert normal.mean == 5
        assert normal.stddev == 2

    def test_distribution_string_kwargs_negative_value(self):
        """Test that negative numeric values are accepted in parameterized specs."""
        normal = dist.DataDistribution.fromName("normal(mean=-3.5, stddev=1)")
        assert normal.mean == -3.5

    def test_distribution_string_unknown_kwarg_raises(self):
        """Test that a kwarg not accepted by the constructor raises ValueError."""
        with pytest.raises(ValueError, match="Unknown keyword argument"):
            dist.DataDistribution.fromName("beta(gobble=5)")

    def test_distribution_string_non_numeric_value_raises(self):
        """Test that a non-numeric value raises ValueError."""
        with pytest.raises(ValueError, match="Invalid value 'gobble' for 'alpha'"):
            dist.DataDistribution.fromName("beta(alpha=gobble)")

    def test_distribution_string_unbalanced_paren_raises(self):
        """Test that an unbalanced parenthesis raises ValueError."""
        with pytest.raises(ValueError, match="Invalid distribution spec"):
            dist.DataDistribution.fromName("beta(")

    def test_distribution_string_missing_value_raises(self):
        """Test that a missing value after '=' raises ValueError."""
        with pytest.raises(ValueError, match="Missing value for keyword 'alpha'"):
            dist.DataDistribution.fromName("beta(alpha=)")

    def test_distribution_string_missing_key_raises(self):
        """Test that a missing key before '=' raises ValueError."""
        with pytest.raises(ValueError, match="Missing keyword for value '5'"):
            dist.DataDistribution.fromName("beta(=5)")

    def test_distribution_string_duplicate_kwarg_raises(self):
        """Test that duplicate kwargs in a spec raise ValueError."""
        with pytest.raises(ValueError, match="Duplicate keyword 'alpha'"):
            dist.DataDistribution.fromName("beta(alpha=1, alpha=2)")

    def test_distribution_string_boolean_value_raises(self):
        """Test that a boolean literal is rejected as a non-numeric value."""
        with pytest.raises(ValueError, match="Invalid value 'True'"):
            dist.DataDistribution.fromName("beta(alpha=True)")

    def test_distribution_string_empty_args(self):
        """Test that an empty argument list falls back to registered defaults."""
        beta = dist.DataDistribution.fromName("beta()")
        assert beta.alpha == 2.0
        assert beta.beta == 5.0
