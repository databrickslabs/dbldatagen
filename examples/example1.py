from datetime import timedelta, datetime
import math
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, isnan, isnull, col, lit, countDistinct

interval = timedelta(days=1, hours=1)
start = datetime(2017, 10, 1, 0, 0, 0)
end = datetime(2018, 10, 1, 6, 0, 0)

schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True),
    StructField("sector_technology_desc", StringType(), True),

])

# build spark session

# global spark

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Word Count") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# will have implied column `id` for ordinal of row
x3 = (datagen.DataGenerator(sparkSession=spark, name="association_oss_cell_info", rows=100000, partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("site_id", minValue=1, maxValue=20, step=1)
      # base column specifies dependent column
      .withIdOutput()
      .withColumnSpec("site_cd", prefix='site', base_column='site_id')
      .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
      # withColumn adds specification for new column
      .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
      .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
      .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
      )

x3_output = x3.build(withTempView=True)

print(x3.schema)
x3_output.printSchema()
# display(x3_output)

analyzer = datagen.DataAnalyzer(x3_output)

print("Summary;", analyzer.summarize())



def extended_summary(df):
    colnames = [c for c in df.columns]
    colnames2 = ["summary"]
    colnames2.extend(colnames)

    summary_df = df.summary()
    summary_colnames = [c for c in summary_df.columns if c != "summary"]
    summary_colnames2 = ["summary"]
    summary_colnames2.extend(summary_colnames)

    print("colnames2", len(colnames2), colnames2)
    print("summary_colnames", len(summary_colnames), summary_colnames)

    summary_null_count = (df.select([count(when(col(c).isNull(), c)).alias(c) for c in summary_colnames])
                          .withColumn("summary", lit('count_isnull'))
                          .select(*summary_colnames2))

    summary_nan_count = (df.select([count(when(isnan(c), c)).alias(c) for c in summary_colnames])
                         .withColumn("summary", lit('count_isnan'))
                         .select(*summary_colnames2))

    summary_distinct_count = (df.select([countDistinct(col(c)).alias(c) for c in summary_colnames])
                              .withColumn("summary", lit('count_distinct'))
                              .select(*summary_colnames2))

    summary_df = summary_df.union(summary_null_count).union(summary_nan_count).union(summary_distinct_count)
    return summary_df


summary_rows = extended_summary(x3_output).collect()
for r in summary_rows:
    print("Summary2", r)
