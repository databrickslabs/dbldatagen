from datetime import timedelta, datetime
import math
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from databrickslabs_testdatagenerator import DateRange
import databrickslabs_testdatagenerator as datagen
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

interval = timedelta(days=1, hours=1)
start = datetime(2017, 10, 1, 0, 0, 0)
end = datetime(2018, 10, 1, 6, 0, 0)

# build spark session
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Word Count") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

schema = datagen.SchemaParser.parseCreateTable(spark, """
    create table Test1 (
    site_id int ,
    site_cd string ,
    c string ,
    c1 string ,
    sector_technology_desc string )
""")

# will have implied column `id` for ordinal of row
x3 = (datagen.DataGenerator(sparkSession=spark, name="association_oss_cell_info", rows=1000000, partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("site_id", data_range=range(1, 10))
      # base column specifies dependent column
      .withIdOutput()
      .withColumnSpec("site_cd", prefix='site', base_column='site_id')
      .withColumn("sector_status_desc", "string", data_range=range(1, 5), prefix='status', random=True)
      # withColumn adds specification for new column
      .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
      .withColumn("last_sync_dt", "timestamp", data_range=DateRange(start, end, timedelta(days=1, hours=1)),
                  random=True)
      .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
      .withColumn("test_cell_flg", "int", values=[0, 1], random=True)
      )

x3_output = x3.build(withTempView=True)

x3_output.printSchema()

x3_output.show()
