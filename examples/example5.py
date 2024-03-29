from datetime import timedelta, datetime

from pyspark.sql import SparkSession

import dbldatagen as dg

interval = timedelta(days=1, hours=1)
start = datetime(2017, 10, 1, 0, 0, 0)
end = datetime(2018, 10, 1, 6, 0, 0)

# build spark session
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Word Count") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

schema = dg.SchemaParser.parseCreateTable(spark, """
    create table Test1 (
    site_id int ,
    site_cd string ,
    c string ,
    c1 string ,
    sector_technology_desc string )
""")

# will have implied column `id` for ordinal of row
x3 = (dg.DataGenerator(sparkSession=spark, name="association_oss_cell_info", rows=1000000, partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("site_id", minValue=1, maxValue=20, step=1)
      # base column specifies dependent column
      .withIdOutput()
      .withColumnSpec("site_cd", prefix='site', baseColumn='site_id')
      .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
      # withColumn adds specification for new column
      .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
      .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
      .withColumn("test_cell_flg", "int", values=[0, 1], random=True)
      )

x3_output = x3.build(withTempView=True)

x3_output.printSchema()

x3_output.show()
