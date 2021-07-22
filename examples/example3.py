from datetime import timedelta, datetime
import math

# examples of use

# interval = timedelta(days=1, hours=1)
# start = datetime(2017,10,1,0,0,0)
# end = datetime(2018,10,1,6,0,0)

# schema = StructType([
#  StructField("a", FloatType(), True),
#  StructField("b", IntegerType(), True),
#  StructField("c", StringType(), True),
#  StructField("c1", StringType(), True)
# ])

# will have implied column `id` for ordinal of row
# x3=(DataGenerator(rows=1000000, partitions=100).withSchema(schema)
#    # withColumnSpec adds specification for existing column
#    .withColumnSpec("a",  minValue=0, maxValue = 1.5, step=0.3, random=True)
#    .withColumnSpec("b",  minValue=10, maxValue=17, step=2, random=True)
#    # base column specifies dependent column
#    .withColumnSpec("c",  prefix='item', baseColumn='b')
#    # withColumn adds specification for new column
#    .withColumn("rand", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
#    .withColumn("start", TimestampType(), begin=start, end=end, interval=interval, random=True)
#
#   )

# x3_output = x3.build()

# if no column spec added - what is default ? generate a random value?
# print(x3_output.describe())
# print("schema", x3.schema)                 # schema of output
# print("inferred schema", x3.interimSchema) # schema during generate
