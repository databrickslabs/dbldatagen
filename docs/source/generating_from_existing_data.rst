.. Databricks Labs Data Generator documentation master file

Generating Synthetic Data from Existing Data or Schema (Experimental)
=====================================================================

A common scenario for generating synthetic data is to be able to generate a synthetic data version of an
existing data set.

The process often progresses along the following steps:

- Analyze your existing data set to determine distribution, values, skew etc of the data
- Generate synthetic data to mirror existing data set
- More advanced use cases may include use of an ML model trained on the original data for adjudicating whether
  the synthetic data conforms under specific criteria to the original data set

The Databricks Labs Data Generator provides the DataAnalyzer class to assist with this process.

For more information, see :data:`~dbldatagen.data_analyzer.DataAnalyzer`

Analyzing the data to be synthesized
------------------------------------

You can use the ``summarizeToDf()`` method to generate a summary analysis of a dataframe.

Example:

.. code-block:: python

   import dbldatagen as dg

   dfSource = spark.read.format("parquet").load("/tmp/your/source/dataset")

   analyzer = dg.DataAnalyzer(sparkSession=spark, df=dfSource)

   display(analyzer.summarizeToDf())

Generating code to produce the synthetic data set
-------------------------------------------------

You can use the methods ``scriptDataGeneratorFromSchema`` and the ``scriptDataGeneratorFromData`` to generate code
from an existing schema or Spark dataframe respectively.

For example, the following code will generate code to synthesize data from an existing schema.

The generated code is intended to be stub code and will need to be modified to match your desired data layout.

.. code-block:: python

   import dbldatagen as dg

   dfSource = spark.read.format("parquet").load("/tmp/your/source/dataset")

   code =  dg.DataAnalyzer.scriptDataGeneratorFromSchema(dfSource.schema)

This might produce the following output:

.. code-block:: python

   import dbldatagen as dg

   # Column definitions are stubs only - modify to generate correct data
   generation_spec = (
        dg.DataGenerator(sparkSession=spark,
                         name='synthetic_data',
                         rows=100000,
                         random=True,
                         )
        .withColumn('asin', 'string', template=r'\\w')
        .withColumn('brand', 'string', template=r'\\w')
        .withColumn('helpful', 'bigint', minValue=1, maxValue=1000000,
                    structType='array',
                    numFeatures=(2,6))
        .withColumn('img', 'string', template=r'\\w')
        .withColumn('price', 'double', minValue=1.0, maxValue=1000000.0, step=0.1)
        .withColumn('rating', 'double', minValue=1.0, maxValue=1000000.0, step=0.1)
        .withColumn('review', 'string', template=r'\\w')
        .withColumn('time', 'bigint', minValue=1, maxValue=1000000)
        .withColumn('title', 'string', template=r'\\w')
        .withColumn('user', 'string', template=r'\\w')
        .withColumn('event_ts', 'timestamp',
                    begin="2020-01-01 01:00:00",
                    end="2020-12-31 23:59:00",
                    interval="1 minute" )
        .withColumn('r_value', 'float', minValue=1.0, maxValue=1000000.0, step=0.1,
                    structType='array',
                    numFeatures=(2,6))
        .withColumn('tf_flag', 'boolean', expr='id % 2 = 1')
        .withColumn('short_value', 'smallint', minValue=1, maxValue=32767)
        .withColumn('byte_value', 'tinyint', minValue=1, maxValue=127)
        .withColumn('decimal_value', 'decimal(10,2)', minValue=1, maxValue=1000)
        .withColumn('decimal_value', 'decimal(10,2)', minValue=1, maxValue=1000)
        .withColumn('date_value', 'date', expr='current_date()')
        .withColumn('binary_value', 'binary',
                    expr="cast('dbldatagen generated synthetic data' as binary)" )
    )


When generating code from a schema, the code generation process does not have information about the data attributes
and so the generated code just illustrates some typical generation expressions.

If we supply a source data frame to the generation process, the data analyzer instance will use information derived
from the summary data analysis to refine attributes in the generated code to match the source data better.

As this is still in the experimental stages, the refinement will continue to evolve over coming versions.

 .. note::

   Note the both of the generate code methods will print the generated code to the output. If you just
   want the generated code as a string, set the ``suppressOutput`` parameter of the methods to True.

For example, the following code will generate synthetic data generation code from a source dataframe.

.. code-block:: python

    import dbldatagen as dg

    # In a Databricks runtime environment
    # The folder `dbfs:/databricks-datasets` contains a variety of sample data sets
    dfSource = spark.read.format("parquet").load("dbfs:/databricks-datasets/amazon/test4K/")

    da = dg.DataAnalyzer(dfSource)

    df2 = da.summarizeToDF()
    generatedCode = da.scriptDataGeneratorFromData(suppressOutput=True)

    print(generatedCode)


It is not intended to generate complete code to reproduce the dataset but serves as a starting point
for generating a synthetic data set that mirrors the original source.

This will produce the following generated code.

.. code-block:: python
    import dbldatagen as dg

    # Column definitions are stubs only - modify to generate correct data
    #

    # values for column `rating`
    rating_weights = [10, 5, 8, 18, 59]
    rating_values = [1.0, 2.0, 3.0, 4.0, 5.0]

    generation_spec = (
        dg.DataGenerator(sparkSession=spark,
                         name='synthetic_data',
                         rows=100000,
                         random=True,
                         )
        .withColumn('asin', 'string', template=r'\\w')
        .withColumn('brand', 'string', template=r'\\w')
        .withColumn('helpful', 'bigint', minValue=0, maxValue=417, structType='array', numFeatures=2)
        .withColumn('img', 'string', template=r'\\w')
        .withColumn('price', 'double', minValue=0.01, maxValue=962.0, step=0.1)
        .withColumn('rating', 'double', weights = rating_weights, values=rating_values)
        .withColumn('review', 'string', template=r'\\w')
        .withColumn('time', 'bigint', minValue=921369600, maxValue=1406073600)
        .withColumn('title', 'string', template=r'\\w')
        .withColumn('user', 'string', template=r'\\w')
        )
