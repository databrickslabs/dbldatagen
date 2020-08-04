# databrickslabs_testdatagenerator.column_spec_options module

This file defines the ColumnSpecOptions class

<!-- !! processed by numpydoc !! -->

### class ColumnSpecOptions(props, \*\*kwargs)
Bases: `object`

Column spec options object - manages options for column specs.

This class has limited functionality - mainly used to validate and document the options, and the class is meant for internal use only.


* **Parameters**

    **props** – Used to pass list of properties for column generation spec property checking.


The following options are permitted on data generator withColumn, withColumnSpec and withColumnSpecs methods:


* **Parameters**

    
    * **name** – Column name


    * **type** – Data type of column. Can be either instance of Spark SQL Datatype such as IntegerType() or string containing SQL name of type


    * **min** – Minimum value for range of generated value. As an alternative, you may use the data_range parameter


    * **max** – Maximum value for range of generated value. As an alternative, you may use the data_range parameter


    * **step** – Step to use for range of generated value. As an alternative, you may use the data_range parameter


    * **random** – If True, will generate random values for column value. Defaults to False


    * **base_column** – Either the string name of the base column, or a list of columns to use to control data generation.


    * **values** – List of discrete values for the colummn. Discrete values for the column can be strings, numbers or constants conforming to type of column


    * **weights** – List of discrete weights for the colummn. Should be integer values.
    For example, you might declare a column for status values with a weighted distribution with the following statement:
    withColumn(“status”, StringType(), values=[‘online’, ‘offline’, ‘unknown’], weights=[3,2,1])


    * **percent_nulls** – Specifies numeric percentage of generated values to be populated with SQL null. For example: percent_nulls=12


    * **unique_values** – Number of unique values for column.
    If the unique values are specified for a timestamp or date field, the values will be chosen
    working back from the end of the previous month,
    unless begin, end and interval parameters are specified


    * **begin** – Beginning of range for date and timestamp fields.
    For dates and timestamp fields, use the begin, end and interval
    or data_range parameters instead of min, max and step


    * **end** – End of range for date and timestamp fields.
    For dates and timestamp fields, use the begin, end and interval
    or data_range parameters instead of min, max and step


    * **interval** – Interval of range for date and timestamp fields.
    For dates and timestamp fields, use the begin, end and interval
    or data_range parameters instead of min, max and step


    * **data_range** – An instance of an NRange or DateRange object. This can be used in place of min, max, step or begin, end, interval.


If the data_range parameter is specified as well as the min, max or step, the results are undetermined.
For more information, see databrickslabs_testdatagenerator.daterange module or databrickslabs_testdatagenerator.nrange module.

<!-- !! processed by numpydoc !! -->
