# Generating and manipulating text data

There are a number of ways to generate and manipulate text data.

- Generating values from a specific set of values
- Formatting text based on an existing value
- Using a SQL expression to transform an existing or random value
- Using the Ipsum Lorem text generator
- Using the general purpose text generator

## Generating data from a specific set of values

You can specify a specific set of values for a column - these can be of the same type as the column data type, 
or if not, at runtime, they will be cast to the column data type automatically.

This is the simplest way to specify a small set of discrete values for a column.

The following example illustrates generating data for specific ranges of values:

```python
import dbldatagen as dg
df_spec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                                  partitions=4)
                       .withIdOutput()
                       .withColumn("code3", StringType(), values=['online', 'offline', 'unknown'])
                       .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True, percent_nulls=5)
                       .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
```

## Generating text from existing values

We can also generate text data from existing values whether numeric, or other data types via a set of transformations:

This can include:
- adding a prefix
- adding a suffix
- formatting an existing value as a string
- using a custom SQL expression to generate a string

The root value in these cases is taken from the `base_column` or in simple cases, may be specified as part of the basic 
column generation.

### Formatting text based on an existing value

Often we want to generate a text value based on some numeric value - by combining a prefix, a suffix, arbitrary 
formatting or other transformations. 

### Using a SQL expression to transform existing or random values

The `expr` attribute can be used to generate data values from arbitrary SQL expressions. These can include expressions 
such as `concat` that generate text results.

## Using Text Generators
The Data Generation framework provides a number of classes for general purpose text generation

### Using the Ipsum Lorem text generator
The Ipsum lorem text generator generates sequences of words, sentances, and paragraphs following the 
Ipsum Lorem convention used in UI mockups. It originates from a technique used in type setting.

See [Wikipedia article on `Lorem Ipsum](https://en.wikipedia.org/wiki/Lorem_ipsum)

For example

```python
import dbldatagen as dg
df_spec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                                  partitions=4)
                            .withIdOutput()
                            .withColumnSpec("sample_text", 
                                            text=dg.ILText(paragraphs=(1, 4), 
                                                           sentences=(2, 6)))
                            )
                            
df = df_spec.build()
num_rows=df.count()                          
```

### Using the general purpose text generator

The `template` attribute allows specification of templated text generation. 

Here are some examples of its use to generate dummy email addresses, ip addressed and phone numbers

```python
import dbldatagen as dg
df_spec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                                  partitions=4)
                            .withIdOutput()
                            .withColumnSpec("email", 
                                            template=r'\w.\w@\w.com|\w@\w.co.u\k')
                            .withColumnSpec("ip_addr", 
                                             template=r'\n.\n.\n.\n')
                            .withColumnSpec("phone", 
                                             template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                            )
                            
df = df_spec.build()
num_rows=df.count()                          
```

