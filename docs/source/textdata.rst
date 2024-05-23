.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Generating and Manipulating Text Data
=====================================

There are a number of ways to generate and manipulate text data.

- Generating values from a specific set of values
- Formatting text based on an existing value
- Using a SQL expression to transform an existing or random value
- Using the Ipsum Lorem text generator
- Using the general purpose text generator

Generating data from a specific set of values
---------------------------------------------
You can specify a specific set of values for a column - these can be of the same type as the column data type, 
or if not, at runtime, they will be cast to the column data type automatically.

This is the simplest way to specify a small set of discrete values for a column.

The following example illustrates generating data for specific ranges of values:

.. code-block:: python

   import dbldatagen as dg
   df_spec = (
       dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000,
                      partitions=4, randomSeedMethod="hash_fieldname")
      .withIdOutput()
      .withColumn("code3", "string", values=['online', 'offline', 'unknown'])
      .withColumn("code4", "string", values=['a', 'b', 'c'], random=True, percentNulls=0.05)
      .withColumn("code5", "string", values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
   )

Generating text from existing values
------------------------------------
We can also generate text data from existing values whether numeric, or other data types via a set of transformations:

This can include:
    - adding a prefix
    - adding a suffix
    - formatting an existing value as a string
    - using a custom SQL expression to generate a string

The root value in these cases is taken from the ``base_column`` or in simple cases, may be specified as part of the basic
column generation.

Formatting text based on an existing value
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Often we want to generate a text value based on some numeric value
    - by combining a prefix
    - by adding a suffix
    - by addeding arbitrary formatting or other transformations.

See the online documentation for the ``ColumnSpecOptions`` class for more details.

Using a SQL expression to transform existing or random values
-------------------------------------------------------------

The ``expr`` attribute can be used to generate data values from arbitrary SQL expressions. These can include expressions
such as ``concat`` that generate text results.

See the online documentation for the ``ColumnSpecOptions`` class for more details.

Using Text Generators
---------------------------------------------

The Data Generation framework provides a number of classes for general purpose text generation

### Using the Ipsum Lorem text generator
The Ipsum lorem text generator generates sequences of words, sentances, and paragraphs following the 
Ipsum Lorem convention used in UI mockups. It originates from a technique used in type setting.

See ``Wikipedia article`` on `Lorem Ipsum <https://en.wikipedia.org/wiki/Lorem_ipsum>`_

The following example illustrates its use:

.. code-block:: python

    import dbldatagen as dg
    df_spec = (
       dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000,
                      partitions=4, randomSeedMethod="hash_fieldname")
       .withIdOutput()
       .withColumn("sample_text", "string", text=dg.ILText(paragraphs=(1, 4),
                       sentences=(2, 6)))
    )

    df = df_spec.build()
    num_rows=df.count()

Using the general purpose text generator
---------------------------------------------

The ``template`` attribute allows specification of templated text generation.

.. note ::
   The ``template`` option is shorthand for ``text=dg.TemplateGenerator(template=...)``

   This can be specified with different options covering how escapes are handled and customizing the word list
   - see the `TemplateGenerator` documentation for more details.


.. code-block:: python

    import dbldatagen as dg
    df_spec = (
         dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000,
                          partitions=4, randomSeedMethod="hash_fieldname")
        .withIdOutput()
        .withColumn("email", "string",
                        template=r'\w.\w@\w.com|\w@\w.co.u\k')
        .withColumn("ip_addr", "string",
                         template=r'\n.\n.\n.\n')
        .withColumn("phone", "string",
                         template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')

        # the following implements the same pattern as for `phone` but using the `TemplateGenerator` class
        .withColumn("phone2", "string",
                         text=dg.TemplateGenerator(r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd'))
        )

    df = df_spec.build()
    num_rows=df.count()

The implementation of the template expansion uses the underlying `TemplateGenerator` class.

TemplateGenerator options
-------------------------

The template generator generates text from a template to allow for generation of synthetic credit card numbers,
VINs, IBANs and many other structured codes.

The base value is passed to the template generation and may be used in the generated text. The base value is the
value the column would have if the template generation had not been applied.

It uses the following special chars:

    ========  ======================================
    Chars     Meaning
    ========  ======================================
    ``\``     Apply escape to next char.
    v0,..v9   Use base value as an array of values and substitute the `nth` element ( 0 .. 9). Always escaped.
    x         Insert a random lowercase hex digit
    X         Insert an uppercase random hex digit
    d         Insert a random lowercase decimal digit
    D         Insert an uppercase random decimal digit
    a         Insert a random lowercase alphabetical character
    A         Insert a random uppercase alphabetical character
    k         Insert a random lowercase alphanumeric character
    K         Insert a random uppercase alphanumeric character
    n         Insert a random number between 0 .. 255 inclusive. This option must always be escaped
    N         Insert a random number between 0 .. 65535 inclusive. This option must always be escaped
    w         Insert a random lowercase word from the ipsum lorem word set. Always escaped
    W         Insert a random uppercase word from the ipsum lorem word set. Always escaped
    ========  ======================================

In all other cases, the char itself is used.

The setting of the ``escapeSpecialChars`` determines how the template generate interprets the special chars.

If set to False, which defaults to `False`, then the special char does not need to be escaped to have its special
meaning. But the special char must be escaped to be treated as a literal char.

So the template ``r"\dr_\v"`` will generate the values ``"dr_0"`` ... ``"dr_999"`` when used via the template option
and applied to the values zero to 999.
Here the the character `d` is escaped to avoid interpretation as a special character.

If set to True, then the special char only has its special meaning when preceded by an escape.

So the option `text=dg.TemplateGenerator(r'dr_\v', escapeSpecialChars=True)` will generate the values
``"dr_0"`` ... ``"dr_999"`` when applied to the values zero to 999.

This conforms to earlier implementations for backwards compatibility.

.. note::
          Setting the argument `escapeSpecialChars=False` means that the special char does not need to be escaped to
          be treated as a special char. But it must be escaped to be treated as a literal char.

          If the ``escapeSpecialChars`` option is set to True, then the following char only has its special
          meaning when preceded by an escape.

          Some options must be always escaped for example  ``\\v``, ``\\n`` and ``\\w``.

          A special case exists for ``\\v`` - if immediately followed by a digit 0 - 9, the underlying base value
          is interpreted as an array of values and the nth element is retrieved where `n` is the digit specified.

          The ``escapeSpecialChars`` is set to False by default for backwards compatibility.

          To use the ``escapeSpecialChars`` option, use the variant
          ``text=dg.TemplateGenerator(template=..., escapeSpecialChars=True)``


Using a custom word list
^^^^^^^^^^^^^^^^^^^^^^^^

The template generator allows specification of a custom word list also. This is a list of words that can be
used in the template generation. The default word list is the `ipsum lorem` word list.

While the `values` option allows for the specification of a list of categorical values, this is transmitted as part of
the generated SQL. The use of the `TemplateGenerator` object with a custom word list allows for specification of much
larger lists of possible values without the need to transmit them as part of the generated SQL.

For example the following code snippet illustrates the use of a custom word list:

.. code-block:: python

    import dbldatagen as dg

    names = ['alpha', 'beta', 'gamma', 'lambda', 'theta']

    df_spec = (
         dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000,
                          partitions=4, randomSeedMethod="hash_fieldname")
        .withIdOutput()
        .withColumn("email", "string",
                        template=r'\w.\w@\w.com|\w@\w.co.u\k')
        .withColumn("ip_addr", "string",
                         template=r'\n.\n.\n.\n')
        .withColumn("phone", "string",
                         template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')

        # implements the same pattern as for `phone` but using the `TemplateGenerator` class
        .withColumn("phone2", "string",
                         text=dg.TemplateGenerator(r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd'))

        # uses a custom word list
        .withColumn("name", "string",
                         text=dg.TemplateGenerator(r'\w \w|\w \w \w|\w \a. \w',
                                                   escapeSpecialChars=True,
                                                   extendedWordList=names))
        )

    df = df_spec.build()
    display(df)

Here the `names` variable is a list of names that can be used in the template generation.

While this is short list in this case, it could be a much larger list of names either
specified as a literal, or read from another dataframe, file, table or produced from another source.

As this is not transmitted as part of the generated SQL, it allows for much larger lists of possible values.

Other forms of text value lookup
--------------------------------

The use of the `values` option and the `template` option with a `TemplateGenerator` instance allow for generation of
data when the range of possible values is known.

But what about scenarios when the list of data is read from a different table or some other form of lookup?

As the output of the data generation `build()` method is a regular PySpark DataFrame, it is possible to join the
generated data with other data sources to generate the required data.

In these cases, the generator can be specified to produce lookup keys that can be used to join with the
other data sources.

