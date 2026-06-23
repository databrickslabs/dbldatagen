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

It uses the following template tokens:

.. list-table:: Template meta-character reference
   :header-rows: 1
   :widths: 18 18 32 26 18

   * - Group
     - Token
     - Inserts or behavior
     - Range or set
     - Always escaped?
   * - Character classes
     - ``a``
     - Random lowercase letter
     - ``a`` through ``z``
     - No; mode-dependent
   * - Character classes
     - ``A``
     - Random uppercase letter
     - ``A`` through ``Z``
     - No; mode-dependent
   * - Character classes
     - ``x``
     - Random lowercase hex digit
     - ``0`` through ``9`` and ``a`` through ``f``
     - No; mode-dependent
   * - Character classes
     - ``X``
     - Random uppercase hex digit
     - ``0`` through ``9`` and ``A`` through ``F``
     - No; mode-dependent
   * - Character classes
     - ``d``
     - Random decimal digit
     - ``0`` through ``9``
     - No; mode-dependent
   * - Character classes
     - ``D``
     - Random non-zero decimal digit
     - ``1`` through ``9``
     - No; mode-dependent
   * - Character classes
     - ``k``
     - Random lowercase alphanumeric character
     - ``a`` through ``z`` and ``0`` through ``9``
     - No; mode-dependent
   * - Character classes
     - ``K``
     - Random uppercase alphanumeric character
     - ``A`` through ``Z`` and ``0`` through ``9``
     - No; mode-dependent
   * - Always-escaped classes
     - ``\n``
     - Random integer text
     - ``0`` through ``255`` inclusive; variable width
     - Yes
   * - Always-escaped classes
     - ``\N``
     - Random integer text
     - ``0`` through ``65535`` inclusive; variable width
     - Yes
   * - Always-escaped classes
     - ``\w``
     - Random lowercase word
     - Lowercase ipsum lorem word set, or ``extendedWordList``
     - Yes
   * - Always-escaped classes
     - ``\W``
     - Random uppercase word
     - Uppercase ipsum lorem word set, or uppercased ``extendedWordList``
     - Yes
   * - Base-value substitution
     - ``\v``
     - Entire base value
     - Value the column would otherwise have had
     - Yes
   * - Base-value substitution
     - ``\v0`` through ``\v9``
     - Base-value element by index
     - Element ``0`` through element ``9`` of an array/list base value
     - Yes
   * - Base-value substitution
     - ``\V``
     - Entire base value
     - Value the column would otherwise have had
     - Yes
   * - Escape
     - ``\``
     - Escapes the following character (see escaping rules below). A backslash is always treated as an
       escape marker and is not emitted as a literal character.
     - See escaping rules below
     - N/A
   * - Alternation
     - ``|``
     - Chooses one template alternative at random per generated value
     - Split on each unescaped ``|``; use ``\|`` for a literal pipe
     - No

In all other cases, the char itself is used.

Escaping and the escapeSpecialChars option
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The setting of ``escapeSpecialChars`` determines how the template generator interprets the character classes
``a``, ``A``, ``x``, ``X``, ``d``, ``D``, ``k`` and ``K``.

The default is ``escapeSpecialChars=False`` for backwards compatibility. In this mode, those character classes have
their special meaning without a leading escape. Escape one of those characters to use it as a literal character.
Always-escaped classes and base-value substitutions, such as ``\n``, ``\w``, ``\v`` and ``\V``, still require the
leading escape.

For example, the template ``r"\dr_\v"`` generates the values ``"dr_0"`` ... ``"dr_999"`` when used via the
``template=`` option and applied to the values zero to 999. Here ``\d`` is an escaped literal ``d``, ``r_`` is
literal text, and ``\v`` substitutes the base value.

If ``escapeSpecialChars=True``, those character classes only have their special meaning when preceded by an escape.
Bare characters are literal. For example, ``text=dg.TemplateGenerator(r"dr_\v", escapeSpecialChars=True)`` generates
the values ``"dr_0"`` ... ``"dr_999"`` when applied to the values zero to 999.

Alternation
^^^^^^^^^^^

An unescaped ``|`` separates the template into alternatives. One alternative is selected at random for each generated
value. Escape a pipe as ``\|`` to include a literal pipe in the generated text.

For example, ``(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd`` generates one of three phone-number shapes. The
parentheses in this template are literal output characters. Similarly, ``\w.\w@\w.com|\w@\w.co.u\k`` generates an
email-like value using one of two domain shapes.

Worked examples
^^^^^^^^^^^^^^^

The template syntax does not include repetition operators or optional groups. Repeat a class character literally for
fixed-width output, such as ``dddd`` for four random decimal digits.

.. list-table:: Template examples
   :header-rows: 1
   :widths: 35 65

   * - Template
     - Generates
   * - ``\n.\n.\n.\n``
     - A dotted IPv4-style address; each octet is a random integer from ``0`` through ``255``.
   * - ``ddd-dd-dddd``
     - A US-SSN-shaped value with decimal digits from ``0`` through ``9``.
   * - ``XXXX-XXXX-XXXX-XXXX``
     - Four groups of uppercase hexadecimal digits separated by hyphens.
   * - ``\w.\w@\w.com|\w@\w.co.u\k``
     - An email-like value in one of two shapes, with words from the configured word list.
   * - ``(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd``
     - A phone-number-shaped value in one of three formats; parentheses are literal characters.
   * - ``r"\dr_\v"`` with default ``escapeSpecialChars=False``
     - ``dr_`` followed by the base value; ``\d`` is a literal ``d`` in default mode.
   * - ``r"dr_\v"`` with ``escapeSpecialChars=True``
     - ``dr_`` followed by the base value; bare ``d`` and ``r`` are literal characters.
   * - ``\v0-\v1``
     - Elements ``0`` and ``1`` of an array/list base value joined by a hyphen.

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
