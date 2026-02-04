.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Declarative Data Generation with DatagenSpec (Experimental)
============================================================

The DatagenSpec API provides a declarative, Pydantic-based approach to defining synthetic data generation
specifications. This allows you to define your data generation requirements as type-safe, serializable
configuration objects that can be validated, stored, and shared.

Unlike the fluent ``DataGenerator`` API which uses method chaining, the spec-based approach lets you
define what data you want in a declarative manner, then execute the generation separately.

.. warning::

   Experimental - This API is experimental and subject to change in future versions.

Key Concepts
------------

The spec-based API consists of several core components:

- **ColumnDefinition**: Defines a single column's specification including name, type, and generation options
- **DatasetDefinition**: Defines a complete table with row count, partitions, and list of columns
- **DatagenSpec**: Top-level specification containing one or more dataset definitions and output configuration
- **Generator**: Executes a DatagenSpec to generate and optionally write data

Benefits of the Spec Approach
-----------------------------

1. **Type Safety**: Pydantic models provide runtime validation and IDE autocompletion
2. **Serialization**: Specs can be serialized to JSON/YAML for storage and version control
3. **Validation**: Built-in validation catches configuration errors before generation
4. **Reusability**: Specs can be parameterized and reused across different contexts
5. **Separation of Concerns**: Definition is separate from execution

Simple Use
----------

For basic usage, define a spec and generate data from it:

.. code-block:: python

   from dbldatagen.spec.generator_spec import DatagenSpec, DatasetDefinition
   from dbldatagen.spec.column_spec import ColumnDefinition
   from dbldatagen.spec.generator_spec_impl import Generator

   # Define columns
   columns = [
       ColumnDefinition(
           name="customer_id",
           type="long",
           options={"minValue": 1000000, "maxValue": 9999999}
       ),
       ColumnDefinition(
           name="name",
           type="string",
           options={"template": r"\w \w|\w \w \w"}
       ),
       ColumnDefinition(
           name="email",
           type="string",
           options={"template": r"\w.\w@\w.com"}
       ),
   ]

   # Create dataset definition
   table_def = DatasetDefinition(
       number_of_rows=10000,
       partitions=4,
       columns=columns
   )

   # Create the spec
   spec = DatagenSpec(
       datasets={"customers": table_def},
       output_destination=None  # No automatic persistence
   )

   # Validate the spec
   spec.validate(strict=False)

   # Generate data
   generator = Generator(spark)
   generator.generateAndWriteData(spec)

Column Definition Options
-------------------------

The ``ColumnDefinition`` class supports various options for controlling data generation:

Basic Options
~~~~~~~~~~~~~

- **name**: Column name (required)
- **type**: Spark SQL data type (e.g., "string", "int", "long", "timestamp", "decimal")
- **primary**: If True, treats column as a primary key with unique values
- **nullable**: If True, column may contain NULL values
- **omit**: If True, column is generated but excluded from final output (useful for intermediate calculations)
- **baseColumn**: Name of another column to use as basis for value generation (default: "id")
- **baseColumnType**: Method for deriving values ("auto", "hash", "values")

Generation Options (in options dict)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Numeric ranges
   ColumnDefinition(
       name="age",
       type="int",
       options={"minValue": 18, "maxValue": 100}
   )

   # Discrete values
   ColumnDefinition(
       name="status",
       type="string",
       options={"values": ["active", "inactive", "pending"]}
   )

   # Template-based strings
   ColumnDefinition(
       name="phone",
       type="string",
       options={"template": r"(ddd)-ddd-dddd"}
   )

   # SQL expressions
   ColumnDefinition(
       name="full_name",
       type="string",
       options={"expr": "concat(first_name, ' ', last_name)"}
   )

   # Random generation
   ColumnDefinition(
       name="score",
       type="float",
       options={"minValue": 0.0, "maxValue": 100.0, "random": True}
   )

Primary Key Columns
~~~~~~~~~~~~~~~~~~~

Primary key columns have special behavior:

.. code-block:: python

   ColumnDefinition(
       name="id",
       type="long",
       primary=True
   )

Primary columns:

- Must have a type defined
- Cannot have min/max options
- Cannot be nullable
- Automatically use the internal ID column as their base

Derived Columns
~~~~~~~~~~~~~~~

Columns can reference other columns using ``baseColumn``:

.. code-block:: python

   columns = [
       ColumnDefinition(
           name="symbol_id",
           type="long",
           options={"minValue": 676, "maxValue": 775}
       ),
       ColumnDefinition(
           name="symbol",
           type="string",
           options={"expr": "upper(char(65 + (symbol_id % 26)))"},
           baseColumn="symbol_id"
       ),
   ]

Intermediate Columns (omit)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``omit=True`` for columns needed in calculations but not in the output:

.. code-block:: python

   columns = [
       ColumnDefinition(
           name="base_price",
           type="decimal",
           options={"minValue": 10.0, "maxValue": 1000.0},
           omit=True  # Not in final output
       ),
       ColumnDefinition(
           name="tax",
           type="decimal",
           options={"expr": "base_price * 0.08"},
           omit=True  # Not in final output
       ),
       ColumnDefinition(
           name="total_price",
           type="decimal",
           options={"expr": "base_price + tax"}
       ),
   ]

Validation
----------

Always validate your spec before generating data:

.. code-block:: python

   spec = DatagenSpec(
       datasets={"my_table": table_def}
   )

   # Strict mode (default): raises ValueError for errors OR warnings
   try:
       result = spec.validate(strict=True)
   except ValueError as e:
       print(f"Validation failed: {e}")

   # Non-strict mode: only raises for errors, tolerates warnings
   try:
       result = spec.validate(strict=False)
       print(f"Warnings: {result.warnings}")
   except ValueError as e:
       print(f"Validation error: {e}")

Validation checks include:

- At least one table is defined
- Each table has at least one column
- Row count is positive
- No duplicate column names within a table
- baseColumn references exist
- No circular dependencies in baseColumn chains
- Primary key constraints are valid
- Generator options are recognized

Output Destinations
-------------------

Data can be written to Unity Catalog tables or file system paths.

Unity Catalog Target
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from dbldatagen.spec.output_targets import UCSchemaTarget

   spec = DatagenSpec(
       datasets={"users": table_def},
       output_destination=UCSchemaTarget(
           catalog="main",
           schema_="my_schema",
           output_format="delta"  # Default
       )
   )

   # Tables written to: main.my_schema.users

File Path Target
~~~~~~~~~~~~~~~~

.. code-block:: python

   from dbldatagen.spec.output_targets import FilePathTarget

   spec = DatagenSpec(
       datasets={"users": table_def},
       output_destination=FilePathTarget(
           base_path="/mnt/data/output",
           output_format="parquet"  # or "csv"
       )
   )

   # Data written to: /mnt/data/output/users/

Generator Options
-----------------

Global options can be passed to control generation behavior:

.. code-block:: python

   spec = DatagenSpec(
       datasets={"users": table_def},
       generator_options={
           "random": True,
           "randomSeed": 42,
           "randomSeedMethod": "hash_fieldname",
           "verbose": True,
           "debug": False
       }
   )

Available options:

- **random**: Enable random data generation
- **randomSeed**: Seed for reproducible random generation
- **randomSeedMethod**: Method for computing random seeds ("fixed", "hash_fieldname")
- **verbose**: Enable verbose logging
- **debug**: Enable debug logging
- **seedColumnName**: Name of internal seed column

Interactive Display
-------------------

For Jupyter notebooks, use ``display_all_tables()`` to visualize the spec:

.. code-block:: python

   spec = DatagenSpec(
       datasets={"users": user_def, "orders": order_def}
   )

   # Displays formatted table definitions in notebook
   spec.display_all_tables()

Complete Example: Stock Ticker Data
-----------------------------------

Here's a comprehensive example generating stock ticker data with OHLC prices:

.. code-block:: python

   import random
   from dbldatagen.spec.generator_spec import DatagenSpec, DatasetDefinition
   from dbldatagen.spec.column_spec import ColumnDefinition
   from dbldatagen.spec.generator_spec_impl import Generator

   def create_stock_ticker_spec(
       number_of_rows: int = 100000,
       num_symbols: int = 100,
       start_date: str = "2024-01-01"
   ) -> DatagenSpec:
       """Create a spec for stock ticker data generation."""

       # Pre-compute random values for variation
       start_values = [1.0 + 199.0 * random.random() for _ in range(10)]
       growth_rates = [-0.1 + 0.35 * random.random() for _ in range(10)]

       columns = [
           # Symbol ID
           ColumnDefinition(
               name="symbol_id",
               type="long",
               options={"minValue": 676, "maxValue": 676 + num_symbols - 1}
           ),

           # Trading date
           ColumnDefinition(
               name="post_date",
               type="date",
               options={"expr": f"date_add(cast('{start_date}' as date), floor(id / {num_symbols}))"}
           ),

           # Starting price (intermediate)
           ColumnDefinition(
               name="start_value",
               type="decimal",
               options={"values": start_values},
               omit=True
           ),

           # Open price
           ColumnDefinition(
               name="open",
               type="decimal",
               options={"expr": "start_value * (1 + 0.02 * sin(id))"}
           ),

           # Close price
           ColumnDefinition(
               name="close",
               type="decimal",
               options={"expr": "start_value * (1 + 0.02 * cos(id))"}
           ),

           # High price
           ColumnDefinition(
               name="high",
               type="decimal",
               options={"expr": "greatest(open, close) * 1.01"}
           ),

           # Low price
           ColumnDefinition(
               name="low",
               type="decimal",
               options={"expr": "least(open, close) * 0.99"}
           ),

           # Volume
           ColumnDefinition(
               name="volume",
               type="long",
               options={"minValue": 100000, "maxValue": 5000000, "random": True}
           ),
       ]

       table_def = DatasetDefinition(
           number_of_rows=number_of_rows,
           columns=columns
       )

       return DatagenSpec(
           datasets={"stock_tickers": table_def},
           generator_options={"randomSeedMethod": "hash_fieldname"}
       )

   # Usage
   spec = create_stock_ticker_spec(number_of_rows=10000, num_symbols=50)
   spec.validate(strict=False)

   generator = Generator(spark)
   generator.generateAndWriteData(spec)

Multi-Table Specs
-----------------

A single DatagenSpec can define multiple related tables:

.. code-block:: python

   customers_def = DatasetDefinition(
       number_of_rows=1000,
       columns=[
           ColumnDefinition(name="customer_id", type="long", primary=True),
           ColumnDefinition(name="name", type="string", options={"template": r"\w \w"}),
       ]
   )

   orders_def = DatasetDefinition(
       number_of_rows=10000,
       columns=[
           ColumnDefinition(name="order_id", type="long", primary=True),
           ColumnDefinition(
               name="customer_id",
               type="long",
               options={"minValue": 1, "maxValue": 1000}  # References customers
           ),
           ColumnDefinition(
               name="order_date",
               type="date",
               options={"expr": "date_sub(current_date(), floor(rand() * 365))"}
           ),
       ]
   )

   spec = DatagenSpec(
       datasets={
           "customers": customers_def,
           "orders": orders_def
       },
       output_destination=UCSchemaTarget(catalog="main", schema_="sales")
   )

   generator = Generator(spark)
   generator.generateAndWriteData(spec)

Parameterized Specs
-------------------

Create reusable spec factories with parameters:

.. code-block:: python

   def create_user_spec(
       number_of_rows: int = 100000,
       partitions: int | None = None,
       random: bool = False
   ) -> DatagenSpec:
       """Factory function for user data specs."""
       columns = [
           ColumnDefinition(
               name="user_id",
               type="long",
               options={"minValue": 1000000, "maxValue": 9999999, "random": random}
           ),
           ColumnDefinition(
               name="email",
               type="string",
               options={"template": r"\w.\w@\w.com", "random": random}
           ),
       ]

       return DatagenSpec(
           datasets={"users": DatasetDefinition(
               number_of_rows=number_of_rows,
               partitions=partitions,
               columns=columns
           )}
       )

   # Pre-configured specs
   SMALL_USER_SPEC = create_user_spec(number_of_rows=1000)
   LARGE_USER_SPEC = create_user_spec(number_of_rows=1000000, partitions=100)
   RANDOM_USER_SPEC = create_user_spec(random=True)

Comparison with DataGenerator API
---------------------------------

The spec-based approach complements the existing fluent API:

.. list-table:: API Comparison
   :header-rows: 1

   * - Feature
     - DataGenerator (Fluent)
     - DatagenSpec (Declarative)
   * - Definition Style
     - Method chaining
     - Configuration objects
   * - Type Safety
     - Limited
     - Full Pydantic validation
   * - Serialization
     - Manual
     - Built-in JSON/dict support
   * - Validation
     - At build time
     - Explicit validate() method
   * - Reusability
     - Through code reuse
     - Through serialized specs
   * - Learning Curve
     - Lower
     - Moderate

Notes for Developers
--------------------

When creating spec-based data generation definitions:

1. **Always validate**: Call ``validate()`` before generating data to catch errors early
2. **Use omit for intermediates**: Mark calculation columns with ``omit=True`` to keep output clean
3. **Mind circular dependencies**: The validator will catch these, but design column dependencies carefully

The spec classes use Pydantic for validation. Both Pydantic v1 and v2 are supported via a
compatibility layer in ``dbldatagen.spec.compat``.
