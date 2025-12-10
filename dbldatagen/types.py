from typing import Literal


DbldatagenBasicType = Literal[
    "string",
    "int",
    "long",
    "float",
    "double",
    "decimal",
    "boolean",
    "date",
    "timestamp",
    "short",
    "byte",
    "binary",
    "integer",
    "bigint",
    "tinyint",
]
"""Type alias representing supported basic Spark SQL data types for column definitions.

Includes both standard SQL types (e.g. string, int, double) and Spark-specific type names
(e.g. bigint, tinyint). These types are used in the ColumnDefinition to specify the data type
for generated columns.
"""
