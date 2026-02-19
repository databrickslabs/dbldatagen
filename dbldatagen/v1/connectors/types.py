"""Type mapping utilities for schema extraction."""

from __future__ import annotations

from dbldatagen.v1.schema import DataType


# SQL type patterns → DataType mapping.
# Keys are tuples of uppercase prefixes to match against.
_SQL_TYPE_MAP: list[tuple[tuple[str, ...], DataType]] = [
    (("TINYINT", "SMALLINT", "MEDIUMINT", "INT2", "INT4", "INT8", "BIGINT", "INTEGER", "INT"), DataType.LONG),
    (("FLOAT", "REAL", "DOUBLE"), DataType.DOUBLE),
    (("DECIMAL", "NUMERIC", "MONEY"), DataType.DECIMAL),
    (("BOOLEAN", "BOOL", "BIT"), DataType.BOOLEAN),
    (("TIMESTAMP", "DATETIME", "TIMESTAMPTZ"), DataType.TIMESTAMP),
    (("DATE",), DataType.DATE),
    (
        ("CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "CLOB", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "STRING"),
        DataType.STRING,
    ),
]

_PYTHON_TYPE_MAP: dict[type, DataType] = {
    int: DataType.LONG,
    float: DataType.DOUBLE,
    bool: DataType.BOOLEAN,
    str: DataType.STRING,
}


def map_sql_type(sql_type: str) -> DataType:
    """Map a SQL type string to a dbldatagen.v1 DataType.

    Normalises to uppercase and strips size/precision qualifiers before matching.
    Returns STRING as fallback for unrecognised types.
    """
    normalised = sql_type.upper().split("(")[0].strip()
    for prefixes, dtype in _SQL_TYPE_MAP:
        if any(normalised == p or normalised.startswith(p) for p in prefixes):
            return dtype
    return DataType.STRING


def map_python_type(py_type: type) -> DataType:
    """Map a Python type to a dbldatagen.v1 DataType."""
    return _PYTHON_TYPE_MAP.get(py_type, DataType.STRING)
