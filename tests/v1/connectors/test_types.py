"""Tests for SQL and Python type mapping."""

from __future__ import annotations

import pytest

from dbldatagen.v1.connectors.types import map_python_type, map_sql_type
from dbldatagen.v1.schema import DataType


class TestMapSqlType:
    @pytest.mark.parametrize(
        "sql_type,expected",
        [
            ("INTEGER", DataType.LONG),
            ("INT", DataType.LONG),
            ("BIGINT", DataType.LONG),
            ("SMALLINT", DataType.LONG),
            ("TINYINT", DataType.LONG),
            ("INT4", DataType.LONG),
            ("INT8", DataType.LONG),
            ("FLOAT", DataType.DOUBLE),
            ("REAL", DataType.DOUBLE),
            ("DOUBLE", DataType.DOUBLE),
            ("DOUBLE PRECISION", DataType.DOUBLE),
            ("DECIMAL(10,2)", DataType.DECIMAL),
            ("NUMERIC(5)", DataType.DECIMAL),
            ("VARCHAR(255)", DataType.STRING),
            ("CHAR(10)", DataType.STRING),
            ("TEXT", DataType.STRING),
            ("NVARCHAR(100)", DataType.STRING),
            ("CLOB", DataType.STRING),
            ("BOOLEAN", DataType.BOOLEAN),
            ("BOOL", DataType.BOOLEAN),
            ("BIT", DataType.BOOLEAN),
            ("DATE", DataType.DATE),
            ("TIMESTAMP", DataType.TIMESTAMP),
            ("DATETIME", DataType.TIMESTAMP),
            ("TIMESTAMPTZ", DataType.TIMESTAMP),
        ],
    )
    def test_known_types(self, sql_type, expected):
        assert map_sql_type(sql_type) == expected

    def test_case_insensitive(self):
        assert map_sql_type("varchar(50)") == DataType.STRING
        assert map_sql_type("Integer") == DataType.LONG

    def test_unknown_defaults_to_string(self):
        assert map_sql_type("XML") == DataType.STRING
        assert map_sql_type("JSONB") == DataType.STRING


class TestMapPythonType:
    @pytest.mark.parametrize(
        "py_type,expected",
        [
            (int, DataType.LONG),
            (float, DataType.DOUBLE),
            (bool, DataType.BOOLEAN),
            (str, DataType.STRING),
        ],
    )
    def test_known_types(self, py_type, expected):
        assert map_python_type(py_type) == expected

    def test_unknown_defaults_to_string(self):
        assert map_python_type(bytes) == DataType.STRING
