"""CSV schema inference connector.

Reads one or more CSV files, infers column types via pandas, and produces a
:class:`~dbldatagen.v1.schema.DataGenPlan` (or YAML file) ready for synthetic
data generation.

Requires the ``v1-csv`` extra::

    pip install 'dbldatagen[v1-csv]'
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dbldatagen.v1.connectors.base import InferredColumn
from dbldatagen.v1.connectors.strategy import select_strategy
from dbldatagen.v1.connectors.types import map_python_type
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    PrimaryKey,
    TableSpec,
)


try:
    import pandas as pd
except ImportError:
    raise ImportError("CSV connector requires pandas. Install with: pip install 'dbldatagen[v1-csv]'") from None


def _table_name_from_path(path: str | Path) -> str:
    """Derive a table name from a CSV file path (stem without extension)."""
    return Path(path).stem


def _infer_columns(df: pd.DataFrame) -> list[InferredColumn]:
    """Inspect a pandas DataFrame and return a list of InferredColumn."""
    columns: list[InferredColumn] = []
    for col_name in df.columns:
        series = df[col_name]
        non_null = series.dropna()
        native_type = str(series.dtype)

        # Detect booleans stored as strings ("true"/"false")
        if series.dtype == object:
            lower_vals = set(non_null.astype(str).str.lower().unique())
            if lower_vals and lower_vals <= {"true", "false"}:
                native_type = "bool"
                synth_dtype = DataType.BOOLEAN
            else:
                synth_dtype = map_python_type(str)
        elif pd.api.types.is_bool_dtype(series):
            synth_dtype = DataType.BOOLEAN
        elif pd.api.types.is_integer_dtype(series):
            synth_dtype = map_python_type(int)
        elif pd.api.types.is_float_dtype(series):
            synth_dtype = map_python_type(float)
        else:
            synth_dtype = map_python_type(str)

        nullable = bool(series.isna().any())
        is_unique = bool(non_null.is_unique) if len(non_null) > 0 else False
        distinct_count = int(non_null.nunique())

        sample_values: list[Any] = non_null.head(50).tolist()

        columns.append(
            InferredColumn(
                name=col_name,
                native_type=native_type,
                synth_dtype=synth_dtype,
                nullable=nullable,
                is_primary_key=False,
                unique=is_unique,
                sample_values=sample_values,
                distinct_count=distinct_count,
            )
        )
    return columns


def _detect_primary_key(columns: list[InferredColumn]) -> str | None:
    """Detect PK: first unique, non-null column with 'id' in name."""
    for col in columns:
        if col.unique and not col.nullable and "id" in col.name.lower():
            return col.name
    return None


def _build_table_spec(
    table_name: str,
    inferred: list[InferredColumn],
    default_rows: int,
) -> TableSpec:
    """Convert inferred columns into a TableSpec."""
    pk_col_name = _detect_primary_key(inferred)

    # Mark the PK column
    for col in inferred:
        if col.name == pk_col_name:
            col.is_primary_key = True

    col_specs: list[ColumnSpec] = []
    for col in inferred:
        strategy = select_strategy(col, default_rows=default_rows)
        col_specs.append(
            ColumnSpec(
                name=col.name,
                dtype=col.synth_dtype,
                gen=strategy,
                nullable=col.nullable,
            )
        )

    primary_key = PrimaryKey(columns=[pk_col_name]) if pk_col_name else None

    return TableSpec(
        name=table_name,
        columns=col_specs,
        rows=default_rows,
        primary_key=primary_key,
    )


class CSVConnector:
    """Extract schema from one or more CSV files and produce a DataGenPlan.

    Args:
        paths: one or more CSV file paths (str or Path).
        default_rows: row count to put in each generated TableSpec.
        pandas_kwargs: extra keyword args forwarded to ``pd.read_csv``.
    """

    def __init__(
        self,
        paths: str | Path | list[str | Path],
        default_rows: int = 1000,
        **pandas_kwargs: Any,  # noqa: ANN401
    ) -> None:
        if isinstance(paths, (str, Path)):
            paths = [paths]
        self.paths = [Path(p) for p in paths]
        self.default_rows = default_rows
        self.pandas_kwargs = pandas_kwargs

    def extract(self) -> DataGenPlan:
        """Read each CSV and return a DataGenPlan."""
        tables: list[TableSpec] = []
        for path in self.paths:
            df = pd.read_csv(path, **self.pandas_kwargs)
            table_name = _table_name_from_path(path)
            inferred = _infer_columns(df)
            table_spec = _build_table_spec(table_name, inferred, self.default_rows)
            tables.append(table_spec)
        return DataGenPlan(tables=tables)

    def to_yaml(self, output_path: str) -> None:
        """Extract the plan and write it as YAML to *output_path*."""
        try:
            import yaml
        except ImportError:
            raise ImportError("YAML export requires PyYAML. Install with: pip install pyyaml") from None
        plan = self.extract()
        data = plan.model_dump(mode="json")
        with open(output_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def extract_from_csv(
    paths: str | Path | list[str | Path],
    default_rows: int = 1000,
    **pandas_kwargs: Any,  # noqa: ANN401
) -> DataGenPlan:
    """Convenience function: extract a DataGenPlan from CSV file(s).

    Args:
        paths: one or more CSV file paths.
        default_rows: row count for each table (default 1000).
        **pandas_kwargs: forwarded to ``pd.read_csv``.

    Returns:
        A :class:`~dbldatagen.v1.schema.DataGenPlan`.
    """
    return CSVConnector(paths, default_rows=default_rows, **pandas_kwargs).extract()
