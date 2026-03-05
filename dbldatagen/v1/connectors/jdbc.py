"""JDBC/SQLAlchemy-based schema extraction connector."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import MetaData as SAMetaData
from sqlalchemy import Table as SATable
from sqlalchemy import create_engine, inspect, text
from sqlalchemy import select as sa_select
from sqlalchemy.engine.reflection import Inspector

from dbldatagen.v1.connectors.base import InferredColumn
from dbldatagen.v1.connectors.strategy import select_strategy
from dbldatagen.v1.connectors.types import map_sql_type
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    ForeignKeyRef,
    PrimaryKey,
    TableSpec,
)


_SAMPLE_LIMIT = 100


class JDBCConnector:
    """Extract schema from a SQL database via SQLAlchemy and produce a DataGenPlan.

    Parameters
    ----------
    connection_string:
        Any SQLAlchemy-compatible URL, e.g. ``"sqlite:///my.db"`` or
        ``"postgresql://user:pass@host/db"``.
    tables:
        Restrict extraction to these table names.  ``None`` means all tables.
    default_rows:
        Row count to assign each :class:`TableSpec` in the plan.
    sample:
        Whether to sample existing data for smarter strategy selection.
    schema:
        Database schema name (``None`` = default schema).
    """

    def __init__(
        self,
        connection_string: str,
        *,
        tables: list[str] | None = None,
        default_rows: int = 1000,
        sample: bool = True,
        schema: str | None = None,
    ) -> None:
        self.engine = create_engine(connection_string)
        self.tables = tables
        self.default_rows = default_rows
        self.sample = sample
        self.schema = schema

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self) -> DataGenPlan:
        """Inspect the database and return a complete :class:`DataGenPlan`."""
        insp = inspect(self.engine)
        table_names = self.tables or insp.get_table_names(schema=self.schema)

        # First pass: collect FK info across all tables so we can mark FK columns.
        all_fks: dict[str, dict[str, str]] = {}  # table -> {col: "ref_table.ref_col"}
        for tbl in table_names:
            all_fks[tbl] = _collect_foreign_keys(insp, tbl, self.schema)

        table_specs: list[TableSpec] = []
        for tbl in table_names:
            table_specs.append(self._extract_table(insp, tbl, all_fks.get(tbl, {})))

        return DataGenPlan(tables=table_specs)

    def to_yaml(self, output_path: str) -> None:
        """Extract and write the plan as a YAML file."""
        plan = self.extract()
        data = plan.model_dump(mode="json")
        Path(output_path).write_text(yaml.dump(data, sort_keys=False))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _extract_table(
        self,
        insp: Inspector,
        table_name: str,
        fk_map: dict[str, str],
    ) -> TableSpec:
        columns_info = insp.get_columns(table_name, schema=self.schema)
        pk_constraint = insp.get_pk_constraint(table_name, schema=self.schema)
        pk_cols = set(pk_constraint.get("constrained_columns", []))

        # Collect unique-constraint columns (single-column constraints only).
        unique_cols: set[str] = set(pk_cols)
        try:
            for uc in insp.get_unique_constraints(table_name, schema=self.schema):
                cols = uc.get("column_names", [])
                if len(cols) == 1:
                    unique_cols.add(cols[0])
        except (NotImplementedError, Exception):
            # Some dialects don't support get_unique_constraints.
            pass

        # Optional sampling
        samples: dict[str, list[Any]] = {}
        distinct_counts: dict[str, int] = {}
        if self.sample:
            samples, distinct_counts = self._sample_table(table_name)

        column_specs: list[ColumnSpec] = []
        for col_info in columns_info:
            col_name = col_info["name"]
            native_type = str(col_info["type"])
            synth_dtype = map_sql_type(native_type)

            is_pk = col_name in pk_cols
            nullable = col_info.get("nullable", True) and not is_pk

            inferred = InferredColumn(
                name=col_name,
                native_type=native_type,
                synth_dtype=synth_dtype,
                nullable=nullable,
                is_primary_key=is_pk,
                is_foreign_key=col_name in fk_map,
                fk_references=fk_map.get(col_name),
                unique=col_name in unique_cols,
                sample_values=samples.get(col_name, []),
                distinct_count=distinct_counts.get(col_name),
            )

            strategy = select_strategy(inferred, default_rows=self.default_rows)

            fk_ref = None
            if inferred.is_foreign_key and inferred.fk_references:
                fk_ref = ForeignKeyRef(ref=inferred.fk_references)

            column_specs.append(
                ColumnSpec(
                    name=col_name,
                    dtype=synth_dtype,
                    gen=strategy,
                    nullable=inferred.nullable,
                    foreign_key=fk_ref,
                )
            )

        pk = PrimaryKey(columns=sorted(pk_cols)) if pk_cols else None

        return TableSpec(
            name=table_name,
            columns=column_specs,
            rows=self.default_rows,
            primary_key=pk,
        )

    def _sample_table(self, table_name: str) -> tuple[dict[str, list[Any]], dict[str, int]]:
        """Return (samples_by_col, distinct_count_by_col) via SQLAlchemy reflection."""
        samples: dict[str, list[Any]] = {}
        distinct_counts: dict[str, int] = {}
        try:
            with self.engine.connect() as conn:
                # Use SQLAlchemy Table API for safe query construction
                try:
                    metadata = SAMetaData()
                    sa_table = SATable(
                        table_name,
                        metadata,
                        autoload_with=self.engine,
                        schema=self.schema,
                    )
                    stmt = sa_select(sa_table).limit(_SAMPLE_LIMIT)
                    rows = conn.execute(stmt).fetchall()
                except Exception:
                    # Fallback for edge cases where reflection fails
                    quoted = self.engine.dialect.identifier_preparer.quote_identifier(table_name)
                    rows = conn.execute(
                        text(f"SELECT * FROM {quoted} LIMIT :n"),
                        {"n": _SAMPLE_LIMIT},
                    ).fetchall()
                if not rows:
                    return samples, distinct_counts
                col_names = list(rows[0]._mapping.keys())
                for col_name in col_names:
                    vals = [row._mapping[col_name] for row in rows]
                    samples[col_name] = vals
                    distinct_counts[col_name] = len(set(vals))
        except Exception:
            pass
        return samples, distinct_counts


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _collect_foreign_keys(insp: Inspector, table_name: str, schema: str | None) -> dict[str, str]:
    """Return {local_col: "referred_table.referred_col"} for *table_name*."""
    fk_map: dict[str, str] = {}
    for fk in insp.get_foreign_keys(table_name, schema=schema):
        referred_table = fk["referred_table"]
        referred_schema = fk.get("referred_schema")
        if referred_schema and referred_schema != schema:
            referred_table = f"{referred_schema}.{referred_table}"
        for local_col, ref_col in zip(fk["constrained_columns"], fk["referred_columns"], strict=False):
            fk_map[local_col] = f"{referred_table}.{ref_col}"
    return fk_map


def extract_from_jdbc(
    connection_string: str,
    *,
    tables: list[str] | None = None,
    **kwargs,
) -> DataGenPlan:
    """Convenience function: create a :class:`JDBCConnector` and extract."""
    connector = JDBCConnector(connection_string, tables=tables, **kwargs)
    return connector.extract()
