"""Build a dbldatagen.v1 DataGenPlan from an InferredSchema."""

from __future__ import annotations

from dbldatagen.v1.connectors.sql.inference import InferredColumn, InferredSchema
from dbldatagen.v1.connectors.sql.name_mapper import map_column_name
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    SequenceColumn,
    TableSpec,
    Zipf,
)


DEFAULT_BASE_ROWS = 1000
CHILD_MULTIPLIER = 5


# ---------------------------------------------------------------------------
# Row count helpers
# ---------------------------------------------------------------------------


def _resolve_row_count(val: int | str) -> int:
    """Resolve a row count that may be a string shorthand like '10K'."""
    if isinstance(val, int):
        return val
    s = str(val).strip().upper()
    suffixes = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    for suffix, mult in suffixes.items():
        if s.endswith(suffix):
            return int(float(s[: -len(suffix)]) * mult)
    return int(s)


def _topo_sort(
    fk_edges: list[tuple[str, str, str, str]],
    table_names: list[str],
) -> list[str]:
    """Topological sort: parents before children."""
    from collections import defaultdict, deque

    # Build adjacency: parent -> children
    children_of: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = dict.fromkeys(table_names, 0)
    for child_t, _, parent_t, _ in fk_edges:
        if child_t == parent_t:
            continue  # skip self-references
        children_of[parent_t].append(child_t)
        in_degree[child_t] = in_degree.get(child_t, 0) + 1

    queue = deque(t for t in table_names if in_degree[t] == 0)
    order: list[str] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for child in children_of[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Append any remaining tables (cycles or disconnected)
    for t in table_names:
        if t not in order:
            order.append(t)

    return order


def _compute_row_counts(
    schema: InferredSchema,
    user_overrides: dict[str, int | str] | None,
) -> dict[str, int]:
    """Compute row counts per table with dependency-aware heuristics."""
    counts: dict[str, int] = {}

    # 1. Apply user overrides
    if user_overrides:
        for table, count in user_overrides.items():
            counts[table] = _resolve_row_count(count)

    # 2. Get parent relationships
    parent_map: dict[str, set[str]] = {}
    for child_t, _, parent_t, _ in schema.fk_edges:
        if child_t == parent_t:
            continue
        parent_map.setdefault(child_t, set()).add(parent_t)

    # 3. Topological order
    order = _topo_sort(schema.fk_edges, list(schema.tables.keys()))

    for table in order:
        if table in counts:
            continue
        parents = parent_map.get(table, set())
        if not parents:
            counts[table] = DEFAULT_BASE_ROWS
        else:
            min_parent = min(counts.get(p, DEFAULT_BASE_ROWS) for p in parents)
            counts[table] = min_parent * CHILD_MULTIPLIER
    return counts


# ---------------------------------------------------------------------------
# Column spec building
# ---------------------------------------------------------------------------


def _build_column_spec(col: InferredColumn) -> ColumnSpec:
    """Convert an InferredColumn to a dbldatagen.v1 ColumnSpec."""
    # PK: always a sequence
    if col.is_pk:
        return ColumnSpec(
            name=col.name,
            dtype=DataType.LONG,
            gen=SequenceColumn(),
        )

    # FK: constant placeholder + ForeignKeyRef
    if col.fk_ref:
        return ColumnSpec(
            name=col.name,
            gen=ConstantColumn(value=None),
            foreign_key=ForeignKeyRef(
                ref=col.fk_ref,
                distribution=Zipf(exponent=1.2),
                nullable=col.nullable,
            ),
        )

    # Regular column: use name_mapper for smart defaults
    spec = map_column_name(col.name, col.inferred_type)

    # Override dtype if inference gave high-confidence non-string type
    if col.type_confidence >= 0.7 and col.inferred_type != DataType.STRING:
        spec.dtype = col.inferred_type

    return spec


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_plan(
    schema: InferredSchema,
    *,
    row_counts: dict[str, int | str] | None = None,
    seed: int = 42,
) -> DataGenPlan:
    """Build a ``DataGenPlan`` from a fully inferred schema.

    Parameters
    ----------
    schema : InferredSchema
        Output from ``infer_schema()``.
    row_counts : dict | None
        Optional per-table row count overrides.
    seed : int
        Global seed.

    Returns
    -------
    DataGenPlan
    """
    counts = _compute_row_counts(schema, row_counts)

    table_specs: list[TableSpec] = []
    # Process in topological order (parents first)
    order = _topo_sort(schema.fk_edges, list(schema.tables.keys()))

    for table_name in order:
        columns = schema.tables.get(table_name, [])
        pk_col = schema.pk_columns.get(table_name)

        col_specs = [_build_column_spec(c) for c in columns]

        table_spec = TableSpec(
            name=table_name,
            columns=col_specs,
            rows=counts.get(table_name, DEFAULT_BASE_ROWS),
            primary_key=PrimaryKey(columns=[pk_col]) if pk_col else None,
        )
        table_specs.append(table_spec)

    return DataGenPlan(tables=table_specs, seed=seed)
