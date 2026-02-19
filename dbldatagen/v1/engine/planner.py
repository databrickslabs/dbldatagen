"""Plan resolution: FK reference validation, topological sort, metadata propagation."""

from __future__ import annotations

import re
import warnings
from collections import deque
from dataclasses import dataclass

from dbldatagen.v1.engine.seed import derive_column_seed
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    Distribution,
    ExpressionColumn,
    PatternColumn,
    SequenceColumn,
    TableSpec,
    UUIDColumn,
)


@dataclass
class PKMetadata:
    """Metadata about a parent table's primary key needed for FK generation."""

    table_name: str
    pk_column: str
    row_count: int
    pk_type: str  # "sequence", "pattern", "uuid"
    pk_seed: int  # The column seed used for PK generation
    pk_start: int  # For sequence PKs: start value
    pk_step: int  # For sequence PKs: step value
    pk_template: str | None  # For pattern PKs
    feistel_N: int | None  # For feistel PKs: domain size


@dataclass
class FKResolution:
    """Resolved FK info for a single FK column."""

    child_table: str
    child_column: str
    parent_meta: PKMetadata
    distribution: Distribution | None
    null_fraction: float


@dataclass
class ResolvedPlan:
    """Fully resolved plan with FK metadata and generation order."""

    generation_order: list[str]  # Table names in dependency order
    fk_resolutions: dict[tuple[str, str], FKResolution]  # (table, column) -> resolution
    plan: DataGenPlan  # Original DataGenPlan


def resolve_plan(plan: DataGenPlan) -> ResolvedPlan:
    """Resolve a DataGenPlan.

    1. Validate all FK references point to existing tables/columns
    2. Validate referenced columns are actually PKs
    3. Build dependency graph
    4. Topological sort (detect cycles)
    5. Extract PK metadata for each referenced parent
    """
    table_map: dict[str, TableSpec] = {t.name: t for t in plan.tables}
    all_table_names = list(table_map.keys())

    # Collect all FK references and validate them
    fk_resolutions: dict[tuple[str, str], FKResolution] = {}

    for table_spec in plan.tables:
        for col_spec in table_spec.columns:
            if col_spec.foreign_key is None:
                continue

            ref = col_spec.foreign_key.ref
            if "." not in ref:
                raise ValueError(
                    f"Invalid FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"expected 'table.column' format"
                )

            parent_table_name, parent_col_name = ref.split(".", 1)

            # Validate parent table exists
            if parent_table_name not in table_map:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"table '{parent_table_name}' does not exist"
                )

            parent_table = table_map[parent_table_name]

            # Validate parent column exists
            parent_col = None
            for pc in parent_table.columns:
                if pc.name == parent_col_name:
                    parent_col = pc
                    break
            if parent_col is None:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"column '{parent_col_name}' does not exist in table '{parent_table_name}'"
                )

            # Validate referenced column is a PK
            if parent_table.primary_key is None:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"table '{parent_table_name}' has no primary key defined"
                )
            if parent_col_name not in parent_table.primary_key.columns:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"column '{parent_col_name}' is not a primary key of '{parent_table_name}'"
                )

            # Extract PK metadata
            parent_meta = _extract_pk_metadata(parent_table, parent_col, plan.seed)

            null_fraction = col_spec.foreign_key.null_fraction
            distribution = col_spec.foreign_key.distribution

            fk_resolutions[(table_spec.name, col_spec.name)] = FKResolution(
                child_table=table_spec.name,
                child_column=col_spec.name,
                parent_meta=parent_meta,
                distribution=distribution,
                null_fraction=null_fraction,
            )

    # Warn about potentially invalid ExpressionColumn references
    _validate_expression_columns(plan)

    # Build dependency graph and topological sort
    dep_graph = _build_dependency_graph(plan)
    generation_order = _topological_sort(dep_graph, all_table_names)

    return ResolvedPlan(
        generation_order=generation_order,
        fk_resolutions=fk_resolutions,
        plan=plan,
    )


def _build_dependency_graph(plan: DataGenPlan) -> dict[str, set[str]]:
    """Build {child_table: {parent_table, ...}} from FK refs."""
    graph: dict[str, set[str]] = {t.name: set() for t in plan.tables}
    for table_spec in plan.tables:
        for col_spec in table_spec.columns:
            if col_spec.foreign_key is not None:
                parent_table_name = col_spec.foreign_key.ref.split(".", 1)[0]
                graph[table_spec.name].add(parent_table_name)
    return graph


def _topological_sort(graph: dict[str, set[str]], all_tables: list[str]) -> list[str]:
    """Kahn's algorithm. Raise ValueError on cycles."""
    # Compute in-degree
    in_degree: dict[str, int] = dict.fromkeys(all_tables, 0)
    # Build adjacency: parent -> children
    adj: dict[str, list[str]] = {t: [] for t in all_tables}
    for child, parents in graph.items():
        for parent in parents:
            adj[parent].append(child)
            in_degree[child] += 1

    queue: deque[str] = deque()
    for t in all_tables:
        if in_degree[t] == 0:
            queue.append(t)

    result: list[str] = []
    while queue:
        node = queue.popleft()
        result.append(node)
        for child in adj[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(result) != len(all_tables):
        # Find tables involved in cycles
        remaining = set(all_tables) - set(result)
        raise ValueError(f"Circular FK dependency detected among tables: {remaining}")

    return result


def _extract_pk_metadata(table_spec: TableSpec, pk_col_spec: ColumnSpec, plan_seed: int) -> PKMetadata:
    """Extract PK generation metadata from a TableSpec."""
    global_seed = table_spec.seed if table_spec.seed is not None else plan_seed
    column_seed = derive_column_seed(global_seed, table_spec.name, pk_col_spec.name)
    row_count = int(table_spec.rows)

    gen = pk_col_spec.gen

    if isinstance(gen, SequenceColumn):
        return PKMetadata(
            table_name=table_spec.name,
            pk_column=pk_col_spec.name,
            row_count=row_count,
            pk_type="sequence",
            pk_seed=column_seed,
            pk_start=gen.start,
            pk_step=gen.step,
            pk_template=None,
            feistel_N=None,
        )

    if isinstance(gen, PatternColumn):
        return PKMetadata(
            table_name=table_spec.name,
            pk_column=pk_col_spec.name,
            row_count=row_count,
            pk_type="pattern",
            pk_seed=column_seed,
            pk_start=0,
            pk_step=1,
            pk_template=gen.template,
            feistel_N=None,
        )

    if isinstance(gen, UUIDColumn):
        return PKMetadata(
            table_name=table_spec.name,
            pk_column=pk_col_spec.name,
            row_count=row_count,
            pk_type="uuid",
            pk_seed=column_seed,
            pk_start=0,
            pk_step=1,
            pk_template=None,
            feistel_N=None,
        )

    # Fallback: treat as sequence
    return PKMetadata(
        table_name=table_spec.name,
        pk_column=pk_col_spec.name,
        row_count=row_count,
        pk_type="sequence",
        pk_seed=column_seed,
        pk_start=0,
        pk_step=1,
        pk_template=None,
        feistel_N=None,
    )


def _validate_expression_columns(plan: DataGenPlan) -> None:
    """Warn if ExpressionColumn references look like undefined column names."""
    for table_spec in plan.tables:
        col_names = {c.name for c in table_spec.columns}
        for col_spec in table_spec.columns:
            if not isinstance(col_spec.gen, ExpressionColumn):
                continue
            # Extract bare identifiers from the expression (heuristic)
            tokens = set(re.findall(r"\b([a-zA-Z_]\w*)\b", col_spec.gen.expr))
            # Exclude common SQL functions/keywords
            sql_builtins = {
                "concat",
                "coalesce",
                "cast",
                "when",
                "otherwise",
                "lit",
                "upper",
                "lower",
                "trim",
                "length",
                "substring",
                "replace",
                "round",
                "floor",
                "ceil",
                "abs",
                "current_timestamp",
                "current_date",
                "date_format",
                "to_date",
                "to_timestamp",
                "null",
                "true",
                "false",
                "and",
                "or",
                "not",
                "is",
                "in",
                "between",
                "like",
                "case",
                "then",
                "else",
                "end",
                "as",
                "int",
                "long",
                "string",
                "double",
                "float",
                "boolean",
                "date",
                "timestamp",
                "array",
                "struct",
                "map",
            }
            candidates = tokens - sql_builtins - col_names
            for candidate in candidates:
                # Only warn for tokens that look like column names (not numbers)
                if not candidate[0].isdigit():
                    warnings.warn(
                        f"ExpressionColumn '{col_spec.name}' in table "
                        f"'{table_spec.name}' references '{candidate}' "
                        f"which is not a column in this table. "
                        f"Available columns: {sorted(col_names)}",
                        stacklevel=3,
                    )
