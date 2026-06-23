"""Plan resolution: validates foreign-key references, sorts tables into
dependency order, and extracts the primary-key metadata each foreign-key column
needs.
"""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass
from typing import cast

from dbldatagen.core.engine.seed import derive_column_seed
from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataGenPlan,
    Distribution,
    ExpressionColumn,
    FakerColumn,
    PatternColumn,
    SequenceColumn,
    TableSpec,
    UUIDColumn,
    parse_fk_ref,
)


# SQL keywords, literals, and type names that appear as bare tokens in
# ExpressionColumn expressions. Function names are intentionally NOT listed: the
# "followed by ``(``" heuristic in _extract_column_references handles function
# calls generically, so this set does not drift as Spark adds builtins.
#
# Trade-off: entries like ``year`` are legal bare in ``interval`` literals but
# also collide with common column-name stems, so a user who types ``year``
# meaning ``year_val`` slips past the validator. Spark's UNRESOLVED_COLUMN at job
# time is the backstop.
_SQL_KEYWORDS: frozenset[str] = frozenset(
    {
        # Keywords / operators
        "and",
        "as",
        "between",
        "case",
        "distinct",
        "else",
        "end",
        "in",
        "interval",
        "is",
        "like",
        "not",
        "or",
        "rlike",
        "then",
        "unknown",
        "when",
        # Literals / special constants (callable without parens)
        "current_date",
        "current_timestamp",
        "current_user",
        "false",
        "null",
        "true",
        # Window-function / aggregation clause keywords, legal as bare tokens
        # inside e.g. ``rank() over (partition by a order by b)``. Same column-name
        # collision trade-off as the interval-unit and type-name entries above.
        "asc",
        "by",
        "current",
        "desc",
        "first",
        "following",
        "group",
        "having",
        "last",
        "nulls",
        "order",
        "over",
        "partition",
        "preceding",
        "range",
        "row",
        "rows",
        "unbounded",
        "where",
        # Interval units (legal as bare tokens inside ``interval`` literals)
        "day",
        "days",
        "hour",
        "hours",
        "microsecond",
        "microseconds",
        "millisecond",
        "milliseconds",
        "minute",
        "minutes",
        "month",
        "months",
        "nanosecond",
        "nanoseconds",
        "quarter",
        "quarters",
        "second",
        "seconds",
        "week",
        "weeks",
        "year",
        "years",
        # Type names
        "array",
        "bigint",
        "binary",
        "boolean",
        "byte",
        "date",
        "decimal",
        "double",
        "float",
        "int",
        "integer",
        "long",
        "map",
        "short",
        "smallint",
        "string",
        "struct",
        "timestamp",
        "tinyint",
    }
)


# Quoted segments whose contents must be stripped before identifier
# tokenization.  Each alternative matches one quoted form with its
# SQL-style doubled-quote escape (e.g. ``'it''s'``).
_QUOTED_SEGMENT = re.compile(
    r"'(?:[^']|'')*'"  # single-quoted string literal
    r'|"(?:[^"]|"")*"'  # double-quoted string literal (ANSI default off)
    r"|`(?:[^`]|``)*`"  # backtick-quoted identifier
)


@dataclass
class PKMetadata:
    """Primary-key metadata a foreign-key column needs to reconstruct parent keys.

    Produced during plan resolution and used by the engine to reconstruct a
    parent key value at a given row index, so the parent table is never
    generated twice.

    Attributes:
        table_name: Name of the parent table.
        pk_column: Name of the primary-key column.
        row_count: Number of rows the parent table produces; the index range
            foreign-key children sample from.
        pk_type: Primary-key kind: "sequence", "pattern", or "uuid".
        pk_seed: Column seed used to generate the primary key, so the child's
            reconstruction matches the parent.
        pk_start: Sequence start value (sequence keys only).
        pk_step: Sequence step value (sequence keys only).
        pk_template: Pattern template (pattern keys only); None otherwise.
    """

    table_name: str
    pk_column: str
    row_count: int
    pk_type: str  # "sequence", "pattern", "uuid"
    pk_seed: int  # The column seed used for PK generation
    pk_start: int  # For sequence PKs: start value
    pk_step: int  # For sequence PKs: step value
    pk_template: str | None  # For pattern PKs


@dataclass
class FKResolution:
    """Resolved foreign-key information for a single column.

    Created per foreign-key column at resolution time and read by the engine to
    sample parent rows and reconstruct the referenced key for each child row.

    Attributes:
        child_table: Name of the table that owns the foreign-key column.
        child_column: Name of the foreign-key column.
        parent_metadata: Primary-key metadata of the referenced parent.
        distribution: Sampling distribution over the parent row range (None
            means uniform).
        null_fraction: Probability in [0.0, 1.0] that a child row is NULL
            instead of resolving the key.
    """

    child_table: str
    child_column: str
    parent_metadata: PKMetadata
    distribution: Distribution | None
    null_fraction: float


@dataclass
class ResolvedPlan:
    """A fully resolved plan with foreign-key metadata and generation order.

    Produced by `resolve_plan(plan)`. Pass it to `generate` or `generate_table`
    to skip re-resolution when generating the same plan multiple times.

    Attributes:
        generation_order: Table names ordered so each parent is built before its
            children.
        fk_resolutions: Map of `(table, column)` to its `FKResolution`.
        plan: `DataGenPlan` used to build this resolved plan. `generate`
            identity-checks it so a resolved plan can't be used against a
            different plan.
    """

    generation_order: list[str]
    fk_resolutions: dict[tuple[str, str], FKResolution]
    plan: DataGenPlan


def resolve_plan(plan: DataGenPlan) -> ResolvedPlan:
    """Resolves a `DataGenPlan` into a generation-ready plan.

    Validates foreign-key references, sorts the tables so each parent comes
    before its children, and extracts the primary-key metadata each foreign-key
    column needs. The result can be reused across repeated generations of the
    same plan so resolution is paid only once.

    Args:
        plan: The plan to resolve. Foreign-key references must use the
            `"table.column"` form and point at a column in the referenced
            table's primary key.

    Returns:
        A `ResolvedPlan` with the generation order, per-column `FKResolution`
        records, and a back-pointer to `plan`.

    Raises:
        ValueError: If a foreign-key reference is malformed, points at a missing
            or non-primary-key column, or the foreign-key graph contains a cycle.
            Also propagated from the expression, seed_from, and primary-key
            validators.
    """
    table_map: dict[str, TableSpec] = {table.name: table for table in plan.tables}
    all_table_names = list(table_map.keys())

    # Per-table column lookups for O(1) access, avoiding an O(N) scan per FK.
    table_col_maps: dict[str, dict[str, ColumnSpec]] = {
        table.name: {col.name: col for col in table.columns} for table in plan.tables
    }

    # Cross-column validators run before the FK loop so _extract_pk_metadata is
    # only reached for PK columns already verified to use a supported strategy;
    # otherwise a bad-PK plan would hit that backstop instead of the friendly
    # _validate_primary_keys error.
    _validate_expression_columns(plan)
    _validate_seed_from(plan)
    _validate_primary_keys(plan)

    fk_resolutions: dict[tuple[str, str], FKResolution] = {}

    for table_spec in plan.tables:
        for col_spec in table_spec.columns:
            if col_spec.foreign_key is None:
                continue

            ref = col_spec.foreign_key.ref
            # ForeignKeyRef validation already ran parse_fk_ref at construction
            # time, so a malformed ref can't reach here; re-call to get the parts.
            parent_table_name, parent_col_name = parse_fk_ref(ref)

            if parent_table_name not in table_map:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"table '{parent_table_name}' does not exist"
                )

            parent_table = table_map[parent_table_name]

            parent_col = table_col_maps[parent_table_name].get(parent_col_name)
            if parent_col is None:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"column '{parent_col_name}' does not exist in table '{parent_table_name}'"
                )

            if parent_table.primary_key is None:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"table '{parent_table_name}' has no primary key defined"
                )
            # A single-column ForeignKeyRef can't deterministically resolve to one
            # row of a composite PK (tuple uniqueness, not sub-column uniqueness),
            # so reject it up front rather than emit join-ambiguous FKs.
            if len(parent_table.primary_key.columns) > 1:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"table '{parent_table_name}' has a composite primary key "
                    f"({parent_table.primary_key.columns}); single-column "
                    f"``ForeignKeyRef`` cannot deterministically resolve to one "
                    f"parent row.  Use a single-column primary key on "
                    f"'{parent_table_name}', or split the FK relationship into "
                    f"a derived single-column key."
                )
            if parent_col_name not in parent_table.primary_key.columns:
                raise ValueError(
                    f"FK reference '{ref}' in {table_spec.name}.{col_spec.name}: "
                    f"column '{parent_col_name}' is not a primary key of '{parent_table_name}'"
                )

            parent_metadata = _extract_pk_metadata(parent_table, parent_col)

            # Both null_fraction sources are legal; the ColumnSpec validator
            # rejects disagreeing non-zero values, so max() here is unambiguous.
            null_fraction = max(col_spec.null_fraction, col_spec.foreign_key.null_fraction)
            distribution = col_spec.foreign_key.distribution

            fk_resolutions[(table_spec.name, col_spec.name)] = FKResolution(
                child_table=table_spec.name,
                child_column=col_spec.name,
                parent_metadata=parent_metadata,
                distribution=distribution,
                null_fraction=null_fraction,
            )

    dep_graph = _build_dependency_graph(plan)
    generation_order = _topological_sort(dep_graph, all_table_names)

    return ResolvedPlan(
        generation_order=generation_order,
        fk_resolutions=fk_resolutions,
        plan=plan,
    )


def _build_dependency_graph(plan: DataGenPlan) -> dict[str, set[str]]:
    """Builds a {table: {parent tables}} dependency graph from foreign keys.

    Every table maps to the set of tables it references through foreign-key
    columns (empty if none).

    Args:
        plan: The plan to inspect.

    Returns:
        A dict mapping each table name to the set of parent table names.
    """
    return {
        table_spec.name: {
            parse_fk_ref(col_spec.foreign_key.ref)[0]
            for col_spec in table_spec.columns
            if col_spec.foreign_key is not None
        }
        for table_spec in plan.tables
    }


def _topological_sort(graph: dict[str, set[str]], all_tables: list[str]) -> list[str]:
    """Orders tables so each parent comes before its children using Kahn's algorithm.

    Args:
        graph: A dependency graph as a dictionary of the form
            {table: {parent tables}}.
        all_tables: All table names to order.

    Returns:
        Table names in dependency order.

    Raises:
        ValueError: If the graph contains a cycle.
    """
    # Parent -> children adjacency, derived from the child -> parents graph.
    adj: dict[str, list[str]] = {
        parent: [child for child, parents in graph.items() if parent in parents] for parent in all_tables
    }

    # In-degree = number of parents each table depends on.
    in_degree: dict[str, int] = {table: len(graph.get(table, set())) for table in all_tables}

    # Seed the queue with every table that has no parents.
    queue: deque[str] = deque(t for t in all_tables if in_degree[t] == 0)
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


def _extract_pk_metadata(table_spec: TableSpec, pk_col_spec: ColumnSpec) -> PKMetadata:
    """Extracts the primary-key metadata a foreign key needs to reconstruct keys.

    Args:
        table_spec: The parent table; its `seed` must be set.
        pk_col_spec: The primary-key column spec.

    Returns:
        The `PKMetadata` for the primary key.

    Raises:
        ValueError: If `table_spec.seed` is None, or the primary key uses an
            unsupported strategy (the plan-time validator should have rejected
            it).
    """
    if table_spec.seed is None:
        raise ValueError(
            f"TableSpec '{table_spec.name}'.seed is None.  Either set "
            f"it explicitly on the TableSpec or go through a "
            f"DataGenPlan (which propagates plan.seed to each table "
            f"during Pydantic validation)."
        )

    table_name = table_spec.name
    pk_column = pk_col_spec.name
    # TableSpec validation normalises ``rows`` (int | str) to int, so this is a
    # pure type assertion with no runtime conversion.
    row_count = cast(int, table_spec.rows)
    pk_seed = derive_column_seed(table_spec.seed, table_spec.name, pk_col_spec.name)

    # _validate_primary_keys rejects any other strategy at plan time; the case _
    # branch is a defensive backstop that surfaces a clear error instead of
    # silently building synthetic-sequence metadata that would corrupt FK children.
    match pk_col_spec.gen:
        case SequenceColumn(start=start, step=step):
            return PKMetadata(
                table_name=table_name,
                pk_column=pk_column,
                row_count=row_count,
                pk_seed=pk_seed,
                pk_type="sequence",
                pk_start=start,
                pk_step=step,
                pk_template=None,
            )
        case PatternColumn(template=template):
            return PKMetadata(
                table_name=table_name,
                pk_column=pk_column,
                row_count=row_count,
                pk_seed=pk_seed,
                pk_type="pattern",
                pk_start=0,
                pk_step=1,
                pk_template=template,
            )
        case UUIDColumn():
            return PKMetadata(
                table_name=table_name,
                pk_column=pk_column,
                row_count=row_count,
                pk_seed=pk_seed,
                pk_type="uuid",
                pk_start=0,
                pk_step=1,
                pk_template=None,
            )
        case _:
            raise ValueError(
                f"_extract_pk_metadata received PK column '{table_spec.name}."
                f"{pk_col_spec.name}' with unsupported strategy "
                f"{type(pk_col_spec.gen).__name__}.  ``_validate_primary_keys`` "
                f"should have rejected this at plan time -- a validator-ordering "
                f"regression has bypassed the check."
            )


def _extract_column_references(expr: str) -> set[str]:
    """Returns the identifiers in an expression that look like column references.

    Strips quoted string and identifier literals, then ignores function calls (a
    name followed by `(`), qualified field access (a name preceded by `.`), and
    SQL keywords, literals, and type names.

    Args:
        expr: A Spark SQL expression string.

    Returns:
        The set of likely column-reference identifiers.
    """
    cleaned = _QUOTED_SEGMENT.sub("", expr)
    return {
        m.group(1)
        for m in re.finditer(r"\b([a-zA-Z_]\w*)\b", cleaned)
        if not cleaned[m.end() :].lstrip().startswith("(")  # not a function call
        and not cleaned[: m.start()].rstrip().endswith(".")  # not a qualified field access
        and m.group(1).lower() not in _SQL_KEYWORDS
    }


def _validate_expression_columns(plan: DataGenPlan) -> None:
    """Validates that every `ExpressionColumn` references a usable column.

    Each referenced name must be a column on the same table and must be available
    when the expression evaluates. Expressions run before foreign-key, Faker, and
    `seed_from` columns are added, so referencing one of those is rejected.

    Args:
        plan: The plan to validate.

    Raises:
        ValueError: If an expression references an unknown column, or one added
            in a later phase (foreign-key, Faker, or seed_from).
    """
    for table_spec in plan.tables:
        col_by_name = {c.name: c for c in table_spec.columns}
        col_names = set(col_by_name)
        # Columns not visible in phase 1: foreign_key or Faker (phase 2), or
        # seed_from (phase 3).
        non_phase1 = {
            c.name
            for c in table_spec.columns
            if c.foreign_key is not None or isinstance(c.gen, FakerColumn) or c.seed_from is not None
        }
        for col_spec in table_spec.columns:
            if not isinstance(col_spec.gen, ExpressionColumn):
                continue
            refs = _extract_column_references(col_spec.gen.expr)
            unknown = refs - col_names
            if unknown:
                raise ValueError(
                    f"ExpressionColumn '{col_spec.name}' in table "
                    f"'{table_spec.name}' references {sorted(unknown)} "
                    f"which are not columns in this table. "
                    f"Available columns: {sorted(col_names)}"
                )
            unreachable = refs & non_phase1
            if unreachable:
                raise ValueError(
                    f"ExpressionColumn '{col_spec.name}' in table "
                    f"'{table_spec.name}' references {sorted(unreachable)}, "
                    f"which are FK / Faker / seed_from columns applied in a "
                    f"later phase.  ExpressionColumn evaluates in the first "
                    f"projection pass, before those columns are added to the "
                    f"DataFrame -- the reference would fail at Spark plan "
                    f"time with UNRESOLVED_COLUMN.  Either reference a "
                    f"regular column instead, or compute the expression at "
                    f"the source (e.g. inline the FK derivation)."
                )


def _validate_seed_from(plan: DataGenPlan) -> None:
    """Validates `seed_from` references.

    Checks that the referenced column exists, that a column does not reference
    itself, and that it does not reference another `seed_from` column (chains are
    not supported).

    Args:
        plan: The plan to validate.

    Raises:
        ValueError: If a `seed_from` reference is missing, self-referential, or
            chained.
    """
    for table_spec in plan.tables:
        col_names = {c.name for c in table_spec.columns}
        col_by_name = {c.name: c for c in table_spec.columns}
        for col_spec in table_spec.columns:
            if not col_spec.seed_from:
                continue
            if col_spec.seed_from not in col_names:
                raise ValueError(
                    f"Column '{col_spec.name}' in table '{table_spec.name}' "
                    f"has seed_from='{col_spec.seed_from}' but that column "
                    f"does not exist. Available: {sorted(col_names)}"
                )
            if col_spec.seed_from == col_spec.name:
                raise ValueError(
                    f"Column '{col_spec.name}' in table '{table_spec.name}' "
                    f"has seed_from='{col_spec.seed_from}' referencing itself.  "
                    f"seed_from must point at a different column."
                )
            target = col_by_name[col_spec.seed_from]
            if target.seed_from is not None:
                raise ValueError(
                    f"Column '{col_spec.name}' in table '{table_spec.name}' "
                    f"has seed_from='{col_spec.seed_from}', but '{col_spec.seed_from}' "
                    f"itself has seed_from='{target.seed_from}'.  seed_from chains "
                    f"are rejected because phase-3 columns are applied in "
                    f"declaration order at generation time, not in dependency-topo "
                    f"order -- chained references break at Spark plan time.  "
                    f"Point '{col_spec.name}' directly at '{target.seed_from}' "
                    f"(or another non-derived column) instead."
                )


def _validate_primary_keys(plan: DataGenPlan) -> None:
    """Validates that primary-key columns exist and use a supported strategy.

    Each named primary-key column must exist, and its strategy must be one of
    `SequenceColumn`, `PatternColumn`, or `UUIDColumn`. Other strategies can't be
    reconstructed deterministically, so a foreign key pointing at them would
    produce values that don't match the parent.

    Args:
        plan: The plan to validate.

    Raises:
        ValueError: If a primary-key column is missing or uses an unsupported
            strategy.
    """
    for table_spec in plan.tables:
        if table_spec.primary_key is None:
            continue
        col_by_name = {c.name: c for c in table_spec.columns}
        for pk_col in table_spec.primary_key.columns:
            if pk_col not in col_by_name:
                raise ValueError(
                    f"Primary key column '{pk_col}' not found in table "
                    f"'{table_spec.name}'. Available: {sorted(col_by_name)}"
                )
            pk_spec = col_by_name[pk_col]
            if not isinstance(pk_spec.gen, (SequenceColumn, PatternColumn, UUIDColumn)):
                raise ValueError(
                    f"Primary key column '{table_spec.name}.{pk_col}' uses "
                    f"strategy {type(pk_spec.gen).__name__}, which is not a "
                    f"supported PK strategy.  Use ``SequenceColumn``, "
                    f"``PatternColumn``, or ``UUIDColumn`` (or the DSL helpers "
                    f"``pk_auto`` / ``pk_pattern`` / ``pk_uuid``).  Non-PK "
                    f"strategies were silently treated as sequence at FK "
                    f"reconstruction time, which produced FK values that did "
                    f"not match the actual PK values."
                )
