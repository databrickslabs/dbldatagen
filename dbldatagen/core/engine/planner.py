"""Plan resolution: FK reference validation, topological sort, metadata propagation."""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass

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
)


# SQL keywords, literals, and type names that appear as bare tokens in
# ExpressionColumn expressions.  Function names are intentionally NOT
# listed: the "followed by ``(``" heuristic in
# ``_extract_column_references`` handles function calls generically so
# this set does not drift as Spark adds builtins.
#
# Trade-off: entries like ``year``/``month``/``day`` are legal bare in
# ``interval`` literals but also collide with common column-name stems.
# If a user types ``year`` meaning ``year_val`` the typo slips past the
# validator — we accept that to avoid false-positives on every interval
# expression.  Spark's UNRESOLVED_COLUMN at job time is the backstop.
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
        # Window function / aggregation clause keywords
        # (legal as bare tokens inside ``rank() over (partition by a order by b)``,
        # ``sum(a) over (rows between unbounded preceding and current row)``,
        # ``filter(x) where y > 0``, ``order by x nulls first``, etc.).
        # Same trade-off as the interval-unit and type-name entries
        # above: a user who picks ``order`` / ``group`` / ``row`` / ``range`` /
        # ``where`` / ``first`` / ``last`` as a bare column name no longer gets
        # plan-time typo detection on the un-suffixed form.  Spark's
        # ``UNRESOLVED_COLUMN`` is still the backstop at job time.
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
    """Metadata about a parent table's primary key needed for FK generation.

    Produced by ``_extract_pk_metadata`` during plan resolution and
    consumed by ``FKResolution.parent_meta`` and the engine's
    ``_reconstruct_parent_pk``: the child FK column reconstructs the
    parent PK value at row index ``i`` from these fields alone, so
    the parent table never has to be materialised twice.

    Attributes:
        table_name: Name of the parent table the PK belongs to.
        pk_column: Name of the PK column on that table.
        row_count: Number of rows the parent table will produce.
          Used as the index range from which FK children sample.
        pk_type: PK generation kind.  One of ``"sequence"``,
          ``"pattern"``, or ``"uuid"``.
        pk_seed: Column seed used when generating the PK column.
          Threaded back into the child's reconstruction so output
          matches the parent byte-for-byte.
        pk_start: For ``pk_type == "sequence"``: the sequence start
          value.  Ignored for other PK types.
        pk_step: For ``pk_type == "sequence"``: the sequence step
          value.  Ignored for other PK types.
        pk_template: For ``pk_type == "pattern"``: the
          ``PatternColumn.template`` string used to format the PK.
          ``None`` for non-pattern PKs.
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
    """Resolved FK info for a single FK column.

    One ``FKResolution`` is created per FK column at ``resolve_plan``
    time and stored in ``ResolvedPlan.fk_resolutions`` keyed by
    ``(child_table, child_column)``.  The engine reads it at
    materialisation to drive the parent-row sampling and reconstruct
    the parent PK value for each child row.

    Attributes:
        child_table: Name of the table that owns the FK column.
        child_column: Name of the FK column on that table.
        parent_meta: ``PKMetadata`` of the referenced parent
          ``(table.column)``.  Carries everything needed to
          reconstruct the parent's PK values without re-materialising
          the parent table.
        distribution: Sampling distribution over the parent row index
          range.  Defaults to the ``ForeignKeyRef.distribution`` set
          on the user-facing spec; ``None`` falls back to ``Uniform``
          at materialisation.
        null_fraction: Probability in ``[0.0, 1.0]`` that a given
          child row emits ``NULL`` instead of resolving the FK.  The
          higher of ``ColumnSpec.null_fraction`` and
          ``ForeignKeyRef.null_fraction`` (validated to agree when
          both non-zero).
    """

    child_table: str
    child_column: str
    parent_meta: PKMetadata
    distribution: Distribution | None
    null_fraction: float


@dataclass
class ResolvedPlan:
    """Fully resolved plan with FK metadata and generation order.

    Produced by ``resolve_plan(plan)``.  Pass into ``generate(spark, plan,
    resolved_plan=...)`` or ``generate_table(spark, table_spec, resolved_plan)``
    to skip re-resolution when generating the same plan multiple times
    (e.g. across seeds, batches, or partitions).

    Attributes:
        generation_order: Table names sorted so each parent is built
            before any of its children.
        fk_resolutions: Per FK column ``(table_name, column_name)`` ->
            ``FKResolution`` describing the parent PK metadata,
            sampling distribution, and null fraction.
        plan: The original ``DataGenPlan`` this resolution was built
            from.  ``generate()`` checks identity here so the resolved
            plan cannot be silently used against a different plan
            object.
    """

    generation_order: list[str]
    fk_resolutions: dict[tuple[str, str], FKResolution]
    plan: DataGenPlan


def resolve_plan(plan: DataGenPlan) -> ResolvedPlan:
    """Resolves a ``DataGenPlan`` into a generation-ready plan.

    Validates every foreign-key reference, builds the table-dependency
    graph, topologically sorts it (so each parent is generated before
    its children), and extracts the PK metadata each FK child needs
    at materialisation.  The result is safe to thread through
    ``generate()`` / ``generate_table()`` so resolution is paid once
    even when the same plan is generated many times (e.g. across
    seeds, partitions, or batches).

    Args:
        plan: The ``DataGenPlan`` to resolve.  All FK ``ref`` values
          must use the ``"table.column"`` form and point at a column
          that is part of the referenced table's ``primary_key``.

    Returns:
        A ``ResolvedPlan`` carrying ``generation_order`` (parents
        before children), per-FK ``FKResolution`` records, and a
        back-pointer to the original ``plan`` so callers downstream
        can identity-check the pairing.

    Raises:
        ValueError: an FK ``ref`` is malformed, points at a missing
          table or column, points at a non-PK column, or the FK graph
          contains a cycle.  Also raised by upstream
          ``expression_columns`` / ``seed_from`` / ``primary_keys``
          validators when their invariants fail.
    """
    table_map: dict[str, TableSpec] = {t.name: t for t in plan.tables}
    all_table_names = list(table_map.keys())

    # Pre-build column lookup dicts for O(1) access (avoids O(N) scan per FK)
    table_col_maps: dict[str, dict[str, ColumnSpec]] = {t.name: {c.name: c for c in t.columns} for t in plan.tables}

    # Cross-column reference validators run BEFORE the FK loop so that
    # _extract_pk_metadata (called from the loop) is only reached for
    # PK columns that have already been verified to use a supported
    # strategy.  A bad-PK plan with a child FK would otherwise hit the
    # ``_extract_pk_metadata`` RuntimeError backstop instead of the
    # friendly ``_validate_primary_keys`` ValueError.
    _validate_expression_columns(plan)
    _validate_seed_from(plan)
    _validate_primary_keys(plan)

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

            # Validate parent column exists (O(1) lookup via pre-built dict)
            parent_col = table_col_maps[parent_table_name].get(parent_col_name)
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
            # ``ForeignKeyRef.ref`` is a single ``"table.column"``;
            # ``_extract_pk_metadata`` and ``PKMetadata`` are
            # single-column by construction.  Pointing at one
            # sub-column of a composite parent PK can match multiple
            # parent rows once the user joins back (the composite PK
            # guarantees tuple uniqueness, not sub-column
            # uniqueness), which breaks the "every FK matches
            # exactly one parent" invariant the engine relies on.
            # Reject up front rather than silently emit
            # join-ambiguous FKs.
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

            # Extract PK metadata
            parent_meta = _extract_pk_metadata(parent_table, parent_col)

            # ColumnSpec.null_fraction and ForeignKeyRef.null_fraction are
            # both legal sources (the ColumnSpec validator rejects
            # disagreeing non-zero values so max() here is unambiguous).
            null_fraction = max(col_spec.null_fraction, col_spec.foreign_key.null_fraction)
            distribution = col_spec.foreign_key.distribution

            fk_resolutions[(table_spec.name, col_spec.name)] = FKResolution(
                child_table=table_spec.name,
                child_column=col_spec.name,
                parent_meta=parent_meta,
                distribution=distribution,
                null_fraction=null_fraction,
            )

    # Build dependency graph and topological sort
    dep_graph = _build_dependency_graph(plan)
    generation_order = _topological_sort(dep_graph, all_table_names)

    return ResolvedPlan(
        generation_order=generation_order,
        fk_resolutions=fk_resolutions,
        plan=plan,
    )


def _build_dependency_graph(plan: DataGenPlan) -> dict[str, set[str]]:
    """Build {child_table: {parent_table, ...}} from FK refs.

    Pure shape transform: every table gets a key, mapping to the set
    of parent tables it references through FK columns (empty set if
    none).  Safe to call ``.ref.split(".", 1)[0]`` here because the
    FK validation loop in ``resolve_plan`` runs first and rejects any
    ref without a ``.`` separator.
    """
    return {
        table_spec.name: {
            col_spec.foreign_key.ref.split(".", 1)[0]
            for col_spec in table_spec.columns
            if col_spec.foreign_key is not None
        }
        for table_spec in plan.tables
    }


def _topological_sort(graph: dict[str, set[str]], all_tables: list[str]) -> list[str]:
    """Kahn's algorithm. Raise ValueError on cycles."""
    # Parent -> children adjacency, derived from the child -> parents graph.
    adj: dict[str, list[str]] = {
        parent: [child for child, parents in graph.items() if parent in parents]
        for parent in all_tables
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
    """Extract PK generation metadata from a TableSpec.

    Raises if ``table_spec.seed is None`` -- matches the strictness of
    ``generate_table``.  A prior implementation silently substituted
    ``plan.seed``, which would have the FK child reconstruct parent
    PKs under a different seed than the parent itself was generated
    under once the generator entry points started raising -- splitting
    the same TableSpec across two seeds on the FK boundary.
    """
    if table_spec.seed is None:
        raise ValueError(
            f"TableSpec '{table_spec.name}'.seed is None.  Either set "
            f"it explicitly on the TableSpec or go through a "
            f"DataGenPlan (which propagates plan.seed to each table "
            f"during Pydantic validation)."
        )

    base_meta = {
        "table_name": table_spec.name,
        "pk_column": pk_col_spec.name,
        "row_count": int(table_spec.rows),
        "pk_seed": derive_column_seed(table_spec.seed, table_spec.name, pk_col_spec.name),
    }

    # ``_validate_primary_keys`` rejects any other strategy at plan
    # time; the ``case _`` branch is a defensive backstop for an
    # invariant bypass and surfaces a clear error instead of silently
    # building synthetic-sequence metadata that would corrupt FK
    # children.
    match pk_col_spec.gen:
        case SequenceColumn(start=start, step=step):
            return PKMetadata(**base_meta, pk_type="sequence", pk_start=start, pk_step=step, pk_template=None)
        case PatternColumn(template=template):
            return PKMetadata(**base_meta, pk_type="pattern", pk_start=0, pk_step=1, pk_template=template)
        case UUIDColumn():
            return PKMetadata(**base_meta, pk_type="uuid", pk_start=0, pk_step=1, pk_template=None)
        case _:
            raise RuntimeError(
                f"_extract_pk_metadata received PK column '{table_spec.name}."
                f"{pk_col_spec.name}' with unsupported strategy "
                f"{type(pk_col_spec.gen).__name__}.  ``_validate_primary_keys`` "
                f"should have rejected this at plan time -- a validator-ordering "
                f"regression has bypassed the check."
            )


def _extract_column_references(expr: str) -> set[str]:
    """Return identifiers in ``expr`` that look like column references.

    Filters out function calls (identifier immediately followed by ``(``),
    qualified field access (identifier preceded by ``.``), and a small
    set of SQL keywords / literals / type names.  Single-quoted string
    literals, double-quoted string literals, and backtick-quoted
    identifiers are all stripped before tokenizing so their contents do
    not false-positive.

    Intentionally does NOT maintain a list of Spark function names: the
    "followed by ``(``" test works for any function Spark adds without
    the allowlist drifting out of date.
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
    """Raise ValueError if an ExpressionColumn references an unknown or unreachable column.

    Two checks, both phrased so the failure surfaces at plan time with
    a clean traceback instead of a downstream Spark ``UNRESOLVED_COLUMN``:

    1. Every referenced name must be a column on the same table.
    2. Every referenced name must be **phase-1-visible**.  The engine
       applies columns in three phases (regular Spark SQL via flat
       ``select``, then FK / Faker via ``withColumn``, then
       ``seed_from``-derived via ``withColumn``).  ``ExpressionColumn``
       runs in phase 1, so referencing an FK / Faker / ``seed_from``
       column means the referenced column isn't in the DataFrame yet
       at evaluation time -- Spark would raise
       ``UNRESOLVED_COLUMN`` far from the offending declaration.
    """
    for table_spec in plan.tables:
        col_by_name = {c.name: c for c in table_spec.columns}
        col_names = set(col_by_name)
        # Names of columns that are NOT phase-1-visible: anything with
        # a foreign_key (phase 2), a FakerColumn strategy (phase 2),
        # or a seed_from (phase 3).
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
    """Validate seed_from references.

    Four checks, in order:

    1. The referenced column exists in the same table.
    2. A column does not reference itself (``a.seed_from = 'a'``).
    3. The referenced column does not itself have ``seed_from`` set
       (no chains).  Chains like ``a -> b -> c`` would break at
       Spark plan time because phase-3 columns are applied in
       declaration order, not dependency-topo order; an ``F.col(b)``
       reference resolves to the un-materialised ``b`` and Spark
       raises ``UNRESOLVED_COLUMN``.  Real users want one source
       column with many derived columns, not chains -- the planner
       rejects the chain shape with a clear error rather than
       complicating the engine to topo-sort phase 3.
    4. The seed_from graph is acyclic -- check 3 already implies
       this for chains, but the explicit cycle walk also catches the
       degenerate single-edge ``a -> a`` (handled by check 2 above)
       and stays as a defensive backstop.

    All four would otherwise fail at Spark query-build with
    ``UNRESOLVED_COLUMN`` or a self-join plan, far from the offending
    column declaration.
    """
    for table_spec in plan.tables:
        col_names = {c.name for c in table_spec.columns}
        col_by_name = {c.name: c for c in table_spec.columns}
        seed_from_map: dict[str, str] = {}
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
            seed_from_map[col_spec.name] = col_spec.seed_from

        # Graph walk: defensive backstop for cycles that bypass the
        # chain check (shouldn't be reachable, but kept so future
        # refactors don't silently regress).
        for start, start_target in seed_from_map.items():
            visited: list[str] = [start]
            cur = start_target
            while cur in seed_from_map:
                if cur in visited:
                    cycle = [*visited[visited.index(cur) :], cur]
                    raise ValueError(
                        f"seed_from cycle in table '{table_spec.name}': "
                        f"{' -> '.join(cycle)}.  Break the cycle by removing "
                        f"one of the seed_from links."
                    )
                visited.append(cur)
                cur = seed_from_map[cur]


def _validate_primary_keys(plan: DataGenPlan) -> None:
    """Validate that PK columns exist and use a supported PK strategy.

    Two checks:

    1. Each named PK column exists on the table.
    2. The PK column's ``gen`` strategy is one the FK reconstruction
       path actually handles: ``SequenceColumn``, ``PatternColumn``,
       or ``UUIDColumn``.  Any other strategy (``RangeColumn``,
       ``ValuesColumn``, ``ConstantColumn``, etc.) cannot be
       reconstructed deterministically from PK metadata alone, so an
       FK pointed at such a PK would emit values that don't match the
       parent's actual PK values.  Reject at plan time so the error
       names the offending column rather than surfacing as silent
       data corruption at materialization.
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
