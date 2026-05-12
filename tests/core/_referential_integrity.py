"""Test-only post-generation referential integrity check.

Lives under ``tests/`` because production ``dbldatagen.core`` has no
caller for it: ``resolve_plan`` validates every FK reference at plan
time, and the engine deterministically reconstructs parent PK values
from ``PKMetadata`` so every emitted FK row is guaranteed to hit a real
parent PK by construction.  This helper is a regression check that the
guarantee actually held byte-for-byte on a generated ``DataFrame`` --
useful as a test oracle, not as a user-facing API.
"""

from __future__ import annotations

from dbldatagen.core.spec.schema import DataGenPlan


def validate_referential_integrity(
    dataframes: dict,
    plan: DataGenPlan,
) -> list[str]:
    """Post-generation check that all FK values reference valid parent PKs.

    Iterates every FK column in the plan and uses a Spark left-anti
    join against the parent PK column to find orphan FK values.
    Useful as a smoke test after large multi-table generation, or as
    a continuous check in regression tests.  ``NULL`` FK values are
    excluded from the orphan check (legitimate when
    ``ForeignKeyRef.null_fraction > 0`` or
    ``ForeignKeyRef.nullable=True``).

    Args:
        dataframes: Mapping of table name to generated ``DataFrame``,
          typically the output of ``generate(spark, plan)``.
        plan: The ``DataGenPlan`` that produced ``dataframes``.

    Returns:
        A list of error messages, one per failing FK column or
        missing table.  An empty list means every FK in ``plan``
        resolves to a real parent PK in ``dataframes``.
    """
    errors: list[str] = []

    for table_spec in plan.tables:
        table_name = table_spec.name
        if table_name not in dataframes:
            errors.append(f"Table '{table_name}' missing from generated DataFrames")
            continue

        child_df = dataframes[table_name]

        for col_spec in table_spec.columns:
            if col_spec.foreign_key is None:
                continue

            ref = col_spec.foreign_key.ref
            parent_table_name, parent_col_name = ref.split(".", 1)

            if parent_table_name not in dataframes:
                errors.append(
                    f"Parent table '{parent_table_name}' missing from generated DataFrames "
                    f"(referenced by {table_name}.{col_spec.name})"
                )
                continue

            parent_df = dataframes[parent_table_name]

            # Filter out nulls from child FK column (nulls are valid for nullable FKs)
            child_non_null = child_df.filter(child_df[col_spec.name].isNotNull()).select(col_spec.name)

            parent_pks = parent_df.select(parent_col_name)

            # Left anti join: find child FK values with no matching parent PK
            orphans = child_non_null.join(
                parent_pks,
                child_non_null[col_spec.name] == parent_pks[parent_col_name],
                "left_anti",
            )

            # Use limit(1) to short-circuit — avoids full table scan
            if orphans.limit(1).count() > 0:
                # Only compute full count for the error message
                orphan_count = orphans.count()
                errors.append(
                    f"{table_name}.{col_spec.name} has {orphan_count} orphan FK values "
                    f"not found in {parent_table_name}.{parent_col_name}"
                )

    return errors
