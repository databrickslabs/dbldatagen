"""Post-generation referential integrity validation.

Uses left anti joins to detect orphan FK values that don't reference
any valid parent PK.
"""

from __future__ import annotations

from dbldatagen.v1.schema import DataGenPlan


def validate_referential_integrity(
    dataframes: dict,
    plan: DataGenPlan,
) -> list[str]:
    """Post-generation check that all FK values reference valid parent PKs.

    Uses left anti join to find orphan FK values.
    Returns list of error messages (empty = valid).
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

            orphan_count = orphans.count()
            if orphan_count > 0:
                errors.append(
                    f"{table_name}.{col_spec.name} has {orphan_count} orphan FK values "
                    f"not found in {parent_table_name}.{parent_col_name}"
                )

    return errors
