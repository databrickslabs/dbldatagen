"""DatagenSpec for Basic User Dataset.

This module defines a declarative Pydantic-based specification for generating
the basic user dataset, corresponding to the BasicUserProvider.
"""

from dbldatagen.spec.generator_spec import DatagenSpec, TableDefinition
from dbldatagen.spec.column_spec import ColumnDefinition


def create_basic_user_spec(
    number_of_rows: int = 100000,
    partitions: int | None = None,
    random: bool = False
) -> DatagenSpec:
    """Create a DatagenSpec for basic user data generation.

    This function creates a declarative specification matching the data generated
    by BasicUserProvider in the datasets module.

    Args:
        number_of_rows: Total number of rows to generate (default: 100,000)
        partitions: Number of Spark partitions to use (default: auto-computed)
        random: If True, generates random data; if False, uses deterministic patterns

    Returns:
        DatagenSpec configured for basic user data generation

    Example:
        >>> spec = create_basic_user_spec(number_of_rows=1000, random=True)
        >>> spec.validate()
        >>> # Use with GeneratorSpecImpl to generate data
    """
    MAX_LONG = 9223372036854775807

    columns = [
        ColumnDefinition(
            name="customer_id",
            type="long",
            options={
                "minValue": 1000000,
                "maxValue": MAX_LONG,
                "random": random
            }
        ),
        ColumnDefinition(
            name="name",
            type="string",
            options={
                "template": r"\w \w|\w \w \w",
                "random": random
            }
        ),
        ColumnDefinition(
            name="email",
            type="string",
            options={
                "template": r"\w.\w@\w.com|\w@\w.co.u\k",
                "random": random
            }
        ),
        ColumnDefinition(
            name="ip_addr",
            type="string",
            options={
                "template": r"\n.\n.\n.\n",
                "random": random
            }
        ),
        ColumnDefinition(
            name="phone",
            type="string",
            options={
                "template": r"(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd",
                "random": random
            }
        ),
    ]

    table_def = TableDefinition(
        number_of_rows=number_of_rows,
        partitions=partitions,
        columns=columns
    )

    spec = DatagenSpec(
        tables={"users": table_def},
        output_destination=None,  # No automatic persistence
        generator_options={
            "randomSeedMethod": "hash_fieldname"
        }
    )

    return spec


# Pre-configured specs for common use cases
BASIC_USER_SPEC_SMALL = create_basic_user_spec(number_of_rows=1000, random=False)
"""Pre-configured spec for small dataset (1,000 rows, deterministic)"""

BASIC_USER_SPEC_MEDIUM = create_basic_user_spec(number_of_rows=100000, random=False)
"""Pre-configured spec for medium dataset (100,000 rows, deterministic)"""

BASIC_USER_SPEC_LARGE = create_basic_user_spec(number_of_rows=1000000, random=False)
"""Pre-configured spec for large dataset (1,000,000 rows, deterministic)"""

BASIC_USER_SPEC_RANDOM = create_basic_user_spec(number_of_rows=100000, random=True)
"""Pre-configured spec for random data (100,000 rows, random)"""

