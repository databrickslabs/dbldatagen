"""Pydantic-based specification API for dbldatagen (Experimental).

This module provides Pydantic models and specifications for defining data generation
in a type-safe, declarative way.

.. warning::
   Experimental - This API is experimental and both APIs and generated code
   are liable to change in future versions.
"""

from typing import Any

# Import only the compat layer by default to avoid triggering Spark/heavy dependencies
from dbldatagen.spec.compat import BaseModel, Field, constr, root_validator, validator


# Lazy imports for heavy modules - import these explicitly when needed
# from .column_spec import ColumnSpec
# from .generator_spec import GeneratorSpec
# from .generator_spec_impl import GeneratorSpecImpl

__all__ = [
    "BaseModel",
    "ColumnDefinition",
    "DatagenSpec",
    "Field",
    "Generator",
    "constr",
    "root_validator",
    "validator",
]


def __getattr__(name: str) -> Any:  # noqa: ANN401
    """Lazy import heavy modules to avoid triggering Spark initialization.

    Note: Imports are intentionally inside this function to enable lazy loading
    and avoid importing heavy dependencies (pandas, IPython, Spark) until needed.
    """
    if name == "ColumnSpec":
        from .column_spec import ColumnDefinition  # noqa: PLC0415

        return ColumnDefinition
    elif name == "GeneratorSpec":
        from .generator_spec import DatagenSpec  # noqa: PLC0415

        return DatagenSpec
    elif name == "GeneratorSpecImpl":
        from .generator_spec_impl import Generator  # noqa: PLC0415

        return Generator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
