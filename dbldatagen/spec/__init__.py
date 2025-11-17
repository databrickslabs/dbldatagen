"""Pydantic-based specification API for dbldatagen.

This module provides Pydantic models and specifications for defining data generation
in a type-safe, declarative way.
"""

# Import only the compat layer by default to avoid triggering Spark/heavy dependencies
from .compat import BaseModel, Field, constr, root_validator, validator

# Lazy imports for heavy modules - import these explicitly when needed
# from .column_spec import ColumnSpec
# from .generator_spec import GeneratorSpec
# from .generator_spec_impl import GeneratorSpecImpl

__all__ = [
    "BaseModel",
    "Field",
    "constr",
    "root_validator",
    "validator",
    "ColumnSpec",
    "GeneratorSpec",
    "GeneratorSpecImpl",
]


def __getattr__(name):
    """Lazy import heavy modules to avoid triggering Spark initialization."""
    if name == "ColumnSpec":
        from .column_spec import ColumnSpec
        return ColumnSpec
    elif name == "GeneratorSpec":
        from .generator_spec import GeneratorSpec
        return GeneratorSpec
    elif name == "GeneratorSpecImpl":
        from .generator_spec_impl import GeneratorSpecImpl
        return GeneratorSpecImpl
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

