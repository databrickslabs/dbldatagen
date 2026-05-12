"""dbldatagen.core.spec -- Declarative Pydantic models and DSL for data generation plans.

The lowercase DSL factory helpers (``integer``, ``decimal``, ``text``,
``array``, ``struct``, ``faker``, ...) live in :mod:`dbldatagen.core.spec.dsl`
and are deliberately not re-exported here.  Several of those names shadow
stdlib modules (``decimal``, ``array``, ``struct``) or popular third-party
packages (``faker``) when flat-imported.  Import the module under a short
alias instead::

    from dbldatagen.core.spec import dsl as dg

    dg.integer("age", 0, 99)
    dg.decimal("price", precision=10, scale=2)
"""

from __future__ import annotations

from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyColumn,
    ForeignKeyRef,
    PrimaryKey,
    TableSpec,
)


__all__ = [
    "ColumnSpec",
    "DataGenPlan",
    "DataType",
    "ForeignKeyColumn",
    "ForeignKeyRef",
    "PrimaryKey",
    "TableSpec",
]
