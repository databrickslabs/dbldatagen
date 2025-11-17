"""Pydantic compatibility layer for supporting both Pydantic V1 and V2.

This module provides a unified interface for Pydantic functionality that works across both
Pydantic V1.x and V2.x versions. It ensures that the dbldatagen spec API works in multiple
environments without requiring specific Pydantic version installations.

The module exports a consistent Pydantic V1-compatible API regardless of which version is installed:

- **BaseModel**: Base class for all Pydantic models
- **Field**: Field definition with metadata and validation
- **constr**: Constrained string type for validation
- **root_validator**: Decorator for model-level validation
- **validator**: Decorator for field-level validation

Usage in other modules:
    Always import from this compat module, not directly from pydantic::

        # Correct
        from .compat import BaseModel, validator

        # Incorrect - don't do this
        from pydantic import BaseModel, validator

Environment Support:
    - **Pydantic V2.x environments**: Imports from pydantic.v1 compatibility layer
    - **Pydantic V1.x environments**: Imports directly from pydantic package
    - **Databricks runtimes**: Works with pre-installed Pydantic versions without conflicts

.. note::
    This approach is inspired by FastAPI's compatibility layer:
    https://github.com/fastapi/fastapi/blob/master/fastapi/_compat.py

Benefits:
    - **No Installation Required**: Works with whatever Pydantic version is available
    - **Databricks Compatible**: Avoids conflicts with pre-installed libraries

Future Migration:
    When ready to migrate to native Pydantic V2 API:
    1. Update application code to use V2 patterns
    2. Modify this compat.py to import from native V2 locations
    3. Test in both environments
    4. Deploy incrementally
"""

try:
    # This will succeed on environments with Pydantic V2.x
    # Pydantic V2 provides a v1 compatibility layer for backwards compatibility
    from pydantic.v1 import BaseModel, Field, constr, root_validator, validator
except ImportError:
    # This will be executed on environments with only Pydantic V1.x
    # Import directly from pydantic since v1 subpackage doesn't exist
    from pydantic import BaseModel, Field, constr, root_validator, validator  # type: ignore[assignment,no-redef]

__all__ = ["BaseModel", "Field", "constr", "root_validator", "validator"]
