# This module acts as a compatibility layer for Pydantic V1 and V2.

try:
    # This will succeed on environments with Pydantic V2.x
    from pydantic.v1 import BaseModel, Field, constr, root_validator, validator
except ImportError:
    # This will be executed on environments with only Pydantic V1.x
    from pydantic import BaseModel, Field, constr, root_validator, validator  # type: ignore[assignment,no-redef]

__all__ = ["BaseModel", "Field", "constr", "root_validator", "validator"]
# In your application code, do this:
# from .compat import BaseModel
# NOT this:
# from pydantic import BaseModel

# FastAPI Notes
# https://github.com/fastapi/fastapi/blob/master/fastapi/_compat.py


"""
## Why This Approach
No Installation Required: It directly addresses your core requirement.
You don't need to %pip install anything, which avoids conflicts with the pre-installed libraries on Databricks.
Single Codebase: You maintain one set of code that is guaranteed to work with the Pydantic V1 API, which is available in both runtimes.

Environment Agnostic: Your application code in models.py has no idea which version of Pydantic is actually installed. The compat.py module handles that complexity completely.

Future-Ready: When you eventually decide to migrate fully to the Pydantic V2 API (to take advantage of its speed and features),
you only need to change your application code and your compat.py import statements, making the transition much clearer.
"""
