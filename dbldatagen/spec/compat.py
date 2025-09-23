# This module acts as a compatibility layer for Pydantic V1 and V2.

try:
    # This will succeed on environments with Pydantic V2.x
    # It imports the V1 API that is bundled within V2.
    from pydantic.v1 import BaseModel, Field, validator, constr

except ImportError:
    # This will be executed on environments with only Pydantic V1.x
    from pydantic import BaseModel, Field, validator, constr

# In your application code, do this:
# from .compat import BaseModel
# NOT this:
# from pydantic import BaseModel

# FastAPI Notes
# https://github.com/fastapi/fastapi/blob/master/fastapi/_compat.py