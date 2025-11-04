To write code that works on both Pydantic V1 and V2 and ensures a smooth future migration, you should code against the V1 API but import it through a compatibility shim. This approach uses V1's syntax, which Pydantic V2 can understand via its built-in V1 compatibility layer.

-----

### \#\# The Golden Rule: Code to V1, Import via a Shim 💡

The core strategy is to **write all your models using Pydantic V1 syntax and features**. You then use a special utility file to handle the imports, which makes your application code completely agnostic to the installed Pydantic version.

-----

### \#\# 1. Implement a Compatibility Shim (`compat.py`)

This is the most critical step. Create a file named `compat.py` in your project that intelligently imports Pydantic components. Your application will import everything from this file instead of directly from `pydantic`.

```python
# compat.py
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
```

-----

### \#\# 2. Stick to V1 Features and Syntax (Do's and Don'ts)

By following these rules in your application code, you ensure the logic works on both versions.

#### **✅ Models and Fields: DO**

  * Use standard `BaseModel` and `Field` for all your data structures. This is the most stable part of the API.

#### **❌ Models and Fields: DON'T**

  * **Do not use `__root__` models**. This V1 feature was removed in V2 and the compatibility is not perfect. Instead, model the data explicitly, even if it feels redundant.
      * **Bad (Avoid):** `class MyList(BaseModel): __root__: list[str]`
      * **Good (Compatible):** `class MyList(BaseModel): items: list[str]`

#### **✅ Configuration: DO**

  * Use the nested `class Config:` for model configuration. This is the V1 way and is fully supported by the V2 compatibility layer.
      * **Example:**
        ```python
        from .compat import BaseModel

        class User(BaseModel):
            id: int
            full_name: str

            class Config:
                orm_mode = True # V2's compatibility layer translates this
                allow_population_by_field_name = True
        ```

#### **❌ Configuration: DON'T**

  * **Do not use the V2 `model_config` dictionary**. This is a V2-only feature.

#### **✅ Validators and Data Types: DO**

  * Use the standard V1 `@validator`. It's robust and works perfectly across both versions.
  * Use V1 constrained types like `constr`, `conint`, `conlist`.
      * **Example:**
        ```python
        from .compat import BaseModel, validator, constr

        class Product(BaseModel):
            name: constr(min_length=3)

            @validator("name")
            def name_must_be_alpha(cls, v):
                if not v.isalpha():
                    raise ValueError("Name must be alphabetic")
                return v
        ```

#### **❌ Validators and Data Types: DON'T**

  * **Do not use V2 decorators** like `@field_validator`, `@model_validator`, or `@field_serializer`.
  * **Do not use the V2 `Annotated` syntax** for validation (e.g., `Annotated[str, StringConstraints(min_length=2)]`).

-----

### \#\# 3. The Easy Migration Path

When you're finally ready to leave V1 behind and upgrade your code to be V2-native, the process will be straightforward because your code is already consistent:

1.  **Change Imports**: Your first step will be a simple find-and-replace to change all `from .compat import ...` statements to `from pydantic import ...`.
2.  **Run a Codelinter**: Tools like **Ruff** have built-in rules that can automatically refactor most of your V1 syntax (like `Config` classes and `@validator`s) to the new V2 syntax.
3.  **Manual Refinements**: Address any complex patterns the automated tools couldn't handle, like replacing your `__root__` model alternatives.