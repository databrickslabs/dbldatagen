import pytest
from dbldatagen.spec.compat import BaseModel, Field, constr, root_validator, validator


class TestPydanticCompat:
    """Tests for Pydantic compatibility layer"""

    def test_basemodel_import(self):
        """Test that BaseModel can be imported and used"""
        assert BaseModel is not None

        # Create a simple model
        class TestModel(BaseModel):
            name: str
            value: int

        instance = TestModel(name="test", value=42)
        assert instance.name == "test"
        assert instance.value == 42

    def test_field_import(self):
        """Test that Field can be imported and used"""
        assert Field is not None

        class TestModel(BaseModel):
            name: str = Field(..., description="A name")
            value: int = Field(default=0, ge=0)

        instance = TestModel(name="test")
        assert instance.value == 0

    def test_constr_import(self):
        """Test that constr can be imported and used"""
        assert constr is not None

        class TestModel(BaseModel):
            code: constr(min_length=3, max_length=10)  # type: ignore

        instance = TestModel(code="ABC")
        assert instance.code == "ABC"

        # Test validation
        with pytest.raises(Exception):  # ValidationError
            TestModel(code="AB")  # Too short

    def test_validator_import(self):
        """Test that validator decorator can be imported and used"""
        assert validator is not None

        class TestModel(BaseModel):
            value: int

            @validator("value")
            def validate_positive(cls, v):
                if v < 0:
                    raise ValueError("must be positive")
                return v

        instance = TestModel(value=10)
        assert instance.value == 10

        with pytest.raises(Exception):  # ValidationError
            TestModel(value=-5)

    def test_root_validator_import(self):
        """Test that root_validator decorator can be imported and used"""
        assert root_validator is not None

        class TestModel(BaseModel):
            min_val: int
            max_val: int

            @root_validator()
            def validate_range(cls, values):
                if values.get("min_val", 0) > values.get("max_val", 100):
                    raise ValueError("min must be <= max")
                return values

        instance = TestModel(min_val=10, max_val=20)
        assert instance.min_val == 10

        with pytest.raises(Exception):  # ValidationError
            TestModel(min_val=30, max_val=20)

    def test_all_exports_present(self):
        """Test that all expected exports are available"""
        from dbldatagen.spec import compat

        assert hasattr(compat, "BaseModel")
        assert hasattr(compat, "Field")
        assert hasattr(compat, "constr")
        assert hasattr(compat, "validator")
        assert hasattr(compat, "root_validator")

        # Check __all__
        expected_exports = ["BaseModel", "Field", "constr", "root_validator", "validator"]
        assert set(compat.__all__) == set(expected_exports)
