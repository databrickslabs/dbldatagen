import pytest


class TestSpecModuleLazyImports:
    """Tests for dbldatagen.spec module lazy imports"""

    def test_compat_exports_available(self):
        """Test that compat layer exports are available at module level"""
        from dbldatagen.spec import BaseModel, Field, constr, root_validator, validator

        assert BaseModel is not None
        assert Field is not None
        assert constr is not None
        assert root_validator is not None
        assert validator is not None

    def test_lazy_import_column_spec_via_getattr(self):
        """Test lazy import via __getattr__ for ColumnSpec"""
        import dbldatagen.spec as spec_module

        # Access via __getattr__ using the name it checks for
        ColumnSpec = getattr(spec_module, "ColumnSpec")

        assert ColumnSpec is not None
        assert ColumnSpec.__name__ == "ColumnDefinition"

    def test_lazy_import_generator_spec_via_getattr(self):
        """Test lazy import via __getattr__ for GeneratorSpec"""
        import dbldatagen.spec as spec_module

        # Access via __getattr__ using the name it checks for
        GeneratorSpec = getattr(spec_module, "GeneratorSpec")

        assert GeneratorSpec is not None
        assert GeneratorSpec.__name__ == "DatagenSpec"

    def test_lazy_import_generator_spec_impl_via_getattr(self):
        """Test lazy import via __getattr__ for GeneratorSpecImpl"""
        import dbldatagen.spec as spec_module

        # Access via __getattr__ using the name it checks for
        GeneratorSpecImpl = getattr(spec_module, "GeneratorSpecImpl")

        assert GeneratorSpecImpl is not None
        assert GeneratorSpecImpl.__name__ == "Generator"

    def test_invalid_attribute_raises_error(self):
        """Test that accessing invalid attributes raises AttributeError"""
        import dbldatagen.spec as spec_module

        with pytest.raises(AttributeError, match="has no attribute 'NonExistentClass'"):
            _ = spec_module.NonExistentClass  # type: ignore

    def test_all_exports_listed(self):
        """Test that __all__ contains expected exports"""
        from dbldatagen import spec

        expected_exports = [
            "BaseModel",
            "ColumnDefinition",
            "DatagenSpec",
            "Field",
            "Generator",
            "constr",
            "root_validator",
            "validator",
        ]

        assert set(spec.__all__) == set(expected_exports)

    def test_direct_import_column_definition(self):
        """Test direct import of ColumnDefinition from spec.column_spec"""
        from dbldatagen.spec.column_spec import ColumnDefinition

        assert ColumnDefinition is not None

        # Verify it can be used
        col = ColumnDefinition(name="test", type="string")
        assert col.name == "test"

    def test_direct_import_datagen_spec(self):
        """Test direct import of DatagenSpec from spec.generator_spec"""
        from dbldatagen.spec.generator_spec import DatagenSpec

        assert DatagenSpec is not None

    def test_direct_import_generator(self):
        """Test direct import of Generator from spec.generator_spec_impl"""
        from dbldatagen.spec.generator_spec_impl import Generator
        from unittest.mock import MagicMock

        assert Generator is not None

        # Verify it can be instantiated
        mock_spark = MagicMock()
        gen = Generator(mock_spark)
        assert gen is not None


class TestSpecModuleDocstring:
    """Tests for spec module documentation"""

    def test_module_has_docstring(self):
        """Test that the spec module has documentation"""
        import dbldatagen.spec as spec_module

        assert spec_module.__doc__ is not None
        assert "Pydantic-based" in spec_module.__doc__
        assert "Experimental" in spec_module.__doc__
