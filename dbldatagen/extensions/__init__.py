"""
Extension modules for dbldatagen.

This package contains optional extensions that can be installed via:
    pip install dbldatagen[extension]
"""

__version__ = "0.1.0" 


try:
    from .spec_generators.uc_generator import DatabricksUCSpecGenerator
    from .spec_generators.employee_generator import EmployeeSpecGenerator
    __all__ = ['DatabricksUCSpecGenerator', 'EmployeeSpecGenerator']
except ImportError:
    __all__ = []