#
# Copyright (C) 2019 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the package contents for the test data generator library

The main entry point for any test data generation activities is
the `DataGenerator` class.

Most of the other classes are used for internal purposes only
"""

from .data_generator import DataGenerator
from .datagen_constants import DEFAULT_RANDOM_SEED, RANDOM_SEED_RANDOM, RANDOM_SEED_FIXED, RANDOM_SEED_HASH_FIELD_NAME
from .utils import ensure, topologicalSort, mkBoundsList, coalesce_values, \
    deprecated, parse_time_interval, DataGenError
from ._version import __version__
from .column_generation_spec import ColumnGenerationSpec
from .column_spec_options import ColumnSpecOptions
from .data_analyzer import DataAnalyzer
from .schema_parser import SchemaParser
from .daterange import DateRange
from .datarange import DataRange
from .nrange import NRange
from .function_builder import ColumnGeneratorBuilder
from .spark_singleton import SparkSingleton
from .text_generators import TemplateGenerator, ILText, TextGenerator
from .text_generator_plugins import PyfuncText, PyfuncTextFactory, FakerTextFactory, fakerText

__all__ = ["data_generator", "data_analyzer", "schema_parser", "daterange", "nrange",
           "column_generation_spec", "utils", "function_builder",
           "spark_singleton", "text_generators", "datarange", "datagen_constants",
           "text_generator_plugins"
           ]


def python_version_check():
    import sys
    if not sys.version_info >= (3, 6):
        raise RuntimeError("Minimum version of Python supported is 3.6")


# lets check for a correct python version or raise an exception
python_version_check()
