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

The main entry point for any test data generation activities is the `DataGenerator` class.

Most of the other classes are used for internal purposes only
"""

__all__ = ["data_generator", "data_analyzer", "schema_parser", "dataranges",
           "column_generation_spec", "utils", "function_builder", "spark_singleton", "text_generators"]

from databrickslabs_testdatagenerator.data_generator import DataGenerator
from databrickslabs_testdatagenerator.utils import ensure, topologicalSort, mkBoundsList
from databrickslabs_testdatagenerator.column_generation_spec import ColumnGenerationSpec
from databrickslabs_testdatagenerator.column_spec_options import ColumnSpecOptions
from databrickslabs_testdatagenerator.data_analyzer import DataAnalyzer
from databrickslabs_testdatagenerator.schema_parser import SchemaParser
from databrickslabs_testdatagenerator.dataranges import DateRange, NRange
from databrickslabs_testdatagenerator.function_builder import ColumnGeneratorBuilder
from .spark_singleton import SparkSingleton
from databrickslabs_testdatagenerator.text_generators import TemplateGenerator, ILText
