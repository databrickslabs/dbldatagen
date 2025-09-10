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
This module defines library constants

"""

# default random seed
DEFAULT_RANDOM_SEED: int = 42
RANDOM_SEED_RANDOM: int = -1
RANDOM_SEED_RANDOM_FLOAT: float = -1.0
RANDOM_SEED_FIXED: str = "fixed"
RANDOM_SEED_HASH_FIELD_NAME: str = "hash_fieldname"

# constants related to seed column
DEFAULT_SEED_COLUMN: str = "id"

# this is the column name produced by `spark.range`
# dont change unless semantics of `spark.range` changes
SPARK_RANGE_COLUMN: str = "id"

# minimum versions for version checks
MIN_PYTHON_VERSION: tuple[int, int] = (3, 8)
MIN_SPARK_VERSION: tuple[int, int, int] = (3, 1, 2)

# options for randon data generation
OPTION_RANDOM: str = "random"
OPTION_RANDOM_SEED_METHOD: str = "randomSeedMethod"
OPTION_RANDOM_SEED: str = "randomSeed"

INFER_DATATYPE: str = "__infer__"

# default parallelism when sparkContext is not available
SPARK_DEFAULT_PARALLELISM: int = 200
