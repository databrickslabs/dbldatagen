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
DEFAULT_RANDOM_SEED = 42
RANDOM_SEED_RANDOM = -1
RANDOM_SEED_RANDOM_FLOAT = -1.0
RANDOM_SEED_FIXED = "fixed"
RANDOM_SEED_HASH_FIELD_NAME = "hash_fieldname"

# minimum versions for version checks
MIN_PYTHON_VERSION = (3, 8)
MIN_SPARK_VERSION = (3, 1, 2)
