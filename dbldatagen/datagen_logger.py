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
This module defines the logger used by the library

"""
import logging

LOGGER_NAME = "dbldatagen"


class LibraryLoggerHelper:
    _logger = None

    @classmethod
    def getLogger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger("dbldatagen")

            date_format = "%Y-%m-%d %H:%M:%S"
            log_format = "[%(name)s][%(asctime)s][%(levelname)s][%(module)s][%(funcName)s] %(message)s"
            formatter = logging.Formatter(log_format, date_format)
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            cls._logger.setLevel(level=logging.INFO)
            cls._logger.addHandler(handler)

            cls._logger.info("Initialized library logger")

        return cls._logger



