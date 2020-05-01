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

import datetime


class DateRange(object):

    @classmethod
    def parseInterval(cls, interval_str):
        assert interval_str is not None
        results = []
        for kv in interval_str.split(","):
            key, value = kv.split('=')
            results.append("'{}':{}".format(key, value))

        return eval("{{ {}  }} ".format(",".join(results)))

    def __init__(self, begin, end, interval=None, datetime_format="%Y-%m-%d %H:%M:%S"):
        assert begin is not None
        assert end is not None

        self.begin = begin if not isinstance(begin, str) else datetime.datetime.strptime(begin, datetime_format)
        self.end = end if not isinstance(end, str) else datetime.datetime.strptime(end, datetime_format)
        self.interval = interval if not isinstance(interval, str) else datetime.timedelta(
            **self.parseInterval(interval))
