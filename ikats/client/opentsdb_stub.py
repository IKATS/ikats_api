# -*- coding: utf-8 -*-
"""
Copyright 2019 CS Systèmes d'Information

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import random

from ikats.client.opentsdb_client import OpenTSDBClient


class Singleton(type):
    """
    Singleton class used to synchronize the databases
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OpenTSDBStub(OpenTSDBClient, metaclass=Singleton):
    """
    Wrapper for Ikats to connect to OpenTSDB api
    """

    DB = {}

    def get_nb_points_of_tsuid(self, tsuid):
        return len(self.DB[tsuid])

    def assign_metric(self, metric, tags):
        return str(hex(random.randint(0, 0xFFFFFFFFFFFFFFFFFFFF))).upper()[2:]

    def get_ts_by_tsuid(self, tsuid, sd, ed=None):
        return self.DB[tsuid]

    def add_points(self, tsuid, data):
        self.DB[tsuid] = data
        return data[0][0], data[-1][0], len(data)
