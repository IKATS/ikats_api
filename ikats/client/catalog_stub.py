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
from ikats.client import CatalogClient


class Singleton(type):
    """
    Singleton class used to synchronize the databases
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CatalogStub(CatalogClient, metaclass=Singleton):
    """
    Catalog client used to connect to Python catalog backend
    """
    db = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_url = "/ikats/algo/catalogue"

    def get_implementation_list(self):
        return self.db

    def get_implementation(self, name):
        return self.db[name]
