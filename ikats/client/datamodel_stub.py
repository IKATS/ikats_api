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

from enum import Enum

from ikats.client import DatamodelClient
from ikats.exceptions import IkatsConflictError, IkatsNotFoundError


class DTYPE(Enum):
    """
    Enum used for Data types of Metadata
    """
    string = "string"
    date = "date"
    number = "number"
    complex = "complex"


class Singleton(type):
    """
    Singleton class used to synchronize the databases
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DatamodelStub(DatamodelClient, metaclass=Singleton):
    """
    Temporal Data Manager api used to connect to JAVA Ikats API
    """

    # [{tsuid:x, funcid:x}]
    db_ts = []

    # {tsuid: [{name:x, value:x, data_type:x}]}
    db_md = dict()

    # [{name:x, description:x, ts_list:[tsuid]}]
    db_ds = []

    # [table]
    db_table = []

    # {rid:data, ...}
    db_rid = {}

    def get_ts_list(self):
        return [{"tsuid": x['tsuid'], "fid": x['funcId']} for x in self.db_ts]

    def dataset_create(self, name, description, ts):
        if name in [x["name"] for x in self.db_ds]:
            raise IkatsConflictError("Dataset %s already exists in database" % name)
        else:
            self.db_ds.append({'name': name, "description": description, "ts_list": ts})
        return True

    def dataset_read(self, name):
        if name not in [x["name"] for x in self.db_ds]:
            raise IkatsNotFoundError("Dataset %s not found in database" % name)
        try:
            return [x["name"] for x in self.db_ds if x["name"] == name][0]
        except IndexError:
            raise IkatsNotFoundError()

    def dataset_delete(self, name, deep=False):
        if name not in [x["name"] for x in self.db_ds]:
            raise IkatsNotFoundError("Dataset %s not found in database" % name)
        temp = [x["name"] for x in self.db_ds if x["name"] != name]
        self.db_ds = temp

    def dataset_list(self):
        return [{x["name"], x['description']} for x in self.db_ds]

    def import_fid(self, tsuid, fid):
        if tsuid in [x['tsuid'] for x in self.db_ts]:
            raise IkatsConflictError()
        if fid in [x['funcid'] for x in self.db_ts]:
            raise IkatsConflictError()
        self.db_ts.append({"tsuid": tsuid, "funcId": fid})

    def get_fid(self, tsuid):
        try:
            return [x['funcid'] for x in self.db_ts if x['tsuid'] == tsuid][0]
        except IndexError:
            raise IkatsNotFoundError()

    def delete_fid(self, tsuid):
        temp = [x for x in self.db_ts if x['tsuid'] != tsuid]
        self.db_ts = temp

    def metadata_create(self, tsuid, name, value, data_type=DTYPE.string, force_update=False):
        self.db_md[tsuid] = {'name': name, 'value': value, "data_type": data_type}

    def metadata_update(self, tsuid, name, value, data_type=DTYPE.string, force_create=False):
        self.db_md[tsuid] = {'name': name, 'value': value, "data_type": data_type}

    def metadata_delete(self, tsuid, name, raise_exception=True):
        md_list = self.db_md[tsuid]
        self.db_md[tsuid] = [x for x in md_list if x["name"] != name]

    def metadata_get(self, ts_list):
        return [self.db_md[x] for x in ts_list]

    def metadata_get_typed(self, ts_list):
        try:
            return [self.db_md[x] for x in ts_list]
        except KeyError:
            raise IkatsNotFoundError()

    def get_ts_from_metadata(self, constraint=None):
        raise NotImplementedError()

    def get_func_id_from_tsuid(self, tsuid):
        return [x['funcId'] for x in self.db_ts if x['tsuid'] == tsuid][0]

    def get_tsuid_from_fid(self, fid):
        try:
            return [x['tsuid'] for x in self.db_ts if x['funcId'] == fid][0]
        except IndexError:
            raise IkatsNotFoundError()

    def search_functional_identifiers(self, criterion_type, criteria_list):
        raise NotImplementedError()

    def table_create(self, data):
        self.db_table.append(data)

    def table_list(self, name=None, strict=True):
        return self.db_table

    def table_read(self, name):
        try:
            return [x for x in self.db_table if x['name'] == name][0]
        except IndexError:
            raise IkatsNotFoundError()

    def table_delete(self, name):
        self.db_table = [x for x in self.db_table if x['name'] != name]

    def ts_delete(self, tsuid, raise_exception=True):
        del self.db_ts[tsuid]

    def pid_results(self, pid):
        raise NotImplementedError()

    def rid_get(self, rid):
        try:
            return self.db_rid[rid]
        except IndexError:
            raise IkatsNotFoundError()

    def rid_delete(self, rid, raise_exception=True):
        del self.db_rid[rid]

    def rid_create(self, data, pid, name=None):
        raise NotImplementedError()
