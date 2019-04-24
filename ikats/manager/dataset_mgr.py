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
from ikats.client.datamodel_client import DatamodelClient
from ikats.client.datamodel_stub import DatamodelStub
from ikats.exceptions import (IkatsConflictError, IkatsException,
                              IkatsInputError, IkatsNotFoundError)
from ikats.lib import check_is_valid_ds_name, check_type
from ikats.manager.generic_mgr_ import IkatsGenericApiEndPoint
from ikats.objects import Dataset, Timeseries


class IkatsDatasetMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Dataset management
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.api.emulate:
            self.dm_client = DatamodelStub(session=self.api.session)
        else:
            self.dm_client = DatamodelClient(session=self.api.session)

    def new(self, name=None, desc=None, ts=None):
        """
        Create an empty local Dataset with optional *name*, *desc* and *ts*

        :param name: Dataset name to use
        :param desc: description of the dataset
        :param ts: list of Timeseries composing the dataset

        :type name: str
        :type desc: str
        :type ts: list of Timeseries

        :returns: the Dataset object
        :rtype: Dataset

        :raises IkatsConflictError: if *name* already present in database
        """
        try:
            self.dm_client.dataset_read(name=name)
        except (IkatsNotFoundError, TypeError):
            # Type error occur when name is None
            return Dataset(api=self.api, name=name, desc=desc, ts=ts)
        raise IkatsConflictError("Dataset already exist. Try using `get()` method")

    def save(self, ds, raise_exception=True):
        """
        Save the dataset to database (creation only, no update available)
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param ds: Dataset to create
        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)

        :type ds: Dataset
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *ds* is not a valid Dataset object
        :raises ValueError: if *ds* doesn't contain any Timeseries
        :raises IkatsConflictError: if Dataset name already exists in database
        """

        check_is_valid_ds_name(ds.name, raise_exception=True)
        if ds.ts is None or (isinstance(ds.ts, list) and not ds.ts):
            raise ValueError("No TS to save")

        for ts in ds.ts:
            if ts.tsuid is None:
                raise IkatsInputError("TS %s doesn't have a TSUID" % ts.fid)

        try:
            self.dm_client.dataset_create(
                name=ds.name,
                description=ds.desc,
                ts=[x.tsuid for x in ds.ts])
        except IkatsException:
            if raise_exception:
                raise
            return False
        return True

    def get(self, name):
        """
        Reads the dataset information from database
        Retrieves description and list of Timeseries

        :param name: Dataset name
        :type name: str

        :returns: the retrieved Dataset object with Timeseries list filled
        :rtype: Dataset

        :raises TypeError: if *name* is not a Dataset
        :raises IkatsNotFoundError: if dataset not found in database
        """
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        result = self.dm_client.dataset_read(name)
        ts = [Timeseries(tsuid=x['tsuid'], fid=x['funcId'], api=self.api) for x in result.get('ts_list', [])]
        description = result.get("description", "")
        return Dataset(api=self.api, name=name, desc=description, ts=ts)

    def fetch(self, dataset):
        """
        Reads the dataset sub-objects from database
        Retrieves the list of Timeseries

        :param dataset: Dataset object
        :type dataset: Dataset

        :returns: the list of Timeseries composing the Dataset
        :rtype: list of Timeseries

        :raises TypeError: if name is not a Dataset
        :raises IkatsNotFoundError: if dataset not found in database
        """
        check_type(value=dataset, allowed_types=Dataset, var_name="dataset", raise_exception=True)

        result = self.dm_client.dataset_read(dataset.name)
        ts = [Timeseries(tsuid=x['tsuid'], fid=x['funcId'], api=self.api) for x in result.get('ts_list', [])]
        return ts

    def delete(self, name, deep=False, raise_exception=True):
        """
        Remove dataset from database
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param name: Dataset name to delete
        :param deep: true to deeply remove dataset (tsuid and metadata erased)
        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)

        :type name: str or Dataset
        :type deep: bool
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *name* is not a str
        :raises TypeError: if *deep* is not a bool
        :raises ValueError: if *name* is a valid name
        :raises IkatsNotFoundError: if dataset not found in database
        """
        check_type(value=deep, allowed_types=[bool, None], var_name="deep", raise_exception=True)
        check_type(value=name, allowed_types=[str, Dataset], var_name="name", raise_exception=True)

        if isinstance(name, Dataset):
            name = name.name

        check_is_valid_ds_name(value=name, raise_exception=True)

        try:
            self.dm_client.dataset_delete(name=name, deep=deep)
        except IkatsException:
            if raise_exception:
                raise
            return False
        return True

    def list(self):
        """
        Get the list of all datasets

        :returns: the list of Dataset objects
        :rtype: list of Dataset
        """

        return [Dataset(name=x["name"], desc=x["description"], api=self.api) for x in self.dm_client.dataset_list()]
