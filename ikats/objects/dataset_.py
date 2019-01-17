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
from ikats.exceptions import IkatsNotFoundError
from ikats.lib import check_is_valid_ds_name, check_type
from ikats.objects.generic_ import IkatsObject
from ikats.objects.timeseries_ import Timeseries


class Dataset(IkatsObject):
    """
    Dataset class composed of information related to a single Dataset
    """

    def __init__(self, api, name=None, desc=None, ts=None):
        """
        See props for members description

        :param api: see IkatsObject

        :param name: Name of the Dataset
        :param desc: Description of the Dataset
        :param ts: List of Timeseries objects

        :type name: str or None
        :type desc: str or None
        :type ts: list of Timeseries
        """

        # Internal variables initialization
        super().__init__(api)
        self.__name = None
        self.__desc = None
        self.__ts = []
        self.__flag_ts_loaded = False

        # Initialization with provided parameters
        self.name = name
        self.desc = desc
        self.ts = ts

    @property
    def name(self):
        """
        Name of the dataset
        """
        return self.__name

    @name.setter
    def name(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="name", raise_exception=True)
        if value is not None:
            check_is_valid_ds_name(value=value, raise_exception=True)
            if self.__name != value:
                self.__flag_ts_loaded = False
                self.__name = value

    @property
    def desc(self):
        """
        Description of the Dataset
        """
        return self.__desc

    @desc.setter
    def desc(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="description", raise_exception=True)
        self.__desc = value
        if value is None:
            self.__desc = ""

    @property
    def ts(self):
        """
        List of Timeseries
        """
        # Lazy loading
        if not self.__flag_ts_loaded:
            if self.name is not None:
                try:
                    self.fetch()
                except IkatsNotFoundError:
                    pass
            self.__flag_ts_loaded = True
        return self.__ts

    @ts.setter
    def ts(self, value):
        check_type(value=value, allowed_types=[list, None], var_name="ts", raise_exception=True)
        if value is not None:
            for ts in value:
                check_type(value=ts, allowed_types=[str, Timeseries], var_name="ts", raise_exception=True)
            self.__ts = [x if isinstance(x, Timeseries) else Timeseries(tsuid=x, api=self.api) for x in value]

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Dataset %s" % self.name

    def __len__(self):
        """
        :returns: the number of Timeseries composing the dataset
        :rtype: int
        """
        return len(self.__ts)

    def __add__(self, other):
        """
        Creates a new Dataset composed of the list of Timeseries of both Original datasets
        The name of the resulting dataset will be empty (to be defined after the operation)
        The description will be: "Concatenation of the datasets %s and %s"

        :param other: other Dataset to add
        :type other: Dataset

        :returns: The new Dataset
        :rtype: Dataset
        """

        description = "Concatenation of the datasets {} and {}"

        return self.api.ds.new(desc=description.format(self.name, other.name),
                               ts=self.ts + other.ts)

    def add_ts(self, ts):
        """
        Append a Timeseries to this Dataset (but no save is performed)

        """
        if isinstance(ts, str):
            # Assuming this is a TSUID as a string
            ts_to_add = [Timeseries(tsuid=ts, api=self.api)]
        elif isinstance(ts, Timeseries):
            # Because we use "extend", the input is converted to a list
            ts_to_add = [ts]
        elif isinstance(ts, list):
            ts_to_add = ts
        else:
            raise TypeError("Unknown type for Timeseries to add")
        self.ts = self.ts.extend(
            [x if isinstance(x, Timeseries) else Timeseries(tsuid=x, api=self.api) for x in ts_to_add])

    def save(self, raise_exception=True):
        """
        Save the dataset to database (creation only, no update available)
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises ValueError: if dataset doesn't contain any Timeseries
        :raises IkatsConflictError: if Dataset name already exists in database
        """
        return self.api.ds.save(ds=self, raise_exception=raise_exception)

    def delete(self, deep=False, raise_exception=True):
        """
        Remove the dataset from database but keep the local object
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param deep: true to deeply remove dataset (tsuid and metadata erased)
        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)

        :type deep: bool
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *deep* is not a bool
        :raises IkatsNotFoundError: if dataset not found in database
        """
        return self.api.ds.delete(name=self.name, deep=deep, raise_exception=raise_exception)

    def fetch(self):
        """
        Reads the dataset sub-objects from database
        Retrieves the list of Timeseries and update object

        :raises TypeError: if name is not a Dataset
        :raises IkatsNotFoundError: if dataset not found in database
        """

        self.ts = self.api.ds.fetch(dataset=self)
