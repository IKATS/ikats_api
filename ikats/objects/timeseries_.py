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

import copy

from ikats.exceptions import IkatsNotFoundError
from ikats.lib import check_is_fid_valid, check_type
from ikats.objects.generic_ import IkatsObject
from ikats.objects.metadata_ import Metadata


class Timeseries(IkatsObject):
    """
    Timeseries class handling a full Timeseries object
    """

    def __init__(self, api, tsuid=None, fid=None, data=None):
        super().__init__(api)

        self.__md = None
        self.__tsuid = None
        self.__fid = None
        self.__data = []
        self.__flag_data_read = False

        self.metadata = Metadata(api=api, tsuid=tsuid)
        self.tsuid = tsuid
        self.fid = fid
        self.data = data

    def __len__(self):
        return len(self.data)

    @property
    def data(self):
        """
        return the data associated to this Timeseries as a numpy array
        :rtype: np.array
        """
        if not self.__flag_data_read and self.__tsuid is not None:
            try:
                self.__data = self.api.ts.fetch(ts=self)
            except IkatsNotFoundError:
                # For fresh created Timeseries, there is no metadata so the `fetch` method can't be performed
                # since it needs the ikats_start_date and ikats_end_date to work properly
                # An exception is raised but nothing else to do
                pass
            self.__flag_data_read = True
        return self.__data

    @data.setter
    def data(self, value):
        if check_type(value, [list], "data", raise_exception=False):
            self.__data = value

    @property
    def metadata(self):
        """
        Metadata object linked to this timeseries
        :rtype: Metadata
        """
        return self.__md

    @metadata.setter
    def metadata(self, value):
        check_type(value=value, allowed_types=[Metadata], var_name="metadata", raise_exception=True)
        self.__md = value

    @property
    def tsuid(self):
        """
        Getter for tsuid
        :rtype: str
        """
        return self.__tsuid

    @tsuid.setter
    def tsuid(self, value):
        """
        Setter for tsuid

        This setter shouldn't be used manually
        """
        check_type(value=value, allowed_types=[str, None], var_name="tsuid", raise_exception=True)
        if self.__tsuid != value:
            self.__tsuid = value
            # Update Metadata link
            self.metadata.tsuid = value

    @property
    def fid(self):
        """
        Getter for fid
        :rtype: str
        """
        return self.__fid

    @fid.setter
    def fid(self, value):
        if value is not None:
            check_is_fid_valid(fid=value, raise_exception=True)
            self.__fid = value

            # Try to get existing tsuid
            self.tsuid = self.api.ts.fid2tsuid(fid=self.fid, raise_exception=False)

    def __str__(self):
        if self.__tsuid is not None:
            return self.__tsuid
        return "<LocalTimeseries>"

    def __repr__(self):
        if self.__tsuid:
            return "Timeseries %s (%s)" % (self.fid, self.tsuid)
        return "<LocalTimeseries>"

    def __add__(self, other):
        ts = copy.deepcopy(self)
        ts.data += other.data
        if ts.tsuid is None:
            ts.tsuid = other.tsuid
        if ts.fid is None:
            ts.fid = other.fid
        return ts

    def save(self, parent=None, generate_metadata=False, raise_exception=True):
        """
        Import time series data points to database or update an existing time series with new points

        if *generate_metadata* is set or if no TSUID is present in *ts* object,
        the *ikats_start_date*, *ikats_end_date* and *qual_nb_points* will be
        overwritten by the first point date, last point date and number of points in *ts.data*

        *parent* is the original time series where metadata shall be taken from
        (except intrinsic ones, eg. *qual_nb_points*)

        If the time series is a new one (object has no tsuid defined), the computation of the metadata is forced

        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param parent: (optional) Timeseries object of inheritance parent
        :param generate_metadata: (optional) Generate metadata (set to False when doing partial import) (Default: True)
        :param raise_exception: (optional) Indicates if exceptions shall be raised (True, default) or not (False)

        :type parent: Timeseries
        :type generate_metadata: bool
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool
        """
        result = self.api.ts.save(ts=self, generate_metadata=generate_metadata, parent=parent,
                                  raise_exception=raise_exception)
        if parent is not None:
            self.metadata.fetch()
        return result and self.metadata.save()

    def delete(self, raise_exception=True):
        """
        Delete the data corresponding to a *ts* object and all associated metadata.
        Note that if time series belongs to a dataset, it will not be removed.
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param raise_exception: (optional) Indicates if IKATS exceptions shall be raised (True, default) or not (False)

        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises IkatsNotFoundError: if timeseries is not found on server
        :raises IkatsConflictError: if timeseries belongs to -at least- one dataset
        """
        return self.api.ts.delete(ts=self, raise_exception=raise_exception)

    # TODO: Doc
    def fetch(self, sd=None, ed=None):
        """
        Retrieve the data corresponding to a Timeseries object as a numpy array

        .. note::
            if omitted, *sd* (start date) and *ed* (end date) will be retrieved from metadata
            if you want a fixed windowed range, set *sd* and *ed* manually (but be aware that the TS may be
            not completely gathered)

        :param sd: (optional) starting date (timestamp in ms from epoch)
        :param ed: (optional) ending date (timestamp in ms from epoch)

        :type sd: int or None
        :type ed: int or None

        :returns:
        :rtype:

        :raises TypeError: if *ts* is not a Timeseries object
        :raises TypeError: if *sd* is not an int
        :raises TypeError: if *ed* is not an int
        :raises IkatsNotFoundError: if TS data points couldn't be retrieved properly
        """
        self.__data = self.api.ts.fetch(ts=self, sd=sd, ed=ed)
