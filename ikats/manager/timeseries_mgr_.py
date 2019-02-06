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

import re

from ikats.client.datamodel_client import DatamodelClient
from ikats.client.datamodel_stub import DatamodelStub
from ikats.client.opentsdb_client import OpenTSDBClient
from ikats.client.opentsdb_stub import OpenTSDBStub
from ikats.exceptions import (IkatsConflictError, IkatsException,
                              IkatsNotFoundError)
from ikats.lib import (MDType, check_is_fid_valid, check_is_valid_epoch,
                       check_type)
from ikats.manager.generic_mgr_ import IkatsGenericApiEndPoint
from ikats.objects import Timeseries

NON_INHERITABLE_PATTERN = re.compile("^qual(.)*|ikats(.)*|funcId")


class IkatsTimeseriesMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Timeseries management
    """

    def __init__(self, *args, **kwargs):
        super(IkatsTimeseriesMgr, self).__init__(*args, **kwargs)
        if self.api.emulate:
            self.tsdb_client = OpenTSDBStub(session=self.api.session)
            self.dm_client = DatamodelStub(session=self.api.session)
        else:
            self.tsdb_client = OpenTSDBClient(session=self.api.session)
            self.dm_client = DatamodelClient(session=self.api.session)

    def new(self, fid=None, data=None):
        """
        Create an empty local Timeseries (if fid not provided)
        If fid is set, the identifier will be created to database

        :param fid: Identifier to create (if provided)
        :param data: List of data points as numpy array or python 2D-list

        :type fid: str
        :type data: list or np.array

        :returns: the Timeseries object
        :rtype: Timeseries

        :raises IkatsConflictError: if *fid* already present in database (use `get` instead of `new`)
        """
        if fid is None:
            ts = Timeseries(api=self.api)
        else:
            ts = self._create_ref(fid=fid)
        ts.data = data
        return ts

    def get(self, fid=None, tsuid=None):
        """
        Returns an existing Timeseries object by providing either its FID or TSUID (only one shall be provided)

        :param fid: FID of the Timeseries
        :param tsuid: TSUID of the Timeseries

        :type fid: str
        :type tsuid: str

        :returns: The Timeseries object
        :rtype: Timeseries

        :raises ValueError: if both *fid* and *tsuid* are set (or none of them)
        :raises IkatsNotFoundError: if the identifier was not found in database
        """

        if bool(fid) == bool(tsuid):
            raise ValueError("fid and tsuid are mutually exclusive")

        if fid is not None:
            tsuid = self.fid2tsuid(fid=fid, raise_exception=True)

        return Timeseries(api=self.api, tsuid=tsuid, fid=fid)

    def save(self, ts, parent=None, generate_metadata=True, raise_exception=True):
        """
        Import timeseries data points to database or update an existing timeseries with new points

        if *generate_metadata* is set or if no TSUID is present in *ts* object,
        the *ikats_start_date*, *ikats_end_date* and *qual_nb_points* will be
        overwritten by the first point date, last point date and number of points in *ts.data*

        *parent* is the original timeseries where metadata shall be taken from
        (except intrinsic ones, eg. *qual_nb_points*)

        If the timeseries is a new one (object has no tsuid defined), the computation of the metadata is forced

        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param ts: Timeseries object containing information about what to create
        :param parent: (optional) Timeseries object of inheritance parent
        :param generate_metadata: Generate metadata (set to False when doing partial import) (Default: True)
        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)

        :type ts: Timeseries
        :type parent: Timeseries
        :type generate_metadata: bool
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *ts* is not a valid Timeseries object

        """

        # Input checks
        check_type(ts, Timeseries, "ts", raise_exception=True)
        check_type(parent, [Timeseries, None], "parent", raise_exception=True)
        check_type(generate_metadata, bool, "generate_metadata", raise_exception=True)
        check_is_fid_valid(ts.fid, raise_exception=True)

        try:
            # First, we shall create the TSUID reference (if not provided)
            if ts.tsuid is None:
                ts.tsuid = self._create_ref(ts.fid).tsuid
                # If the TS is fresh, we force the creation of the metadata
                generate_metadata = True

            # Add points to this TSUID
            start_date, end_date, nb_points = self.tsdb_client.add_points(tsuid=ts.tsuid, data=ts.data)

            if generate_metadata:
                # ikats_start_date
                self.dm_client.metadata_update(tsuid=ts.tsuid, name='ikats_start_date', value=start_date,
                                               data_type=MDType.DATE, force_create=True)
                ts.metadata.set(name='ikats_start_date', value=start_date, dtype=MDType.DATE)

                # ikats_end_date
                self.dm_client.metadata_update(tsuid=ts.tsuid, name='ikats_end_date', value=end_date,
                                               data_type=MDType.DATE, force_create=True)
                ts.metadata.set(name='ikats_end_date', value=end_date, dtype=MDType.DATE)

                # qual_nb_points
                self.dm_client.metadata_update(tsuid=ts.tsuid, name='qual_nb_points', value=nb_points,
                                               data_type=MDType.NUMBER, force_create=True)
                ts.metadata.set(name='qual_nb_points', value=nb_points, dtype=MDType.NUMBER)

            # Inherit from parent when it is defined
            if parent is not None:
                self.inherit(ts=ts, parent=parent)
        except IkatsException:
            if raise_exception:
                raise
            return False
        return True

    def delete(self, ts, raise_exception=True):
        """
        Delete the data corresponding to a *ts* object and all associated metadata

        Note that if timeseries belongs to a dataset it will not be removed

        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param ts: tsuid of the timeseries or Timeseries Object to remove
        :param raise_exception: (optional) Indicates if IKATS exceptions shall be raised (True, default) or not (False)

        :type ts: str or Timeseries
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *ts* is not a str nor a Timeseries
        :raises IkatsNotFoundError: if timeseries is not found on server
        :raises IkatsConflictError: if timeseries belongs to -at least- one dataset
        """

        check_type(value=ts, allowed_types=[str, Timeseries], var_name="ts", raise_exception=True)

        tsuid = ts

        if isinstance(ts, Timeseries):
            if ts.tsuid is not None:
                tsuid = ts.tsuid
            elif ts.fid is not None:
                try:
                    tsuid = self.dm_client.get_tsuid_from_fid(fid=ts.fid)
                except IkatsException:
                    if raise_exception:
                        raise
                    return False
            else:
                raise ValueError("Timeseries object shall have set at least tsuid or fid")

        return self.dm_client.ts_delete(tsuid=tsuid, raise_exception=raise_exception)

    def list(self):
        """
        Get the list of all Timeseries from database

        .. note::
           This action may take a while

        :returns: the list of Timeseries object
        :rtype: list
        """

        return [Timeseries(tsuid=x["tsuid"], fid=x["funcId"], api=self.api) for x in
                self.dm_client.get_ts_list()]

    def fetch(self, ts, sd=None, ed=None):
        """
        Retrieve the data corresponding to a Timeseries object as a numpy array

        .. note::
            if omitted, *sd* (start date) and *ed* (end date) will be retrieved from metadata
            if you want a fixed windowed range, set *sd* and *ed* manually (but be aware that the TS may be
            not completely gathered)

        :param ts: Timeseries object
        :param sd: (optional) starting date (timestamp in ms from epoch)
        :param ed: (optional) ending date (timestamp in ms from epoch)

        :type ts: Timeseries
        :type sd: int or None
        :type ed: int or None

        :returns: The data points
        :rtype: np.array

        :raises TypeError: if *ts* is not a Timeseries object
        :raises TypeError: if *sd* is not an int
        :raises TypeError: if *ed* is not an int
        :raises IkatsNotFoundError: if TS data points couldn't be retrieved properly
        """

        check_type(value=ts, allowed_types=Timeseries, var_name="ts", raise_exception=True)
        check_type(value=sd, allowed_types=[int, None], var_name="sd", raise_exception=True)
        check_type(value=ed, allowed_types=[int, None], var_name="ed", raise_exception=True)

        if sd is None:
            sd = ts.metadata.get(name="ikats_start_date")
        check_is_valid_epoch(value=sd, raise_exception=True)

        if ed is None:
            ed = ts.metadata.get(name="ikats_end_date")
        check_is_valid_epoch(value=ed, raise_exception=True)

        try:
            data_points = self.tsdb_client.get_ts_by_tsuid(tsuid=ts.tsuid, sd=sd, ed=ed)

            # Return the points
            return data_points
        except ValueError:
            raise IkatsNotFoundError("TS data points couldn't be retrieved properly")

    def inherit(self, ts, parent):
        """
        Make a timeseries inherit of parent's metadata according to a pattern (not all metadata inherited)

        :param ts: TS object in IKATS (which will inherit)
        :param parent: TS object in IKATS of inheritance parent

        :type ts: Timeseries
        :param parent: Timeseries
        """
        try:
            result = self.dm_client.metadata_get_typed([parent.tsuid])[parent.tsuid]

            for meta_name in result:
                # Flag metadata as "not deleted"
                result[meta_name]["deleted"] = False

                if not NON_INHERITABLE_PATTERN.match(meta_name):
                    self.dm_client.metadata_create(tsuid=ts.tsuid, name=meta_name, value=result[meta_name]["value"],
                                                   data_type=MDType(result[meta_name]["dtype"]),
                                                   force_update=True)
        except(ValueError, TypeError, SystemError) as exception:
            self.api.session.log.warning(
                "Can't get metadata of parent TS (%s), nothing will be inherited; \nreason: %s", parent, exception)

    def find_from_meta(self, constraint=None):
        """
        From a metadata constraint provided in parameter, the method get a TS list matching these constraints

        Example of constraint:
            | {
            |     frequency: [1, 2],
            |     flight_phase: 8
            | }
        will find the TS having the following metadata:
            | (frequency == 1 OR frequency == 2)
            | AND
            | flight_phase == 8

        :param constraint: constraint definition
        :type constraint: dict

        :returns: list of TSUID matching the constraints
        :rtype: dict

        :raises TypeError: if *constraint* is not a dict
        """

        return self.dm_client.get_ts_from_metadata(constraint=constraint)

    def tsuid2fid(self, tsuid, raise_exception=True):
        """
        Retrieve the functional ID associated to the tsuid param.

        :param tsuid: one tsuid value
        :param raise_exception: Allow to specify if the action shall assert if not found or not

        :type tsuid: str
        :type raise_exception: bool

        :returns: retrieved functional identifier value
        :rtype: str

        :raises TypeError:  if tsuid is not a defined str
        :raises ValueError: no functional ID matching the tsuid
        :raises ServerError: http answer with status : 500 <= status < 600
        """
        try:
            return self.dm_client.get_func_id_from_tsuid(tsuid=tsuid)
        except IkatsException:
            if raise_exception:
                raise
            return None

    def fid2tsuid(self, fid, raise_exception=True):
        """
        Retrieve the TSUID associated to the functional ID param.

        :param fid: the functional Identifier
        :param raise_exception: Allow to specify if the action shall assert if not found or not

        :type fid: str
        :type raise_exception: bool

        :returns: retrieved TSUID value or None if not found
        :rtype: str

        :raises TypeError:  if fid is not str
        :raises IkatsNotFoundError: no match
        """

        check_is_fid_valid(fid=fid)

        # Check if fid already associated to an existing tsuid
        try:
            return self.dm_client.get_tsuid_from_fid(fid=fid)
        except IkatsException:
            if raise_exception:
                raise
            return None

    def _create_ref(self, fid):
        """
        Create a reference of timeseries in temporal data database and associate it to fid
        in temporal database for future use.
        Shall be used before create method in case of parallel creation of data (import data via spark for example)

        :param fid: Functional Identifier of the TS in Ikats
        :type fid: str

        :returns: A prepared Timeseries object
        :rtype: Timeseries

        :raises IkatsConflictError: if FID already present in database (use `get` instead of `new`)
        """
        check_is_fid_valid(fid, raise_exception=True)
        try:
            # Check if fid already associated to an existing tsuid
            tsuid = self.dm_client.get_tsuid_from_fid(fid=fid)
            # if fid already exists in database, raise a conflict exception
            raise IkatsConflictError("%s already associated to an existing tsuid: %s" % (fid, tsuid))

        except IkatsNotFoundError:
            # Creation of a new tsuid
            metric, tags = self.tsdb_client.gen_metric_tags()
            tsuid = self.tsdb_client.assign_metric(metric=metric, tags=tags)

            # finally importing tsuid/fid pair in non temporal database
            self.dm_client.import_fid(tsuid=tsuid, fid=fid)

            return Timeseries(tsuid=tsuid, fid=fid, api=self.api)
