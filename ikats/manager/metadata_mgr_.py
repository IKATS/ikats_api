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
from ikats.exceptions import IkatsException
from ikats.lib import MDType, check_type
from ikats.manager.generic_mgr_ import IkatsGenericApiEndPoint
from ikats.objects import Metadata


class IkatsMetadataMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Metadata management
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.api.emulate:
            self.dm_client = DatamodelStub(session=self.api.session)
        else:
            self.dm_client = DatamodelClient(session=self.api.session)

    def save(self, tsuid, name, value, dtype=MDType.STRING, raise_exception=True):
        """
        Save a metadata into Datamodel
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param tsuid: timeseries identifier (TSUID)
        :param name: name of the metadata to save
        :param value: Value of the metadata to save
        :param dtype: data type of the metadata save
        :param raise_exception: (optional) Indicates if Ikats exceptions shall be raised (True, default) or not (False)

        :type tsuid: str
        :type name: str
        :type value: str or number
        :type dtype: DTYPE
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *tsuid* not a str
        :raises TypeError: if *name* not a str
        :raises TypeError: if *value* not a str nor a number
        :raises TypeError: if *dtype* not a MDType

        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *name* is empty
        :raises ValueError: if *value* is empty
        :raises IkatsConflictError: if metadata couldn't be saved
        """

        try:
            result = self.dm_client.metadata_create(tsuid=tsuid, name=name, value=value,
                                                    data_type=dtype,
                                                    force_update=True)
            return result
        except IkatsException:
            if raise_exception:
                raise
            return False

    def delete(self, tsuid, name, raise_exception=True):
        """
        Delete a metadata associated to a TSUID
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param tsuid: tsuid associated to this metadata
        :param name: Name of the metadata
        :param raise_exception: (optional) Indicates if Ikats exceptions shall be raised (True, default) or not (False)

        :type tsuid: str
        :type name: str
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises IkatsNotFoundError: if metadata doesn't exist
        """
        return self.dm_client.metadata_delete(tsuid=tsuid, name=name, raise_exception=raise_exception)

    def fetch(self, metadata):
        """
        Fetch and return metadata information about the Metadata object provided

        The returned dict has the following format:
        {
          'md1':{'value':'value1', 'dtype': 'dtype', 'deleted': False},
          'md2':{'value':'value2', 'dtype': 'dtype', 'deleted': False}
        }

        :param metadata: Metadata object containing a valid tsuid
        :type metadata: Metadata

        :returns: the object containing information about each metadata matching the TSUID.
        :rtype: dict

        :raises IkatsNotFoundError: if metadata doesn't exist
        """

        check_type(value=metadata, allowed_types=Metadata, raise_exception=True)
        result = self.dm_client.metadata_get_typed(ts_list=[metadata.tsuid])[metadata.tsuid]

        for md in result:
            # Converts MDType
            result[md]["dtype"] = result[md]["dtype"]
            # Flag metadata as "not deleted"
            result[md]["deleted"] = False

        return result
