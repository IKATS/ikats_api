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
from ikats.lib import MDType, check_type
from ikats.objects.generic_ import IkatsObject


class Metadata(IkatsObject):
    """
    Collection of Metadata information associated to a TSUID
    No data are fetch directly (lazy mode)
    """

    def __init__(self, api, tsuid=None):
        """
        :returns:

        :param api: see IkatsObject

        :param tsuid: TS identifier to link to these metadata

        :type tsuid: str
        """
        super().__init__(api)

        # Initialize
        self.__tsuid = None
        self.__data = None

        # Assign
        self.tsuid = tsuid

    @property
    def data(self):
        """
        Raw data object containing the dict of metadata
        Format is
        self.__data["metadata_name"] = {"value": "x", "dtype": "y", "deleted": False}
        'deleted' flag is used to mark metadata as deleted and trigger the deletion on save action
        :rtype: dict
        """
        return self.__data

    @data.setter
    def data(self, value):
        pass

    @property
    def tsuid(self):
        """
        TS identifier (TSUID) linked with these metadata
        :rtype: str
        """
        return self.__tsuid

    @tsuid.setter
    def tsuid(self, value):
        check_type(value, [str, None], "tsuid")
        self.__tsuid = value

    def set(self, name, value, dtype=None):
        """
        Create or update a metadata *locally*
        To synchronize with database, use

        :param name: name of the metadata to set
        :param value: value of the metadata
        :param dtype: metadata type

        :type name: str
        :type value: int, float, str
        :type dtype: DTYPE
        """
        # Empty local database
        if self.__data is None:
            self.__data = dict()

        # Metadata is absent
        if name not in self.__data:
            self.__data[name] = dict()

        # Set value
        self.__data[name]["value"] = value
        # Reset 'deleted' flag
        self.__data[name]["deleted"] = False
        # Update dtype to specified value or former value or string (default)
        if dtype is not None:
            self.__data[name]["dtype"] = dtype
        else:
            self.__data[name]["dtype"] = self.__data[name].get("dtype", MDType.STRING)

    def get(self, name):
        """
        Get the metadata value if present in local cache.
        If cache is empty, fetch the data from database

        :param name: name of the metadata to get

        :type name: str

        :returns: the value. the type depends on defined type

        :raises IkatsNotFoundError: if metadata doesn't exist
        """

        # Update metadata if empty
        if self.__data is None:
            self.fetch()

        if name not in self.__data or self.__data[name]["deleted"]:
            raise IkatsNotFoundError("Metadata '%s' not defined" % name)

        value = self.__data[name]["value"]

        # Format the value depending on type
        if self.__data[name]["dtype"] == MDType.STRING:
            return str(value)
        if self.__data[name]["dtype"] == MDType.NUMBER:
            if float(value).is_integer():
                return int(value)
            return float(value)
        if self.__data[name]["dtype"] == MDType.DATE:
            return int(value)
        return value

    def get_type(self, name):
        """
        Get the metadata type if present in local cache.
        If cache is empty, fetch the data from database

        :param name: name of the metadata to get
        :type name: str

        :returns: the type of the value
        :rtype: DTYPE

        :raises IkatsNotFoundError: if metadata doesn't exist
        """

        # Input check
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        # Update metadata if empty
        if self.__data is None:
            self.fetch()

        # A metadata marked as 'deleted' shall not be returned
        if name not in self.__data or self.__data[name]["deleted"]:
            raise IkatsNotFoundError("Metadata '%s' not defined" % name)

        return self.__data[name]["dtype"]

    def save(self):
        """
        Save the local Metadata database to the remote database.
        - New metadata will be created
        - Existing metadata will be updated with local values (overwriting remote ones)
        - metadata marked as 'deleted' will be deleted on remote database.
          If they don't exist, log the error and return False

        :returns: the action status: True if everything fine, False otherwise
        :rtype: bool

        """
        result = True
        for md_name in self.__data:
            if self.__data[md_name]["deleted"]:
                result = result and self.api.md.delete(tsuid=self.tsuid, name=md_name)
            else:
                result = result and self.api.md.save(tsuid=self.tsuid,
                                                     name=md_name,
                                                     value=self.__data[md_name]["value"],
                                                     dtype=self.__data[md_name]["dtype"])

        return result

    def delete(self, name):
        """
        Mark a metadata as 'deleted'
        The deletion will be trigger on remote side upon `Metadata.save()` action

        The marked metadata won't be accessible locally anymore
        However, the metadata can still be recreated again (using Metadata.set())

        :param name: Name of the metadata to delete
        :type name: str
        """
        # Input check
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        # Empty local database
        if self.__data is None:
            self.__data = dict()

        # Metadata is absent
        if name not in self.__data:
            self.__data[name] = dict()

        self.__data[name]["deleted"] = True

    def fetch(self):
        """
        Fetch Metadata for the linked TSUID.
        Overwrite local cache
        """
        if self.tsuid is None:
            raise ValueError("No TSUID linked")

        # Get the results
        self.__data = self.api.md.fetch(metadata=self)

    def __repr__(self):
        return "%s Metadata associated to TSUID %s" % (len(self.__data.keys()), self.__tsuid)

    def __len__(self):
        return len(self.__data.keys())
