#!/bin/python3
from enum import Enum

from ikats.objects.generic_ import IkatsObject
from ikats.lib import check_type


class DTYPE(Enum):
    """
    Enum used for Data types of Metadata
    """
    STRING = "string"
    DATE = "date"
    NUMBER = "number"
    COMPLEX = "complex"


class Metadata(IkatsObject):
    """
    Collection of Metadata information
    """

    def __init__(self, api, tsuid=None):
        """
        Initialization of the Metadata object
        No data are fetch (lazy mode)
        :param api:
        :param tsuid:
        """
        super().__init__(api)

        # Initialize
        self.__tsuid = None
        # Data are stored using the following format:
        # self.__data["metadata_name"] = {"value": "x", "dtype": "y", "deleted": False}
        # 'deleted' flag is used to mark metadata as deleted and trigger the deletion on save action
        self.__data = None

        # Assign
        self.tsuid = tsuid

    @property
    def tsuid(self):
        return self.__tsuid

    @tsuid.setter
    def tsuid(self, value):
        check_type(value, [str, None], "tsuid")
        self.__tsuid = value

    def fetch(self):
        """
        Fetch Metadata for the linked TSUID.
        In case of conflict between fetch and local data, fetch ones will overwrite local ones.
        """
        if self.tsuid is None:
            raise ValueError("No TSUID linked")

        # Get the results
        results = self.api.md.fetch(metadata=self)
        # Flag all retrieved Metadata as "not deleted"
        for md_name in results:
            results[md_name]["deleted"] = False

        # Empty local database
        if self.__data is None:
            self.__data = dict()
        # Overwrite previous ones
        self.__data.update(results)

    def set(self, name, value, dtype=None):
        """
        Create or update a metadata locally

        :param name:
        :param value:
        :param dtype:
        :return:
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
            self.__data[name]["dtype"] = self.__data[name].get("dtype", DTYPE.STRING)

    def get(self, name):
        """
        Get the metadata value if present in local cache.
        If cache is empty, fetch the data from database

        :param name: name of the metadata to get
        :type name: str

        :return: the value with defined type
        """

        # Update metadata if empty
        if self.__data is None:
            self.fetch()

        if name not in self.__data or self.__data[name]["deleted"]:
            raise ValueError("Metadata '%s' not defined" % name)

        value = self.__data[name]["value"]

        # Format the value depending on type
        if self.__data[name]["dtype"] == DTYPE.STRING:
            return str(value)
        elif self.__data[name]["dtype"] == DTYPE.NUMBER:
            if float(value).is_integer():
                return int(value)
            else:
                return float(value)
        elif self.__data[name]["dtype"] == DTYPE.DATE:
            return int(value)
        else:
            return value

    def get_type(self, name):
        """
        Get the metadata type if present in local cache.
        If cache is empty, fetch the data from database

        :param name: name of the metadata to get
        :type name: str

        :return: the value with defined type
        """

        # Input check
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        # Update metadata if empty
        if self.__data is None:
            self.fetch()

        # A metadata marked as 'deleted' shall not be returned
        if name not in self.__data or self.__data[name]["deleted"]:
            raise ValueError("Metadata '%s' not defined" % name)

        return self.__data[name]["dtype"]

    def delete(self, name):
        """
        Mark a metadata as 'deleted'
        The deletion will occur on remote side upon Metadata.save() action

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

    def __repr__(self):
        return "%s Metadata associated to TSUID %s" % (len(self.__data.keys()), self.__tsuid)

    def save(self):
        """
        Save the local Metadata database to the remote database.
        - New metadata will be created
        - Existing metadata will be updated with local values (overwriting remote ones)
        - metadata marked as 'deleted' will be deleted on remote database.
          If they don't exist, log the error and return False

        :return: the action status: True if everything fine, False otherwise
        :rtype: bool
        """
        result = True
        for md_name in self.__data:
            if self.__data[md_name]["deleted"]:
                result = result and self.api.md.delete()
            else:
                result = result and self.api.md.create(tsuid=self.tsuid,
                                                       name=md_name,
                                                       value=self.__data[md_name]["value"],
                                                       dtype=self.__data[md_name]["dtype"],
                                                       force_update=True)

        return result

    def __len__(self):
        return len(self.__data.keys())
