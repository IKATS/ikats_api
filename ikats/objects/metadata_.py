#!/bin/python3

from ikats.objects.generic_ import IkatsObject
from ikats.lib import check_type


class Metadata(IkatsObject):
    """
    Collection of Metadata information
    """
    def __init__(self, api, tsuid=None, kv_pairs=None):
        super().__init__(api)
        self.__tsuid = tsuid
        # Data are stored using the following format:
        # self.__data[metadata_name] = {"value": x, "dtype": y}
        self.__data = {}

        # Parse data kv_pairs
        pass

    @property
    def tsuid(self):
        return self.__tsuid

    @tsuid.setter
    def tsuid(self, value):
        check_type(value, [str, None], "tsuid")
        self.__tsuid = value

    def set(self, name, value, dtype='string'):
        if name not in self.__data:
            self.__data[name] = {}
        self.__data[name]["value"] = value
        self.__data[name]["dtype"] = dtype

    def get(self, name):
        if name not in self.__data:
            raise ValueError("Metadata '%s' not defined" % name)
        return self.__data[name]["value"]

    def __repr__(self):
        return self.__data

    def read(self, tsuid=None, name=None):
        if tsuid is None and self.tsuid is None:
            raise ValueError("No TSUID provided")
        if self.__data == {}:
            pass
        if name is not None:
            return self.__data.get(name, None)

    def save(self, tsuid=None, name=None, value=None, dtype=str):
        pass

    def __len__(self):
        return len(self.__data)
