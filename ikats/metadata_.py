#!/bin/python3
from .utils import check_type


class Metadata:
    def __init__(self, tsuid=None, session=None):
        self.__tsuid = tsuid
        self.__data = {}
        self.__session = session

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
